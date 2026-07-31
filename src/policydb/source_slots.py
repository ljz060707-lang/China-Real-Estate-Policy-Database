from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlsplit

import polars as pl

from policydb.crawl.dedup import canonicalize_url
from policydb.crawl.registry import load_registry
from policydb.scope import load_cities_105
from policydb.settings import Settings
from policydb.source_discovery import (
    REQUIRED_ROLES,
    ROLE_TERMS,
    is_reusable_source_entry,
    load_source_requirements,
)
from policydb.transform.normalization import stable_id

SLOT_STATUSES = {"unresolved", "candidate", "verified", "enabled", "rejected"}

COVERAGE_STATUS_NOTES = {
    "verified_enabled_source": "已有核验且启用的来源。",
    "enabled_source_pending_verification": "已有启用来源，但其官方入口证据仍待核验。",
    "department_entry_candidate": "已有独立部门或对应官方入口候选；仍未核验、未启用。",
    "municipal_portal_substitute_candidate": "仅有市政府统一公开入口替代候选；不得视为部门官网。",
    "content_evidence_only": "只有真实政策正文页证据；正文页不得作为持续采集入口。",
    "other_candidate_pending_review": "存在其他类型候选，尚不足以归为可持续入口。",
    "no_candidate": "尚无真实URL候选；保持未核验、未启用并等待人工补充。",
}


def _atomic_parquet(frame: pl.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    frame.write_parquet(temp, compression="zstd")
    os.replace(temp, path)


def _official_domain(url: str) -> bool:
    host = (urlsplit(url).hostname or "").lower().rstrip(".")
    return host == "gov.cn" or host.endswith(".gov.cn")


def slot_paths(settings: Settings | None = None) -> tuple[Path, Path]:
    settings = settings or Settings.discover()
    return (
        settings.curated / "source_requirement_slots.parquet",
        settings.curated / "source_candidates.parquet",
    )


def build_requirement_slots(settings: Settings | None = None) -> dict:
    """Materialize the auditable 105 × 5 requirement grid without inventing URLs."""
    settings = settings or Settings.discover()
    requirements = load_source_requirements(settings)
    cities = load_cities_105(settings)
    role_lookup = {
        item["city_id"]: list(item.get("required_roles") or REQUIRED_ROLES)
        for item in requirements.get("cities", [])
    }
    slot_path, candidate_path = slot_paths(settings)
    existing_candidates = (
        pl.read_parquet(candidate_path)
        if candidate_path.exists()
        else pl.DataFrame()
    )
    registry = load_registry(settings)
    now = datetime.now(UTC).isoformat()
    rows: list[dict] = []
    for city in cities.iter_rows(named=True):
        roles = role_lookup.get(str(city["city_id"]), list(REQUIRED_ROLES))
        for role in roles:
            slot_id = stable_id(city["city_id"], role, prefix="SLOT")
            candidates = (
                existing_candidates.filter(pl.col("slot_id") == slot_id)
                if existing_candidates.height
                else existing_candidates
            )
            registered = [
                source
                for source in registry
                if str(city["city_id"]) in source.city_ids
                and (source.agency_type == role or source.source_role == role)
            ]
            verified_count = (
                candidates.filter(pl.col("is_verified").fill_null(False)).height
                if candidates.height and "is_verified" in candidates.columns
                else 0
            )
            enabled_count = sum(source.crawl_enabled for source in registered)
            verified_enabled_count = sum(
                source.crawl_enabled and source.official_domain_verified
                for source in registered
            )
            enabled_pending_verification_count = max(
                0, enabled_count - verified_enabled_count
            )
            candidate_count = candidates.height
            candidate_kinds = (
                candidates["candidate_kind"].drop_nulls().to_list()
                if candidates.height and "candidate_kind" in candidates.columns
                else []
            )
            department_entry_count = sum(
                value in {"department_entry_candidate", "official_entry_candidate"}
                for value in candidate_kinds
            )
            municipal_substitute_count = sum(
                value == "municipal_portal_substitute_candidate"
                for value in candidate_kinds
            )
            content_evidence_count = sum(
                value == "policy_content_evidence" for value in candidate_kinds
            )
            other_candidate_count = max(
                0,
                candidate_count
                - department_entry_count
                - municipal_substitute_count
                - content_evidence_count,
            )
            coverage_status = (
                "verified_enabled_source"
                if verified_enabled_count
                else "enabled_source_pending_verification"
                if enabled_pending_verification_count
                else "department_entry_candidate"
                if department_entry_count
                else "municipal_portal_substitute_candidate"
                if municipal_substitute_count
                else "content_evidence_only"
                if content_evidence_count
                else "other_candidate_pending_review"
                if other_candidate_count
                else "no_candidate"
            )
            status = (
                "enabled"
                if enabled_count
                else "verified"
                if verified_count
                else "candidate"
                if candidate_count
                else "unresolved"
            )
            rows.append(
                {
                    "slot_id": slot_id,
                    "city_id": str(city["city_id"]),
                    "city_name": str(city["city_name"]),
                    "province_name": str(city["province_name"]),
                    "source_role": role,
                    "required": True,
                    "requirement_version": str(requirements.get("version", 1)),
                    "status": status,
                    "preferred_source_id": next(
                        (source.source_id for source in registered if source.crawl_enabled),
                        registered[0].source_id if registered else None,
                    ),
                    "candidate_count": candidate_count,
                    "verified_candidate_count": verified_count,
                    "enabled_source_count": enabled_count,
                    "verified_enabled_source_count": verified_enabled_count,
                    "enabled_pending_verification_count": enabled_pending_verification_count,
                    "department_entry_candidate_count": department_entry_count,
                    "municipal_substitute_candidate_count": municipal_substitute_count,
                    "content_evidence_count": content_evidence_count,
                    "other_candidate_count": other_candidate_count,
                    "coverage_status": coverage_status,
                    "resolution_note": COVERAGE_STATUS_NOTES[coverage_status],
                    "updated_at": now,
                }
            )
    frame = pl.DataFrame(rows, infer_schema_length=None).sort(
        ["province_name", "city_name", "source_role"]
    )
    _atomic_parquet(frame, slot_path)
    if frame.height != 525 or frame["city_id"].n_unique() != 105:
        raise ValueError(
            f"requirement grid is invalid: rows={frame.height}, cities={frame['city_id'].n_unique()}"
        )
    return audit_525(settings)


def upsert_candidates(
    rows: list[dict], settings: Settings | None = None
) -> dict[str, int]:
    settings = settings or Settings.discover()
    slot_path, candidate_path = slot_paths(settings)
    if not slot_path.exists():
        build_requirement_slots(settings)
    slots = pl.read_parquet(slot_path)
    now = datetime.now(UTC).isoformat()
    normalized: list[dict] = []
    for row in rows:
        city_id = str(row["city_id"])
        role = str(row["source_role"])
        match = slots.filter(
            (pl.col("city_id") == city_id) & (pl.col("source_role") == role)
        )
        if match.height != 1:
            raise ValueError(f"unknown source slot: {city_id}/{role}")
        candidate_url = str(row["candidate_url"])
        canonical = canonicalize_url(candidate_url)
        domain = (urlsplit(canonical).hostname or "").lower()
        slot_id = str(match[0, "slot_id"])
        normalized.append(
            {
                "candidate_id": stable_id(slot_id, canonical, prefix="SRCCAND"),
                "slot_id": slot_id,
                "city_id": city_id,
                "source_role": role,
                "candidate_url": candidate_url,
                "canonical_url": canonical,
                "domain": domain,
                "site_name": row.get("site_name"),
                "department_name": row.get("department_name"),
                "discovery_method": row.get("discovery_method", "registry_or_seed"),
                "discovery_evidence_url": row.get("discovery_evidence_url"),
                "discovery_evidence_text": row.get("discovery_evidence_text"),
                "official_domain_evidence": row.get("official_domain_evidence"),
                "city_match_evidence": row.get("city_match_evidence"),
                "role_match_evidence": row.get("role_match_evidence"),
                "http_status": row.get("http_status"),
                "final_url": row.get("final_url"),
                "redirect_chain_json": row.get("redirect_chain_json", "[]"),
                "network_route": row.get("network_route", "unknown"),
                "health_status": row.get("health_status", "pending"),
                "robots_status": row.get("robots_status", "unknown"),
                "parser_status": row.get("parser_status", "pending"),
                "official_confidence": float(row.get("official_confidence", 0.0)),
                "city_confidence": float(row.get("city_confidence", 0.0)),
                "role_confidence": float(row.get("role_confidence", 0.0)),
                "overall_confidence": float(row.get("overall_confidence", 0.0)),
                "is_official": bool(row.get("is_official", _official_domain(canonical))),
                "is_verified": bool(row.get("is_verified", False)),
                "is_enabled": bool(row.get("is_enabled", False)),
                "manual_review_status": row.get("manual_review_status", "pending"),
                "first_seen_at": row.get("first_seen_at", now),
                "last_checked_at": row.get("last_checked_at"),
                "candidate_kind": row.get("candidate_kind"),
                "page_type": row.get("page_type"),
                "entry_eligible": bool(row.get("entry_eligible", False)),
                "role_assignment_method": row.get("role_assignment_method"),
                "generation_batch_id": row.get("generation_batch_id"),
                "is_seed_derived": bool(row.get("is_seed_derived", False)),
                "has_seed_evidence": bool(row.get("has_seed_evidence", False)),
                "seed_evidence_count": int(row.get("seed_evidence_count", 0)),
                "source_record_count": int(row.get("source_record_count", 0)),
                "evidence_count": int(row.get("evidence_count", 0)),
                "conflict_count": int(row.get("conflict_count", 0)),
                "has_cross_jurisdiction_conflict": bool(
                    row.get("has_cross_jurisdiction_conflict", False)
                ),
                "notes": row.get("notes"),
            }
        )
    incoming_by_id = {str(row["candidate_id"]): row for row in normalized}
    if candidate_path.exists():
        existing = pl.read_parquet(candidate_path)
        existing_by_id = {
            str(row["candidate_id"]): row for row in existing.iter_rows(named=True)
        }
        for candidate_id, incoming_row in incoming_by_id.items():
            previous = existing_by_id.get(candidate_id)
            if not previous:
                existing_by_id[candidate_id] = incoming_row
                continue
            # Seed-derived evidence must never downgrade an already verified or
            # enabled registry candidate for the same slot and canonical URL.
            merged = {**previous, **incoming_row}
            merged["is_verified"] = bool(
                previous.get("is_verified") or incoming_row.get("is_verified")
            )
            merged["is_enabled"] = bool(
                previous.get("is_enabled") or incoming_row.get("is_enabled")
            )
            merged["first_seen_at"] = previous.get("first_seen_at") or incoming_row.get(
                "first_seen_at"
            )
            previous_is_seed = bool(previous.get("is_seed_derived"))
            incoming_is_seed = bool(incoming_row.get("is_seed_derived"))
            merged["has_seed_evidence"] = bool(
                previous.get("has_seed_evidence")
                or incoming_row.get("has_seed_evidence")
                or previous_is_seed
                or incoming_is_seed
            )
            merged["seed_evidence_count"] = max(
                int(previous.get("seed_evidence_count") or 0),
                int(incoming_row.get("seed_evidence_count") or 0),
            )
            # A URL with any pre-existing registry/search provenance is not a
            # newly created seed candidate.  The seed relationship lives in
            # source_candidate_evidence and must not inherit enablement.
            merged["is_seed_derived"] = previous_is_seed and incoming_is_seed
            if incoming_is_seed and not previous_is_seed:
                merged["discovery_method"] = previous.get("discovery_method")
                merged["discovery_evidence_url"] = previous.get(
                    "discovery_evidence_url"
                )
                merged["first_seen_at"] = previous.get("first_seen_at")
            if previous.get("manual_review_status") in {"approved", "verified"}:
                merged["manual_review_status"] = previous["manual_review_status"]
            existing_by_id[candidate_id] = merged
        incoming = pl.DataFrame(
            list(existing_by_id.values()), infer_schema_length=None
        )
    else:
        incoming = pl.DataFrame(list(incoming_by_id.values()), infer_schema_length=None)
    incoming = incoming.sort(["city_id", "source_role", "candidate_id"])
    _atomic_parquet(incoming, candidate_path)
    build_requirement_slots(settings)
    return {"upserted": len(rows), "total": incoming.height}


def seed_candidates_from_registry(settings: Settings | None = None) -> dict:
    settings = settings or Settings.discover()
    cities = load_cities_105(settings)
    city_lookup = {str(row["city_id"]): row for row in cities.iter_rows(named=True)}
    rows: list[dict] = []
    for source in load_registry(settings):
        role = (
            source.agency_type
            if source.agency_type in REQUIRED_ROLES
            else source.source_role
        )
        if role not in REQUIRED_ROLES:
            continue
        urls = [
            source.homepage_url,
            *source.list_page_urls,
            *source.seed_urls,
        ]
        for city_id in source.city_ids:
            city = city_lookup.get(str(city_id))
            if not city:
                continue
            for url in dict.fromkeys(item for item in urls if item):
                official = _official_domain(str(url))
                city_name = str(city["city_name_short"])
                city_evidence = city_name in source.source_name or city_name in str(url)
                role_term = ROLE_TERMS.get(role, "")
                role_evidence = (
                    role_term in source.source_name
                    or source.agency_type == role
                    or source.source_role == role
                )
                rows.append(
                    {
                        "city_id": city_id,
                        "source_role": role,
                        "candidate_url": url,
                        "site_name": source.source_name,
                        "department_name": source.organization_name_standardized,
                        "discovery_method": source.discovery_method
                        or "existing_registry",
                        "discovery_evidence_text": f"existing source_id={source.source_id}",
                        "official_domain_evidence": (
                            "hostname is gov.cn or a subdomain" if official else None
                        ),
                        "city_match_evidence": (
                            f"registry city_ids contains {city_id}" if city_evidence else None
                        ),
                        "role_match_evidence": (
                            f"registry role={role}" if role_evidence else None
                        ),
                        "official_confidence": 1.0 if official else 0.2,
                        "city_confidence": 1.0 if city_id in source.city_ids else 0.0,
                        "role_confidence": 1.0 if role_evidence else 0.0,
                        "overall_confidence": (
                            1.0 if official and city_evidence and role_evidence else 0.5
                        ),
                        "is_official": official,
                        "is_verified": bool(
                            source.official_domain_verified
                            and city_evidence
                            and role_evidence
                        ),
                        "is_enabled": source.crawl_enabled,
                        "manual_review_status": (
                            "approved"
                            if source.official_domain_verified
                            else "pending"
                        ),
                        "health_status": source.health_status,
                        "last_checked_at": (
                            source.last_health_at.isoformat()
                            if source.last_health_at
                            else None
                        ),
                    }
                )
    if not rows:
        build_requirement_slots(settings)
        return {"upserted": 0, "total": 0}
    return upsert_candidates(rows, settings)


def list_candidates(
    *,
    city: str | None = None,
    status: str | None = None,
    settings: Settings | None = None,
) -> pl.DataFrame:
    settings = settings or Settings.discover()
    slot_path, candidate_path = slot_paths(settings)
    if not candidate_path.exists():
        return pl.DataFrame()
    frame = pl.read_parquet(candidate_path)
    if city:
        cities = load_cities_105(settings).filter(
            (pl.col("city_name") == city)
            | (pl.col("city_name_short") == city)
            | (pl.col("city_id") == city)
        )
        if cities.height != 1:
            raise ValueError(f"city is not uniquely resolved: {city}")
        frame = frame.filter(pl.col("city_id") == cities[0, "city_id"])
    if status:
        if status == "pending":
            frame = frame.filter(~pl.col("is_verified"))
        elif status == "verified":
            frame = frame.filter(pl.col("is_verified"))
        elif status == "enabled":
            frame = frame.filter(pl.col("is_enabled"))
        else:
            frame = frame.filter(pl.col("manual_review_status") == status)
    return frame


def verify_candidates(
    *,
    city: str | None = None,
    settings: Settings | None = None,
) -> dict:
    """Apply only deterministic evidence gates; no candidate is enabled here."""
    settings = settings or Settings.discover()
    frame = list_candidates(city=city, settings=settings)
    if frame.is_empty():
        return {"checked": 0, "verified": 0, "enabled": 0}
    slots = pl.read_parquet(slot_paths(settings)[0]).select(
        "slot_id", "city_name", "source_role"
    )
    frame = frame.join(slots, on=["slot_id", "source_role"], how="left")
    verified_rows: list[dict] = []
    for row in frame.iter_rows(named=True):
        official = _official_domain(str(row["canonical_url"]))
        city_short = str(row["city_name"]).removesuffix("市")
        city_ok = bool(
            row.get("city_match_evidence")
            or city_short in str(row.get("site_name") or "")
            or city_short in str(row.get("candidate_url") or "")
        )
        role_term = ROLE_TERMS.get(str(row["source_role"]), "")
        role_ok = bool(
            row.get("role_match_evidence")
            or role_term in str(row.get("site_name") or "")
            or role_term in str(row.get("department_name") or "")
        )
        health_ok = str(row.get("health_status") or "").lower() in {
            "healthy",
            "ok",
            "direct_ok",
        }
        entry_ok = bool(row.get("entry_eligible", False)) or (
            str(row.get("candidate_kind") or "")
            in {"department_entry_candidate", "official_entry_candidate", "municipal_portal_substitute_candidate"}
            and is_reusable_source_entry(str(row["canonical_url"]))
        )
        verified = official and city_ok and role_ok and health_ok and entry_ok
        row.update(
            {
                "official_confidence": 1.0 if official else 0.0,
                "city_confidence": 1.0 if city_ok else 0.0,
                "role_confidence": 1.0 if role_ok else 0.0,
                "entry_eligible": entry_ok,
                "overall_confidence": 1.0 if verified else min(
                    0.89,
                    (
                        float(official)
                        + float(city_ok)
                        + float(role_ok)
                        + float(health_ok)
                    )
                    / 4,
                ),
                "is_official": official,
                "is_verified": verified,
                "manual_review_status": "approved" if verified else "pending",
                "last_checked_at": datetime.now(UTC).isoformat(),
            }
        )
        row.pop("city_name", None)
        verified_rows.append(row)
    upsert_candidates(verified_rows, settings)
    return {
        "checked": len(verified_rows),
        "verified": sum(bool(row["is_verified"]) for row in verified_rows),
        "enabled": sum(bool(row["is_enabled"]) for row in verified_rows),
    }


def resolve_slot(
    slot_id: str,
    *,
    candidate_id: str | None = None,
    note: str | None = None,
    settings: Settings | None = None,
) -> dict:
    settings = settings or Settings.discover()
    slot_path, candidate_path = slot_paths(settings)
    slots = pl.read_parquet(slot_path)
    if slots.filter(pl.col("slot_id") == slot_id).height != 1:
        raise ValueError(f"unknown slot_id: {slot_id}")
    if candidate_id:
        candidates = pl.read_parquet(candidate_path)
        candidate = candidates.filter(
            (pl.col("candidate_id") == candidate_id)
            & (pl.col("slot_id") == slot_id)
            & pl.col("is_verified")
        )
        if candidate.height != 1:
            raise ValueError("preferred candidate must be verified and belong to the slot")
        preferred = candidate_id
        status = "verified"
    else:
        preferred = None
        status = "unresolved"
    slots = slots.with_columns(
        pl.when(pl.col("slot_id") == slot_id)
        .then(pl.lit(preferred))
        .otherwise(pl.col("preferred_source_id"))
        .alias("preferred_source_id"),
        pl.when(pl.col("slot_id") == slot_id)
        .then(pl.lit(status))
        .otherwise(pl.col("status"))
        .alias("status"),
        pl.when(pl.col("slot_id") == slot_id)
        .then(pl.lit(note))
        .otherwise(pl.col("resolution_note"))
        .alias("resolution_note"),
    )
    _atomic_parquet(slots, slot_path)
    audit_path = settings.logs / "source_slot_audit.jsonl"
    audit_path.parent.mkdir(parents=True, exist_ok=True)
    with audit_path.open("a", encoding="utf-8") as stream:
        stream.write(
            json.dumps(
                {
                    "slot_id": slot_id,
                    "candidate_id": candidate_id,
                    "note": note,
                    "at": datetime.now(UTC).isoformat(),
                },
                ensure_ascii=False,
            )
            + "\n"
        )
    return {"slot_id": slot_id, "status": status, "preferred": preferred}


def audit_525(settings: Settings | None = None) -> dict:
    settings = settings or Settings.discover()
    slot_path, _ = slot_paths(settings)
    if not slot_path.exists():
        return build_requirement_slots(settings)
    frame = pl.read_parquet(slot_path)
    output = settings.outputs / "acceptance"
    output.mkdir(parents=True, exist_ok=True)
    csv_path = output / "source_525_audit.csv"
    frame.write_csv(csv_path)
    required = frame.height
    with_candidate = frame.filter(pl.col("candidate_count") > 0).height
    verified = frame.filter(pl.col("verified_candidate_count") > 0).height
    enabled = frame.filter(pl.col("enabled_source_count") > 0).height
    coverage_status_counts = (
        dict(frame.group_by("coverage_status").len().iter_rows())
        if "coverage_status" in frame.columns
        else {}
    )
    return {
        "required_slots": required,
        "slots_with_candidate": with_candidate,
        "slots_with_verified_candidate": verified,
        "slots_with_enabled_source": enabled,
        "enabled_unverified_slots": frame.filter(
            (pl.col("enabled_source_count") > 0)
            & (pl.col("verified_candidate_count") == 0)
        ).height,
        "slots_unresolved": required - verified,
        "candidate_coverage_pct": round(with_candidate / required * 100, 2),
        "verified_coverage_pct": round(verified / required * 100, 2),
        "enabled_coverage_pct": round(enabled / required * 100, 2),
        "coverage_status_counts": {
            str(key): int(value) for key, value in coverage_status_counts.items()
        },
        "cities": frame["city_id"].n_unique(),
        "output": str(csv_path),
    }
