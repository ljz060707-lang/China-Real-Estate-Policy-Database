from __future__ import annotations

import json
import os
import re
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urljoin, urlsplit

import polars as pl
from bs4 import BeautifulSoup

from policydb.crawl.dedup import canonicalize_url
from policydb.crawl.fetcher import RespectfulFetcher
from policydb.crawl.models import RegisteredSource
from policydb.crawl.registry import (
    load_registry,
    materialize_registry_parquet,
    save_registry_atomic,
)
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

ROLE_ALIASES = {
    "municipal_government": ("人民政府", "市政府"),
    "government_gazette": ("政府公报", "人民政府公报"),
    "housing_department": ("住房和城乡建设", "住房保障", "房产局", "住建局"),
    "provident_fund_center": ("住房公积金", "公积金中心"),
    "natural_resources_department": ("自然资源", "规划和自然资源"),
}

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
            verified_domains = {
                str(row["domain"]).lower()
                for row in candidates.iter_rows(named=True)
                if bool(row.get("is_verified"))
            }
            verified_enabled_count = sum(
                source.crawl_enabled
                and source.official_domain_verified
                and source.domain.lower() in verified_domains
                for source in registered
            )
            enabled_pending_verification_count = max(
                0, enabled_count - verified_enabled_count
            )
            direct_healthy_count = sum(
                bool(row.get("is_verified"))
                and str(row.get("network_route") or "").lower()
                in {"direct", "direct_ok", "curl_fallback_ok"}
                for row in candidates.iter_rows(named=True)
            )
            parser_ready_count = sum(
                bool(row.get("is_verified"))
                and str(row.get("parser_status") or "").lower()
                in {"ok", "verified", "list_detected", "pagination_detected"}
                for row in candidates.iter_rows(named=True)
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
                    "preferred_candidate_id": next(
                        (
                            str(row["candidate_id"])
                            for row in candidates.iter_rows(named=True)
                            if bool(row.get("is_verified"))
                        ),
                        None,
                    ),
                    "candidate_count": candidate_count,
                    "registered_source_count": len(registered),
                    "verified_candidate_count": verified_count,
                    "enabled_source_count": enabled_count,
                    "verified_enabled_source_count": verified_enabled_count,
                    "enabled_pending_verification_count": enabled_pending_verification_count,
                    "direct_healthy_candidate_count": direct_healthy_count,
                    "parser_ready_candidate_count": parser_ready_count,
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
    rows: list[dict],
    settings: Settings | None = None,
    *,
    authoritative_review: bool = False,
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
                "pagination_strategy": row.get("pagination_strategy", "unknown"),
                "health_probe_count": int(row.get("health_probe_count") or 0),
                "health_probe_success_count": int(
                    row.get("health_probe_success_count") or 0
                ),
                "probe_evidence_json": row.get("probe_evidence_json", "[]"),
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
                "substitute_for_role": row.get("substitute_for_role"),
                "substitute_reason": row.get("substitute_reason"),
                "official_evidence": row.get("official_evidence"),
                "entry_evidence": row.get("entry_evidence"),
                "pagination_evidence": row.get("pagination_evidence"),
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
            # Ordinary discovery evidence must never downgrade a reviewed row.
            # A deterministic re-review is authoritative, however: otherwise a
            # detail page once marked verified can never be revoked.
            merged = {**previous, **incoming_row}
            if authoritative_review:
                merged["is_verified"] = bool(incoming_row.get("is_verified"))
                merged["is_enabled"] = bool(
                    incoming_row.get("is_enabled") and merged["is_verified"]
                )
            else:
                merged["is_verified"] = bool(
                    previous.get("is_verified") or incoming_row.get("is_verified")
                )
                merged["is_enabled"] = bool(
                    previous.get("is_enabled") or incoming_row.get("is_enabled")
                )
                if not incoming_row.get("last_checked_at"):
                    for field in (
                        "http_status",
                        "final_url",
                        "redirect_chain_json",
                        "network_route",
                        "health_status",
                        "robots_status",
                        "parser_status",
                        "pagination_strategy",
                        "health_probe_count",
                        "health_probe_success_count",
                        "probe_evidence_json",
                        "last_checked_at",
                    ):
                        merged[field] = previous.get(field)
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
            if (
                not authoritative_review
                and previous.get("manual_review_status") in {"approved", "verified"}
            ):
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
        primary_role = (
            source.agency_type
            if source.agency_type in REQUIRED_ROLES
            else source.source_role
        )
        role_urls: list[tuple[str, list[str | None]]] = []
        if primary_role in REQUIRED_ROLES:
            role_urls.append(
                (
                    primary_role,
                    [source.homepage_url, *source.list_page_urls, *source.seed_urls],
                )
            )
        if source.gazette_url:
            role_urls.append(("government_gazette", [source.gazette_url]))
        if not role_urls:
            continue
        for city_id in source.city_ids:
            city = city_lookup.get(str(city_id))
            if not city:
                continue
            for role, urls in role_urls:
                for url in dict.fromkeys(item for item in urls if item):
                    official = _official_domain(str(url))
                    city_name = str(city["city_name_short"])
                    city_evidence = city_name in source.source_name or city_name in str(url)
                    role_evidence = (
                        (role == "government_gazette" and url == source.gazette_url)
                        or source.agency_type == role
                        or source.source_role == role
                    )
                    rows.append({
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
                        # Registry metadata is discovery evidence, not a live
                        # parser/network attestation. A real probe and the
                        # deterministic verifier must approve this candidate.
                        "is_verified": False,
                        "is_enabled": False,
                        "manual_review_status": "pending_probe",
                        "health_status": source.health_status,
                        "last_checked_at": (
                            source.last_health_at.isoformat()
                            if source.last_health_at
                            else None
                        ),
                    })
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


def probe_candidates(
    *,
    city: str | None = None,
    limit: int | None = None,
    rounds: int = 2,
    settings: Settings | None = None,
    fetcher: RespectfulFetcher | None = None,
) -> dict:
    """Fetch candidate entries and retain auditable list/parser evidence."""
    settings = settings or Settings.discover()
    frame = list_candidates(city=city, settings=settings)
    if limit is not None:
        frame = frame.head(limit)
    if frame.is_empty():
        return {"checked": 0, "healthy": 0, "parser_verified": 0, "failed": 0}
    fetcher = fetcher or RespectfulFetcher(
        user_agent=settings.user_agent,
        timeout=settings.request_timeout,
        connect_timeout=settings.connect_timeout,
        retries=settings.max_retries,
        rate_limit=settings.default_rate_limit,
        check_robots=settings.respect_robots,
    )
    checked_rows: list[dict] = []
    healthy = parser_verified = failed = 0
    if rounds < 2:
        raise ValueError("candidate verification requires at least two independent probes")
    for row in frame.iter_rows(named=True):
        url = str(row["canonical_url"])
        evidence: list[dict] = []
        successful_rounds = 0
        parser_rounds = 0
        last_result = None
        last_detail_count = 0
        last_pagination_detected = False
        for round_number in range(1, rounds + 1):
            try:
                result = fetcher.fetch(url)
                last_result = result
                body_ok = bool(result.body)
                direct_ok = result.network_route in {
                    "direct",
                    "direct_ok",
                    "curl_fallback_ok",
                    "injected_client",
                }
                status_ok = result.status_code == 200 and body_ok and direct_ok
                detail_count = 0
                pagination_detected = False
                page_title = None
                breadcrumb_detected = False
                navigation_count = 0
                js_shell = False
                if "html" in str(result.content_type or "").lower() and body_ok:
                    soup = BeautifulSoup(result.body, "html.parser")
                    page_title = soup.title.get_text(" ", strip=True) if soup.title else None
                    text = soup.get_text(" ", strip=True)
                    breadcrumb_detected = bool(
                        soup.select(".breadcrumb, .crumb, .当前位置, [class*='location']")
                    )
                    navigation_count = len(soup.select("nav a, .nav a, [class*='menu'] a"))
                    js_shell = len(text) < 40 and bool(soup.find("script"))
                    origin = (urlsplit(result.final_url).hostname or "").lower()
                    for anchor in soup.find_all("a", href=True):
                        label = anchor.get_text(" ", strip=True)
                        target = urljoin(result.final_url, str(anchor.get("href") or ""))
                        if (urlsplit(target).hostname or "").lower() != origin:
                            continue
                        if canonicalize_url(target) == canonicalize_url(result.final_url):
                            continue
                        if anchor.get("rel") == ["next"] or re.search(
                            r"(?:下一页|下页|next|后页)", label, re.I
                        ):
                            pagination_detected = True
                            continue
                        if len(label) >= 4 and not re.search(
                            r"^(?:首页|登录|注册|返回|网站地图)$", label
                        ):
                            detail_count += 1
                parser_ok = status_ok and detail_count > 0 and not js_shell
                successful_rounds += int(status_ok)
                parser_rounds += int(parser_ok)
                last_detail_count = detail_count
                last_pagination_detected = pagination_detected
                evidence.append(
                    {
                        "round": round_number,
                        "checked_at": datetime.now(UTC).isoformat(),
                        "status_code": result.status_code,
                        "final_url": result.final_url,
                        "network_route": result.network_route,
                        "response_sha256": result.response_sha256,
                        "page_title": page_title,
                        "breadcrumb_detected": breadcrumb_detected,
                        "navigation_link_count": navigation_count,
                        "detail_link_count": detail_count,
                        "pagination_detected": pagination_detected,
                        "js_shell": js_shell,
                        "parser_ok": parser_ok,
                    }
                )
            except Exception as exc:
                evidence.append(
                    {
                        "round": round_number,
                        "checked_at": datetime.now(UTC).isoformat(),
                        "error_type": type(exc).__name__,
                    }
                )
        try:
            result = last_result
            if result is None:
                raise RuntimeError("all candidate probes failed")
            body_ok = bool(result.body)
            status_ok = successful_rounds == rounds
            parser_ok = parser_rounds == rounds
            parser_status = (
                "pagination_detected"
                if parser_ok and last_pagination_detected
                else "list_detected"
                if parser_ok
                else "no_list_links"
            )
            row.update(
                {
                    "http_status": result.status_code,
                    "final_url": result.final_url,
                    "redirect_chain_json": json.dumps(
                        result.redirect_chain, ensure_ascii=False, default=str
                    ),
                    "network_route": result.network_route,
                    "health_status": "healthy" if status_ok else "unhealthy",
                    "parser_status": parser_status,
                    "pagination_strategy": (
                        "next_link"
                        if last_pagination_detected
                        else "natural_single_page"
                        if parser_ok
                        else "unknown"
                    ),
                    "health_probe_count": rounds,
                    "health_probe_success_count": successful_rounds,
                    "probe_evidence_json": json.dumps(
                        evidence, ensure_ascii=False, default=str
                    ),
                    "last_checked_at": datetime.now(UTC).isoformat(),
                    "notes": json.dumps(
                        {
                            "detail_link_count": last_detail_count,
                            "pagination_detected": last_pagination_detected,
                            "response_sha256": result.response_sha256,
                        },
                        ensure_ascii=False,
                    ),
                }
            )
            healthy += int(status_ok)
            parser_verified += int(parser_ok)
        except Exception as exc:
            row.update(
                {
                    "health_status": "unhealthy",
                    "parser_status": "fetch_failed",
                    "pagination_strategy": "unknown",
                    "health_probe_count": rounds,
                    "health_probe_success_count": successful_rounds,
                    "probe_evidence_json": json.dumps(
                        evidence, ensure_ascii=False, default=str
                    ),
                    "last_checked_at": datetime.now(UTC).isoformat(),
                    "notes": json.dumps(
                        {"error_type": type(exc).__name__}, ensure_ascii=False
                    ),
                }
            )
            failed += 1
        checked_rows.append(row)
    upsert_candidates(checked_rows, settings, authoritative_review=True)
    verification = verify_candidates(city=city, settings=settings)
    return {
        "checked": len(checked_rows),
        "healthy": healthy,
        "parser_verified": parser_verified,
        "failed": failed,
        "verification": verification,
    }


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
    registry_by_id = {source.source_id: source for source in load_registry(settings)}
    verified_rows: list[dict] = []
    for row in frame.iter_rows(named=True):
        official = _official_domain(str(row["canonical_url"]))
        evidence_text = str(row.get("discovery_evidence_text") or "")
        source_id = (
            evidence_text.split("source_id=", 1)[1].split()[0].split(";", 1)[0]
            if "source_id=" in evidence_text
            else ""
        )
        registered = registry_by_id.get(source_id)
        registry_city_ok = bool(
            registered and str(row["city_id"]) in registered.city_ids
        )
        city_short = str(row["city_name"]).removesuffix("市")
        city_ok = bool(
            registry_city_ok
            or city_short in str(row.get("site_name") or "")
            or city_short in str(row.get("candidate_url") or "")
        )
        role = str(row["source_role"])
        role_terms = ROLE_ALIASES.get(role, (ROLE_TERMS.get(role, ""),))
        role_haystack = " ".join(
            str(row.get(field) or "")
            for field in ("site_name", "department_name")
        )
        registry_role_ok = bool(
            registered
            and (registered.agency_type == role or registered.source_role == role)
        )
        gazette_role_ok = bool(
            registered
            and role == "government_gazette"
            and registered.gazette_url
            and canonicalize_url(registered.gazette_url)
            == canonicalize_url(str(row["canonical_url"]))
        )
        role_ok = bool(
            registry_role_ok
            or gazette_role_ok
            or any(term and term in role_haystack for term in role_terms)
        )
        health_ok = str(row.get("health_status") or "").lower() in {
            "healthy",
            "ok",
            "direct_ok",
        }
        candidate_kind = str(row.get("candidate_kind") or "")
        page_type = str(row.get("page_type") or "")
        # Labels are evidence, never a bypass. Every promoted URL must itself be
        # reusable; policy/article detail pages are permanently ineligible.
        entry_ok = (
            is_reusable_source_entry(str(row["canonical_url"]))
            and page_type not in {"policy_detail", "content_page"}
            and candidate_kind != "policy_content_evidence"
        )
        parser_ok = str(row.get("parser_status") or "").lower() in {
            "ok",
            "verified",
            "list_detected",
            "pagination_detected",
        }
        http_ok = int(row.get("http_status") or 0) == 200
        two_probes_ok = int(row.get("health_probe_success_count") or 0) >= 2
        pagination_ok = str(row.get("pagination_strategy") or "") in {
            "next_link",
            "natural_single_page",
            "sitemap",
            "bounded_cursor",
        }
        verified = (
            official
            and city_ok
            and role_ok
            and health_ok
            and entry_ok
            and parser_ok
            and http_ok
            and two_probes_ok
            and pagination_ok
        )
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
                        + float(health_ok and parser_ok and http_ok)
                    )
                    / 4,
                ),
                "is_official": official,
                "is_verified": verified,
                "is_enabled": bool(row.get("is_enabled")) and verified,
                "manual_review_status": "approved" if verified else "rejected_by_gate",
                "last_checked_at": datetime.now(UTC).isoformat(),
            }
        )
        row.pop("city_name", None)
        verified_rows.append(row)
    upsert_candidates(verified_rows, settings, authoritative_review=True)
    return {
        "checked": len(verified_rows),
        "verified": sum(bool(row["is_verified"]) for row in verified_rows),
        "enabled": sum(bool(row["is_enabled"]) for row in verified_rows),
    }


def promote_candidate(
    candidate_id: str, *, settings: Settings | None = None
) -> dict:
    """Promote one verified reusable entry into the registry, disabled by default."""
    settings = settings or Settings.discover()
    frame = list_candidates(settings=settings).filter(
        pl.col("candidate_id") == candidate_id
    )
    if frame.height != 1:
        raise ValueError(f"unknown candidate_id: {candidate_id}")
    row = frame.row(0, named=True)
    if not bool(row.get("is_verified")):
        raise ValueError("candidate must pass deterministic verification first")
    if not is_reusable_source_entry(str(row["canonical_url"])):
        raise ValueError("candidate is a content/detail page, not a reusable source entry")
    if str(row.get("health_status") or "").lower() not in {
        "healthy", "ok", "direct_ok", "proxy_ok", "curl_fallback_ok"
    }:
        raise ValueError("candidate has no successful real-network health evidence")
    if str(row.get("parser_status") or "").lower() not in {
        "ok", "verified", "list_detected", "pagination_detected"
    }:
        raise ValueError("candidate parser/list-page evidence is not verified")

    canonical = str(row["canonical_url"])
    parsed = urlsplit(canonical)
    role = str(row["source_role"])
    city_id = str(row["city_id"])
    source_id = stable_id(city_id, role, parsed.hostname or canonical, prefix="SRC")
    sources = load_registry(settings)
    existing_index = next(
        (index for index, source in enumerate(sources) if source.source_id == source_id),
        None,
    )
    now = datetime.now(UTC)
    source = RegisteredSource(
        source_id=source_id,
        source_name=str(row.get("site_name") or row.get("department_name") or source_id),
        domain=str(parsed.hostname or ""),
        source_type="official_government",
        source_role=role,
        agency_type=role,
        official_status="official",
        seed_urls=[],
        list_page_urls=[canonical],
        homepage_url=f"{parsed.scheme}://{parsed.netloc}/",
        parser_adapter="generic_government",
        crawl_enabled=False,
        recommended_enabled=False,
        source_health_score=100.0,
        city_ids=[city_id],
        scope_type="municipal",
        required_level="required",
        verified_at=now,
        official_domain_verified=True,
        health_status="healthy",
        tls_status="ok",
        discovery_method=str(row.get("discovery_method") or "candidate_promotion"),
        substitute_for_role=row.get("substitute_for_role"),
        substitute_reason=row.get("substitute_reason"),
        official_evidence=(
            row.get("official_evidence") or row.get("official_domain_evidence")
        ),
        city_evidence=row.get("city_match_evidence"),
        role_evidence=row.get("role_match_evidence"),
        entry_evidence=(
            row.get("entry_evidence") or "reusable entry URL and live list probe"
        ),
        pagination_evidence=(
            row.get("pagination_evidence") or row.get("pagination_strategy")
        ),
        historical_entry_urls=[],
    )
    if existing_index is None:
        sources.append(source)
        action = f"promote_candidate={candidate_id};source={source_id}"
    else:
        previous = sources[existing_index]
        source = source.model_copy(
            update={
                "seed_urls": previous.seed_urls,
                "list_page_urls": sorted(set(previous.list_page_urls + [canonical])),
                "crawl_enabled": previous.crawl_enabled,
            }
        )
        sources[existing_index] = source
        action = f"merge_promoted_candidate={candidate_id};source={source_id}"
    save_registry_atomic(sources, settings, action=action)
    materialize_registry_parquet(sources, settings)
    build_requirement_slots(settings)
    return {"candidate_id": candidate_id, "source_id": source_id, "crawl_enabled": source.crawl_enabled}


def promote_verified_candidates(
    *,
    city: str | None = None,
    slot_id: str | None = None,
    settings: Settings | None = None,
) -> dict:
    """Promote every fully verified candidate in scope, without enabling it."""
    settings = settings or Settings.discover()
    frame = list_candidates(city=city, status="verified", settings=settings)
    if slot_id:
        frame = frame.filter(pl.col("slot_id") == slot_id)
    results = [
        promote_candidate(str(row["candidate_id"]), settings=settings)
        for row in frame.iter_rows(named=True)
    ]
    return {
        "city": city,
        "slot_id": slot_id,
        "promoted_candidates": len(results),
        "source_ids": sorted({str(row["source_id"]) for row in results}),
        "crawl_enabled": False,
    }


def enable_verified_sources(
    *, city: str | None = None, settings: Settings | None = None
) -> dict:
    """Strictly enable all eligible registered sources in scope."""
    settings = settings or Settings.discover()
    city_id = None
    if city:
        match = load_cities_105(settings).filter(
            (pl.col("city_name") == city)
            | (pl.col("city_name_short") == city)
            | (pl.col("city_id") == city)
        )
        if match.height != 1:
            raise ValueError(f"city is not uniquely resolved: {city}")
        city_id = str(match[0, "city_id"])
    enabled: list[str] = []
    rejected: list[dict] = []
    for source in load_registry(settings):
        if source.crawl_enabled or (city_id and city_id not in source.city_ids):
            continue
        try:
            enable_source_strict(source.source_id, settings=settings)
            enabled.append(source.source_id)
        except ValueError as exc:
            rejected.append(
                {"source_id": source.source_id, "reason": str(exc)}
            )
    return {
        "city": city,
        "enabled_count": len(enabled),
        "enabled_source_ids": enabled,
        "rejected_count": len(rejected),
        "rejected": rejected,
    }


def enable_source_strict(
    source_id: str, *, settings: Settings | None = None
) -> dict:
    """Enable only a reviewed source with a reusable list entry and healthy probe."""
    settings = settings or Settings.discover()
    sources = load_registry(settings)
    matches = [source for source in sources if source.source_id == source_id]
    if len(matches) != 1:
        raise ValueError(f"unknown source_id: {source_id}")
    source = matches[0]
    entries = [source.homepage_url, *source.list_page_urls]
    if not source.official_domain_verified:
        raise ValueError("official domain has not been verified")
    if source.health_status.lower() not in {"healthy", "ok", "direct_ok"}:
        raise ValueError("source has no current healthy direct-route evidence")
    if not any(url and is_reusable_source_entry(url) for url in entries):
        raise ValueError("source has no reusable homepage/list entry")
    if source.agency_type not in REQUIRED_ROLES and source.source_role not in REQUIRED_ROLES:
        raise ValueError("source is not assigned to a required role")
    role = (
        source.agency_type
        if source.agency_type in REQUIRED_ROLES
        else source.source_role
    )
    candidates = list_candidates(settings=settings)
    candidate_ok = any(
        bool(row.get("is_verified"))
        and str(row.get("city_id")) in source.city_ids
        and str(row.get("source_role")) == role
        and str(row.get("domain") or "").lower() == source.domain.lower()
        and str(row.get("parser_status") or "").lower()
        in {"ok", "verified", "list_detected", "pagination_detected"}
        for row in candidates.iter_rows(named=True)
    )
    if not candidate_ok:
        raise ValueError("source has no verified candidate with live parser evidence")
    updated = [
        item.model_copy(update={"crawl_enabled": True, "recommended_enabled": True})
        if item.source_id == source_id
        else item
        for item in sources
    ]
    save_registry_atomic(updated, settings, action=f"strict_enable={source_id}")
    materialize_registry_parquet(updated, settings)
    build_requirement_slots(settings)
    return {"source_id": source_id, "crawl_enabled": True}


def reconcile_registry(
    *, apply: bool = False, settings: Settings | None = None
) -> dict:
    """Find enabled registry rows that have no verified reusable candidate entry."""
    settings = settings or Settings.discover()
    candidates = list_candidates(settings=settings)
    valid_keys = {
        (str(row["city_id"]), str(row["source_role"]), str(row["domain"]))
        for row in candidates.iter_rows(named=True)
        if bool(row.get("is_verified"))
        and is_reusable_source_entry(str(row.get("canonical_url") or ""))
    }
    sources = load_registry(settings)
    invalid_ids: list[str] = []
    updated: list[RegisteredSource] = []
    for source in sources:
        role = source.agency_type if source.agency_type in REQUIRED_ROLES else source.source_role
        valid = any((city_id, role, source.domain.lower()) in valid_keys for city_id in source.city_ids)
        if source.crawl_enabled and role in REQUIRED_ROLES and not valid:
            invalid_ids.append(source.source_id)
            source = source.model_copy(
                update={
                    "crawl_enabled": False if apply else source.crawl_enabled,
                    "recommended_enabled": False,
                    "health_reason": "no_verified_reusable_entry",
                }
            )
        updated.append(source)
    if apply and invalid_ids:
        save_registry_atomic(updated, settings, action=f"reconcile_disable={len(invalid_ids)}")
        materialize_registry_parquet(updated, settings)
        build_requirement_slots(settings)
    return {"apply": apply, "invalid_enabled_count": len(invalid_ids), "source_ids": invalid_ids}


def reconcile_registry_roles(
    *, apply: bool = False, settings: Settings | None = None
) -> dict:
    """Correct only high-confidence organization-name role mismatches."""
    settings = settings or Settings.discover()
    sources = load_registry(settings)
    updated: list[RegisteredSource] = []
    changes: list[dict] = []
    for source in sources:
        name = " ".join(
            value
            for value in (
                source.source_name,
                source.organization_name_standardized or "",
            )
            if value
        )
        target = None
        evidence = None
        if "公积金" in name and source.agency_type != "provident_fund_center":
            target = "provident_fund_center"
            evidence = "organization name contains 公积金"
        if target:
            changes.append(
                {
                    "source_id": source.source_id,
                    "from_role": source.agency_type,
                    "to_role": target,
                    "evidence": evidence,
                }
            )
            if apply:
                source = source.model_copy(
                    update={
                        "agency_type": target,
                        "original_agency_type": source.agency_type,
                        "role_assignment_evidence": evidence,
                        "updated_at": datetime.now(UTC).isoformat(),
                    }
                )
        updated.append(source)
    if apply and changes:
        save_registry_atomic(
            updated, settings, action=f"reconcile_roles={len(changes)}"
        )
        materialize_registry_parquet(updated, settings)
        seed_candidates_from_registry(settings)
        verify_candidates(settings=settings)
        build_requirement_slots(settings)
    return {"apply": apply, "change_count": len(changes), "changes": changes}


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
        .otherwise(pl.col("preferred_candidate_id"))
        .alias("preferred_candidate_id"),
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
    registered = frame.filter(pl.col("registered_source_count") > 0).height
    direct_healthy = frame.filter(pl.col("direct_healthy_candidate_count") > 0).height
    parser_ready = frame.filter(pl.col("parser_ready_candidate_count") > 0).height
    coverage_status_counts = (
        dict(frame.group_by("coverage_status").len().iter_rows())
        if "coverage_status" in frame.columns
        else {}
    )
    return {
        "required_slots": required,
        "slots_resolved": verified,
        "slots_with_candidate": with_candidate,
        "slots_with_verified_candidate": verified,
        "slots_verified": verified,
        "slots_registered": registered,
        "slots_with_enabled_source": enabled,
        "slots_enabled": enabled,
        "slots_direct_healthy": direct_healthy,
        "slots_parser_ready": parser_ready,
        "enabled_unverified_slots": frame.filter(
            (pl.col("enabled_source_count") > 0)
            & (pl.col("verified_enabled_source_count") == 0)
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
