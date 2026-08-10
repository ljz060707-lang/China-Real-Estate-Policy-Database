from __future__ import annotations

import hashlib
import re
from collections import defaultdict
from datetime import UTC, date, datetime
from io import BytesIO
from pathlib import Path
from urllib.parse import urlsplit

import polars as pl

from policydb.crawl.dedup import canonicalize_url
from policydb.parquet_store import atomic_write_parquet, read_parquet_snapshot
from policydb.scope import load_cities_105
from policydb.settings import Settings
from policydb.source_discovery import REQUIRED_ROLES, is_reusable_source_entry
from policydb.source_slots import build_requirement_slots, slot_paths, upsert_candidates
from policydb.transform.normalization import stable_id

GENERATOR_VERSION = "seed-record-jurisdiction-v1.1"

COVERAGE_STATUSES = (
    "no_candidate",
    "content_evidence_only",
    "municipal_portal_substitute_candidate",
    "department_entry_candidate",
    "other_candidate_pending_review",
    "enabled_source_pending_verification",
    "verified_enabled_source",
)

CANDIDATE_KINDS = {
    "policy_content_evidence",
    "municipal_portal_substitute_candidate",
    "department_entry_candidate",
    "official_entry_candidate",
}

_CONTENT_PATH = re.compile(
    r"(?:\.(?:s?html?|jhtml|aspx?)(?:$|[?#])|"
    r"/(?:art|article|content|detail|gi_news|info|news|notice|policy)/|"
    r"/t?20\d{2}(?:[-_/]?\d{2})|"
    r"[?&](?:id|articleid|infoid|docid|contentid)=)",
    re.IGNORECASE,
)
_ENTRY_PATH = re.compile(
    r"(?:^/?$|/(?:index(?:\.s?html?)?)?$|"
    r"/(?:zwgk|zfxxgk|zfgb|gkml|policy|zcwj|tzgg)/?$)",
    re.IGNORECASE,
)

_ROLE_HOST_PATTERNS = {
    "provident_fund_center": re.compile(
        r"(?:^|[.-])(?:z?fgjj|gjj|housefund)(?:[.-]|$)", re.IGNORECASE
    ),
    "natural_resources_department": re.compile(
        r"(?:^|[.-])(?:zrzy|ghzrzy|ghj|gtj|land|mnr)(?:[.-]|$)", re.IGNORECASE
    ),
    "housing_department": re.compile(
        r"(?:^|[.-])(?:zjj|zjw|fcj|fgj|jw|jsj|住房|房产)(?:[.-]|$)",
        re.IGNORECASE,
    ),
    "government_gazette": re.compile(
        r"(?:^|[.-])(?:zfgb|gongbao)(?:[.-]|$)", re.IGNORECASE
    ),
}

_ROLE_TEXT_TERMS = {
    "provident_fund_center": ("住房公积金管理中心", "公积金中心"),
    "natural_resources_department": (
        "自然资源和规划局",
        "自然资源局",
        "规划和自然资源局",
        "国土资源局",
    ),
    "housing_department": (
        "住房和城乡建设局",
        "住房城乡建设局",
        "住建局",
        "房产管理局",
        "住房保障和房产管理局",
    ),
    "government_gazette": ("政府公报",),
}


def is_official_gov_url(url: str | None) -> bool:
    """Return True only for the gov.cn registrable domain and its subdomains."""
    if not url:
        return False
    host = (urlsplit(str(url).strip()).hostname or "").lower().rstrip(".")
    return host == "gov.cn" or host.endswith(".gov.cn")


def classify_seed_page(url: str) -> str:
    """Conservatively distinguish content pages from reusable entry pages."""
    parsed = urlsplit(url)
    target = f"{parsed.path}?{parsed.query}" if parsed.query else parsed.path
    if is_reusable_source_entry(url) and not parsed.query:
        return "site_or_column_entry"
    if _CONTENT_PATH.search(target):
        return "policy_content_page"
    return "unknown_page"


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_parquet(frame: pl.DataFrame, path: Path) -> None:
    atomic_write_parquet(frame, path, {"module": "seed_source_candidates"})


def seed_candidate_paths(settings: Settings | None = None) -> tuple[Path, Path]:
    settings = settings or Settings.discover()
    return (
        settings.curated / "source_candidate_evidence.parquet",
        settings.curated / "source_candidate_generation_runs.parquet",
    )


def _batch_id(paths: list[Path]) -> str:
    evidence = [GENERATOR_VERSION]
    evidence.extend(
        f"{path.name}:{_file_sha256(path)}" for path in paths if path.exists()
    )
    return stable_id(*evidence, prefix="SRCSEEDRUN")


def _safe_string(value: object | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    value = str(value).strip()
    return value or None


def _registered_role(source: dict | None, city_id: str) -> tuple[str | None, str | None]:
    if not source:
        return None, None
    role = str(source.get("agency_type") or "")
    if role not in REQUIRED_ROLES:
        role = str(source.get("source_role") or "")
    if role not in REQUIRED_ROLES:
        return None, None
    registered_cities = [str(item) for item in (source.get("city_ids") or [])]
    if registered_cities and city_id not in registered_cities:
        return None, "registry_city_conflict"
    return role, "source_registry_role"


def infer_seed_role(
    *,
    url: str,
    title: str | None,
    source: dict | None,
    city_id: str,
) -> tuple[str, str, str]:
    """Infer one slot role without treating policy topic text as department proof."""
    registered, conflict = _registered_role(source, city_id)
    if registered and registered != "municipal_government":
        return registered, "source_registry_role", f"registry agency_type={registered}"
    host = (urlsplit(url).hostname or "").lower()
    source_label = " ".join(
        value
        for value in (
            _safe_string(source.get("source_name")) if source else None,
            _safe_string(source.get("agency_name")) if source else None,
        )
        if value
    )
    for role, pattern in _ROLE_HOST_PATTERNS.items():
        if pattern.search(host):
            return role, "official_host_pattern", f"official host matched {pattern.pattern}"
    if registered:
        return registered, "source_registry_role", f"registry agency_type={registered}"
    for role, terms in _ROLE_TEXT_TERMS.items():
        if source_label and any(term in source_label for term in terms):
            return role, "registered_source_label", f"source label matched {role}"
    if conflict:
        return "municipal_government", conflict, conflict
    # The record-jurisdiction edge proves that the content concerns the city, but
    # it does not prove a department.  Keep such evidence in the municipal slot.
    return (
        "municipal_government",
        "record_jurisdiction_fallback",
        "record-jurisdiction relation supports the city only; department is unknown",
    )


def _load_seed_rows(settings: Settings) -> pl.DataFrame:
    records_path = settings.curated / "records.parquet"
    seed_path = settings.curated / "source_seed_records.parquet"
    if seed_path.exists():
        seeds = read_parquet_snapshot(seed_path).select(
            "record_id",
            pl.col("seed_url").alias("original_url"),
            "source_id",
            "source_sheet",
            "source_cell",
        )
    else:
        records = read_parquet_snapshot(records_path)
        seeds = records.select(
            "record_id",
            pl.col("primary_source_url").alias("original_url"),
            pl.lit(None, dtype=pl.String).alias("source_id"),
            "source_sheet",
            pl.format("row:{}", pl.col("source_row")).alias("source_cell"),
        )
    return seeds.filter(pl.col("original_url").is_not_null()).unique(
        ["record_id", "original_url"], maintain_order=True
    )


def _city_record_edges(settings: Settings) -> tuple[pl.DataFrame, dict[str, int]]:
    relations = read_parquet_snapshot(settings.curated / "record_jurisdictions.parquet")
    jurisdictions = read_parquet_snapshot(settings.curated / "jurisdictions.parquet")
    cities = load_cities_105(settings).with_columns(
        pl.col("city_code").cast(pl.String).str.zfill(6).alias("administrative_code")
    )
    edges = (
        relations.join(
            jurisdictions.select("jurisdiction_id", "administrative_code", "level"),
            on="jurisdiction_id",
            how="left",
        )
        .join(
            cities.select(
                "city_id",
                "city_name",
                "city_name_short",
                "province_name",
                "administrative_code",
            ),
            on="administrative_code",
            how="inner",
        )
        .unique(["record_id", "city_id", "relation_type"], maintain_order=True)
    )
    city_counts = dict(
        edges.group_by("record_id")
        .agg(pl.col("city_id").n_unique().alias("city_count"))
        .select("record_id", "city_count")
        .iter_rows()
    )
    return edges, {str(key): int(value) for key, value in city_counts.items()}


def _candidate_kind(page_type: str, role: str) -> tuple[str, bool]:
    if page_type != "site_or_column_entry":
        return "policy_content_evidence", False
    if role == "municipal_government":
        return "official_entry_candidate", True
    return "department_entry_candidate", True


def _merge_evidence(existing: pl.DataFrame, rows: list[dict]) -> pl.DataFrame:
    incoming_ids = {str(row["evidence_id"]) for row in rows}
    existing_by_id = {}
    if existing.height:
        for row in existing.iter_rows(named=True):
            same_generator_family = str(row.get("generator_version") or "").startswith(
                "seed-record-jurisdiction-v"
            )
            if same_generator_family and str(row["evidence_id"]) not in incoming_ids:
                continue
            existing_by_id[str(row["evidence_id"])] = row
    for row in rows:
        old = existing_by_id.get(str(row["evidence_id"]))
        if old and old.get("generated_at"):
            row["generated_at"] = old["generated_at"]
        existing_by_id[str(row["evidence_id"])] = row
    return pl.DataFrame(list(existing_by_id.values()), infer_schema_length=None).sort(
        ["city_id", "source_role", "candidate_id", "record_id"]
    )


def generate_candidates_from_seed_records(
    settings: Settings | None = None,
    *,
    write: bool = True,
) -> dict:
    """Create disabled, unverified candidates from real seed URLs and city edges."""
    settings = settings or Settings.discover()
    required = [
        settings.curated / "records.parquet",
        settings.curated / "record_jurisdictions.parquet",
        settings.curated / "jurisdictions.parquet",
        settings.curated / "cities_105.parquet",
    ]
    missing = [str(path) for path in required if not path.exists()]
    if missing:
        raise FileNotFoundError(f"seed candidate inputs are missing: {missing}")
    seed_path = settings.curated / "source_seed_records.parquet"
    batch_id = _batch_id([*required, seed_path])
    generated_at = datetime.now(UTC).isoformat()
    records = read_parquet_snapshot(settings.curated / "records.parquet").select(
        "record_id", "title", "record_date", "geography_original"
    )
    seeds = _load_seed_rows(settings)
    edges, city_counts = _city_record_edges(settings)
    joined = seeds.join(records, on="record_id", how="left").join(
        edges, on="record_id", how="inner", suffix="_relation"
    )
    registry_path = settings.curated / "source_registry.parquet"
    registry = (
        read_parquet_snapshot(registry_path).to_dicts() if registry_path.exists() else []
    )
    sources_by_id = {str(row["source_id"]): row for row in registry}
    sources_by_domain: dict[str, list[dict]] = defaultdict(list)
    for source in registry:
        domain = str(source.get("domain") or "").lower().removeprefix("www.")
        if domain:
            sources_by_domain[domain].append(source)

    evidence_rows: list[dict] = []
    candidate_rows: dict[str, dict] = {}
    rejected_non_gov = 0
    for row in joined.iter_rows(named=True):
        original_url = str(row["original_url"]).strip()
        if not is_official_gov_url(original_url):
            rejected_non_gov += 1
            continue
        canonical_url = canonicalize_url(original_url)
        host = (urlsplit(canonical_url).hostname or "").lower().removeprefix("www.")
        source = sources_by_id.get(str(row.get("source_id") or ""))
        if not source:
            matching = sources_by_domain.get(host, [])
            source = matching[0] if len(matching) == 1 else None
        role, role_method, role_evidence = infer_seed_role(
            url=canonical_url,
            title=_safe_string(row.get("title")),
            source=source,
            city_id=str(row["city_id"]),
        )
        page_type = classify_seed_page(canonical_url)
        kind, entry_eligible = _candidate_kind(page_type, role)
        slot_id = stable_id(str(row["city_id"]), role, prefix="SLOT")
        candidate_id = stable_id(slot_id, canonical_url, prefix="SRCCAND")
        cross_city = city_counts.get(str(row["record_id"]), 0) > 1
        review_reason = "record_maps_to_multiple_105_cities" if cross_city else None
        evidence_id = stable_id(
            candidate_id,
            str(row["record_id"]),
            str(row["jurisdiction_id"]),
            str(row["relation_type"]),
            prefix="SRCEVID",
        )
        city_evidence = (
            f"record_jurisdictions {row['relation_type']} -> "
            f"{row['jurisdiction_name']} ({row['administrative_code']})"
        )
        evidence_rows.append(
            {
                "evidence_id": evidence_id,
                "candidate_id": candidate_id,
                "slot_id": slot_id,
                "city_id": str(row["city_id"]),
                "city_name": str(row["city_name"]),
                "province_name": str(row["province_name"]),
                "source_role": role,
                "candidate_kind": kind,
                "page_type": page_type,
                "entry_eligible": entry_eligible,
                "record_id": str(row["record_id"]),
                "record_title": _safe_string(row.get("title")),
                "record_date": _safe_string(row.get("record_date")),
                "original_url": original_url,
                "canonical_url": canonical_url,
                "source_id": _safe_string(row.get("source_id")),
                "source_sheet": _safe_string(row.get("source_sheet")),
                "source_cell": _safe_string(row.get("source_cell")),
                "geography_original": _safe_string(row.get("geography_original")),
                "jurisdiction_id": str(row["jurisdiction_id"]),
                "jurisdiction_name": str(row["jurisdiction_name"]),
                "relation_type": str(row["relation_type"]),
                "match_method": _safe_string(row.get("match_method")),
                "match_confidence": float(row.get("match_confidence") or 0.0),
                "role_assignment_method": role_method,
                "role_assignment_evidence": role_evidence,
                "official_domain_evidence": f"hostname={host}; suffix=.gov.cn",
                "city_match_evidence": city_evidence,
                "needs_manual_review": cross_city,
                "review_reason": review_reason,
                "is_verified": False,
                "is_enabled": False,
                "generation_batch_id": batch_id,
                "generator_version": GENERATOR_VERSION,
                "generated_at": generated_at,
            }
        )
        current = candidate_rows.setdefault(
            candidate_id,
            {
                "city_id": str(row["city_id"]),
                "source_role": role,
                "candidate_url": original_url,
                "site_name": _safe_string(source.get("source_name")) if source else host,
                "department_name": _safe_string(source.get("agency_name")) if source else None,
                "discovery_method": "seed_record_jurisdiction",
                "discovery_evidence_url": original_url,
                "discovery_evidence_text": city_evidence,
                "official_domain_evidence": f"hostname={host}; suffix=.gov.cn",
                "city_match_evidence": city_evidence,
                "role_match_evidence": role_evidence,
                "official_confidence": 1.0,
                "city_confidence": float(row.get("match_confidence") or 0.0),
                "role_confidence": 0.95 if role_method != "record_jurisdiction_fallback" else 0.4,
                "overall_confidence": 0.75 if entry_eligible else 0.55,
                "is_official": True,
                "is_verified": False,
                "is_enabled": False,
                "manual_review_status": "needs_review" if cross_city else "pending",
                "candidate_kind": kind,
                "page_type": page_type,
                "entry_eligible": entry_eligible,
                "role_assignment_method": role_method,
                "generation_batch_id": batch_id,
                "is_seed_derived": True,
                "has_seed_evidence": True,
                "seed_evidence_count": 0,
                "source_record_count": 0,
                "evidence_count": 0,
                "conflict_count": 0,
                "has_cross_jurisdiction_conflict": False,
                "notes": (
                    "Seed URL evidence only; unverified and disabled. "
                    "Content pages are not reusable crawl entries."
                ),
            },
        )
        current["source_record_count"] += 1
        current["evidence_count"] += 1
        current["seed_evidence_count"] += 1
        current["conflict_count"] += int(cross_city)
        current["has_cross_jurisdiction_conflict"] = bool(
            current["has_cross_jurisdiction_conflict"] or cross_city
        )
        if cross_city:
            current["manual_review_status"] = "needs_review"

    # A real city-government entry may stand in for missing department entries,
    # but remains a disabled substitute and never claims to be that department.
    by_city_role: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for item in candidate_rows.values():
        by_city_role[(item["city_id"], item["source_role"])].append(item)
    municipal_entries = {
        city_id: [
            item
            for item in items
            if item["entry_eligible"]
            and item["candidate_kind"] == "official_entry_candidate"
        ]
        for (city_id, role), items in by_city_role.items()
        if role == "municipal_government"
    }
    existing_evidence_by_candidate: dict[str, list[dict]] = defaultdict(list)
    for item in evidence_rows:
        existing_evidence_by_candidate[item["candidate_id"]].append(item)
    substitute_rows: list[dict] = []
    for city_id, municipal in municipal_entries.items():
        for role in REQUIRED_ROLES:
            if role == "municipal_government":
                continue
            independent = any(
                item["entry_eligible"]
                and item["candidate_kind"] == "department_entry_candidate"
                for item in by_city_role.get((city_id, role), [])
            )
            if independent:
                continue
            for entry in municipal:
                canonical_url = canonicalize_url(str(entry["candidate_url"]))
                slot_id = stable_id(city_id, role, prefix="SLOT")
                candidate_id = stable_id(slot_id, canonical_url, prefix="SRCCAND")
                substitute = {
                    **entry,
                    "city_id": city_id,
                    "source_role": role,
                    "candidate_kind": "municipal_portal_substitute_candidate",
                    "role_assignment_method": "municipal_entry_substitute",
                    "role_match_evidence": (
                        f"No independent {role} entry found; exact municipal entry retained only as substitute"
                    ),
                    "is_verified": False,
                    "is_enabled": False,
                    "manual_review_status": "pending",
                    "overall_confidence": min(float(entry["overall_confidence"]), 0.6),
                }
                candidate_rows[candidate_id] = substitute
                original_id = stable_id(
                    stable_id(city_id, "municipal_government", prefix="SLOT"),
                    canonical_url,
                    prefix="SRCCAND",
                )
                for evidence in existing_evidence_by_candidate.get(original_id, []):
                    copied = {
                        **evidence,
                        "candidate_id": candidate_id,
                        "slot_id": slot_id,
                        "source_role": role,
                        "candidate_kind": "municipal_portal_substitute_candidate",
                        "role_assignment_method": "municipal_entry_substitute",
                        "role_assignment_evidence": substitute["role_match_evidence"],
                    }
                    copied["evidence_id"] = stable_id(
                        candidate_id,
                        copied["record_id"],
                        copied["jurisdiction_id"],
                        copied["relation_type"],
                        prefix="SRCEVID",
                    )
                    substitute_rows.append(copied)
    evidence_rows.extend(substitute_rows)

    evidence_path, runs_path = seed_candidate_paths(settings)
    if write:
        _, candidate_path = slot_paths(settings)
        current_candidate_ids = set(candidate_rows)
        if candidate_path.exists():
            existing_candidates = read_parquet_snapshot(candidate_path)
            kept: list[dict] = []
            for candidate in existing_candidates.iter_rows(named=True):
                candidate_id = str(candidate["candidate_id"])
                pure_seed = bool(candidate.get("is_seed_derived"))
                if pure_seed and candidate_id not in current_candidate_ids:
                    continue
                if candidate_id not in current_candidate_ids:
                    candidate["has_seed_evidence"] = False
                    candidate["seed_evidence_count"] = 0
                    candidate["generation_batch_id"] = None
                kept.append(candidate)
            _atomic_parquet(
                pl.DataFrame(kept, infer_schema_length=None), candidate_path
            )
        build_requirement_slots(settings)
        upsert_candidates(list(candidate_rows.values()), settings)
        existing_evidence = (
            read_parquet_snapshot(evidence_path) if evidence_path.exists() else pl.DataFrame()
        )
        evidence_frame = _merge_evidence(existing_evidence, evidence_rows)
        _atomic_parquet(evidence_frame, evidence_path)
        run_row = {
            "generation_batch_id": batch_id,
            "generator_version": GENERATOR_VERSION,
            "generated_at": generated_at,
            "candidate_count": len(candidate_rows),
            "evidence_count": len(evidence_rows),
            "official_seed_record_count": len(
                {row["record_id"] for row in evidence_rows}
            ),
            "rejected_non_gov_url_count": rejected_non_gov,
        }
        if runs_path.exists():
            runs = read_parquet_snapshot(runs_path).filter(
                pl.col("generation_batch_id") != batch_id
            )
            runs = pl.concat(
                [runs, pl.DataFrame([run_row], infer_schema_length=None)],
                how="diagonal_relaxed",
            )
        else:
            runs = pl.DataFrame([run_row], infer_schema_length=None)
        _atomic_parquet(runs, runs_path)
        build_requirement_slots(settings)

    content_count = sum(
        row["candidate_kind"] == "policy_content_evidence"
        for row in candidate_rows.values()
    )
    result = {
        "generation_batch_id": batch_id,
        "write_applied": write,
        "joined_seed_city_edges": joined.height,
        "candidate_count": len(candidate_rows),
        "unique_url_count": len(
            {canonicalize_url(str(row["candidate_url"])) for row in candidate_rows.values()}
        ),
        "evidence_count": len(evidence_rows),
        "record_count": len({row["record_id"] for row in evidence_rows}),
        "city_count": len({row["city_id"] for row in evidence_rows}),
        "conflict_evidence_count": sum(
            bool(row["needs_manual_review"]) for row in evidence_rows
        ),
        "content_page_rejected_as_entry_count": content_count,
        "municipal_substitute_candidate_count": sum(
            row["candidate_kind"] == "municipal_portal_substitute_candidate"
            for row in candidate_rows.values()
        ),
        "department_entry_candidate_count": sum(
            row["candidate_kind"] == "department_entry_candidate"
            for row in candidate_rows.values()
        ),
        "rejected_non_gov_url_count": rejected_non_gov,
    }
    if write:
        result["coverage"] = export_source_candidate_audit(settings=settings)
    return result


def source_candidate_audit_frame(
    settings: Settings | None = None,
    *,
    city: str | None = None,
    source_role: str | None = None,
    coverage_status: str | None = None,
) -> pl.DataFrame:
    settings = settings or Settings.discover()
    slot_path, candidate_path = slot_paths(settings)
    if not slot_path.exists():
        return pl.DataFrame()
    slots = read_parquet_snapshot(slot_path)
    candidates = (
        read_parquet_snapshot(candidate_path) if candidate_path.exists() else pl.DataFrame()
    )
    if candidates.height:
        frame = slots.join(candidates, on=["slot_id", "city_id", "source_role"], how="left")
    else:
        frame = slots
    if city:
        frame = frame.filter(
            (pl.col("city_id") == city)
            | (pl.col("city_name") == city)
            | (pl.col("city_name").str.strip_suffix("市") == city)
        )
    if source_role:
        frame = frame.filter(pl.col("source_role") == source_role)
    if coverage_status:
        if coverage_status not in COVERAGE_STATUSES:
            raise ValueError(f"unsupported coverage status: {coverage_status}")
        frame = frame.filter(pl.col("coverage_status") == coverage_status)
    return frame.sort(["province_name", "city_name", "source_role", "candidate_id"])


def export_source_candidate_audit(
    output: Path | None = None,
    *,
    settings: Settings | None = None,
    city: str | None = None,
    source_role: str | None = None,
    coverage_status: str | None = None,
) -> dict:
    settings = settings or Settings.discover()
    frame = source_candidate_audit_frame(
        settings,
        city=city,
        source_role=source_role,
        coverage_status=coverage_status,
    )
    output_dir = settings.outputs / "source_candidates"
    output_dir.mkdir(parents=True, exist_ok=True)
    if output:
        paths = [output]
    else:
        paths = [
            output_dir / "source_candidate_audit.csv",
            output_dir / "source_candidate_audit.parquet",
            output_dir / "source_candidate_audit.xlsx",
        ]
    for path in paths:
        path.parent.mkdir(parents=True, exist_ok=True)
        suffix = path.suffix.lower()
        if suffix == ".csv":
            frame.write_csv(path)
        elif suffix == ".parquet":
            atomic_write_parquet(frame, path, {"module": "seed_source_candidates.export"})
        elif suffix == ".xlsx":
            frame.write_excel(path, autofit=True)
        else:
            raise ValueError(f"unsupported source audit export: {suffix}")
    status_counts = (
        dict(frame.select("slot_id", "coverage_status").unique().group_by("coverage_status").len().iter_rows())
        if frame.height
        else {}
    )
    candidate_rows = frame.filter(pl.col("candidate_id").is_not_null()) if frame.height and "candidate_id" in frame.columns else pl.DataFrame()
    return {
        "slot_count": frame["slot_id"].n_unique() if frame.height else 0,
        "candidate_count": candidate_rows["candidate_id"].n_unique() if candidate_rows.height else 0,
        "unique_url_count": candidate_rows["canonical_url"].n_unique() if candidate_rows.height else 0,
        "coverage_status_counts": {str(key): int(value) for key, value in status_counts.items()},
        "outputs": [str(path.resolve()) for path in paths],
    }


def audit_download_bytes(frame: pl.DataFrame, suffix: str) -> bytes:
    """Small UI helper kept independent from Streamlit."""
    if suffix == ".csv":
        return frame.write_csv().encode("utf-8-sig")
    if suffix == ".parquet":
        buffer = BytesIO()
        frame.write_parquet(buffer, compression="zstd")
        return buffer.getvalue()
    if suffix == ".xlsx":
        buffer = BytesIO()
        frame.write_excel(buffer, autofit=True)
        return buffer.getvalue()
    raise ValueError(f"unsupported download format: {suffix}")
