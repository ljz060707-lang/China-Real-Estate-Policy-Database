from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from urllib.parse import urlsplit

import polars as pl

from policydb.crawl.dedup import canonicalize_url
from policydb.parquet_store import atomic_write_parquet, read_parquet_snapshot
from policydb.scope import load_cities_105
from policydb.settings import Settings
from policydb.source_discovery import is_reusable_source_entry
from policydb.source_slots import slot_paths

_ROLE_HOST_HINTS = {
    "municipal_government": ("gov.cn", "zwfw.gov.cn"),
    "government_gazette": ("gongbao", "szfgb", "/gb/", "/gongbao"),
    "housing_department": ("zjw", "zjj", "cdzj", "housing"),
    "provident_fund_center": ("gjj", "zfgjj"),
    "natural_resources_department": ("ghj", "ghzrzy", "zrzy", "mpnr"),
}
_NON_SOURCE_HOST_MARKERS = ("news.", "zhidao.", "sohu.", "bendibao.", "m12333.", "baidu.")


def _official(url: str) -> bool:
    host = (urlsplit(url).hostname or "").lower().removeprefix("www.")
    return host == "gov.cn" or host.endswith(".gov.cn")


def _city_aliases(settings: Settings) -> dict[str, tuple[str, ...]]:
    cities = load_cities_105(settings)
    return {
        str(row["city_id"]): tuple(
            value.lower()
            for value in {
                str(row["city_name"]),
                str(row["city_name_short"]),
                *str(row["aliases"] or "").split("|"),
            }
            if value and value != "None"
        )
        for row in cities.iter_rows(named=True)
    }


def prefilter_ai_candidates(settings: Settings, *, output: Path | None = None) -> dict:
    """Filter only the existing AI batch; never writes source candidates."""
    _, candidate_path = slot_paths(settings)
    frame = read_parquet_snapshot(candidate_path).filter(
        pl.col("discovery_method") == "ai_assisted_search"
    )
    aliases = _city_aliases(settings)
    rows: list[dict] = []
    for row in frame.iter_rows(named=True):
        url = canonicalize_url(str(row.get("canonical_url") or row.get("candidate_url") or ""))
        parsed = urlsplit(url)
        host = (parsed.hostname or "").lower()
        haystack = " ".join(
            str(row.get(field) or "").lower()
            for field in ("site_name", "department_name", "discovery_evidence_text")
        )
        reasons: list[str] = []
        if parsed.scheme not in {"http", "https"} or not parsed.hostname:
            reasons.append("invalid_http_url")
        if not _official(url):
            reasons.append("non_official_domain")
        if parsed.path.lower().endswith((".pdf", ".doc", ".docx", ".xls", ".xlsx")):
            reasons.append("document_file")
        if not is_reusable_source_entry(url):
            reasons.append("not_reusable_entry")
        if str(row.get("candidate_kind") or "") == "policy_content_evidence" or str(row.get("page_type") or "") in {"policy_content_page", "policy_detail", "content_page"}:
            reasons.append("policy_detail_or_content")
        if any(marker in host for marker in _NON_SOURCE_HOST_MARKERS):
            reasons.append("news_or_repost_or_search_host")
        city_id = str(row["city_id"])
        if not any(alias in host or alias in parsed.path.lower() or alias in haystack for alias in aliases.get(city_id, ())):
            reasons.append("city_evidence_missing")
        role = str(row["source_role"])
        role_evidence = str(row.get("role_match_evidence") or "")
        role_hints = _ROLE_HOST_HINTS.get(role, ())
        if not role_evidence and not any(hint in host or hint in parsed.path.lower() for hint in role_hints):
            reasons.append("role_evidence_missing")
        rows.append({
            **row,
            "canonical_url": url,
            "prefilter_status": "shortlist" if not reasons else "rejected_by_deterministic_prefilter",
            "prefilter_reasons": json.dumps(reasons, ensure_ascii=False),
            "city_evidence_method": "city_alias_in_host_path_or_search_evidence" if "city_evidence_missing" not in reasons else None,
            "role_evidence_method": "role_evidence_field_or_role_host_hint" if "role_evidence_missing" not in reasons else None,
        })
    result = pl.DataFrame(rows, infer_schema_length=None) if rows else pl.DataFrame()
    if result.height:
        result = result.with_columns(
            pl.col("prefilter_status").eq("shortlist").alias("is_shortlist"),
            pl.int_range(0, pl.len()).over(["slot_id", "canonical_url"]).alias("duplicate_rank"),
        )
        result = result.with_columns(
            (pl.col("is_shortlist") & (pl.col("duplicate_rank") == 0)).alias("is_shortlist_unique")
        )
    shortlists = result.filter(pl.col("is_shortlist_unique")) if result.height else result
    summary = {
        "input_rows": frame.height,
        "input_slots": frame["slot_id"].n_unique() if frame.height else 0,
        "shortlist_rows": shortlists.height,
        "shortlist_slots": shortlists["slot_id"].n_unique() if shortlists.height else 0,
        "rejected_rows": frame.height - shortlists.height,
        "per_slot": shortlists.group_by(["slot_id", "city_id", "source_role"]).len().sort("slot_id").to_dicts() if shortlists.height else [],
        "rejection_reasons": [
            {"reason": reason, "count": count}
            for reason, count in Counter(
                reason
                for value in result["prefilter_reasons"].to_list()
                for reason in json.loads(value)
            ).most_common()
        ] if result.height else [],
    }
    if output is not None:
        output.mkdir(parents=True, exist_ok=True)
        atomic_write_parquet(result, output / "candidate_prefilter.parquet", {"job_id": "source-candidate-prefilter"})
        atomic_write_parquet(shortlists, output / "candidate_shortlists.parquet", {"job_id": "source-candidate-shortlists"})
        (output / "candidate_prefilter_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return {"summary": summary, "prefilter": result, "shortlists": shortlists}
