from __future__ import annotations

import json
from collections import Counter
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import polars as pl

from policydb.crawl.dedup import canonicalize_url
from policydb.parquet_store import atomic_write_parquet, read_parquet_snapshot
from policydb.scope import load_cities_105
from policydb.settings import Settings
from policydb.source_discovery import ROLE_SYNONYMS, is_reusable_source_entry
from policydb.source_jurisdiction import (
    JurisdictionMapping,
    is_central_authority_host,
    is_clear_detail_url,
    load_jurisdiction_mappings,
    mapping_for_candidate,
)
from policydb.source_slots import slot_paths

_ROLE_HOST_HINTS = {
    "municipal_government": ("gov.cn", "zwfw.gov.cn"),
    "government_gazette": ("gongbao", "szfgb", "/gb/", "/gongbao"),
    "housing_department": ("zjw", "zjj", "cdzj", "housing"),
    "provident_fund_center": ("gjj", "zfgjj"),
    "natural_resources_department": ("ghj", "ghzrzy", "zrzy", "mpnr"),
}
_NON_SOURCE_HOST_MARKERS = ("news.", "zhidao.", "sohu.", "bendibao.", "m12333.", "baidu.")


def _safe_aliases(settings: Settings) -> dict[str, tuple[str, ...]]:
    try:
        return _city_aliases(settings)
    except (FileNotFoundError, ValueError, KeyError):
        return {}


def _page_evidence_text(row: Mapping[str, Any]) -> str:
    """Return only evidence extracted from the real candidate page.

    Search title/snippet/query text is intentionally excluded from this
    helper.  It may rank a candidate, but it cannot establish jurisdiction or
    role until a real page has been fetched.
    """

    return " ".join(
        str(row.get(key) or "").strip()
        for key in (
            "page_title",
            "page_heading",
            "breadcrumb",
            "page_text_excerpt",
            "institution_evidence",
            "page_city_evidence",
            "page_role_evidence",
            "page_agency_evidence",
            "page_entry_type_evidence",
            "page_pagination_evidence",
        )
        if str(row.get(key) or "").strip()
    ).lower()


def _role_match(row: Mapping[str, Any]) -> bool:
    role = str(row.get("source_role") or "")
    host = (urlsplit(str(row.get("canonical_url") or row.get("candidate_url") or "")).hostname or "").lower()
    path = urlsplit(str(row.get("canonical_url") or row.get("candidate_url") or "")).path.lower()
    evidence = str(row.get("role_match_evidence") or "").lower()
    hints = _ROLE_HOST_HINTS.get(role, ())
    page_evidence = _page_evidence_text(row)
    role_terms = ROLE_SYNONYMS.get(role, ())
    return bool(evidence.strip()) or any(hint.lower() in host or hint.lower() in path for hint in hints) or any(
        term.lower() in page_evidence for term in role_terms if term
    )


def _list_shape_hint(url: str) -> bool:
    parsed = urlsplit(url)
    basename = parsed.path.rstrip("/").rsplit("/", 1)[-1].lower()
    return bool(
        basename in {"index", "index.html", "index.htm", "index.shtml", "list.html", "list.shtml"}
        or "index" in basename
        or "/zwgk/" in parsed.path.lower()
        or "/zccc/" in parsed.path.lower()
        or "/gkml/" in parsed.path.lower()
    )


def _city_match(
    row: Mapping[str, Any],
    aliases: Mapping[str, tuple[str, ...]],
    mapping: Mapping[str, Any],
) -> tuple[bool, bool]:
    city_id = str(row.get("city_id") or "")
    url = str(row.get("canonical_url") or row.get("candidate_url") or "")
    parsed = urlsplit(url)
    target = " ".join((parsed.netloc, parsed.path)).lower()
    city_evidence = str(row.get("city_match_evidence") or "").lower()
    page_evidence = _page_evidence_text(row)
    target_aliases = aliases.get(city_id, ())
    url_match = any(alias and alias in target for alias in target_aliases)
    # Search title/snippet is preserved as evidence, but does not by itself
    # establish city identity.  A city_id or URL/path/explicit evidence method
    # is required unless an approved jurisdiction mapping applies.
    explicit_evidence = bool(
        city_id.lower() in city_evidence
        or (any(alias in city_evidence for alias in target_aliases) and "title_only" not in city_evidence)
        or any(alias in page_evidence for alias in target_aliases if len(alias) >= 2)
    )
    mapping_pass = str(mapping.get("status") or "") == "PASS"
    mapping_fail = str(mapping.get("status") or "") == "FAIL"
    return mapping_pass or url_match or explicit_evidence, mapping_fail


def _weak_relevance(row: Mapping[str, Any], aliases: Mapping[str, tuple[str, ...]], mapping: Mapping[str, Any]) -> bool:
    """Identify B-class candidates worth one bounded evidence fetch."""

    role = str(row.get("source_role") or "")
    url = str(row.get("canonical_url") or row.get("candidate_url") or "")
    parsed = urlsplit(url)
    discovery_text = " ".join(
        str(row.get(key) or "")
        for key in (
            "candidate_title",
            "candidate_snippet",
            "site_name",
            "department_name",
            "discovery_evidence_text",
        )
    ).lower()
    url_text = " ".join((parsed.netloc, parsed.path)).lower()
    role_terms = ROLE_SYNONYMS.get(role, ())
    city_terms = tuple(value for value in aliases.get(str(row.get("city_id") or ""), ()) if len(value) >= 2)
    return bool(
        mapping.get("status") == "PASS"
        or any(term.lower() in discovery_text or term.lower() in url_text for term in role_terms if term)
        or any(term.lower() in discovery_text or term.lower() in url_text for term in city_terms)
        or any(hint.lower() in url_text for hint in _ROLE_HOST_HINTS.get(role, ()))
    )


def prefilter_candidate_frame(
    settings: Settings,
    frame: pl.DataFrame,
    *,
    mappings: Iterable[JurisdictionMapping] | None = None,
) -> pl.DataFrame:
    """Deterministically filter search evidence before formal candidate work.

    The returned frame retains every input row.  Rejected rows are evidence
    only; callers must select formal candidates from ``prefilter_status``
    ``shortlist`` rows.
    """

    if frame.is_empty():
        return frame
    aliases = _safe_aliases(settings)
    mapping_list = list(mappings) if mappings is not None else load_jurisdiction_mappings(settings)
    rows: list[dict[str, Any]] = []
    for raw in frame.iter_rows(named=True):
        row = dict(raw)
        url = canonicalize_url(str(row.get("canonical_url") or row.get("candidate_url") or ""))
        parsed = urlsplit(url)
        host = (parsed.hostname or "").lower().rstrip(".")
        title = str(row.get("candidate_title") or row.get("title") or "").strip()
        snippet = str(row.get("candidate_snippet") or row.get("snippet") or "").strip()
        site_name = str(row.get("site_name") or title).strip() or None
        department_name = str(row.get("department_name") or title).strip() or None
        evidence_text = str(row.get("discovery_evidence_text") or "").strip()
        if title or snippet:
            evidence_text = " ".join(value for value in (evidence_text, title, snippet) if value)[:4000]
        row.update(
            {
                "candidate_url": row.get("candidate_url") or url,
                "canonical_url": url,
                "candidate_title": title or None,
                "candidate_snippet": snippet or None,
                "site_name": site_name,
                "department_name": department_name,
                "discovery_evidence_text": evidence_text or None,
                "evidence_source": row.get("evidence_source") or "search_provider",
            }
        )
        slot = {"city_id": row.get("city_id"), "source_role": row.get("source_role")}
        mapping = mapping_for_candidate(row, slot, settings=settings, mappings=mapping_list)
        reasons: list[str] = []
        if parsed.scheme not in {"http", "https"} or not parsed.hostname:
            reasons.append("invalid_http_url")
        if not _official(url):
            reasons.append("non_official_domain")
        if parsed.path.lower().endswith((".pdf", ".doc", ".docx", ".xls", ".xlsx")):
            reasons.append("document_file")
        if is_clear_detail_url(url):
            reasons.append("detail_or_legal_page")
        static_entry_hint = is_reusable_source_entry(url) or _list_shape_hint(url)
        if not static_entry_hint:
            reasons.append("not_reusable_entry")
        if (
            (str(row.get("candidate_kind") or "") == "policy_content_evidence" or str(row.get("page_type") or "") in {"policy_content_page", "policy_detail", "content_page"})
            and not _list_shape_hint(url)
        ):
            reasons.append("policy_detail_or_content")
        if any(marker in host for marker in _NON_SOURCE_HOST_MARKERS):
            reasons.append("news_or_repost_or_search_host")
        if is_central_authority_host(host) and mapping.get("status") != "PASS":
            reasons.append("central_authority_wrongly_assigned")
        city_ok, mapping_fail = _city_match(row, aliases, mapping)
        semantic_registry_available = bool(aliases or mapping_list)
        if mapping_fail:
            reasons.append(str(mapping.get("reason_code") or "jurisdiction_mapping_mismatch"))
        if not city_ok and semantic_registry_available:
            reasons.append("city_evidence_missing")
        if not _role_match(row) and semantic_registry_available:
            reasons.append("role_evidence_missing")
        unique_reasons = list(dict.fromkeys(reasons))
        missing_evidence = {"city_evidence_missing", "role_evidence_missing"}
        hard_reasons = [reason for reason in unique_reasons if reason not in missing_evidence]
        enrichment_status = str(row.get("evidence_enrichment_status") or "").lower()
        can_enrich = bool(
            semantic_registry_available
            and not hard_reasons
            and any(reason in missing_evidence for reason in unique_reasons)
            and _weak_relevance(row, aliases, mapping)
            and enrichment_status not in {"completed", "failed"}
        )
        page_evidence = _page_evidence_text(row)
        page_city_match = any(
            alias in page_evidence
            for alias in aliases.get(str(row.get("city_id") or ""), ())
            if len(alias) >= 2
        )
        page_role_match = any(
            term.lower() in page_evidence
            for term in ROLE_SYNONYMS.get(str(row.get("source_role") or ""), ())
            if term
        )
        city_evidence_value = str(row.get("city_match_evidence") or "").strip() or (
            f"jurisdiction_mapping:{mapping.get('mapping_id')}" if mapping.get("status") == "PASS" else None
        )
        if page_city_match:
            city_evidence_value = "page_evidence:city_match"
        role_evidence_value = str(row.get("role_match_evidence") or "").strip() or (
            f"role_host_hint:{row.get('source_role')}" if _role_match(row) else None
        )
        if page_role_match:
            role_evidence_value = "page_evidence:role_match"
        if not unique_reasons:
            prefilter_status = "shortlist"
        elif can_enrich:
            prefilter_status = "evidence_enrichment_probe"
        else:
            prefilter_status = "rejected_by_deterministic_prefilter"
        row.update(
            {
                "prefilter_status": prefilter_status,
                "prefilter_reasons": json.dumps(unique_reasons, ensure_ascii=False),
                "prefilter_reason_codes": unique_reasons,
                "city_match_evidence": city_evidence_value,
                "role_match_evidence": role_evidence_value,
                "city_evidence_method": (
                    "approved_jurisdiction_mapping"
                    if mapping.get("status") == "PASS"
                    else "real_page_evidence"
                    if page_city_match
                    else "url_host_or_path_or_explicit_evidence"
                    if city_ok
                    else None
                ),
                "role_evidence_method": (
                    "real_page_evidence"
                    if page_role_match
                    else "role_evidence_field_or_role_host_hint"
                    if _role_match(row)
                    else None
                ),
                "jurisdiction_mapping_id": mapping.get("mapping_id"),
                "jurisdiction_evidence_id": mapping.get("evidence_id"),
                "source_bundle_id": row.get("source_bundle_id") or mapping.get("source_bundle_id"),
                "jurisdiction_mapping_status": mapping.get("status"),
                "jurisdiction_mapping_reason_code": mapping.get("reason_code"),
            }
        )
        rows.append(row)
    return pl.DataFrame(rows, infer_schema_length=None)


def rank_candidate_proposals(
    frame: pl.DataFrame,
    *,
    settings: Settings | None = None,
    mappings: Iterable[JurisdictionMapping] | None = None,
    max_candidates: int = 3,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Apply fixed deterministic scores, then use AI confidence only as a tie-break."""

    if frame.is_empty():
        return frame, frame
    mapping_list = list(mappings) if mappings is not None else (
        load_jurisdiction_mappings(settings) if settings is not None else []
    )
    aliases = _safe_aliases(settings) if settings is not None else {}
    rows: list[dict[str, Any]] = []
    for raw in frame.iter_rows(named=True):
        row = dict(raw)
        url = str(row.get("canonical_url") or row.get("candidate_url") or "")
        parsed = urlsplit(url)
        host = (parsed.hostname or "").lower()
        mapping = mapping_for_candidate(row, row, settings=settings, mappings=mapping_list) if (settings is not None or mapping_list) else {}
        city_ok, _ = _city_match(row, aliases, mapping) if settings is not None else (False, False)
        role_ok = _role_match(row)
        detail = is_clear_detail_url(url) or str(row.get("page_type") or "") in {"policy_content_page", "policy_detail", "content_page"}
        score = 0
        score_reasons: list[str] = []
        if _official(url):
            score += 100
            score_reasons.append("official_gov")
        if city_ok or str(row.get("city_match_evidence") or ""):
            score += 80
            score_reasons.append("exact_city")
        if role_ok:
            score += 80
            score_reasons.append("exact_role")
        if mapping.get("status") == "PASS":
            score += 60
            score_reasons.append("approved_central_mapping")
        if is_reusable_source_entry(url) or _list_shape_hint(url):
            score += 50
            score_reasons.append("homepage_or_list_shape")
        if row.get("source_bundle_id") or mapping.get("source_bundle_id"):
            score += 30
            score_reasons.append("same_org_bundle")
        if detail:
            score -= 100
            score_reasons.append("detail_page")
        reasons = set(row.get("prefilter_reason_codes") or [])
        if "city_evidence_missing" in reasons or "city_mismatch" in reasons:
            score -= 120
            score_reasons.append("city_mismatch")
        if "role_evidence_missing" in reasons or "source_role_mismatch" in reasons:
            score -= 120
            score_reasons.append("role_mismatch")
        if is_central_authority_host(host) and mapping.get("status") != "PASS":
            score -= 150
            score_reasons.append("central_wrong_assignment")
        row["deterministic_score"] = score
        row["deterministic_score_reasons"] = json.dumps(score_reasons, ensure_ascii=False)
        rows.append(row)
    rows.sort(
        key=lambda row: (
            0 if row.get("prefilter_status", "shortlist") == "shortlist" else 1,
            -int(row.get("deterministic_score") or 0),
            -float(row.get("ai_confidence") or 0),
            str(row.get("canonical_url") or row.get("candidate_url") or ""),
        )
    )
    selected = [row for row in rows if row.get("prefilter_status", "shortlist") == "shortlist"][:max_candidates]
    selected_ids = {str(row.get("proposal_id") or row.get("candidate_url")) for row in selected}
    evidence_rows: list[dict[str, Any]] = []
    for _index, row in enumerate(rows, start=1):
        key = str(row.get("proposal_id") or row.get("candidate_url"))
        row["selection_status"] = "selected_top3" if key in selected_ids else (
            "evidence_enrichment_probe"
            if row.get("prefilter_status") == "evidence_enrichment_probe"
            else "rejected_by_deterministic_prefilter"
            if row.get("prefilter_status", "shortlist") != "shortlist"
            else "search_evidence_only"
        )
        row["selection_rank"] = next((rank for rank, item in enumerate(selected, start=1) if str(item.get("proposal_id") or item.get("candidate_url")) == key), None)
        evidence_rows.append(row)
    selected_rows = [row for row in evidence_rows if row.get("selection_status") == "selected_top3"]
    selected_frame = pl.DataFrame(selected_rows, infer_schema_length=None) if selected_rows else frame.head(0)
    evidence_frame = pl.DataFrame(evidence_rows, infer_schema_length=None) if evidence_rows else frame.head(0)
    return selected_frame, evidence_frame


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
    result = prefilter_candidate_frame(settings, frame)
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
