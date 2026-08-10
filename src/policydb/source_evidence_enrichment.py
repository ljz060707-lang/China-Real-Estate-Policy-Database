"""Bounded deterministic page-evidence enrichment for source candidates.

Search results are discovery evidence, not admission evidence.  This module
performs one bounded fetch for a small set of official, weakly related
candidates so that the existing deterministic prefilter can reconsider them
with real page evidence.  It never writes the candidate registry and never
sets verification or enablement fields.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections import defaultdict
from typing import Any
from urllib.parse import urljoin, urlsplit, urlunsplit

import polars as pl
from bs4 import BeautifulSoup

from policydb.crawl.dedup import canonicalize_url
from policydb.crawl.fetcher import RespectfulFetcher
from policydb.source_discovery import ROLE_SYNONYMS, is_reusable_source_entry
from policydb.source_jurisdiction import is_clear_detail_url


def _official(url: str) -> bool:
    host = (urlsplit(url).hostname or "").lower().removeprefix("www.")
    return host == "gov.cn" or host.endswith(".gov.cn")


_FILE_SUFFIXES = (".pdf", ".doc", ".docx", ".xls", ".xlsx", ".zip", ".rar")
_ENTRY_TERMS = (
    "policy",
    "notice",
    "announcement",
    "regulation",
    "information",
    "disclosure",
    "gazette",
    "bulletin",
    "policy",
    "政策",
    "通知",
    "公告",
    "法规",
    "政务公开",
    "公报",
    "目录",
    "文件",
)


_PAGE_EVIDENCE_FIELDS = (
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
    "page_redirect_chain_json",
    "page_same_domain_links_json",
    "page_same_domain_link_count",
    "page_content_type",
    "page_final_url",
    "page_http_status",
    "page_network_route",
    "page_response_sha256",
)

_ENRICHMENT_STATE_FIELDS = (
    "evidence_enrichment_status",
    "evidence_enrichment_attempts",
    "evidence_enrichment_error",
    "evidence_enrichment_run_id",
    "evidence_enrichment_url",
    "enrichment_probe_hash",
)


def _detach_parent_page_evidence(row: dict[str, Any], *, parent_url: str) -> dict[str, Any]:
    """Keep parent provenance without treating it as child-page evidence."""

    detached = dict(row)
    for field in _PAGE_EVIDENCE_FIELDS:
        value = detached.get(field)
        if value not in (None, "", [], {}):
            detached[f"parent_{field}"] = value
        detached[field] = None
    for field in _ENRICHMENT_STATE_FIELDS:
        detached[field] = None
    detached["parent_page_url"] = parent_url
    return detached


def _source_bundle_id(row: dict[str, Any]) -> str:
    host = (urlsplit(str(row.get("page_final_url") or row.get("candidate_url") or "")).hostname or "").lower()
    role = str(row.get("source_role") or "")
    return str(row.get("source_bundle_id") or f"BUNDLE_{hashlib.sha256(f'{host}|{role}'.encode()).hexdigest()[:16].upper()}")


def _same_domain_link_score(link: dict[str, str], *, role: str) -> tuple[int, list[str]]:
    url = canonicalize_url(str(link.get("url") or ""))
    label = str(link.get("label") or "")
    parsed = urlsplit(url)
    text = f"{label} {parsed.path} {parsed.query}".lower()
    reasons: list[str] = []
    if not _official(url) or parsed.scheme not in {"http", "https"}:
        return -1, ["non_official_or_invalid"]
    if parsed.path.lower().endswith(_FILE_SUFFIXES) or not is_reusable_source_entry(url):
        return -1, ["file_or_detail_page"]
    if any(marker in text for marker in ("search", "query=", "keyword=", "login", "register")):
        return -1, ["search_or_account_page"]
    score = 10
    if any(term.lower() in text for term in _ENTRY_TERMS):
        score += 30
        reasons.append("policy_entry_term")
    role_terms = ROLE_SYNONYMS.get(role, (role,))
    if any(term and term.lower() in text for term in role_terms):
        score += 40
        reasons.append("role_term")
    if any(token in parsed.path.lower() for token in ("/zwgk", "/gkml", "/zcfg", "/zcfgk", "/gongbao", "/gb/", "/info", "/bulletin", "/notice")):
        score += 25
        reasons.append("list_or_disclosure_path")
    if re.search(r"(?:index|list|column|channel|bulletin|notice)[_-]?[0-9]*\.(?:s?html?|jhtml|aspx?)$", parsed.path, re.I):
        score += 15
        reasons.append("list_filename")
    return score, reasons


def _derived_same_domain_rows(row: dict[str, Any], *, max_rows: int) -> list[dict[str, Any]]:
    """Turn real same-domain links into bounded evidence proposals.

    The links are not treated as verified sources.  They are derived from an
    already fetched page and must pass the normal prefilter and two-probe
    admission gates later.
    """

    try:
        links = json.loads(str(row.get("page_same_domain_links_json") or "[]"))
    except (TypeError, json.JSONDecodeError):
        links = []
    if not isinstance(links, list):
        return []
    ranked: list[tuple[int, str, list[str], dict[str, str]]] = []
    seen: set[str] = set()
    for raw in links:
        if not isinstance(raw, dict):
            continue
        target = canonicalize_url(str(raw.get("url") or ""))
        if not target or target in seen:
            continue
        seen.add(target)
        score, reasons = _same_domain_link_score(raw, role=str(row.get("source_role") or ""))
        if score >= 35:
            ranked.append((score, target, reasons, {"url": target, "label": str(raw.get("label") or "")}))
    ranked.sort(key=lambda item: (-item[0], item[1]))
    parent = canonicalize_url(str(row.get("canonical_url") or row.get("candidate_url") or ""))
    result: list[dict[str, Any]] = []
    for score, target, reasons, link in ranked[:max_rows]:
        label = _clean(link.get("label"), 500) or str(row.get("page_title") or "") or "same-domain official entry"
        detached = _detach_parent_page_evidence(row, parent_url=parent)
        result.append(
            {
                **detached,
                "proposal_id": f"{row.get('proposal_id')}_LINK_{hashlib.sha256(target.encode()).hexdigest()[:12]}",
                "candidate_url": target,
                "canonical_url": target,
                "candidate_title": label,
                "candidate_snippet": f"same-domain link from {parent}; {';'.join(reasons)}",
                "discovery_method": "page_enrichment_same_domain",
                "discovery_provider": "page_enrichment",
                "discovery_evidence_text": f"parent={parent}; link_label={label}; reasons={','.join(reasons)}",
                "evidence_source": "page_enrichment",
                "source_bundle_id": _source_bundle_id(row),
                "candidate_kind": "page_enrichment_entry_candidate",
                "entry_eligible_guess": False,
                "selection_status": "page_enrichment_evidence",
                "selection_rank": None,
                "same_domain_parent_url": parent,
                "same_domain_link_score": score,
                "same_domain_link_reasons": reasons,
            }
        )
    return result


def _detail_parent_urls(url: str) -> list[str]:
    """Generate bounded, structural parent-entry hypotheses from a detail URL.

    These are hypotheses only.  Each URL must be fetched and pass the normal
    deterministic prefilter before it can become a formal candidate.
    """

    parsed = urlsplit(canonicalize_url(url))
    parts = [part for part in parsed.path.split("/") if part]
    if len(parts) < 2:
        return []
    lowered = [part.lower() for part in parts]
    prefixes: list[list[str]] = []
    for index, part in enumerate(lowered):
        if part in {"art", "article", "content", "detail", "news", "show", "view"} and index > 0:
            prefixes.append(parts[:index])
    prefixes.append(parts[:-1])
    results: list[str] = []
    seen: set[str] = set()
    for prefix in prefixes:
        if not prefix:
            continue
        for suffix in ("index.html", ""):
            path = "/" + "/".join(prefix)
            path += f"/{suffix}" if suffix else "/"
            candidate = canonicalize_url(urlunsplit((parsed.scheme, parsed.netloc, path, "", "")))
            if candidate and candidate != canonicalize_url(url) and candidate not in seen:
                seen.add(candidate)
                results.append(candidate)
    return results


def _derived_detail_parent_rows(frame: pl.DataFrame, *, max_per_slot: int) -> list[dict[str, Any]]:
    """Create evidence-only parent hypotheses for official detail results."""

    if frame.is_empty() or max_per_slot <= 0:
        return []
    existing = {
        canonicalize_url(str(row.get("canonical_url") or row.get("candidate_url") or ""))
        for row in frame.iter_rows(named=True)
    }
    counts: defaultdict[str, int] = defaultdict(int)
    result: list[dict[str, Any]] = []
    for raw in frame.iter_rows(named=True):
        row = dict(raw)
        url = canonicalize_url(str(row.get("canonical_url") or row.get("candidate_url") or ""))
        if not _official(url) or not is_clear_detail_url(url):
            continue
        slot_id = str(row.get("slot_id") or "")
        for rank, parent_url in enumerate(_detail_parent_urls(url), start=1):
            if counts[slot_id] >= max_per_slot or parent_url in existing:
                continue
            existing.add(parent_url)
            counts[slot_id] += 1
            parent_id = hashlib.sha256(f"{row.get('proposal_id')}|{parent_url}".encode()).hexdigest()[:12]
            result.append(
                {
                    **row,
                    "proposal_id": f"{row.get('proposal_id')}_PARENT_{parent_id}",
                    "candidate_url": parent_url,
                    "canonical_url": parent_url,
                    "candidate_title": "derived parent entry hypothesis",
                    "candidate_snippet": f"parent entry hypothesis derived from official detail URL {url}",
                    "discovery_method": "detail_parent_path_hypothesis",
                    "discovery_provider": "page_structure",
                    "discovery_evidence_text": f"detail_url={url}; parent_rank={rank}; structural_parent_path",
                    "evidence_source": "detail_parent_hypothesis",
                    "candidate_kind": "detail_parent_entry_hypothesis",
                    "entry_eligible_guess": False,
                    "selection_status": "page_enrichment_evidence",
                    "selection_rank": None,
                    "prefilter_status": "evidence_enrichment_probe",
                    "prefilter_reasons": json.dumps(["derived_from_detail_page"], ensure_ascii=False),
                    "prefilter_reason_codes": ["derived_from_detail_page"],
                    "detail_parent_of": url,
                    "detail_parent_rank": rank,
                    "evidence_enrichment_status": None,
                }
            )
    return result


def _decode_body(body: bytes) -> str:
    for encoding in ("utf-8", "gb18030", "big5"):
        try:
            return body.decode(encoding)
        except UnicodeDecodeError:
            continue
    return body.decode("utf-8", errors="replace")


def _clean(value: object, limit: int = 4000) -> str | None:
    text = " ".join(str(value or "").split()).strip()
    return text[:limit] or None


def _page_fields(result: Any) -> dict[str, Any]:
    html = _decode_body(bytes(result.body or b""))
    soup = BeautifulSoup(html, "html.parser")
    title = _clean(soup.title.get_text(" ", strip=True) if soup.title else None, 500)
    heading = _clean(
        " ".join(
            node.get_text(" ", strip=True)
            for node in soup.select("h1, h2, .title, .article-title, [class*='title']")[:4]
        ),
        800,
    )
    breadcrumb = _clean(
        " ".join(
            node.get_text(" ", strip=True)
            for node in soup.select(
                ".breadcrumb, .crumb, .location, [class*='breadcrumb'], [class*='crumb'], [class*='location']"
            )[:3]
        ),
        800,
    )
    page_text = _clean(soup.get_text(" ", strip=True), 4000)
    final_url = canonicalize_url(str(result.final_url or result.requested_url))
    origin = (urlsplit(final_url).hostname or "").lower()
    same_domain_links: list[dict[str, str]] = []
    for anchor in soup.find_all("a", href=True):
        target = canonicalize_url(urljoin(final_url, str(anchor.get("href") or "")))
        if not target or (urlsplit(target).hostname or "").lower() != origin:
            continue
        if target == final_url:
            continue
        label = _clean(anchor.get_text(" ", strip=True), 240)
        if label or target:
            same_domain_links.append({"url": target, "label": label or ""})
    same_domain_links = list(
        {item["url"]: item for item in same_domain_links}.values()
    )[:80]
    institution_evidence = _clean(" | ".join(value for value in (title, heading, breadcrumb) if value), 1800)
    redirect_chain = getattr(result, "redirect_chain", []) or []
    return {
        "page_title": title,
        "page_heading": heading,
        "breadcrumb": breadcrumb,
        "page_text_excerpt": page_text,
        "institution_evidence": institution_evidence,
        # These are extracted page evidence, not adjudications.  Keeping the
        # aliases at the enrichment boundary lets prefilter, candidate
        # registry, human review and gate audit refer to the same evidence.
        "page_city_evidence": breadcrumb,
        "page_role_evidence": institution_evidence,
        "page_agency_evidence": institution_evidence,
        "page_entry_type_evidence": f"same_domain_link_count={len(same_domain_links)}" if same_domain_links else None,
        "page_pagination_evidence": None,
        "page_redirect_chain_json": json.dumps(redirect_chain, ensure_ascii=False, default=str),
        "page_same_domain_links_json": json.dumps(same_domain_links, ensure_ascii=False),
        "page_same_domain_link_count": len(same_domain_links),
        "page_content_type": str(result.content_type or "") or None,
        "page_final_url": final_url,
        "page_http_status": int(result.status_code or 0),
        "page_network_route": str(result.network_route or "unknown"),
        "page_response_sha256": str(result.response_sha256 or "") or None,
    }


def select_evidence_enrichment_candidates(
    frame: pl.DataFrame,
    *,
    max_per_slot: int = 3,
) -> pl.DataFrame:
    """Select only the bounded B-class candidates eligible for page evidence."""

    if frame.is_empty() or "prefilter_status" not in frame.columns:
        return frame.head(0)
    rows = [
        dict(row)
        for row in frame.iter_rows(named=True)
        if row.get("prefilter_status") == "evidence_enrichment_probe"
        and _official(str(row.get("canonical_url") or row.get("candidate_url") or ""))
    ]
    rows.sort(
        key=lambda row: (
            str(row.get("slot_id") or ""),
            -int(row.get("deterministic_score") or 0),
            str(row.get("canonical_url") or row.get("candidate_url") or ""),
        )
    )
    selected: list[dict[str, Any]] = []
    by_slot: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_slot[str(row.get("slot_id") or "")].append(row)
    for slot_id in sorted(by_slot):
        slot_rows = by_slot[slot_id]
        ordinary = [
            row for row in slot_rows if row.get("discovery_method") != "detail_parent_path_hypothesis"
        ]
        parent_hypotheses = [
            row for row in slot_rows if row.get("discovery_method") == "detail_parent_path_hypothesis"
        ]
        # Reserve at most one enrichment slot for a structural parent.  A
        # failed parent hypothesis must not starve an original official
        # candidate that may already be a usable entry.
        ordinary_limit = max_per_slot - 1 if parent_hypotheses else max_per_slot
        chosen = ordinary[:ordinary_limit]
        if parent_hypotheses:
            chosen.append(parent_hypotheses[0])
        selected.extend(chosen[:max_per_slot])
    return pl.DataFrame(selected, infer_schema_length=None) if selected else frame.head(0)


def enrich_candidate_evidence(
    frame: pl.DataFrame,
    *,
    fetcher: RespectfulFetcher,
    max_per_slot: int = 3,
    run_id: str | None = None,
) -> tuple[pl.DataFrame, list[dict[str, Any]]]:
    """Fetch bounded page evidence and return an enriched evidence frame.

    The returned rows remain proposals.  A later call to
    ``prefilter_candidate_frame`` must still promote them to ``shortlist``
    before the normal Top-3 candidate upsert and strict two-probe gates.
    """

    if frame.is_empty():
        return frame, []
    parent_rows = _derived_detail_parent_rows(frame, max_per_slot=max_per_slot)
    working = frame
    if parent_rows:
        working = pl.concat(
            [frame, pl.DataFrame(parent_rows, infer_schema_length=None)],
            how="diagonal_relaxed",
        )
    selected = select_evidence_enrichment_candidates(working, max_per_slot=max_per_slot)
    selected_urls = {
        str(row.get("canonical_url") or row.get("candidate_url") or "")
        for row in selected.iter_rows(named=True)
    }
    rows: list[dict[str, Any]] = []
    evidence: list[dict[str, Any]] = []
    attempted: set[str] = set()
    derived_per_slot: defaultdict[str, int] = defaultdict(int)
    for raw in working.iter_rows(named=True):
        row = dict(raw)
        url = canonicalize_url(str(row.get("canonical_url") or row.get("candidate_url") or ""))
        if url not in selected_urls or url in attempted:
            rows.append(row)
            continue
        attempted.add(url)
        row.update(
            {
                "evidence_enrichment_status": "started",
                "evidence_enrichment_attempts": int(row.get("evidence_enrichment_attempts") or 0) + 1,
                "evidence_enrichment_run_id": run_id,
                "evidence_enrichment_url": url,
            }
        )
        try:
            result = fetcher.fetch(url)
            parsed = _page_fields(result)
            page_hash = str(parsed.get("page_response_sha256") or "")
            enrichment_hash = hashlib.sha256(
                json.dumps(
                    {
                        "url": url,
                        "final_url": parsed.get("page_final_url"),
                        "response_sha256": page_hash,
                        "status": parsed.get("page_http_status"),
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ).encode("utf-8")
            ).hexdigest()
            row.update(
                {
                    **parsed,
                    "evidence_enrichment_status": "completed",
                    "evidence_enrichment_error": None,
                    "enrichment_probe_hash": enrichment_hash,
                }
            )
            evidence.append(
                {
                    "slot_id": row.get("slot_id"),
                    "proposal_id": row.get("proposal_id"),
                    "candidate_url": url,
                    "status": "completed",
                    "run_id": run_id,
                    "requested_url": url,
                    "final_url": parsed.get("page_final_url"),
                    "http_status": parsed.get("page_http_status"),
                    "network_route": parsed.get("page_network_route"),
                    "response_sha256": page_hash,
                    "enrichment_probe_hash": enrichment_hash,
                    "page_title": parsed.get("page_title"),
                    "page_same_domain_link_count": parsed.get("page_same_domain_link_count"),
                }
            )
            slot_id = str(row.get("slot_id") or "")
            derived = _derived_same_domain_rows(
                row,
                max_rows=max(0, max_per_slot * 2 - derived_per_slot[slot_id]),
            )
            rows.extend(derived)
            derived_per_slot[slot_id] += len(derived)
            for item in derived:
                evidence.append(
                    {
                        "slot_id": item.get("slot_id"),
                        "proposal_id": item.get("proposal_id"),
                        "candidate_url": item.get("candidate_url"),
                        "status": "derived_same_domain_link",
                        "run_id": run_id,
                        "parent_url": url,
                        "final_url": item.get("candidate_url"),
                        "evidence_source": "page_enrichment",
                        "same_domain_link_score": item.get("same_domain_link_score"),
                    }
                )
        except Exception as exc:
            row.update(
                {
                    "evidence_enrichment_status": "failed",
                    "evidence_enrichment_error": type(exc).__name__,
                }
            )
            evidence.append(
                {
                    "slot_id": row.get("slot_id"),
                    "proposal_id": row.get("proposal_id"),
                    "candidate_url": url,
                    "status": "failed",
                    "run_id": run_id,
                    "error_type": type(exc).__name__,
                }
            )
        rows.append(row)
    return pl.DataFrame(rows, infer_schema_length=None), evidence
