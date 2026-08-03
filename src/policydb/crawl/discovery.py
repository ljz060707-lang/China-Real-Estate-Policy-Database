from __future__ import annotations

import json
import re
from datetime import UTC, date, datetime
from urllib.parse import quote_plus, urlencode, urljoin, urlsplit

import polars as pl
from bs4 import BeautifulSoup

from policydb.crawl.dedup import canonicalize_url
from policydb.crawl.models import (
    DiscoveryCandidate,
    DiscoveryRequest,
    RegisteredSource,
)
from policydb.transform.normalization import stable_id

_GAZETTE_ISSUE_REF_RE = re.compile(
    r"showUrl\(\s*(20[0-9]{2})\s*,\s*([0-9]+)\s*\)", re.I
)
_COMPACT_DATE_RE = re.compile(r"(?<!\d)(20\d{2})(\d{2})(\d{2})(?!\d)")

_DATE_RE = re.compile(r"(?<!\d)(20\d{2})[-/.年](\d{1,2})[-/.月](\d{1,2})日?")
_SKIP_SUFFIXES = (".jpg", ".jpeg", ".png", ".gif", ".css", ".js", ".zip", ".rar")
_SKIP_WORDS = ("登录", "注册", "首页", "javascript:", "mailto:", "#")


def _date_hint(text: str) -> date | None:
    match = _DATE_RE.search(text)
    if not match:
        return None
    try:
        return date(*map(int, match.groups()))
    except ValueError:
        return None


def _candidate_link(parent_url: str, href: str, label: str) -> str | None:
    if not href or any(word in href.lower() or word in label for word in _SKIP_WORDS):
        return None
    absolute = urljoin(parent_url, href)
    if urlsplit(absolute).netloc != urlsplit(parent_url).netloc:
        return None
    if urlsplit(absolute).path.lower().endswith(_SKIP_SUFFIXES):
        return None
    if canonicalize_url(absolute) == canonicalize_url(parent_url):
        return None
    return absolute


def _compact_url_date(text: str) -> date | None:
    """Extract an exact YYYYMMDD date when an archive URL exposes one."""
    match = _COMPACT_DATE_RE.search(text)
    if not match:
        return None
    try:
        return date(*map(int, match.groups()))
    except ValueError:
        return None


def _fetch_with_referer(fetcher, url: str, referer: str):
    """Keep production Referer evidence while supporting older test doubles."""
    try:
        return fetcher.fetch(url, referer=referer)
    except TypeError as exc:
        if "referer" not in str(exc).lower():
            raise
        return fetcher.fetch(url)


def _same_gazette_host(url: str, index_url: str) -> bool:
    """Accept the site's www/non-www pair, but reject cross-domain API results."""
    candidate_host = (urlsplit(url).hostname or "").lower().removeprefix("www.")
    index_host = (urlsplit(index_url).hostname or "").lower().removeprefix("www.")
    return bool(candidate_host and index_host and candidate_host == index_host)


def _gazette_issue_url(payload: object) -> str | None:
    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, dict):
            value = data.get("url") or data.get("href")
            if value:
                return str(value).strip()
        value = payload.get("url") or payload.get("href")
        return str(value).strip() if value else None
    if isinstance(payload, list) and payload:
        return _gazette_issue_url(payload[0])
    return None


def _discover_gazette_issue_candidates(
    *,
    fetcher,
    request: DiscoveryRequest,
    source: RegisteredSource,
    index_result,
    issue_refs: list[tuple[int, int]],
) -> tuple[list[DiscoveryCandidate], dict[str, object]]:
    """Resolve an official gazette's JavaScript issue index into crawl items.

    Beijing's history page uses ``showUrl(year, issue)`` instead of anchors. A
    generic anchor walker sees only breadcrumb links, which are not historical
    documents. The endpoint calls below are deterministic discovery evidence;
    the returned issue URLs still go through the ordinary HTTP, parser, and
    completion gates in ``CrawlPipeline``.
    """
    candidates: dict[str, DiscoveryCandidate] = {}
    eligible_refs = [
        (year, issue)
        for year, issue in issue_refs
        if not request.start_date or year >= request.start_date.year
        if not request.end_date or year <= request.end_date.year
    ]
    truncated = len(eligible_refs) > request.max_candidates
    errors: list[dict[str, object]] = []
    endpoint_count = 0
    base_url = str(index_result.final_url or "").rstrip("/") + "/"
    for year, issue in eligible_refs[: request.max_candidates]:
        endpoint = urljoin(
            base_url,
            "findUrl?" + urlencode({"gbnf": str(year), "gbqs": str(issue)}),
        )
        endpoint_count += 1
        try:
            endpoint_result = _fetch_with_referer(fetcher, endpoint, str(index_result.final_url))
            if endpoint_result.status_code != 200 or not endpoint_result.body:
                raise ValueError(f"endpoint_http_{endpoint_result.status_code}")
            payload = json.loads(endpoint_result.body.decode("utf-8", errors="replace"))
            issue_url = _gazette_issue_url(payload)
            if (
                not issue_url
                or urlsplit(issue_url).scheme not in {"http", "https"}
                or not _same_gazette_host(issue_url, str(index_result.final_url))
            ):
                raise ValueError("invalid_same_domain_issue_url")
            canonical = canonicalize_url(issue_url)
            hint = _compact_url_date(issue_url)
            if hint and request.start_date and hint < request.start_date:
                continue
            if hint and request.end_date and hint > request.end_date:
                continue
            candidates.setdefault(
                canonical,
                DiscoveryCandidate(
                    candidate_id=stable_id(
                        request.run_id, source.source_id, canonical, prefix="CAND"
                    ),
                    run_id=request.run_id,
                    discovery_mode=request.mode,
                    source_id=source.source_id,
                    url=issue_url,
                    canonical_url=canonical,
                    parent_url=str(index_result.final_url),
                    title_hint=f"government gazette {year} issue {issue}",
                    date_hint=hint,
                    source_role=source.source_role,
                    discovered_at=datetime.now(UTC),
                    discovery_score=0.95 if hint else 0.9,
                ),
            )
        except Exception as exc:
            errors.append(
                {
                    "year": year,
                    "issue": issue,
                    "endpoint_url": endpoint,
                    "error_type": type(exc).__name__,
                }
            )
    complete = not truncated and not errors
    return list(candidates.values()), {
        "special_strategy": "gazette_issue_index",
        "issue_ref_count": len(eligible_refs),
        "endpoint_count": endpoint_count,
        "resolved_issue_count": len(candidates),
        "issue_resolution_error_count": len(errors),
        "issue_resolution_errors": errors[:20],
        "complete": complete,
    }


class ListPageDiscovery:
    """Discover detail pages from ordinary government HTML lists with bounded pagination."""

    def __init__(self, fetcher) -> None:
        self.fetcher = fetcher
        self.last_scan: dict[str, object] = {}

    def discover(
        self,
        request: DiscoveryRequest,
        source: RegisteredSource,
        *,
        resume_from: dict[str, object] | None = None,
    ) -> list[DiscoveryCandidate]:
        resume_from = dict(resume_from or {})
        resume_next = str(resume_from.get("next_page") or "").strip()
        queue = [resume_next] if resume_next else list(dict.fromkeys(source.list_page_urls))
        visited = {
            canonicalize_url(str(value))
            for value in (resume_from.get("visited_urls") or [])
            if value
        }
        candidates: dict[str, DiscoveryCandidate] = {}
        pages = 0
        consecutive_old_pages = 0
        stop_reason = "pagination_exhausted"
        last_page: str | None = None
        last_seen_date: date | None = None
        special_mode = False
        special_metadata: dict[str, object] = {}
        while queue and pages < request.max_pages and len(candidates) < request.max_candidates:
            page_url = queue.pop(0)
            canonical_page = canonicalize_url(page_url)
            if canonical_page in visited:
                continue
            visited.add(canonical_page)
            last_page = page_url
            pages += 1
            result = self.fetcher.fetch(page_url)
            raw_body = result.body.decode("utf-8", errors="replace")
            issue_refs = list(dict.fromkeys(_GAZETTE_ISSUE_REF_RE.findall(raw_body)))
            if source.source_role == "government_gazette" and issue_refs:
                special_mode = True
                parsed_refs = [(int(year), int(issue)) for year, issue in issue_refs]
                issue_candidates, special_metadata = _discover_gazette_issue_candidates(
                    fetcher=self.fetcher,
                    request=request,
                    source=source,
                    index_result=result,
                    issue_refs=parsed_refs,
                )
                for item in issue_candidates:
                    candidates.setdefault(item.canonical_url, item)
                    if item.date_hint:
                        last_seen_date = max(last_seen_date or item.date_hint, item.date_hint)
                queue.clear()
                stop_reason = (
                    "pagination_exhausted"
                    if special_metadata.get("complete")
                    else "gazette_issue_resolution_incomplete"
                )
                break
            soup = BeautifulSoup(result.body, "html.parser")
            page_dates: list[date] = []
            for anchor in soup.find_all("a", href=True):
                label = anchor.get_text(" ", strip=True)
                absolute = _candidate_link(result.final_url, anchor.get("href", ""), label)
                if not absolute:
                    continue
                context = " ".join(
                    [label, anchor.parent.get_text(" ", strip=True) if anchor.parent else ""]
                )
                hint = _date_hint(context)
                if hint:
                    page_dates.append(hint)
                    last_seen_date = max(last_seen_date or hint, hint)
                is_next = bool(
                    anchor.get("rel") == ["next"]
                    or re.search(r"下一页|下页|next|后页", label, re.I)
                )
                if is_next:
                    if canonicalize_url(absolute) not in visited:
                        queue.append(absolute)
                    continue
                if hint and request.start_date and hint < request.start_date:
                    continue
                if hint and request.end_date and hint > request.end_date:
                    continue
                canonical = canonicalize_url(absolute)
                candidates.setdefault(
                    canonical,
                    DiscoveryCandidate(
                        candidate_id=stable_id(
                            request.run_id, source.source_id, canonical, prefix="CAND"
                        ),
                        run_id=request.run_id,
                        discovery_mode=request.mode,
                        source_id=source.source_id,
                        url=absolute,
                        canonical_url=canonical,
                        parent_url=result.final_url,
                        title_hint=label or None,
                        date_hint=hint,
                        source_role=source.source_role,
                        discovered_at=datetime.now(UTC),
                        discovery_score=0.75 if hint else 0.6,
                    ),
                )
                if len(candidates) >= request.max_candidates:
                    stop_reason = "candidate_limit"
                    break
            if (
                request.start_date
                and page_dates
                and max(page_dates) < request.start_date
            ):
                consecutive_old_pages += 1
            else:
                consecutive_old_pages = 0
            if consecutive_old_pages >= 2:
                queue.clear()
                stop_reason = "stable_before_start_date"
                break
        if not special_mode:
            if pages >= request.max_pages and queue:
                stop_reason = "page_limit"
            elif len(candidates) >= request.max_candidates:
                stop_reason = "candidate_limit"
        cursor = queue[0] if queue else None
        if special_mode:
            cursor = json.dumps(
                {
                    "strategy": special_metadata.get("special_strategy"),
                    "issue_ref_count": special_metadata.get("issue_ref_count", 0),
                    "endpoint_count": special_metadata.get("endpoint_count", 0),
                    "resolved_issue_count": special_metadata.get("resolved_issue_count", 0),
                    "issue_resolution_error_count": special_metadata.get(
                        "issue_resolution_error_count", 0
                    ),
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        self.last_scan = {
            "pages_scanned": pages,
            "pagination_exhausted": (
                not queue
                and stop_reason
                in {"pagination_exhausted", "stable_before_start_date"}
            ),
            "stop_reason": stop_reason,
            "candidate_count": len(candidates),
            "max_pages": request.max_pages,
            "max_candidates": request.max_candidates,
            "last_page": last_page,
            "next_page": queue[0] if queue else None,
            "last_seen_date": last_seen_date.isoformat() if last_seen_date else None,
            "cursor": cursor,
            "visited_urls": sorted(visited),
            "resumed_from": resume_next or None,
            **special_metadata,
        }
        return list(candidates.values())


class OfficialRegistryDiscovery:
    def discover(self, request: DiscoveryRequest, source: RegisteredSource) -> list[DiscoveryCandidate]:
        now = datetime.now(UTC)
        return [
            DiscoveryCandidate(
                candidate_id=stable_id(request.run_id, source.source_id, canonicalize_url(url), prefix="CAND"),
                run_id=request.run_id,
                discovery_mode=request.mode,
                source_id=source.source_id,
                url=url,
                canonical_url=canonicalize_url(url),
                source_role=source.source_role,
                discovered_at=now,
                discovery_score=0.8 if source.official_status == "official" else 0.5,
            )
            for url in source.seed_urls[: request.max_candidates]
        ]


class SeedBacktrackDiscovery(OfficialRegistryDiscovery):
    pass


class SiteSearchDiscovery(OfficialRegistryDiscovery):
    pass


class WebSearchDiscovery(OfficialRegistryDiscovery):
    pass


class MissingSourceRecoveryDiscovery(OfficialRegistryDiscovery):
    pass


def discover_seed_items(
    source: RegisteredSource,
    run_id: str,
    *,
    city_id: str | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
) -> list[dict]:
    now = datetime.now(UTC)
    urls = dict.fromkeys(source.list_page_urls + source.seed_urls)

    def in_requested_window(url: str) -> bool:
        years = [int(value) for value in re.findall(r"20\d{2}", url)]
        if not years:
            return True
        return not (
            (start_date and max(years) < start_date.year)
            or (end_date and min(years) > end_date.year)
        )

    return [
        {
            "item_id": stable_id(source.source_id, canonicalize_url(url), prefix="CRAWLITEM"),
            "run_id": run_id,
            "source_id": source.source_id,
            "url": url,
            "canonical_url": canonicalize_url(url),
            "status": "pending",
            "city_id": city_id,
            "query_year": None,
            "keyword_group": None,
            "retry_count": 0,
            "first_seen_at": now.isoformat(),
            "last_seen_at": now.isoformat(),
            "created_at": now.isoformat(),
            "updated_at": now.isoformat(),
        }
        for url in urls
        if in_requested_window(url)
    ]


def discover_search_items(
    source: RegisteredSource,
    run_id: str,
    cities: pl.DataFrame,
    years: range,
    keyword_groups: dict[str, list[str]],
) -> list[dict]:
    if not source.search_url_template:
        return []
    now = datetime.now(UTC)
    rows = []
    for city in cities.iter_rows(named=True):
        for year in years:
            for group, terms in keyword_groups.items():
                keyword = " ".join(terms[:6])
                url = source.search_url_template.format(
                    city=quote_plus(city["city_name"]),
                    city_id=city["city_id"],
                    year=year,
                    keyword=quote_plus(keyword),
                    keyword_group=group,
                )
                canonical = canonicalize_url(url)
                rows.append(
                    {
                        "item_id": stable_id(source.source_id, canonical, prefix="CRAWLITEM"),
                        "run_id": run_id,
                        "source_id": source.source_id,
                        "url": url,
                        "canonical_url": canonical,
                        "status": "pending",
                        "city_id": city["city_id"],
                        "query_year": year,
                        "keyword_group": group,
                        "retry_count": 0,
                        "first_seen_at": now.isoformat(),
                        "last_seen_at": now.isoformat(),
                        "created_at": now.isoformat(),
                        "updated_at": now.isoformat(),
                    }
                )
    return rows
