from __future__ import annotations

import re
from datetime import UTC, date, datetime
from urllib.parse import quote_plus, urljoin, urlsplit

import polars as pl
from bs4 import BeautifulSoup

from policydb.crawl.dedup import canonicalize_url
from policydb.crawl.models import (
    DiscoveryCandidate,
    DiscoveryRequest,
    RegisteredSource,
)
from policydb.transform.normalization import stable_id

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


class ListPageDiscovery:
    """Discover detail pages from ordinary government HTML lists with bounded pagination."""

    def __init__(self, fetcher) -> None:
        self.fetcher = fetcher

    def discover(
        self, request: DiscoveryRequest, source: RegisteredSource
    ) -> list[DiscoveryCandidate]:
        queue = list(dict.fromkeys(source.list_page_urls))
        visited: set[str] = set()
        candidates: dict[str, DiscoveryCandidate] = {}
        pages = 0
        while queue and pages < request.max_pages and len(candidates) < request.max_candidates:
            page_url = queue.pop(0)
            canonical_page = canonicalize_url(page_url)
            if canonical_page in visited:
                continue
            visited.add(canonical_page)
            pages += 1
            result = self.fetcher.fetch(page_url)
            soup = BeautifulSoup(result.body, "html.parser")
            for anchor in soup.find_all("a", href=True):
                label = anchor.get_text(" ", strip=True)
                absolute = _candidate_link(result.final_url, anchor.get("href", ""), label)
                if not absolute:
                    continue
                context = " ".join(
                    [label, anchor.parent.get_text(" ", strip=True) if anchor.parent else ""]
                )
                hint = _date_hint(context)
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
                    break
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


def discover_seed_items(source: RegisteredSource, run_id: str) -> list[dict]:
    now = datetime.now(UTC)
    urls = dict.fromkeys(source.list_page_urls + source.seed_urls)
    return [
        {
            "item_id": stable_id(source.source_id, canonicalize_url(url), prefix="CRAWLITEM"),
            "run_id": run_id,
            "source_id": source.source_id,
            "url": url,
            "canonical_url": canonicalize_url(url),
            "status": "pending",
            "city_id": None,
            "query_year": None,
            "keyword_group": None,
            "retry_count": 0,
            "first_seen_at": now.isoformat(),
            "last_seen_at": now.isoformat(),
            "created_at": now.isoformat(),
            "updated_at": now.isoformat(),
        }
        for url in urls
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
