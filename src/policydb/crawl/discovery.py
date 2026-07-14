from __future__ import annotations

from datetime import UTC, datetime
from urllib.parse import quote_plus

import polars as pl

from policydb.crawl.dedup import canonicalize_url
from policydb.crawl.models import RegisteredSource
from policydb.transform.normalization import stable_id


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
