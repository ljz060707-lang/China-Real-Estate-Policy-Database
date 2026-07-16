from __future__ import annotations

from datetime import UTC, datetime

import polars as pl
from bs4 import BeautifulSoup

from policydb.crawl.fetcher import RespectfulFetcher
from policydb.crawl.models import RegisteredSource
from policydb.crawl.registry import load_registry, save_registry_atomic, set_sources_enabled
from policydb.settings import Settings


def evaluate_source(source: RegisteredSource, fetcher: RespectfulFetcher) -> dict:
    official = source.official_status in {"official", "official_reprint"}
    entry = (source.list_page_urls or source.seed_urls or [None])[0]
    accessible = body_ok = detail_links = False
    response_ms = None
    error_type = None
    started = datetime.now(UTC)
    if entry:
        try:
            result = fetcher.fetch(entry)
            response_ms = (datetime.now(UTC) - started).total_seconds() * 1000
            accessible = result.status_code == 200
            body_ok = bool(result.body)
            if "html" in (result.content_type or "").lower():
                soup = BeautifulSoup(result.body, "html.parser")
                detail_links = any(anchor.get_text(" ", strip=True) for anchor in soup.find_all("a", href=True))
            else:
                detail_links = body_ok
        except Exception as exc:
            error_type = type(exc).__name__
    score = (
        (25 if official else 0)
        + (25 if accessible else 0)
        + (20 if detail_links else 0)
        + (20 if body_ok else 0)
        + (10 if source.list_page_urls else 0)
    )
    recommended = official and accessible and body_ok and detail_links and score >= 80
    return {
        "source_id": source.source_id,
        "source_name": source.source_name,
        "official_status": source.official_status,
        "crawl_enabled": source.crawl_enabled,
        "entry_url": entry,
        "entry_accessible": accessible,
        "detail_link_detected": detail_links,
        "body_parse_success": body_ok,
        "average_response_ms": response_ms,
        "error_type": error_type,
        "source_health_score": float(score),
        "recommended_enabled": recommended,
        "evaluated_at": datetime.now(UTC).isoformat(),
    }


def evaluate_sources(
    settings: Settings | None = None,
    *,
    limit: int | None = None,
    fetcher: RespectfulFetcher | None = None,
) -> dict:
    settings = settings or Settings.discover()
    fetcher = fetcher or RespectfulFetcher(
        user_agent=settings.user_agent,
        timeout=settings.request_timeout,
        connect_timeout=settings.connect_timeout,
        retries=settings.max_retries,
        rate_limit=settings.default_rate_limit,
        check_robots=settings.respect_robots,
    )
    sources = load_registry(settings)
    if limit:
        sources = sources[:limit]
    rows = [evaluate_source(source, fetcher) for source in sources]
    path = settings.curated / "source_health.parquet"
    if rows:
        path.parent.mkdir(parents=True, exist_ok=True)
        pl.DataFrame(rows).write_parquet(path, compression="zstd")
    index = {row["source_id"]: row for row in rows}
    updated = []
    for source in load_registry(settings):
        row = index.get(source.source_id)
        if row:
            source = source.model_copy(
                update={
                    "source_health_score": row["source_health_score"],
                    "recommended_enabled": row["recommended_enabled"],
                    "last_health_at": row["evaluated_at"],
                    "last_error": row["error_type"],
                }
            )
        updated.append(source)
    if rows and not settings.read_only:
        save_registry_atomic(updated, settings, action=f"health_evaluation={len(rows)}")
    return {
        "evaluated": len(rows),
        "recommended": sum(bool(row["recommended_enabled"]) for row in rows),
        "unhealthy": sum(float(row["source_health_score"]) < 50 for row in rows),
        "path": str(path),
    }


def enable_recommended(settings: Settings | None = None, *, limit: int = 20) -> dict:
    settings = settings or Settings.discover()
    candidates = [
        source.source_id
        for source in load_registry(settings)
        if source.recommended_enabled and not source.crawl_enabled
    ][:limit]
    result = set_sources_enabled(candidates, True, settings) if candidates else {"changed": 0}
    return {**result, "source_ids": candidates}


def disable_unhealthy(settings: Settings | None = None) -> dict:
    settings = settings or Settings.discover()
    candidates = [
        source.source_id
        for source in load_registry(settings)
        if source.crawl_enabled and (source.source_health_score or 0) < 50
    ]
    result = set_sources_enabled(candidates, False, settings) if candidates else {"changed": 0}
    return {**result, "source_ids": candidates}
