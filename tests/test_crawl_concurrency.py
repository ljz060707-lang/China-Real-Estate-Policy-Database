from __future__ import annotations

import threading
import time
from collections import Counter
from datetime import UTC, date, datetime
from pathlib import Path
from urllib.parse import urlsplit

import polars as pl
import pytest
import yaml

from policydb.crawl.fetcher import FetchTask, HostAwareFetchPool
from policydb.crawl.models import FetchResult
from policydb.crawl.pipeline import CrawlPipeline
from policydb.settings import Settings


class _TrackingFetcher:
    def __init__(self, *, failing_url: str | None = None) -> None:
        self.failing_url = failing_url
        self.lock = threading.Lock()
        self.active = 0
        self.active_by_host: Counter[str] = Counter()
        self.max_active = 0
        self.max_active_by_host: Counter[str] = Counter()

    def fetch(
        self,
        url: str,
        *,
        etag: str | None = None,
        last_modified: str | None = None,
    ) -> FetchResult:
        del etag, last_modified
        host = urlsplit(url).netloc
        with self.lock:
            self.active += 1
            self.active_by_host[host] += 1
            self.max_active = max(self.max_active, self.active)
            self.max_active_by_host[host] = max(
                self.max_active_by_host[host], self.active_by_host[host]
            )
        try:
            time.sleep(0.08)
            if url == self.failing_url:
                raise RuntimeError("bounded fetch failure")
            now = datetime.now(UTC)
            return FetchResult(
                requested_url=url,
                final_url=url,
                status_code=200,
                content_type="text/html",
                body=b"<html><title>policy</title><body>content</body></html>",
                response_sha256="sha",
                retrieved_at=now,
                request_started_at=now,
                request_finished_at=now,
                response_bytes=55,
            )
        finally:
            with self.lock:
                self.active -= 1
                self.active_by_host[host] -= 1


def _tasks() -> list[FetchTask]:
    return [
        FetchTask(key="a1", url="https://a.gov.cn/1"),
        FetchTask(key="b1", url="https://b.gov.cn/1"),
        FetchTask(key="a2", url="https://a.gov.cn/2"),
        FetchTask(key="b2", url="https://b.gov.cn/2"),
    ]


def test_host_aware_pool_parallelizes_hosts_but_serializes_each_host() -> None:
    fetcher = _TrackingFetcher()
    started = time.perf_counter()
    with HostAwareFetchPool(
        fetcher,
        _tasks(),
        max_workers=4,
        per_host_concurrency=1,
    ) as pool:
        results = [pool.get(task.key) for task in _tasks()]
    elapsed = time.perf_counter() - started

    assert [result.requested_url for result in results] == [task.url for task in _tasks()]
    assert fetcher.max_active >= 2
    assert max(fetcher.max_active_by_host.values()) == 1
    assert elapsed < 0.28


def test_host_aware_pool_failure_does_not_cancel_other_hosts() -> None:
    failing_url = "https://a.gov.cn/1"
    fetcher = _TrackingFetcher(failing_url=failing_url)
    tasks = _tasks()
    with HostAwareFetchPool(
        fetcher,
        tasks,
        max_workers=4,
        per_host_concurrency=1,
    ) as pool:
        with pytest.raises(RuntimeError, match="bounded fetch failure"):
            pool.get("a1")
        assert pool.get("b1").status_code == 200
        assert pool.get("a2").status_code == 200
        assert pool.get("b2").status_code == 200

    metrics = pool.metrics()
    assert metrics["attempted"] == 4
    assert metrics["succeeded"] == 3
    assert metrics["failed"] == 1
    assert metrics["global_concurrency"] == 4
    assert metrics["per_host_concurrency"] == 1
    assert metrics["timeout_rate"] == 0.0
    assert metrics["http_429_rate"] == 0.0
    assert metrics["http_5xx_rate"] == 0.0
    assert metrics["cache_reuse_rate"] == 0.0


def _write_registry(root: Path) -> None:
    reference = root / "data" / "reference"
    (root / "data" / "curated").mkdir(parents=True)
    reference.mkdir(parents=True)
    sources = []
    for ordinal, host in enumerate(("a.gov.cn", "b.gov.cn", "c.gov.cn", "d.gov.cn")):
        sources.append(
            {
                "source_id": f"SRC_{ordinal}",
                "source_name": f"Source {ordinal}",
                "domain": host,
                "source_type": "government",
                "source_role": "canonical_candidate",
                "official_status": "official",
                "seed_urls": [f"https://{host}/policy"],
                "crawl_enabled": True,
                "priority": ordinal,
                "rate_limit": 0,
            }
        )
    (reference / "source_registry.yaml").write_text(
        yaml.safe_dump({"sources": sources}),
        encoding="utf-8",
    )


def test_pipeline_uses_bounded_fetch_lane_and_keeps_single_commit(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    _write_registry(root)
    fetcher = _TrackingFetcher()
    pipeline = CrawlPipeline(Settings(root=root), fetcher=fetcher)
    plan = pipeline.plan(
        run_type="concurrency_contract",
        start_date=date(2018, 1, 1),
        end_date=date(2018, 12, 31),
    )

    result = pipeline.run(
        plan["run_id"],
        fetch_concurrency=4,
        per_host_concurrency=1,
    )

    versions = pl.read_parquet(
        root / "data" / "curated" / "policy_document_versions.parquet"
    )
    assert result["fetched"] == 4
    assert result["failed"] == 0
    assert result["throughput"]["global_concurrency"] == 4
    assert result["throughput"]["parser_concurrency"] == 1
    assert result["throughput"]["documents_per_minute"] > 0
    assert result["throughput"]["writer_batch_rows"] >= 4
    assert result["throughput"]["db_rows_per_second"] > 0
    assert fetcher.max_active >= 2
    assert versions.height == 4
    assert versions["document_version_id"].n_unique() == 4


def test_pipeline_audits_batch_duplicate_as_not_created_twice(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    _write_registry(root)
    fetcher = _TrackingFetcher()
    pipeline = CrawlPipeline(Settings(root=root), fetcher=fetcher)
    now = date(2018, 1, 1)
    planned = pipeline.plan_external_candidates(
        "RUN_DUPLICATE_BATCH",
        [
            {
                "queue_item_id": "Q1",
                "episode_id": "EP_TEST",
                "source_id": "SRC_0",
                "city_id": "CITY_0",
                "canonical_url": "https://a.gov.cn/policy",
                "url": "https://a.gov.cn/policy",
                "query_type": "policy",
                "published_at": "2018-01-01",
            },
            {
                "queue_item_id": "Q2",
                "episode_id": "EP_TEST",
                "source_id": "SRC_0",
                "city_id": "CITY_0",
                "canonical_url": "https://a.gov.cn/policy",
                "url": "https://a.gov.cn/policy",
                "query_type": "policy",
                "published_at": "2018-01-01",
            },
        ],
        start_date=now,
        end_date=date(2018, 12, 31),
    )
    audit_path = root / "data" / "curated" / "http_audit.parquet"

    pipeline.run(planned["run_id"], audit_path=audit_path)

    audit = pl.read_parquet(audit_path).sort("crawl_item_id")
    assert audit.height == 2
    assert audit["document_version_created"].to_list() == [True, False]


def test_pipeline_reuses_valid_document_version_without_network_fetch(tmp_path: Path) -> None:
    """Historical backfill should reuse a valid local version by explicit opt-in."""
    root = tmp_path / "repo"
    _write_registry(root)
    fetch_calls: list[str] = []

    class _CountingFetcher(_TrackingFetcher):
        def fetch(self, url: str, *, etag=None, last_modified=None):
            fetch_calls.append(url)
            return super().fetch(url, etag=etag, last_modified=last_modified)

    fetcher = _CountingFetcher()
    pipeline = CrawlPipeline(Settings(root=root), fetcher=fetcher)
    first = pipeline.plan_external_candidates(
        "RUN_REUSE_FIRST",
        [
            {
                "queue_item_id": "Q1",
                "episode_id": "EP_TEST",
                "source_id": "SRC_0",
                "city_id": "CITY_0",
                "canonical_url": "https://a.gov.cn/reuse",
                "url": "https://a.gov.cn/reuse",
                "query_type": "policy",
                "published_at": "2018-01-01",
            }
        ],
        start_date=date(2018, 1, 1),
        end_date=date(2018, 12, 31),
    )
    pipeline.run(first["run_id"])

    second = pipeline.plan_external_candidates(
        "RUN_REUSE_SECOND",
        [
            {
                "queue_item_id": "Q2",
                "episode_id": "EP_TEST",
                "source_id": "SRC_0",
                "city_id": "CITY_0",
                "canonical_url": "https://a.gov.cn/reuse",
                "url": "https://a.gov.cn/reuse",
                "query_type": "policy",
                "published_at": "2018-01-01",
            }
        ],
        start_date=date(2018, 1, 1),
        end_date=date(2018, 12, 31),
    )
    audit_path = root / "data" / "curated" / "http_audit.parquet"
    pipeline.run(
        second["run_id"],
        audit_path=audit_path,
        reuse_existing_documents=True,
    )

    assert len(fetch_calls) == 1
    reused = pl.read_parquet(audit_path).filter(
        pl.col("crawl_run_id") == second["run_id"]
    )
    assert reused.height == 1
    assert reused[0, "fetch_status"] == "CACHE_REUSE"
    assert reused[0, "real_network_fetch"] is False
    assert reused[0, "cache_hit"] is True
    assert reused[0, "document_version_id"]
