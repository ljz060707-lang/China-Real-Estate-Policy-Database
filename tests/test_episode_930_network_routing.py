from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import polars as pl

import policydb.crawl.service as crawl_service
import policydb.episode_930_production as production
from policydb.config.providers import SearchResult
from policydb.crawl.dedup import content_sha256
from policydb.crawl.models import FetchResult
from policydb.crawl.pipeline import CrawlPipeline
from policydb.crawl.service import CrawlService
from policydb.episode_930 import Episode930Pipeline, EpisodeConfig
from policydb.jobs.models import CrawlJobRequest
from policydb.parquet_store import read_parquet_snapshot
from policydb.settings import Settings


def _settings(tmp_path: Path) -> Settings:
    data_root = tmp_path / "data"
    (data_root / "curated").mkdir(parents=True)
    (data_root / "outputs").mkdir(parents=True)
    return Settings(
        root=tmp_path,
        data_root_path=data_root,
        curated_path=data_root / "curated",
        outputs_path=data_root / "outputs",
    )


class _Fetcher:
    rate_limit = 0.0

    def fetch(self, url: str, **_: object) -> FetchResult:
        body = b"<html><title>930 policy</title><body>policy text</body></html>"
        started = datetime.now(UTC)
        return FetchResult(
            requested_url=url,
            final_url=url,
            status_code=200,
            content_type="text/html",
            body=body,
            response_sha256=content_sha256(body),
            retrieved_at=datetime.now(UTC),
            request_started_at=started,
            request_finished_at=datetime.now(UTC),
            response_bytes=len(body),
            network_source="LIVE_HTTP",
        )


class _Provider:
    name = "test-search"

    def search(self, query: str, **_: object) -> list[SearchResult]:
        return [
            SearchResult(
                url="https://example.gov.cn/930-policy",
                title=query,
                snippet="official result",
            )
        ]


def test_external_candidate_uses_normal_pipeline_and_persists_live_http_audit(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    pipeline = CrawlPipeline(settings, fetcher=_Fetcher())
    planned = pipeline.plan_external_candidates(
        "CRAWLRUN_930_TEST",
        [
            {
                "queue_item_id": "Q1",
                "episode_id": "EP_2016_930_TIGHTENING",
                "city_id": "CITY_A",
                "query_type": "market",
                "url": "https://example.gov.cn/930-policy",
                "canonical_url": "https://example.gov.cn/930-policy",
                "source_id": "SRC_1",
            }
        ],
        start_date=date(2016, 9, 25),
        end_date=date(2016, 10, 10),
    )
    item_id = planned["links"][0]["crawl_item_id"]
    result = pipeline.run(
        "CRAWLRUN_930_TEST",
        audit_path=tmp_path / "930_QUEUE_HTTP_AUDIT.parquet",
        audit_context={
            item_id: {
                "episode_id": "EP_2016_930_TIGHTENING",
                "queue_item_id": "Q1",
                "city_id": "CITY_A",
            }
        },
    )
    assert result["fetched"] == 1
    audit = read_parquet_snapshot(tmp_path / "930_QUEUE_HTTP_AUDIT.parquet")
    row = audit.row(0, named=True)
    assert row["queue_item_id"] == "Q1"
    assert row["network_source"] == "LIVE_HTTP"
    assert row["real_network_fetch"] is True
    assert row["http_status"] == 200
    assert row["response_bytes"] > 0
    assert row["content_sha256"]
    assert row["document_version_id"]
    assert row["request_started_at"]
    assert row["request_finished_at"]


def test_historical_episode_930_passes_queue_query_to_search_and_fetch(
    tmp_path: Path, monkeypatch
) -> None:
    settings = _settings(tmp_path)
    output = settings.outputs / "special_projects" / "2016_930"
    output.mkdir(parents=True)
    queue_path = output / "930_TASK_QUEUE.parquet"
    pl.DataFrame(
        [
            {
                "queue_item_id": "Q1",
                "episode_id": "EP_2016_930_TIGHTENING",
                "city_id": "CITY_A",
                "query_type": "market",
                "query_text": "City A policy 2016",
                "window_start": date(2016, 9, 25),
                "window_end": date(2016, 10, 10),
                "status": "RUNNING",
            }
        ]
    ).write_parquet(queue_path)
    monkeypatch.setattr(crawl_service, "build_search_fallback", lambda _settings: _Provider())
    service = CrawlService(settings, pipeline=CrawlPipeline(settings, fetcher=_Fetcher()))
    request = CrawlJobRequest(
        mode="historical_episode_930",
        episode_id="EP_2016_930_TIGHTENING",
        episode_run_id="EP930_TEST_RUN",
        episode_queue_path=str(queue_path),
        episode_output_path=str(output),
        episode_queue_item_ids=["Q1"],
        start_date=date(2016, 9, 1),
        end_date=date(2016, 10, 31),
        max_fetches=5,
        max_attachment_attempts=0,
        rebuild_database=False,
        run_validation=False,
    )
    result = service.execute(request)
    assert result["metrics"]["search_calls"] == 1
    assert result["metrics"]["search_results"] == 1
    assert result["metrics"]["real_network_fetches"] == 1
    assert result["metrics"]["successful_http_200"] == 1
    search = read_parquet_snapshot(output / "930_QUEUE_SEARCH_EXECUTION.parquet")
    assert search.filter(pl.col("queue_item_id") == "Q1").height == 1
    audit = read_parquet_snapshot(output / "930_QUEUE_HTTP_AUDIT.parquet")
    assert audit.filter(pl.col("queue_item_id") == "Q1").height == 1


def test_queue_update_does_not_call_task_completion_a_network_success(
    tmp_path: Path, monkeypatch
) -> None:
    settings = _settings(tmp_path)
    monkeypatch.setattr(production, "load_cities_105", lambda _settings: pl.DataFrame([{"city_id": "CITY_A", "city_name": "A", "city_name_short": "A", "province_name": "P", "province_code": "P", "aliases": ""}]))
    monkeypatch.setattr(production, "load_registry", lambda _settings: [])
    controller = production.Episode930ProductionController(settings, run_id="EP930_QUEUE_TEST", city_limit=1, max_ai_calls=0)
    controller.build_plan()
    queue = read_parquet_snapshot(controller.queue_path)
    queue_id = str(queue[0, "queue_item_id"])
    controller._update_queue(
        cities=["CITY_A"],
        queue_item_ids=[queue_id],
        status="CRAWL_COMPLETED",
        metrics={},
    )
    updated = read_parquet_snapshot(controller.queue_path).filter(pl.col("queue_item_id") == queue_id).row(0, named=True)
    assert updated["status"] == "RETRY_WAIT"
    assert updated["execution_status"] == "TASK_FAILED"
    assert updated["fetch_status"] == "NOT_ATTEMPTED"
    assert updated["real_network_fetch"] is False


def test_empty_action_parameter_batch_keeps_keyed_schemas(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    pipeline = Episode930Pipeline(
        settings,
        config=EpisodeConfig(run_search=False, run_ai=False, apply=True),
        output=tmp_path / "episode-output",
    )
    documents = pl.DataFrame(
        schema={
            "is_formal_eligible": pl.Boolean,
            "official_text": pl.String,
            "document_id": pl.String,
        }
    )
    actions, params, _ = pipeline.extract_actions(documents)
    assert actions.height == 0
    assert params.height == 0
    assert {"action_id"}.issubset(actions.columns)
    assert {"episode_id", "action_id", "parameter_name"}.issubset(params.columns)
