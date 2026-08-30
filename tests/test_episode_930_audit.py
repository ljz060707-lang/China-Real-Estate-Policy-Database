from __future__ import annotations

import json
from pathlib import Path

import polars as pl

from policydb.parquet_store import atomic_write_parquet
from scripts.audit_episode_930_blockers import (
    build_completed_provenance,
    build_false_completion_eligibility,
    build_false_completion_recovery,
    build_postprocess_recovery,
    preserve_recovery_runtime,
)


def _write_run(output: Path, run_id: str, status: str) -> None:
    run = output / "production_runs" / run_id
    run.mkdir(parents=True)
    (run / "STATE.json").write_text(
        json.dumps({"stage": "API_CLASSIFICATION"}), encoding="utf-8"
    )
    (run / "CHECKPOINT.json").write_text(
        json.dumps({"status": status, "stage": "930_API_CLASSIFY_PASS1"}),
        encoding="utf-8",
    )


def test_postprocess_audit_distinguishes_active_stale_and_planned_runs(tmp_path: Path) -> None:
    output = tmp_path / "outputs" / "special_projects" / "2016_930"
    output.mkdir(parents=True)
    _write_run(output, "EP930RUN_ACTIVE", "RUNNING")
    _write_run(output, "EP930RUN_STALE", "RUNNING")
    _write_run(output, "EP930RUN_PLANNED", "PLANNED")
    (output / "930_PROGRESS_SNAPSHOT.json").write_text(
        json.dumps({"run_id": "EP930RUN_ACTIVE"}), encoding="utf-8"
    )

    recovery = build_postprocess_recovery(output)

    assert recovery.get_column("run_id").to_list() == ["EP930RUN_STALE"]


def test_completed_provenance_classifies_reuse_and_only_false_completion_recovers(tmp_path: Path) -> None:
    output = tmp_path / "outputs" / "special_projects" / "2016_930"
    output.mkdir(parents=True)
    queue_rows = [
        {"queue_item_id": "Q1", "status": "CRAWL_COMPLETED", "city": "A", "query_text": "live", "search_executed": True, "search_call_count": 1, "fetch_status": "LIVE_FETCH_SUCCESS", "http_request_count": 1, "real_network_fetch": True, "cache_hit": False},
        {"queue_item_id": "Q2", "status": "CRAWL_COMPLETED", "city": "B", "query_text": "no-url", "search_executed": True, "search_call_count": 1, "fetch_status": "NOT_ATTEMPTED", "http_request_count": 0, "real_network_fetch": False, "cache_hit": False},
        {"queue_item_id": "Q3", "status": "CRAWL_COMPLETED", "city": "C", "query_text": "not-fetched", "search_executed": True, "search_call_count": 1, "fetch_status": "NOT_ATTEMPTED", "http_request_count": 0, "real_network_fetch": False, "cache_hit": False},
        {"queue_item_id": "Q4", "status": "CRAWL_COMPLETED", "city": "D", "query_text": "missing", "search_executed": False, "search_call_count": 0, "fetch_status": "NOT_ATTEMPTED", "http_request_count": 0, "real_network_fetch": False, "cache_hit": False},
        {"queue_item_id": "Q5", "status": "CRAWL_COMPLETED", "city": "E", "query_text": "cache", "search_executed": False, "search_call_count": 0, "fetch_status": "NOT_ATTEMPTED", "http_request_count": 0, "real_network_fetch": False, "cache_hit": True},
        {"queue_item_id": "Q6", "status": "CRAWL_COMPLETED", "city": "F", "query_text": "local", "search_executed": False, "search_call_count": 0, "fetch_status": "LOCAL_DB_REUSE", "http_request_count": 0, "real_network_fetch": False, "cache_hit": False},
        {"queue_item_id": "Q7", "status": "CRAWL_COMPLETED", "city": "G", "query_text": "unknown", "search_executed": False, "search_call_count": 0, "fetch_status": "LIVE_FETCH_SUCCESS", "http_request_count": 1, "real_network_fetch": True, "cache_hit": False},
    ]
    atomic_write_parquet(pl.DataFrame(queue_rows), output / "930_TASK_QUEUE.parquet", {"test": "provenance"}, key_columns=("queue_item_id",))
    atomic_write_parquet(pl.DataFrame([
        {"queue_item_id": "Q1", "result_url": "https://example.gov.cn/a"},
        {"queue_item_id": "Q2", "result_url": None},
        {"queue_item_id": "Q3", "result_url": "https://example.gov.cn/c"},
    ]), output / "930_QUEUE_SEARCH_EXECUTION.parquet", {"test": "provenance"}, key_columns=())
    atomic_write_parquet(pl.DataFrame([
        {"queue_item_id": "Q1", "http_status": 200, "real_network_fetch": True, "cache_hit": False, "document_version_id": "DV1", "content_sha256": "H1", "response_bytes": 10},
    ]), output / "930_QUEUE_HTTP_AUDIT.parquet", {"test": "provenance"}, key_columns=())

    audit = build_completed_provenance(output)
    classes = dict(zip(audit.get_column("queue_item_id").to_list(), audit.get_column("provenance_class").to_list(), strict=True))
    recovery = audit.filter(pl.col("needs_recovery"))

    assert classes == {
        "Q1": "LIVE_SEARCH_AND_FETCH",
        "Q2": "LIVE_SEARCH_NO_NEW_URL",
        "Q3": "FETCH_NOT_EXECUTED",
        "Q4": "SEARCH_NOT_EXECUTED",
        "Q5": "CACHE_ONLY",
        "Q6": "LOCAL_DB_REUSE",
        "Q7": "UNKNOWN_PROVENANCE",
    }
    assert recovery.get_column("queue_item_id").to_list() == ["Q4"]


def test_false_completion_eligibility_only_marks_in_scope_rows_for_recovery(tmp_path: Path) -> None:
    output = tmp_path / "outputs" / "special_projects" / "2016_930"
    output.mkdir(parents=True)
    queue = pl.DataFrame([
        {"queue_item_id": "Q1", "episode_id": "EP_2016_930_TIGHTENING", "city_id": "C1", "city": "A", "task_stage": "930_DISCOVERY", "query_type": "market", "query_text": "A 房地产市场调控", "window_start": "2016-09-25", "window_end": "2016-10-10", "status": "CRAWL_COMPLETED", "search_executed": False, "fetch_status": "NOT_ATTEMPTED", "result_status": "NO_RESULT", "document_version_id": None},
        {"queue_item_id": "Q2", "episode_id": "EP_2016_930_TIGHTENING", "city_id": "C2", "city": "B", "task_stage": "930_DISCOVERY", "query_type": "market", "query_text": "B 房地产市场调控", "window_start": "2016-09-25", "window_end": "2016-10-10", "status": "CRAWL_COMPLETED", "search_executed": False, "fetch_status": "NOT_ATTEMPTED", "result_status": "DUPLICATE", "document_version_id": None},
        {"queue_item_id": "Q3", "episode_id": "EP_2016_930_TIGHTENING", "city_id": "C3", "city": "C", "task_stage": "OTHER", "query_type": "market", "query_text": "C 房地产市场调控", "window_start": "2015-01-01", "window_end": "2015-02-01", "status": "CRAWL_COMPLETED", "search_executed": False, "fetch_status": "NOT_ATTEMPTED", "result_status": "NO_RESULT", "document_version_id": None},
    ])
    atomic_write_parquet(queue, output / "930_TASK_QUEUE.parquet", {"test": "eligibility"}, key_columns=("queue_item_id",))
    atomic_write_parquet(
        pl.DataFrame([{"queue_item_id": "Q1", "city": "A", "query": "A 房地产市场调控", "provenance_class": "SEARCH_NOT_EXECUTED", "needs_recovery": True}, {"queue_item_id": "Q2", "city": "B", "query": "B 房地产市场调控", "provenance_class": "SEARCH_NOT_EXECUTED", "needs_recovery": True}, {"queue_item_id": "Q3", "city": "C", "query": "C 房地产市场调控", "provenance_class": "SEARCH_NOT_EXECUTED", "needs_recovery": True}]),
        output / "930_COMPLETED_PROVENANCE_AUDIT.parquet",
        {"test": "eligibility"},
        key_columns=("queue_item_id",),
    )

    provenance = pl.read_parquet(output / "930_COMPLETED_PROVENANCE_AUDIT.parquet")
    eligibility = build_false_completion_eligibility(output, provenance)
    recovery = build_false_completion_recovery(eligibility)
    statuses = dict(zip(eligibility.get_column("queue_item_id"), eligibility.get_column("eligibility_status"), strict=True))

    assert statuses == {"Q1": "RECOVERY_REQUIRED", "Q2": "DUPLICATE_OR_SUPERSEDED", "Q3": "OUT_OF_SCOPE"}
    assert recovery.get_column("queue_item_id").to_list() == ["Q1"]
    assert recovery.get_column("status").to_list() == ["RECOVERY_REQUIRED"]


def test_false_completion_audit_preserves_runtime_completion() -> None:
    fresh = pl.DataFrame([
        {
            "recovery_id": "R1",
            "episode_id": "EP_2016_930_TIGHTENING",
            "queue_item_id": "Q1",
            "city": "C",
            "query": "q",
            "provenance_class": "SEARCH_NOT_EXECUTED",
            "status": "RECOVERY_REQUIRED",
            "reason_code": "SEARCH_AND_FETCH_EVIDENCE_MISSING",
            "recoverable_without_refetch": False,
            "requires_search": True,
            "requires_fetch": True,
            "created_at": "t0",
            "updated_at": "t0",
        }
    ])
    existing = fresh.with_columns(
        pl.lit("RECOVERY_COMPLETED").alias("status"),
        pl.lit("DV1").alias("document_version_id"),
        pl.lit("t1").alias("completed_at"),
    )

    merged = preserve_recovery_runtime(fresh, existing)

    assert merged.item(0, "status") == "RECOVERY_COMPLETED"
    assert merged.item(0, "document_version_id") == "DV1"
    assert merged.item(0, "completed_at") == "t1"
