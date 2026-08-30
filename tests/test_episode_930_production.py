from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import polars as pl

import policydb.episode_930_production as production
from policydb.ai_audit import AIAuditStore
from policydb.episode_930 import (
    ActionClassification,
    ActionClassificationPayload,
    Episode930Pipeline,
    EpisodeConfig,
    _parse_effective_evidence,
)
from policydb.episode_930_production import (
    CERTIFICATION_STAGE_REQUIREMENTS,
    QUEUE_SCHEMA,
    Episode930ProductionController,
    api_fast_lane_document_priorities,
    api_recovery_transition,
    build_core_document_lineage,
    certification_batch_transition,
    certification_gate_from_ledger,
    core_action_coverage_metrics,
    select_api_fast_lane_inputs,
)
from policydb.jobs.models import CrawlJobRequest, JobState
from policydb.parquet_store import atomic_write_parquet, read_parquet_snapshot
from policydb.settings import Settings


def _settings(tmp_path: Path) -> Settings:
    data_root = tmp_path / "data"
    curated = data_root / "curated"
    outputs = data_root / "outputs"
    curated.mkdir(parents=True)
    outputs.mkdir(parents=True)
    return Settings(
        root=tmp_path,
        data_root_path=data_root,
        curated_path=curated,
        outputs_path=outputs,
    )


def _cities() -> pl.DataFrame:
    return pl.DataFrame(
        [
            {
                "city_id": "CITY_A",
                "city_name": "城市甲",
                "city_name_short": "甲",
                "province_name": "省甲",
                "province_code": "PA",
                "aliases": "",
            },
            {
                "city_id": "CITY_B",
                "city_name": "城市乙",
                "city_name_short": "乙",
                "province_name": "省乙",
                "province_code": "PB",
                "aliases": "",
            },
        ]
    )


def test_core_action_coverage_requires_explicit_lineage_for_namespaced_ids() -> None:
    coverage = pl.DataFrame(
        [
            {
                "document_id": "DOC930PROD_1",
                "city": "南京市",
                "eligible": True,
                "status": "COMPLETED",
            },
            {
                "document_id": "DOC930PROD_2",
                "city": "南京市",
                "eligible": True,
                "status": "COMPLETED",
            }
        ]
    )
    scope = {
        "core_document_ids": {"DOC930_1"},
        "core_city_ids": {"CITY_320100"},
        "core_city_names": {"南京市"},
    }

    assert core_action_coverage_metrics(
        coverage,
        scope,
        document_lineage={"DOC930PROD_1": "DOC930_1"},
    ) == (1, 1)


def test_core_document_lineage_uses_exact_identity_not_city_membership() -> None:
    scope = {
        "core_documents": pl.DataFrame(
            [
                {
                    "document_id": "DOC930_1",
                    "city_id": "CITY_320100",
                    "city": "南京市",
                    "canonical_url": "https://example.gov.cn/policy/1",
                    "content_hash": "hash-1",
                }
            ]
        )
    }
    production_documents = pl.DataFrame(
        [
            {
                "document_id": "DOC930PROD_1",
                "city_id": "CITY_320100",
                "city": "南京市",
                "canonical_url": "https://example.gov.cn/policy/1",
                "content_hash": "different-hash",
            },
            {
                "document_id": "DOC930PROD_2",
                "city_id": "CITY_320100",
                "city": "南京市",
                "canonical_url": "https://example.gov.cn/policy/2",
                "content_hash": "different-hash-2",
            },
            {
                "document_id": "DOC930PROD_3",
                "city_id": "CITY_330100",
                "city": "杭州市",
                "canonical_url": "https://example.gov.cn/policy/1",
                "content_hash": "different-hash-3",
            },
        ]
    )

    assert build_core_document_lineage(scope, production_documents) == {
        "DOC930PROD_1": "DOC930_1"
    }


def test_empty_core_action_coverage_does_not_receive_document_credit() -> None:
    scope = {"core_document_ids": {"DOC930_1", "DOC930_2"}}

    assert core_action_coverage_metrics(pl.DataFrame(), scope) == (0, 0)


def test_episode_plan_is_idempotent_and_preserves_queue_state(
    tmp_path: Path, monkeypatch
) -> None:
    settings = _settings(tmp_path)
    monkeypatch.setattr(production, "load_cities_105", lambda _settings: _cities())
    monkeypatch.setattr(production, "load_registry", lambda _settings: [])
    controller = Episode930ProductionController(
        settings, run_id="EP930_TEST_PLAN", city_limit=2, max_ai_calls=0
    )

    first = controller.build_plan()
    assert first["search_plan_rows"] == 30
    assert first["queue_total"] == 30

    queue = read_parquet_snapshot(controller.queue_path)
    rows = queue.to_dicts()
    rows[0].update(
        {
            "status": "COMPLETED",
            "attempt_count": 4,
            "completed_at": "2026-08-14T00:00:00+00:00",
            "failure_reason": None,
        }
    )
    atomic_write_parquet(
        pl.DataFrame(rows, schema=QUEUE_SCHEMA),
        controller.queue_path,
        {"test": "idempotence"},
        key_columns=("queue_item_id",),
    )

    second = controller.build_plan()
    assert second["queue_total"] == 30
    restored = read_parquet_snapshot(controller.queue_path)
    preserved = restored.filter(pl.col("queue_item_id") == rows[0]["queue_item_id"]).row(
        0, named=True
    )
    assert preserved["status"] == "COMPLETED"
    assert preserved["attempt_count"] == 4


def test_episode_queue_reclaims_expired_running_lease(
    tmp_path: Path, monkeypatch
) -> None:
    settings = _settings(tmp_path)
    monkeypatch.setattr(production, "load_cities_105", lambda _settings: _cities())
    monkeypatch.setattr(production, "load_registry", lambda _settings: [])
    controller = Episode930ProductionController(
        settings, run_id="EP930_TEST_LEASE", city_limit=2, max_ai_calls=0
    )
    controller.build_plan()
    queue = read_parquet_snapshot(controller.queue_path)
    rows = queue.to_dicts()
    expired = (datetime.now(UTC) - timedelta(hours=2)).isoformat()
    rows[0].update(
        {
            "status": "RUNNING",
            "lease_owner": "JOB_OLD",
            "lease_acquired_at": expired,
            "lease_expires_at": expired,
        }
    )
    atomic_write_parquet(
        pl.DataFrame(rows, schema=QUEUE_SCHEMA),
        controller.queue_path,
        {"test": "expired-lease"},
        key_columns=("queue_item_id",),
    )

    claim = controller._claim_queue(["CITY_A", "CITY_B"], job_id="JOB_NEW")
    assert claim["claimed"] == 2
    updated = read_parquet_snapshot(controller.queue_path)
    claimed = updated.filter(pl.col("queue_item_id").is_in(claim["queue_item_ids"]))
    assert claimed.height == 2
    assert set(claimed.get_column("status").to_list()) == {"RUNNING"}
    assert set(claimed.get_column("lease_owner").to_list()) == {"JOB_NEW"}
    assert all(isinstance(value, str) for value in claimed.get_column("lease_expires_at").to_list())


def test_job_state_accepts_auditable_textual_episode_counter() -> None:
    state = JobState(
        job_id="JOB_TEST",
        mode="historical_episode_930",
        counters={"episode_id": "EP_2016_930_TIGHTENING", "selected_cities": 5},
    )
    assert state.counters["episode_id"] == "EP_2016_930_TIGHTENING"


def test_same_job_can_reclaim_its_own_unexpired_lease(
    tmp_path: Path, monkeypatch
) -> None:
    settings = _settings(tmp_path)
    monkeypatch.setattr(production, "load_cities_105", lambda _settings: _cities())
    monkeypatch.setattr(production, "load_registry", lambda _settings: [])
    controller = Episode930ProductionController(
        settings, run_id="EP930_TEST_OWN_LEASE", city_limit=2, max_ai_calls=0
    )
    controller.build_plan()
    first = controller._claim_queue(["CITY_A"], job_id="JOB_RESUME")
    assert first["claimed"] == 1
    second = controller._claim_queue(["CITY_A"], job_id="JOB_RESUME")
    assert second["claimed"] == 1
    assert second["queue_item_ids"] == first["queue_item_ids"]


def test_false_completion_claim_does_not_rewrite_raw_completed_status(
    tmp_path: Path, monkeypatch
) -> None:
    settings = _settings(tmp_path)
    monkeypatch.setattr(production, "load_cities_105", lambda _settings: _cities())
    monkeypatch.setattr(production, "load_registry", lambda _settings: [])
    controller = Episode930ProductionController(
        settings, run_id="EP930_TEST_HISTORY_SAFE", city_limit=1, max_ai_calls=0
    )
    controller.build_plan()
    queue = read_parquet_snapshot(controller.queue_path)
    target = queue.filter(pl.col("city_id") == "CITY_A").row(0, named=True)
    target_id = str(target["queue_item_id"])
    queue = queue.with_columns(
        pl.when(pl.col("queue_item_id") == target_id)
        .then(pl.lit("COMPLETED"))
        .otherwise(pl.col("status"))
        .alias("status")
    )
    atomic_write_parquet(
        queue,
        controller.queue_path,
        {"test": "historical-terminal"},
        key_columns=("queue_item_id",),
    )
    recovery = pl.DataFrame(
        [
            {
                "recovery_id": "R1",
                "queue_item_id": target_id,
                "status": "RECOVERY_REQUIRED",
                "updated_at": "t0",
            }
        ]
    )
    atomic_write_parquet(
        recovery,
        controller.false_recovery_path,
        {"test": "recovery-overlay"},
        key_columns=("recovery_id",),
    )
    raw_before_claim = controller.queue_path.read_bytes()

    claim = controller._claim_queue(["CITY_A"], job_id="JOB_RECOVERY")

    assert claim["queue_item_ids"] == [target_id]
    raw_after = read_parquet_snapshot(controller.queue_path)
    assert raw_after.filter(pl.col("queue_item_id") == target_id).item(0, "status") == "COMPLETED"
    assert controller.queue_path.read_bytes() == raw_before_claim
    recovery_after = read_parquet_snapshot(controller.false_recovery_path)
    assert recovery_after.item(0, "status") == "RUNNING"
    assert recovery_after.item(0, "lease_owner") == "JOB_RECOVERY"
    claim_audit = read_parquet_snapshot(controller.recovery_claim_audit_path)
    assert claim_audit.item(0, "task_id") == target_id
    assert claim_audit.item(0, "normalized_priority") == 2
    assert claim_audit.item(0, "work_source") == "FINAL_RECOVERY"
    assert claim_audit.item(0, "worker_generation") == "POST_PRIORITY_HOTFIX_WORKER"


def test_completed_raw_queue_runs_cached_convergence_without_starting_crawler(
    tmp_path: Path, monkeypatch
) -> None:
    settings = _settings(tmp_path)
    controller = Episode930ProductionController(
        settings, run_id="EP930_TEST_CACHED_CONVERGENCE", city_limit=1, max_ai_calls=1
    )
    atomic_write_parquet(
        pl.DataFrame(
            [{"queue_item_id": "Q1", "status": "CRAWL_COMPLETED"}],
            schema={"queue_item_id": pl.String, "status": pl.String},
        ),
        controller.queue_path,
        {"test": "cached-convergence"},
        key_columns=("queue_item_id",),
    )
    monkeypatch.setattr(controller, "build_plan", lambda: {"queue_total": 1})
    monkeypatch.setattr(controller, "_selected_cities", lambda _cities: [])
    monkeypatch.setattr(
        controller,
        "_next_work_source",
        lambda _cities: {
            "work_source": production.WORK_SOURCE_ORDINARY_RAW_PENDING,
            "cities": [],
            "reason_code": "NO_RAW_WORK",
        },
    )
    monkeypatch.setattr(
        controller,
        "_claim_queue",
        lambda _cities, *, job_id, work_source: {
            "claimed": 0,
            "queue_item_ids": [],
            "work_source": work_source,
        },
    )
    monkeypatch.setattr(
        production,
        "analysis_ready_scope_entities",
        lambda _output: {"core_document_ids": set()},
    )
    monkeypatch.setattr(production, "load_api_fast_lane_plan", lambda _output: (pl.DataFrame(), None))
    monkeypatch.setattr(production, "api_fast_lane_document_priorities", lambda _output: {})
    monkeypatch.setattr(
        controller,
        "_recover_api_failures",
        lambda **_kwargs: {
            "recovery_attempted": 0,
            "recovery_success": 0,
            "recovery_gate": "BACKOFF_SINGLE_PROBE",
        },
    )
    monkeypatch.setattr(
        production,
        "CrawlService",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("cached convergence must not start CrawlService")
        ),
    )

    progress_stages: list[str] = []
    result = controller.run_job(
        "JOB_CACHED_CONVERGENCE",
        CrawlJobRequest(mode="historical_episode_930", episode_id="EP_2016_930_TIGHTENING"),
        progress=lambda stage, _current, _total, _message, _counters: progress_stages.append(stage),
    )

    assert result["cached_convergence"] is True
    assert result["postprocess"]["api_recovery"]["recovery_gate"] == "BACKOFF_SINGLE_PROBE"
    assert progress_stages[-1:] == ["enriching"]
    assert (controller.handoff_path).exists()


def test_run_audit_metrics_separate_api_outcomes_and_pdf_archive(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    controller = Episode930ProductionController(
        settings, run_id="EP930_TEST_METRICS", city_limit=1, max_ai_calls=3
    )
    request_dir = controller.run_dir / "05_API_CLASSIFICATION" / "ai_audit" / "requests"
    request_dir.mkdir(parents=True)
    common = {
        "provider": "test",
        "model": "test-model",
        "input_summary": {"pass_name": "first_pass"},
    }
    (request_dir / "completed.json").write_text(
        json.dumps({
            **common,
            "request_id": "completed",
            "status": "response_completed",
            "total_tokens": 12,
            "estimated_cost_usd": None,
        }),
        encoding="utf-8",
    )
    (request_dir / "failed.json").write_text(
        json.dumps({
            **common,
            "request_id": "failed",
            "status": "response_failed",
        }),
        encoding="utf-8",
    )
    (request_dir / "started.json").write_text(
        json.dumps({
            **common,
            "request_id": "started",
            "status": "request_started",
        }),
        encoding="utf-8",
    )
    api = controller._api_audit_metrics()
    assert api["api_attempts"] == 3
    assert api["api_success"] == 1
    assert api["api_failed"] == 1
    assert api["api_deferred"] == 1
    assert api["api_in_flight"] == 1
    assert api["tokens"] == 12
    assert api["cost"] is None

    attachment_path = settings.curated / "attachments.parquet"
    atomic_write_parquet(
        pl.DataFrame(
            [
                {
                    "attachment_id": "A1",
                    "run_id": "CRAWL_TEST",
                    "parent_item_id": "I1",
                    "url": "https://example.gov.cn/a.pdf",
                    "local_path": "archive/a.pdf",
                    "content_sha256": "hash-a",
                    "status": "FETCHED",
                },
                {
                    "attachment_id": "A2",
                    "run_id": "CRAWL_TEST",
                    "parent_item_id": "I2",
                    "url": "https://example.gov.cn/b.pdf",
                    "local_path": None,
                    "content_sha256": None,
                    "status": "PENDING_ATTACHMENT",
                },
                {
                    "attachment_id": "A3",
                    "run_id": "CRAWL_TEST",
                    "parent_item_id": "I3",
                    "url": "https://example.gov.cn/c.pdf",
                    "local_path": None,
                    "content_sha256": None,
                    "status": "FAILED",
                },
            ]
        ),
        attachment_path,
        {"test": "attachment-metrics"},
        key_columns=("attachment_id",),
    )
    attachments = controller._attachment_metrics("CRAWL_TEST")
    assert attachments["attachments_found"] == 3
    assert attachments["pdfs_found"] == 3
    assert attachments["attachments_archived"] == 1
    assert attachments["pdfs_archived"] == 1
    assert attachments["pending"] == 1
    assert attachments["failed"] == 1


def test_new_run_does_not_inherit_shared_snapshot_counters(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    first = Episode930ProductionController(
        settings, run_id="EP930_TEST_FIRST", city_limit=1, max_ai_calls=0
    )
    first._snapshot(queue_completed=9, documents_found=4)
    second = Episode930ProductionController(
        settings, run_id="EP930_TEST_SECOND", city_limit=1, max_ai_calls=0
    )
    snapshot = second._snapshot()
    assert snapshot["run_id"] == "EP930_TEST_SECOND"
    assert snapshot["queue_completed"] == 0
    assert snapshot["documents_found"] == 0


def test_implicit_city_selection_rotates_to_least_processed_city(
    tmp_path: Path, monkeypatch
) -> None:
    settings = _settings(tmp_path)
    cities = _cities().vstack(
        pl.DataFrame(
            [
                {
                    "city_id": "CITY_C",
                    "city_name": "鍩庡競涓?",
                    "city_name_short": "涓?",
                    "province_name": "鐪佷笁",
                    "province_code": "PC",
                    "aliases": "",
                }
            ]
        )
    )
    monkeypatch.setattr(production, "load_cities_105", lambda _settings: cities)
    monkeypatch.setattr(production, "load_registry", lambda _settings: [])
    controller = Episode930ProductionController(
        settings, run_id="EP930_TEST_ROTATION", city_limit=1, max_ai_calls=0
    )
    controller.build_plan()
    queue = read_parquet_snapshot(controller.queue_path)
    rows = queue.to_dicts()
    for row in rows:
        if row["city_id"] == "CITY_A":
            row["status"] = "CRAWL_COMPLETED"
    atomic_write_parquet(
        pl.DataFrame(rows, schema=QUEUE_SCHEMA),
        controller.queue_path,
        {"test": "rotation"},
        key_columns=("queue_item_id",),
    )
    assert controller._selected_cities(None) == ["CITY_B"]


def test_snapshot_rejects_stale_run_id_and_preserves_selected_run(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    controller = Episode930ProductionController(
        settings, run_id="EP930_TEST_CURRENT", city_limit=1, max_ai_calls=0
    )
    controller.snapshot_path.write_text(
        json.dumps({"run_id": "EP930_TEST_OLD", "documents_found": 99}),
        encoding="utf-8",
    )
    snapshot = controller._snapshot(selected_cities=["CITY_A"])
    assert snapshot["run_id"] == "EP930_TEST_CURRENT"
    assert snapshot["documents_found"] == 0
    assert snapshot["selected_cities"] == ["CITY_A"]


def test_api_audit_metrics_marks_mixed_outcome_as_not_fully_operational(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    controller = Episode930ProductionController(
        settings, run_id="EP930_TEST_MIXED_API", city_limit=1, max_ai_calls=2
    )
    request_dir = controller.run_dir / "05_API_CLASSIFICATION" / "ai_audit" / "requests"
    request_dir.mkdir(parents=True)
    for name, status in (("ok", "response_completed"), ("bad", "response_failed")):
        (request_dir / f"{name}.json").write_text(
            json.dumps(
                {
                    "request_id": name,
                    "status": status,
                    "input_summary": {"pass_name": "first_pass"},
                    "total_tokens": 7 if status == "response_completed" else None,
                }
            ),
            encoding="utf-8",
        )
    metrics = controller._api_audit_metrics()
    assert metrics["api_success"] == 1
    assert metrics["api_failed"] == 1
    assert metrics["api_deferred"] == 1


def test_effective_date_evidence_accepts_explicit_and_publication_based_language() -> None:
    explicit = _parse_effective_evidence(
        "本通知自2016年9月30日起施行。",
        datetime(2016, 9, 28, tzinfo=UTC).date(),
    )
    publication = _parse_effective_evidence(
        "本通知自印发之日起执行。",
        datetime(2016, 9, 28, tzinfo=UTC).date(),
    )
    missing = _parse_effective_evidence(
        "本通知发布后请各部门做好宣传。",
        datetime(2016, 9, 28, tzinfo=UTC).date(),
    )
    assert explicit[:3] == (datetime(2016, 9, 30, tzinfo=UTC).date(), "HIGH", "EXPLICIT_EFFECTIVE_DATE")
    assert publication[:3] == (datetime(2016, 9, 28, tzinfo=UTC).date(), "HIGH", "PUBLICATION_DATE_EFFECTIVE")
    assert missing[:3] == (None, "LOW", "NO_EXPLICIT_EFFECTIVE_DATE")
    assert publication[3]


def test_typed_gap_register_separates_date_parameter_classification_and_attachment(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    pipeline = Episode930Pipeline(
        settings,
        config=EpisodeConfig(run_search=False, run_ai=False, apply=True),
        output=tmp_path / "episode-output",
    )
    documents = pl.DataFrame(
        [
            {
                "document_id": "DOC1",
                "city": "城市甲",
                "official_url": "https://example.gov.cn/policy",
                "effective_date": None,
                "official_text": "提高首付比例。",
                "is_formal_eligible": True,
            }
        ]
    )
    actions = pl.DataFrame(
        [
            {
                "document_id": "DOC1",
                "action_id": "ACT1",
                "city": "城市甲",
                "action_text": "提高首付比例",
                "policy_type": "COMMERCIAL_DOWNPAYMENT",
                "official_text_excerpt": "提高首付比例",
            }
        ]
    )
    gaps, metrics = pipeline.build_gap_register(
        documents,
        actions,
        pl.DataFrame(),
        pl.DataFrame(),
        attachment_metrics={"pending": 1, "retryable_failure": 0},
        ai_rows=pl.DataFrame(),
    )
    gap_types = set(gaps.get_column("gap_type").to_list())
    assert {"DATE_GAP", "PARAMETER_GAP", "CLASSIFICATION_GAP", "ATTACHMENT_GAP"}.issubset(gap_types)
    assert metrics["gap_rows"] == gaps.height


def test_api_failure_artifact_is_retryable_and_redacts_secrets(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    controller = Episode930ProductionController(
        settings, run_id="EP930_TEST_API_FAILURE", city_limit=1, max_ai_calls=2
    )
    request_dir = controller.run_dir / "05_API_CLASSIFICATION" / "ai_audit" / "requests"
    request_dir.mkdir(parents=True)
    (request_dir / "failed.json").write_text(
        json.dumps(
            {
                "request_id": "REQ1",
                "request_hash": "HASH1",
                "slot_id": "DOC1",
                "status": "response_failed",
                "error_type": "TimeoutError",
                "error_message": "Authorization: Bearer sk-secret-value",
                "updated_at": "2026-08-14T00:00:00+00:00",
                "input_summary": {"pass_name": "first_pass"},
            }
        ),
        encoding="utf-8",
    )
    result = controller._write_api_failure_artifact(
        pl.DataFrame([{"document_id": "DOC1", "content_hash": "CONTENT1"}])
    )
    assert result["retryable_failures"] == 1
    failures = read_parquet_snapshot(controller.output / "930_API_FAILURES.parquet")
    row = failures.row(0, named=True)
    assert row["retryable"] is True
    assert "sk-secret" not in row["error_message_safe"]
    assert row["content_hash"] == "CONTENT1"


def test_api_failure_artifact_uses_request_hash_index_without_global_scan(
    tmp_path: Path, monkeypatch
) -> None:
    settings = _settings(tmp_path)
    controller = Episode930ProductionController(
        settings, run_id="EP930_TEST_API_FAILURE_INDEX", city_limit=1, max_ai_calls=1
    )
    request_dir = controller.run_dir / "05_API_CLASSIFICATION" / "ai_audit" / "requests"
    request_dir.mkdir(parents=True)
    (request_dir / "failed.json").write_text(
        json.dumps(
            {
                "request_id": "REQ_INDEX",
                "request_hash": "HASH_INDEX",
                "slot_id": "DOC_INDEX",
                "status": "response_failed",
                "error_type": "TimeoutError",
                "error_message": "temporary provider timeout",
                "updated_at": "2026-08-14T00:00:00+00:00",
                "input_summary": {"pass_name": "first_pass"},
            }
        ),
        encoding="utf-8",
    )
    global_dir = settings.outputs / "ai_audit" / "requests"
    global_dir.mkdir(parents=True)
    (global_dir / "HASH_INDEX.json").write_text(
        json.dumps(
            {
                "request_hash": "HASH_INDEX",
                "status": "response_completed",
            }
        ),
        encoding="utf-8",
    )
    original_glob = Path.glob

    def reject_global_scan(path: Path, pattern: str):
        if path == global_dir and pattern == "*.json":
            raise AssertionError("global audit directory must not be scanned")
        return original_glob(path, pattern)

    monkeypatch.setattr(Path, "glob", reject_global_scan)

    controller._write_api_failure_artifact(
        pl.DataFrame([{"document_id": "DOC_INDEX", "content_hash": "CONTENT_INDEX"}])
    )

    failures = read_parquet_snapshot(controller.output / "930_API_FAILURES.parquet")
    assert failures.item(0, "status") == "RESOLVED"


def test_api_failure_artifact_handles_response_payload_schema_drift(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    controller = Episode930ProductionController(
        settings, run_id="EP930_TEST_PAYLOAD_SCHEMA_DRIFT", city_limit=1, max_ai_calls=1
    )
    legacy_payload = {f"legacy_field_{index:02d}": f"value-{index}" for index in range(28)}
    legacy_payload.update(
        {
            "classification": {"direction": "TIGHTENING"},
            "document_id": "DOC_LEGACY",
        }
    )
    atomic_write_parquet(
        pl.DataFrame(
            [
                {
                    "failure_id": "LEGACY_FAILURE",
                    "request_hash": "LEGACY_HASH",
                    "retryable": True,
                    "status": "RETRYABLE_FAILURE",
                    "recovery_status": "PENDING_RETRY",
                    "raw_response_payload": legacy_payload,
                }
            ]
        ),
        controller.output / "930_API_FAILURES.parquet",
        {"test": "payload_schema_drift"},
        key_columns=("failure_id",),
    )
    request_dir = controller.run_dir / "05_API_CLASSIFICATION" / "ai_audit" / "requests"
    request_dir.mkdir(parents=True)
    (request_dir / "new-schema.json").write_text(
        json.dumps(
            {
                "request_id": "REQ_NEW_SCHEMA",
                "request_hash": "HASH_NEW_SCHEMA",
                "slot_id": "DOC_NEW_SCHEMA",
                "status": "response_failed",
                "error_type": "SchemaValidationError",
                "error_message": "schema mismatch",
                "failure_class": "SCHEMA_VALIDATION_FAILURE",
                "updated_at": "2026-08-15T00:00:00+00:00",
                "input_summary": {"pass_name": "first_pass"},
                "raw_response_payload": {
                    "classification": [{"direction": "TIGHTENING"}],
                    "document_id": "DOC_NEW_SCHEMA",
                },
            }
        ),
        encoding="utf-8",
    )

    result = controller._write_api_failure_artifact(
        pl.DataFrame([{"document_id": "DOC_NEW_SCHEMA", "content_hash": "CONTENT_NEW"}])
    )

    assert result["failure_rows"] == 2
    failures = read_parquet_snapshot(controller.output / "930_API_FAILURES.parquet")
    assert failures.height == 2
    assert failures.schema["raw_response_payload"] == pl.String
    payload = json.loads(
        failures.filter(pl.col("failure_id") != "LEGACY_FAILURE")
        .item(0, "raw_response_payload")
    )
    assert payload["document_id"] == "DOC_NEW_SCHEMA"


def test_402_failure_stays_in_provider_recovery_queue_until_recovered(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    controller = Episode930ProductionController(
        settings, run_id="EP930_TEST_API_402", city_limit=1, max_ai_calls=2
    )
    request_dir = controller.run_dir / "05_API_CLASSIFICATION" / "ai_audit" / "requests"
    request_dir.mkdir(parents=True)
    (request_dir / "failed-402.json").write_text(
        json.dumps(
            {
                "request_id": "REQ402",
                "request_hash": "HASH402",
                "slot_id": "DOC402",
                "provider": "siliconflow",
                "model": "test-model",
                "status": "response_failed",
                "error_type": "APIStatusError",
                "error_message": "Error code: 402 - account balance is insufficient",
                "updated_at": "2026-08-14T00:00:00+00:00",
                "input_summary": {"pass_name": "first_pass"},
            }
        ),
        encoding="utf-8",
    )

    result = controller._write_api_failure_artifact(
        pl.DataFrame([{"document_id": "DOC402", "content_hash": "CONTENT402"}])
    )
    queue = read_parquet_snapshot(controller.output / "930_API_RECOVERY_QUEUE.parquet")
    assert result["provider_recovery_pending"] == 1
    assert queue.height == 1
    row = queue.row(0, named=True)
    assert row["status"] == "AI_DEFERRED"
    assert row["recovery_status"] == "PENDING_PROVIDER_RECOVERY"
    provider_status = json.loads(
        controller.provider_status_path.read_text(encoding="utf-8")
    )
    assert provider_status["status"] == "BLOCKED_EXTERNAL_402"


def test_api_recovery_transition_requires_single_probe_schema_before_micro5() -> None:
    failed = api_recovery_transition("SINGLE_PROBE", 1, 0, False)
    assert failed["next_phase"] == "BACKOFF_SINGLE_PROBE"
    assert failed["backoff"] is True

    valid = api_recovery_transition("SINGLE_PROBE", 1, 1, True)
    assert valid["next_phase"] == "MICRO_5"
    assert valid["backoff"] is False

    unstable = api_recovery_transition("MICRO_5", 5, 3, True)
    assert unstable["next_phase"] == "BACKOFF_SINGLE_PROBE"

    stable = api_recovery_transition("MICRO_5", 5, 4, True)
    assert stable["next_phase"] == "MICRO_20"

    final = api_recovery_transition("MICRO_20", 20, 16, True)
    assert final["next_phase"] == "BACKLOG_CONSUMPTION"


def test_micro_certification_cannot_advance_from_partial_real_batch() -> None:
    """A stage label is not a completed certification batch."""

    micro5_partial = api_recovery_transition("MICRO_5", 1, 1, True)
    assert micro5_partial["next_phase"] == "MICRO_5"
    assert micro5_partial["batch_status"] == "RUNNING"

    micro20_partial = api_recovery_transition("MICRO_20", 19, 16, True)
    assert micro20_partial["next_phase"] == "MICRO_20"
    assert micro20_partial["batch_status"] == "RUNNING"


def test_micro20_pass_feasibility_marks_threshold_unreachable() -> None:
    impossible = production.certification_batch_feasibility("MICRO_20", 8, 3)
    assert impossible["remaining_slots"] == 12
    assert impossible["successes_needed"] == 13
    assert impossible["max_possible_valid"] == 15
    assert impossible["required_valid"] == 16
    assert impossible["pass_possible"] is False
    assert impossible["pass_possible_reason"] == "PASS_THRESHOLD_MATHEMATICALLY_UNREACHABLE"

    boundary = production.certification_batch_feasibility("MICRO_20", 8, 4)
    assert boundary["pass_possible"] is True

    one_slot_possible = production.certification_batch_feasibility("MICRO_20", 19, 15)
    assert one_slot_possible["remaining_slots"] == 1
    assert one_slot_possible["pass_possible"] is True

    one_slot_impossible = production.certification_batch_feasibility("MICRO_20", 19, 14)
    assert one_slot_impossible["pass_possible"] is False


def test_due_micro20_schema_failure_resumes_same_batch_without_provider_block(
    tmp_path: Path, monkeypatch
) -> None:
    """A schema failure with a successful HTTP call must not strand ordinal 2."""

    settings = _settings(tmp_path)
    output = settings.outputs / "special_projects" / "2016_930"
    output.mkdir(parents=True)
    controller = Episode930ProductionController(
        settings, output=output, run_id="EP930_TEST_MICRO20_RESUME", max_ai_calls=1
    )

    atomic_write_parquet(
        pl.DataFrame(
            [
                {
                    "failure_id": "F1",
                    "document_id": "DOC1",
                    "status": "RETRYABLE_FAILURE",
                    "recovery_status": "PENDING_RETRY",
                    "next_retry_at": "2000-01-01T00:00:00+00:00",
                    "created_at": "2000-01-01T00:00:00+00:00",
                }
            ]
        ),
        output / "930_API_RECOVERY_QUEUE.parquet",
        {"test": "micro20-resume"},
        key_columns=("failure_id",),
    )
    atomic_write_parquet(
        pl.DataFrame([{"document_id": "DOC1", "content_hash": "HASH1"}]),
        settings.curated / "policy_episode_documents.parquet",
        {"test": "micro20-doc"},
        key_columns=("document_id",),
    )
    atomic_write_parquet(
        pl.DataFrame([{"document_id": "DOC1"}]),
        settings.curated / "policy_episode_actions.parquet",
        {"test": "micro20-action"},
        key_columns=("document_id",),
    )
    controller.provider_status_path.write_text(
        json.dumps(
            {
                "status": "SCHEMA_VALIDATION_FAILURE",
                "api_balance_status": "call_succeeded",
                "primary_provider_unavailable": False,
            }
        ),
        encoding="utf-8",
    )
    controller.recovery_state_path.write_text(
        json.dumps(
            {
                "phase": "BACKOFF_SINGLE_PROBE",
                "last_phase": "MICRO_5",
                "last_success_documents": 5,
                "last_success_rate": 1.0,
                "schema_valid": True,
                "next_retry_at": "2000-01-01T00:00:00+00:00",
            }
        ),
        encoding="utf-8",
    )
    active_batch = {
        "certification_batch_id": "BATCH_MICRO20",
        "stage": "MICRO_20",
        "required_attempts": 20,
        "real_provider_attempts": 1,
        "schema_valid_successes": 0,
        "pending_attempts": 19,
        "batch_status": "RETRY_WAIT",
    }
    monkeypatch.setattr(
        controller,
        "_replay_schema_failures_locally",
        lambda queue: (queue, {"recovered": 0}),
    )
    monkeypatch.setattr(
        controller,
        "_ensure_certification_batch",
        lambda _state, _provider: (pl.DataFrame(), active_batch),
    )
    called = {"provider_seam": 0}

    class FakePipeline:
        def __init__(self, *_args, **kwargs) -> None:
            self.output = Path(kwargs["output"])

        def classify_actions(self, _documents, _actions):
            called["provider_seam"] += 1
            request_dir = self.output / "05_API_CLASSIFICATION" / "ai_audit" / "requests"
            request_dir.mkdir(parents=True, exist_ok=True)
            (request_dir / "ordinal-2.json").write_text(
                json.dumps(
                    {
                        "slot_id": "DOC1",
                        "request_id": "REQ_ORDINAL_2",
                        "status": "response_failed",
                        "error_type": "SchemaValidationError",
                        "failure_class": "SCHEMA_VALIDATION_FAILURE",
                        "input_summary": {"pass_name": "first_pass"},
                        "provider": "siliconflow",
                        "model": "test-model",
                        "started_at": "2026-08-25T00:00:00+00:00",
                        "updated_at": "2026-08-25T00:00:01+00:00",
                        "transport_started": True,
                    }
                ),
                encoding="utf-8",
            )
            return pl.DataFrame(), {"ai_status": "operational", "api_cache_hit_document_ids": []}

    monkeypatch.setattr(production, "Episode930Pipeline", FakePipeline)
    monkeypatch.setattr(
        controller,
        "_record_certification_attempts",
        lambda *_args, **_kwargs: pl.DataFrame(schema=production.CERTIFICATION_ATTEMPT_SCHEMA),
    )
    monkeypatch.setattr(
        controller,
        "_update_certification_batch",
        lambda *_args, **_kwargs: (
            pl.DataFrame(),
            {
                "next_phase": "MICRO_20",
                "backoff": True,
                "success_rate": 0.0,
                "schema_valid_successes": 0,
                "reason_code": "SCHEMA_VALIDATION_FAILURE",
            },
        ),
    )
    monkeypatch.setattr(
        production,
        "certification_gate_from_ledger",
        lambda _ledger: {"certification": "BLOCKED_BY_CERTIFICATION_BATCH"},
    )

    result = controller._recover_api_failures()

    assert called["provider_seam"] == 1
    assert result["recovery_attempted"] == 1
    assert result["recovery_gate"] == "MICRO_20"
    assert result["reason_code"] != "PROVIDER_NOT_RECOVERED_FOR_MICRO_BATCH"


def test_due_micro20_provider_failure_resumes_same_batch_on_next_retry(
    tmp_path: Path, monkeypatch
) -> None:
    """A due hard-provider retry must reach the next ordinal in the same batch."""

    settings = _settings(tmp_path)
    output = settings.outputs / "special_projects" / "2016_930"
    output.mkdir(parents=True)
    controller = Episode930ProductionController(
        settings, output=output, run_id="EP930_TEST_MICRO20_PROVIDER_RETRY", max_ai_calls=1
    )

    atomic_write_parquet(
        pl.DataFrame(
            [
                {
                    "failure_id": "F1",
                    "document_id": "DOC1",
                    "status": "RETRYABLE_FAILURE",
                    "recovery_status": "PENDING_PROVIDER_RECOVERY",
                    "next_retry_at": "2000-01-01T00:00:00+00:00",
                    "created_at": "2000-01-01T00:00:00+00:00",
                }
            ]
        ),
        output / "930_API_RECOVERY_QUEUE.parquet",
        {"test": "micro20-provider-retry"},
        key_columns=("failure_id",),
    )
    atomic_write_parquet(
        pl.DataFrame([{"document_id": "DOC1", "content_hash": "HASH1"}]),
        settings.curated / "policy_episode_documents.parquet",
        {"test": "micro20-provider-doc"},
        key_columns=("document_id",),
    )
    atomic_write_parquet(
        pl.DataFrame([{"document_id": "DOC1"}]),
        settings.curated / "policy_episode_actions.parquet",
        {"test": "micro20-provider-action"},
        key_columns=("document_id",),
    )
    controller.provider_status_path.write_text(
        json.dumps(
            {
                "status": "PRIMARY_PROVIDER_UNAVAILABLE",
                "api_balance_status": "unknown",
                "primary_provider_unavailable": True,
            }
        ),
        encoding="utf-8",
    )
    controller.recovery_state_path.write_text(
        json.dumps(
            {
                "phase": "BACKOFF_SINGLE_PROBE",
                "last_phase": "MICRO_20",
                "last_success_documents": 0,
                "last_success_rate": 0.0,
                "schema_valid": False,
                "next_retry_at": "2000-01-01T00:00:00+00:00",
            }
        ),
        encoding="utf-8",
    )
    active_batch = {
        "certification_batch_id": "BATCH_MICRO20_PROVIDER_RETRY",
        "stage": "MICRO_20",
        "required_attempts": 20,
        "real_provider_attempts": 2,
        "schema_valid_successes": 0,
        "pending_attempts": 18,
        "batch_status": "RETRY_WAIT",
    }
    monkeypatch.setattr(
        controller,
        "_replay_schema_failures_locally",
        lambda queue: (queue, {"recovered": 0}),
    )
    monkeypatch.setattr(
        controller,
        "_ensure_certification_batch",
        lambda _state, _provider: (pl.DataFrame(), active_batch),
    )
    called = {"provider_seam": 0}

    class FakePipeline:
        def __init__(self, *_args, **kwargs) -> None:
            self.output = Path(kwargs["output"])

        def classify_actions(self, _documents, _actions):
            called["provider_seam"] += 1
            request_dir = self.output / "05_API_CLASSIFICATION" / "ai_audit" / "requests"
            request_dir.mkdir(parents=True, exist_ok=True)
            (request_dir / "ordinal-3.json").write_text(
                json.dumps(
                    {
                        "slot_id": "DOC1",
                        "request_id": "REQ_ORDINAL_3",
                        "status": "response_failed",
                        "error_type": "ConnectTimeout",
                        "failure_class": "CONNECT_TIMEOUT",
                        "input_summary": {"pass_name": "first_pass"},
                        "provider": "siliconflow",
                        "model": "test-model",
                        "started_at": "2026-08-25T00:00:00+00:00",
                        "updated_at": "2026-08-25T00:00:01+00:00",
                        "transport_started": True,
                    }
                ),
                encoding="utf-8",
            )
            return pl.DataFrame(), {"ai_status": "provider_unavailable", "api_cache_hit_document_ids": []}

    monkeypatch.setattr(production, "Episode930Pipeline", FakePipeline)
    monkeypatch.setattr(
        controller,
        "_record_certification_attempts",
        lambda *_args, **_kwargs: pl.DataFrame(schema=production.CERTIFICATION_ATTEMPT_SCHEMA),
    )
    monkeypatch.setattr(
        controller,
        "_update_certification_batch",
        lambda *_args, **_kwargs: (
            pl.DataFrame(),
            {
                "next_phase": "BACKOFF_SINGLE_PROBE",
                "backoff": True,
                "success_rate": 0.0,
                "schema_valid_successes": 0,
                "reason_code": "CONNECT_TIMEOUT",
            },
        ),
    )
    monkeypatch.setattr(
        production,
        "certification_gate_from_ledger",
        lambda _ledger: {"certification": "BLOCKED_BY_CERTIFICATION_BATCH"},
    )

    result = controller._recover_api_failures()

    assert called["provider_seam"] == 1
    assert result["recovery_attempted"] == 1
    assert result["recovery_gate"] == "BACKOFF_SINGLE_PROBE"
    assert result["reason_code"] != "PROVIDER_NOT_RECOVERED_FOR_MICRO_BATCH"


def test_certification_gate_requires_all_persisted_batches() -> None:
    partial = certification_gate_from_ledger(
        [
            {
                "stage": "SINGLE",
                "batch_status": "PASS",
                "real_provider_attempts": 1,
                "schema_valid_successes": 1,
                "updated_at": "2026-08-24T10:00:00+00:00",
            },
            {
                "stage": "MICRO_5",
                "batch_status": "RUNNING",
                "real_provider_attempts": 1,
                "schema_valid_successes": 1,
                "updated_at": "2026-08-24T10:01:00+00:00",
            },
        ]
    )
    assert partial["certification"] == "BLOCKED_BY_CERTIFICATION_BATCH"
    assert partial["stages"]["MICRO_5"]["pending_attempts"] == 4
    assert partial["stages"]["MICRO_5"]["passed"] is False

    complete = certification_gate_from_ledger(
        [
            {
                "stage": "SINGLE",
                "batch_status": "PASS",
                "real_provider_attempts": 1,
                "schema_valid_successes": 1,
                "updated_at": "2026-08-24T10:00:00+00:00",
            },
            {
                "stage": "MICRO_5",
                "batch_status": "PASS",
                "real_provider_attempts": 5,
                "schema_valid_successes": 4,
                "updated_at": "2026-08-24T10:01:00+00:00",
            },
            {
                "stage": "MICRO_20",
                "batch_status": "PASS",
                "real_provider_attempts": 20,
                "schema_valid_successes": 16,
                "updated_at": "2026-08-24T10:02:00+00:00",
            },
        ]
    )
    assert complete["certification"] == "PASS"


def test_cache_reuse_does_not_fill_certification_batch() -> None:
    result = certification_batch_transition(
        "MICRO_5",
        1,
        1,
        True,
        cache_reuse_count=4,
    )
    assert result["next_phase"] == "MICRO_5"
    assert result["real_provider_attempts"] == 1
    assert result["pending_attempts"] == CERTIFICATION_STAGE_REQUIREMENTS["MICRO_5"] - 1
    assert result["cache_reuse_count"] == 4


def test_new_certification_ledger_does_not_infer_micro5_from_old_phase(tmp_path) -> None:
    controller = Episode930ProductionController(
        _settings(tmp_path),
        output=tmp_path / "output",
        run_id="RUN_CERTIFICATION_NEW",
    )
    _, batch = controller._ensure_certification_batch(
        {
            "phase": "MICRO_5",
            "last_phase": "SINGLE_PROBE",
            "last_success_documents": 1,
            "schema_valid": True,
        },
        {"provider": "siliconflow", "model": "test-model"},
    )
    assert batch is not None
    assert batch["stage"] == "SINGLE"


def test_single_certification_stage_maps_to_runtime_single_probe() -> None:
    assert production._certification_runtime_phase("SINGLE") == "SINGLE_PROBE"


def test_certification_attempt_and_batch_ledgers_persist_real_success(tmp_path) -> None:
    controller = Episode930ProductionController(
        _settings(tmp_path),
        output=tmp_path / "output",
        run_id="RUN_CERTIFICATION_SUCCESS",
    )
    _, batch = controller._ensure_certification_batch(
        {"phase": "SINGLE_PROBE"},
        {"provider": "siliconflow", "model": "test-model"},
    )
    assert batch is not None
    attempts = controller._record_certification_attempts(
        batch,
        [
            {
                "request_id": "REQ_SINGLE_1",
                "slot_id": "DOC_SINGLE_1",
                "status": "response_completed",
                "provider": "siliconflow",
                "model": "test-model",
                "started_at": "2026-08-24T10:00:00+00:00",
                "completed_at": "2026-08-24T10:00:02+00:00",
                "http_status": 200,
                "schema_valid": True,
                "transport_started": True,
                "response_payload": {"actions": []},
                "input_summary": {"pass_name": "first_pass"},
            }
        ],
        set(),
        {"DOC_SINGLE_1"},
    )
    assert attempts.height == 1
    attempt = attempts.row(0, named=True)
    assert attempt["provider_attempt"] is True
    assert attempt["ordinal"] == 1
    assert (controller.certification_attempt_path).exists()

    ledger, transition = controller._update_certification_batch(batch, attempts)
    assert transition["next_phase"] == "MICRO_5"
    assert ledger.filter(pl.col("batch_status") == "PASS").height == 1
    assert read_parquet_snapshot(controller.certification_ledger_path).height == 1


def test_certification_failure_is_recorded_and_enters_retry_wait(tmp_path) -> None:
    controller = Episode930ProductionController(
        _settings(tmp_path),
        output=tmp_path / "output",
        run_id="RUN_CERTIFICATION_FAILURE",
    )
    _, batch = controller._ensure_certification_batch(
        {"phase": "MICRO_5"},
        {"provider": "siliconflow", "model": "test-model"},
    )
    assert batch is not None
    attempts = controller._record_certification_attempts(
        batch,
        [
            {
                "request_id": "REQ_MICRO_1",
                "slot_id": "DOC_MICRO_1",
                "status": "response_failed",
                "provider": "siliconflow",
                "model": "test-model",
                "started_at": "2026-08-24T10:00:00+00:00",
                "completed_at": "2026-08-24T10:02:00+00:00",
                "http_status": None,
                "schema_valid": False,
                "transport_started": True,
                "failure_class": "READ_TIMEOUT",
                "input_summary": {"pass_name": "first_pass"},
            }
        ],
        set(),
        {"DOC_MICRO_1"},
    )
    _, transition = controller._update_certification_batch(batch, attempts)
    assert transition["next_phase"] == "BACKOFF_SINGLE_PROBE"
    assert transition["batch_status"] == "RETRY_WAIT"
    assert attempts.filter(pl.col("provider_attempt")).height == 1


def test_backlog_transition_preserves_stable_behavior() -> None:
    stable = api_recovery_transition("BACKLOG_CONSUMPTION", 20, 16, True)
    assert stable["next_phase"] == "BACKLOG_CONSUMPTION"
    assert stable["batch_status"] == "NOT_APPLICABLE"

    unstable = api_recovery_transition("BACKLOG_CONSUMPTION", 20, 10, True)
    assert unstable["next_phase"] == "BACKOFF_SINGLE_PROBE"
    assert unstable["backoff"] is True


def test_api_fast_lane_selects_only_plan_lineage() -> None:
    documents = pl.DataFrame(
        [
            {
                "document_id": "DOC_MATCH",
                "content_hash": "HASH_MATCH",
                "official_url": "https://example.gov.cn/policy/1",
            },
            {
                "document_id": "DOC_OTHER",
                "content_hash": "HASH_OTHER",
                "official_url": "https://example.gov.cn/policy/2",
            },
        ]
    )
    actions = pl.DataFrame(
        [
            {"document_id": "DOC_MATCH", "action_id": "ACT_MATCH"},
            {"document_id": "DOC_OTHER", "action_id": "ACT_OTHER"},
        ]
    )
    plan = pl.DataFrame(
        [
            {
                "action_id": "ACT_MATCH",
                "document_id": "CURATED_DOC_MATCH",
                "official_url": "https://example.gov.cn/policy/1",
                "content_hash": "HASH_MATCH",
                "priority": 0,
            }
        ]
    )

    selected_documents, selected_actions, metrics = select_api_fast_lane_inputs(
        documents, actions, plan
    )

    assert selected_documents.get_column("document_id").to_list() == ["DOC_MATCH"]
    assert selected_actions.get_column("action_id").to_list() == ["ACT_MATCH"]
    assert metrics["reason_code"] == "FAST_LANE_LINEAGE_MATCH"


def test_api_fast_lane_priority_preserves_zero_as_highest(tmp_path: Path) -> None:
    output = tmp_path / "episode-output"
    snapshot = output / "treatment_universe_closure" / "20260815T000000Z"
    snapshot.mkdir(parents=True)
    (output / "930_ANALYSIS_READY_SCOPE.json").write_text(
        json.dumps({"scope_hash": "SCOPE_HASH", "frozen": True}),
        encoding="utf-8",
    )
    (snapshot / "EP930_TREATMENT_UNIVERSE_CLOSURE_MANIFEST.json").write_text(
        json.dumps({"scope": {"scope_hash": "SCOPE_HASH"}}),
        encoding="utf-8",
    )
    pl.DataFrame(
        [
            {"action_id": "A0", "document_id": "DOC1", "priority": 0},
            {"action_id": "A2", "document_id": "DOC1", "priority": 2},
        ]
    ).write_csv(snapshot / "EP930_API_FAST_LANE_PLAN.csv")

    priorities = api_fast_lane_document_priorities(output)

    assert priorities == {"DOC1": 0}


def test_api_recovery_audit_marks_cache_reuse_as_not_a_provider_probe() -> None:
    summary = production.summarize_api_recovery_probe(
        {"DOC1"},
        [],
        {"api_cache_hit_document_ids": ["DOC1"]},
    )

    assert summary["attempted_ids"] == set()
    assert summary["success_ids"] == set()
    assert summary["cache_reuse_ids"] == {"DOC1"}
    assert summary["reason_code"] == "CACHE_REUSE_NOT_A_PROVIDER_PROBE"


def test_recovery_single_probe_bypasses_existing_ai_cache(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("SILICONFLOW_CHAT_MODEL", "test-model")
    settings = _settings(tmp_path)
    doc = {
        "document_id": "DOC_PROBE",
        "city_id": "CITY_A",
        "document_title": "Test policy",
        "issuer": "Test issuer",
        "publication_date": "2016-09-30",
        "official_text": "Test official policy text",
    }
    payload = {
        "document_id": "DOC_PROBE",
        "actions": [{"action_id": "ACT_PROBE", "action_text": "test action"}],
    }

    class FakeProvider:
        def __init__(self) -> None:
            self.calls = 0

        def structured(self, **_kwargs):
            self.calls += 1
            trace = SimpleNamespace(
                raw_response_hash="response-hash",
                prompt_tokens=1,
                completion_tokens=2,
                latency_seconds=0.01,
                http_status=200,
                response_received=True,
                response_bytes=10,
                json_parse_ok=True,
                schema_valid=True,
                configured_read_timeout=30.0,
                configured_connect_timeout=10.0,
                max_retries=0,
                transport_started=True,
            )
            return ActionClassificationPayload(actions=[ActionClassification(action_id="ACT_PROBE")]), trace

    provider = FakeProvider()
    first_pipeline = Episode930Pipeline(settings, output=tmp_path / "run-first")
    first_audit = AIAuditStore(
        first_pipeline.phase_dirs["05_API_CLASSIFICATION"],
        global_root=settings.outputs,
    )
    first = first_pipeline._ai_call(
        provider,
        first_audit,
        doc,
        payload,
        "first_pass",
        ActionClassificationPayload,
        [],
    )
    assert first[3] is False
    assert provider.calls == 1
    assert first_audit.records()[0]["schema_version"] == "episode_930_action_classification_v1"

    probe_pipeline = Episode930Pipeline(
        settings,
        config=EpisodeConfig(bypass_ai_cache=True),
        output=tmp_path / "run-probe",
    )
    probe_audit = AIAuditStore(
        probe_pipeline.phase_dirs["05_API_CLASSIFICATION"],
        global_root=settings.outputs,
    )
    probe = probe_pipeline._ai_call(
        provider,
        probe_audit,
        doc,
        payload,
        "first_pass",
        ActionClassificationPayload,
        [],
        bypass_cache=True,
    )

    assert probe[3] is False
    assert probe[1] == 1
    assert provider.calls == 2
    assert len(list((tmp_path / "data" / "outputs" / "ai_audit" / "requests").glob("*.json"))) == 2


def test_provider_status_is_recovered_after_any_completed_response(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    controller = Episode930ProductionController(
        settings, run_id="EP930_TEST_API_RECOVERED", city_limit=1, max_ai_calls=1
    )
    request_dir = controller.run_dir / "05_API_CLASSIFICATION" / "ai_audit" / "requests"
    request_dir.mkdir(parents=True)
    (request_dir / "completed.json").write_text(
        json.dumps(
            {
                "request_id": "REQ_OK",
                "request_hash": "HASH_OK",
                "slot_id": "DOC_OK",
                "provider": "siliconflow",
                "model": "test-model",
                "status": "response_completed",
                "completed_at": "2026-08-14T00:00:01+00:00",
                "input_summary": {"pass_name": "second_review"},
            }
        ),
        encoding="utf-8",
    )
    result = controller._write_api_failure_artifact(pl.DataFrame())
    provider_status = json.loads(
        controller.provider_status_path.read_text(encoding="utf-8")
    )
    assert result["provider_status"] == "OPERATIONAL"
    assert provider_status["status"] == "OPERATIONAL"


def test_schema_failure_replay_uses_cached_payload_without_provider(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    controller = Episode930ProductionController(
        settings, run_id="EP930_TEST_LOCAL_REPLAY", city_limit=1, max_ai_calls=1
    )
    queue = pl.DataFrame(
        [
            {
                "failure_id": "F1",
                "request_id": "REQ1",
                "request_hash": "HASH1",
                "document_id": "DOC1",
                "pass_name": "first_pass",
                "failure_class": "SCHEMA_VALIDATION_FAILURE",
                "raw_response_payload": {
                    "result": {
                        "actions": [
                            {
                                "action_id": "A1",
                                "policy_type": "PURCHASE_RESTRICTION",
                                "direction": "TIGHTENING",
                            }
                        ]
                    }
                },
                "status": "RETRYABLE_FAILURE",
                "recovery_status": "PENDING_RETRY",
            }
        ]
    )

    updated, metrics = controller._replay_schema_failures_locally(queue)

    assert metrics == {"attempted": 1, "recovered": 1, "failed": 0}
    assert updated.item(0, "recovery_status") == "RECOVERED_LOCAL_REPLAY"
    assert (controller.output / "930_API_SCHEMA_REPLAY_RECEIPT.parquet").exists()
    assert (
        controller.run_dir
        / "05_API_CLASSIFICATION"
        / "2016_930_API_CLASSIFICATION.parquet"
        ).exists()


def test_schema_failure_replay_decodes_stable_payload_json(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    controller = Episode930ProductionController(
        settings, run_id="EP930_TEST_LOCAL_REPLAY_JSON", city_limit=1, max_ai_calls=1
    )
    payload = {
        "result": {
            "actions": [
                {
                    "action_id": "A1",
                    "policy_type": "PURCHASE_RESTRICTION",
                    "direction": "TIGHTENING",
                }
            ]
        }
    }
    queue = pl.DataFrame(
        [
            {
                "failure_id": "F1",
                "request_id": "REQ1",
                "request_hash": "HASH1",
                "document_id": "DOC1",
                "pass_name": "first_pass",
                "failure_class": "SCHEMA_VALIDATION_FAILURE",
                "raw_response_payload": json.dumps(payload, ensure_ascii=False),
                "status": "RETRYABLE_FAILURE",
                "recovery_status": "PENDING_RETRY",
            }
        ]
    )

    updated, metrics = controller._replay_schema_failures_locally(queue)

    assert metrics == {"attempted": 1, "recovered": 1, "failed": 0}
    assert updated.item(0, "recovery_status") == "RECOVERED_LOCAL_REPLAY"


def test_legacy_timeout_audit_is_classified_without_inventing_http_status() -> None:
    diagnostics = Episode930ProductionController._legacy_failure_diagnostics(
        "APITimeoutError", "request timed out"
    )

    assert diagnostics["failure_class"] == "UNKNOWN_PROVIDER_FAILURE"
    assert diagnostics["timeout_type"] == "unspecified"
    assert diagnostics["response_received"] is False
    assert "http_status" not in diagnostics


def test_empty_gap_audit_retains_parquet_key_schema(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    pipeline = Episode930Pipeline(
        settings,
        config=EpisodeConfig(run_search=False, run_ai=False, apply=False),
        output=tmp_path / "episode",
    )
    discovery = pl.DataFrame(
        schema={
            "city_id": pl.String,
            "city": pl.String,
            "province": pl.String,
            "mentioned_as_930_city": pl.Boolean,
        }
    )
    atomic_write_parquet(
        discovery,
        pipeline.phase_dirs["01_DISCOVERY"] / "2016_930_CITY_DISCOVERY.parquet",
        {"test": "empty-gap-input"},
        key_columns=("city_id",),
    )

    matrix, gaps, metrics = pipeline.gap_audit(pl.DataFrame(), pass_number=2)

    assert matrix.columns[:3] == ["episode_id", "audit_pass", "city_id"]
    assert {"audit_pass", "city", "policy_tool"}.issubset(matrix.columns)
    assert "gap_id" in gaps.columns
    assert metrics["matrix_cells"] == 0


def test_checkpoint_separates_episode_status_from_last_micro_batch(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    controller = Episode930ProductionController(
        settings, run_id="EP930_TEST_STATUS", city_limit=1, max_ai_calls=0
    )
    checkpoint = controller._write_checkpoint(
        stage="930_FINAL_COVERAGE_AUDIT",
        status="COMPLETED_WITH_WARNINGS",
        episode_status="RUNNING",
        last_micro_batch_status="COMPLETED_WITH_WARNINGS",
        next_batch_status="PENDING",
    )
    snapshot = json.loads(controller.snapshot_path.read_text(encoding="utf-8"))
    assert checkpoint["status"] == "COMPLETED_WITH_WARNINGS"
    assert snapshot["status"] == "RUNNING"
    assert snapshot["last_micro_batch_status"] == "COMPLETED_WITH_WARNINGS"
    assert snapshot["next_batch_status"] == "PENDING"
