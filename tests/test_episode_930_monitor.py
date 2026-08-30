from __future__ import annotations

import json
from pathlib import Path

import polars as pl

from policydb.episode_930 import Episode930Pipeline, EpisodeConfig
from policydb.episode_930_monitor import (
    api_health,
    build_monitor_snapshot,
    reconcile_queue,
    recovery_claim_metrics,
    stage_progress,
)
from policydb.parquet_store import atomic_write_parquet
from policydb.settings import Settings


def _write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False), encoding="utf-8")


def test_monitor_snapshot_uses_real_denominators_and_writes_history(tmp_path: Path) -> None:
    output = tmp_path / "outputs" / "special_projects" / "2016_930"
    output.mkdir(parents=True)
    _write_json(
        output / "930_PROGRESS_SNAPSHOT.json",
        {
            "episode_id": "EP_2016_930_TIGHTENING",
            "status": "RUNNING",
            "stage": "930_API_CLASSIFY_PASS1",
            "run_id": "RUN1",
            "queue_total": 10,
            "queue_completed": 3,
            "queue_pending": 7,
            "documents_found": 2,
            "official_documents": 2,
            "actions_extracted": 4,
            "actions_classified": 0,
            "api_pass1_success": 0,
            "api_pass2_success": 0,
            "dates_verified": 1,
            "parameters_extracted": 1,
            "formal_actions_promoted": 0,
            "gaps_remaining": 2,
            "pdfs_found": 1,
            "pdfs_archived": 0,
            "last_real_progress_at": "2026-08-14T06:00:00+00:00",
            "heartbeat_at": "2026-08-14T06:01:00+00:00",
        },
    )
    _write_json(
        output / "930_API_PROVIDER_STATUS.json",
        {"status": "TEMPORARY_PROVIDER_FAILURE"},
    )
    atomic_write_parquet(
        pl.DataFrame([{"queue_item_id": "Q1", "result_url": "https://a.gov.cn/1"}]),
        output / "930_QUEUE_SEARCH_EXECUTION.parquet",
        {"test": "monitor"},
        key_columns=("queue_item_id",),
    )
    atomic_write_parquet(
        pl.DataFrame([{"queue_item_id": "Q1", "http_status": 200, "real_network_fetch": True, "response_bytes": 12, "document_version_id": "DV1", "cache_hit": False}]),
        output / "930_QUEUE_HTTP_AUDIT.parquet",
        {"test": "monitor"},
        key_columns=("queue_item_id",),
    )

    snapshot = build_monitor_snapshot(output, write=True)

    assert snapshot["crawl"]["real_network_fetches"] == 1
    assert snapshot["crawl"]["document_versions"] == 1
    assert snapshot["stage_progress"]["api_pass1"]["total"] == 2
    assert snapshot["csv_readiness"]["analysis_ready"] is False
    assert snapshot["analysis_ready_eta"] == "BLOCKED_BY_API"
    assert snapshot["final_complete_eta"] == "BLOCKED_BY_API"
    assert snapshot["CURRENT_BATCH_PROGRESS"]["progress_scope"] == "CURRENT_BATCH_PROGRESS"
    assert snapshot["CSV_READINESS_GATE"]["gate_scope"] == "CSV_READINESS_GATE"
    assert (output / "930_MONITOR_SNAPSHOT.json").exists()
    assert (output / "930_PROGRESS_HISTORY.parquet").exists()


def test_monitor_separates_analysis_gap_gate_from_global_gap_gate(tmp_path: Path) -> None:
    data_root = tmp_path
    output = data_root / "outputs" / "special_projects" / "2016_930"
    curated = data_root / "curated"
    output.mkdir(parents=True)
    curated.mkdir(parents=True)
    _write_json(
        output / "930_PROGRESS_SNAPSHOT.json",
        {
            "episode_id": "EP_2016_930_TIGHTENING",
            "status": "RUNNING",
            "run_id": "RUN_GAP_SCOPE",
            "queue_total": 2,
            "queue_completed": 2,
            "queue_pending": 0,
            "documents_found": 2,
            "official_documents": 2,
            "actions_extracted": 2,
            "formal_actions_promoted": 2,
        },
    )
    _write_json(
        output / "930_ANALYSIS_READY_SCOPE.json",
        {
            "scope_version": "930-analysis-ready-v1",
            "scope_hash": "scope-hash",
            "city_ids": ["C_CORE"],
            "cities": ["Core City"],
            "queue_item_ids": ["Q_CORE"],
            "episode_window": ["2016-09-25", "2016-10-10"],
        },
    )
    _write_json(output / "930_API_PROVIDER_STATUS.json", {"status": "TEMPORARY_PROVIDER_FAILURE"})
    atomic_write_parquet(
        pl.DataFrame(
            [
                {
                    "episode_id": "EP_2016_930_TIGHTENING",
                    "document_id": "D_CORE",
                    "city_id": "C_CORE",
                    "city": "Core City",
                    "announcement_date": "2016-10-01",
                    "publication_date": "2016-10-01",
                    "is_formal_eligible": True,
                },
                {
                    "episode_id": "EP_2016_930_TIGHTENING",
                    "document_id": "D_GLOBAL",
                    "city_id": "C_OTHER",
                    "city": "Other City",
                    "announcement_date": "2016-10-01",
                    "publication_date": "2016-10-01",
                    "is_formal_eligible": True,
                },
            ]
        ),
        curated / "policy_episode_documents.parquet",
        {"test": "core-global-gap"},
        key_columns=("document_id",),
    )
    atomic_write_parquet(
        pl.DataFrame(
            [
                {
                    "episode_id": "EP_2016_930_TIGHTENING",
                    "document_id": "D_CORE",
                    "action_id": "A_CORE",
                },
                {
                    "episode_id": "EP_2016_930_TIGHTENING",
                    "document_id": "D_GLOBAL",
                    "action_id": "A_GLOBAL",
                },
            ]
        ),
        curated / "policy_episode_actions.parquet",
        {"test": "core-global-gap"},
        key_columns=("action_id",),
    )
    gap_dir = output / "production_runs" / "RUN_GAP_SCOPE" / "03_GAP_AUDIT"
    gap_dir.mkdir(parents=True)
    atomic_write_parquet(
        pl.DataFrame(
            [{"gap_id": "G_GLOBAL", "document_id": "D_GLOBAL", "action_id": None, "city_id": "C_OTHER", "severity": "HIGH"}]
        ),
        gap_dir / "2016_930_GAP_REGISTER.parquet",
        {"test": "core-global-gap"},
        key_columns=("gap_id",),
    )

    snapshot = build_monitor_snapshot(output)

    assert snapshot["CSV_READINESS_GATE"]["critical_gaps"] == 0
    assert snapshot["CSV_READINESS_GATE"]["global_critical_gaps"] == 1
    assert snapshot["stage_progress"]["analysis_ready_gap_audit"]["readiness_gate"] == "PASS"
    assert snapshot["stage_progress"]["gap_audit"]["readiness_gate"] == "FAIL"
    assert snapshot["GLOBAL_EPISODE_PROGRESS"]["analysis_ready_core_blocking_gaps"]["blocking_gap_count"] == 0
    assert snapshot["GLOBAL_EPISODE_PROGRESS"]["global_final_blocking_gaps"]["blocking_gap_count"] == 1


def test_monitor_uses_curated_global_gap_register_over_batch_gap_snapshot(
    tmp_path: Path,
) -> None:
    data_root = tmp_path
    output = data_root / "outputs" / "special_projects" / "2016_930"
    curated = data_root / "curated"
    output.mkdir(parents=True)
    curated.mkdir(parents=True)
    _write_json(
        output / "930_PROGRESS_SNAPSHOT.json",
        {
            "episode_id": "EP_2016_930_TIGHTENING",
            "status": "RUNNING",
            "run_id": "RUN_BATCH_GAP",
            "queue_total": 2,
            "queue_completed": 2,
            "queue_pending": 0,
        },
    )
    _write_json(
        output / "930_ANALYSIS_READY_SCOPE.json",
        {
            "scope_version": "930-analysis-ready-v1",
            "scope_hash": "scope-hash",
            "city_ids": ["C_CORE"],
            "cities": ["Core City"],
            "queue_item_ids": ["Q_CORE"],
            "episode_window": ["2016-09-25", "2016-10-10"],
        },
    )
    batch_dir = output / "production_runs" / "RUN_BATCH_GAP" / "03_GAP_AUDIT"
    batch_dir.mkdir(parents=True)
    atomic_write_parquet(
        pl.DataFrame(
            [
                {
                    "gap_id": "G_BATCH",
                    "document_id": None,
                    "action_id": None,
                    "city_id": "C_OTHER",
                    "severity": "HIGH",
                }
            ]
        ),
        batch_dir / "2016_930_GAP_REGISTER.parquet",
        {"test": "global-gap-authority"},
        key_columns=("gap_id",),
    )
    atomic_write_parquet(
        pl.DataFrame(
            [
                {
                    "episode_id": "EP_2016_930_TIGHTENING",
                    "gap_id": "G_CORE_GLOBAL",
                    "document_id": None,
                    "action_id": None,
                    "city_id": "C_CORE",
                    "severity": "HIGH",
                },
                {
                    "episode_id": "EP_2016_930_TIGHTENING",
                    "gap_id": "G_OTHER_GLOBAL",
                    "document_id": None,
                    "action_id": None,
                    "city_id": "C_OTHER",
                    "severity": "HIGH",
                },
            ]
        ),
        curated / "policy_episode_gaps.parquet",
        {"test": "global-gap-authority"},
        key_columns=("gap_id",),
    )

    snapshot = build_monitor_snapshot(output)

    assert snapshot["gap_authoritative_source"] == str(
        curated / "policy_episode_gaps.parquet"
    )
    assert snapshot["GLOBAL_EPISODE_PROGRESS"]["global_final_blocking_gaps"][
        "blocking_gap_count"
    ] == 2
    assert snapshot["GLOBAL_EPISODE_PROGRESS"]["analysis_ready_core_blocking_gaps"][
        "blocking_gap_count"
    ] == 1


def test_monitor_exposes_recent_recovery_claim_priorities_and_core_state(tmp_path: Path) -> None:
    output = tmp_path / "outputs" / "special_projects" / "2016_930"
    output.mkdir(parents=True)
    _write_json(
        output / "930_ANALYSIS_READY_SCOPE.json",
        {"queue_item_ids": ["Q0", "Q1", "Q2"], "scope_hash": "scope"},
    )
    atomic_write_parquet(
        pl.DataFrame(
            [
                {"queue_item_id": "Q0", "status": "RECOVERY_COMPLETED"},
                {"queue_item_id": "Q1", "status": "RUNNING"},
                {"queue_item_id": "Q2", "status": "RECOVERY_REQUIRED"},
            ]
        ),
        output / "930_FALSE_COMPLETION_RECOVERY_QUEUE.parquet",
        {"test": "claim_metrics"},
        key_columns=("queue_item_id",),
    )
    atomic_write_parquet(
        pl.DataFrame(
            [
                {"task_id": "Q0", "normalized_priority": 0, "claimed_at": "2026-08-14T01:00:00+00:00"},
                {"task_id": "Q3", "normalized_priority": 1, "claimed_at": "2026-08-14T02:00:00+00:00"},
                {"task_id": "Q4", "normalized_priority": 2, "claimed_at": "2026-08-14T03:00:00+00:00"},
            ]
        ),
        output / "930_RECOVERY_CLAIM_AUDIT.parquet",
        {"test": "claim_metrics"},
        key_columns=("task_id",),
    )

    metrics = recovery_claim_metrics(
        output,
        {
            "core_eligible_total": 3,
            "core_verified": 1,
            "core_coverage_percent": 33.33,
        },
    )

    assert metrics["recent_10_priority_counts"] == {"0": 1, "1": 1, "2": 1}
    assert metrics["core_verified"] == 1
    assert metrics["core_running"] == 1
    assert metrics["core_required"] == 1
    assert metrics["core_coverage"] == 33.33


def test_monitor_prefers_atomic_queue_counts_over_stale_progress_json(tmp_path: Path) -> None:
    output = tmp_path / "outputs" / "special_projects" / "2016_930"
    output.mkdir(parents=True)
    _write_json(
        output / "930_PROGRESS_SNAPSHOT.json",
        {"queue_total": 7, "queue_completed": 3, "queue_pending": 4, "queue_running": 1},
    )
    atomic_write_parquet(
        pl.DataFrame({"status": ["CRAWL_COMPLETED"] * 4 + ["PENDING"] * 2}),
        output / "930_TASK_QUEUE.parquet",
        {"test": "monitor_queue"},
        key_columns=(),
    )

    snapshot = build_monitor_snapshot(output)

    assert snapshot["queue_total"] == 6
    assert snapshot["queue_completed"] == 4
    assert snapshot["queue_pending"] == 2
    assert snapshot["queue_running"] == 0
    assert snapshot["queue_reconciliation"]["accounted_total"] == 6
    assert snapshot["queue_reconciliation"]["consistent"] is True


def test_queue_reconciliation_marks_terminal_lease_reference_stale(tmp_path: Path) -> None:
    output = tmp_path / "outputs" / "special_projects" / "2016_930"
    output.mkdir(parents=True)
    atomic_write_parquet(
        pl.DataFrame({
            "queue_item_id": ["Q1", "Q2", "Q3"],
            "status": ["CRAWL_COMPLETED", "PENDING", "PENDING"],
            "execution_status": ["TASK_COMPLETED", "PENDING", "PENDING"],
            "fetch_status": ["LIVE_FETCH_SUCCESS", "NOT_ATTEMPTED", "NOT_ATTEMPTED"],
            "lease_owner": [None, None, None],
            "lease_expires_at": [None, None, None],
        }),
        output / "930_TASK_QUEUE.parquet",
        {"test": "stale_lease"},
        key_columns=("queue_item_id",),
    )
    reconciliation = reconcile_queue(output, {"lease": {"queue_item_ids": ["Q1"]}})

    assert reconciliation["accounted_statuses"] == {
        "completed": 1,
        "retry": 0,
        "leased": 0,
        "active": 0,
        "pending": 2,
    }
    assert reconciliation["accounted_total"] == 3
    assert reconciliation["stale_completed_lease_references"] == 1
    assert reconciliation["lease_reference_items"][0]["reference_state"] == "STALE_COMPLETED_REFERENCE"


def test_queue_reconciliation_identifies_active_recovery_reference(tmp_path: Path) -> None:
    output = tmp_path / "outputs" / "special_projects" / "2016_930"
    output.mkdir(parents=True)
    atomic_write_parquet(
        pl.DataFrame({
            "queue_item_id": ["Q1", "Q2"],
            "status": ["CRAWL_COMPLETED", "PENDING"],
        }),
        output / "930_TASK_QUEUE.parquet",
        {"test": "active_historical_recovery"},
        key_columns=("queue_item_id",),
    )

    reconciliation = reconcile_queue(
        output,
        {"status": "RUNNING", "lease": {"queue_item_ids": ["Q1"]}},
    )

    assert reconciliation["accounted_total"] == 2
    assert reconciliation["active_terminal_history_references"] == 1
    assert reconciliation["stale_completed_lease_references"] == 0
    assert reconciliation["lease_reference_items"][0]["reference_state"] == "ACTIVE_RECOVERY_REFERENCE_TO_TERMINAL_HISTORY"


def test_api_health_marks_provider_stalled_after_fifteen_minutes(tmp_path: Path) -> None:
    output = tmp_path / "outputs" / "special_projects" / "2016_930"
    output.mkdir(parents=True)
    _write_json(output / "930_API_PROVIDER_STATUS.json", {
        "status": "TEMPORARY_PROVIDER_FAILURE",
        "last_success_at": "2020-01-01T00:00:00+00:00",
    })
    _write_json(output / "930_API_RECOVERY_STATE.json", {
        "phase": "PROBE",
        "last_attempted_documents": 1,
        "last_success_documents": 0,
        "last_attempt_at": "2020-01-01T00:00:00+00:00",
    })
    health = api_health(output, {"documents_found": 1, "api_pass1_success": 0, "api_pass2_success": 0}, {"document_versions": 5})

    assert health["pass1_waiting"] == 5
    assert health["pass2_success"] == 0
    assert health["pass2_eligible"] == 0
    assert health["pass2_waiting"] == 0
    assert health["pass2_not_yet_eligible"] == 5
    assert health["unprocessed_total"] == 5
    assert health["status"] == "STALLED"
    assert health["no_success_for_15m"] is True
    assert health["monitor_network_probe_executed"] is False


def test_api_health_exposes_scheduled_next_retry(tmp_path: Path) -> None:
    output = tmp_path / "outputs" / "special_projects" / "2016_930"
    output.mkdir(parents=True)
    _write_json(output / "930_API_PROVIDER_STATUS.json", {"status": "TEMPORARY_PROVIDER_FAILURE"})
    _write_json(
        output / "930_API_RECOVERY_STATE.json",
        {
            "phase": "BACKOFF_SINGLE_PROBE",
            "last_attempt_at": "2026-08-14T22:13:22.815896+00:00",
            "next_retry_at": "2026-08-14T22:43:22.815896+00:00",
        },
    )

    health = api_health(
        output,
        {"documents_found": 1, "api_pass1_success": 0, "api_pass2_success": 0},
        {"document_versions": 1},
    )

    assert health["next_retry_at"] == "2026-08-14T22:43:22.815896+00:00"


def test_api_health_flags_cache_reuse_retry_window_as_missed_probe(tmp_path: Path) -> None:
    output = tmp_path / "outputs" / "special_projects" / "2016_930"
    output.mkdir(parents=True)
    _write_json(output / "930_API_PROVIDER_STATUS.json", {"status": "OPERATIONAL"})
    _write_json(
        output / "930_API_RECOVERY_STATE.json",
        {
            "phase": "BACKOFF_SINGLE_PROBE",
            "reason_code": "CACHE_REUSE_NOT_A_PROVIDER_PROBE",
            "last_attempted_documents": 0,
            "provider_probe_attempted_documents": 0,
            "api_cache_hits": 1,
            "next_retry_at": "2026-08-15T02:24:34+00:00",
        },
    )

    health = api_health(
        output,
        {"documents_found": 1, "api_pass1_success": 0, "api_pass2_success": 0},
        {"document_versions": 1},
    )

    assert health["recovery_lane_missed_retry_window"] is True


def test_api_health_reports_core_backlog_with_pass2_dependency(tmp_path: Path) -> None:
    output = tmp_path / "outputs" / "special_projects" / "2016_930"
    output.mkdir(parents=True)
    _write_json(output / "930_API_PROVIDER_STATUS.json", {"status": "TEMPORARY_PROVIDER_FAILURE"})
    _write_json(output / "930_API_RECOVERY_STATE.json", {"phase": "BACKOFF_SINGLE_PROBE"})
    api_rows = pl.DataFrame(
        [
            {"document_id": "D_CORE_1", "action_id": "A1", "pass_name": "first_pass"},
            {"document_id": "D_GLOBAL", "action_id": "AG", "pass_name": "first_pass"},
        ]
    )

    health = api_health(
        output,
        {"documents_found": 3, "api_pass1_success": 1, "api_pass2_success": 0},
        {"document_versions": 3},
        core_document_ids={"D_CORE_1", "D_CORE_2"},
        api_rows=api_rows,
    )

    assert health["core_pass1_eligible"] == 2
    assert health["core_pass1_success"] == 1
    assert health["core_pass1_waiting"] == 1
    assert health["core_pass2_not_eligible"] == 1
    assert health["core_pass2_eligible"] == 1
    assert health["core_pass2_waiting"] == 1
    assert health["core_pass2_success"] == 0


def test_api_health_does_not_unlock_recovery_from_generic_provider_recovered(tmp_path: Path) -> None:
    output = tmp_path / "outputs" / "special_projects" / "2016_930"
    output.mkdir(parents=True)
    _write_json(output / "930_API_PROVIDER_STATUS.json", {"status": "RECOVERED"})
    _write_json(output / "930_API_RECOVERY_STATE.json", {
        "phase": "SINGLE_PROBE",
        "last_attempted_documents": 1,
        "last_success_documents": 0,
        "schema_valid": False,
    })

    health = api_health(
        output,
        {"documents_found": 1, "api_pass1_success": 0, "api_pass2_success": 0},
        {"document_versions": 1},
    )

    assert health["provider_status"] == "RECOVERED"
    # No recorded success timestamp is already beyond the 15-minute health
    # boundary; generic provider recovery must not hide the stalled backlog.
    assert health["status"] == "STALLED"
    assert health["recovery_gate_blocked"] is True
    assert health["recovery_gate"] == "BACKOFF_SINGLE_PROBE"
    assert health["backlog_consumption_allowed"] is False


def test_stage_progress_does_not_use_queue_completed_as_api_denominator(tmp_path: Path) -> None:
    progress = {
        "queue_total": 525,
        "documents_found": 5,
        "actions_extracted": 10,
        "api_pass1_success": 2,
        "api_pass2_success": 1,
        "dates_verified": 4,
        "parameters_extracted": 3,
        "formal_actions_promoted": 1,
        "gaps_remaining": 0,
    }
    stages = stage_progress(progress, {"search_calls": 10}, {"documents": 5}, tmp_path)

    assert stages["api_pass1"]["completed"] == 2
    assert stages["api_pass1"]["total"] == 5
    assert stages["api_pass1"]["total"] != 525


def test_action_export_is_action_level_and_provisional_until_gate(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    settings = Settings(
        root=tmp_path,
        data_root_path=data_root,
        curated_path=data_root / "curated",
        outputs_path=data_root / "outputs",
    )
    settings.curated.mkdir(parents=True)
    pipeline = Episode930Pipeline(
        settings,
        config=EpisodeConfig(run_search=False, run_ai=False, apply=True),
        output=data_root / "outputs" / "special_projects" / "2016_930",
    )
    documents = pl.DataFrame([{
        "document_id": "DOC1",
        "city": "City A",
        "province": "Province A",
        "document_title": "930 policy",
        "document_number": "NO-1",
        "issuer": "City A Government",
        "official_url": "https://www.gov.cn/doc1",
        "publication_date": "2016-10-01",
        "effective_date": "2016-10-01",
        "effective_date_basis": "PUBLICATION_DATE_EFFECTIVE",
        "date_evidence_text": "自发布之日起执行",
    }])
    actions = pl.DataFrame([{
        "episode_id": "EP_2016_930_TIGHTENING",
        "document_id": "DOC1",
        "action_id": "ACT1",
        "action_text": "提高首付比例",
        "policy_type": "COMMERCIAL_DOWNPAYMENT",
        "policy_subtype": None,
        "mechanism_labels": ["CREDIT_TIGHTENING"],
        "action_direction": "TIGHTENING",
        "announcement_date": "2016-10-01",
        "publication_date": "2016-10-01",
        "effective_date": "2016-10-01",
        "implementation_date": "2016-10-01",
        "date_confidence": "HIGH",
        "classification_confidence": 0.8,
        "episode_confidence": 0.9,
        "official_text_excerpt": "提高首付比例",
    }])
    params = pl.DataFrame([{
        "action_id": "ACT1",
        "parameter_name": "downpayment_ratio",
        "old_value": "30%",
        "new_value": "40%",
        "unit": "%",
    }])

    result = pipeline.final_export(
        documents,
        actions,
        params,
        pl.DataFrame(),
        pl.DataFrame(),
        pl.DataFrame(),
        {"analysis_ready": False},
    )
    exported = pl.read_csv(result["final_export"].replace(".xlsx", ".csv"))

    assert exported.height == 1
    assert exported.get_column("action_id").to_list() == ["ACT1"]
    assert exported.get_column("parameter_name").to_list() == ["downpayment_ratio"]
    assert exported.get_column("export_status").to_list() == ["PROVISIONAL"]
    assert "api_pass1_status" in exported.columns
