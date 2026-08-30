from __future__ import annotations

import json
from pathlib import Path

import polars as pl

import scripts.crpd_autonomous_controller as controller
from scripts.crpd_autonomous_controller import (
    Paths,
    command_specs,
    coverage_summary,
    crawl_shard_summary,
    crawl_stage_semantics,
    current_log_status,
    default_config,
    finalize_terminal_run,
    install,
    next_stage_after,
    progress_watchdog,
    redact,
    resume,
    stage_has_structured_validation_warning,
    stop,
    supervisor_decision,
    write_progress_snapshot,
)


def make_paths(tmp_path: Path) -> tuple[Paths, dict]:
    project = tmp_path / "project"
    data = tmp_path / "data"
    (project / ".venv" / "Scripts").mkdir(parents=True)
    (data / "database").mkdir(parents=True)
    config_path = project / "config.json"
    paths = Paths(project=project, data=data, config=config_path)
    config = default_config(project, data)
    config_path.write_text(json.dumps(config), encoding="utf-8")
    return paths, config


def test_install_stop_and_resume_are_persistent(tmp_path: Path) -> None:
    paths, config = make_paths(tmp_path)

    assert install(paths, config) == 0
    assert paths.master.exists()
    assert paths.lock.exists()
    assert stop(paths, "test") == 0
    assert paths.stop.exists()
    assert resume(paths) == 0
    assert not paths.stop.exists()


def test_coverage_requires_explicit_saturation_gate(tmp_path: Path) -> None:
    paths, config = make_paths(tmp_path)
    install(paths, config)
    summary = paths.data / "outputs" / "coverage" / "LATEST_COVERAGE_SUMMARY.json"
    summary.parent.mkdir(parents=True, exist_ok=True)

    summary.write_text(json.dumps({"city_count": 105}), encoding="utf-8")
    assert coverage_summary(paths)["saturated"] is False
    summary.write_text(json.dumps({"WEB_CRAWL_SATURATED": True}), encoding="utf-8")
    assert coverage_summary(paths)["saturated"] is True


def test_stage_machine_and_redaction() -> None:
    assert next_stage_after("COVERAGE_AUDIT", False) == "RECOVER_MISSING"
    assert next_stage_after("COVERAGE_AUDIT", True) == "PDF_STAGE"
    assert next_stage_after("COVERAGE_AUDIT", False, False, recent_complete=False) == "RECENT_30D_PRIORITY"
    assert next_stage_after("RECENT_30D_PRIORITY", False, True, recent_complete=False) == "RECENT_30D_PRIORITY"
    assert next_stage_after("RECENT_30D_PRIORITY", False, True, recent_complete=True) == "RECENT_COVERAGE_AUDIT"
    assert "secret-value" not in redact("Authorization: Bearer secret-value")


def test_legacy_handoff_distinguishes_terminal_failure_from_pending_summary(tmp_path: Path) -> None:
    paths, config = make_paths(tmp_path)
    install(paths, config)
    run_dir = paths.data / "logs" / "audited_full_backfill" / "legacy-run"
    run_dir.mkdir(parents=True)
    master = run_dir / "master.log"
    checkpoint = paths.data / "curated" / "crawl_shards.parquet"
    checkpoint.parent.mkdir(parents=True, exist_ok=True)
    checkpoint.write_bytes(b"checkpoint")

    master.write_text("[ERROR] 阶段失败（exit=1）：crawl_CITY_320100\n", encoding="utf-8")
    failed = current_log_status(paths)
    assert failed["status"] == "TERMINAL_FAILED"
    assert failed["safe_ended"] is True
    assert failed["reason"] == "LEGACY_RUN_TERMINAL_FAILED"

    master.write_text("阶段执行中，尚无总结标记\n", encoding="utf-8")
    pending = current_log_status(paths)
    assert pending["status"] == "TRUE_SUMMARY_PENDING"
    assert pending["safe_ended"] is False

    master.write_text("补扫流程结束\n", encoding="utf-8")
    successful = current_log_status(paths)
    assert successful["status"] == "SUCCESSFUL_END"
    assert successful["safe_ended"] is True

    active = current_log_status(paths, active=True)
    assert active["status"] == "ACTIVE"
    assert active["safe_ended"] is False


def test_terminal_failed_handoff_starts_a_fresh_run_id(tmp_path: Path, monkeypatch) -> None:
    paths, config = make_paths(tmp_path)
    install(paths, config)
    paths.master.write_text(
        json.dumps(
            {
                "automation_id": "AUTO_TEST",
                "status": "RETRY_WAIT",
                "stage": "CRAWL",
                "next_stage": "CRAWL",
                "run_id": "RUN_OLD",
            }
        ),
        encoding="utf-8",
    )
    empty_processes = {
        "current_backfill": [],
        "legacy_supervisor": [],
        "external_writers": [],
        "autonomous_workers": [],
    }
    monkeypatch.setattr(controller, "process_snapshot", lambda: empty_processes)
    monkeypatch.setattr(controller, "disk_status", lambda *_args: {"status": "OK"})
    monkeypatch.setattr(
        controller,
        "current_log_status",
        lambda *_args, **_kwargs: {
            "status": "TERMINAL_FAILED",
            "safe_ended": True,
            "reason": "LEGACY_RUN_TERMINAL_FAILED",
        },
    )
    monkeypatch.setattr(controller, "run_id", lambda *_args: "RUN_NEW")

    class FakeLock:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    monkeypatch.setattr(controller, "FileLock", lambda *_args, **_kwargs: FakeLock())
    seen = {}

    class FakeProcess:
        pid = 4321

    def fake_popen(command, **_kwargs):
        seen["command"] = command
        return FakeProcess()

    monkeypatch.setattr(controller.subprocess, "Popen", fake_popen)

    assert supervisor_decision(paths, config) == 0
    state = json.loads(paths.master.read_text(encoding="utf-8"))
    assert state["run_id"] == "RUN_NEW"
    assert "--run-id" in seen["command"]
    assert seen["command"][seen["command"].index("--run-id") + 1] == "RUN_NEW"
    assert state["last_error"] is None


def test_safe_handoff_routes_failed_historical_run_to_coverage_audit(tmp_path: Path, monkeypatch) -> None:
    paths, config = make_paths(tmp_path)
    install(paths, config)
    paths.master.write_text(
        json.dumps(
            {
                "automation_id": "AUTO_TEST",
                "status": "RETRY_WAIT",
                "stage": "CRAWL_AGAIN",
                "next_stage": "CRAWL_AGAIN",
                "run_id": "RUN_OLD",
            }
        ),
        encoding="utf-8",
    )
    (paths.automation / "SAFE_HANDOFF_ROUTE_20260811T1710Z.json").write_text(
        json.dumps(
            {
                "status": "REQUESTED",
                "route_to": "COVERAGE_AUDIT",
                "reason": "SAFE_HANDOFF_AFTER_DURABLE_SHARD_CHECKPOINT",
                "source_run_id": "RUN_OLD",
            }
        ),
        encoding="utf-8",
    )
    empty_processes = {
        "current_backfill": [],
        "legacy_supervisor": [],
        "external_writers": [],
        "autonomous_workers": [],
    }
    monkeypatch.setattr(controller, "process_snapshot", lambda: empty_processes)
    monkeypatch.setattr(controller, "disk_status", lambda *_args: {"status": "OK"})
    monkeypatch.setattr(
        controller,
        "current_log_status",
        lambda *_args, **_kwargs: {"status": "TERMINAL_FAILED", "safe_ended": True},
    )
    monkeypatch.setattr(controller, "run_id", lambda *_args: "RUN_HANDOFF")

    class FakeLock:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    monkeypatch.setattr(controller, "FileLock", lambda *_args, **_kwargs: FakeLock())
    seen = {}

    class FakeProcess:
        pid = 4321

    def fake_popen(command, **_kwargs):
        seen["command"] = command
        return FakeProcess()

    monkeypatch.setattr(controller.subprocess, "Popen", fake_popen)

    assert supervisor_decision(paths, config) == 0
    state = json.loads(paths.master.read_text(encoding="utf-8"))
    assert state["stage"] == "COVERAGE_AUDIT"
    assert state["run_id"] == "RUN_HANDOFF"
    assert seen["command"][seen["command"].index("--stage") + 1] == "COVERAGE_AUDIT"
    assert controller.latest_safe_handoff(paths) is None


def test_stale_recent_run_is_terminally_reconciled_and_resumed(tmp_path: Path, monkeypatch) -> None:
    paths, config = make_paths(tmp_path)
    install(paths, config)
    paths.master.write_text(
        json.dumps(
            {
                "automation_id": "AUTO_TEST",
                "status": "WAIT_CURRENT_RUN",
                "stage": "WAIT_CURRENT_RUN",
                "next_stage": "RECENT_30D_PRIORITY",
                "run_id": "RUN_RECENT_OLD",
            }
        ),
        encoding="utf-8",
    )
    paths.current_run.write_text(
        json.dumps(
            {
                "run_id": "RUN_RECENT_OLD",
                "stage": "RECENT_30D_PRIORITY",
                "pid": 99999,
                "status": "RUNNING",
            }
        ),
        encoding="utf-8",
    )
    (paths.automation / "RECENT_30D_STATE.json").write_text(
        json.dumps({"status": "RUNNING", "current_item": "ITEM_1"}),
        encoding="utf-8",
    )
    empty_processes = {
        "current_backfill": [],
        "legacy_supervisor": [],
        "external_writers": [],
        "autonomous_workers": [],
    }
    monkeypatch.setattr(controller, "process_snapshot", lambda: empty_processes)
    monkeypatch.setattr(controller, "disk_status", lambda *_args: {"status": "OK"})
    monkeypatch.setattr(
        controller,
        "current_log_status",
        lambda *_args, **_kwargs: {"status": "TRUE_SUMMARY_PENDING", "safe_ended": False},
    )
    monkeypatch.setattr(controller, "run_id", lambda *_args: "RUN_RECENT_NEW")

    class FakeLock:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    monkeypatch.setattr(controller, "FileLock", lambda *_args, **_kwargs: FakeLock())
    seen = {}

    class FakeProcess:
        pid = 4321

    def fake_popen(command, **_kwargs):
        seen["command"] = command
        return FakeProcess()

    monkeypatch.setattr(controller.subprocess, "Popen", fake_popen)

    assert supervisor_decision(paths, config) == 0
    current = json.loads(paths.current_run.read_text(encoding="utf-8"))
    assert current["status"] == "TERMINATED"
    assert current["terminal_reason"] == "WORKER_PROCESS_MISSING"
    assert seen["command"][seen["command"].index("--stage") + 1] == "RECENT_30D_PRIORITY"
    assert seen["command"][seen["command"].index("--run-id") + 1] == "RUN_RECENT_NEW"
    assert controller.latest_safe_handoff(paths) is None


def test_incomplete_validation_report_is_a_warning_not_a_command_failure(tmp_path: Path) -> None:
    paths, config = make_paths(tmp_path)
    install(paths, config)
    log = paths.supervisor_logs / "RUN_TEST_NORMALIZE_2.log"
    log.write_text(
        """[2026-08-10T00:00:00Z] COMMAND validate --group all
{
  "validation_group": "all",
  "record_count": 10,
  "passed": false,
  "v2_group_results": {"coverage": false, "release": false}
}
[2026-08-10T00:00:01Z] EXIT_CODE 1
""",
        encoding="utf-8",
    )
    assert stage_has_structured_validation_warning(paths, "RUN_TEST", "NORMALIZE_2") is True


def test_crawl_again_uses_existing_audited_backfill_script(tmp_path: Path) -> None:
    paths, config = make_paths(tmp_path)
    install(paths, config)
    (paths.project / ".venv" / "Scripts" / "python.exe").touch()
    (paths.project / ".venv" / "Scripts" / "policydb.exe").touch()
    script = paths.project / "scripts" / "CRPD_Audited_Full_Backfill.ps1"
    script.parent.mkdir(parents=True)
    script.touch()

    specs = command_specs(paths, config, "CRAWL_AGAIN", "RUN_TEST")
    assert len(specs) == 1
    command = specs[0]
    assert "CRPD_Audited_Full_Backfill.ps1" in command[command.index("-File") + 1]
    assert "-ExistingSourcesOnly" in command
    assert "-SkipAI" in command
    assert command_specs(paths, config, "RECOVER_MISSING", "RUN_TEST")[0][2:] == [
        "recover-missing",
        "--max-fetches",
        "20",
    ]
    recent_command = command_specs(paths, config, "RECENT_30D_PRIORITY", "RUN_TEST")[0]
    assert recent_command[1:5] == ["-m", "policydb.autopilot_cli", "recent-30d", "run"]
    assert "--apply" in recent_command
    assert "--resume" in recent_command


def test_recovery_zero_work_is_blocked_when_pending_shards_remain(tmp_path: Path) -> None:
    paths, config = make_paths(tmp_path)
    install(paths, config)
    shard_path = paths.data / "curated" / "crawl_shards.parquet"
    shard_path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame([{"shard_id": "S1", "status": "pending"}]).write_parquet(shard_path)
    log = paths.supervisor_logs / "RUN_TEST_RECOVER_MISSING_1.log"
    log.write_text(
        '{"metrics":{"source_count":0,"candidate_count":0,"fetched":0,"failed":0,"document_versions":0},"warning":true}\n',
        encoding="utf-8",
    )
    before_shards = crawl_shard_summary(paths)
    before_progress = {"status": "MISSING", "rows": None, "latest_created_at": None}
    ok, reason, details = crawl_stage_semantics(
        paths,
        run="RUN_TEST",
        stage="RECOVER_MISSING",
        command_stage="RECOVER_MISSING_1",
        argv=["policydb.exe", "crawl", "recover-missing"],
        code=0,
        before_shards=before_shards,
        before_progress=before_progress,
    )
    assert ok is False
    assert reason == "NO_PROGRESS_PENDING_WORK"
    assert details["shards_after"]["pending"] == 1


def test_coverage_audit_routes_to_real_crawl_when_shards_are_pending() -> None:
    assert next_stage_after("COVERAGE_AUDIT", False, True) == "CRAWL_AGAIN"
    assert next_stage_after("COVERAGE_AUDIT", False, False) == "RECOVER_MISSING"


def test_recent_completion_routes_through_coverage_audit_before_rolling() -> None:
    assert next_stage_after(
        "RECENT_30D_PRIORITY",
        False,
        False,
        recent_complete=True,
        rolling_enabled=True,
    ) == "RECENT_COVERAGE_AUDIT"
    assert next_stage_after(
        "RECENT_COVERAGE_AUDIT",
        False,
        False,
        recent_complete=True,
        rolling_enabled=True,
    ) == "ROLLING_24M_FULL_CITY_BACKFILL"


def test_terminal_run_finalize_is_idempotent(tmp_path: Path) -> None:
    paths, config = make_paths(tmp_path)
    install(paths, config)
    paths.current_run.write_text(
        json.dumps(
            {
                "run_id": "RUN_TERMINAL",
                "stage": "RECENT_30D_PRIORITY",
                "status": "COMPLETED",
                "reason": "SUCCESS",
                "completed_at": "2026-08-12T14:37:03Z",
                "pid": 99999,
            }
        ),
        encoding="utf-8",
    )
    (paths.automation / "RECENT_30D_STATE.json").write_text(
        json.dumps({"status": "PARTIAL", "queue_size": 4}), encoding="utf-8"
    )
    processes = {
        "current_backfill": [],
        "legacy_supervisor": [],
        "external_writers": [],
        "autonomous_workers": [],
    }

    first = finalize_terminal_run(paths, processes, config)
    second = finalize_terminal_run(paths, processes, config)
    assert first is not None
    assert second is not None
    assert first["handoff_id"] == second["handoff_id"]
    assert first["route_to"] == "RECENT_30D_PRIORITY"
    assert len(list(paths.automation.glob("TERMINAL_HANDOFF_*.json"))) == 1
    state = json.loads(paths.master.read_text(encoding="utf-8"))
    assert state["next_stage"] == "RECENT_30D_PRIORITY"


def test_progress_snapshot_is_atomic_and_separates_stage_queues(tmp_path: Path) -> None:
    paths, config = make_paths(tmp_path)
    install(paths, config)
    recent = paths.data / "outputs" / "recent_30d"
    rolling = paths.data / "outputs" / "rolling_24m"
    recent.mkdir(parents=True, exist_ok=True)
    rolling.mkdir(parents=True, exist_ok=True)
    pl.DataFrame([{"item_id": "R1", "status": "SUCCESS", "attempts": 1, "city_id": "C1"}]).write_parquet(recent / "RECENT_30D_QUEUE.parquet")
    pl.DataFrame([{"queue_item_id": "Q1", "status": "PENDING", "attempt_count": 0, "city_id": "C1"}]).write_parquet(rolling / "ROLLING_24M_QUEUE.parquet")
    snapshot = write_progress_snapshot(paths, state={"status": "RUNNING", "stage": "RECENT_30D_PRIORITY", "last_heartbeat_at": "now"}, force=True)
    assert snapshot["recent_30d"]["completed"] == 1
    assert snapshot["rolling_24m"]["pending"] == 1
    assert json.loads((paths.automation / "PROGRESS_SNAPSHOT.json").read_text(encoding="utf-8"))["stage"] == "RECENT_30D_PRIORITY"


def test_progress_watchdog_separates_stale_business_progress_from_live_worker(tmp_path: Path) -> None:
    paths, config = make_paths(tmp_path)
    install(paths, config)
    (paths.automation / "MASTER_STATE.json").write_text(
        json.dumps({"stage": "RECENT_30D_PRIORITY", "last_heartbeat_at": "2099-01-01T00:00:00Z"}),
        encoding="utf-8",
    )
    (paths.automation / "RECENT_30D_STATE.json").write_text(
        json.dumps({"last_real_progress_at": "2020-01-01T00:00:00Z", "updated_at": "2020-01-01T00:00:00Z"}),
        encoding="utf-8",
    )
    result = progress_watchdog(
        paths,
        {"watchdog": {"no_progress_minutes": 1}},
        {"autonomous_workers": [{"pid": 123}], "current_backfill": [], "legacy_supervisor": [], "external_writers": []},
    )
    assert result["status"] == "NO_REAL_PROGRESS"
    assert result["active_worker"] is True
    assert result["safe_recovery"] == "deferred_until_no_active_worker"
    assert json.loads((paths.automation / "PROGRESS_WATCHDOG.json").read_text(encoding="utf-8"))["status"] == "NO_REAL_PROGRESS"
