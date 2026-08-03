"""Regression tests for the Windows scheduled-task reliability boundary."""

from __future__ import annotations

import importlib.util
import json
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MONITOR_PATH = ROOT / "scripts" / "monitor_all_cities_since_2018.py"
_SPEC = importlib.util.spec_from_file_location("crpd_task_monitor", MONITOR_PATH)
assert _SPEC and _SPEC.loader
monitor = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(monitor)


def _write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False), encoding="utf-8")


def _automation(tmp_path: Path, *, status: str = "RUNNING", pid: int | None = 99999) -> Path:
    automation = tmp_path / "AUTO_existing"
    cycle = automation / "cycle_0001"
    cycle.mkdir(parents=True)
    now = datetime.now(UTC).isoformat()
    _write_json(
        automation / "automation_state.json",
        {
            "automation_id": "AUTO_existing",
            "status": status,
            "runner_pid": pid,
            "current_cycle": "cycle_0001",
            "current_cycle_dir": str(cycle),
            "current_run_id": "AUTO_existing_cycle_0001",
            "last_progress_at": now,
            "last_heartbeat_at": now,
            "stalled_after_hours": 6,
            "total_slots": 525,
        },
    )
    _write_json(
        cycle / "current_status.json",
        {
            "run_id": "AUTO_existing_cycle_0001",
            "status": "SOURCE_COMPLETION",
            "global_status": "SOURCE_COMPLETION",
            "last_progress_at": now,
            "last_heartbeat_at": now,
            "verified": 6,
            "enabled": 6,
        },
    )
    _write_json(
        cycle / "database_sync_status.json",
        {
            "total_slots": 525,
            "resolved_slots": 69,
            "verified_slots": 6,
            "enabled_slots": 6,
            "backfilled_slots": 5,
            "unresolved_slots": 456,
            "total_documents": 1898,
            "critical_gaps": 0,
            "consistency_errors": [],
        },
    )
    _write_json(cycle / "budget_usage.json", {"used": {}, "limits": {"http_calls": 50000}})
    return automation


def test_ready_task_and_missing_old_pid_are_blocked(monkeypatch, tmp_path: Path) -> None:
    automation = _automation(tmp_path)
    monkeypatch.setattr(monitor, "STOP_FILE", tmp_path / "STOP_FULL_SYNC")

    health = monitor.build_health(
        automation,
        task_info={"state": "Ready", "last_task_result": 3221225786},
        processes=[],
    )

    assert health["declared_status"] == "RUNNING"
    assert health["effective_status"] == "STOPPED_UNEXPECTEDLY"
    assert health["scheduled_task_state"] == "Ready"
    assert health["runner_pid_actual"] is None
    assert health["runner_process_running"] is False
    assert health["orphaned_runner"] is True
    assert health["last_task_result_hex"] == "0xC000013A"
    assert health["health_gate"] == "BLOCKED"


def test_live_runner_and_worker_are_resolved_dynamically(monkeypatch, tmp_path: Path) -> None:
    automation = _automation(tmp_path)
    monkeypatch.setattr(monitor, "STOP_FILE", tmp_path / "STOP_FULL_SYNC")
    processes = [
        {
            "ProcessId": 501,
            "Name": "powershell.exe",
            "CommandLine": "powershell.exe -File run_all_cities_since_2018.ps1",
        },
        {
            "ProcessId": 502,
            "Name": "python.exe",
            "CommandLine": "python.exe -m policydb.autopilot_cli full-sync resume",
        },
    ]

    health = monitor.build_health(
        automation,
        task_info={"state": "Running", "last_task_result": 0},
        processes=processes,
    )

    assert health["effective_status"] == "RUNNING"
    assert health["runner_pid_recorded"] == 99999
    assert health["runner_pid_actual"] == 501
    assert health["runner_process_running"] is True
    assert health["worker_process_count"] == 1
    assert health["health_gate"] == "PASS"


def test_stop_request_is_distinct_from_unexpected_stop(monkeypatch, tmp_path: Path) -> None:
    automation = _automation(tmp_path)
    stop_file = tmp_path / "STOP_FULL_SYNC"
    stop_file.write_text("operator_stop_request\n", encoding="utf-8")
    monkeypatch.setattr(monitor, "STOP_FILE", stop_file)

    health = monitor.build_health(
        automation,
        task_info={"state": "Ready", "last_task_result": 21},
        processes=[],
    )

    assert health["effective_status"] == "STOPPED_REQUESTED"
    assert health["orphaned_runner"] is False
    assert health["restart_required"] is False
    assert health["health_gate"] == "BLOCKED"


def test_exit_record_is_reported_and_status_is_not_running(monkeypatch, tmp_path: Path) -> None:
    automation = _automation(tmp_path, status="FAILED", pid=None)
    _write_json(
        automation / "runner_exit.json",
        {
            "exit_code": 1,
            "exit_reason": "unhandled_exception",
            "exit_time": "2026-08-03T02:10:00+00:00",
        },
    )
    monkeypatch.setattr(monitor, "STOP_FILE", tmp_path / "STOP_FULL_SYNC")

    health = monitor.build_health(
        automation,
        task_info={"state": "Ready", "last_task_result": 1},
        processes=[],
    )

    assert health["declared_status"] == "FAILED"
    assert health["effective_status"] == "FAILED"
    assert health["last_exit_reason"] == "unhandled_exception"
    assert health["health_gate"] == "BLOCKED"


def test_start_script_uses_scheduler_and_not_a_foreground_runner() -> None:
    start_script = (ROOT / "scripts" / "start_all_cities_task.ps1").read_text(encoding="utf-8")

    assert "Start-ScheduledTask" in start_script
    assert "Start-Process" not in start_script
    assert "-NonInteractive" in start_script
    assert "<StopOnIdleEnd>true</StopOnIdleEnd>" in start_script


def test_runner_contains_resume_and_exit_evidence_contract() -> None:
    runner = (ROOT / "scripts" / "run_all_cities_since_2018.ps1").read_text(encoding="utf-8")

    assert "cycle_resumed" in runner
    assert '"--resume"' in runner
    assert "runner_exit.json" in runner
    assert "current_status.json" in runner
    assert "external_termination_or_process_loss" in runner
    assert "$ErrorActionPreference = \"Stop\"" in runner


def test_deployment_scripts_set_utf8_output() -> None:
    for name in (
        "run_all_cities_since_2018.ps1",
        "start_all_cities_task.ps1",
        "stop_all_cities_task.ps1",
        "check_all_cities_task.ps1",
    ):
        content = (ROOT / "scripts" / name).read_text(encoding="utf-8")
        assert "$OutputEncoding = $Utf8" in content
