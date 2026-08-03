"""Read-only health monitor for the independent CRPD all-city task."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_ROOT = Path(os.environ.get("CRPD_DATA_ROOT", r"D:\Data Set\CRPD"))
AUTOMATION_ROOT = DATA_ROOT / "outputs" / "all_cities_since_2018"
STOP_FILE = DATA_ROOT / "control" / "STOP_FULL_SYNC"
TASK_NAME = "CRPD-All-Cities-Since-2018"
ACTIVE_DECLARED_STATUSES = {
    "STARTING",
    "RUNNING",
    "SOURCE_COMPLETION",
    "DISCOVERING",
    "CRAWLING",
    "BACKFILLING",
    "INCREMENTAL",
    "REPAIRING",
    "PAUSED_BUDGET",
}


def read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        value = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def atomic_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{os.urandom(4).hex()}.tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def parse_timestamp(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def number(value: Any, default: int | float = 0) -> int | float:
    try:
        return float(value) if isinstance(value, float) else int(value)
    except (TypeError, ValueError):
        return default


def process_running(pid: Any) -> bool:
    try:
        pid_value = int(pid)
    except (TypeError, ValueError):
        return False
    if pid_value <= 0:
        return False
    try:
        completed = subprocess.run(
            ["tasklist.exe", "/FI", f"PID eq {pid_value}", "/FO", "CSV", "/NH"],
            capture_output=True,
            text=True,
            check=False,
            timeout=5,
        )
    except (OSError, subprocess.SubprocessError):
        return False
    return str(pid_value) in completed.stdout


def _powershell_json(script: str, *, timeout: int = 10) -> Any:
    """Run a read-only Windows query without inheriting a user profile."""

    utf8_setup = (
        "[Console]::OutputEncoding=[System.Text.Encoding]::UTF8;"
        "$OutputEncoding=[System.Text.Encoding]::UTF8;"
    )
    try:
        completed = subprocess.run(
            [
                "powershell.exe",
                "-NoLogo",
                "-NoProfile",
                "-NonInteractive",
                "-ExecutionPolicy",
                "Bypass",
                "-Command",
                utf8_setup + script,
            ],
            capture_output=True,
            text=False,
            check=False,
            timeout=timeout,
        )
    except (OSError, subprocess.SubprocessError):
        return {}
    raw_stdout = completed.stdout or b""
    if completed.returncode != 0 or not raw_stdout.strip():
        return {}
    for encoding in ("utf-8-sig", "utf-16", "gb18030"):
        try:
            decoded = raw_stdout.decode(encoding)
            return json.loads(decoded)
        except (UnicodeDecodeError, json.JSONDecodeError):
            continue
    return {}


def query_scheduled_task(task_name: str = TASK_NAME) -> dict[str, Any]:
    """Return live Task Scheduler state; never infer it from the checkpoint."""

    escaped_name = task_name.replace("'", "''")
    script = (
        "$task=Get-ScheduledTask -TaskName '"
        + escaped_name
        + "' -ErrorAction Stop;"
        "$info=Get-ScheduledTaskInfo -TaskName '"
        + escaped_name
        + "' -ErrorAction Stop;"
        "[ordered]@{state=[string]$task.State;last_task_result=$info.LastTaskResult;"
        "last_run_time=$info.LastRunTime;next_run_time=$info.NextRunTime}|"
        "ConvertTo-Json -Compress"
    )
    value = _powershell_json(script)
    return value if isinstance(value, dict) else {}


def query_processes() -> list[dict[str, Any]]:
    """Return process command lines so runner and worker PIDs are resolved dynamically."""

    script = (
        "$items=Get-CimInstance Win32_Process -ErrorAction SilentlyContinue | "
        "Select-Object ProcessId,ParentProcessId,Name,ExecutablePath,CommandLine,CreationDate;"
        "@($items)|ConvertTo-Json -Compress -Depth 4"
    )
    value = _powershell_json(script)
    if isinstance(value, dict):
        return [value]
    return [item for item in value if isinstance(item, dict)] if isinstance(value, list) else []


def identify_processes(processes: list[dict[str, Any]]) -> tuple[list[int], list[int]]:
    """Return (runner PIDs, Python worker PIDs) from live command lines."""

    runner_pids: list[int] = []
    worker_pids: list[int] = []
    for process in processes:
        try:
            pid = int(process.get("ProcessId"))
        except (TypeError, ValueError):
            continue
        command_line = str(process.get("CommandLine") or "").lower()
        name = str(process.get("Name") or "").lower()
        if "run_all_cities_since_2018.ps1" in command_line:
            runner_pids.append(pid)
        if name in {"python.exe", "pythonw.exe", "pwsh.exe"} and (
            "policydb.autopilot_cli" in command_line
            or "autopilot_cli.py" in command_line
        ):
            worker_pids.append(pid)
    return sorted(set(runner_pids)), sorted(set(worker_pids))


def latest_automation(root: Path) -> Path | None:
    candidates = [path for path in root.iterdir()] if root.exists() else []
    candidates = [path for path in candidates if path.is_dir() and (path / "automation_state.json").exists()]
    return max(candidates, key=lambda path: path.stat().st_mtime) if candidates else None


def latest_cycle(automation_dir: Path, state: dict[str, Any]) -> Path | None:
    explicit = state.get("current_cycle_dir")
    if explicit and Path(str(explicit)).exists():
        return Path(str(explicit))
    cycles = sorted(path for path in automation_dir.glob("cycle_*") if path.is_dir())
    return cycles[-1] if cycles else None


def _task_result_hex(value: Any) -> str | None:
    try:
        return f"0x{int(value) & 0xFFFFFFFF:08X}"
    except (TypeError, ValueError):
        return None


def _file_timestamp(path: Path) -> str | None:
    try:
        return datetime.fromtimestamp(path.stat().st_mtime, tz=UTC).isoformat()
    except OSError:
        return None


def build_health(
    automation_dir: Path,
    *,
    task_info: dict[str, Any] | None = None,
    processes: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    state = read_json(automation_dir / "automation_state.json")
    cycle_dir = latest_cycle(automation_dir, state)
    current = read_json(cycle_dir / "current_status.json") if cycle_dir else {}
    database = read_json(cycle_dir / "database_sync_status.json") if cycle_dir else {}
    budget = read_json(cycle_dir / "budget_usage.json") if cycle_dir else {}
    provider = read_json(cycle_dir / "provider_health.json") if cycle_dir else {}
    runner_exit = read_json(automation_dir / "runner_exit.json")
    used = budget.get("used") if isinstance(budget.get("used"), dict) else {}
    limits = budget.get("limits") if isinstance(budget.get("limits"), dict) else {}
    progress_value = current.get("last_progress_at") or state.get("last_progress_at")
    progress_at = parse_timestamp(progress_value)
    stalled_after_hours = float(state.get("stalled_after_hours") or 6)
    age_hours = (datetime.now(UTC) - progress_at).total_seconds() / 3600 if progress_at else None
    task = task_info if task_info is not None else query_scheduled_task()
    live_processes = processes if processes is not None else query_processes()
    runner_pids, worker_pids = identify_processes(live_processes)
    runner_pid_recorded = state.get("runner_pid")
    runner_pid_actual = runner_pids[0] if runner_pids else None
    running = runner_pid_actual is not None
    task_state = str(task.get("state") or "UNKNOWN")
    task_is_running = task_state.lower() == "running"
    task_result = task.get("last_task_result")
    stop_requested = STOP_FILE.exists()
    declared_status = str(state.get("status") or current.get("status") or "UNKNOWN")
    stalled = bool(
        not stop_requested
        and running
        and age_hours is not None
        and age_hours > stalled_after_hours
        and declared_status not in {"COMPLETED", "FAILED", "STOPPED"}
    )
    consistency_errors = database.get("consistency_errors") or []
    current_status_path = cycle_dir / "current_status.json" if cycle_dir else None
    last_status_update_at = _file_timestamp(current_status_path) if current_status_path else None
    status_age_seconds = None
    if last_status_update_at:
        status_timestamp = parse_timestamp(last_status_update_at)
        if status_timestamp:
            status_age_seconds = max(0.0, (datetime.now(UTC) - status_timestamp).total_seconds())

    unexpected_stop = bool(
        not stop_requested
        and declared_status in ACTIVE_DECLARED_STATUSES
        and not task_is_running
        and not running
    )
    task_missing = not task
    if stop_requested:
        effective_status = "STOPPED_REQUESTED"
    elif unexpected_stop:
        effective_status = "STOPPED_UNEXPECTEDLY"
    elif task_missing:
        effective_status = "TASK_NOT_FOUND"
    elif task_is_running and running:
        effective_status = "RUNNING"
    elif task_is_running and not running:
        effective_status = "RUNNER_MISSING"
    elif declared_status in {"COMPLETED", "FAILED", "STOPPED", "STALLED", "PAUSED_AFTER_CYCLE"}:
        effective_status = declared_status
    else:
        effective_status = "STOPPED"

    current_heartbeat = current.get("last_heartbeat_at") or state.get("last_heartbeat_at")
    heartbeat_at = parse_timestamp(current_heartbeat)
    heartbeat_age_seconds = (
        max(0.0, (datetime.now(UTC) - heartbeat_at).total_seconds()) if heartbeat_at else None
    )
    health_blocked = bool(
        stop_requested
        or stalled
        or unexpected_stop
        or task_missing
        or (task_is_running and not running)
        or consistency_errors
        or number(database.get("critical_gaps")) > 0
        or effective_status not in {"RUNNING", "COMPLETED"}
    )
    health = {
        "generated_at": datetime.now(UTC).isoformat(),
        "task_name": TASK_NAME,
        "automation_id": state.get("automation_id") or automation_dir.name,
        "automation_dir": str(automation_dir),
        "cycle": cycle_dir.name if cycle_dir else None,
        "cycle_dir": str(cycle_dir) if cycle_dir else None,
        "run_id": current.get("run_id") or state.get("current_run_id"),
        "runner_pid": runner_pid_actual or runner_pid_recorded,
        "runner_pid_recorded": runner_pid_recorded,
        "runner_pid_actual": runner_pid_actual,
        "runner_process_running": running,
        "worker_process_count": len(worker_pids),
        "worker_pids": worker_pids,
        "declared_status": declared_status,
        "effective_status": effective_status,
        "scheduled_task_state": task_state,
        "last_task_result_decimal": number(task_result, 0) if task_result is not None else None,
        "last_task_result_hex": _task_result_hex(task_result),
        "status": declared_status,
        "global_status": database.get("global_status") or current.get("global_status"),
        "slots": {
            "total": number(database.get("total_slots"), 525),
            "resolved": number(database.get("resolved_slots")),
            "verified": number(database.get("verified_slots") or current.get("verified")),
            "enabled": number(database.get("enabled_slots") or current.get("enabled")),
            "backfilled": number(database.get("backfilled_slots")),
            "current": number(database.get("current_slots")),
            "unresolved": number(database.get("unresolved_slots") or current.get("unresolved")),
        },
        "documents": {
            "total": number(database.get("total_documents")),
            "added_last_run": number(database.get("documents_added_last_run")),
            "updated_last_run": number(database.get("documents_updated_last_run")),
        },
        "gaps": {
            "open": number(database.get("open_gaps")),
            "critical": number(database.get("critical_gaps")),
        },
        "ai": {
            "provider": state.get("provider"),
            "model": state.get("model"),
            "calls": number(current.get("ai_calls") or used.get("ai_calls")),
            "tokens": current.get("tokens", used.get("tokens")),
            "estimated_cost_usd": current.get("estimated_cost_usd", state.get("estimated_cost_usd")),
            "provider_status": current.get("provider_status") or provider.get("llm"),
            "usage_status": current.get("usage_status") or provider.get("usage_status"),
        },
        "search_calls": number(current.get("search_calls") or used.get("search_calls")),
        "http": {
            "used": number(current.get("http_calls") or used.get("http_calls")),
            "limit": limits.get("http_calls", state.get("max_http_calls")),
        },
        "latest_error": current.get("latest_error") or state.get("latest_error"),
        "last_progress_at": progress_value,
        "last_heartbeat_at": current.get("last_heartbeat_at") or state.get("last_heartbeat_at"),
        "heartbeat_age_seconds": heartbeat_age_seconds,
        "last_status_update_at": last_status_update_at,
        "status_age_seconds": status_age_seconds,
        "progress_age_hours": age_hours,
        "stalled": stalled,
        "stop_file_present": stop_requested,
        "consistency_errors": consistency_errors,
        "critical_gaps": number(database.get("critical_gaps")),
        "orphaned_runner": unexpected_stop,
        "restart_required": unexpected_stop or (task_is_running and not running),
        "last_exit_code": runner_exit.get("exit_code"),
        "last_exit_reason": runner_exit.get("exit_reason"),
        "last_exit_time": runner_exit.get("exit_time"),
        "health_gate": "BLOCKED" if health_blocked else "PASS",
    }
    return health


def main() -> int:
    parser = argparse.ArgumentParser(description="Read-only CRPD all-city task health monitor")
    parser.add_argument("--automation-root", type=Path, default=AUTOMATION_ROOT)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    automation_dir = latest_automation(args.automation_root)
    if automation_dir is None:
        health = {
            "generated_at": datetime.now(UTC).isoformat(),
            "task_name": TASK_NAME,
            "status": "NOT_STARTED",
            "health_gate": "BLOCKED",
            "stop_file_present": STOP_FILE.exists(),
            "declared_status": "NOT_STARTED",
            "effective_status": "NOT_STARTED",
            "scheduled_task_state": str(query_scheduled_task().get("state") or "UNKNOWN"),
            "runner_pid_recorded": None,
            "runner_pid_actual": None,
            "runner_process_running": False,
            "worker_process_count": 0,
            "orphaned_runner": False,
            "restart_required": True,
        }
        output = args.output or args.automation_root / "health_report.json"
    else:
        health = build_health(automation_dir)
        output = args.output or automation_dir / "health_report.json"
    atomic_json(output, health)
    print(json.dumps(health, ensure_ascii=False, indent=2, default=str))
    return 0 if health.get("health_gate") != "BLOCKED" else 10


if __name__ == "__main__":
    raise SystemExit(main())
