"""Persistent, Codex-independent controller for CRPD database completion.

The controller is deliberately conservative at the hand-off boundary.  It does
not start a worker while the existing audited full-backfill or its legacy
resume supervisor is alive.  All state writes are atomic and all stage
commands are an explicit allow-list; no user supplied shell text is executed.
"""

from __future__ import annotations

import argparse
import base64
import datetime as dt
import hashlib
import json
import os
import queue
import re
import shutil
import subprocess
import threading
import time
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import polars as pl
import psutil
from filelock import FileLock, Timeout

UTC = dt.UTC
STAGES = (
    "WAIT_CURRENT_RUN",
    "CRAWL",
    "NORMALIZE",
    "DEDUP",
    "AI_CLASSIFY",
    "AI_VERIFY",
    "ARCHIVE",
    "COVERAGE_AUDIT",
    "RECENT_30D_PRIORITY",
    "RECENT_COVERAGE_AUDIT",
    "ROLLING_24M_FULL_CITY_BACKFILL",
    "ROLLING_24M_COVERAGE_AUDIT",
    "ROLLING_24M_RECOVER_MISSING",
    "ROLLING_24M_SECOND_PASS",
    "ROLLING_24M_SATURATED",
    "RECOVER_MISSING",
    "CRAWL_AGAIN",
    "HISTORICAL_CRAWL_AGAIN",
    "PDF_STAGE",
    "PDF_VERIFY",
    "FINAL_AUDIT",
    "COMPLETE",
)
AI_STAGES = {"AI_CLASSIFY", "AI_VERIFY"}
CRAWL_STAGES = {"CRAWL", "CRAWL_AGAIN", "HISTORICAL_CRAWL_AGAIN", "RECOVER_MISSING"}
ROLLING_STAGES = {
    "ROLLING_24M_FULL_CITY_BACKFILL",
    "ROLLING_24M_RECOVER_MISSING",
    "ROLLING_24M_SECOND_PASS",
}
RETRYABLE_SHARD_STATUSES = {
    "pending",
    "failed",
    "partial_network",
    "partial_parser",
    "partial_cap",
}
ACTIVE_SHARD_STATUSES = {"running", "fetching", "discovering"}
SOURCE_GAP_SHARD_STATUSES = {"source_incomplete"}
BLOCKING_TERMS = (
    "BUDGET_LEDGER_INCONSISTENT",
    "CHECKPOINT_CONFLICT",
    "CHECKPOINT_CORRUPT",
    "CONSISTENCY_ERROR",
    "CRITICAL_GAP",
    "DATABASE CORRUPT",
    "DUCKDB\\ERROR",
    "SCHEMA DRIFT",
    "SCHEMA_ERROR",
    "DISK_FULL",
    "NO SPACE",
    "WRITE CONFLICT",
    "LOCK CONFLICT",
    "RECENT_FORMAL_INGEST_BLOCKED",
)
SENSITIVE_PATTERNS = (
    (re.compile(r"(?i)(authorization\s*[:=]\s*bearer\s+)[^\s]+"), r"\1[REDACTED]"),
    (re.compile(r"(?i)(bearer\s+)[A-Za-z0-9._~+/=-]+"), r"\1[REDACTED]"),
    (re.compile(r"(?i)(api[_-]?key|access[_-]?token|secret|password)\s*[:=]\s*[^\s,;]+"), r"\1=[REDACTED]"),
    (re.compile(r"\bsk-[A-Za-z0-9_-]+\b"), "[REDACTED]"),
)


def utc_now() -> str:
    return dt.datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def run_id(prefix: str = "RUN") -> str:
    return f"{prefix}_{dt.datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}_{os.getpid()}"


def redact(value: str) -> str:
    result = value
    for pattern, replacement in SENSITIVE_PATTERNS:
        result = pattern.sub(replacement, result)
    return result


def atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temp.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, default=str) + "\n",
        encoding="utf-8",
    )
    os.replace(temp, path)


def read_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return default


def append_jsonl(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(value, ensure_ascii=False, default=str) + "\n"
    with path.open("a", encoding="utf-8") as handle:
        handle.write(line)
        handle.flush()
        os.fsync(handle.fileno())


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def decode_encoded_command(command_line: str) -> str:
    tokens = command_line.split()
    for index, token in enumerate(tokens[:-1]):
        if token.lower() in {"-encodedcommand", "-enc"}:
            try:
                raw = base64.b64decode(tokens[index + 1])
                return raw.decode("utf-16-le", errors="replace")
            except (ValueError, UnicodeError):
                return ""
    return ""


def process_command(process: psutil.Process) -> str:
    try:
        command = " ".join(process.cmdline())
    except (psutil.AccessDenied, psutil.NoSuchProcess, OSError):
        command = ""
    return command


def is_autonomous_process(command: str) -> bool:
    lowered = command.lower()
    return "crpd_autonomous_controller.py" in lowered or "crpd_autonomous_worker.ps1" in lowered


def is_legacy_supervisor(process: psutil.Process, command: str) -> bool:
    if process.name().lower() not in {"powershell.exe", "pwsh.exe", "powershell"}:
        return False
    decoded = decode_encoded_command(command)
    combined = f"{command}\n{decoded}".lower()
    return "-encodedcommand" in command.lower() and (
        "crpd_audited_full_backfill" in combined
        or "full_crawl_resume" in combined
        or "audited_full_backfill" in combined
    )


def is_current_backfill(process: psutil.Process, command: str) -> bool:
    if is_autonomous_process(command):
        return False
    name = process.name().lower()
    lowered = command.lower()
    if name in {"powershell.exe", "pwsh.exe", "powershell"}:
        return "crpd_audited_full_backfill.ps1" in lowered or "crpd_full_historical_ai" in lowered
    if name in {"policydb.exe", "python.exe", "python"}:
        return "crawl exhaustive-" in lowered or "crawl exhaustive" in lowered
    return False


def is_external_writer(process: psutil.Process, command: str) -> bool:
    if is_autonomous_process(command):
        return False
    name = process.name().lower()
    lowered = command.lower()
    if name not in {"policydb.exe", "python.exe", "python", "powershell.exe", "pwsh.exe"}:
        return False
    if is_legacy_supervisor(process, command) or is_current_backfill(process, command):
        return True
    if name in {"policydb.exe", "python.exe", "python"}:
        return any(
            marker in lowered
            for marker in (
                " crawl ",
                " archive ",
                " build-database",
                " source",
                " ai ",
                " review ",
            )
        )
    return False


def process_snapshot() -> dict[str, Any]:
    current: list[dict[str, Any]] = []
    legacy: list[dict[str, Any]] = []
    writers: list[dict[str, Any]] = []
    own_pid = os.getpid()
    for process in psutil.process_iter(["pid", "ppid", "name", "create_time"]):
        if process.pid == own_pid:
            continue
        command = process_command(process)
        if not command:
            continue
        item = {
            "pid": process.pid,
            "ppid": process.info.get("ppid"),
            "name": process.info.get("name"),
            "create_time": process.info.get("create_time"),
            "command_sha256": sha256_text(redact(command)),
        }
        if is_current_backfill(process, command):
            current.append(item)
        if is_legacy_supervisor(process, command):
            legacy.append(item)
        if is_external_writer(process, command):
            writers.append(item)
    autonomous = []
    for process in psutil.process_iter(["pid", "ppid", "name", "create_time"]):
        if process.pid == own_pid:
            continue
        command = process_command(process)
        if command and is_autonomous_process(command) and " worker " in f" {command.lower()} ":
            autonomous.append(
                {
                    "pid": process.pid,
                    "ppid": process.info.get("ppid"),
                    "name": process.info.get("name"),
                    "create_time": process.info.get("create_time"),
                }
            )
    return {
        "current_backfill": current,
        "legacy_supervisor": legacy,
        "external_writers": writers,
        "autonomous_workers": autonomous,
    }


@dataclass(frozen=True)
class Paths:
    project: Path
    data: Path
    config: Path

    @property
    def automation(self) -> Path:
        return self.data / "automation"

    @property
    def supervisor_logs(self) -> Path:
        return self.data / "logs" / "autonomous_supervisor"

    @property
    def master(self) -> Path:
        return self.automation / "MASTER_STATE.json"

    @property
    def history(self) -> Path:
        return self.automation / "RUN_HISTORY.jsonl"

    @property
    def current_run(self) -> Path:
        return self.automation / "CURRENT_RUN.json"

    @property
    def lock(self) -> Path:
        return self.automation / "AUTOMATION.lock"

    @property
    def stop(self) -> Path:
        return self.automation / "STOP"

    @property
    def blocked(self) -> Path:
        return self.automation / "AUTOMATION_BLOCKED.json"


def default_config(project: Path, data: Path) -> dict[str, Any]:
    return {
        "automation_id_prefix": "AUTO",
        "task_name": "CRPD_Autonomous_Database_Completion",
        "scheduler_interval_minutes": 30,
        "start_date": "2018-01-01",
        "end_date": "today",
        "max_recovery_fetches": 20,
        "crawl": {
            "script": str(project / "scripts" / "CRPD_Audited_Full_Backfill.ps1"),
            "max_pages_per_source": 3000,
            "max_candidates_per_shard": 100000,
            "max_fetches_per_shard": 100000,
            "network_retry_passes": 2,
            "existing_sources_only": True,
            "skip_ai": True,
        },
        "pdf_limit": 30,
        "recent_30d": {
            "max_items": 20,
            "max_pages_per_source": 30,
            "max_candidates_per_shard": 500,
            "max_fetches_per_shard": 500,
        },
        "rolling_24m": {
            "enabled": True,
            "max_items": 5,
            "max_pages_per_source": 300,
            "max_candidates_per_shard": 5000,
            "max_fetches_per_shard": 5000,
            "max_attempts": 3,
            "pdf_discovery_limit": 30,
        },
        "watchdog": {
            "no_progress_minutes": 15,
            "recovery_cooldown_minutes": 15,
        },
        "disk": {"warn_free_gb": 50, "stop_free_gb": 20},
        "paths": {
            "project_root": str(project),
            "data_root": str(data),
            "database": str(data / "database" / "policydb.duckdb"),
            "curated": str(data / "curated"),
            "outputs": str(data / "outputs"),
            "policy_archive": str(data / "archive"),
            "pdf_archive": str(data / "raw" / "pdf"),
        },
        "saturation": {
            "city_count": 105,
            "overall_city_month_min": 0.95,
            "per_city_min": 0.90,
            "max_unexplained_gap_months": 12,
            "repeat_new_document_max": 0.01,
            "repeat_new_detail_url_max": 0.01,
        },
        "state_machine": list(STAGES),
        "ai_failure_policy": "DEFERRED_AI_QUEUE",
        "secret_policy": "use existing environment or SecretStore; never write credentials",
    }


def load_config(paths: Paths) -> dict[str, Any]:
    config = read_json(paths.config, {})
    if not isinstance(config, dict):
        raise ValueError(f"Autonomous config must be a JSON object: {paths.config}")
    merged = default_config(paths.project, paths.data)
    for key, value in config.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = {**merged[key], **value}
        else:
            merged[key] = value
    return merged


def ensure_initial_files(paths: Paths, config: dict[str, Any]) -> dict[str, Any]:
    paths.automation.mkdir(parents=True, exist_ok=True)
    paths.supervisor_logs.mkdir(parents=True, exist_ok=True)
    paths.lock.touch(exist_ok=True)
    for directory in (paths.data / "outputs" / "coverage", paths.data / "checkpoints"):
        directory.mkdir(parents=True, exist_ok=True)
    automation_id = str(read_json(paths.master, {}).get("automation_id") or run_id("AUTO"))
    initial = {
        "automation_id": automation_id,
        "status": "READY",
        "stage": "WAIT_CURRENT_RUN",
        "run_id": None,
        "current_run_active": False,
        "worker_pid": None,
        "last_reason": "INITIALIZED",
        "last_error": None,
        "last_progress_at": None,
        "last_heartbeat_at": utc_now(),
        "next_stage": "WAIT_CURRENT_RUN",
        "coverage": {"saturated": False, "status": "UNKNOWN"},
        "disk": {},
        "updated_at": utc_now(),
    }
    current = read_json(paths.master, None)
    if not isinstance(current, dict):
        atomic_json(paths.master, initial)
    if not paths.current_run.exists():
        atomic_json(paths.current_run, {"status": "NOT_STARTED", "updated_at": utc_now()})
    if not paths.history.exists():
        paths.history.touch()
    defaults = {
        "COVERAGE_STATE.json": {"status": "UNKNOWN", "saturated": False, "updated_at": utc_now()},
        "AI_QUEUE_STATE.json": {"status": "NOT_STARTED", "deferred": 0, "updated_at": utc_now()},
        "PDF_ARCHIVE_STATE.json": {"status": "NOT_STARTED", "updated_at": utc_now()},
    }
    for name, value in defaults.items():
        target = paths.automation / name
        if not target.exists():
            atomic_json(target, value)
    storage = {
        "resolved_at": utc_now(),
        "data_root": str(paths.data),
        "database": str(paths.data / "database" / "policydb.duckdb"),
        "curated": str(paths.data / "curated"),
        "outputs": str(paths.data / "outputs"),
        "database_exists": (paths.data / "database" / "policydb.duckdb").exists(),
        "curated_exists": (paths.data / "curated").exists(),
        "outputs_exists": (paths.data / "outputs").exists(),
        "resolution_method": "explicit configured E drive paths",
    }
    atomic_json(paths.automation / "STORAGE_RESOLUTION.json", storage)
    pdf_archive = paths.data / "raw" / "pdf"
    policy_archive = paths.data / "archive"
    atomic_json(
        paths.automation / "ARCHIVE_STORAGE_RESOLUTION.json",
        {
            "resolved_at": utc_now(),
            "policy_archive_root": str(policy_archive),
            "policy_archive_exists": policy_archive.exists(),
            "pdf_archive_root": str(pdf_archive),
            "pdf_archive_exists": pdf_archive.exists(),
            "pdf_config_source": str(paths.project / "config" / "pdf_pipeline.yaml"),
            "raw_pdf_policy": "outside git; immutable content-addressed archive",
        },
    )
    return read_json(paths.master, initial)


def state_update(paths: Paths, updates: dict[str, Any]) -> dict[str, Any]:
    state = read_json(paths.master, {})
    if not isinstance(state, dict):
        state = {}
    state.update(updates)
    state["updated_at"] = utc_now()
    state["last_heartbeat_at"] = utc_now()
    atomic_json(paths.master, state)
    try:
        write_progress_snapshot(paths, state=state)
    except Exception:  # noqa: BLE001 - monitoring must never stop production
        pass
    return state


def _queue_progress(paths: Paths, relative: str, *, complete_statuses: set[str]) -> dict[str, Any]:
    path = paths.data / "outputs" / relative
    if not path.is_file():
        return {"total": 0, "completed": 0, "pending": 0, "running": 0, "failed": 0, "retryable": 0, "source_incomplete": 0}
    try:
        frame = pl.read_parquet(path)
        if "status" not in frame.columns:
            return {"total": frame.height, "completed": 0, "pending": None, "running": None, "failed": None, "retryable": None, "source_incomplete": None}
        statuses = frame.get_column("status").cast(pl.String)
        retryable = {"PENDING", "RUNNING", "RETRY_WAIT", "PARTIAL_NETWORK", "PARTIAL_TEMPORAL"}
        return {
            "total": frame.height,
            "completed": int(statuses.is_in(list(complete_statuses)).sum()),
            "pending": int(statuses.eq("PENDING").sum()),
            "running": int(statuses.eq("RUNNING").sum()),
            "failed": int(statuses.eq("FAILED").sum()),
            "retryable": int(statuses.is_in(list(retryable)).sum()),
            "source_incomplete": int(statuses.eq("SOURCE_INCOMPLETE").sum()),
            "cities_checked": int(frame.filter(pl.col("attempts") > 0).get_column("city_id").n_unique())
            if "attempts" in frame.columns and "city_id" in frame.columns
            else int(frame.filter(pl.col("attempt_count") > 0).get_column("city_id").n_unique())
            if "attempt_count" in frame.columns and "city_id" in frame.columns
            else 0,
            "source_slots_checked": int(statuses.is_in(list(complete_statuses)).sum()),
        }
    except (OSError, pl.exceptions.PolarsError, ValueError):
        return {"total": None, "completed": None, "pending": None, "running": None, "failed": None, "retryable": None, "source_incomplete": None}


def write_progress_snapshot(paths: Paths, *, state: dict[str, Any] | None = None, force: bool = False) -> dict[str, Any]:
    """Write a lightweight, atomic dashboard snapshot at most every 30s."""

    target = paths.automation / "PROGRESS_SNAPSHOT.json"
    if not force and target.exists():
        try:
            age = time.time() - target.stat().st_mtime
            if age < 30:
                return read_json(target, {})
        except OSError:
            pass
    master = state if isinstance(state, dict) else read_json(paths.master, {})
    recent = read_json(paths.automation / "RECENT_30D_STATE.json", {})
    rolling = read_json(paths.automation / "ROLLING_24M_STATE.json", {})
    recent_queue = _queue_progress(paths, "recent_30d/RECENT_30D_QUEUE.parquet", complete_statuses={"SUCCESS", "ZERO_CONFIRMED", "SOURCE_INCOMPLETE", "FAILED"})
    rolling_queue = _queue_progress(paths, "rolling_24m/ROLLING_24M_QUEUE.parquet", complete_statuses={"POLICY_FOUND", "CONFIRMED_ZERO", "COMPLETE_UNVERIFIED", "SOURCE_INCOMPLETE", "FAILED"})
    shards = crawl_shard_summary(paths)
    real_progress_values = [
        _parse_utc_timestamp(value)
        for value in (
            master.get("last_progress_at"),
            recent.get("last_real_progress_at"),
            rolling.get("last_real_progress_at"),
        )
        if _parse_utc_timestamp(value) is not None
    ]
    latest_real_progress = max(real_progress_values) if real_progress_values else None
    payload = {
        "timestamp": utc_now(),
        "system_status": master.get("status"),
        "stage": master.get("stage"),
        "batch": master.get("run_id"),
        "stage_status": recent.get("stage_status") or rolling.get("stage_status"),
        "batch_status": recent.get("batch_status") or rolling.get("batch_status"),
        "current_step": recent.get("last_event") or rolling.get("last_event") or master.get("last_reason"),
        "current_city": recent.get("current_city") or rolling.get("current_city") or recent.get("current_item") or rolling.get("current_queue_item"),
        "current_source": recent.get("current_source") or rolling.get("current_source") or rolling.get("current_queue_item"),
        "current_source_role": recent.get("current_source_role") or rolling.get("current_source_role"),
        "current_window": {
            "recent_start": recent.get("start_date"),
            "recent_end": recent.get("end_date"),
            "rolling_start": rolling.get("window_start"),
            "rolling_end": rolling.get("window_end"),
        },
        "recent_30d": {
            "total": recent_queue.get("total"),
            "completed": recent_queue.get("completed"),
            "pending": recent_queue.get("pending"),
            "running": recent_queue.get("running"),
            "source_incomplete": recent_queue.get("source_incomplete"),
            "cities_checked": recent_queue.get("cities_checked", 0),
        },
        "rolling_24m": {
            "total": rolling_queue.get("total"),
            "completed": rolling_queue.get("completed"),
            "pending": rolling_queue.get("pending"),
            "running": rolling_queue.get("running"),
            "failed": rolling_queue.get("failed"),
            "source_incomplete": rolling_queue.get("source_incomplete"),
            "cities_checked": rolling_queue.get("cities_checked", 0),
            "cities_total": 105,
            "source_slots_checked": rolling_queue.get("source_slots_checked", 0),
            "source_slots_total": rolling_queue.get("total"),
            "city_month_checked": None,
            "city_month_total": None,
            "progress_pct": round(rolling_queue["completed"] / rolling_queue["total"] * 100, 2)
            if rolling_queue.get("total") not in (None, 0) and rolling_queue.get("completed") is not None
            else None,
        },
        "historical": {
            "pending_shards": shards.get("pending"),
            "retryable": shards.get("retryable"),
            "source_incomplete": shards.get("source_gaps"),
        },
        "throughput": {
            "items_per_hour": None,
            "documents_per_hour": None,
            "records_per_hour": None,
            "pdfs_per_hour": None,
            "status": "INSUFFICIENT_WINDOW",
        },
        "last_real_progress_at": latest_real_progress.isoformat().replace("+00:00", "Z") if latest_real_progress else None,
        "heartbeat_at": master.get("last_heartbeat_at"),
    }
    atomic_json(target, payload)
    return payload


def _parse_utc_timestamp(value: Any) -> dt.datetime | None:
    if not value:
        return None
    try:
        parsed = dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def progress_watchdog(paths: Paths, config: dict[str, Any], processes: dict[str, Any] | None = None) -> dict[str, Any]:
    """Check durable business progress separately from controller heartbeats."""

    processes = processes or process_snapshot()
    master = read_json(paths.master, {})
    recent = read_json(paths.automation / "RECENT_30D_STATE.json", {})
    rolling = read_json(paths.automation / "ROLLING_24M_STATE.json", {})
    snapshot = read_json(paths.automation / "PROGRESS_SNAPSHOT.json", {})
    stage = str(master.get("stage") or "")
    candidates: list[tuple[str, dt.datetime]] = []
    if stage.startswith("RECENT"):
        for label, value in (
            ("recent.last_real_progress_at", recent.get("last_real_progress_at")),
            ("recent.updated_at", recent.get("updated_at")),
        ):
            parsed = _parse_utc_timestamp(value)
            if parsed:
                candidates.append((label, parsed))
    elif stage.startswith("ROLLING"):
        for label, value in (
            ("rolling.last_real_progress_at", rolling.get("last_real_progress_at")),
            ("rolling.updated_at", rolling.get("updated_at")),
        ):
            parsed = _parse_utc_timestamp(value)
            if parsed:
                candidates.append((label, parsed))
    else:
        progress = _progress_snapshot(paths)
        parsed = _parse_utc_timestamp(progress.get("latest_created_at"))
        if parsed:
            candidates.append(("pipeline_progress_events.latest_created_at", parsed))
    parsed = _parse_utc_timestamp(snapshot.get("last_real_progress_at"))
    if parsed:
        candidates.append(("progress_snapshot.last_real_progress_at", parsed))
    for relative in (
        "outputs/recent_30d/RECENT_30D_QUEUE.parquet",
        "outputs/rolling_24m/ROLLING_24M_QUEUE.parquet",
        "curated/crawl_shards.parquet",
        "curated/policy_document_versions.parquet",
        "curated/records.parquet",
    ):
        path = paths.data / relative
        if path.exists():
            try:
                candidates.append((f"mtime:{relative}", dt.datetime.fromtimestamp(path.stat().st_mtime, UTC)))
            except OSError:
                continue
    latest_label, latest = max(candidates, key=lambda item: item[1]) if candidates else (None, None)
    now = dt.datetime.now(UTC)
    age_seconds = max(0.0, (now - latest).total_seconds()) if latest else None
    watchdog_config = config.get("watchdog") if isinstance(config.get("watchdog"), dict) else {}
    threshold = max(1, int(watchdog_config.get("no_progress_minutes", 15))) * 60
    active_worker = bool(processes.get("autonomous_workers"))
    status = "INSUFFICIENT_EVIDENCE" if latest is None else "OK" if age_seconds < threshold else "NO_REAL_PROGRESS"
    payload = {
        "status": status,
        "stage": stage,
        "checked_at": utc_now(),
        "threshold_seconds": threshold,
        "last_real_progress_at": latest.isoformat().replace("+00:00", "Z") if latest else None,
        "last_real_progress_source": latest_label,
        "age_seconds": round(age_seconds, 1) if age_seconds is not None else None,
        "controller_heartbeat_at": master.get("last_heartbeat_at"),
        "active_worker": active_worker,
        "safe_recovery": "deferred_until_no_active_worker" if status == "NO_REAL_PROGRESS" and active_worker else "eligible" if status == "NO_REAL_PROGRESS" else "not_needed",
        "recovery_policy": "reuse_terminal_finalize_and_stale_lease_reconciliation; never kill an active writer",
    }
    atomic_json(paths.automation / "PROGRESS_WATCHDOG.json", payload)
    return payload


def transition(paths: Paths, run: str, stage: str, status: str, reason: str, **extra: Any) -> None:
    event = {
        "event": "state_transition",
        "transition_id": f"{run}:{stage}:{status}:{reason}",
        "idempotency_key": sha256_text(f"{run}|{stage}|{status}|{reason}"),
        "run_id": run,
        "stage": stage,
        "status": status,
        "reason_code": reason,
        "timestamp": utc_now(),
        **extra,
    }
    append_jsonl(paths.history, event)
    state_update(
        paths,
        {
            "stage": stage,
            "status": status,
            "run_id": run,
            "last_reason": reason,
            "last_transition": event,
        },
    )


def current_log_status(paths: Paths, *, active: bool = False) -> dict[str, Any]:
    root = paths.data / "logs" / "audited_full_backfill"
    directories = sorted((p for p in root.glob("*") if p.is_dir()), key=lambda p: p.name, reverse=True)
    if not directories:
        return {"exists": False, "safe_ended": False, "reason": "NO_FULL_BACKFILL_LOG"}
    latest = directories[0]
    master = latest / "master.log"
    if not master.exists():
        return {"exists": True, "safe_ended": False, "reason": "MASTER_LOG_MISSING", "run_dir": str(latest)}
    try:
        with master.open("rb") as handle:
            handle.seek(max(0, master.stat().st_size - 262144))
            tail = handle.read().decode("utf-8", errors="replace")
    except OSError as exc:
        return {"exists": True, "safe_ended": False, "reason": f"MASTER_LOG_READ_ERROR:{type(exc).__name__}"}
    marker = "补扫流程结束" in tail or "backfill complete" in tail.lower() or "full backfill complete" in tail.lower()
    failure_evidence = [
        redact(line.strip())
        for line in tail.splitlines()
        if re.search(
            r"(?:阶段失败|exit\s*=\s*[1-9]|traceback|fatal|unhandled|process.+(?:failed|exit))",
            line,
            flags=re.IGNORECASE,
        )
    ][-8:]
    checkpoint = any(
        path.is_file() and path.stat().st_size > 0
        for path in (
            paths.data / "curated" / "crawl_shards.parquet",
            paths.data / "curated" / "crawl_items.parquet",
        )
    ) or any(path.is_file() and path.stat().st_size > 0 for path in (paths.data / "checkpoints").glob("*") if (paths.data / "checkpoints").exists())
    if active:
        handoff_status = "ACTIVE"
        handoff_reason = "CURRENT_RUN_ACTIVE"
        handoff_ready = False
    elif marker and checkpoint:
        handoff_status = "SUCCESSFUL_END"
        handoff_reason = "RUN_SUMMARY_AND_CHECKPOINT_READY"
        handoff_ready = True
    elif failure_evidence:
        # A legacy runner can exit without writing its success marker.  Its
        # explicit stage failure evidence is still enough to establish a safe
        # hand-off: retry from the durable checkpoint, never as a success.
        handoff_status = "TERMINAL_FAILED"
        handoff_reason = "LEGACY_RUN_TERMINAL_FAILED"
        handoff_ready = True
    else:
        handoff_status = "TRUE_SUMMARY_PENDING"
        handoff_reason = "RUN_SUMMARY_PENDING"
        handoff_ready = False
    return {
        "exists": True,
        "status": handoff_status,
        "safe_ended": handoff_ready,
        "reason": handoff_reason,
        "run_dir": str(latest),
        "master_log": str(master),
        "summary_marker": marker,
        "checkpoint_ready": checkpoint,
        "failure_evidence": failure_evidence,
        "last_write_at": dt.datetime.fromtimestamp(master.stat().st_mtime, UTC).isoformat(),
    }


def latest_safe_handoff(paths: Paths) -> dict[str, Any] | None:
    """Return the newest explicit safe-handoff route request, if any."""

    candidates = sorted(
        paths.automation.glob("SAFE_HANDOFF_ROUTE_*.json"),
        key=lambda path: path.name,
        reverse=True,
    )
    for path in candidates:
        try:
            payload = json.loads(path.read_text(encoding="utf-8-sig"))
        except (OSError, json.JSONDecodeError):
            continue
        if (
            isinstance(payload, dict)
            and payload.get("status") == "REQUESTED"
            and payload.get("route_to") in {
                "COVERAGE_AUDIT",
                "RECENT_30D_PRIORITY",
                "RECENT_COVERAGE_AUDIT",
                "ROLLING_24M_FULL_CITY_BACKFILL",
            }
            and not safe_handoff_consumed(paths, payload)
        ):
            return payload
    return None


def safe_handoff_consumed(paths: Paths, payload: dict[str, Any]) -> bool:
    handoff_id = str(payload.get("handoff_id") or "")
    source_run_id = str(payload.get("source_run_id") or "")
    receipt_key = handoff_id or source_run_id
    if not receipt_key:
        return False
    receipt_id = sha256_text(receipt_key)[:16]
    return (paths.automation / f"SAFE_HANDOFF_CONSUMED_{receipt_id}.json").exists()


def consume_safe_handoff(paths: Paths, payload: dict[str, Any]) -> None:
    handoff_id = str(payload.get("handoff_id") or "")
    source_run_id = str(payload.get("source_run_id") or "")
    if not (source_run_id or handoff_id) or safe_handoff_consumed(paths, payload):
        return
    receipt_key = handoff_id or source_run_id
    receipt_id = sha256_text(receipt_key)[:16]
    atomic_json(
        paths.automation / f"SAFE_HANDOFF_CONSUMED_{receipt_id}.json",
        {
            "status": "CONSUMED",
            "source_run_id": source_run_id,
            "handoff_id": handoff_id or None,
            "route_to": payload.get("route_to"),
            "consumed_at": utc_now(),
        },
    )


TERMINAL_RUN_STATUSES = {"COMPLETED", "FAILED", "PARTIAL", "TERMINATED", "NO_WORK_REQUIRED"}


def finalize_terminal_run(
    paths: Paths,
    processes: dict[str, Any],
    config: dict[str, Any],
) -> dict[str, Any] | None:
    """Consume a terminal current run exactly once.

    A terminal worker result is durable evidence even when the worker missed
    the controller tick that normally writes a summary.  The handoff marker is
    deterministic, so repeated scheduler ticks can only reuse the same route.
    """

    if any(processes.get(name) for name in ("autonomous_workers", "current_backfill", "legacy_supervisor", "external_writers")):
        return None
    current = read_json(paths.current_run, {})
    if not isinstance(current, dict) or str(current.get("status")) not in TERMINAL_RUN_STATUSES:
        return None
    run = str(current.get("run_id") or "")
    stage = str(current.get("stage") or "")
    completed_at = str(current.get("completed_at") or current.get("updated_at") or "")
    if not run or not stage:
        return None
    handoff_id = sha256_text(f"{run}|{stage}|{completed_at}")[:24]
    marker_path = paths.automation / f"TERMINAL_HANDOFF_{handoff_id}.json"
    route_path = paths.automation / f"SAFE_HANDOFF_ROUTE_TERMINAL_{handoff_id}.json"
    if marker_path.exists() and route_path.exists():
        return read_json(route_path, None)

    recent_state = read_json(paths.automation / "RECENT_30D_STATE.json", {})
    rolling_state = read_json(paths.automation / "ROLLING_24M_STATE.json", {})
    if stage == "RECENT_30D_PRIORITY":
        next_stage = "RECENT_COVERAGE_AUDIT" if recent_state.get("status") == "COMPLETE" else "RECENT_30D_PRIORITY"
    elif stage in ROLLING_STAGES or stage == "ROLLING_24M_COVERAGE_AUDIT":
        next_stage = "ROLLING_24M_COVERAGE_AUDIT" if rolling_state.get("status") == "COMPLETE" else "ROLLING_24M_FULL_CITY_BACKFILL"
    else:
        next_stage = str(read_json(paths.master, {}).get("next_stage") or "CRAWL")

    summary = None
    for candidate in (
        paths.automation / f"RECENT_30D_RUN_SUMMARY_{run}.json",
        paths.automation / f"ROLLING_24M_RUN_SUMMARY_{run}.json",
        paths.automation / f"TERMINAL_RUN_SUMMARY_{run}.json",
    ):
        value = read_json(candidate, None)
        if isinstance(value, dict):
            summary = value
            break
    if summary is None:
        summary = {
            "run_id": run,
            "stage": stage,
            "status": current.get("status"),
            "reason": current.get("reason"),
            "completed_at": completed_at or None,
            "reconstructed": True,
            "recent_state": recent_state if stage == "RECENT_30D_PRIORITY" else None,
            "rolling_state": rolling_state if stage in ROLLING_STAGES else None,
            "created_at": utc_now(),
        }
    summary = {**summary, "terminal_handoff_id": handoff_id, "terminal_summary_reconstructed": bool(summary.get("reconstructed"))}
    atomic_json(paths.automation / f"TERMINAL_RUN_SUMMARY_{run}.json", summary)
    route = {
        "status": "REQUESTED",
        "handoff_id": handoff_id,
        "source_run_id": run,
        "source_stage": stage,
        "route_to": next_stage,
        "reason_code": "FINALIZE_TERMINAL_RUN",
        "summary_path": str(paths.automation / f"TERMINAL_RUN_SUMMARY_{run}.json"),
        "created_at": utc_now(),
    }
    atomic_json(marker_path, {**route, "status": "FINALIZED", "finalized_at": utc_now()})
    atomic_json(route_path, route)
    atomic_json(
        paths.current_run,
        {
            **current,
            "pid": None,
            "status": str(current.get("status")),
            "handoff_id": handoff_id,
            "handoff_consumed": False,
        },
    )
    state_update(
        paths,
        {
            "status": "READY_FOR_NEXT_STAGE",
            "stage": stage,
            "next_stage": next_stage,
            "current_run_active": False,
            "worker_pid": None,
            "last_reason": "FINALIZE_TERMINAL_RUN",
            "last_error": None,
            "last_progress_at": completed_at or utc_now(),
        },
    )
    append_jsonl(
        paths.history,
        {
            "event": "terminal_run_finalized",
            "run_id": run,
            "stage": stage,
            "status": current.get("status"),
            "handoff_id": handoff_id,
            "next_stage": next_stage,
            "reason_code": "FINALIZE_TERMINAL_RUN",
            "timestamp": utc_now(),
        },
    )
    return route


def _reconcile_stale_recent_run(paths: Paths, processes: dict[str, Any]) -> dict[str, Any] | None:
    """Durably close a recent run whose worker process disappeared.

    This is deliberately narrower than the legacy full-backfill handoff.  It
    only runs when the persisted current run is recent-priority, marked
    RUNNING, and process discovery finds no autonomous worker or writer.
    """

    current = read_json(paths.current_run, {})
    if not isinstance(current, dict):
        return None
    if str(current.get("stage")) != "RECENT_30D_PRIORITY" or str(current.get("status")) != "RUNNING":
        return None
    if processes.get("autonomous_workers") or processes.get("external_writers"):
        return None
    recent_state = read_json(paths.automation / "RECENT_30D_STATE.json", {})
    if str(recent_state.get("status")) == "COMPLETE":
        return None
    run = str(current.get("run_id") or "")
    if not run:
        return None
    from policydb.recent_priority import reconcile_stale_recent_run
    from policydb.settings import Settings

    summary = reconcile_stale_recent_run(
        Settings(
            root=paths.project,
            data_root_path=paths.data,
            database_path=paths.data / "database" / "policydb.duckdb",
            curated_path=paths.data / "curated",
            outputs_path=paths.data / "outputs",
            automation_path=paths.data / "automation",
        ),
        run_id=run,
        reason="WORKER_PROCESS_MISSING",
    )
    atomic_json(
        paths.current_run,
        {
            **current,
            "status": "TERMINATED",
            "reason": "RECENT_30D_STALE_RUN_RECONCILED",
            "terminal_reason": "WORKER_PROCESS_MISSING",
            "pid": None,
            "completed_at": utc_now(),
        },
    )
    route = {
        "status": "REQUESTED",
        "route_to": "RECENT_30D_PRIORITY",
        "source_run_id": run,
        "reason_code": "RECENT_30D_STALE_RUN_RECONCILED",
        "summary": summary,
        "created_at": utc_now(),
    }
    atomic_json(
        paths.automation / f"SAFE_HANDOFF_ROUTE_{dt.datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}_RECENT.json",
        route,
    )
    state_update(
        paths,
        {
            "status": "READY_FOR_NEXT_STAGE",
            "stage": "WAIT_CURRENT_RUN",
            "next_stage": "RECENT_30D_PRIORITY",
            "current_run_active": False,
            "worker_pid": None,
            "last_reason": "RECENT_30D_STALE_RUN_RECONCILED",
            "last_error": None,
            "last_progress_at": utc_now(),
        },
    )
    append_jsonl(
        paths.history,
        {
            "event": "recent_run_reconciled",
            "run_id": run,
            "reason_code": "WORKER_PROCESS_MISSING",
            "timestamp": utc_now(),
            "summary": summary,
        },
    )
    return {"current_run": current, "summary": summary, "route": route}


def disk_status(paths: Paths, config: dict[str, Any]) -> dict[str, Any]:
    thresholds = config.get("disk", {})
    warn = float(thresholds.get("warn_free_gb", 50))
    stop = float(thresholds.get("stop_free_gb", 20))
    result: dict[str, Any] = {"warn_free_gb": warn, "stop_free_gb": stop, "status": "OK"}
    for label, path in (("data", paths.data), ("project", paths.project)):
        usage = shutil.disk_usage(path)
        free_gb = usage.free / (1024**3)
        result[label] = {"path": str(path), "free_gb": round(free_gb, 2), "total_gb": round(usage.total / (1024**3), 2)}
        if free_gb < stop:
            result["status"] = "STOP"
        elif free_gb < warn and result["status"] == "OK":
            result["status"] = "WARN"
    return result


def coverage_summary(paths: Paths) -> dict[str, Any]:
    candidates = (
        paths.data / "outputs" / "coverage" / "LATEST_COVERAGE_SUMMARY.json",
        paths.data / "outputs" / "coverage" / "coverage_summary.json",
        paths.data / "outputs" / "exhaustive_crawl_acceptance.json",
    )
    for candidate in candidates:
        value = read_json(candidate, None)
        if isinstance(value, dict):
            explicit = value.get("WEB_CRAWL_SATURATED")
            if explicit is None:
                explicit = value.get("web_crawl_saturated")
            if explicit is None:
                explicit = str(value.get("status", "")).upper() == "SATURATED"
            return {
                "status": "SATURATED" if explicit is True else "NOT_SATURATED",
                "saturated": bool(explicit is True),
                "source": str(candidate),
                "summary": value,
                "updated_at": utc_now(),
            }
    return {"status": "UNKNOWN", "saturated": False, "source": None, "updated_at": utc_now()}


def crawl_shard_summary(paths: Paths) -> dict[str, Any]:
    """Read the durable crawl checkpoint without changing it.

    A zero-row recovery command is only a legal no-op when this audit proves
    there is no runnable or retryable work left.  Missing or unreadable
    checkpoints are therefore explicit blockers, never an empty success.
    """

    path = paths.data / "curated" / "crawl_shards.parquet"
    if not path.is_file():
        return {
            "status": "MISSING",
            "path": str(path),
            "rows": None,
            "counts": {},
            "pending": None,
            "retryable": None,
            "active": None,
            "source_gaps": None,
            "actionable": None,
        }
    try:
        frame = pl.read_parquet(path)
        if "status" not in frame.columns:
            raise ValueError("crawl_shards checkpoint has no status column")
        counts = {
            str(status): int(count)
            for status, count in frame.group_by("status").len().iter_rows()
        }
    except (OSError, pl.exceptions.PolarsError, ValueError) as exc:
        return {
            "status": "UNREADABLE",
            "path": str(path),
            "rows": None,
            "counts": {},
            "pending": None,
            "retryable": None,
            "active": None,
            "source_gaps": None,
            "actionable": None,
            "error_type": type(exc).__name__,
        }
    pending = int(counts.get("pending", 0))
    retryable = sum(int(counts.get(status, 0)) for status in RETRYABLE_SHARD_STATUSES - {"pending"})
    active = sum(int(counts.get(status, 0)) for status in ACTIVE_SHARD_STATUSES)
    source_gaps = sum(int(counts.get(status, 0)) for status in SOURCE_GAP_SHARD_STATUSES)
    return {
        "status": "AVAILABLE",
        "path": str(path),
        "rows": frame.height,
        "counts": counts,
        "pending": pending,
        "retryable": retryable,
        "active": active,
        "source_gaps": source_gaps,
        "actionable": pending + retryable + active,
    }


def _progress_snapshot(paths: Paths) -> dict[str, Any]:
    path = paths.data / "curated" / "pipeline_progress_events.parquet"
    if not path.is_file():
        return {"status": "MISSING", "rows": None, "latest_created_at": None}
    try:
        frame = pl.read_parquet(path)
        latest = None
        if "created_at" in frame.columns and frame.height:
            latest = str(frame.select(pl.col("created_at").cast(pl.String).max()).item())
        return {"status": "AVAILABLE", "rows": frame.height, "latest_created_at": latest}
    except (OSError, pl.exceptions.PolarsError, ValueError) as exc:
        return {
            "status": "UNREADABLE",
            "rows": None,
            "latest_created_at": None,
            "error_type": type(exc).__name__,
        }


def _stage_json_payloads(paths: Paths, run: str, stage: str) -> list[dict[str, Any]]:
    log_path = paths.supervisor_logs / f"{run}_{stage}.log"
    try:
        text = log_path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return []
    decoder = json.JSONDecoder()
    payloads: list[dict[str, Any]] = []
    for match in re.finditer(r"\{", text):
        try:
            value, _ = decoder.raw_decode(text[match.start() :])
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            payloads.append(value)
    return payloads


def _last_metrics_payload(paths: Paths, run: str, stage: str) -> dict[str, Any]:
    for payload in reversed(_stage_json_payloads(paths, run, stage)):
        metrics = payload.get("metrics")
        if isinstance(metrics, dict):
            return {
                "metrics": metrics,
                "warning": bool(payload.get("warning")),
                "job_id": payload.get("job_id"),
            }
    return {"metrics": {}, "warning": False, "job_id": None}


def _is_full_backfill_command(argv: list[str]) -> bool:
    return any("crpd_audited_full_backfill.ps1" in str(value).lower() for value in argv)


def crawl_stage_semantics(
    paths: Paths,
    *,
    run: str,
    stage: str,
    command_stage: str,
    argv: list[str],
    code: int,
    before_shards: dict[str, Any],
    before_progress: dict[str, Any],
) -> tuple[bool, str, dict[str, Any]]:
    """Classify crawl command completion from durable evidence, not exit code."""

    after_shards = crawl_shard_summary(paths)
    after_progress = _progress_snapshot(paths)
    metrics_payload = _last_metrics_payload(paths, run, command_stage)
    before_rows = before_progress.get("rows")
    after_rows = after_progress.get("rows")
    new_events = (
        max(0, int(after_rows) - int(before_rows))
        if before_rows is not None and after_rows is not None
        else None
    )
    details = {
        "stage": stage,
        "command_stage": command_stage,
        "exit_code": code,
        "full_backfill_command": _is_full_backfill_command(argv),
        "metrics_payload": metrics_payload,
        "shards_before": before_shards,
        "shards_after": after_shards,
        "progress_before": before_progress,
        "progress_after": after_progress,
        "new_progress_events": new_events,
        "coverage": coverage_summary(paths),
    }
    if code != 0:
        return False, f"COMMAND_FAILED:{code}", details
    if after_shards["status"] in {"MISSING", "UNREADABLE"}:
        return False, f"CRAWL_CHECKPOINT_{after_shards['status']}", details

    metrics = metrics_payload["metrics"]
    if command_stage.lower().startswith("recover_missing"):
        zero_recovery = bool(metrics_payload["warning"]) and all(
            int(metrics.get(key, 0) or 0) == 0
            for key in ("source_count", "candidate_count", "fetched", "failed", "document_versions")
        )
        unresolved = int(after_shards["actionable"] or 0) + int(after_shards["source_gaps"] or 0)
        if zero_recovery:
            if unresolved:
                return False, "NO_PROGRESS_PENDING_WORK", details
            if details["coverage"].get("status") != "SATURATED":
                return False, "NO_PROGRESS_COVERAGE_UNKNOWN", details
            return True, "NO_WORK_REQUIRED", details
        return True, "RECOVERY_COMPLETED", details

    if new_events and new_events > 0:
        if int(after_shards["actionable"] or 0) > 0:
            return True, "CRAWL_COMPLETED_WITH_REMAINING_WORK", details
        return True, "CRAWL_COMPLETED", details
    if int(after_shards["actionable"] or 0) == 0 and int(after_shards["source_gaps"] or 0) == 0:
        return True, "NO_PENDING_SHARDS", details
    return False, "NO_PROGRESS_PENDING_WORK", details


def command_environment(paths: Paths) -> dict[str, str]:
    env = os.environ.copy()
    env.update(
        {
            "POLICYDB_ROOT": str(paths.project),
            "CRPD_DATA_ROOT": str(paths.data),
            "POLICYDB_DATABASE": str(paths.data / "database" / "policydb.duckdb"),
            "POLICYDB_CURATED_ROOT": str(paths.data / "curated"),
            "POLICYDB_OUTPUT_ROOT": str(paths.data / "outputs"),
            "POLICYDB_LOG_ROOT": str(paths.data / "logs"),
            "CRPD_ARCHIVE_ROOT": str(paths.data / "archive"),
            "PYTHONUTF8": "1",
            "PYTHONIOENCODING": "utf-8",
        }
    )
    return env


def command_specs(paths: Paths, config: dict[str, Any], stage: str, run: str) -> list[list[str]]:
    python = paths.project / ".venv" / "Scripts" / "python.exe"
    policydb = paths.project / ".venv" / "Scripts" / "policydb.exe"
    if not python.exists() or not policydb.exists():
        raise FileNotFoundError("fixed project virtual environment is incomplete")
    recovery = str(int(config.get("max_recovery_fetches", 20)))
    pdf_limit = str(int(config.get("pdf_limit", 30)))
    recent = config.get("recent_30d") if isinstance(config.get("recent_30d"), dict) else {}
    if stage == "RECENT_30D_PRIORITY":
        return [[
            str(python),
            "-m",
            "policydb.autopilot_cli",
            "recent-30d",
            "run",
            "--max-items",
            str(int(recent.get("max_items", 20))),
            "--max-pages-per-source",
            str(int(recent.get("max_pages_per_source", 30))),
            "--max-candidates-per-shard",
            str(int(recent.get("max_candidates_per_shard", 500))),
            "--max-fetches-per-shard",
            str(int(recent.get("max_fetches_per_shard", 500))),
            "--apply",
            "--resume",
        ]]
    if stage == "RECENT_COVERAGE_AUDIT":
        return [[str(python), "-m", "policydb.autopilot_cli", "recent-30d", "audit", "--resume"]]
    if stage in ROLLING_STAGES:
        rolling = config.get("rolling_24m") if isinstance(config.get("rolling_24m"), dict) else {}
        command_name = "run"
        return [[
            str(python),
            "-m",
            "policydb.autopilot_cli",
            "rolling-24m",
            command_name,
            "--max-items",
            str(int(rolling.get("max_items", 5))),
            "--max-pages-per-source",
            str(int(rolling.get("max_pages_per_source", 300))),
            "--max-candidates-per-shard",
            str(int(rolling.get("max_candidates_per_shard", 5000))),
            "--max-fetches-per-shard",
            str(int(rolling.get("max_fetches_per_shard", 5000))),
            "--max-attempts",
            str(int(rolling.get("max_attempts", 3))),
            "--pdf-discovery-limit",
            str(int(rolling.get("pdf_discovery_limit", 30))),
            "--apply",
            "--resume",
        ]]
    if stage == "ROLLING_24M_COVERAGE_AUDIT":
        return [[str(python), "-m", "policydb.autopilot_cli", "rolling-24m", "audit", "--resume"]]
    if stage == "ROLLING_24M_SATURATED":
        return [[str(python), "-m", "policydb.autopilot_cli", "rolling-24m", "audit", "--resume"]]
    if stage == "RECOVER_MISSING":
        return [[str(policydb), "crawl", "recover-missing", "--max-fetches", recovery]]
    if stage in {"CRAWL", "CRAWL_AGAIN", "HISTORICAL_CRAWL_AGAIN"}:
        crawl = config.get("crawl") if isinstance(config.get("crawl"), dict) else {}
        script = Path(str(crawl.get("script") or paths.project / "scripts" / "CRPD_Audited_Full_Backfill.ps1"))
        if not script.is_file():
            raise FileNotFoundError(f"autonomous crawl script is missing: {script}")
        command = [
            "powershell.exe",
            "-NoLogo",
            "-NoProfile",
            "-NonInteractive",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(script),
            "-ProjectRoot",
            str(paths.project),
            "-DataRoot",
            str(paths.data),
            "-StartDate",
            str(crawl.get("start_date") or config.get("start_date") or "2018-01-01"),
            "-EndDate",
            str(crawl.get("end_date") or config.get("end_date") or "today"),
            "-MaxPagesPerSource",
            str(int(crawl.get("max_pages_per_source", 3000))),
            "-MaxCandidatesPerShard",
            str(int(crawl.get("max_candidates_per_shard", 100000))),
            "-MaxFetchesPerShard",
            str(int(crawl.get("max_fetches_per_shard", 100000))),
            "-NetworkRetryPasses",
            str(int(crawl.get("network_retry_passes", 2))),
        ]
        if crawl.get("existing_sources_only", True):
            command.append("-ExistingSourcesOnly")
        if crawl.get("skip_ai", True):
            command.append("-SkipAI")
        return [command]
    if stage == "NORMALIZE":
        return [[str(policydb), "build-database"], [str(policydb), "validate", "--group", "all"]]
    if stage == "DEDUP":
        return [[str(policydb), "ai", "deduplicate"], [str(policydb), "confidence", "build"]]
    if stage == "AI_CLASSIFY":
        return [[str(policydb), "ai", "classify", "--run-id", run]]
    if stage == "AI_VERIFY":
        return [[str(policydb), "ai", "verify", "--run-id", run]]
    if stage == "ARCHIVE":
        return [[str(policydb), "archive", "sync"], [str(policydb), "archive", "recover-missing"]]
    if stage == "COVERAGE_AUDIT":
        return [
            [str(policydb), "audit", "exhaustive"],
            [str(policydb), "coverage", "build"],
            [str(policydb), "progress", "export", "--format", "json"],
            [str(policydb), "progress", "export", "--format", "csv"],
        ]
    if stage == "PDF_STAGE":
        return [[str(python), "-m", "policydb.autopilot_cli", "pdf", "run", "--root", str(paths.data), "--limit", pdf_limit, "--apply", "--run-id", run]]
    if stage == "PDF_VERIFY":
        report = paths.automation / f"pdf_report_{run}.json"
        return [
            [str(python), "-m", "policydb.autopilot_cli", "pdf", "status", "--root", str(paths.data)],
            [str(python), "-m", "policydb.autopilot_cli", "pdf", "report", "--root", str(paths.data), "--output", str(report)],
        ]
    if stage == "FINAL_AUDIT":
        return [
            [str(policydb), "validate", "--group", "all"],
            [str(policydb), "audit", "exhaustive"],
            [str(policydb), "progress", "export", "--format", "json"],
        ]
    return []


def log_line(log_path: Path, line: str) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as handle:
        handle.write(redact(line.rstrip("\r\n")) + "\n")
        handle.flush()


def run_command(paths: Paths, argv: list[str], run: str, stage: str) -> int:
    log_path = paths.supervisor_logs / f"{run}_{stage}.log"
    log_line(log_path, f"[{utc_now()}] COMMAND {json.dumps(argv, ensure_ascii=False)}")
    output_queue: queue.Queue[str | None] = queue.Queue()
    try:
        process = subprocess.Popen(
            argv,
            cwd=str(paths.project),
            env=command_environment(paths),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
    except OSError as exc:
        log_line(log_path, f"[{utc_now()}] START_ERROR {type(exc).__name__}: {exc}")
        return 1

    def drain() -> None:
        assert process.stdout is not None
        try:
            for line in process.stdout:
                output_queue.put(line)
        finally:
            output_queue.put(None)

    reader = threading.Thread(target=drain, name=f"crpd-{stage}-reader", daemon=True)
    reader.start()
    last_heartbeat = 0.0

    def record_output(item: str) -> None:
        nonlocal last_heartbeat
        log_line(log_path, item)
        now = time.monotonic()
        if now - last_heartbeat >= 10:
            runtime_stage = next(
                (
                    candidate
                    for candidate in ("CRAWL_AGAIN", "HISTORICAL_CRAWL_AGAIN", "RECOVER_MISSING", "CRAWL")
                    if stage.startswith(candidate)
                ),
                stage if stage in STAGES else stage.split("_", 1)[0],
            )
            updates = {
                "status": "RUNNING",
                "stage": runtime_stage,
                "run_id": run,
                "last_reason": "COMMAND_HEARTBEAT",
            }
            if stage in {"RECENT_30D_PRIORITY", *ROLLING_STAGES} and any(
                marker in item for marker in ("processed_items", "records_promoted", "documents_found")
            ):
                updates["last_progress_at"] = utc_now()
            state_update(paths, updates)
            last_heartbeat = now

    while process.poll() is None or not output_queue.empty():
        try:
            while True:
                item = output_queue.get_nowait()
                if item is None:
                    break
                record_output(item)
        except queue.Empty:
            pass
        time.sleep(2)
    reader.join(timeout=5)
    try:
        while True:
            item = output_queue.get_nowait()
            if item is None:
                continue
            record_output(item)
    except queue.Empty:
        pass
    code = int(process.returncode or 0)
    log_line(log_path, f"[{utc_now()}] EXIT_CODE {code}")
    return code


def stage_output_has_blocker(paths: Paths, run: str, stage: str) -> tuple[bool, str | None]:
    log_path = paths.supervisor_logs / f"{run}_{stage}.log"
    try:
        text = log_path.read_text(encoding="utf-8", errors="replace").upper()
    except OSError:
        return False, None
    for term in BLOCKING_TERMS:
        if term in text:
            return True, term
    return False, None


def stage_has_structured_validation_warning(paths: Paths, run: str, stage: str) -> bool:
    """Recognize a completed validation report with data-quality gaps.

    ``policydb validate --group all`` uses exit code 1 when the current
    dataset is incomplete.  A parseable report is still useful evidence for
    the crawl and must not be confused with a command crash or storage error.
    """

    log_path = paths.supervisor_logs / f"{run}_{stage}.log"
    try:
        text = log_path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return False
    decoder = json.JSONDecoder()
    for match in re.finditer(r"(?m)^\{", text):
        try:
            report, _ = decoder.raw_decode(text[match.start() :])
        except json.JSONDecodeError:
            continue
        if (
            report.get("validation_group") == "all"
            and report.get("passed") is False
            and isinstance(report.get("v2_group_results"), dict)
            and "record_count" in report
        ):
            return True
    return False


def run_stage(paths: Paths, config: dict[str, Any], stage: str, run: str) -> tuple[bool, str]:
    specs = command_specs(paths, config, stage, run)
    for index, argv in enumerate(specs, 1):
        before_shards = crawl_shard_summary(paths) if stage in CRAWL_STAGES else {}
        before_progress = _progress_snapshot(paths) if stage in CRAWL_STAGES else {}
        code = run_command(paths, argv, run, f"{stage}_{index}")
        if stage in CRAWL_STAGES:
            command_stage = f"{stage}_{index}"
            ok, reason, details = crawl_stage_semantics(
                paths,
                run=run,
                stage=stage,
                command_stage=command_stage,
                argv=argv,
                code=code,
                before_shards=before_shards,
                before_progress=before_progress,
            )
            log_line(
                paths.supervisor_logs / f"{run}_{command_stage}.log",
                f"[{utc_now()}] CRAWL_SEMANTIC {json.dumps(details, ensure_ascii=False, default=str)}",
            )
            state_update(paths, {"last_crawl_audit": details, "last_reason": reason})
            if not ok:
                blocker, blocker_reason = stage_output_has_blocker(paths, run, command_stage)
                if blocker:
                    return False, f"BLOCKING:{blocker_reason}"
                return False, reason
            continue
        if code != 0:
            blocker, reason = stage_output_has_blocker(paths, run, f"{stage}_{index}")
            if blocker:
                return False, f"BLOCKING:{reason}"
            if (
                stage == "NORMALIZE"
                and index == 2
                and stage_has_structured_validation_warning(paths, run, f"{stage}_{index}")
            ):
                return True, "VALIDATION_WARNINGS"
            if stage in AI_STAGES:
                return True, "DEFERRED_AI_QUEUE"
            return False, f"COMMAND_FAILED:{code}"
    return True, "SUCCESS"


def _spawn_controller_tick(paths: Paths) -> None:
    """Schedule one delayed supervisor tick after a successful worker batch."""

    python = paths.project / ".venv" / "Scripts" / "python.exe"
    controller = Path(__file__).resolve()
    if not python.exists() or paths.stop.exists():
        return
    code = (
        "import subprocess,sys,time; time.sleep(3); "
        "subprocess.run([sys.executable,sys.argv[1],'supervisor','--project-root',"
        "sys.argv[2],'--data-root',sys.argv[3],'--config',sys.argv[4]],check=False)"
    )
    command = [str(python), "-c", code, str(controller), str(paths.project), str(paths.data), str(paths.config)]
    log_path = paths.supervisor_logs / f"{run_id('AUTOCONTINUE')}.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    handle = log_path.open("a", encoding="utf-8")
    try:
        subprocess.Popen(
            command,
            cwd=str(paths.project),
            env=command_environment(paths),
            stdout=handle,
            stderr=subprocess.STDOUT,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0)
            | getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0),
        )
    finally:
        handle.close()


def next_stage_after(
    stage: str,
    saturated: bool,
    crawl_work_pending: bool | None = None,
    recent_complete: bool = True,
    *,
    rolling_enabled: bool = False,
    rolling_complete: bool = False,
    rolling_retryable: bool = False,
) -> str:
    if stage == "CRAWL":
        return "NORMALIZE"
    if stage == "NORMALIZE":
        return "DEDUP"
    if stage == "DEDUP":
        return "AI_CLASSIFY"
    if stage == "AI_CLASSIFY":
        return "AI_VERIFY"
    if stage == "AI_VERIFY":
        return "ARCHIVE"
    if stage == "ARCHIVE":
        return "COVERAGE_AUDIT"
    if stage == "COVERAGE_AUDIT":
        if not recent_complete:
            return "RECENT_30D_PRIORITY"
        if rolling_enabled:
            return "ROLLING_24M_FULL_CITY_BACKFILL"
        if saturated:
            return "PDF_STAGE"
        if crawl_work_pending is True:
            return "CRAWL_AGAIN"
        return "RECOVER_MISSING"
    if stage == "RECOVER_MISSING":
        return "CRAWL_AGAIN"
    if stage == "CRAWL_AGAIN":
        return "COVERAGE_AUDIT"
    if stage == "RECENT_30D_PRIORITY":
        if not recent_complete:
            return "RECENT_30D_PRIORITY"
        return "RECENT_COVERAGE_AUDIT"
    if stage == "RECENT_COVERAGE_AUDIT":
        return "ROLLING_24M_FULL_CITY_BACKFILL" if rolling_enabled else ("CRAWL_AGAIN" if crawl_work_pending else "COVERAGE_AUDIT")
    if stage in ROLLING_STAGES:
        return "ROLLING_24M_FULL_CITY_BACKFILL" if not rolling_complete else "ROLLING_24M_COVERAGE_AUDIT"
    if stage == "ROLLING_24M_COVERAGE_AUDIT":
        return "ROLLING_24M_RECOVER_MISSING" if rolling_retryable else "ROLLING_24M_SATURATED"
    if stage == "ROLLING_24M_SATURATED":
        return "HISTORICAL_CRAWL_AGAIN"
    if stage == "HISTORICAL_CRAWL_AGAIN":
        return "NORMALIZE"
    if stage == "PDF_STAGE":
        return "PDF_VERIFY"
    if stage == "PDF_VERIFY":
        return "FINAL_AUDIT"
    if stage == "FINAL_AUDIT":
        return "COMPLETE"
    return "WAIT_CURRENT_RUN"


def write_blocked(paths: Paths, run: str, reason: str, details: dict[str, Any] | None = None) -> None:
    atomic_json(
        paths.blocked,
        {
            "status": "BLOCKED",
            "run_id": run,
            "reason_code": reason,
            "details": details or {},
            "created_at": utc_now(),
            "action": "no new worker will be started until the blocker is resolved",
        },
    )
    state_update(paths, {"status": "BLOCKED", "last_reason": reason, "last_error": reason})


def supervisor_decision(paths: Paths, config: dict[str, Any], dry_run: bool = False) -> int:
    state = ensure_initial_files(paths, config)
    processes = process_snapshot()
    disk = disk_status(paths, config)
    watchdog = progress_watchdog(paths, config, processes)
    safe_handoff = latest_safe_handoff(paths)
    terminal_handoff = None
    if disk["status"] == "STOP":
        write_blocked(paths, str(state.get("run_id") or run_id()), "DISK_FREE_SPACE_BELOW_STOP", disk)
        return 0
    if processes["autonomous_workers"]:
        state_update(paths, {"status": "WORKER_ACTIVE", "worker_pid": processes["autonomous_workers"][0]["pid"], "disk": disk})
        return 0
    if paths.stop.exists():
        state_update(paths, {"status": "STOPPED", "stage": "WAIT_CURRENT_RUN", "last_reason": "STOP_FILE_PRESENT", "disk": disk})
        return 0
    current_active = bool(processes["current_backfill"] or processes["legacy_supervisor"] or processes["external_writers"])
    recent_reconciliation = None
    if not current_active:
        terminal_handoff = finalize_terminal_run(paths, processes, config)
        if terminal_handoff is not None:
            safe_handoff = latest_safe_handoff(paths) or terminal_handoff
        recent_reconciliation = _reconcile_stale_recent_run(paths, processes)
        if recent_reconciliation is not None:
            safe_handoff = latest_safe_handoff(paths) or safe_handoff
    handoff = current_log_status(paths, active=current_active)
    report = {
        "automation_id": state.get("automation_id"),
        "checked_at": utc_now(),
        "dry_run": dry_run,
        "current_run_active": current_active,
        "current_backfill_processes": processes["current_backfill"],
        "legacy_supervisor_processes": processes["legacy_supervisor"],
        "external_writers": processes["external_writers"],
        "autonomous_workers": processes["autonomous_workers"],
        "handoff": handoff,
        "recent_reconciliation": recent_reconciliation,
        "terminal_handoff": terminal_handoff,
        "disk": disk,
        "watchdog": watchdog,
        "will_start_worker": False,
        "reason": "UNKNOWN",
    }
    if current_active:
        report["reason"] = "CURRENT_RUN_ACTIVE"
        atomic_json(paths.automation / "AUTOMATION_DRY_RUN_REPORT.json", report) if dry_run else None
        state_update(
            paths,
            {
                "status": "WAIT_CURRENT_RUN",
                "stage": "WAIT_CURRENT_RUN",
                "current_run_active": True,
                "worker_pid": None,
                "last_reason": "CURRENT_RUN_ACTIVE",
                "disk": disk,
            },
        )
        append_jsonl(paths.history, {"event": "supervisor_check", "status": "CURRENT_RUN_ACTIVE", "timestamp": utc_now(), "details": report})
        return 0
    if not handoff.get("safe_ended") and safe_handoff is None and terminal_handoff is None:
        report["reason"] = str(handoff.get("reason") or "CURRENT_RUN_SUMMARY_PENDING")
        atomic_json(paths.automation / "AUTOMATION_DRY_RUN_REPORT.json", report) if dry_run else None
        state_update(paths, {"status": "WAIT_CURRENT_RUN", "stage": "WAIT_CURRENT_RUN", "current_run_active": False, "last_reason": report["reason"], "disk": disk})
        return 0
    if dry_run:
        report["reason"] = "HANDOFF_READY_NO_WORKER_STARTED"
        report["will_start_worker"] = True
        atomic_json(paths.automation / "AUTOMATION_DRY_RUN_REPORT.json", report)
        return 0
    try:
        with FileLock(str(paths.lock), timeout=0.2):
            latest = read_json(paths.master, state)
            if latest.get("status") == "COMPLETE":
                return 0
            stage = str(latest.get("next_stage") or latest.get("stage") or "CRAWL")
            if stage == "WAIT_CURRENT_RUN":
                stage = "CRAWL"
            safe_handoff = latest_safe_handoff(paths)
            if safe_handoff and safe_handoff.get("route_to") in {
                "RECENT_30D_PRIORITY",
                "RECENT_COVERAGE_AUDIT",
                "ROLLING_24M_FULL_CITY_BACKFILL",
            }:
                stage = str(safe_handoff.get("route_to"))
                consume_safe_handoff(paths, safe_handoff)
                append_jsonl(
                    paths.history,
                    {
                        "event": "stage_route",
                        "run_id": str(latest.get("run_id") or ""),
                        "from_stage": str(latest.get("next_stage") or latest.get("stage") or ""),
                        "to_stage": stage,
                        "reason_code": "SAFE_HANDOFF_TO_RECENT_PRIORITY",
                        "timestamp": utc_now(),
                        "handoff": safe_handoff,
                    },
                )
            elif safe_handoff and stage in {"CRAWL", "CRAWL_AGAIN", "RECOVER_MISSING"}:
                stage = "COVERAGE_AUDIT"
                consume_safe_handoff(paths, safe_handoff)
                append_jsonl(
                    paths.history,
                    {
                        "event": "stage_route",
                        "run_id": str(latest.get("run_id") or ""),
                        "from_stage": str(latest.get("next_stage") or latest.get("stage") or ""),
                        "to_stage": stage,
                        "reason_code": "SAFE_HANDOFF_TO_RECENT_PRIORITY",
                        "timestamp": utc_now(),
                        "handoff": safe_handoff,
                    },
                )
            if stage == "RECOVER_MISSING":
                shard_audit = crawl_shard_summary(paths)
                if shard_audit.get("actionable"):
                    stage = "CRAWL_AGAIN"
                    append_jsonl(
                        paths.history,
                        {
                            "event": "stage_route",
                            "run_id": str(latest.get("run_id") or ""),
                            "from_stage": "RECOVER_MISSING",
                            "to_stage": "CRAWL_AGAIN",
                            "reason_code": "PENDING_SHARDS_REQUIRE_REAL_CRAWL",
                            "timestamp": utc_now(),
                            "shard_audit": shard_audit,
                        },
                    )
            # A failed handoff is a completed attempt, not a resumable stage
            # within the same run.  Reusing its identifier would overwrite the
            # failed run's current state and make recovery/audit ambiguous.
            worker_run = run_id() if handoff.get("status") == "TERMINAL_FAILED" or safe_handoff is not None or terminal_handoff is not None else str(latest.get("run_id") or run_id())
            worker = paths.project / ".venv" / "Scripts" / "python.exe"
            controller = Path(__file__).resolve()
            log_path = paths.supervisor_logs / f"{worker_run}_launcher.log"
            handle = log_path.open("a", encoding="utf-8")
            command = [str(worker), str(controller), "worker", "--project-root", str(paths.project), "--data-root", str(paths.data), "--config", str(paths.config), "--stage", stage, "--run-id", worker_run]
            flags = getattr(subprocess, "CREATE_NO_WINDOW", 0) | getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
            child = subprocess.Popen(command, cwd=str(paths.project), env=command_environment(paths), stdout=handle, stderr=subprocess.STDOUT, creationflags=flags)
            handle.close()
            state_update(paths, {"status": "WORKER_STARTED", "stage": stage, "next_stage": stage, "run_id": worker_run, "worker_pid": child.pid, "current_run_active": False, "last_reason": "WORKER_STARTED", "last_error": None, "disk": disk})
            append_jsonl(paths.history, {"event": "worker_started", "run_id": worker_run, "stage": stage, "pid": child.pid, "timestamp": utc_now(), "command_sha256": sha256_text("\0".join(command))})
    except Timeout:
        state_update(paths, {"status": "WORKER_ACTIVE", "last_reason": "AUTOMATION_LOCK_HELD"})
    return 0


def worker_run(paths: Paths, config: dict[str, Any], stage: str, run: str) -> int:
    if paths.stop.exists():
        state_update(paths, {"status": "STOPPED", "stage": stage, "last_reason": "STOP_FILE_PRESENT", "worker_pid": None})
        return 0
    processes = process_snapshot()
    if processes["current_backfill"] or processes["legacy_supervisor"]:
        state_update(paths, {"status": "WAIT_CURRENT_RUN", "stage": "WAIT_CURRENT_RUN", "last_reason": "CURRENT_RUN_ACTIVE", "worker_pid": None})
        return 0
    auto_continue = False
    try:
        lock = FileLock(str(paths.lock), timeout=0.2)
        lock.acquire()
    except Timeout:
        return 0
    try:
        disk = disk_status(paths, config)
        if disk["status"] == "STOP":
            write_blocked(paths, run, "DISK_FREE_SPACE_BELOW_STOP", disk)
            return 0
        if stage not in STAGES or stage in {"WAIT_CURRENT_RUN", "COMPLETE"}:
            stage = "CRAWL"
        transition(paths, run, stage, "RUNNING", "STAGE_STARTED", pid=os.getpid())
        atomic_json(paths.current_run, {"run_id": run, "stage": stage, "pid": os.getpid(), "status": "RUNNING", "started_at": utc_now()})
        ok, reason = run_stage(paths, config, stage, run)
        atomic_json(paths.current_run, {"run_id": run, "stage": stage, "pid": os.getpid(), "status": "COMPLETED" if ok else "FAILED", "reason": reason, "completed_at": utc_now()})
        if not ok and reason.startswith("BLOCKING:"):
            write_blocked(paths, run, reason.split(":", 1)[1])
            transition(paths, run, stage, "BLOCKED", reason.split(":", 1)[1])
            return 0
        if not ok:
            transition(paths, run, stage, "RETRY_WAIT", reason)
            state_update(paths, {"status": "RETRY_WAIT", "next_stage": stage, "worker_pid": None, "last_error": reason, "last_progress_at": utc_now()})
            return 0
        if reason == "DEFERRED_AI_QUEUE":
            atomic_json(paths.automation / "AI_QUEUE_STATE.json", {"status": "DEFERRED", "last_stage": stage, "reason": reason, "updated_at": utc_now()})
        if stage == "COVERAGE_AUDIT":
            coverage = coverage_summary(paths)
            atomic_json(paths.automation / "COVERAGE_STATE.json", coverage)
        else:
            coverage = read_json(paths.automation / "COVERAGE_STATE.json", {"saturated": False})
        saturated = bool(coverage.get("saturated") is True)
        shard_audit = crawl_shard_summary(paths)
        crawl_work_pending = (
            bool(shard_audit.get("actionable"))
            if shard_audit.get("actionable") is not None
            else None
        )
        recent_state = read_json(paths.automation / "RECENT_30D_STATE.json", {})
        recent_complete = recent_state.get("status") == "COMPLETE"
        rolling_state = read_json(paths.automation / "ROLLING_24M_STATE.json", {})
        rolling_complete = rolling_state.get("status") == "COMPLETE"
        rolling_retryable = int(rolling_state.get("retryable", rolling_state.get("partial", 0)) or 0) > 0
        rolling_config = config.get("rolling_24m") if isinstance(config.get("rolling_24m"), dict) else {}
        rolling_enabled = bool(rolling_config.get("enabled", True))
        next_stage = next_stage_after(
            stage,
            saturated,
            crawl_work_pending,
            recent_complete=recent_complete,
            rolling_enabled=rolling_enabled,
            rolling_complete=rolling_complete,
            rolling_retryable=rolling_retryable,
        )
        if next_stage == "COMPLETE":
            transition(paths, run, stage, "COMPLETE", "FINAL_AUDIT_PASSED")
            state_update(paths, {"status": "COMPLETE", "stage": "COMPLETE", "next_stage": "COMPLETE", "worker_pid": None, "last_progress_at": utc_now(), "coverage": coverage})
        else:
            transition(paths, run, stage, "COMPLETED", reason, next_stage=next_stage)
            state_update(paths, {"status": "READY_FOR_NEXT_STAGE", "stage": stage, "next_stage": next_stage, "worker_pid": None, "last_progress_at": utc_now(), "coverage": coverage, "last_error": None})
            auto_continue = next_stage not in {"COMPLETE", "WAIT_CURRENT_RUN"} and not paths.stop.exists()
        return 0
    except Exception as exc:  # noqa: BLE001 - persistent controller must classify unexpected failures
        reason = f"UNHANDLED:{type(exc).__name__}"
        log_line(paths.supervisor_logs / f"{run}_{stage}_controller.log", f"{reason}: {exc}")
        write_blocked(paths, run, reason, {"message": redact(str(exc))})
        return 1
    finally:
        try:
            lock.release()
        except Exception:  # noqa: BLE001 - releasing a best-effort process lock
            pass
        if auto_continue:
            _spawn_controller_tick(paths)


def status(paths: Paths) -> int:
    state = read_json(paths.master, {"status": "NOT_INSTALLED"})
    processes = process_snapshot()
    output = {
        "master_state": state,
        "current_run": read_json(paths.current_run, {}),
        "coverage": read_json(paths.automation / "COVERAGE_STATE.json", {}),
        "progress_watchdog": read_json(paths.automation / "PROGRESS_WATCHDOG.json", {}),
        "ai_queue": read_json(paths.automation / "AI_QUEUE_STATE.json", {}),
        "pdf_archive": read_json(paths.automation / "PDF_ARCHIVE_STATE.json", {}),
        "recent_30d": read_json(paths.automation / "RECENT_30D_STATE.json", {}),
        "processes": processes,
        "stop_file": paths.stop.exists(),
        "blocked_file": paths.blocked.exists(),
        "paths": {
            "automation": str(paths.automation),
            "database": str(paths.data / "database" / "policydb.duckdb"),
            "curated": str(paths.data / "curated"),
            "outputs": str(paths.data / "outputs"),
        },
        "checked_at": utc_now(),
    }
    print(json.dumps(output, ensure_ascii=False, indent=2, default=str))
    return 0


def install(paths: Paths, config: dict[str, Any]) -> int:
    ensure_initial_files(paths, config)
    atomic_json(paths.automation / "AUTOMATION_CONFIG.json", config)
    print(json.dumps({"status": "INSTALLED", "automation": str(paths.automation), "task_name": config["task_name"]}, ensure_ascii=False, indent=2))
    return 0


def dry_run(paths: Paths, config: dict[str, Any]) -> int:
    ensure_initial_files(paths, config)
    return supervisor_decision(paths, config, dry_run=True)


def stop(paths: Paths, reason: str) -> int:
    atomic_json(paths.stop, {"requested_at": utc_now(), "requested_by_pid": os.getpid(), "reason": redact(reason)})
    state_update(paths, {"status": "STOP_REQUESTED", "last_reason": "STOP_FILE_CREATED"})
    print(json.dumps({"status": "STOP_REQUESTED", "stop_file": str(paths.stop)}, ensure_ascii=False))
    return 0


def resume(paths: Paths) -> int:
    try:
        paths.stop.unlink()
    except FileNotFoundError:
        pass
    state = read_json(paths.master, {})
    if state.get("status") in {"STOPPED", "STOP_REQUESTED"}:
        state_update(paths, {"status": "READY", "last_reason": "STOP_CLEARED"})
    print(json.dumps({"status": "RESUME_READY", "stop_file": paths.stop.exists()}, ensure_ascii=False))
    return 0


def record_task(paths: Paths, task_name: str, action: str, interval_minutes: int, state: str) -> int:
    if action == "__GENERATE__":
        supervisor = paths.project / "scripts" / "CRPD_Autonomous_Supervisor.ps1"
        action = (
            f'powershell.exe -NoLogo -NoProfile -NonInteractive -ExecutionPolicy Bypass '
            f'-WindowStyle Hidden -File "{supervisor}" -ProjectRoot "{paths.project}" '
            f'-DataRoot "{paths.data}"'
        )
    atomic_json(
        paths.automation / "TASK_SCHEDULER_REGISTRATION.json",
        {
            "task_name": task_name,
            "state": state,
            "registered_at": utc_now(),
            "interval_minutes": interval_minutes,
            "action_executable": "powershell.exe",
            "action": redact(action),
            "working_directory": str(paths.project),
            "logon_type": "InteractiveToken",
            "multiple_instances": "IgnoreNew",
            "execution_time_limit": "PT0S",
            "project_root": str(paths.project),
            "data_root": str(paths.data),
            "secret_policy": "no secrets in task action or arguments",
        },
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("install", "dry-run", "supervisor", "worker", "status", "stop", "resume", "record-task"))
    parser.add_argument("--project-root", required=True)
    parser.add_argument("--data-root", required=True)
    parser.add_argument("--config", required=True)
    parser.add_argument("--stage", default="CRAWL")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--reason", default="operator_requested")
    parser.add_argument("--task-name", default="CRPD_Autonomous_Database_Completion")
    parser.add_argument("--action", default="powershell.exe")
    parser.add_argument("--interval-minutes", type=int, default=30)
    parser.add_argument("--task-state", default="UNKNOWN")
    return parser


def main(argv: Iterable[str] | None = None) -> int:
    args = build_parser().parse_args(list(argv) if argv is not None else None)
    project = Path(args.project_root).resolve()
    data = Path(args.data_root).resolve()
    paths = Paths(project=project, data=data, config=Path(args.config).resolve())
    config = load_config(paths)
    if args.command == "install":
        return install(paths, config)
    if args.command == "dry-run":
        return dry_run(paths, config)
    if args.command == "supervisor":
        return supervisor_decision(paths, config)
    if args.command == "worker":
        return worker_run(paths, config, args.stage, args.run_id or run_id())
    if args.command == "status":
        return status(paths)
    if args.command == "stop":
        return stop(paths, args.reason)
    if args.command == "resume":
        return resume(paths)
    return record_task(paths, args.task_name, args.action, args.interval_minutes, args.task_state)


if __name__ == "__main__":
    raise SystemExit(main())
