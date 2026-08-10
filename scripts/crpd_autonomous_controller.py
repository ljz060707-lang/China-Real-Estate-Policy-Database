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
    "RECOVER_MISSING",
    "CRAWL_AGAIN",
    "PDF_STAGE",
    "PDF_VERIFY",
    "FINAL_AUDIT",
    "COMPLETE",
)
AI_STAGES = {"AI_CLASSIFY", "AI_VERIFY"}
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
        "pdf_limit": 30,
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
    return state


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


def current_log_status(paths: Paths) -> dict[str, Any]:
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
    checkpoint = any(
        path.is_file() and path.stat().st_size > 0
        for path in (
            paths.data / "curated" / "crawl_shards.parquet",
            paths.data / "curated" / "crawl_items.parquet",
        )
    ) or any(path.is_file() and path.stat().st_size > 0 for path in (paths.data / "checkpoints").glob("*") if (paths.data / "checkpoints").exists())
    return {
        "exists": True,
        "safe_ended": bool(marker and checkpoint),
        "reason": "RUN_SUMMARY_AND_CHECKPOINT_READY" if marker and checkpoint else "RUN_SUMMARY_PENDING",
        "run_dir": str(latest),
        "master_log": str(master),
        "summary_marker": marker,
        "checkpoint_ready": checkpoint,
        "last_write_at": dt.datetime.fromtimestamp(master.stat().st_mtime, UTC).isoformat(),
    }


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
    if stage in {"CRAWL", "RECOVER_MISSING", "CRAWL_AGAIN"}:
        return [[str(policydb), "crawl", "recover-missing", "--max-fetches", recovery]]
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
    while process.poll() is None or not output_queue.empty():
        try:
            while True:
                item = output_queue.get_nowait()
                if item is None:
                    break
                log_line(log_path, item)
        except queue.Empty:
            pass
        time.sleep(2)
    reader.join(timeout=5)
    try:
        while True:
            item = output_queue.get_nowait()
            if item is None:
                continue
            log_line(log_path, item)
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


def run_stage(paths: Paths, config: dict[str, Any], stage: str, run: str) -> tuple[bool, str]:
    specs = command_specs(paths, config, stage, run)
    for index, argv in enumerate(specs, 1):
        code = run_command(paths, argv, run, f"{stage}_{index}")
        if code != 0:
            blocker, reason = stage_output_has_blocker(paths, run, f"{stage}_{index}")
            if blocker:
                return False, f"BLOCKING:{reason}"
            if stage in AI_STAGES:
                return True, "DEFERRED_AI_QUEUE"
            return False, f"COMMAND_FAILED:{code}"
    return True, "SUCCESS"


def next_stage_after(stage: str, saturated: bool) -> str:
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
        return "PDF_STAGE" if saturated else "RECOVER_MISSING"
    if stage == "RECOVER_MISSING":
        return "CRAWL_AGAIN"
    if stage == "CRAWL_AGAIN":
        return "COVERAGE_AUDIT"
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
    handoff = current_log_status(paths)
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
        "disk": disk,
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
    if not handoff.get("safe_ended"):
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
            worker_run = str(latest.get("run_id") or run_id())
            worker = paths.project / ".venv" / "Scripts" / "python.exe"
            controller = Path(__file__).resolve()
            log_path = paths.supervisor_logs / f"{worker_run}_launcher.log"
            handle = log_path.open("a", encoding="utf-8")
            command = [str(worker), str(controller), "worker", "--project-root", str(paths.project), "--data-root", str(paths.data), "--config", str(paths.config), "--stage", stage, "--run-id", worker_run]
            flags = getattr(subprocess, "CREATE_NO_WINDOW", 0) | getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
            child = subprocess.Popen(command, cwd=str(paths.project), env=command_environment(paths), stdout=handle, stderr=subprocess.STDOUT, creationflags=flags)
            handle.close()
            state_update(paths, {"status": "WORKER_STARTED", "stage": stage, "next_stage": stage, "run_id": worker_run, "worker_pid": child.pid, "current_run_active": False, "last_reason": "WORKER_STARTED", "disk": disk})
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
        next_stage = next_stage_after(stage, saturated)
        if next_stage == "COMPLETE":
            transition(paths, run, stage, "COMPLETE", "FINAL_AUDIT_PASSED")
            state_update(paths, {"status": "COMPLETE", "stage": "COMPLETE", "next_stage": "COMPLETE", "worker_pid": None, "last_progress_at": utc_now(), "coverage": coverage})
        else:
            transition(paths, run, stage, "COMPLETED", reason, next_stage=next_stage)
            state_update(paths, {"status": "READY_FOR_NEXT_STAGE", "stage": stage, "next_stage": next_stage, "worker_pid": None, "last_progress_at": utc_now(), "coverage": coverage, "last_error": None})
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


def status(paths: Paths) -> int:
    state = read_json(paths.master, {"status": "NOT_INSTALLED"})
    processes = process_snapshot()
    output = {
        "master_state": state,
        "current_run": read_json(paths.current_run, {}),
        "coverage": read_json(paths.automation / "COVERAGE_STATE.json", {}),
        "ai_queue": read_json(paths.automation / "AI_QUEUE_STATE.json", {}),
        "pdf_archive": read_json(paths.automation / "PDF_ARCHIVE_STATE.json", {}),
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
