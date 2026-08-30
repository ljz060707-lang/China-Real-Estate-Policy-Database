"""EP930 final-convergence coordinator.

This module is deliberately a coordinator, not a crawler.  It performs a
small, read-only reconciliation of the existing EP930 evidence, writes a
root-document closure and a lightweight status snapshot, and starts the
official ``episode_930_autorun`` only after the single-writer audit is clear.
The coordinator never calls the provider itself and never mutates the raw
1575-item queue.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import os
import re
import subprocess
import sys
import time
from collections import defaultdict
from collections.abc import Iterable, Mapping
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import polars as pl
import psutil

from policydb.episode_930 import EPISODE_ID
from policydb.episode_930_production import (
    certification_gate_from_ledger,
    read_certification_ledger,
)

SCOPE_VERSION = "930-analysis-ready-v1"
SCOPE_HASH = "a751e9a99d405bc91dc4a5c4a19f900b398400e1e88b43f724e034209807a90d"
CORE_START = date(2016, 9, 25)
CORE_END = date(2016, 10, 10)
EXTENDED_START = date(2016, 9, 20)
EXTENDED_END = date(2016, 10, 15)
DISCOVERY_START = date(2016, 9, 1)
DISCOVERY_END = date(2016, 10, 31)
FINAL_MEMBERSHIP_STATUSES = {
    "CONFIRMED_EP930_CORE",
    "CONFIRMED_EP930_EXTENDED",
    "CONFIRMED_EP930_REPRINT",
    "CONFIRMED_OUTSIDE_EP930",
    "SUPPORTING_ONLY",
    "DUPLICATE_OR_REDUNDANT",
    "MANUAL_FINAL",
}
STOP_FILE_NAMES = ("STOP_EPISODE_930", "STOP_FULL_SYNC", "STOP_AUTOPILOT")


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _safe_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _json_bytes(value: Mapping[str, Any]) -> bytes:
    return (json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n").encode("utf-8")


def _write_if_changed(path: Path, payload: bytes) -> bool:
    try:
        if path.read_bytes() == payload:
            return False
    except OSError:
        pass
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f".{path.name}.tmp.{os.getpid()}")
    try:
        temp.write_bytes(payload)
        os.replace(temp, path)
    finally:
        temp.unlink(missing_ok=True)
    return True


def _write_json_if_changed(path: Path, value: Mapping[str, Any]) -> bool:
    return _write_if_changed(path, _json_bytes(value))


def _parse_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value or "").strip()
    if not text or text.lower() in {"none", "null", "nan"}:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        pass
    match = re.search(r"(20\d{2})[^0-9]{1,4}(\d{1,2})[^0-9]{1,4}(\d{1,2})", text)
    if not match:
        return None
    try:
        return date(int(match.group(1)), int(match.group(2)), int(match.group(3)))
    except ValueError:
        return None


def _dates_from_values(values: Iterable[Any]) -> list[date]:
    dates: list[date] = []
    for value in values:
        parsed = _parse_date(value)
        if parsed:
            dates.append(parsed)
        for match in re.finditer(r"(20\d{2})[^0-9]{1,4}(\d{1,2})[^0-9]{1,4}(\d{1,2})", str(value or "")):
            try:
                candidate = date(int(match.group(1)), int(match.group(2)), int(match.group(3)))
            except ValueError:
                continue
            if candidate not in dates:
                dates.append(candidate)
    return sorted(dates)


def classify_membership(row: Mapping[str, Any]) -> tuple[str, str, list[date]]:
    """Return a closed membership state without treating page dates as policy dates."""

    page_dates = _dates_from_values(
        (row.get("publication_date"), row.get("announcement_date"), row.get("reprint_date"), row.get("reprint_page_date"))
    )
    text = " ".join(
        str(row.get(key) or "")
        for key in ("official_text", "policy_title", "document_title", "action_text", "date_evidence_text")
    )
    text_dates = _dates_from_values((text,))
    all_dates = sorted(set(page_dates + text_dates))
    underlying_2016 = [d for d in text_dates if d.year == 2016]
    explicit_2016 = [d for d in _dates_from_values((row.get("underlying_policy_date"),)) if d.year == 2016]
    underlying_2016 = sorted(set(underlying_2016 + explicit_2016))

    if underlying_2016 and any(d.year > 2016 for d in page_dates):
        return (
            "CONFIRMED_EP930_REPRINT",
            "2016 underlying policy evidence is distinct from the later reprint page date",
            all_dates,
        )
    if any(CORE_START <= d <= CORE_END for d in underlying_2016):
        return "CONFIRMED_EP930_CORE", "underlying policy date is inside the frozen core window", all_dates
    if any(EXTENDED_START <= d <= EXTENDED_END for d in underlying_2016):
        return "CONFIRMED_EP930_EXTENDED", "underlying policy date is inside the extended episode window", all_dates
    if underlying_2016 and any(DISCOVERY_START <= d <= DISCOVERY_END for d in underlying_2016):
        return "MANUAL_FINAL", "2016 evidence exists but is outside the treatment windows or lacks a stable date basis", all_dates
    if any(d.year >= 2017 for d in all_dates):
        return "CONFIRMED_OUTSIDE_EP930", "available page and text evidence is later than the 2016 episode", all_dates
    if any(d.year == 2016 for d in all_dates):
        return "MANUAL_FINAL", "2016 is mentioned but the underlying policy date cannot be placed deterministically", all_dates
    return "MANUAL_FINAL", "no auditable underlying episode date was found", all_dates


def _latest_v3_release(data_root: Path) -> Path | None:
    candidates = list((data_root / "promotion_rehearsal").glob("*_CLOSURE_V3/release/EP930_FINAL_ROOT_OBJECTS.csv"))
    return max(candidates, key=lambda path: path.stat().st_mtime) if candidates else None


def _curated_documents(path: Path) -> dict[str, dict[str, Any]]:
    if not path.exists():
        return {}
    try:
        frame = pl.read_parquet(path)
    except Exception:
        return {}
    result: dict[str, dict[str, Any]] = {}
    for row in frame.iter_rows(named=True):
        document_id = str(row.get("document_id") or "")
        if document_id and document_id not in result:
            result[document_id] = row
    return result


def _root_rows(path: Path) -> dict[str, dict[str, Any]]:
    if not path.exists():
        return {}
    result: dict[str, dict[str, Any]] = {}
    with path.open(encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            document_id = str(row.get("document_id") or "")
            if document_id and document_id not in result:
                result[document_id] = row
    return result


def _csv_bytes(rows: list[dict[str, Any]], fieldnames: list[str]) -> bytes:
    stream = io.StringIO(newline="")
    writer = csv.DictWriter(stream, fieldnames=fieldnames, extrasaction="ignore", lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return ("\ufeff" + stream.getvalue()).encode("utf-8")


ROOT_FIELDS = [
    "document_id", "evidence_unit_id", "city", "issuer", "title", "document_number", "official_url",
    "publication_date", "announcement_date", "explicit_effective_date", "underlying_policy_date", "reprint_date",
    "episode_membership", "membership_reason", "direction_context", "geography_state", "date_state", "recovery_state",
    "treatment_path", "root_blockers",
]


def build_root_document_closure(data_root: Path, output: Path) -> dict[str, Any]:
    """Build one auditable row per existing root document; never writes curated data."""

    v3 = _latest_v3_release(data_root)
    if v3 is None:
        return {"status": "BLOCKED", "reason": "V3_ROOT_OBJECTS_NOT_FOUND", "documents": 0}
    roots = _root_rows(v3)
    curated = _curated_documents(data_root / "curated" / "policy_episode_documents.parquet")
    rows: list[dict[str, Any]] = []
    for document_id in sorted(roots):
        root = roots[document_id]
        doc = {**root, **curated.get(document_id, {})}
        underlying = _dates_from_values((doc.get("underlying_policy_date"),))
        if not underlying:
            text_dates = _dates_from_values((doc.get("official_text"), doc.get("document_title"), doc.get("policy_title"), doc.get("action_text")))
            underlying = [d for d in text_dates if d.year == 2016]
        page_dates = _dates_from_values((doc.get("publication_date"), doc.get("announcement_date")))
        membership, reason, all_dates = classify_membership({**doc, "underlying_policy_date": underlying[0].isoformat() if underlying else None})
        page_date = page_dates[0].isoformat() if page_dates else ""
        underlying_date = underlying[0].isoformat() if underlying else ""
        outside = membership == "CONFIRMED_OUTSIDE_EP930"
        raw_recovery = str(root.get("recovery_state") or "")
        if raw_recovery in {"REUSED", "RECOVERED", "EXCLUDED", "SOURCE_UNAVAILABLE_FINAL", "MANUAL_FINAL"}:
            recovery_state = raw_recovery
        elif outside:
            recovery_state = "EXCLUDED"
        else:
            recovery_state = "MANUAL_FINAL"
        blockers = "OUTSIDE_EPISODE_TREATMENT_PATH" if outside else str(root.get("root_blockers") or "")
        rows.append(
            {
                "document_id": document_id,
                "evidence_unit_id": str(root.get("evidence_unit_id") or f"EVIDENCE:{document_id}"),
                "city": str(doc.get("city") or root.get("city") or ""),
                "issuer": str(doc.get("issuer") or ""),
                "title": str(doc.get("document_title") or doc.get("policy_title") or ""),
                "document_number": str(doc.get("document_number") or ""),
                "official_url": str(doc.get("official_url") or doc.get("canonical_url") or doc.get("final_url") or ""),
                "publication_date": str(_parse_date(doc.get("publication_date")) or ""),
                "announcement_date": str(_parse_date(doc.get("announcement_date")) or ""),
                "explicit_effective_date": str(_parse_date(doc.get("effective_date")) or ""),
                "underlying_policy_date": underlying_date,
                "reprint_date": page_date if page_dates and any(d.year > 2016 for d in page_dates) else "",
                "episode_membership": membership,
                "membership_reason": reason,
                "direction_context": "NOT_REQUIRED_OUTSIDE_EPISODE" if outside else str(root.get("direction_state") or "UNRESOLVED"),
                "geography_state": "PASS" if str(doc.get("city") or root.get("city") or "") else "UNRESOLVED",
                "date_state": "OUTSIDE_TREATMENT_PATH" if outside else str(root.get("date_state") or "UNRESOLVED"),
                "recovery_state": recovery_state,
                "treatment_path": "EXCLUDED" if outside else ("INCLUDED" if membership.startswith("CONFIRMED_EP930_") else "MANUAL"),
                "root_blockers": blockers,
            }
        )
    target = output / "EP930_ROOT_DOCUMENT_CLOSURE.csv"
    _write_if_changed(target, _csv_bytes(rows, ROOT_FIELDS))
    counts: dict[str, int] = defaultdict(int)
    for row in rows:
        counts[row["episode_membership"]] += 1
    return {
        "status": "PASS" if rows and set(counts).issubset(FINAL_MEMBERSHIP_STATUSES) else "BLOCKED",
        "source_release": str(v3.parent.parent),
        "path": str(target),
        "documents": len(rows),
        "membership_counts": dict(sorted(counts.items())),
        "included_documents": sum(row["treatment_path"] == "INCLUDED" for row in rows),
        "all_dates_observed": sorted({d.isoformat() for row in rows for d in _dates_from_values((row["underlying_policy_date"],))}),
    }


RECOVERY_FIELDS = [
    "underlying_evidence_key", "queue_item_id", "city_id", "city", "source_role", "status", "recovery_required",
    "document_version_id", "crawl_item_id", "cache_hit", "real_network_fetch", "reason", "deduplicated_from_count",
]


def build_recovery_closure(data_root: Path, output: Path) -> dict[str, Any]:
    source = _latest_v3_release(data_root)
    path = source.parent / "EP930_RECOVERY_FINAL_DISPOSITION.csv" if source else None
    if path is None or not path.exists():
        return {"status": "BLOCKED", "reason": "RECOVERY_DISPOSITION_NOT_FOUND", "items": 0}
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    with path.open(encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            key = str(row.get("document_version_id") or row.get("crawl_item_id") or f"QUEUE:{row.get('queue_item_id') or ''}")
            groups[key].append(row)
    rows: list[dict[str, Any]] = []
    for key, group in sorted(groups.items()):
        first = group[0]
        cache_hit = str(first.get("cache_hit") or "").lower() == "true"
        network_fetch = str(first.get("real_network_fetch") or "").lower() == "true"
        if str(first.get("recovery_required") or "").lower() == "false":
            status = "EXCLUDED"
            reason = "not recovery-required in authoritative disposition"
        elif cache_hit:
            status = "REUSED"
            reason = "existing cache evidence is linked"
        elif network_fetch:
            status = "RECOVERED"
            reason = "real network fetch is recorded"
        elif "permanent" in str(first.get("failure_reason") or "").lower() or "404" in str(first.get("failure_reason") or ""):
            status = "SOURCE_UNAVAILABLE_FINAL"
            reason = str(first.get("failure_reason") or "permanent source failure")
        else:
            status = "MANUAL_FINAL"
            reason = "no document version or fetch evidence; retained for explicit final review"
        rows.append(
            {
                "underlying_evidence_key": key,
                "queue_item_id": str(first.get("queue_item_id") or ""),
                "city_id": str(first.get("city_id") or ""),
                "city": str(first.get("city") or ""),
                "source_role": str(first.get("source_role") or ""),
                "status": status,
                "recovery_required": str(first.get("recovery_required") or ""),
                "document_version_id": str(first.get("document_version_id") or ""),
                "crawl_item_id": str(first.get("crawl_item_id") or ""),
                "cache_hit": str(first.get("cache_hit") or ""),
                "real_network_fetch": str(first.get("real_network_fetch") or ""),
                "reason": reason,
                "deduplicated_from_count": len(group),
            }
        )
    target = output / "EP930_RECOVERY_FINAL_DISPOSITION_V4.csv"
    _write_if_changed(target, _csv_bytes(rows, RECOVERY_FIELDS))
    counts: dict[str, int] = defaultdict(int)
    for row in rows:
        counts[row["status"]] += 1
    return {"status": "PASS", "path": str(target), "items": len(rows), "status_counts": dict(sorted(counts.items()))}


def _process_commandline(proc: psutil.Process) -> str:
    try:
        return " ".join(proc.cmdline())
    except (psutil.AccessDenied, psutil.NoSuchProcess, psutil.ZombieProcess):
        return ""


def process_audit(exclude_pid: int | None = None) -> dict[str, Any]:
    writer_processes: list[dict[str, Any]] = []
    controllers: list[dict[str, Any]] = []
    for proc in psutil.process_iter(["pid", "name"]):
        if exclude_pid and proc.pid == exclude_pid:
            continue
        cmd = _process_commandline(proc)
        low = cmd.lower()
        if "episode_930_v4" in low or "final_convergence_autopilot_v4" in low:
            continue
        is_controller = "episode_930_autorun" in low and ("python" in low or "policydb" in low)
        is_backfill = "backfill_engine.py" in low and ("python" in low or "uv run" in low)
        is_worker = "policydb.jobs.worker" in low or "crawl.service" in low
        try:
            parent_pid = proc.ppid()
        except (psutil.AccessDenied, psutil.NoSuchProcess, psutil.ZombieProcess):
            parent_pid = None
        payload = {"pid": proc.pid, "parent_pid": parent_pid, "name": proc.info.get("name"), "command": cmd[:500]}
        if is_controller:
            controllers.append(payload)
        if is_backfill or is_worker:
            writer_processes.append({**payload, "kind": "backfill" if is_backfill else "worker"})
    writer_pids = {int(row["pid"]) for row in writer_processes}
    parent_by_pid = {int(row["pid"]): row.get("parent_pid") for row in writer_processes}
    chain_roots: set[int] = set()
    for pid in writer_pids:
        current = pid
        visited: set[int] = set()
        while current in parent_by_pid and current not in visited:
            visited.add(current)
            parent = parent_by_pid.get(current)
            if not isinstance(parent, int) or parent not in writer_pids:
                break
            current = parent
        chain_roots.add(current)
    controller_pids = {int(row["pid"]) for row in controllers}
    controller_parent_by_pid = {int(row["pid"]): row.get("parent_pid") for row in controllers}
    controller_roots: set[int] = set()
    for pid in controller_pids:
        current = pid
        visited: set[int] = set()
        while current in controller_parent_by_pid and current not in visited:
            visited.add(current)
            parent = controller_parent_by_pid.get(current)
            if not isinstance(parent, int) or parent not in controller_pids:
                break
            current = parent
        controller_roots.add(current)
    return {
        "writer_capable_processes": writer_processes,
        "writer_capable_process_count": len(writer_processes),
        "writer_chain_count": len(chain_roots),
        "writer_chain_roots": sorted(chain_roots),
        "official_controllers": controllers,
        "official_controller_roots": sorted(controller_roots),
        "official_controller_count": len(controller_roots),
    }


def _lock_audit(data_root: Path, exclude_pid: int | None = None) -> list[dict[str, Any]]:
    paths = [data_root / "logs" / "policydb-write.lock", data_root / "production" / "current" / "logs" / "policydb-write.lock"]
    audits: list[dict[str, Any]] = []
    for path in paths:
        if not path.exists():
            continue
        payload = _safe_json(path)
        pid = payload.get("pid")
        try:
            live = bool(pid and psutil.pid_exists(int(pid)) and int(pid) != int(exclude_pid or -1))
        except (TypeError, ValueError):
            live = True
        audits.append({"path": str(path), "pid": pid, "live": live, "job_id": payload.get("job_id")})
    return audits


def _scope_audit(data_root: Path) -> dict[str, Any]:
    source = _latest_v3_release(data_root)
    path = source.parent / "EP930_SCOPE_DEFINITION.json" if source else None
    scope = _safe_json(path) if path else {}
    actual = scope.get("scope_hash")
    return {
        "scope_version": scope.get("scope_version"),
        "scope_unit": "queue_item",
        "scope_city_count": scope.get("city_count"),
        "scope_queue_item_count": len(scope.get("queue_item_ids") or []),
        "scope_hash": actual,
        "frozen": bool(scope.get("frozen")),
        "scope_hash_unchanged": bool(scope.get("frozen")) and actual == SCOPE_HASH,
    }


def _queue_metrics(monitor: Mapping[str, Any], state: Mapping[str, Any]) -> dict[str, Any]:
    reconciliation = monitor.get("queue_reconciliation") or {}
    queue = state.get("queue") or {}
    total = int(reconciliation.get("total") or queue.get("total") or 0)
    statuses = reconciliation.get("accounted_statuses") or {}
    accounted = int(reconciliation.get("accounted_total") or sum(int(v or 0) for v in statuses.values()))
    return {
        "total": total,
        "raw_completed": int(monitor.get("raw_queue_completed") or queue.get("completed") or 0),
        "accounted_total": accounted,
        "consistent": bool(reconciliation.get("consistent")) and accounted == total == 1575,
        "accounted_statuses": statuses,
    }


def _api_summary(output: Path, monitor: Mapping[str, Any] | None = None) -> dict[str, Any]:
    state = _safe_json(output / "930_API_RECOVERY_STATE.json")
    provider = _safe_json(output / "930_API_PROVIDER_STATUS.json")
    certification = certification_gate_from_ledger(read_certification_ledger(output))
    health = (monitor or {}).get("api_health") or {}
    next_retry = state.get("next_retry_at")
    due = True
    if next_retry:
        try:
            due = datetime.fromisoformat(str(next_retry).replace("Z", "+00:00")) <= datetime.now(UTC)
        except ValueError:
            due = True
    return {
        "provider": provider.get("provider"),
        "model": provider.get("model"),
        "provider_status": provider.get("status"),
        "phase": state.get("phase"),
        "last_attempt_at": state.get("last_attempt_at"),
        "next_retry_at": next_retry,
        "retry_due": due,
        "last_success_documents": state.get("last_success_documents"),
        "last_success_rate": state.get("last_success_rate"),
        "schema_valid": state.get("schema_valid"),
        "certification": certification["certification"],
        "certification_reason": certification["reason_code"],
        "certification_stages": certification["stages"],
        "core_pass1_waiting": int(state.get("core_pass1_waiting") or health.get("core_pass1_waiting") or 0),
        "core_pass1_success": int(state.get("core_pass1_success") or health.get("core_pass1_success") or 0),
        "core_pass2_waiting": int(state.get("core_pass2_waiting") or health.get("core_pass2_waiting") or 0),
        "manual_api_calls": 0,
    }


def _controller_command(repo_root: Path, output: Path, data_root: Path) -> list[str]:
    python = repo_root / ".venv" / "Scripts" / "python.exe"
    if not python.exists():
        python = Path(sys.executable)
    return [
        str(python), "-m", "policydb.episode_930_autorun",
        "--output", str(output), "--city-limit", "1", "--max-ai-calls", "1",
        "--max-fetches", "1", "--poll-seconds", "15", "--max-cycles", "1",
    ]


def _start_official_controller(repo_root: Path, data_root: Path, output: Path) -> dict[str, Any]:
    stdout = output / "EP930_V4_CONTROLLER.stdout.log"
    stderr = output / "EP930_V4_CONTROLLER.stderr.log"
    env = os.environ.copy()
    env["CRPD_DATA_ROOT"] = str(data_root)
    env["PYTHONPATH"] = str(repo_root / "src") + os.pathsep + env.get("PYTHONPATH", "")
    command = _controller_command(repo_root, output, data_root)
    creationflags = 0
    if os.name == "nt":
        creationflags = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0) | getattr(subprocess, "DETACHED_PROCESS", 0)
    try:
        with stdout.open("ab") as out, stderr.open("ab") as err:
            process = subprocess.Popen(
                command,
                cwd=str(repo_root),
                env=env,
                stdin=subprocess.DEVNULL,
                stdout=out,
                stderr=err,
                close_fds=True,
                creationflags=creationflags,
            )
    except OSError as exc:
        return {"started": False, "reason": f"CONTROLLER_START_ERROR:{type(exc).__name__}"}
    return {"started": True, "pid": process.pid, "started_at": _now(), "command": command}


def _previous_controller_is_live(previous: Mapping[str, Any]) -> bool:
    """Return whether the last recorded controller launch still owns a live PID.

    A launch timestamp alone is not a retry backoff.  The V4 coordinator may
    only suppress a due retry while the controller started by that launch is
    still alive; an exited or failed launch must not create an artificial
    cooldown window.
    """

    launch = previous.get("official_controller", {}).get("last_launch")
    if not isinstance(launch, Mapping) or not launch.get("started"):
        return False
    try:
        pid = int(launch.get("pid"))
    except (TypeError, ValueError):
        return False
    try:
        process = psutil.Process(pid)
        if not process.is_running():
            return False
        try:
            command = " ".join(process.cmdline()).lower()
        except psutil.AccessDenied:
            # A live but unreadable PID is safer to treat as active than to
            # risk starting a second official controller.
            return True
        return "episode_930_autorun" in command
    except (psutil.NoSuchProcess, psutil.ZombieProcess):
        return False
    except psutil.AccessDenied:
        return True


def run_cycle(repo_root: Path, data_root: Path, output: Path, *, own_pid: int | None = None) -> dict[str, Any]:
    output.mkdir(parents=True, exist_ok=True)
    monitor = _safe_json(output / "930_MONITOR_SNAPSHOT.json")
    state = _safe_json(output / "930_AUTORUN_STATE.json")
    scope = _scope_audit(data_root)
    root = build_root_document_closure(data_root, output)
    recovery = build_recovery_closure(data_root, output)
    processes = process_audit(own_pid)
    locks = _lock_audit(data_root, own_pid)
    queue = _queue_metrics(monitor, state)
    api = _api_summary(output, monitor)
    stop_files = [str(data_root / "control" / name) for name in STOP_FILE_NAMES if (data_root / "control" / name).exists()]
    live_writer_lock = any(item.get("live") for item in locks)
    writer_busy = processes["writer_chain_count"] > 0 or live_writer_lock
    previous = _safe_json(output / "EP930_CONVERGENCE_STATUS.json")
    last_launch_at = previous.get("official_controller", {}).get("last_launch_at")
    launch_cooldown = False
    if last_launch_at:
        try:
            recent_launch = (datetime.now(UTC) - datetime.fromisoformat(str(last_launch_at).replace("Z", "+00:00"))).total_seconds() < 600
            launch_cooldown = recent_launch and _previous_controller_is_live(previous)
        except ValueError:
            launch_cooldown = False

    launch: dict[str, Any] = {"started": False, "reason": "NOT_ELIGIBLE"}
    can_start = (
        queue["consistent"]
        and not stop_files
        and scope["scope_hash_unchanged"]
        and not writer_busy
        and processes["official_controller_count"] == 0
        and api["retry_due"]
        and not launch_cooldown
    )
    if can_start:
        launch = _start_official_controller(repo_root, data_root, output)
        if launch.get("started"):
            launch["reason"] = "SINGLE_WRITER_CLEAR_AND_OFFICIAL_RETRY_DUE"
    decision = "CONTROLLER_RUNNING" if processes["official_controller_count"] else "WAITING_SINGLE_WRITER" if writer_busy else "WAITING_API_RETRY_OR_ROOT_EVIDENCE"
    if launch.get("started"):
        decision = "OFFICIAL_CONTROLLER_STARTED"
    if stop_files:
        decision = "SAFETY_STOP_FILE_PRESENT"
    if not queue["consistent"] or not scope["scope_hash_unchanged"]:
        decision = "SAFETY_BLOCKED_RECONCILIATION_OR_SCOPE"
    membership_counts = root.get("membership_counts") or {}
    current_root_blockers: list[str] = []
    if int(membership_counts.get("MANUAL_FINAL", 0)):
        current_root_blockers.append("MEMBERSHIP_MANUAL_FINAL")
    if root.get("included_documents", 0) == 0:
        current_root_blockers.append("NO_IN_SCOPE_TREATMENT_EVIDENCE")
    status = {
        "automation_id": "ep930-final-convergence-autopilot-v4",
        "updated_at": _now(),
        "episode_id": EPISODE_ID,
        "decision": decision,
        "root_document_closure": root,
        "recovery_closure": recovery,
        "queue": queue,
        "scope": scope,
        "processes": processes,
        "writer_locks": locks,
        "coordination": {
            "single_writer_required": True,
            "writer_busy": writer_busy,
            "writer_capable_process_count": processes["writer_capable_process_count"],
            "writer_chain_count": processes["writer_chain_count"],
            "shared_scheduler_episode_task_available": False,
            "shared_scheduler_reason": "backfill master has no EP930-specific task; existing HIGH tasks are ordinary annual tasks",
            "maintenance_handoff": "WAIT_FOR_NATURAL_WRITER_BOUNDARY",
            "raw_queue_mutated": False,
            "manual_api_calls": 0,
        },
        "api": api,
        "official_controller": {
            "active_count": processes["official_controller_count"] + int(bool(launch.get("started"))),
            "active_pids": (processes.get("official_controller_roots") or [
                row["pid"] for row in processes["official_controllers"]
            ]) + ([launch["pid"]] if launch.get("started") else []),
            "last_launch_at": launch.get("started_at") or last_launch_at,
            "last_launch": launch,
        },
        "gates": {
            "membership_unresolved": int(membership_counts.get("MANUAL_FINAL", 0)),
            "direction_unresolved_included": 0 if root.get("included_documents", 0) == 0 else None,
            "date_unresolved_included": 0 if root.get("included_documents", 0) == 0 else None,
            "api_certification": api.get("certification", "BLOCKED_BY_CERTIFICATION_BATCH"),
            "formal_actions": 16,
            "new_formal_actions": 0,
            "preserved_isolated_formal_actions": 16,
            "release_validator": "NOT_RUN_UPSTREAM_GATE_OPEN",
            "promotion": "DO_NOT_PROMOTE",
            "blockers": current_root_blockers,
        },
        "safety": {
            "frozen_scope_hash_unchanged": scope["scope_hash_unchanged"],
            "queue_reconciliation_1575": queue["consistent"],
            "single_writer_invariant": processes["writer_chain_count"] <= 1 and sum(bool(item.get("live")) for item in locks) <= 1,
            "full_crawl_started": False,
            "valid_document_versions_refetched": False,
        },
    }
    _write_json_if_changed(output / "EP930_CONVERGENCE_STATUS.json", status)
    if launch.get("started"):
        with (output / "EP930_V4_CONTROLLER_LAUNCHES.jsonl").open("a", encoding="utf-8") as stream:
            stream.write(json.dumps({"timestamp": _now(), **launch}, ensure_ascii=False, default=str) + "\n")
    return status


class V4Lock:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.acquired = False

    def acquire(self) -> None:
        payload = _json_bytes({"pid": os.getpid(), "acquired_at": _now(), "automation_id": "ep930-final-convergence-autopilot-v4"})
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            handle = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError as exc:
            current = _safe_json(self.path)
            pid = current.get("pid")
            if isinstance(pid, int) and psutil.pid_exists(pid):
                raise RuntimeError(f"EP930 V4 already active: pid={pid}") from exc
            stale = self.path.with_name(f"{self.path.name}.stale.{int(time.time())}")
            os.replace(self.path, stale)
            handle = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        with os.fdopen(handle, "wb") as stream:
            stream.write(payload)
        self.acquired = True

    def release(self) -> None:
        if self.acquired:
            self.path.unlink(missing_ok=True)
            self.acquired = False


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="EP930 V4 convergence coordinator")
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parents[2])
    parser.add_argument("--data-root", type=Path, default=None)
    parser.add_argument("--output", type=Path, default=None)
    parser.add_argument("--poll-seconds", type=int, default=90)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--max-runtime-seconds", type=int, default=0)
    args = parser.parse_args(argv)
    repo_root = args.repo_root.resolve()
    data_root = (args.data_root or Path(os.environ.get("CRPD_DATA_ROOT", r"E:\Data Set\CRPD"))).resolve()
    output = (args.output or data_root / "outputs" / "special_projects" / "2016_930").resolve()
    lock = V4Lock(output / "EP930_V4_AUTOPILOT.lock")
    try:
        lock.acquire()
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 20
    started = time.monotonic()
    try:
        while True:
            run_cycle(repo_root, data_root, output, own_pid=os.getpid())
            if args.once or (args.max_runtime_seconds and time.monotonic() - started >= args.max_runtime_seconds):
                return 0
            time.sleep(max(30, args.poll_seconds))
    finally:
        lock.release()


__all__ = [
    "FINAL_MEMBERSHIP_STATUSES",
    "SCOPE_HASH",
    "SCOPE_VERSION",
    "build_recovery_closure",
    "build_root_document_closure",
    "classify_membership",
    "main",
    "process_audit",
    "run_cycle",
]


if __name__ == "__main__":
    raise SystemExit(main())
