"""Codex-independent bounded-job runner for the 2016 930 episode.

This module is intentionally only a scheduler adapter.  It does not discover
URLs, fetch pages, or write curated tables itself.  Each iteration creates one
formal ``historical_episode_930`` JobManager job; the existing worker owns the
network, checkpoint, archive, promotion, and single-writer boundaries.

The runner is safe to leave outside the Codex session.  It adopts an existing
live episode worker instead of creating a second one, requests cancellation
through the durable JobManager flag when a STOP file appears, and only starts
the next bounded job after the previous worker reaches a terminal state.
"""

from __future__ import annotations

import argparse
import json
import os
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import psutil

from policydb.episode_930 import EPISODE_ID
from policydb.episode_930_monitor import EXECUTION_MODE
from policydb.jobs.manager import JobManager, atomic_json
from policydb.jobs.models import CrawlJobRequest, JobState
from policydb.parquet_store import read_parquet_snapshot
from policydb.settings import Settings

TERMINAL_STATUSES = {"completed", "completed_with_warnings", "failed", "cancelled"}
STOP_FILE_NAMES = ("STOP_EPISODE_930", "STOP_FULL_SYNC", "STOP_AUTOPILOT")
FINAL_CONVERGENCE_COOLDOWN_SECONDS = 300


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _alive(pid: int | None) -> bool:
    return bool(pid and pid > 0 and psutil.pid_exists(pid))


def _json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


class Episode930AutoRunner:
    """Run resumable bounded episode jobs without becoming a crawler."""

    def __init__(
        self,
        settings: Settings | None = None,
        *,
        output: Path | None = None,
        city_limit: int = 5,
        max_ai_calls: int = 10,
        max_fetches: int = 30,
        poll_seconds: int = 15,
        max_cycles: int = 0,
        max_consecutive_failures: int = 3,
    ) -> None:
        self.settings = settings or Settings.discover()
        self.output = (
            output
            or self.settings.outputs / "special_projects" / "2016_930"
        ).resolve()
        self.output.mkdir(parents=True, exist_ok=True)
        self.state_path = self.output / "930_AUTORUN_STATE.json"
        self.events_path = self.output / "930_AUTORUN_EVENTS.jsonl"
        self.lock_path = self.output / "930_AUTORUN.lock"
        self.city_limit = max(1, min(int(city_limit), 105))
        self.max_ai_calls = max(0, int(max_ai_calls))
        self.max_fetches = max(1, int(max_fetches))
        self.poll_seconds = max(2, int(poll_seconds))
        self.max_cycles = max(0, int(max_cycles))
        self.max_consecutive_failures = max(1, int(max_consecutive_failures))
        self._lock_acquired = False

    def _stop_paths(self) -> list[Path]:
        return [self.settings.control / name for name in STOP_FILE_NAMES]

    def stop_requested(self) -> bool:
        return any(path.exists() for path in self._stop_paths())

    def _queue_summary(self) -> dict[str, int]:
        path = self.output / "930_TASK_QUEUE.parquet"
        if not path.exists():
            return {"total": 0, "completed": 0, "pending": 0, "running": 0, "failed": 0}
        try:
            frame = read_parquet_snapshot(path)
        except Exception:
            return {"total": -1, "completed": -1, "pending": -1, "running": -1, "failed": -1}
        if frame.is_empty() or "status" not in frame.columns:
            return {"total": frame.height, "completed": 0, "pending": 0, "running": 0, "failed": 0}
        status = frame.get_column("status")
        return {
            "total": frame.height,
            "completed": int(status.is_in(["CRAWL_COMPLETED", "COMPLETED"]).sum()),
            "pending": int(status.is_in(["PENDING", "RETRY_WAIT"]).sum()),
            "running": int(status.eq("RUNNING").sum()),
            "failed": int(status.eq("FAILED").sum()),
        }

    def _final_convergence_state(self) -> dict[str, Any]:
        """Describe post-queue work that must keep the formal runner alive.

        Queue completion only proves that the raw episode queue has no active
        crawl items. API recovery, frozen-core recovery, and cached evidence
        closure are separate durable lanes and may still be required. This
        method reads their state without issuing network requests or changing
        any queue/table.
        """

        monitor = _json(self.output / "930_MONITOR_SNAPSHOT.json")
        api_health = monitor.get("api_health") if isinstance(monitor.get("api_health"), dict) else {}
        api_state = _json(self.output / "930_API_RECOVERY_STATE.json")
        gate = _json(self.output / "930_ANALYSIS_READY_GATE.json")
        discovery = monitor.get("analysis_ready_discovery_progress")
        discovery = discovery if isinstance(discovery, dict) else {}

        def as_int(value: Any) -> int:
            try:
                return max(0, int(value or 0))
            except (TypeError, ValueError):
                return 0

        pass1_waiting = max(
            as_int(api_health.get("pass1_waiting")),
            as_int(gate.get("pass1_waiting")),
        )
        pass2_waiting = max(
            as_int(api_health.get("pass2_waiting")),
            as_int(gate.get("pass2_waiting")),
        )
        pass2_not_eligible = max(
            as_int(api_health.get("pass2_not_yet_eligible")),
            as_int(gate.get("pass2_not_yet_eligible")),
        )
        recovery_pending = 0
        recovery_path = self.output / "930_API_RECOVERY_QUEUE.parquet"
        if recovery_path.exists():
            try:
                recovery = read_parquet_snapshot(recovery_path)
                if not recovery.is_empty():
                    if "recovery_status" in recovery.columns:
                        resolved = {
                            "RECOVERED",
                            "RECOVERED_LOCAL_REPLAY",
                            "COMPLETED",
                            "RESOLVED",
                        }
                        recovery_pending = int(
                            (~recovery.get_column("recovery_status").is_in(sorted(resolved))).sum()
                        )
                    elif "status" in recovery.columns:
                        resolved = {"RECOVERED", "COMPLETED", "RESOLVED"}
                        recovery_pending = int(
                            (~recovery.get_column("status").is_in(sorted(resolved))).sum()
                        )
            except Exception:
                # A malformed optional recovery artifact must not make the
                # runner claim COMPLETE. Leave the lane pending so the
                # formal worker can surface the durable error.
                recovery_pending = 1

        false_recovery_pending = as_int(monitor.get("false_completion_recovery_required"))
        claim_audit = monitor.get("recovery_claim_audit")
        claim_audit = claim_audit if isinstance(claim_audit, dict) else {}
        core_recovery_pending = max(
            as_int(discovery.get("core_recovery_required")),
            as_int(claim_audit.get("core_required")),
        )
        pending = bool(
            pass1_waiting
            or pass2_waiting
            or pass2_not_eligible
            or recovery_pending
            or false_recovery_pending
            or core_recovery_pending
        )

        next_retry = (
            api_state.get("next_retry_at")
            or api_health.get("next_retry_at")
            or (monitor.get("api_recovery") or {}).get("next_retry_at")
        )
        parsed_retry: datetime | None = None
        if next_retry:
            try:
                parsed_retry = datetime.fromisoformat(str(next_retry).replace("Z", "+00:00"))
                if parsed_retry.tzinfo is None:
                    parsed_retry = parsed_retry.replace(tzinfo=UTC)
            except ValueError:
                parsed_retry = None
        fingerprint = json.dumps(
            {
                "next_retry_at": next_retry,
                "last_attempt_at": api_state.get("last_attempt_at") or api_health.get("last_attempt_at"),
                "phase": api_state.get("phase") or api_health.get("recovery_phase"),
                "pass1_waiting": pass1_waiting,
                "pass2_waiting": pass2_waiting,
                "pass2_not_yet_eligible": pass2_not_eligible,
                "recovery_pending": recovery_pending,
                "false_recovery_pending": false_recovery_pending,
                "core_recovery_pending": core_recovery_pending,
            },
            sort_keys=True,
        )
        prior = _json(self.state_path)
        prior_attempt = prior.get("final_convergence_last_attempt_at")
        cooldown_active = False
        if prior_attempt and prior.get("final_convergence_fingerprint") == fingerprint:
            try:
                attempt_time = datetime.fromisoformat(str(prior_attempt).replace("Z", "+00:00"))
                if attempt_time.tzinfo is None:
                    attempt_time = attempt_time.replace(tzinfo=UTC)
                cooldown_active = (
                    datetime.now(UTC) - attempt_time
                ).total_seconds() < FINAL_CONVERGENCE_COOLDOWN_SECONDS
            except ValueError:
                cooldown_active = False
        retry_due = parsed_retry is None or parsed_retry <= datetime.now(UTC)
        due = bool(pending and retry_due and not cooldown_active)
        reasons: list[str] = []
        if pass1_waiting or pass2_waiting or pass2_not_eligible:
            reasons.append("API_RECOVERY_PENDING")
        if recovery_pending:
            reasons.append("API_RECOVERY_QUEUE_PENDING")
        if false_recovery_pending:
            reasons.append("FALSE_COMPLETION_RECOVERY_PENDING")
        if core_recovery_pending:
            reasons.append("ANALYSIS_READY_CORE_RECOVERY_PENDING")
        return {
            "pending": pending,
            "due": due,
            "next_retry_at": next_retry,
            "pass1_waiting": pass1_waiting,
            "pass2_waiting": pass2_waiting,
            "pass2_not_yet_eligible": pass2_not_eligible,
            "recovery_pending": recovery_pending,
            "false_recovery_pending": false_recovery_pending,
            "core_recovery_pending": core_recovery_pending,
            "fingerprint": fingerprint,
            "cooldown_active": cooldown_active,
            "reason_codes": reasons,
        }

    def _write_state(self, **updates: Any) -> dict[str, Any]:
        state = _json(self.state_path)
        base: dict[str, Any] = {
            "episode_id": EPISODE_ID,
            "execution_mode": EXECUTION_MODE,
            "status": "INITIALIZING",
            "runner_pid": os.getpid(),
            "current_job_id": None,
            "cycles_started": 0,
            "cycles_completed": 0,
            "consecutive_failures": 0,
            "queue": self._queue_summary(),
            "stop_requested": self.stop_requested(),
            "stop_files": [str(path) for path in self._stop_paths() if path.exists()],
            "last_real_progress_at": _json(self.output / "930_PROGRESS_SNAPSHOT.json").get("last_real_progress_at"),
            "heartbeat_at": _now(),
            "updated_at": _now(),
            "runner_version": "930-autorun-v1",
        }
        base.update(state)
        base.update(updates)
        base["queue"] = self._queue_summary()
        base["stop_requested"] = self.stop_requested()
        base["stop_files"] = [str(path) for path in self._stop_paths() if path.exists()]
        # These fields describe this live runner, not the previous state file.
        # Reassert them after merging the durable state so a resumed process
        # cannot report a stale PID or stale business-progress timestamp.
        base["runner_pid"] = os.getpid()
        base["execution_mode"] = EXECUTION_MODE
        snapshot = _json(self.output / "930_PROGRESS_SNAPSHOT.json")
        if snapshot.get("last_real_progress_at"):
            base["last_real_progress_at"] = snapshot["last_real_progress_at"]
        base["episode_status"] = snapshot.get("status") or base.get("episode_status")
        base["last_micro_batch_status"] = snapshot.get("last_micro_batch_status") or base.get("last_micro_batch_status")
        base["next_batch_status"] = snapshot.get("next_batch_status") or base.get("next_batch_status")
        base["heartbeat_at"] = _now()
        base["updated_at"] = base["heartbeat_at"]
        atomic_json(self.state_path, base)
        return base

    def _event(self, event: str, **data: Any) -> None:
        self.events_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {"event": event, "episode_id": EPISODE_ID, "timestamp": _now(), **data}
        with self.events_path.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(payload, ensure_ascii=False, default=str) + "\n")

    def acquire_lock(self) -> None:
        self.output.mkdir(parents=True, exist_ok=True)
        payload = {"pid": os.getpid(), "episode_id": EPISODE_ID, "acquired_at": _now()}
        try:
            handle = os.open(self.lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError as exc:
            current = _json(self.lock_path)
            pid = current.get("pid")
            if _alive(int(pid)) if isinstance(pid, int) or str(pid).isdigit() else False:
                raise RuntimeError(f"930 autorunner already active: pid={pid}") from exc
            stale = self.lock_path.with_name(f"{self.lock_path.name}.stale.{int(time.time())}")
            os.replace(self.lock_path, stale)
            handle = os.open(self.lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        with os.fdopen(handle, "w", encoding="utf-8") as stream:
            json.dump(payload, stream, ensure_ascii=False)
        self._lock_acquired = True
        self._event("runner_started", pid=os.getpid())

    def release_lock(self) -> None:
        if self._lock_acquired:
            self.lock_path.unlink(missing_ok=True)
            self._lock_acquired = False
            self._event("runner_stopped", pid=os.getpid())

    def _existing_episode_job(self, manager: JobManager) -> JobState | None:
        for state in manager.list_states(limit=100):
            if state.mode != "historical_episode_930" or state.status in TERMINAL_STATUSES:
                continue
            if state.pid is None or _alive(state.pid):
                return state
        return None

    def _active_write_lock(self) -> dict[str, Any] | None:
        """Return a live formal writer lock, without removing or changing it."""

        path = self.settings.logs / "policydb-write.lock"
        if not path.exists():
            return None
        payload = _json(path)
        pid = payload.get("pid")
        try:
            live = _alive(int(pid)) if pid is not None else True
        except (TypeError, ValueError):
            live = True
        return {"path": str(path), "pid": pid, "job_id": payload.get("job_id"), "live": live}

    def _request(self) -> CrawlJobRequest:
        return CrawlJobRequest(
            mode="historical_episode_930",
            episode_id=EPISODE_ID,
            episode_city_limit=self.city_limit,
            episode_max_ai_calls=self.max_ai_calls,
            max_fetches=self.max_fetches,
            max_attachment_attempts=1,
            enabled_only=True,
            run_glm=False,
            run_verification=False,
            rebuild_database=False,
            run_validation=False,
            official_first=True,
            processing_mode="full",
            resume=True,
        )

    def _start_or_adopt(self, manager: JobManager) -> JobState:
        existing = self._existing_episode_job(manager)
        if existing is not None:
            if existing.status == "queued" and not existing.pid:
                started = manager.start(existing.job_id)
                self._event("queued_worker_started", job_id=existing.job_id, pid=started.pid)
                return manager.inspect_state(existing.job_id)
            self._event("worker_adopted", job_id=existing.job_id, pid=existing.pid)
            return existing
        request = self._request()
        state = manager.create(request)
        started = manager.start(state.job_id)
        self._event("worker_started", job_id=state.job_id, pid=started.pid, request=request.model_dump(mode="json"))
        return manager.inspect_state(state.job_id)

    def _wait_job(self, manager: JobManager, state: JobState) -> JobState:
        cancel_sent = False
        while True:
            current = manager.inspect_state(state.job_id)
            self._write_state(
                status="STOP_REQUESTED" if self.stop_requested() else "RUNNING",
                current_job_id=state.job_id,
                current_job_status=current.status,
                current_job_stage=current.stage,
                current_job_pid=current.pid,
                current_job_run_id=current.run_id,
                current_job_error=current.error_message,
            )
            if self.stop_requested() and not cancel_sent and current.status not in TERMINAL_STATUSES:
                manager.cancel(state.job_id)
                cancel_sent = True
                self._event("safe_stop_requested", job_id=state.job_id, reason="STOP_FILE")
            if current.status in TERMINAL_STATUSES:
                return current
            time.sleep(self.poll_seconds)

    def _consume_handoff(self, finished: JobState) -> dict[str, Any]:
        """Require a durable handoff marker before starting the next batch."""

        run_id = str(finished.run_id or "")
        path = self.output / "production_runs" / run_id / "HANDOFF.json"
        handoff = _json(path) if run_id and path.exists() else {}
        if not handoff:
            self._event("terminal_handoff_missing", job_id=finished.job_id, run_id=run_id)
            return {"handoff_status": "MISSING", "last_micro_batch_status": "UNKNOWN", "next_batch_autostart": False}
        self._event(
            "terminal_handoff_consumed",
            job_id=finished.job_id,
            run_id=run_id,
            last_micro_batch_status=handoff.get("last_micro_batch_status") or handoff.get("status"),
            episode_status=handoff.get("episode_status"),
        )
        return {"handoff_status": "CONSUMED", "last_micro_batch_status": handoff.get("last_micro_batch_status") or handoff.get("status"), "next_batch_autostart": bool(handoff.get("next_batch_autostart", True))}

    def run(self) -> dict[str, Any]:
        self.acquire_lock()
        manager = JobManager(self.settings)
        cycles_run = 0
        try:
            self._write_state(status="RUNNING")
            while True:
                if self.stop_requested():
                    result = self._write_state(status="STOPPED", reason_code="STOP_FILE")
                    self._event("runner_stopped_by_request")
                    return {**result, "exit_code": 21}
                queue = self._queue_summary()
                if queue["total"] > 0 and queue["pending"] == 0 and queue["running"] == 0 and queue["failed"] == 0:
                    convergence = self._final_convergence_state()
                    existing = self._existing_episode_job(manager)
                    if existing is not None:
                        self._write_state(
                            status="RUNNING",
                            current_job_id=existing.job_id,
                            current_job_status=existing.status,
                            current_job_pid=existing.pid,
                            final_convergence=convergence,
                        )
                        self._event(
                            "queue_complete_active_worker_wait",
                            queue=queue,
                            job_id=existing.job_id,
                            final_convergence=convergence,
                        )
                        time.sleep(self.poll_seconds)
                        continue
                    if not convergence["pending"]:
                        result = self._write_state(status="COMPLETE", current_job_id=None)
                        self._event("queue_complete", queue=queue)
                        return {**result, "exit_code": 0}
                    if not convergence["due"]:
                        self._write_state(
                            status="WAITING_FINAL_CONVERGENCE",
                            current_job_id=None,
                            final_convergence=convergence,
                            next_batch_status="WAITING_RETRY_WINDOW",
                            next_batch_autostart=False,
                        )
                        self._event(
                            "queue_complete_waiting_final_convergence",
                            queue=queue,
                            final_convergence=convergence,
                        )
                        time.sleep(self.poll_seconds)
                        continue
                if queue["total"] > 0 and queue["pending"] == 0 and queue["running"] == 0 and queue["failed"] > 0:
                    result = self._write_state(
                        status="BLOCKED",
                        current_job_id=None,
                        reason_code="QUEUE_FAILED_ITEMS",
                    )
                    self._event("queue_failed_items", queue=queue)
                    return {**result, "exit_code": 20}
                if self.max_cycles and cycles_run >= self.max_cycles:
                    result = self._write_state(status="PAUSED_MAX_CYCLES", current_job_id=None)
                    self._event("max_cycles_reached", queue=queue)
                    return {**result, "exit_code": 0}
                write_lock = self._active_write_lock()
                if write_lock is not None:
                    result = self._write_state(
                        status="WAITING_SINGLE_WRITER",
                        current_job_id=None,
                        active_write_lock=write_lock,
                    )
                    self._event("writer_busy_wait", **write_lock)
                    time.sleep(self.poll_seconds)
                    continue
                previous = _json(self.state_path)
                state = self._start_or_adopt(manager)
                cycles_run += 1
                cycles = int(previous.get("cycles_started", 0)) + 1
                self._write_state(status="RUNNING", current_job_id=state.job_id, cycles_started=cycles)
                finished = self._wait_job(manager, state)
                failures = int(previous.get("consecutive_failures", 0))
                if finished.status in {"failed", "cancelled"}:
                    failures += 1
                    self._write_state(
                        status="BLOCKED" if failures >= self.max_consecutive_failures else "RETRY_WAIT",
                        current_job_id=finished.job_id,
                        current_job_status=finished.status,
                        consecutive_failures=failures,
                        last_error=finished.error_message,
                    )
                    self._event("worker_terminal_failure", job_id=finished.job_id, status=finished.status, failures=failures)
                    if failures >= self.max_consecutive_failures:
                        return {**self._json(self.state_path), "exit_code": 20}
                    time.sleep(min(300, 30 * failures))
                    continue
                self._write_state(
                    status="RUNNING",
                    current_job_id=None,
                    cycles_completed=int(previous.get("cycles_completed", 0)) + 1,
                    consecutive_failures=0,
                    last_error=None,
                    last_job_status=finished.status,
                    last_job_stage=finished.stage,
                    last_job_run_id=finished.run_id,
                )
                self._event("worker_terminal_success", job_id=finished.job_id, status=finished.status, run_id=finished.run_id)
                handoff = self._consume_handoff(finished)
                queue_after_handoff = self._queue_summary()
                convergence_after_handoff = self._final_convergence_state()
                queue_pending = queue_after_handoff.get("pending", 0) > 0
                final_pending = bool(convergence_after_handoff.get("pending"))
                if queue_pending:
                    next_status = "PENDING"
                    next_autostart = True
                    next_state = "RUNNING"
                elif final_pending and convergence_after_handoff.get("due"):
                    next_status = "FINAL_CONVERGENCE_DUE"
                    next_autostart = True
                    next_state = "RUNNING"
                elif final_pending:
                    next_status = "WAITING_RETRY_WINDOW"
                    next_autostart = False
                    next_state = "WAITING_FINAL_CONVERGENCE"
                else:
                    next_status = "NOT_REQUIRED"
                    next_autostart = False
                    next_state = "COMPLETE"
                self._write_state(
                    status=next_state,
                    current_job_id=None,
                    last_micro_batch_status=handoff.get("last_micro_batch_status"),
                    next_batch_status=next_status,
                    next_batch_autostart=next_autostart,
                    final_convergence=convergence_after_handoff,
                    final_convergence_last_attempt_at=_now(),
                    final_convergence_fingerprint=convergence_after_handoff.get("fingerprint"),
                )
                if queue_pending or (final_pending and convergence_after_handoff.get("due")):
                    self._event("next_micro_batch_autostart_ready", queue=queue_after_handoff)
        finally:
            self.release_lock()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run resumable CRPD 2016 930 jobs")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--city-limit", type=int, default=5)
    parser.add_argument("--max-ai-calls", type=int, default=10)
    parser.add_argument("--max-fetches", type=int, default=30)
    parser.add_argument("--poll-seconds", type=int, default=15)
    parser.add_argument("--max-cycles", type=int, default=0)
    parser.add_argument("--max-consecutive-failures", type=int, default=3)
    args = parser.parse_args()
    result = Episode930AutoRunner(
        output=args.output,
        city_limit=args.city_limit,
        max_ai_calls=args.max_ai_calls,
        max_fetches=args.max_fetches,
        poll_seconds=args.poll_seconds,
        max_cycles=args.max_cycles,
        max_consecutive_failures=args.max_consecutive_failures,
    ).run()
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    raise SystemExit(int(result.get("exit_code", 1)))


if __name__ == "__main__":
    main()
