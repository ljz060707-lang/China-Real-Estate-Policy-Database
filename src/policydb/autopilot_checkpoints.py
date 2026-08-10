"""Cross-run checkpoint primitives for bounded source-completion runs."""

from __future__ import annotations

import hashlib
import json
import os
import time
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

TERMINAL_SLOT_STATUSES = {"COMPLETED", "HUMAN_REVIEW", "VERIFIED", "ENABLED"}
STATUS_PRIORITY = {
    "UNRESOLVED": 0,
    "RETRY_WAIT": 1,
    "FAILED_RECOVERABLE": 1,
    "COMPLETED": 2,
    "HUMAN_REVIEW": 3,
    "VERIFIED": 4,
    "ENABLED": 5,
}


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


def _parse_time(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def work_fingerprint(row: Mapping[str, Any]) -> str:
    payload = {
        str(key): value
        for key, value in row.items()
        if str(key) not in {"updated_at", "last_progress_at", "last_heartbeat_at"}
    }
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def is_slot_claimable(
    slot: Mapping[str, Any],
    checkpoint: Mapping[str, Any] | None = None,
    *,
    now: datetime | None = None,
    active_slot_ids: set[str] | None = None,
    research_mode: bool = False,
) -> bool:
    """Single claimable predicate shared by planning, run, resume and workers."""
    now = now or datetime.now(UTC)
    active_slot_ids = active_slot_ids or set()
    slot_id = str(slot.get("slot_id") or "")
    if not slot_id or slot_id in active_slot_ids:
        return False
    if str(slot.get("active_batch_id") or slot.get("claimed_by_batch") or "").strip():
        return False
    if any(bool(slot.get(key)) for key in ("is_verified", "verified", "best_candidate_verified")):
        return False
    if any(int(slot.get(key) or 0) > 0 for key in ("verified_candidate_count", "verified_slots", "slots_with_verified_candidate")):
        return False
    if bool(slot.get("is_enabled")) or int(slot.get("enabled_source_count") or 0) > 0:
        return False
    status = str(slot.get("status") or slot.get("work_status") or "").upper()
    if status in TERMINAL_SLOT_STATUSES or status in {"HUMAN_REVIEW", "VERIFIED", "ENABLED"}:
        return False
    if status == "RETRY_WAIT":
        # The materialized slot queue may carry only the status while the
        # authoritative retry deadline lives in the append-only checkpoint.
        # Prefer the row value when present, then fall back to the checkpoint;
        # otherwise a retry_wait row would remain blocked forever after resume.
        retry_at = _parse_time(slot.get("next_retry_at")) or _parse_time((checkpoint or {}).get("next_retry_at"))
        if (retry_at or now + timedelta(seconds=1)) > now:
            return False
    if status in {
        "HUMAN_REVIEW",
        "CANDIDATE_FAILED_AMBIGUOUS",
        "BLOCKED_ROLE_CONFLICT",
    } or (
        status == "NO_CANDIDATE_MANUAL_RESEARCH" and not research_mode
    ) or str(slot.get("manual_review_status") or "").lower() in {"human_review", "pending_human_review", "approved"}:
        return False
    if checkpoint:
        checkpoint_status = str(checkpoint.get("status") or "").upper()
        if checkpoint_status in TERMINAL_SLOT_STATUSES:
            return False
        if checkpoint_status == "RETRY_WAIT" and (_parse_time(checkpoint.get("next_retry_at")) or now + timedelta(seconds=1)) > now:
            return False
        if checkpoint_status == "CLAIMED":
            lease_until = _parse_time(checkpoint.get("lease_until"))
            if lease_until and lease_until > now:
                return False
    return True


@contextmanager
def _exclusive_file_lock(path: Path, *, timeout_seconds: float = 15.0) -> Iterator[None]:
    """Small cross-process lock without adding a dependency to the project."""
    path.parent.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + timeout_seconds
    while True:
        try:
            fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.write(fd, f"pid={os.getpid()}\n".encode("ascii"))
            os.close(fd)
            break
        except FileExistsError:
            try:
                if time.time() - path.stat().st_mtime > timeout_seconds * 8:
                    path.unlink()
                    continue
            except FileNotFoundError:
                continue
            if time.monotonic() >= deadline:
                raise TimeoutError(f"checkpoint lock timeout: {path}") from None
            time.sleep(0.02)
    try:
        yield
    finally:
        try:
            path.unlink()
        except FileNotFoundError:
            pass


class GlobalSlotCheckpointStore:
    """Append-only global slot checkpoint log plus atomic claim operations."""

    def __init__(self, root: Path):
        self.root = root
        self.path = root / "slot_checkpoints.jsonl"
        self.lock_path = root / ".slot_checkpoints.lock"
        self.root.mkdir(parents=True, exist_ok=True)

    def _read_events(self) -> list[dict[str, Any]]:
        if not self.path.exists():
            return []
        events: list[dict[str, Any]] = []
        for line in self.path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                item = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(item, dict) and item.get("slot_id"):
                events.append(item)
        return events

    def _latest(self, events: list[dict[str, Any]] | None = None) -> dict[str, dict[str, Any]]:
        latest: dict[str, dict[str, Any]] = {}
        for item in events if events is not None else self._read_events():
            latest[str(item["slot_id"])] = item
        return latest

    def snapshot(self) -> dict[str, dict[str, Any]]:
        return self._latest()

    def history(self) -> list[dict[str, Any]]:
        return self._read_events()

    def _append_unlocked(self, event: Mapping[str, Any]) -> dict[str, Any]:
        self.root.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8", newline="") as handle:
            handle.write(json.dumps(dict(event), ensure_ascii=False, sort_keys=True, default=str) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        return dict(event)

    def append(self, event: Mapping[str, Any]) -> dict[str, Any]:
        with _exclusive_file_lock(self.lock_path):
            return self._append_unlocked(event)

    def claim(
        self,
        slot: Mapping[str, Any],
        *,
        run_id: str,
        lease_seconds: int = 1800,
        now: datetime | None = None,
        research_mode: bool = False,
    ) -> tuple[bool, dict[str, Any]]:
        now = now or datetime.now(UTC)
        slot_id = str(slot.get("slot_id") or "")
        fingerprint = work_fingerprint(slot)
        if not slot_id:
            return False, {"reason": "missing_slot_id"}
        with _exclusive_file_lock(self.lock_path):
            events = self._read_events()
            latest = self._latest(events).get(slot_id)
            if not is_slot_claimable(slot, latest, now=now, research_mode=research_mode):
                if latest and str(latest.get("status")) == "CLAIMED" and str(latest.get("run_id")) == run_id:
                    return True, {**latest, "reason": "same_run_claim_reused"}
                return False, {"reason": "already_claimed_or_completed", "slot_id": slot_id, "checkpoint": latest}
            timestamp = now.isoformat()
            event = {
                "event": "SLOT_CLAIMED",
                "slot_id": slot_id,
                "work_fingerprint": fingerprint,
                "status": "CLAIMED",
                "run_id": run_id,
                "claimed_at": timestamp,
                "lease_until": (now + timedelta(seconds=lease_seconds)).isoformat(),
                "updated_at": timestamp,
                "idempotency_key": f"{slot_id}:CLAIMED:{fingerprint}",
            }
            return True, self._append_unlocked(event)

    def terminal(
        self,
        slot: Mapping[str, Any],
        *,
        status: str,
        run_id: str,
        terminal_outcome: str,
        ai_call_persisted: bool,
        next_retry_at: str | None = None,
        evidence_ids: list[str] | None = None,
    ) -> dict[str, Any]:
        status = status.upper()
        if status not in TERMINAL_SLOT_STATUSES | {"RETRY_WAIT", "FAILED_RECOVERABLE"}:
            raise ValueError(f"invalid terminal checkpoint status: {status}")
        timestamp = utc_now()
        fingerprint = work_fingerprint(slot)
        slot_id = str(slot["slot_id"])
        with _exclusive_file_lock(self.lock_path):
            latest = self._latest(self._read_events()).get(slot_id)
            latest_status = str((latest or {}).get("status") or "UNRESOLVED").upper()
            latest_priority = STATUS_PRIORITY.get(latest_status, 0)
            requested_priority = STATUS_PRIORITY.get(status, 0)
            if latest_status in TERMINAL_SLOT_STATUSES and requested_priority < latest_priority:
                return {**latest, "event": "CHECKPOINT_NON_REGRESSION_IGNORED", "ignored_status": status}
            if latest_status == status and str((latest or {}).get("work_fingerprint")) == fingerprint:
                return {**latest, "event": "CHECKPOINT_REUSED"}
            event = {
                "event": "CHECKPOINT_COMPLETED" if status not in {"RETRY_WAIT", "FAILED_RECOVERABLE"} else "CHECKPOINT_RETRY_WAIT",
                "slot_id": slot_id,
                "work_fingerprint": fingerprint,
                "status": status,
                "run_id": run_id,
                "claimed_at": None,
                "lease_until": None,
                "completed_at": timestamp if status not in {"RETRY_WAIT", "FAILED_RECOVERABLE"} else None,
                "next_retry_at": next_retry_at,
                "terminal_outcome": terminal_outcome,
                "ai_call_persisted": bool(ai_call_persisted),
                "evidence_ids": evidence_ids or [],
                "updated_at": timestamp,
                "idempotency_key": f"{slot_id}:{status}:{fingerprint}",
            }
            return self._append_unlocked(event)

    def requeue_zero_yield_run(
        self,
        run_dir: Path,
        *,
        repair_run_id: str,
    ) -> dict[str, Any]:
        """Append recoverable requeue events for a zero-yield historical run.

        This is an append-only checkpoint repair.  It is deliberately narrow:
        only slots whose run summary records no formal application, no probe,
        no human-review item and no strict yield, and whose latest checkpoint
        is the old zero-yield ``COMPLETED`` outcome, are requeued.  Existing
        run history remains untouched.
        """

        summary_path = Path(run_dir) / "run_summary.json"
        if not summary_path.exists():
            return {"status": "NO_RUN_SUMMARY", "requeued": 0, "slots": []}
        try:
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            return {"status": "INVALID_RUN_SUMMARY", "error_type": type(exc).__name__, "requeued": 0, "slots": []}
        candidates = [
            item
            for item in summary.get("slot_results", [])
            if str(item.get("slot_id") or "")
            and int(item.get("applied_candidates") or 0) == 0
            and int(item.get("probed_candidates") or 0) == 0
            and int(item.get("human_review") or 0) == 0
        ]
        if not candidates:
            return {"status": "NO_ZERO_YIELD_SLOTS", "requeued": 0, "slots": []}
        source_run_id = str(summary.get("run_dir") or Path(run_dir).name)
        source_run_id = source_run_id.replace("\\", "/").rstrip("/").split("/")[-1]
        requeued: list[str] = []
        skipped: list[dict[str, Any]] = []
        timestamp = utc_now()
        with _exclusive_file_lock(self.lock_path):
            latest = self._latest(self._read_events())
            for item in candidates:
                slot_id = str(item["slot_id"])
                previous = latest.get(slot_id) or {}
                previous_status = str(previous.get("status") or "").upper()
                if previous_status != "COMPLETED" or str(previous.get("terminal_outcome") or "") != "deterministic_probe_completed_without_verified_candidate":
                    skipped.append({"slot_id": slot_id, "reason": "latest_checkpoint_not_zero_yield_completed", "status": previous_status})
                    continue
                idempotency_key = f"{slot_id}:REQUEUE:{source_run_id}"
                if any(
                    event.get("idempotency_key") == idempotency_key
                    for event in self._read_events()
                ):
                    skipped.append({"slot_id": slot_id, "reason": "requeue_already_recorded"})
                    continue
                event = {
                    "event": "CHECKPOINT_REQUEUE_REQUESTED",
                    "slot_id": slot_id,
                    "work_fingerprint": previous.get("work_fingerprint"),
                    "status": "FAILED_RECOVERABLE",
                    "run_id": repair_run_id,
                    "source_run_id": source_run_id,
                    "requeue_from_status": previous_status,
                    "terminal_outcome": "evidence_enrichment_requeue",
                    "reason_code": "historical_zero_yield_prefilter_block",
                    "claimed_at": None,
                    "lease_until": None,
                    "next_retry_at": None,
                    "ai_call_persisted": bool(previous.get("ai_call_persisted")),
                    "evidence_ids": previous.get("evidence_ids") or [],
                    "updated_at": timestamp,
                    "idempotency_key": idempotency_key,
                }
                self._append_unlocked(event)
                latest[slot_id] = event
                requeued.append(slot_id)
        return {
            "status": "REQUEUED" if requeued else "NO_ELIGIBLE_ZERO_YIELD_SLOTS",
            "source_run_id": source_run_id,
            "repair_run_id": repair_run_id,
            "requeued": len(requeued),
            "slots": requeued,
            "skipped": skipped,
        }

    def backfill_from_run_dirs(
        self,
        outputs_root: Path,
        *,
        apply: bool = False,
        run_dir: Path | None = None,
    ) -> dict[str, Any]:
        """Inspect or append missing global checkpoints from historical run summaries."""
        proposals: list[dict[str, Any]] = []
        duplicates: list[dict[str, Any]] = []
        run_paths = [run_dir] if run_dir is not None else (
            sorted(outputs_root.iterdir()) if outputs_root.exists() else []
        )
        seen: dict[str, dict[str, Any]] = {}
        for run_path in run_paths:
            if not run_path.is_dir() or run_path.resolve() == self.root.resolve():
                continue
            summary_path = run_path / "run_summary.json"
            if not summary_path.exists():
                continue
            try:
                summary = json.loads(summary_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            for result in summary.get("slot_results", []):
                if not result.get("completed") or not result.get("slot_id"):
                    continue
                slot_id = str(result["slot_id"])
                status = "HUMAN_REVIEW" if int(result.get("human_review") or 0) > 0 else "COMPLETED"
                item = {
                    "event": "CHECKPOINT_BACKFILLED",
                    "slot_id": slot_id,
                    "work_fingerprint": f"legacy:{run_path.name}:{slot_id}",
                    "status": status,
                    "run_id": str(summary.get("run_dir") or run_path.name).split("\\")[-1],
                    "terminal_outcome": "legacy_run_summary",
                    "ai_call_persisted": bool(summary.get("persisted_ai_calls")),
                    "source_run_dir": str(run_path),
                    "updated_at": utc_now(),
                }
                if slot_id in seen:
                    duplicates.append({
                        "event": "DUPLICATE_COMPLETED_SLOT_DETECTED",
                        "slot_id": slot_id,
                        "duplicate_run_id": item["run_id"],
                        "original_run_id": seen[slot_id]["run_id"],
                        "provider_calls_already_spent": summary.get("ai_calls"),
                        "preserved_history": True,
                        "repair_action": "preserve_historical_run_and_use_global_terminal_checkpoint",
                    })
                else:
                    seen[slot_id] = item
                    proposals.append(item)
        existing = self._latest()
        missing = [item for item in proposals if str(item["slot_id"]) not in existing]
        backfilled = 0
        if apply:
            with _exclusive_file_lock(self.lock_path):
                existing = self._latest(self._read_events())
                for item in missing:
                    if str(item["slot_id"]) in existing:
                        continue
                    self._append_unlocked(item)
                    existing[str(item["slot_id"])] = item
                    backfilled += 1
        return {
            "apply": apply,
            "proposed": len(proposals),
            "backfilled": backfilled,
            "proposals": missing,
            "duplicates": duplicates,
        }
