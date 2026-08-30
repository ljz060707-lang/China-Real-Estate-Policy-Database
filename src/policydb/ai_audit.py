from __future__ import annotations

import json
import os
import re
import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from policydb.autopilot_checkpoints import _exclusive_file_lock


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


def _redact_error(value: object) -> str:
    text = str(value or "")[:500]
    text = re.sub(
        r"(?i)(api[_-]?key|authorization|bearer|token|password|secret)\s*[:=]\s*[^,\s]+",
        r"\1=[REDACTED]",
        text,
    )
    text = re.sub(r"(?i)bearer\s+[^\s,]+", "Bearer [REDACTED]", text)
    text = re.sub(r"\bsk-[A-Za-z0-9_-]+", "[REDACTED]", text)
    return text


class AIAuditStore:
    """Crash-safe per-request audit records with no secret-bearing fields."""

    def __init__(self, root: Path, *, global_root: Path | None = None):
        self.root = root / "ai_audit"
        self.requests = self.root / "requests"
        self.requests.mkdir(parents=True, exist_ok=True)
        self.global_root = global_root
        self.global_requests = (global_root / "ai_audit" / "requests") if global_root else None
        self.global_lock = (global_root / ".ai_audit.lock") if global_root else None
        self.last_reservation_action = "claimed"
        if self.global_requests is not None:
            self.global_requests.mkdir(parents=True, exist_ok=True)

    def _path(self, request_id: str) -> Path:
        safe = "".join(ch for ch in request_id if ch.isalnum() or ch in "_-" )
        if not safe:
            raise ValueError("invalid request_id")
        return self.requests / f"{safe}.json"

    def _write(self, request_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        target = self._path(request_id)
        data = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str) + "\n"
        fd, temp_name = tempfile.mkstemp(prefix=f".{target.name}.", suffix=".tmp", dir=target.parent)
        try:
            with os.fdopen(fd, "w", encoding="utf-8", newline="") as handle:
                handle.write(data)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temp_name, target)
        finally:
            if os.path.exists(temp_name):
                os.unlink(temp_name)
        return payload

    def _global_path(self, request_hash: str) -> Path:
        if self.global_requests is None:
            raise ValueError("global audit root is not configured")
        safe = "".join(ch for ch in str(request_hash) if ch.isalnum() or ch in "_-")
        if not safe:
            raise ValueError("invalid request_hash")
        return self.global_requests / f"{safe}.json"

    def _write_target(self, target: Path, payload: dict[str, Any]) -> dict[str, Any]:
        target.parent.mkdir(parents=True, exist_ok=True)
        data = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str) + "\n"
        fd, temp_name = tempfile.mkstemp(prefix=f".{target.name}.", suffix=".tmp", dir=target.parent)
        try:
            with os.fdopen(fd, "w", encoding="utf-8", newline="") as handle:
                handle.write(data)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temp_name, target)
        finally:
            if os.path.exists(temp_name):
                os.unlink(temp_name)
        return payload

    def _bootstrap_global_unlocked(self) -> None:
        if self.global_root is None or self.global_requests is None:
            return
        for path in self.global_root.rglob("ai_audit/requests/*.json"):
            try:
                record = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            if record.get("status") != "response_completed" or not record.get("request_hash"):
                continue
            target = self._global_path(str(record["request_hash"]))
            if target.exists():
                continue
            self._write_target(target, {key: value for key, value in record.items() if key not in {"api_key", "authorization", "headers", "request_headers"}})

    def reserve(self, payload: dict[str, Any]) -> tuple[str, dict[str, Any] | None]:
        """Atomically reserve a request hash or return a reusable/in-flight record."""
        if self.global_root is None or not payload.get("request_hash"):
            self.last_reservation_action = "claimed"
            return "claimed", None
        with _exclusive_file_lock(self.global_lock):
            self._bootstrap_global_unlocked()
            target = self._global_path(str(payload["request_hash"]))
            existing = json.loads(target.read_text(encoding="utf-8")) if target.exists() else None
            if existing and existing.get("status") == "response_completed":
                self.last_reservation_action = "reused"
                return "reused", existing
            if existing and existing.get("status") == "request_started" and existing.get("run_id") != payload.get("run_id"):
                started_at = existing.get("started_at")
                try:
                    age = datetime.now(UTC) - datetime.fromisoformat(str(started_at).replace("Z", "+00:00"))
                except (TypeError, ValueError):
                    age = timedelta.max
                if age < timedelta(seconds=1800):
                    self.last_reservation_action = "in_flight"
                    return "in_flight", existing
            now = utc_now()
            record = {key: value for key, value in payload.items() if key not in {"api_key", "authorization", "headers", "request_headers"}}
            record.update({"status": "request_started", "started_at": now, "updated_at": now})
            self._write_target(target, record)
            self.last_reservation_action = "claimed"
            return "claimed", record

    def reuse(self, payload: dict[str, Any], existing: dict[str, Any] | None) -> dict[str, Any]:
        now = utc_now()
        record = {key: value for key, value in payload.items() if key not in {"api_key", "authorization", "headers", "request_headers"}}
        record.update({
            "status": "response_completed",
            "started_at": existing.get("started_at", now) if existing else now,
            "completed_at": existing.get("completed_at", now) if existing else now,
            "updated_at": now,
            "cache_hit": True,
            "reused_ai_call": True,
            "reused_from_run_id": existing.get("run_id") if existing else None,
            "response_hash": existing.get("response_hash") if existing else None,
            "response_payload": existing.get("response_payload") if existing else None,
            "prompt_tokens": existing.get("prompt_tokens") if existing else None,
            "completion_tokens": existing.get("completion_tokens") if existing else None,
            "total_tokens": existing.get("total_tokens") if existing else None,
            "estimated_cost_usd": existing.get("estimated_cost_usd") if existing else None,
        })
        return self._write(str(record["request_id"]), record)

    def _update_global(self, record: dict[str, Any]) -> None:
        if self.global_root is None or not record.get("request_hash"):
            return
        with _exclusive_file_lock(self.global_lock):
            self._bootstrap_global_unlocked()
            target = self._global_path(str(record["request_hash"]))
            existing = json.loads(target.read_text(encoding="utf-8")) if target.exists() else {}
            if existing.get("status") == "response_completed" and existing.get("run_id") != record.get("run_id"):
                return
            if existing.get("status") == "request_started" and existing.get("run_id") != record.get("run_id"):
                return
            self._write_target(target, {key: value for key, value in record.items() if key not in {"api_key", "authorization", "headers", "request_headers"}})

    def start(self, payload: dict[str, Any]) -> dict[str, Any]:
        required = ("request_id", "slot_id", "provider", "model", "prompt_version", "prompt_hash", "request_hash", "cache_key")
        missing = [name for name in required if name not in payload]
        if missing:
            raise ValueError(f"missing audit fields: {', '.join(missing)}")
        now = utc_now()
        record = {key: value for key, value in payload.items() if key not in {"api_key", "authorization", "headers", "request_headers"}}
        record.update({"status": "request_started", "started_at": now, "updated_at": now, "attempt": 0})
        return self._write(str(record["request_id"]), record)

    def update(self, request_id: str, **updates: Any) -> dict[str, Any]:
        path = self._path(request_id)
        if not path.exists():
            raise FileNotFoundError(path)
        record = json.loads(path.read_text(encoding="utf-8"))
        record.update({key: value for key, value in updates.items() if key not in {"api_key", "authorization", "headers", "request_headers"}})
        record["updated_at"] = utc_now()
        return self._write(request_id, record)

    def complete(self, request_id: str, **updates: Any) -> dict[str, Any]:
        record = self.update(request_id, status="response_completed", completed_at=utc_now(), **updates)
        self._update_global(record)
        return record

    def fail(
        self,
        request_id: str,
        *,
        status: str = "response_failed",
        error_type: str,
        error_message: str,
        diagnostics: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if status not in {"response_failed", "interrupted"}:
            raise ValueError("invalid failure status")
        allowed_diagnostics = {
            "transport_started",
            "dns_ok",
            "connect_ok",
            "http_status",
            "response_received",
            "response_bytes",
            "latency_ms",
            "timeout_type",
            "json_parse_ok",
            "schema_valid",
            "schema_errors",
            "provider_error_code",
            "provider_error_message_sanitized",
            "failure_class",
            "raw_response_hash",
            "raw_fields",
            "raw_response_payload",
            "configured_read_timeout",
            "configured_connect_timeout",
            "max_retries",
        }
        safe_diagnostics = {
            key: value
            for key, value in (diagnostics or {}).items()
            if key in allowed_diagnostics
        }
        record = self.update(
            request_id,
            status=status,
            error_type=error_type,
            error_message=_redact_error(error_message),
            **safe_diagnostics,
        )
        self._update_global(record)
        return record

    def recover_interrupted(self) -> list[dict[str, Any]]:
        recovered = []
        for path in sorted(self.requests.glob("*.json")):
            record = json.loads(path.read_text(encoding="utf-8"))
            if record.get("status") == "request_started":
                recovered.append(self.fail(record["request_id"], status="interrupted", error_type="process_interrupted", error_message="request was still started at resume"))
        return recovered

    def records(self) -> list[dict[str, Any]]:
        return [json.loads(path.read_text(encoding="utf-8")) for path in sorted(self.requests.glob("*.json"))]
