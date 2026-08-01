from __future__ import annotations

import json
import os
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


class AIAuditStore:
    """Crash-safe per-request audit records with no secret-bearing fields."""

    def __init__(self, root: Path):
        self.root = root / "ai_audit"
        self.requests = self.root / "requests"
        self.requests.mkdir(parents=True, exist_ok=True)

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
        return self.update(request_id, status="response_completed", completed_at=utc_now(), **updates)

    def fail(self, request_id: str, *, status: str = "response_failed", error_type: str, error_message: str) -> dict[str, Any]:
        if status not in {"response_failed", "interrupted"}:
            raise ValueError("invalid failure status")
        return self.update(request_id, status=status, error_type=error_type, error_message=error_message[:500])

    def recover_interrupted(self) -> list[dict[str, Any]]:
        recovered = []
        for path in sorted(self.requests.glob("*.json")):
            record = json.loads(path.read_text(encoding="utf-8"))
            if record.get("status") == "request_started":
                recovered.append(self.fail(record["request_id"], status="interrupted", error_type="process_interrupted", error_message="request was still started at resume"))
        return recovered

    def records(self) -> list[dict[str, Any]]:
        return [json.loads(path.read_text(encoding="utf-8")) for path in sorted(self.requests.glob("*.json"))]
