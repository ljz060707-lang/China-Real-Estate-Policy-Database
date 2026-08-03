"""Validated Dashboard operation queue.

Streamlit writes only JSON job requests.  A separate local worker claims a
request and calls the same Python business-layer controllers used by the CLI.
No user-provided shell text is ever executed.
"""

from __future__ import annotations

import json
import os
import uuid
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from policydb.fast_bulk_ingest import FastBulkConfig, FastBulkIngestController
from policydb.full_sync import FullSyncConfig, FullSyncController
from policydb.parquet_store import read_parquet_snapshot
from policydb.settings import Settings
from policydb.source_discovery import REQUIRED_ROLES

ALLOWED_ACTIONS = {"fast_bulk_ingest", "city_fast_ingest", "city_complete", "source_resume", "refresh_metrics", "research_snapshot"}
ACTIVE_STATUSES = {"QUEUED", "RUNNING"}


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temp.write_text(json.dumps(dict(payload), ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
        os.replace(temp, path)
    finally:
        temp.unlink(missing_ok=True)


def _city_ids(settings: Settings) -> set[str]:
    path = settings.curated / "source_requirement_slots.parquet"
    if not path.exists():
        return set()
    frame = read_parquet_snapshot(path, columns=["city_id"])
    return {str(value) for value in frame.get_column("city_id").drop_nulls().unique().to_list()}


def validate_job_request(settings: Settings, action: str, scope: Mapping[str, Any] | None = None, *, confirmed: bool = False) -> dict[str, Any]:
    action = str(action or "").strip()
    if action not in ALLOWED_ACTIONS:
        raise ValueError(f"unsupported Dashboard action: {action}")
    scope = dict(scope or {})
    cities = [str(value) for value in (scope.get("cities") or []) if value]
    roles = [str(value) for value in (scope.get("source_roles") or []) if value]
    if action in {"fast_bulk_ingest", "city_fast_ingest", "city_complete"} and not confirmed:
        raise ValueError("confirmation is required for a crawl operation")
    if action in {"city_fast_ingest", "city_complete"} and len(cities) != 1:
        raise ValueError("city operation requires exactly one registered city")
    if action == "source_resume" and not str(scope.get("source_id") or ""):
        raise ValueError("source_resume requires source_id")
    invalid_cities = sorted(set(cities) - _city_ids(settings))
    if invalid_cities:
        raise ValueError(f"unknown city_id(s): {invalid_cities}")
    invalid_roles = sorted(set(roles) - set(REQUIRED_ROLES))
    if invalid_roles:
        raise ValueError(f"unknown source role(s): {invalid_roles}")
    return {"action": action, "scope": {"cities": cities, "source_roles": roles, "source_id": scope.get("source_id"), "max_cities": scope.get("max_cities")}, "confirmed": bool(confirmed)}


def _job_dir(settings: Settings) -> Path:
    return settings.data_root / "control" / "dashboard_jobs"


def enqueue_job(settings: Settings, action: str, scope: Mapping[str, Any] | None = None, *, confirmed: bool = False) -> dict[str, Any]:
    request = validate_job_request(settings, action, scope, confirmed=confirmed)
    for path in _job_dir(settings).glob("*.json"):
        try:
            existing = json.loads(path.read_text(encoding="utf-8-sig"))
        except (OSError, json.JSONDecodeError):
            continue
        if existing.get("status") in ACTIVE_STATUSES:
            raise RuntimeError(f"an operation is already active: {existing.get('job_id')}")
    job = {"job_id": f"JOB_{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}_{uuid.uuid4().hex[:8]}", **request, "requested_at": _now(), "status": "QUEUED", "requested_by": "dashboard"}
    _atomic_json(_job_dir(settings) / f"{job['job_id']}.json", job)
    return job


def list_jobs(settings: Settings, *, limit: int = 50) -> list[dict[str, Any]]:
    rows = []
    for path in sorted(_job_dir(settings).glob("*.json"), key=lambda item: item.stat().st_mtime, reverse=True):
        try:
            rows.append(json.loads(path.read_text(encoding="utf-8-sig")))
        except (OSError, json.JSONDecodeError):
            continue
        if len(rows) >= limit:
            break
    return rows


def _update_job(path: Path, updates: Mapping[str, Any]) -> dict[str, Any]:
    current = json.loads(path.read_text(encoding="utf-8-sig"))
    current.update(dict(updates))
    current["updated_at"] = _now()
    _atomic_json(path, current)
    return current


def run_next_job(settings: Settings | None = None) -> dict[str, Any] | None:
    settings = settings or Settings.discover()
    queued = [row for row in list_jobs(settings) if row.get("status") == "QUEUED"]
    if not queued:
        return None
    job = queued[-1]
    path = _job_dir(settings) / f"{job['job_id']}.json"
    _update_job(path, {"status": "RUNNING", "started_at": _now()})
    try:
        action = job["action"]
        cities = job.get("scope", {}).get("cities") or []
        if action in {"fast_bulk_ingest", "city_fast_ingest"}:
            config = FastBulkConfig(apply=True, resume=True, max_cities=1 if action == "city_fast_ingest" else job.get("scope", {}).get("max_cities"), city_ids=tuple(cities))
            result = FastBulkIngestController(settings, config=config).run(city_ids=cities)
        elif action == "city_complete":
            roles = tuple(str(value) for value in (job.get("scope", {}).get("source_roles") or []) if value)
            if roles:
                config = FastBulkConfig(apply=True, resume=True, max_cities=1, city_ids=(cities[0],), source_roles=roles)
                result = FastBulkIngestController(settings, config=config).run(city_ids=cities)
            else:
                config = FullSyncConfig(scope="city", city_id=cities[0], all_five_source_roles=True, backfill=True, incremental=True, resume=True, apply=True, max_slots=5, max_sources=5)
                result = FullSyncController(settings, config=config).run(command="run")
        elif action == "source_resume":
            config = FullSyncConfig(scope="source", source_id=str(job["scope"].get("source_id")), backfill=True, incremental=True, resume=True, apply=True, max_sources=1)
            result = FullSyncController(settings, config=config).run(command="run")
        elif action == "research_snapshot":
            from policydb.research_snapshot import create_research_snapshot
            result = create_research_snapshot(settings)
        else:
            result = {"status": "REFRESH_REQUESTED"}
        return _update_job(path, {"status": "COMPLETED", "finished_at": _now(), "result": result})
    except Exception as exc:
        return _update_job(path, {"status": "FAILED", "finished_at": _now(), "error_type": type(exc).__name__, "error": str(exc)[:1000]})


__all__ = ["ACTIVE_STATUSES", "ALLOWED_ACTIONS", "enqueue_job", "list_jobs", "run_next_job", "validate_job_request"]
