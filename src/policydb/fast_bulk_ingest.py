"""Breadth-first Bronze ingestion with bounded, resumable source work.

The fast mode deliberately reuses :mod:`policydb.full_sync` for each source.
It only changes scheduling and budgets; source admission, HTTP fetching,
parsing, deduplication, and checkpoint writes remain owned by the existing
deterministic pipeline.  Gold/policy-intensity code is intentionally not
imported here.
"""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import polars as pl
import yaml

from policydb.full_sync import (
    FullSyncConfig,
    FullSyncController,
    build_sync_plan,
    source_is_crawl_ready,
)
from policydb.parquet_store import read_parquet_snapshot
from policydb.settings import Settings
from policydb.source_discovery import REQUIRED_ROLES

FAST_BULK_INGEST = "FAST_BULK_INGEST"
SOURCE_STATUSES = (
    "SUCCESS",
    "COMPLETE_WITH_GAPS",
    "PARTIAL_BUT_USABLE",
    "PARTIAL_EMPTY",
    "PAUSED_BUDGET",
    "RETRY_WAIT",
    "HUMAN_REVIEW",
    "FAILED_TERMINAL",
    "SKIPPED_DEPENDENCY",
)


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(json.dumps(dict(payload), ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _append_jsonl(path: Path, row: Mapping[str, Any], *, unique_key: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    key = str(row.get(unique_key) or "")
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            try:
                if str(json.loads(line).get(unique_key) or "") == key:
                    return
            except json.JSONDecodeError:
                continue
    with path.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(dict(row), ensure_ascii=False, default=str) + "\n")


def _read(settings: Settings, name: str, columns: Sequence[str] | None = None) -> pl.DataFrame:
    path = settings.curated / f"{name}.parquet"
    if not path.exists():
        return pl.DataFrame()
    try:
        return read_parquet_snapshot(path, columns=columns)
    except pl.exceptions.ColumnNotFoundError:
        try:
            frame = read_parquet_snapshot(path)
            requested = list(columns or frame.columns)
            return frame.select([column for column in requested if column in frame.columns])
        except (OSError, pl.exceptions.PolarsError):
            return pl.DataFrame()
    except (OSError, pl.exceptions.PolarsError):
        return pl.DataFrame()


def _bool(value: object) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _parse_dt(value: object) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


@dataclass(slots=True)
class FastBulkConfig:
    """Safe defaults for one bounded breadth-first run."""

    mode: str = FAST_BULK_INGEST
    max_minutes_per_source: int = 10
    max_list_pages_per_source: int = 30
    max_documents_per_source: int = 300
    max_document_retries: int = 2
    max_attachment_attempts: int = 1
    source_concurrency: int = 2
    document_concurrency: int = 6
    city_round_robin: bool = True
    start_date: date = field(default_factory=lambda: date(2018, 1, 1))
    end_date: date = field(default_factory=date.today)
    max_cities: int | None = None
    max_sources: int | None = None
    max_http_calls: int = 1000
    max_ai_calls: int = 0
    max_search_calls: int = 0
    source_roles: tuple[str, ...] = REQUIRED_ROLES
    discover_missing: bool = False
    apply: bool = False
    dry_run: bool = False
    resume: bool = True
    gold_enabled: bool = False
    output: Path | None = None
    city_ids: tuple[str, ...] = ()

    def validate(self) -> None:
        if self.mode != FAST_BULK_INGEST:
            raise ValueError(f"unsupported fast mode: {self.mode}")
        for name in (
            "max_minutes_per_source",
            "max_list_pages_per_source",
            "max_documents_per_source",
            "max_document_retries",
            "max_attachment_attempts",
            "source_concurrency",
            "document_concurrency",
            "max_http_calls",
        ):
            if int(getattr(self, name)) < 1:
                raise ValueError(f"{name} must be positive")
        for name in ("max_cities", "max_sources"):
            value = getattr(self, name)
            if value is not None and int(value) < 1:
                raise ValueError(f"{name} must be positive when set")
        if self.end_date < self.start_date:
            raise ValueError("end_date must not precede start_date")
        if self.gold_enabled:
            raise ValueError("Gold policy-intensity is a disabled placeholder and cannot run")
        invalid = set(self.source_roles) - set(REQUIRED_ROLES)
        if invalid:
            raise ValueError(f"unsupported source roles: {sorted(invalid)}")

    @classmethod
    def from_mapping(cls, mapping: Mapping[str, Any] | None) -> FastBulkConfig:
        raw = dict(mapping or {})
        section = raw.get("fast_bulk_ingest", raw)
        if not isinstance(section, Mapping):
            section = {}
        values: dict[str, Any] = {}
        fields = cls.__dataclass_fields__
        for name in fields:
            if name in section:
                values[name] = section[name]
        if "start_date" in values:
            values["start_date"] = date.fromisoformat(str(values["start_date"]))
        if "end_date" in values:
            values["end_date"] = date.today() if str(values["end_date"]).lower() == "today" else date.fromisoformat(str(values["end_date"]))
        for name in ("source_roles", "city_ids"):
            if name in values:
                values[name] = tuple(str(item) for item in (values[name] or ()))
        if values.get("output"):
            values["output"] = Path(str(values["output"]))
        config = cls(**values)
        config.validate()
        return config


def load_fast_bulk_config(path: Path | None = None) -> FastBulkConfig:
    if path is None:
        return FastBulkConfig()
    payload = yaml.safe_load(path.read_text(encoding="utf-8-sig")) if path.exists() else {}
    if not isinstance(payload, Mapping):
        return FastBulkConfig()
    merged = dict(payload)
    window = payload.get("date_window")
    section = dict(payload.get("fast_bulk_ingest") or {}) if isinstance(payload.get("fast_bulk_ingest"), Mapping) else {}
    if isinstance(window, Mapping):
        for name in ("start_date", "end_date"):
            if name not in section and name in window:
                section[name] = window[name]
    merged["fast_bulk_ingest"] = section
    return FastBulkConfig.from_mapping(merged)


def _source_city_ids(row: Mapping[str, Any]) -> set[str]:
    raw = row.get("city_ids") or row.get("city_id") or []
    if isinstance(raw, str):
        try:
            decoded = json.loads(raw)
            raw = decoded if isinstance(decoded, list) else [raw]
        except json.JSONDecodeError:
            raw = [raw]
    return {str(value) for value in raw if value}


def _city_metrics(settings: Settings) -> dict[str, dict[str, Any]]:
    slots = _read(settings, "source_requirement_slots", ["city_id", "city_name", "province_name", "source_role", "status"])
    if slots.is_empty():
        return {}
    metrics: dict[str, dict[str, Any]] = {}
    for row in slots.to_dicts():
        city_id = str(row.get("city_id") or "")
        if not city_id:
            continue
        value = metrics.setdefault(city_id, {"city_id": city_id, "city_name": row.get("city_name"), "province_name": row.get("province_name"), "roles": {}, "documents": 0, "years": set()})
        value["roles"][str(row.get("source_role") or "")] = str(row.get("status") or "UNRESOLVED")
    sources = _read(settings, "source_registry", ["source_id", "city_ids", "source_role", "agency_type"])
    source_city: dict[str, set[str]] = {}
    for row in sources.to_dicts():
        source_city[str(row.get("source_id"))] = _source_city_ids(row)
    documents = _read(settings, "policy_document_versions", ["source_id", "created_at", "first_seen_at"])
    if not documents.is_empty():
        for row in documents.to_dicts():
            for city_id in source_city.get(str(row.get("source_id")), set()):
                if city_id in metrics:
                    metrics[city_id]["documents"] += 1
                    timestamp = row.get("created_at") or row.get("first_seen_at")
                    parsed = _parse_dt(timestamp)
                    if parsed:
                        metrics[city_id]["years"].add(parsed.year)
    for value in metrics.values():
        value["missing_roles"] = sum(role not in value["roles"] or value["roles"][role] not in {"enabled", "verified", "resolved"} for role in REQUIRED_ROLES)
        value["missing_years"] = max(0, date.today().year - 2018 + 1 - len(value["years"]))
    return metrics


def _ready_source_rows(settings: Settings, config: FastBulkConfig) -> list[dict[str, Any]]:
    planning = FullSyncConfig(
        max_slots=525,
        max_sources=10000,
        max_documents=config.max_documents_per_source,
        max_http_calls=config.max_http_calls,
        backfill=True,
        backfill_from=config.start_date,
        backfill_to=config.end_date,
        resume=config.resume,
    )
    plan = build_sync_plan(settings, planning)
    allowed = set(config.source_roles)
    rows = []
    now = datetime.now(UTC)
    for row in plan["sources"]:
        role = str(row.get("agency_type") or row.get("source_role") or "")
        if role not in allowed or not _bool(row.get("crawl_enabled")) or not source_is_crawl_ready(row):
            continue
        retry_at = _parse_dt(row.get("next_retry_at"))
        if retry_at and retry_at > now:
            continue
        rows.append(dict(row))
    return rows


def select_city_source_queue(
    settings: Settings,
    config: FastBulkConfig | None = None,
    *,
    city_ids: Sequence[str] | None = None,
) -> dict[str, Any]:
    """Select cities by breadth priority and sources in city/role round-robin."""
    config = config or FastBulkConfig()
    config.validate()
    metrics = _city_metrics(settings)
    requested = {str(value) for value in (city_ids or config.city_ids) if value}
    cities = [value for key, value in metrics.items() if not requested or key in requested]
    ready = _ready_source_rows(settings, config)
    ready_city_ids = {city_id for row in ready for city_id in _source_city_ids(row)}
    # A city with no documents but no registered crawl-ready source cannot
    # make progress in Bronze.  Keep it in the global queue, but let a
    # no-document city with an actionable source win a bounded round.
    cities.sort(key=lambda value: (value["city_id"] not in ready_city_ids, value["documents"] > 0, value["documents"], -value["missing_years"], -value["missing_roles"], value["city_id"]))
    if config.max_cities is not None:
        cities = cities[: config.max_cities]
    by_city_role: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in ready:
        roles = {str(row.get("agency_type") or ""), str(row.get("source_role") or "")}
        for city_id in _source_city_ids(row):
            for role in roles & set(config.source_roles):
                by_city_role.setdefault((city_id, role), []).append(row)
    for rows in by_city_role.values():
        rows.sort(key=lambda item: (int(item.get("priority") or 0), str(item.get("source_id") or "")))
    tasks: list[dict[str, Any]] = []
    if config.city_round_robin:
        city_role_pairs = ((city, role) for city in cities for role in config.source_roles)
    else:
        city_role_pairs = ((city, role) for role in config.source_roles for city in cities)
    for city, role in city_role_pairs:
        candidates = by_city_role.get((city["city_id"], role), [])
        if not candidates:
            continue
        row = candidates[0]
        tasks.append({
            "task_id": hashlib.sha256(f"{city['city_id']}|{role}|{row.get('source_id')}".encode()).hexdigest()[:24],
            "city_id": city["city_id"],
            "city_name": city["city_name"],
            "province_name": city["province_name"],
            "source_id": str(row.get("source_id")),
            "source_role": role,
            "source_name": row.get("source_name"),
            "source_state": row.get("source_state"),
        })
        if config.max_sources is not None and len(tasks) >= config.max_sources:
            break
    if config.max_sources is not None:
        tasks = tasks[: config.max_sources]
    return {"cities": cities, "tasks": tasks, "city_count": len(cities), "source_count": len(tasks), "role_order": list(config.source_roles)}


def _status_from_summary(summary: Mapping[str, Any]) -> str:
    results = summary.get("source_results") or []
    result = results[0] if results else summary
    category = str(result.get("status_category") or "").upper()
    status = str(result.get("status") or summary.get("status") or "").lower()
    if category in SOURCE_STATUSES:
        return category
    if status in {"completed", "complete"}:
        return "COMPLETE_WITH_GAPS" if category == "COMPLETE_WITH_GAPS" else "SUCCESS"
    if status in {"partial", "cancelled"}:
        fetched = int(result.get("fetched") or result.get("persisted_fetched") or 0)
        return "PARTIAL_BUT_USABLE" if fetched else "PARTIAL_EMPTY"
    if status in {"paused_budget", "budget_zero"}:
        return "PAUSED_BUDGET"
    if status in {"retry_wait", "failed_recoverable"}:
        return "RETRY_WAIT"
    if status == "human_review":
        return "HUMAN_REVIEW"
    if status in {"skipped_dependency", "blocked_no_enabled_sources"}:
        return "SKIPPED_DEPENDENCY"
    return "FAILED_TERMINAL" if int(summary.get("exit_code") or 0) not in {0, 10} else "RETRY_WAIT"


class FastBulkIngestController:
    """One bounded run; writes only append/checkpoint artifacts plus pipeline data."""

    def __init__(self, settings: Settings | None = None, *, config: FastBulkConfig | None = None, output: Path | None = None, run_id: str | None = None):
        self.settings = settings or Settings.discover()
        self.config = config or FastBulkConfig()
        self.config.validate()
        self.run_id = run_id or f"FAST_{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}"
        self.run_dir = output or self.config.output or self.settings.outputs / "fast_bulk_ingest" / self.run_id
        self.state_path = self.run_dir / "current_status.json"
        self.transition_path = self.run_dir / "state_transitions.jsonl"
        self.checkpoint_path = self.run_dir / "fast_bulk_checkpoints.jsonl"
        self.lock_path = self.settings.jobs / "fast_bulk_ingest.lock"
        self._lock_acquired = False

    def _state(self) -> dict[str, Any]:
        if self.state_path.exists():
            try:
                return json.loads(self.state_path.read_text(encoding="utf-8-sig"))
            except (OSError, json.JSONDecodeError):
                pass
        return {
            "automation_id": self.run_id,
            "run_id": self.run_id,
            "mode": FAST_BULK_INGEST,
            "status": "INITIALIZING",
            "round": "ROUND_1_FAST_COVERAGE",
            "started_at": _now(),
            "last_progress_at": _now(),
            "last_heartbeat_at": _now(),
            "planned_cities": 0,
            "planned_sources": 0,
            "processed_cities": 0,
            "processed_sources": 0,
            "documents_added": 0,
            "source_results": [],
            "gold": {"enabled": False, "status": "DISABLED_PLACEHOLDER", "policy_intensity_calls": 0},
            "budgets": {"http_calls": 0, "ai_calls": 0, "search_calls": 0, "tokens": None, "estimated_cost": None},
        }

    def _write_state(self, updates: Mapping[str, Any]) -> dict[str, Any]:
        current = self._state()
        current.update(dict(updates))
        current["last_heartbeat_at"] = _now()
        if updates.get("status") not in {"INITIALIZING", None}:
            current["last_progress_at"] = _now()
        _atomic_json(self.state_path, current)
        _atomic_json(self.settings.outputs / "fast_bulk_ingest" / "current_status.json", current)
        return current

    def _transition(self, event_type: str, *, task: Mapping[str, Any] | None = None, reason_code: str) -> None:
        row = {"run_id": self.run_id, "event_type": event_type, "slot_id": None, "source_id": (task or {}).get("source_id"), "city_id": (task or {}).get("city_id"), "reason_code": reason_code, "timestamp": _now()}
        row["idempotency_key"] = hashlib.sha256(json.dumps(row, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
        _append_jsonl(self.transition_path, row, unique_key="idempotency_key")

    def _claim_lock(self) -> None:
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            stream = self.lock_path.open("x", encoding="utf-8")
        except FileExistsError as exc:
            raise RuntimeError(f"FAST_BULK_INGEST already has an active lock: {self.lock_path}") from exc
        stream.write(json.dumps({"run_id": self.run_id, "pid": os.getpid(), "created_at": _now()}))
        stream.close()
        self._lock_acquired = True

    def _release_lock(self) -> None:
        if self._lock_acquired:
            self.lock_path.unlink(missing_ok=True)
            self._lock_acquired = False

    def _stop_requested(self) -> bool:
        return (self.run_dir / "STOP_AUTOPILOT").exists() or (self.settings.data_root / "control" / "STOP_FULL_SYNC").exists()

    def _existing_completed(self) -> set[str]:
        result: set[str] = set()
        if not self.checkpoint_path.exists():
            return result
        for line in self.checkpoint_path.read_text(encoding="utf-8").splitlines():
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if row.get("checkpoint_type") == "SOURCE_COMPLETED" and row.get("task_id"):
                result.add(str(row["task_id"]))
        return result

    def run(self, *, city_ids: Sequence[str] | None = None) -> dict[str, Any]:
        if self.config.dry_run or not self.config.apply:
            queue = select_city_source_queue(self.settings, self.config, city_ids=city_ids)
            state = self._write_state({"status": "PLANNED", "planned_cities": queue["city_count"], "planned_sources": queue["source_count"], "queue": queue})
            return {"run_id": self.run_id, "run_dir": str(self.run_dir), "status": "PLANNED", "queue": queue, "state": state, "exit_code": 0}
        self._claim_lock()
        try:
            queue = select_city_source_queue(self.settings, self.config, city_ids=city_ids)
            state = self._write_state({"status": "RUNNING", "planned_cities": queue["city_count"], "planned_sources": queue["source_count"], "queue": queue, "current_step": "batch_claimed"})
            _atomic_json(self.run_dir / "fast_bulk_manifest.json", {"run_id": self.run_id, "mode": FAST_BULK_INGEST, "config": {key: str(value) if isinstance(value, (Path, date)) else value for key, value in asdict(self.config).items()}, "gold_enabled": False, "created_at": _now()})
            self._transition("batch_claimed", reason_code="round_1_fast_coverage")
            completed = self._existing_completed() if self.config.resume else set()
            results = list(state.get("source_results") or [])
            docs_added = int(state.get("documents_added") or 0)
            for task in queue["tasks"]:
                if self._stop_requested():
                    self._transition("batch_paused", task=task, reason_code="stop_file")
                    self._write_state({"status": "PAUSED_BUDGET", "stop_reason": "STOP_FULL_SYNC"})
                    break
                if task["task_id"] in completed:
                    continue
                self._transition("slot_claimed", task=task, reason_code="city_round_robin")
                self._write_state({"status": "RUNNING", "current_city": task["city_name"], "current_city_id": task["city_id"], "current_source": task["source_id"], "current_source_role": task["source_role"], "current_step": "source_claimed", "processed_sources": len(results)})
                task_dir = self.run_dir / "sources" / task["task_id"]
                full_config = FullSyncConfig(
                    scope="source",
                    source_id=task["source_id"],
                    discovery_mode="DISABLED",
                    backfill=True,
                    backfill_from=self.config.start_date,
                    backfill_to=self.config.end_date,
                    max_slots=1,
                    max_sources=1,
                    max_documents=self.config.max_documents_per_source,
                    max_minutes_per_source=self.config.max_minutes_per_source,
                    max_list_pages_per_source=self.config.max_list_pages_per_source,
                    max_document_retries=self.config.max_document_retries,
                    max_attachment_attempts=self.config.max_attachment_attempts,
                    max_http_calls=self.config.max_http_calls,
                    max_ai_calls=0,
                    max_search_calls=0,
                    crawl_concurrency=self.config.document_concurrency,
                    resume=self.config.resume,
                    apply=True,
                    dry_run=False,
                    output=task_dir,
                )
                self._transition("source_run_started", task=task, reason_code="bounded_source_budget")
                try:
                    summary = FullSyncController(self.settings, config=full_config, output=task_dir, run_id=task["task_id"]).run(command="run")
                    status = _status_from_summary(summary)
                    source_result = {"task_id": task["task_id"], "city_id": task["city_id"], "city_name": task["city_name"], "source_id": task["source_id"], "source_role": task["source_role"], "status": status, "fetched": int(sum(int(item.get("fetched") or 0) for item in (summary.get("source_results") or []))), "exit_code": summary.get("exit_code"), "run_dir": str(task_dir), "reason": (summary.get("source_results") or [{}])[0].get("reason_code"), "error": (summary.get("source_results") or [{}])[0].get("error_message")}
                except Exception as exc:
                    source_result = {**task, "status": "FAILED_TERMINAL", "fetched": 0, "error": f"{type(exc).__name__}: {str(exc)[:500]}", "run_dir": str(task_dir)}
                results.append(source_result)
                docs_added += int(source_result.get("fetched") or 0)
                checkpoint_type = "SOURCE_COMPLETED" if source_result["status"] in {"SUCCESS", "COMPLETE_WITH_GAPS"} else "SOURCE_ATTEMPTED"
                checkpoint = {"checkpoint_type": checkpoint_type, "task_id": task["task_id"], "run_id": self.run_id, "source_id": task["source_id"], "status": source_result["status"], "created_at": _now()}
                checkpoint["checkpoint_id"] = hashlib.sha256(f"{self.run_id}|{task['task_id']}|{checkpoint_type}|{source_result['status']}".encode()).hexdigest()
                _append_jsonl(self.checkpoint_path, checkpoint, unique_key="checkpoint_id")
                self._transition("source_run_completed", task=task, reason_code=source_result["status"])
                self._write_state({"source_results": results, "processed_sources": len(results), "processed_cities": len({str(item.get('city_id')) for item in results}), "documents_added": docs_added, "current_step": "checkpointed"})
            final_status = "PAUSED_BUDGET" if self._stop_requested() else "COMPLETED"
            self._transition("batch_completed", reason_code="stop_file" if final_status == "PAUSED_BUDGET" else "bounded_round_finished")
            state = self._write_state({"status": final_status, "source_results": results, "processed_sources": len(results), "processed_cities": len({str(item.get('city_id')) for item in results}), "documents_added": docs_added, "current_city": None, "current_source": None, "current_step": "batch_completed", "completed_at": _now()})
            return {"run_id": self.run_id, "run_dir": str(self.run_dir), "status": final_status, "source_results": results, "documents_added": docs_added, "processed_sources": len(results), "processed_cities": len({str(item.get('city_id')) for item in results}), "planned_cities": queue["city_count"], "planned_sources": queue["source_count"], "exit_code": 10 if final_status == "PAUSED_BUDGET" else 0, "state": state}
        finally:
            self._release_lock()


__all__ = [
    "FAST_BULK_INGEST",
    "SOURCE_STATUSES",
    "FastBulkConfig",
    "FastBulkIngestController",
    "load_fast_bulk_config",
    "select_city_source_queue",
]
