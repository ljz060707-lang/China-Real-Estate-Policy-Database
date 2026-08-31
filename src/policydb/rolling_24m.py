"""Resumable rolling 24-month city/source backfill.

The queue is deliberately one row per ``city x enabled source``.  A row is
an auditable historical list session; the crawler may discover and bucket
multiple month shards during that session.  This keeps the existing shard
granularity while avoiding a fresh top-of-list request for every month.

This module is a scheduler boundary around the existing ``ExhaustiveCrawler``
and promotion pipeline.  It is not a second crawler and it never promotes a
document without the normal deterministic relevance and quality gates.
"""

from __future__ import annotations

import calendar
import json
import os
import shutil
import tempfile
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import polars as pl
from filelock import FileLock

from policydb.archive import archive_document_versions
from policydb.crawl.registry import load_registry
from policydb.dedup_audit import materialize_policy_identity
from policydb.exhaustive import ExhaustiveCrawler
from policydb.ingest.promote_versions import promote_document_versions
from policydb.ingest.relevance import audit_recent_relevance
from policydb.parquet_store import ParquetStoreError, atomic_write_parquet, read_parquet_snapshot
from policydb.pdf_pipeline import PDFPipeline, load_pdf_config
from policydb.query.database import build_database
from policydb.recent_priority import _pdf_counts, _run_version_ids
from policydb.scope import load_cities_105
from policydb.settings import Settings
from policydb.transform.normalization import stable_id

QUEUE_NAME = "ROLLING_24M_QUEUE.parquet"
STATE_NAME = "ROLLING_24M_STATE.json"
SUMMARY_NAME = "ROLLING_24M_SUMMARY.json"
COMPLETE_STATUSES = {
    "POLICY_FOUND",
    "CONFIRMED_ZERO",
    "COMPLETE_UNVERIFIED",
    "SOURCE_INCOMPLETE",
    "FAILED",
}
RETRYABLE_STATUSES = {"PENDING", "RUNNING", "RETRY_WAIT", "PARTIAL_NETWORK", "PARTIAL_TEMPORAL"}


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


def _stop_requested(settings: Settings) -> bool:
    """Return whether a rolling session should yield at its next safe boundary."""

    return any(
        path.exists()
        for path in (
            settings.automation / "STOP",
            settings.data_root / "control" / "STOP_FULL_SYNC",
            settings.data_root / "control" / "STOP_AUTOPILOT",
        )
    )


def _atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    temporary = Path(name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(payload, stream, ensure_ascii=False, indent=2, default=str)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _month_start(value: date) -> date:
    return value.replace(day=1)


def _add_months(value: date, months: int) -> date:
    index = value.year * 12 + value.month - 1 + months
    year, month_index = divmod(index, 12)
    month = month_index + 1
    day = min(value.day, calendar.monthrange(year, month)[1])
    return date(year, month, day)


def rolling_window(today: date | None = None) -> tuple[date, date]:
    """Return a dynamic 24-calendar-month target ending on ``today``.

    The metadata preserves the exact date 24 months before the end date.  The
    month-bucket helper below intentionally reports the latest 24 calendar
    buckets; the crawler still receives the exact date boundaries.
    """

    end = today or date.today()
    return _add_months(end, -24), end


def target_months(start_date: date, end_date: date) -> list[str]:
    end = _month_start(end_date)
    current = max(_month_start(start_date), _month_start(_add_months(end_date, -23)))
    values: list[str] = []
    while current <= end:
        values.append(current.strftime("%Y-%m"))
        current = _add_months(current, 1)
    return values


@dataclass(frozen=True)
class Rolling24MConfig:
    start_date: date | None = None
    end_date: date | None = None
    max_items: int = 5
    max_pages_per_source: int = 300
    max_candidates_per_shard: int = 5000
    max_fetches_per_shard: int = 5000
    max_attempts: int = 3
    pdf_discovery_limit: int = 30
    apply: bool = False
    resume: bool = True

    @classmethod
    def default(cls, *, today: date | None = None, **updates: Any) -> Rolling24MConfig:
        start, end = rolling_window(today)
        values: dict[str, Any] = {"start_date": start, "end_date": end}
        values.update({key: value for key, value in updates.items() if value is not None})
        return cls(**values)

    def resolved_window(self) -> tuple[date, date]:
        default_start, default_end = rolling_window()
        return self.start_date or default_start, self.end_date or default_end

    def validate(self) -> None:
        start, end = self.resolved_window()
        if start > end:
            raise ValueError("rolling-24m start_date must not be after end_date")
        for name in (
            "max_items",
            "max_pages_per_source",
            "max_candidates_per_shard",
            "max_fetches_per_shard",
            "max_attempts",
        ):
            if int(getattr(self, name)) < 1:
                raise ValueError(f"rolling-24m {name} must be positive")


def _root(settings: Settings) -> Path:
    path = settings.outputs / "rolling_24m"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _queue_path(settings: Settings) -> Path:
    return _root(settings) / QUEUE_NAME


def _state_path(settings: Settings) -> Path:
    settings.automation.mkdir(parents=True, exist_ok=True)
    return settings.automation / STATE_NAME


def _write_progress_snapshot(settings: Settings, queue: pl.DataFrame, state: dict[str, Any]) -> None:
    """Publish the current rolling source-session position atomically."""

    path = settings.automation / "PROGRESS_SNAPSHOT.json"
    try:
        previous = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
    except (OSError, json.JSONDecodeError):
        previous = {}
    if not isinstance(previous, dict):
        previous = {}
    statuses = queue.get_column("status") if queue.height else pl.Series([], dtype=pl.String)
    completed = int(statuses.is_in(list(COMPLETE_STATUSES)).sum()) if queue.height else 0
    total = queue.height
    target_months_value = json.loads(queue[0, "target_months"]) if queue.height and queue[0, "target_months"] else []
    checked = queue.filter(pl.col("status").is_in(list(COMPLETE_STATUSES))) if queue.height else queue
    started = queue.filter(pl.col("attempt_count") > 0) if queue.height else queue
    payload = {
        **previous,
        "timestamp": _now(),
        "stage": "ROLLING_24M_FULL_CITY_BACKFILL",
        "batch": state.get("batch_id"),
        "stage_status": state.get("stage_status"),
        "batch_status": state.get("batch_status"),
        "current_step": state.get("last_event") or "SOURCE_SESSION",
        "current_city": state.get("current_city"),
        "current_source": state.get("current_source"),
        "current_source_role": state.get("current_source_role"),
        "current_window": {
            **(previous.get("current_window") or {}),
            "rolling_start": state.get("window_start"),
            "rolling_end": state.get("window_end"),
        },
        "rolling_24m": {
            **(previous.get("rolling_24m") or {}),
            "total": total,
            "completed": completed,
            "pending": int((statuses == "PENDING").sum()) if queue.height else 0,
            "running": int((statuses == "RUNNING").sum()) if queue.height else 0,
            "failed": int((statuses == "FAILED").sum()) if queue.height else 0,
            "retryable": int(statuses.is_in(list(RETRYABLE_STATUSES)).sum()) if queue.height else 0,
            "cities_checked": int(checked.get_column("city_id").n_unique()) if checked.height else 0,
            "cities_total": 105,
            "source_slots_checked": completed,
            "source_slots_total": total,
            "city_month_checked": int(started.get_column("city_id").n_unique()) * len(target_months_value) if started.height else 0,
            "city_month_total": 105 * len(target_months_value),
            "progress_pct": round(100.0 * completed / total, 2) if total else 0.0,
        },
        "last_real_progress_at": state.get("last_real_progress_at") or _now(),
        "heartbeat_at": _now(),
    }
    _atomic_json(path, payload)


def _schema(frame: pl.DataFrame) -> pl.DataFrame:
    columns = {
        "queue_item_id": pl.String,
        "city_id": pl.String,
        "city_code": pl.String,
        "city_name": pl.String,
        "source_id": pl.String,
        "source_role": pl.String,
        "official_domain": pl.String,
        "entry_url": pl.String,
        "window_start": pl.String,
        "window_end": pl.String,
        "target_months": pl.String,
        "priority": pl.Int64,
        "status": pl.String,
        "attempt_count": pl.Int64,
        "last_attempt_at": pl.String,
        "completed_at": pl.String,
        "failure_reason": pl.String,
        "documents_found": pl.Int64,
        "versions_created": pl.Int64,
        "records_promoted": pl.Int64,
        "pdfs_found": pl.Int64,
        "pdfs_archived": pl.Int64,
        "pdf_discovery_count": pl.Int64,
        "coverage_status": pl.String,
        "lease_owner": pl.String,
        "lease_acquired_at": pl.String,
        "lease_expires_at": pl.String,
        "run_ids_json": pl.String,
        "updated_at": pl.String,
    }
    frame = frame.clone()
    for name, dtype in columns.items():
        if name not in frame.columns:
            frame = frame.with_columns(pl.lit(None, dtype=dtype).alias(name))
        else:
            frame = frame.with_columns(pl.col(name).cast(dtype, strict=False).alias(name))
    return frame.select(list(columns))


def _write_queue(settings: Settings, frame: pl.DataFrame) -> None:
    atomic_write_parquet(
        _schema(frame),
        _queue_path(settings),
        {"module": "rolling_24m", "operation": "checkpoint"},
        key_columns=("queue_item_id",),
    )


def _city_priority(city: dict[str, Any]) -> int:
    value = str(city.get("city_tier_existing") or "").lower()
    if any(token in value for token in ("一线", "first", "1")):
        return 0
    if any(token in value for token in ("省会", "capital", "二线", "2")):
        return 1
    return 2


def _base_rows(settings: Settings, start: date, end: date) -> list[dict[str, Any]]:
    cities = load_cities_105(settings)
    registry = load_registry(settings)
    months = target_months(start, end)
    rows: list[dict[str, Any]] = []
    covered_city_ids: set[str] = set()
    for city in cities.iter_rows(named=True):
        city_id = str(city["city_id"])
        city_sources = [
            source
            for source in registry
            if source.crawl_enabled
            and str(source.official_status).lower() == "official"
            and city_id in {str(value) for value in source.city_ids}
        ]
        if city_sources:
            covered_city_ids.add(city_id)
        for source in city_sources:
            role = str(source.agency_type or source.source_role)
            queue_id = stable_id(
                city_id,
                source.source_id,
                role,
                start.isoformat(),
                end.isoformat(),
                prefix="ROLL24",
            )
            rows.append(
                {
                    "queue_item_id": queue_id,
                    "city_id": city_id,
                    "city_code": str(city["city_code"]),
                    "city_name": str(city["city_name"]),
                    "source_id": str(source.source_id),
                    "source_role": role,
                    "official_domain": str(source.domain),
                    "entry_url": (list(source.list_page_urls) or [source.homepage_url])[0],
                    "window_start": start.isoformat(),
                    "window_end": end.isoformat(),
                    "target_months": json.dumps(months, ensure_ascii=False),
                    "priority": _city_priority(city),
                    "status": "PENDING",
                    "attempt_count": 0,
                    "last_attempt_at": None,
                    "completed_at": None,
                    "failure_reason": None,
                    "documents_found": 0,
                    "versions_created": 0,
                    "records_promoted": 0,
                    "pdfs_found": 0,
                    "pdfs_archived": 0,
                    "pdf_discovery_count": 0,
                    "coverage_status": "NOT_STARTED",
                    "lease_owner": None,
                    "lease_acquired_at": None,
                    "lease_expires_at": None,
                    "run_ids_json": "[]",
                    "updated_at": _now(),
                }
            )
    for city in cities.iter_rows(named=True):
        city_id = str(city["city_id"])
        if city_id in covered_city_ids:
            continue
        rows.append(
            {
                "queue_item_id": stable_id(city_id, "SOURCE_INCOMPLETE", start.isoformat(), end.isoformat(), prefix="ROLL24"),
                "city_id": city_id,
                "city_code": str(city["city_code"]),
                "city_name": str(city["city_name"]),
                "source_id": f"UNRESOLVED:{city_id}",
                "source_role": "source_incomplete",
                "official_domain": None,
                "entry_url": None,
                "window_start": start.isoformat(),
                "window_end": end.isoformat(),
                "target_months": json.dumps(months, ensure_ascii=False),
                "priority": _city_priority(city),
                "status": "SOURCE_INCOMPLETE",
                "attempt_count": 1,
                "last_attempt_at": _now(),
                "completed_at": _now(),
                "failure_reason": "NO_ENABLED_OFFICIAL_SOURCE",
                "documents_found": 0,
                "versions_created": 0,
                "records_promoted": 0,
                "pdfs_found": 0,
                "pdfs_archived": 0,
                "pdf_discovery_count": 0,
                "coverage_status": "SOURCE_INCOMPLETE",
                "lease_owner": None,
                "lease_acquired_at": None,
                "lease_expires_at": None,
                "run_ids_json": "[]",
                "updated_at": _now(),
            }
        )
    return sorted(rows, key=lambda row: (row["priority"], row["city_name"], row["source_role"], row["source_id"]))


def _archive_previous_queue(settings: Settings, previous: pl.DataFrame) -> None:
    if previous.is_empty() or not {"window_start", "window_end"}.issubset(previous.columns):
        return
    starts = previous.get_column("window_start").drop_nulls().unique().to_list()
    ends = previous.get_column("window_end").drop_nulls().unique().to_list()
    if len(starts) != 1 or len(ends) != 1:
        return
    history = _root(settings) / "history"
    history.mkdir(parents=True, exist_ok=True)
    target = history / f"ROLLING_24M_QUEUE_{starts[0]}_{ends[0]}.parquet"
    if not target.exists():
        shutil.copy2(_queue_path(settings), target)


def build_rolling_queue(
    settings: Settings | None = None,
    *,
    config: Rolling24MConfig | None = None,
    resume: bool = True,
) -> pl.DataFrame:
    settings = settings or Settings.discover()
    config = config or Rolling24MConfig.default()
    config.validate()
    start, end = config.resolved_window()
    path = _queue_path(settings)
    previous = _schema(read_parquet_snapshot(path)) if path.exists() else pl.DataFrame()
    if previous.height:
        old_windows = set(previous.get_column("window_start").drop_nulls().to_list()) | set(
            previous.get_column("window_end").drop_nulls().to_list()
        )
        if old_windows - {start.isoformat(), end.isoformat()}:
            _archive_previous_queue(settings, previous)
            previous = pl.DataFrame()
    old_by_id = {str(row["queue_item_id"]): row for row in previous.iter_rows(named=True)} if previous.height else {}
    rows = _base_rows(settings, start, end)
    preserved: list[dict[str, Any]] = []
    for row in rows:
        old = old_by_id.get(row["queue_item_id"])
        if old and resume:
            for key in (
                "status", "attempt_count", "last_attempt_at", "completed_at", "failure_reason",
                "documents_found", "versions_created", "records_promoted", "pdfs_found",
                "pdfs_archived", "pdf_discovery_count", "coverage_status", "run_ids_json", "updated_at",
            ):
                if key in old:
                    row[key] = old[key]
            if row.get("status") == "RUNNING":
                row.update(
                    status="RETRY_WAIT",
                    failure_reason="WORKER_LOST_OR_STALE_LEASE",
                    coverage_status="PARTIAL_NETWORK",
                    lease_owner=None,
                    lease_acquired_at=None,
                    lease_expires_at=None,
                )
        preserved.append(row)
    frame = _schema(pl.DataFrame(preserved, infer_schema_length=None))
    _write_queue(settings, frame)
    _atomic_json(
        _state_path(settings),
        {
            "status": "PLANNED",
            "stage_status": "RUNNING" if frame.height else "COMPLETE",
            "batch_status": "NOT_STARTED",
            "queue_total": frame.height,
            "window_start": start.isoformat(),
            "window_end": end.isoformat(),
            "target_months": target_months(start, end),
            "updated_at": _now(),
        },
    )
    return frame


def _counts(frame: pl.DataFrame) -> dict[str, int]:
    if frame.is_empty():
        return {"total": 0, "completed": 0, "pending": 0, "running": 0, "retryable": 0, "failed": 0}
    status = frame.get_column("status").cast(pl.String)
    return {
        "total": frame.height,
        "completed": int(status.is_in(list(COMPLETE_STATUSES)).sum()),
        "pending": int(status.eq("PENDING").sum()),
        "running": int(status.eq("RUNNING").sum()),
        "retryable": int(status.is_in(list(RETRYABLE_STATUSES)).sum()),
        "failed": int(status.eq("FAILED").sum()),
    }


def _summary(settings: Settings, queue: pl.DataFrame, state: dict[str, Any]) -> dict[str, Any]:
    queue = _schema(queue)
    counts = _counts(queue)
    target = json.loads(queue[0, "target_months"]) if queue.height and queue[0, "target_months"] else []
    checked = queue.filter(pl.col("status").is_in(list(COMPLETE_STATUSES))) if queue.height else queue
    started = queue.filter(pl.col("attempt_count") > 0) if queue.height else queue
    city_month_total = 105 * len(target)
    city_month_checked = started.get_column("city_id").n_unique() * len(target) if started.height else 0
    summary = {
        **state,
        **counts,
        "queue_total": queue.height,
        "cities_total": 105,
        "cities_started": started.get_column("city_id").n_unique() if started.height else 0,
        "cities_completed": checked.get_column("city_id").n_unique() if checked.height else 0,
        "source_slots_total": queue.height,
        "source_slots_checked": checked.height,
        "city_month_total": city_month_total,
        "city_month_checked": min(city_month_total, city_month_checked),
        "coverage_pct": round((checked.height / queue.height) * 100, 2) if queue.height else 0.0,
        "documents_found": int(queue.get_column("documents_found").sum() or 0) if queue.height else 0,
        "versions": int(queue.get_column("versions_created").sum() or 0) if queue.height else 0,
        "records_promoted": int(queue.get_column("records_promoted").sum() or 0) if queue.height else 0,
        "pdfs_found": int(queue.get_column("pdfs_found").sum() or 0) if queue.height else 0,
        "pdfs_archived": int(queue.get_column("pdfs_archived").sum() or 0) if queue.height else 0,
        "pdf_discovery_count": int(queue.get_column("pdf_discovery_count").sum() or 0) if queue.height else 0,
        "policy_found": int(queue.filter(pl.col("status") == "POLICY_FOUND").height) if queue.height else 0,
        "confirmed_zero": int(queue.filter(pl.col("status") == "CONFIRMED_ZERO").height) if queue.height else 0,
        "partial": int(queue.filter(pl.col("status").is_in(["PARTIAL_NETWORK", "PARTIAL_TEMPORAL", "RETRY_WAIT"])).height) if queue.height else 0,
        "source_incomplete": int(queue.filter(pl.col("status") == "SOURCE_INCOMPLETE").height) if queue.height else 0,
        "updated_at": _now(),
    }
    _atomic_json(_root(settings) / SUMMARY_NAME, summary)
    _atomic_json(_state_path(settings), summary)
    return summary


def rolling_audit(settings: Settings | None = None, *, config: Rolling24MConfig | None = None) -> dict[str, Any]:
    settings = settings or Settings.discover()
    config = config or Rolling24MConfig.default()
    queue = build_rolling_queue(settings, config=config, resume=True)
    start, end = config.resolved_window()
    state = {
        "status": "COMPLETE" if _counts(queue)["pending"] == 0 and _counts(queue)["running"] == 0 and _counts(queue)["retryable"] == 0 else "PARTIAL",
        "stage_status": "COMPLETE" if _counts(queue)["pending"] == 0 and _counts(queue)["running"] == 0 and _counts(queue)["retryable"] == 0 else "RUNNING",
        "batch_status": "AUDIT_COMPLETED",
        "window_start": start.isoformat(),
        "window_end": end.isoformat(),
        "coverage_audit": "PASS" if _counts(queue)["retryable"] == 0 else "PARTIAL_WITH_RETRYABLE",
    }
    return _summary(settings, queue, state)


def run_rolling_24m(
    settings: Settings | None = None,
    *,
    config: Rolling24MConfig | None = None,
    run_id: str | None = None,
) -> dict[str, Any]:
    """Run a bounded source-session slice and checkpoint after every item."""

    settings = settings or Settings.discover()
    config = config or Rolling24MConfig.default()
    config.validate()
    run_id = run_id or f"ROLL24_{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}_{os.getpid()}"
    lock = FileLock(str(settings.automation / "ROLLING_24M_WRITER.lock"), timeout=0.2)
    with lock:
        queue = build_rolling_queue(settings, config=config, resume=config.resume)
        start, end = config.resolved_window()
        state = {
            "status": "RUNNING" if config.apply else "PLANNED",
            "stage_status": "RUNNING" if config.apply else "PLANNED",
            "batch_status": "RUNNING" if config.apply else "PLANNED",
            "batch_id": run_id,
            "current_queue_item": None,
            "current_city": None,
            "current_source": None,
            "current_source_role": None,
            "window_start": start.isoformat(),
            "window_end": end.isoformat(),
            "last_real_progress_at": _now(),
            "updated_at": _now(),
        }
        if not config.apply:
            return _summary(settings, queue, state)
        if _stop_requested(settings):
            state.update(
                {
                    "status": "PAUSED_BUDGET",
                    "stage_status": "PAUSED",
                    "batch_status": "STOP_REQUESTED",
                    "last_event": "STOP_REQUESTED",
                    "updated_at": _now(),
                }
            )
            summary = _summary(settings, queue, state)
            summary.update({"run_id": run_id, "processed_items": 0, "blocking_error": None, "exit_code": 0})
            _atomic_json(_root(settings) / f"ROLLING_24M_RUN_SUMMARY_{run_id}.json", summary)
            return summary
        candidates = [
            (index, row)
            for index, row in enumerate(queue.iter_rows(named=True))
            if str(row.get("status")) in RETRYABLE_STATUSES
            and int(row.get("attempt_count") or 0) < config.max_attempts
        ][: config.max_items]
        processed = 0
        blocking: str | None = None
        stop_seen = False
        for _index, row in candidates:
            item_id = str(row["queue_item_id"])
            acquired = _now()
            expires = datetime.now(UTC).replace(microsecond=0)
            expires = expires.replace(second=expires.second)  # keep deterministic ISO output
            expires_text = (expires.timestamp() + 3600)
            lease_expiry = datetime.fromtimestamp(expires_text, UTC).isoformat()
            queue = queue.with_columns(
                pl.when(pl.col("queue_item_id") == item_id).then(pl.lit("RUNNING")).otherwise(pl.col("status")).alias("status"),
                pl.when(pl.col("queue_item_id") == item_id).then(pl.col("attempt_count") + 1).otherwise(pl.col("attempt_count")).alias("attempt_count"),
                pl.when(pl.col("queue_item_id") == item_id).then(pl.lit(acquired)).otherwise(pl.col("last_attempt_at")).alias("last_attempt_at"),
                pl.when(pl.col("queue_item_id") == item_id).then(pl.lit(run_id)).otherwise(pl.col("lease_owner")).alias("lease_owner"),
                pl.when(pl.col("queue_item_id") == item_id).then(pl.lit(acquired)).otherwise(pl.col("lease_acquired_at")).alias("lease_acquired_at"),
                pl.when(pl.col("queue_item_id") == item_id).then(pl.lit(lease_expiry)).otherwise(pl.col("lease_expires_at")).alias("lease_expires_at"),
                pl.when(pl.col("queue_item_id") == item_id).then(pl.lit(_now())).otherwise(pl.col("updated_at")).alias("updated_at"),
            )
            state.update({
                "current_queue_item": item_id,
                "current_city": str(row["city_name"]),
                "current_source": str(row["source_id"]),
                "current_source_role": str(row["source_role"]),
                "current_window": {"start": str(row["window_start"]), "end": str(row["window_end"])},
                "processed_items": processed,
                "updated_at": _now(),
            })
            _write_queue(settings, queue)
            _atomic_json(_state_path(settings), state)
            _write_progress_snapshot(settings, queue, state)
            try:
                crawler = ExhaustiveCrawler(settings)
                result = crawler.run_city(
                    str(row["city_id"]),
                    start_date=start,
                    end_date=end,
                    source_roles=[str(row["source_role"])],
                    source_ids=[str(row["source_id"])],
                    max_pages_per_source=config.max_pages_per_source,
                    max_candidates_per_shard=config.max_candidates_per_shard,
                    max_fetches_per_shard=config.max_fetches_per_shard,
                    resume=config.resume,
                )
                run_ids = [str(value) for value in result.get("run_ids", [])]
                version_ids = _run_version_ids(settings, run_ids)
                promotion = promote_document_versions(settings, run_ids=run_ids, apply=True)
                relevance = audit_recent_relevance(settings, run_ids=run_ids, apply=True)
                for crawl_run in run_ids:
                    archive_document_versions(settings, run_id=crawl_run)
                    materialize_policy_identity(settings, run_id=crawl_run)
                build_database(settings, materialize_geography=False)
                try:
                    pdf_discovery = PDFPipeline(settings, config=load_pdf_config(settings)).discover(
                        limit=config.pdf_discovery_limit,
                        city_id=str(row["city_id"]),
                        source_id=str(row["source_id"]),
                        run_id=run_ids[-1] if run_ids else None,
                    )
                except Exception as pdf_exc:  # noqa: BLE001 - PDF is a side queue
                    pdf_discovery = {"discovered": 0, "error": f"{type(pdf_exc).__name__}: {str(pdf_exc)[:300]}"}
                documents = int(sum(result.get("run_metrics", {}).get(crawl_run, {}).get("fetched", 0) for crawl_run in run_ids))
                failed = int(sum(result.get("run_metrics", {}).get(crawl_run, {}).get("failed", 0) for crawl_run in run_ids))
                pdf_found, pdf_archived, _ = _pdf_counts(settings, version_ids)
                if documents:
                    status = "POLICY_FOUND"
                elif failed:
                    status = "PARTIAL_NETWORK" if int(row.get("attempt_count") or 0) < config.max_attempts else "FAILED"
                elif result.get("processed_shards"):
                    status = "CONFIRMED_ZERO"
                else:
                    status = "SOURCE_INCOMPLETE"
                failure_reason = None if status in {"POLICY_FOUND", "CONFIRMED_ZERO"} else f"failed_fetches={failed}" if failed else "NO_PROCESSED_SHARD"
                update = {
                    "status": status,
                    "documents_found": documents,
                    "versions_created": len(version_ids),
                    "records_promoted": int(promotion.get("promoted_records", 0)),
                    "pdfs_found": pdf_found,
                    "pdfs_archived": pdf_archived,
                    "pdf_discovery_count": int(pdf_discovery.get("discovered", 0) or 0),
                    "coverage_status": status,
                    "failure_reason": failure_reason,
                    "completed_at": _now() if status in COMPLETE_STATUSES else None,
                    "lease_owner": None,
                    "lease_acquired_at": None,
                    "lease_expires_at": None,
                    "run_ids_json": json.dumps(run_ids, ensure_ascii=False),
                    "updated_at": _now(),
                }
                for column, value in update.items():
                    queue = queue.with_columns(pl.when(pl.col("queue_item_id") == item_id).then(pl.lit(value)).otherwise(pl.col(column)).alias(column))
                state.update({"last_real_progress_at": _now(), "last_event": "FORMAL_RECORDS_PROMOTED", "relevance_rejected": int(relevance.get("rejected_versions", 0))})
            except Exception as exc:  # noqa: BLE001 - queue state is the recovery contract
                text = f"{type(exc).__name__}: {str(exc)[:500]}"
                blocking = text if isinstance(exc, (OSError, ParquetStoreError)) or any(term in text.lower() for term in ("duckdb", "database", "schema", "checkpoint", "write conflict")) else blocking
                status = "FAILED" if int(row.get("attempt_count") or 0) >= config.max_attempts else "RETRY_WAIT"
                update = {
                    "status": status,
                    "coverage_status": "FAILED" if status == "FAILED" else "PARTIAL_NETWORK",
                    "failure_reason": text,
                    "completed_at": _now() if status == "FAILED" else None,
                    "lease_owner": None,
                    "lease_acquired_at": None,
                    "lease_expires_at": None,
                    "updated_at": _now(),
                }
                for column, value in update.items():
                    queue = queue.with_columns(pl.when(pl.col("queue_item_id") == item_id).then(pl.lit(value)).otherwise(pl.col(column)).alias(column))
            processed += 1
            _write_queue(settings, queue)
            state.update({
                "processed_items": processed,
                "current_queue_item": None,
                "current_city": None,
                "current_source": None,
                "current_source_role": None,
                "batch_status": "COMPLETED",
                "last_real_progress_at": _now(),
                "updated_at": _now(),
            })
            _atomic_json(_state_path(settings), state)
            _write_progress_snapshot(settings, queue, state)
            if _stop_requested(settings):
                stop_seen = True
                break
            if blocking:
                state.update({"status": "BLOCKED", "stage_status": "BLOCKED", "blocking_reason": blocking})
                break
        counts = _counts(queue)
        complete = counts["pending"] == 0 and counts["running"] == 0 and counts["retryable"] == 0
        state.update({
            "status": "PAUSED_BUDGET" if stop_seen else ("BLOCKED" if blocking else ("COMPLETE" if complete else "PARTIAL")),
            "stage_status": "PAUSED" if stop_seen else ("BLOCKED" if blocking else ("COMPLETE" if complete else "RUNNING")),
            "batch_status": "STOP_REQUESTED" if stop_seen else ("BLOCKED" if blocking else "COMPLETED"),
            "last_event": "STOP_REQUESTED" if stop_seen else state.get("last_event"),
            "current_queue_item": None,
            "current_city": None,
            "current_source": None,
            "current_source_role": None,
            "updated_at": _now(),
        })
        if blocking:
            state["blocking_reason"] = blocking
        summary = _summary(settings, queue, state)
        summary.update({"run_id": run_id, "processed_items": processed, "blocking_error": blocking, "exit_code": 1 if blocking else 0})
        _atomic_json(_root(settings) / f"ROLLING_24M_RUN_SUMMARY_{run_id}.json", summary)
        return summary


__all__ = [
    "COMPLETE_STATUSES",
    "RETRYABLE_STATUSES",
    "Rolling24MConfig",
    "build_rolling_queue",
    "rolling_audit",
    "rolling_window",
    "run_rolling_24m",
    "target_months",
]
