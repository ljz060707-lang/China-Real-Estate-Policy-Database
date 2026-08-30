"""Unified, read-only Dashboard snapshot over CRPD runtime artifacts.

The crawler writes immutable Parquet snapshots and atomic JSON state.  This
module reads those products without owning a writer lock or a long DuckDB
transaction.  Live crawl progress and strict completeness are intentionally
separate concepts.
"""

from __future__ import annotations

import json
import re
import time
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import UTC, datetime
from functools import lru_cache
from pathlib import Path
from typing import Any

import duckdb
import polars as pl
import psutil

from policydb.dashboard_formatting import parse_datetime
from policydb.dashboard_logging import log_dashboard_exception
from policydb.parquet_store import read_parquet_snapshot
from policydb.settings import Settings

TERMINAL_SHARD_STATUSES = {
    "complete_policy_found",
    "complete_unverified",
    "certified_complete",
    "confirmed_zero",
    "partial_cap",
    "partial_network",
    "partial_parser",
    "partial_archive",
    "partial_temporal",
    "source_incomplete",
    "failed",
}
AUTOMATION_CRAWL_STAGES = {
    "CRAWL",
    "CRAWL_AGAIN",
    "HISTORICAL_CRAWL_AGAIN",
    "RECOVER_MISSING",
    "ROLLING_24M_FULL_CITY_BACKFILL",
    "ROLLING_24M_RECOVER_MISSING",
    "ROLLING_24M_SECOND_PASS",
}
AUTOMATION_STATUS_MAP = {
    "READY": "READY_FOR_NEXT_STAGE",
    "WORKER_STARTED": "RUNNING",
    "WORKER_ACTIVE": "RUNNING",
    "RUNNING": "RUNNING",
    "WAIT_CURRENT_RUN": "WAIT_CURRENT_RUN",
    "RETRY_WAIT": "RETRY_WAIT",
    "BLOCKED": "BLOCKED",
    "COMPLETE": "COMPLETE",
    "READY_FOR_NEXT_STAGE": "READY_FOR_NEXT_STAGE",
}


@dataclass(frozen=True)
class ProgressMetric:
    label: str
    value: float | int | None
    numerator: int | float | None
    denominator: int | float | None
    status: str
    source: str
    updated_at: str | None
    definition: str


@dataclass
class DashboardSnapshot:
    generated_at: str
    data_root: str
    database_path: str
    system: dict[str, Any] = field(default_factory=dict)
    crawler: dict[str, Any] = field(default_factory=dict)
    coverage: dict[str, Any] = field(default_factory=dict)
    documents: dict[str, Any] = field(default_factory=dict)
    quality: dict[str, Any] = field(default_factory=dict)
    ai: dict[str, Any] = field(default_factory=dict)
    review: dict[str, Any] = field(default_factory=dict)
    archive: dict[str, Any] = field(default_factory=dict)
    frames: dict[str, pl.DataFrame] = field(default_factory=dict)
    availability: dict[str, dict[str, Any]] = field(default_factory=dict)


_LAST_GOOD_SNAPSHOTS: dict[str, DashboardSnapshot] = {}
_READ_RETRY_DELAYS = (0.0, 0.2, 0.4)


def _file_stamp(path: Path) -> tuple[int, int]:
    try:
        stat = path.stat()
        return stat.st_mtime_ns, stat.st_size
    except OSError:
        return 0, 0


@lru_cache(maxsize=128)
def _cached_parquet(
    path_text: str,
    mtime_ns: int,
    size: int,
    columns: tuple[str, ...] | None,
) -> pl.DataFrame:
    del mtime_ns, size
    path = Path(path_text)
    if columns:
        schema = pl.read_parquet_schema(path)
        selected = [column for column in columns if column in schema]
        return read_parquet_snapshot(path, columns=selected)
    return read_parquet_snapshot(path)


def _read_frame(
    settings: Settings,
    name: str,
    *,
    columns: tuple[str, ...] | None = None,
) -> tuple[pl.DataFrame, dict[str, Any]]:
    path = settings.curated / f"{name}.parquet"
    stamp, size = _file_stamp(path)
    meta = {
        "path": str(path),
        "exists": bool(stamp),
        "mtime_ns": stamp or None,
        "updated_at": (
            datetime.fromtimestamp(stamp / 1_000_000_000, tz=UTC).isoformat() if stamp else None
        ),
        "status": "available" if stamp else "unavailable",
        "error_type": None,
    }
    if not stamp:
        return pl.DataFrame(), meta
    last_error: BaseException | None = None
    for attempt, delay in enumerate(_READ_RETRY_DELAYS):
        if delay:
            time.sleep(delay)
        try:
            frame = _cached_parquet(str(path), stamp, size, columns).clone()
            return frame, meta
        except Exception as exc:
            last_error = exc
            if attempt == len(_READ_RETRY_DELAYS) - 1:
                break
    assert last_error is not None
    meta.update(status="unavailable", error_type=type(last_error).__name__)
    log_dashboard_exception(
        settings,
        "Dashboard Parquet snapshot read failed after retries",
        component="dashboard_snapshot",
        operation="read_frame",
        data_source=str(path),
        relation=name,
        query=f"read_parquet({name}.parquet)",
        error=last_error,
    )
    return pl.DataFrame(), meta


@lru_cache(maxsize=64)
def _cached_json(path_text: str, mtime_ns: int, size: int) -> dict[str, Any]:
    del mtime_ns, size
    value = json.loads(Path(path_text).read_text(encoding="utf-8-sig"))
    if not isinstance(value, dict):
        raise ValueError("Dashboard state JSON must contain an object")
    return value


def _read_json(path: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    stamp, size = _file_stamp(path)
    meta = {
        "path": str(path),
        "exists": bool(stamp),
        "mtime_ns": stamp or None,
        "updated_at": (
            datetime.fromtimestamp(stamp / 1_000_000_000, tz=UTC).isoformat() if stamp else None
        ),
        "status": "available" if stamp else "unavailable",
        "error_type": None,
    }
    if not stamp:
        return {}, meta
    try:
        return dict(_cached_json(str(path), stamp, size)), meta
    except (OSError, json.JSONDecodeError, ValueError) as exc:
        meta.update(status="unavailable", error_type=type(exc).__name__)
        return {}, meta


def _episode_930_progress(settings: Settings) -> tuple[dict[str, Any], dict[str, Any]]:
    """Read the atomic 930 production snapshot as a read-only dashboard input."""

    output = settings.outputs / "special_projects" / "2016_930"
    progress, meta = _read_json(output / "930_PROGRESS_SNAPSHOT.json")
    autorun, autorun_meta = _read_json(output / "930_AUTORUN_STATE.json")
    provider_status, provider_meta = _read_json(output / "930_API_PROVIDER_STATUS.json")
    recovery_queue_path = output / "930_API_RECOVERY_QUEUE.parquet"
    recovery_queue = pl.DataFrame()
    try:
        if recovery_queue_path.exists():
            recovery_queue = read_parquet_snapshot(recovery_queue_path)
    except (OSError, pl.exceptions.PolarsError, ValueError):
        recovery_queue = pl.DataFrame()
    if progress:
        writer_lock = settings.logs / "policydb-write.lock"
        autorun_lock = output / "930_AUTORUN.lock"
        current_stage = str(progress.get("stage") or "")
        progress["autorun"] = {
            "lock_present": autorun_lock.exists(),
            "status": autorun.get("status"),
            "runner_pid": autorun.get("runner_pid"),
            "current_job_id": autorun.get("current_job_id"),
            "current_job_status": autorun.get("current_job_status"),
            "active_worker": bool(autorun.get("current_job_id") and autorun.get("current_job_status") not in {None, "completed", "completed_with_warnings", "failed", "cancelled"}),
            "active_fetch": current_stage in {"930_DISCOVERY", "930_OFFICIAL_RECOVERY"},
            "active_writer": writer_lock.exists(),
            "last_real_progress_at": progress.get("last_real_progress_at"),
            "heartbeat_at": progress.get("heartbeat_at"),
        }
        if provider_status:
            progress["api_status"] = provider_status.get("status") or progress.get("api_status")
            progress["api_provider_status"] = provider_status.get("status")
            progress["api_balance_status"] = provider_status.get("api_balance_status")
            progress["api_provider_updated_at"] = provider_status.get("updated_at")
        progress["api_recovery_queue"] = {
            "total": recovery_queue.height,
            "pending": recovery_queue.height,
            "provider_recovery_pending": (
                recovery_queue.filter(
                    pl.col("recovery_status") == "PENDING_PROVIDER_RECOVERY"
                ).height
                if not recovery_queue.is_empty() and "recovery_status" in recovery_queue.columns
                else 0
            ),
        }
        progress["autorun_state_meta"] = autorun_meta
    meta["autorun_state"] = autorun_meta
    meta["provider_status"] = provider_meta
    return progress, meta


_REPRESENTATIVE_RELATIONS = (
    "records",
    "policy_document_versions",
    "crawl_items",
    "source_sync_state",
)


def _database_error_status(error: BaseException) -> str:
    message = str(error).lower()
    if any(
        marker in message
        for marker in (
            "no files found that match the pattern",
            "does not exist",
            "catalog error",
            "table with name",
        )
    ):
        return "INDEX_REFRESH_PENDING"
    if any(
        marker in message
        for marker in (
            "lock",
            "busy",
            "being used",
            "transaction conflict",
            "file is being modified",
        )
    ):
        return "DATABASE_UPDATING"
    return "QUERY_UNAVAILABLE"


def _curated_fallback_info(curated_path: Path) -> dict[str, Any]:
    records_path = curated_path / "records.parquet"
    result = {
        "available": False,
        "path": str(curated_path),
        "records_path": str(records_path),
        "status": "unavailable",
        "error_type": None,
    }
    if not records_path.is_file():
        return result
    try:
        schema = pl.read_parquet_schema(records_path)
        if "record_id" not in schema:
            result.update(status="unavailable", error_type="missing_record_id")
            return result
        result.update(available=True, status="available")
    except (OSError, pl.exceptions.PolarsError, ValueError) as exc:
        result.update(error_type=type(exc).__name__)
    return result


@lru_cache(maxsize=8)
def _database_health(
    path_text: str,
    mtime_ns: int,
    size: int,
    curated_path_text: str,
    curated_mtime_ns: int,
    curated_size: int,
) -> dict[str, Any]:
    del mtime_ns, size, curated_mtime_ns, curated_size
    path = Path(path_text)
    curated = _curated_fallback_info(Path(curated_path_text))
    file_state = {
        "path": str(path),
        "exists": path.is_file(),
        "status": "available" if path.is_file() else "missing",
    }
    result: dict[str, Any] = {
        "status": "QUERY_UNAVAILABLE",
        "formal_status": "QUERY_UNAVAILABLE",
        "queryable": False,
        "formal_queryable": False,
        "mode": "unavailable",
        "reason": "database_missing" if not path.is_file() else None,
        "file": file_state,
        "connect": {"status": "not_attempted", "ok": False, "error_type": None, "error": None},
        "representative_query": {
            "status": "not_attempted",
            "ok": False,
            "relations": {},
            "errors": [],
        },
        "representative_queries": {},
        "curated_fallback": curated,
        "fallback_available": bool(curated["available"]),
    }
    if not path.is_file():
        result["formal_status"] = "QUERY_UNAVAILABLE"
        if curated["available"]:
            result.update(status="CURATED_FALLBACK", mode="curated_fallback")
        return result

    try:
        connection = duckdb.connect(str(path), read_only=True)
    except (duckdb.Error, OSError) as exc:
        formal_status = _database_error_status(exc)
        result.update(
            formal_status=formal_status,
            reason=type(exc).__name__,
            connect={
                "status": "failed",
                "ok": False,
                "error_type": type(exc).__name__,
                "error": str(exc),
            },
        )
        if curated["available"]:
            result.update(status="CURATED_FALLBACK", mode="curated_fallback")
        else:
            result["status"] = formal_status
        return result

    result["connect"] = {"status": "connected", "ok": True, "error_type": None, "error": None}
    queries: dict[str, dict[str, Any]] = {}
    errors: list[dict[str, Any]] = []
    try:
        for relation in _REPRESENTATIVE_RELATIONS:
            sql = f"SELECT count(*) FROM {relation}"
            try:
                count = int(connection.execute(sql).fetchone()[0])
                queries[relation] = {
                    "sql": sql,
                    "ok": True,
                    "count": count,
                    "error_type": None,
                    "error": None,
                }
            except (duckdb.Error, OSError) as exc:
                entry = {
                    "sql": sql,
                    "ok": False,
                    "count": None,
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                }
                queries[relation] = entry
                errors.append({"relation": relation, **entry})
    finally:
        connection.close()

    query_ok = not errors and len(queries) == len(_REPRESENTATIVE_RELATIONS)
    result["representative_queries"] = queries
    result["representative_query"] = {
        "status": "passed" if query_ok else "failed",
        "ok": query_ok,
        "relations": queries,
        "errors": errors,
    }
    if query_ok:
        result.update(
            status="HEALTHY",
            formal_status="HEALTHY",
            queryable=True,
            formal_queryable=True,
            mode="duckdb",
            reason=None,
        )
        return result

    first_error = errors[0] if errors else None
    formal_status = _database_error_status(Exception(first_error["error"])) if first_error else "QUERY_UNAVAILABLE"
    result.update(
        status=formal_status,
        formal_status=formal_status,
        queryable=False,
        formal_queryable=False,
        reason=first_error.get("error_type") if first_error else "representative_query_failed",
    )
    if curated["available"]:
        result.update(status="CURATED_FALLBACK", mode="curated_fallback")
    return result


def database_health(settings: Settings) -> dict[str, Any]:
    stamp, size = _file_stamp(settings.database)
    curated_stamp, curated_size = _file_stamp(settings.curated / "records.parquet")
    health: dict[str, Any] = {}
    for _attempt, delay in enumerate(_READ_RETRY_DELAYS):
        if delay:
            time.sleep(delay)
        health = dict(
            _database_health(
                str(settings.database),
                stamp,
                size,
                str(settings.curated),
                curated_stamp,
                curated_size,
            )
        )
        if health.get("status") == "HEALTHY":
            break
        # Failure results must not remain cached across a transient index swap
        # or a recovered read-only connection.
        _database_health.cache_clear()
    health.update(
        path=str(settings.database),
        exists=settings.database.exists(),
        updated_at=(
            datetime.fromtimestamp(stamp / 1_000_000_000, tz=UTC).isoformat() if stamp else None
        ),
    )
    return health


@lru_cache(maxsize=8)
def _cached_backfill_log(path_text: str, mtime_ns: int, size: int) -> dict[str, Any]:
    del mtime_ns
    path = Path(path_text)
    with path.open("rb") as stream:
        stream.seek(max(0, size - 256 * 1024))
        text = stream.read().decode("utf-8", errors="replace")
    matches = list(re.finditer(r"\[(\d+)/(\d+)\].*?(开始|城市阶段结束)", text))
    if not matches:
        return {}
    latest = matches[-1]
    index, total = int(latest.group(1)), int(latest.group(2))
    event = latest.group(3)
    return {
        "city_index": index,
        "city_total": total,
        "completed_cities": index if event == "城市阶段结束" else max(0, index - 1),
        "event": event,
        "path": str(path),
    }


def _backfill_log_progress(settings: Settings) -> dict[str, Any]:
    root = settings.logs / "audited_full_backfill"
    if not root.exists():
        return {}
    candidates = [path / "master.log" for path in root.iterdir() if path.is_dir()]
    candidates = [path for path in candidates if path.exists()]
    if not candidates:
        return {}
    latest = max(candidates, key=lambda path: path.stat().st_mtime_ns)
    stamp, size = _file_stamp(latest)
    try:
        result = dict(_cached_backfill_log(str(latest), stamp, size))
    except OSError:
        return {}
    result["updated_at"] = datetime.fromtimestamp(stamp / 1_000_000_000, tz=UTC).isoformat()
    return result


def _process_inventory(project_root: Path) -> dict[str, Any]:
    crawlers: list[dict[str, Any]] = []
    dashboards: list[dict[str, Any]] = []
    runner_pid: int | None = None
    project_marker = str(project_root).lower()
    for process in psutil.process_iter(["pid", "ppid", "name", "create_time", "cmdline"]):
        try:
            command = " ".join(process.info.get("cmdline") or [])
            lowered = command.lower()
            if project_marker not in lowered:
                continue
            item = {
                "pid": int(process.info["pid"]),
                "parent_pid": int(process.info.get("ppid") or 0),
                "started_at": datetime.fromtimestamp(
                    float(process.info.get("create_time") or 0), tz=UTC
                ).isoformat(),
            }
            if "crpd_audited_full_backfill.ps1" in lowered:
                runner_pid = item["pid"]
            elif "crawl exhaustive-city" in lowered or "crawl exhaustive-resume" in lowered:
                crawlers.append(item)
            elif "streamlit run" in lowered and "app\\dashboard.py" in lowered:
                dashboards.append(item)
        except (OSError, psutil.Error, ValueError, TypeError):
            continue
    return {
        "runner_pid": runner_pid,
        "crawler_processes": sorted(crawlers, key=lambda row: row["pid"]),
        "dashboard_processes": sorted(dashboards, key=lambda row: row["pid"]),
    }


def _count(frame: pl.DataFrame, expression: pl.Expr) -> int:
    if frame.is_empty():
        return 0
    try:
        value = frame.select(expression.sum()).item()
        return int(value or 0)
    except (pl.exceptions.PolarsError, TypeError, ValueError):
        return 0


def _nonempty(frame: pl.DataFrame, column: str) -> pl.Expr:
    return pl.col(column).is_not_null() & (pl.col(column).cast(pl.String).str.strip_chars() != "")


def _latest_event(events: pl.DataFrame, shards: pl.DataFrame) -> dict[str, Any]:
    if events.is_empty() or "created_at" not in events.columns:
        return {}
    row = events.sort("created_at").tail(1).row(0, named=True)
    shard: dict[str, Any] = {}
    if not shards.is_empty() and row.get("shard_id") and "shard_id" in shards.columns:
        matched = shards.filter(pl.col("shard_id") == row["shard_id"])
        if matched.height:
            shard = matched.tail(1).row(0, named=True)
    return {**row, **{f"shard_{key}": value for key, value in shard.items()}}


def _event_window(events: pl.DataFrame, shards: pl.DataFrame, limit: int = 20) -> pl.DataFrame:
    if events.is_empty():
        return pl.DataFrame()
    selected = events.sort("created_at", descending=True).head(limit)
    if shards.is_empty() or "shard_id" not in selected.columns:
        return selected
    shard_columns = [
        name
        for name in ("shard_id", "city_id", "city_name", "source_role", "start_date", "end_date")
        if name in shards.columns
    ]
    return selected.join(
        shards.select(shard_columns).unique(subset=["shard_id"], keep="last"),
        on="shard_id",
        how="left",
    )


def _dashboard_runtime_status(master: dict[str, Any]) -> str:
    raw = str(master.get("status") or "UNKNOWN")
    if bool(master.get("current_run_active")):
        return "WAIT_CURRENT_RUN"
    return AUTOMATION_STATUS_MAP.get(raw, raw)


def _automation_live_state(
    master: dict[str, Any],
    latest: dict[str, Any],
    *,
    now: datetime,
    progress_snapshot: dict[str, Any] | None = None,
    event_fresh_seconds: int = 180,
) -> dict[str, Any]:
    """Derive current position only when the authoritative state permits it."""

    stage = str(master.get("stage") or "UNKNOWN")
    master_heartbeat = parse_datetime(master.get("last_heartbeat_at"))
    latest_event_at = parse_datetime(latest.get("created_at"))
    event_age = (
        (now - latest_event_at.astimezone(UTC)).total_seconds()
        if latest_event_at
        else None
    )
    event_fresh = event_age is not None and event_age <= event_fresh_seconds
    progress_snapshot = progress_snapshot if isinstance(progress_snapshot, dict) else {}
    progress_stage = str(progress_snapshot.get("stage") or "")
    rolling_position_available = stage.startswith("ROLLING_24M") and progress_stage == stage
    current_position_available = stage in AUTOMATION_CRAWL_STAGES and (event_fresh or rolling_position_available)
    position = latest if event_fresh else {}
    if rolling_position_available:
        position = {
            "city_name": progress_snapshot.get("current_city"),
            "source_id": progress_snapshot.get("current_source"),
            "start_date": (progress_snapshot.get("current_window") or {}).get("rolling_start"),
            "end_date": (progress_snapshot.get("current_window") or {}).get("rolling_end"),
            "message": "rolling 24-month source session",
        }
    heartbeat_age = (
        (now - master_heartbeat.astimezone(UTC)).total_seconds()
        if master_heartbeat
        else None
    )
    heartbeat_status = (
        "fresh"
        if heartbeat_age is not None and heartbeat_age <= event_fresh_seconds
        else "stale"
        if heartbeat_age is not None
        else "unavailable"
    )
    last_position = {
        "batch_id": latest.get("batch_id"),
        "shard_id": latest.get("shard_id"),
        "city_id": latest.get("shard_city_id"),
        "city_name": latest.get("shard_city_name"),
        "source_role": latest.get("shard_source_role"),
        "source_id": latest.get("shard_source_id"),
        "start_date": latest.get("shard_start_date"),
        "end_date": latest.get("shard_end_date"),
        "stage": latest.get("stage"),
        "message": latest.get("message"),
        "event_at": latest.get("created_at"),
        "fresh": event_fresh,
    }
    return {
        "status": _dashboard_runtime_status(master),
        "raw_status": master.get("status"),
        "stage": stage,
        "run_id": master.get("run_id"),
        "automation_id": master.get("automation_id"),
        "worker_pid": master.get("worker_pid"),
        "next_stage": master.get("next_stage"),
        "last_heartbeat_at": master.get("last_heartbeat_at"),
        "heartbeat_age_seconds": heartbeat_age,
        "heartbeat_status": heartbeat_status,
        "event_fresh": event_fresh,
        "current_position_available": current_position_available,
        "current_position": position,
        "last_crawl_position": last_position,
        "progress_snapshot": progress_snapshot,
    }


def _metric(
    label: str,
    numerator: int | float | None,
    denominator: int | float | None,
    *,
    source: str,
    updated_at: str | None,
    definition: str,
    status: str = "available",
) -> ProgressMetric:
    value = None
    if numerator is not None and denominator not in (None, 0):
        value = float(numerator) / float(denominator)
    return ProgressMetric(
        label=label,
        value=value,
        numerator=numerator,
        denominator=denominator,
        status=status,
        source=source,
        updated_at=updated_at,
        definition=definition,
    )


def _city_progress(shards: pl.DataFrame, city_total: int) -> tuple[int, int, pl.DataFrame]:
    if shards.is_empty() or "city_id" not in shards.columns:
        return 0, city_total, pl.DataFrame()
    rows: list[dict[str, Any]] = []
    shards = shards.filter(pl.col("city_id").is_not_null() & pl.col("city_name").is_not_null())
    for key, group in shards.group_by(["city_id", "city_name"]):
        city_id, city_name = key
        runnable = group.filter(pl.col("status") != "split_parent")
        terminal = _count(runnable, pl.col("status").is_in(TERMINAL_SHARD_STATUSES))
        planned = runnable.height
        rows.append(
            {
                "city_id": city_id,
                "city_name": city_name,
                "processed_shards": terminal,
                "planned_shards": planned,
                "city_status": "completed" if planned and terminal == planned else "running",
            }
        )
    frame = pl.DataFrame(rows, infer_schema_length=None).sort(["city_status", "city_name"])
    completed = _count(frame, pl.col("city_status") == "completed")
    return completed, city_total, frame


def _document_quality(records: pl.DataFrame) -> dict[str, int]:
    if records.is_empty():
        return {
            "total": 0,
            "missing_title": 0,
            "missing_date": 0,
            "missing_text": 0,
            "short_text": 0,
            "missing_source": 0,
            "duplicate_url": 0,
            "duplicate_hash": 0,
        }
    total = records.height
    result = {"total": total}
    for key, column in (
        ("missing_title", "title"),
        ("missing_date", "record_date"),
        ("missing_text", "full_text"),
        ("missing_source", "primary_source_url"),
    ):
        result[key] = (
            total - _count(records, _nonempty(records, column))
            if column in records.columns
            else total
        )
    result["short_text"] = (
        _count(records, pl.col("full_text").fill_null("").str.len_chars() < 100)
        if "full_text" in records.columns
        else total
    )
    for key, column in (
        ("duplicate_url", "primary_source_url"),
        ("duplicate_hash", "content_hash"),
    ):
        if column not in records.columns:
            result[key] = 0
            continue
        valid = records.filter(_nonempty(records, column))
        result[key] = max(0, valid.height - valid.get_column(column).n_unique())
    return result


def _available_frame_count(
    frames: dict[str, pl.DataFrame],
    availability: dict[str, dict[str, Any]],
    name: str,
) -> int | None:
    """Return a count only when the corresponding snapshot was read successfully."""

    if (availability.get(name) or {}).get("status") != "available":
        return None
    return frames.get(name, pl.DataFrame()).height


def _build_dashboard_snapshot(
    settings: Settings | None = None,
    *,
    event_limit: int = 20,
) -> DashboardSnapshot:
    settings = settings or Settings.discover()
    now = datetime.now(UTC)
    snapshot = DashboardSnapshot(
        generated_at=now.isoformat(),
        data_root=str(settings.data_root),
        database_path=str(settings.database),
    )

    frames: dict[str, pl.DataFrame] = {}
    availability: dict[str, dict[str, Any]] = {}
    requested = {
        "pipeline_progress_events": None,
        "crawl_shards": None,
        "source_requirement_slots": None,
        "source_slot_progress": None,
        "city_year_progress": None,
        "city_source_year_progress": None,
        "records": (
            "record_id",
            "record_date",
            "title",
            "full_text",
            "primary_source_url",
            "content_hash",
            "official_status",
            "manual_review_status",
            "updated_at",
        ),
        "record_geographies_normalized": (
            "record_id",
            "city_id",
            "city_name",
            "province_name",
        ),
        "policy_document_versions": (
            "document_version_id",
            "record_id",
            "source_id",
            "http_status",
            "parse_status",
            "content_type",
            "created_at",
            "updated_at",
        ),
        "crawl_items": (
            "item_id",
            "run_id",
            "source_id",
            "city_id",
            "query_year",
            "status",
            "updated_at",
            "last_checked_at",
        ),
        "crawl_runs": None,
        "source_registry": None,
        "source_sync_state": None,
        "source_candidates": (
            "candidate_id",
            "slot_id",
            "city_id",
            "source_role",
            "candidate_url",
            "is_official",
            "is_verified",
            "is_enabled",
            "manual_review_status",
            "overall_confidence",
            "verification_failed_gates",
            "updated_at",
        ),
        "coverage_gaps": None,
        "pdf_assets": None,
        "pdf_processing_events": None,
        "llm_extractions": None,
        "llm_verifications": None,
    }
    for name, columns in requested.items():
        frame, meta = _read_frame(settings, name, columns=columns)
        frames[name] = frame
        availability[name] = meta

    automation: dict[str, dict[str, Any]] = {}
    for name in (
        "MASTER_STATE",
        "CURRENT_RUN",
        "COVERAGE_STATE",
        "AI_QUEUE_STATE",
        "PDF_ARCHIVE_STATE",
        "PROGRESS_SNAPSHOT",
        "PROGRESS_WATCHDOG",
    ):
        value, meta = _read_json(settings.data_root / "automation" / f"{name}.json")
        automation[name] = value
        availability[f"automation/{name}"] = meta
    episode_930, episode_930_meta = _episode_930_progress(settings)
    availability["special_projects/2016_930/930_PROGRESS_SNAPSHOT"] = episode_930_meta

    events = frames["pipeline_progress_events"]
    shards = frames["crawl_shards"]
    slots = frames["source_requirement_slots"]
    latest = _latest_event(events, shards)
    recent_events = _event_window(events, shards, event_limit)
    event_updated_at = availability["pipeline_progress_events"].get("updated_at")

    process_state = _process_inventory(settings.root)
    database = database_health(settings)
    log_progress = _backfill_log_progress(settings)
    master = automation["MASTER_STATE"]
    live_state = _automation_live_state(
        master,
        latest,
        now=now,
        progress_snapshot=automation["PROGRESS_SNAPSHOT"],
    )
    current_position = live_state["current_position"]
    crawler_processes = process_state["crawler_processes"]
    crawler_running = bool(crawler_processes) or live_state["status"] == "RUNNING"
    registry = frames["source_registry"]
    current_source_name = None
    if (
        current_position.get("shard_source_id")
        and not registry.is_empty()
        and {"source_id", "source_name"}.issubset(registry.columns)
    ):
        matched_source = registry.filter(
            pl.col("source_id") == current_position["shard_source_id"]
        )
        if matched_source.height:
            current_source_name = matched_source.tail(1)[0, "source_name"]

    current_batch = str(current_position.get("batch_id") or "")
    batch_shards = (
        shards.filter(pl.col("batch_id") == current_batch)
        if current_batch and not shards.is_empty() and "batch_id" in shards.columns
        else pl.DataFrame()
    )
    processed_batch = _count(
        batch_shards,
        pl.col("status").is_in(TERMINAL_SHARD_STATUSES),
    )
    planned_batch = (
        batch_shards.filter(pl.col("status") != "split_parent").height
        if not batch_shards.is_empty()
        else None
    )

    city_total = slots.get_column("city_id").n_unique() if not slots.is_empty() else 105
    completed_cities, city_denominator, city_frame = _city_progress(shards, int(city_total or 105))
    if log_progress.get("city_total"):
        completed_cities = int(log_progress["completed_cities"])
        city_denominator = int(log_progress["city_total"])

    snapshot.system = {
        "database": database,
        "processes": process_state,
        "automation": automation,
        "automation_status": live_state["status"],
        "automation_raw_status": live_state["raw_status"],
        "automation_stage": live_state["stage"],
        "automation_id": master.get("automation_id"),
        "automation_heartbeat_at": master.get("last_heartbeat_at"),
        "automation_live_state": live_state,
        "disk": master.get("disk") or {},
        "data_root_exists": settings.data_root.exists(),
        "curated_root_exists": settings.curated.exists(),
        "crawler_running": crawler_running,
        "backfill_log_progress": log_progress,
        "progress_snapshot": automation["PROGRESS_SNAPSHOT"],
        "progress_watchdog": automation["PROGRESS_WATCHDOG"],
        "episode_930_progress": episode_930,
    }
    snapshot.crawler = {
        "running": crawler_running,
        "status": live_state["status"],
        "raw_status": live_state["raw_status"],
        "stage": live_state["stage"],
        "run_id": live_state["run_id"],
        "automation_id": live_state["automation_id"],
        "batch_id": current_position.get("batch_id"),
        "shard_id": current_position.get("shard_id"),
        "city_id": current_position.get("shard_city_id"),
        "city_name": current_position.get("shard_city_name"),
        "source_role": current_position.get("shard_source_role"),
        "source_id": current_position.get("shard_source_id"),
        "source_name": current_source_name,
        "start_date": current_position.get("shard_start_date"),
        "end_date": current_position.get("shard_end_date"),
        "message": current_position.get("message"),
        "latest_event_at": latest.get("created_at"),
        "last_heartbeat_at": live_state["last_heartbeat_at"],
        "heartbeat_age_seconds": live_state["heartbeat_age_seconds"],
        "heartbeat_status": live_state["heartbeat_status"],
        "event_fresh": live_state["event_fresh"],
        "current_position_available": live_state["current_position_available"],
        "worker_pid": live_state["worker_pid"]
        or (crawler_processes[-1]["pid"] if crawler_processes else None),
        "runner_pid": process_state["runner_pid"],
        "started_at": crawler_processes[0]["started_at"] if crawler_processes else None,
        "next_stage": live_state["next_stage"],
        "last_crawl_position": live_state["last_crawl_position"],
        "processed_shards": processed_batch,
        "planned_shards": planned_batch,
        "total_fetched_requests": int(shards.get_column("fetched").sum() or 0)
        if not shards.is_empty() and "fetched" in shards.columns
        else 0,
        "total_failed_requests": int(shards.get_column("failed").sum() or 0)
        if not shards.is_empty() and "failed" in shards.columns
        else 0,
        "recent_events": recent_events,
        "city_progress": city_frame,
    }

    total_slots = slots.height if not slots.is_empty() else None
    verified_slots = (
        _count(slots, pl.col("verified_candidate_count") > 0) if not slots.is_empty() else None
    )
    enabled_slots = (
        _count(slots, pl.col("enabled_source_count") > 0) if not slots.is_empty() else None
    )
    resolved_slots = (
        _count(slots, pl.col("status") != "unresolved") if not slots.is_empty() else None
    )
    enabled_unverified = (
        int(slots.get_column("enabled_pending_verification_count").sum() or 0)
        if not slots.is_empty() and "enabled_pending_verification_count" in slots.columns
        else None
    )
    gaps = frames["coverage_gaps"]
    gaps_available = (availability.get("coverage_gaps") or {}).get("status") == "available"
    open_gaps = (
        _count(
            gaps,
            ~pl.col("status")
            .fill_null("")
            .str.to_lowercase()
            .is_in(["resolved", "closed", "ignored"]),
        )
        if gaps_available and not gaps.is_empty() and "status" in gaps.columns
        else 0 if gaps_available else None
    )
    critical_gaps = (
        _count(
            gaps,
            pl.col("severity").fill_null("").str.to_lowercase().is_in(["critical", "high"])
            & ~pl.col("status")
            .fill_null("")
            .str.to_lowercase()
            .is_in(["resolved", "closed", "ignored"]),
        )
        if gaps_available and not gaps.is_empty() and {"severity", "status"}.issubset(gaps.columns)
        else 0 if gaps_available else None
    )
    metrics = {
        "city_live_progress": _metric(
            "已完成城市",
            completed_cities,
            city_denominator,
            source=("全量历史回溯运行日志" if log_progress else "实时分片快照"),
            updated_at=(
                log_progress.get("updated_at") or availability["crawl_shards"].get("updated_at")
            ),
            definition="全量回溯 runner 已明确结束城市阶段的数量；当前运行城市不提前计为完成。",
        ),
        "batch_shard_progress": _metric(
            "当前城市分片",
            processed_batch if planned_batch is not None else None,
            planned_batch,
            source="实时分片与流水线事件快照",
            updated_at=event_updated_at,
            definition="当前批次已到达终态的分片数 / 已规划可运行分片数。",
        ),
        "resolved_slots": _metric(
            "已解决来源槽位",
            resolved_slots,
            total_slots,
            source="525 来源槽位审计快照",
            updated_at=availability["source_requirement_slots"].get("updated_at"),
            definition="525 个必需槽位中不再处于 unresolved 的数量。",
        ),
        "verified_slots": _metric(
            "严格核验来源槽位",
            verified_slots,
            total_slots,
            source="525 来源槽位审计快照",
            updated_at=availability["source_requirement_slots"].get("updated_at"),
            definition="至少存在一个通过确定性严格验证候选的槽位数。",
        ),
        "enabled_slots": _metric(
            "已启用来源槽位",
            enabled_slots,
            total_slots,
            source="525 来源槽位审计快照",
            updated_at=availability["source_requirement_slots"].get("updated_at"),
            definition="至少存在一个启用来源的必需槽位数。",
        ),
    }
    snapshot.coverage = {
        "metrics": metrics,
        "total_slots": total_slots,
        "resolved_slots": resolved_slots,
        "verified_slots": verified_slots,
        "enabled_slots": enabled_slots,
        "unresolved_slots": (total_slots - resolved_slots)
        if total_slots is not None and resolved_slots is not None
        else None,
        "enabled_unverified": enabled_unverified,
        "open_gaps": open_gaps,
        "critical_gaps": critical_gaps,
        "strict_health": "warning" if enabled_unverified else "healthy",
    }

    records = frames["records"]
    geographies = frames["record_geographies_normalized"]
    records_available = (availability.get("records") or {}).get("status") == "available"
    geography_available = (
        (availability.get("record_geographies_normalized") or {}).get("status") == "available"
    )
    cities_with_documents = (
        geographies.join(records.select("record_id").unique(), on="record_id", how="inner")
        .get_column("city_id")
        .drop_nulls()
        .n_unique()
        if records_available and geography_available and not records.is_empty() and not geographies.is_empty()
        else 0 if records_available and geography_available else None
    )
    earliest = (
        records.get_column("record_date").drop_nulls().min()
        if not records.is_empty() and "record_date" in records.columns
        else None
    )
    latest_date = (
        records.get_column("record_date").drop_nulls().max()
        if not records.is_empty() and "record_date" in records.columns
        else None
    )
    snapshot.documents = {
        "records": _available_frame_count(frames, availability, "records"),
        "document_versions": _available_frame_count(
            frames, availability, "policy_document_versions"
        ),
        "crawl_items": _available_frame_count(frames, availability, "crawl_items"),
        "cities_with_documents": (
            int(cities_with_documents) if cities_with_documents is not None else None
        ),
        "earliest_date": earliest,
        "latest_date": latest_date,
        "last_updated_at": availability["records"].get("updated_at"),
    }

    quality = _document_quality(records)
    quality.update(
        open_gaps=open_gaps,
        critical_gaps=critical_gaps,
        source_gate_inconsistencies=int(enabled_unverified or 0),
    )
    snapshot.quality = quality

    candidates = frames["source_candidates"]
    manual_candidates = (
        _count(
            candidates,
            pl.col("manual_review_status")
            .fill_null("")
            .str.to_lowercase()
            .is_in(["pending", "human_review", "requires_human_review", "needs_research"]),
        )
        if not candidates.is_empty() and "manual_review_status" in candidates.columns
        else 0
    )
    low_confidence = (
        _count(
            candidates,
            pl.col("overall_confidence").is_not_null() & (pl.col("overall_confidence") < 0.7),
        )
        if not candidates.is_empty() and "overall_confidence" in candidates.columns
        else 0
    )
    snapshot.review = {
        "source_candidates": manual_candidates,
        "low_confidence_candidates": low_confidence,
        "document_issues": sum(
            int(quality[key])
            for key in ("missing_title", "missing_date", "missing_text", "missing_source")
        ),
    }

    ai_state = automation["AI_QUEUE_STATE"]
    snapshot.ai = {
        "status": ai_state.get("status"),
        "deferred": ai_state.get("deferred"),
        "updated_at": ai_state.get("updated_at"),
        "extractions": frames["llm_extractions"].height,
        "verifications": frames["llm_verifications"].height,
        "current_crawl_ai_enabled": False if crawler_running else None,
    }

    pdf_assets = frames["pdf_assets"]
    snapshot.archive = {
        "pdf_status": automation["PDF_ARCHIVE_STATE"].get("status"),
        "pdf_assets": pdf_assets.height,
        "valid_pdf_assets": _count(pdf_assets, pl.col("valid_pdf"))
        if not pdf_assets.is_empty() and "valid_pdf" in pdf_assets.columns
        else 0,
        "parsed_pdf_assets": _count(pdf_assets, pl.col("text_char_count").fill_null(0) > 0)
        if not pdf_assets.is_empty() and "text_char_count" in pdf_assets.columns
        else 0,
        "scanned_pdf_assets": _count(pdf_assets, pl.col("is_scanned"))
        if not pdf_assets.is_empty() and "is_scanned" in pdf_assets.columns
        else 0,
        "archive_root_exists": settings.archive_root.exists(),
    }

    snapshot.frames = {
        "recent_events": recent_events,
        "city_progress": city_frame,
        "source_slots": slots,
        "city_year_progress": frames["city_year_progress"],
        "city_source_year_progress": frames["city_source_year_progress"],
        "records": records,
        "record_geographies": geographies,
        "crawl_runs": frames["crawl_runs"],
        "source_registry": frames["source_registry"],
        "source_sync_state": frames["source_sync_state"],
        "source_candidates": candidates,
        "coverage_gaps": gaps,
    }
    snapshot.availability = availability
    return snapshot


def _snapshot_cache_key(settings: Settings) -> str:
    return f"{settings.database.resolve()}::{settings.curated.resolve()}"


def _last_good_snapshot(settings: Settings) -> DashboardSnapshot | None:
    cached = _LAST_GOOD_SNAPSHOTS.get(_snapshot_cache_key(settings))
    return deepcopy(cached) if cached is not None else None


def _serve_last_good_snapshot(
    settings: Settings,
    *,
    reason: str,
) -> DashboardSnapshot | None:
    snapshot = _last_good_snapshot(settings)
    if snapshot is None:
        return None
    snapshot.system = dict(snapshot.system)
    snapshot.system.update(
        snapshot_status="LAST_GOOD",
        snapshot_warning="数据正在更新，当前展示上一份成功快照。",
        snapshot_served_at=datetime.now(UTC).isoformat(),
        snapshot_failure_reason=reason,
    )
    snapshot.availability = dict(snapshot.availability)
    snapshot.availability["snapshot"] = {
        "status": "last_good",
        "error_type": reason,
        "updated_at": snapshot.generated_at,
    }
    return snapshot


def load_dashboard_snapshot(
    settings: Settings | None = None,
    *,
    event_limit: int = 20,
) -> DashboardSnapshot:
    """Build a read-only snapshot with retries and a last-known-good fallback."""

    settings = settings or Settings.discover()
    last_error: BaseException | None = None
    for attempt, delay in enumerate(_READ_RETRY_DELAYS):
        if delay:
            time.sleep(delay)
        try:
            snapshot = _build_dashboard_snapshot(settings, event_limit=event_limit)
            records_meta = snapshot.availability.get("records") or {}
            if records_meta.get("status") == "available":
                snapshot.system = dict(snapshot.system)
                snapshot.system["snapshot_status"] = "FRESH"
                _LAST_GOOD_SNAPSHOTS[_snapshot_cache_key(settings)] = deepcopy(snapshot)
                return snapshot
            fallback = _serve_last_good_snapshot(
                settings,
                reason="records_snapshot_unavailable",
            )
            if fallback is not None:
                return fallback
            return snapshot
        except Exception as exc:
            last_error = exc
            if attempt == len(_READ_RETRY_DELAYS) - 1:
                break
            continue

    assert last_error is not None
    log_dashboard_exception(
        settings,
        "Dashboard snapshot build failed after retries",
        component="dashboard_snapshot",
        operation="build_snapshot",
        data_source=str(settings.database),
        relation="dashboard_snapshot",
        query="load_dashboard_snapshot",
        error=last_error,
    )
    fallback = _serve_last_good_snapshot(settings, reason=type(last_error).__name__)
    if fallback is not None:
        return fallback
    raise last_error


def clear_dashboard_caches() -> None:
    _cached_parquet.cache_clear()
    _cached_json.cache_clear()
    _database_health.cache_clear()
    _cached_backfill_log.cache_clear()
    _LAST_GOOD_SNAPSHOTS.clear()


__all__ = [
    "DashboardSnapshot",
    "ProgressMetric",
    "clear_dashboard_caches",
    "database_health",
    "load_dashboard_snapshot",
]
