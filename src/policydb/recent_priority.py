"""Bounded, resumable recent-30-day crawl priority stage.

This stage deliberately reuses ``ExhaustiveCrawler`` and the existing archive
and database builders.  It is a scheduler boundary, not a second crawler:
one queue item is processed at a time, its version rows are promoted, its
official files are archived, and the formal database is rebuilt before the
next item is claimed.
"""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import polars as pl
from filelock import FileLock

from policydb.archive import archive_document_versions
from policydb.crawl.registry import load_registry
from policydb.dedup_audit import materialize_policy_identity
from policydb.exhaustive import ExhaustiveCrawler
from policydb.ingest.promote_versions import promote_document_versions
from policydb.parquet_store import (
    ParquetStoreError,
    atomic_write_parquet,
    read_parquet_snapshot,
)
from policydb.query.database import build_database
from policydb.scope import load_cities_105
from policydb.settings import Settings
from policydb.transform.normalization import stable_id

QUEUE_NAME = "RECENT_30D_QUEUE.parquet"
STATE_NAME = "RECENT_30D_STATE.json"
RECENT_STATUS_COMPLETE = {"SUCCESS", "ZERO_CONFIRMED"}


@dataclass(frozen=True)
class Recent30DConfig:
    start_date: date
    end_date: date
    max_items: int = 20
    max_pages_per_source: int = 30
    max_candidates_per_shard: int = 500
    max_fetches_per_shard: int = 500
    apply: bool = False
    resume: bool = True

    @classmethod
    def default(cls, *, today: date | None = None, **updates: Any) -> Recent30DConfig:
        end = today or date.today()
        defaults = {
            "start_date": end - timedelta(days=30),
            "end_date": end,
        }
        defaults.update({key: value for key, value in updates.items() if value is not None})
        return cls(**defaults)

    def validate(self) -> None:
        if self.start_date > self.end_date:
            raise ValueError("recent-30d start_date must not be after end_date")
        if self.max_items < 1:
            raise ValueError("recent-30d max_items must be positive")
        if self.max_pages_per_source < 1 or self.max_candidates_per_shard < 1 or self.max_fetches_per_shard < 1:
            raise ValueError("recent-30d crawl limits must be positive")


def _utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


def _atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump(payload, stream, ensure_ascii=False, indent=2, default=str)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _output_root(settings: Settings) -> Path:
    path = settings.outputs / "recent_30d"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _queue_path(settings: Settings) -> Path:
    return _output_root(settings) / QUEUE_NAME


def _state_path(settings: Settings) -> Path:
    settings.automation.mkdir(parents=True, exist_ok=True)
    return settings.automation / STATE_NAME


def _queue_rows(settings: Settings, config: Recent30DConfig) -> list[dict[str, Any]]:
    cities = load_cities_105(settings)
    registry = load_registry(settings)
    rows: list[dict[str, Any]] = []
    for city in cities.iter_rows(named=True):
        city_id = str(city["city_id"])
        for source in registry:
            if not source.crawl_enabled or str(source.official_status).lower() != "official":
                continue
            if city_id not in {str(value) for value in source.city_ids}:
                continue
            role = str(source.agency_type or source.source_role)
            item_id = stable_id(
                city_id,
                source.source_id,
                role,
                config.start_date.isoformat(),
                config.end_date.isoformat(),
                prefix="RECENT30",
            )
            rows.append(
                {
                    "item_id": item_id,
                    "city_id": city_id,
                    "city_name": city["city_name"],
                    "source_id": str(source.source_id),
                    "source_role": role,
                    "official_domain": str(source.domain),
                    "entry_url": (list(source.list_page_urls) or [source.homepage_url])[0],
                    "start_date": config.start_date.isoformat(),
                    "end_date": config.end_date.isoformat(),
                    "status": "PENDING",
                    "attempts": 0,
                    "run_ids_json": "[]",
                    "batch_ids_json": "[]",
                    "documents_found": 0,
                    "document_versions": 0,
                    "records_promoted": 0,
                    "rejected_versions": 0,
                    "pdfs_found": 0,
                    "pdfs_archived": 0,
                    "list_checked": False,
                    "zero_confirmed": False,
                    "last_event": None,
                    "last_event_at": None,
                    "last_error": None,
                    "started_at": None,
                    "completed_at": None,
                    "updated_at": _utc_now(),
                }
            )
    # Role-first ordering makes the first bounded batch touch many cities and
    # prevents a single city's slow source family from monopolising the queue.
    return sorted(rows, key=lambda row: (row["source_role"], row["city_name"], row["source_id"]))


def build_recent_queue(
    settings: Settings | None = None,
    *,
    config: Recent30DConfig | None = None,
    resume: bool = True,
) -> pl.DataFrame:
    settings = settings or Settings.discover()
    config = config or Recent30DConfig.default()
    config.validate()
    path = _queue_path(settings)
    previous = read_parquet_snapshot(path) if path.exists() else pl.DataFrame()
    previous_by_id = {
        str(row["item_id"]): row for row in previous.iter_rows(named=True)
    } if "item_id" in previous.columns else {}
    rows = _queue_rows(settings, config)
    preserved: list[dict[str, Any]] = []
    for row in rows:
        old = previous_by_id.get(row["item_id"])
        if old and resume:
            for key in (
                "status", "attempts", "run_ids_json", "batch_ids_json", "documents_found",
                "document_versions", "records_promoted", "pdfs_found", "pdfs_archived",
                "rejected_versions",
                "list_checked", "zero_confirmed", "last_event", "last_event_at", "last_error",
                "started_at", "completed_at",
            ):
                if key in old:
                    row[key] = old[key]
            if row.get("status") == "RUNNING":
                # A process can die after claiming an item but before its
                # checkpoint.  Requeue that exact item; promotion and archive
                # are idempotent, so recovery never needs a new work item.
                row["status"] = "PENDING"
                row["last_error"] = "requeued_after_interrupted_recent_worker"
        preserved.append(row)
    frame = pl.DataFrame(preserved, infer_schema_length=None)
    atomic_write_parquet(
        frame,
        path,
        {"module": "recent_priority", "scope": "official_enabled_sources"},
        key_columns=("item_id",),
    )
    _atomic_json(
        _state_path(settings),
        {
            "status": "PLANNED",
            "queue_total": frame.height,
            "start_date": config.start_date.isoformat(),
            "end_date": config.end_date.isoformat(),
            "updated_at": _utc_now(),
        },
    )
    return frame


def _write_queue(settings: Settings, frame: pl.DataFrame) -> None:
    atomic_write_parquet(
        frame,
        _queue_path(settings),
        {"module": "recent_priority", "operation": "checkpoint"},
        key_columns=("item_id",),
    )


def _max_record_date(settings: Settings) -> str | None:
    path = settings.curated / "records.parquet"
    if not path.exists():
        return None
    frame = read_parquet_snapshot(path)
    if "record_date" not in frame.columns or frame.height == 0:
        return None
    values = frame.get_column("record_date").drop_nulls()
    return str(values.max()) if len(values) else None


def _is_blocking_exception(exc: Exception) -> bool:
    message = str(exc).lower()
    if isinstance(exc, (OSError, ParquetStoreError)):
        return True
    return any(
        term in message
        for term in (
            "duckdb",
            "database",
            "schema",
            "checkpoint",
            "parquet lock",
            "write conflict",
        )
    )


def _run_version_ids(settings: Settings, run_ids: list[str]) -> list[str]:
    version_path = settings.curated / "policy_document_versions.parquet"
    item_path = settings.curated / "crawl_items.parquet"
    if not version_path.exists() or not item_path.exists() or not run_ids:
        return []
    items = read_parquet_snapshot(item_path).filter(pl.col("run_id").is_in(run_ids))
    if items.is_empty():
        return []
    return (
        read_parquet_snapshot(version_path)
        .filter(pl.col("crawl_item_id").is_in(items.get_column("item_id").to_list()))
        .get_column("document_version_id")
        .cast(pl.String)
        .to_list()
    )


def _pdf_counts(settings: Settings, version_ids: list[str]) -> tuple[int, int, pl.DataFrame]:
    if not version_ids:
        return 0, 0, pl.DataFrame()
    versions_path = settings.curated / "policy_document_versions.parquet"
    files_path = settings.curated / "policy_files.parquet"
    versions = read_parquet_snapshot(versions_path) if versions_path.exists() else pl.DataFrame()
    files = read_parquet_snapshot(files_path) if files_path.exists() else pl.DataFrame()
    pdf_ids = (
        versions.filter(
            pl.col("document_version_id").is_in(version_ids)
            & pl.col("content_type").cast(pl.String, strict=False).str.to_lowercase().str.contains("pdf")
        ).get_column("document_version_id").cast(pl.String).to_list()
        if versions.height
        else []
    )
    manifest = files.filter(pl.col("document_version_id").is_in(version_ids)) if files.height else pl.DataFrame()
    archived = (
        manifest.filter(
            (pl.col("archive_status") == "archived")
            & pl.col("content_type").cast(pl.String, strict=False).str.to_lowercase().str.contains("pdf")
        ).height
        if manifest.height
        else 0
    )
    return len(pdf_ids), archived, manifest


def _write_outputs(settings: Settings, queue: pl.DataFrame, *, state: dict[str, Any]) -> dict[str, Any]:
    root = _output_root(settings)
    _write_queue(settings, queue)
    completed = queue.filter(pl.col("status").is_in(list(RECENT_STATUS_COMPLETE))) if queue.height else queue
    city_coverage = (
        queue.group_by(["city_id", "city_name"])
        .agg(
            pl.len().alias("source_count"),
            pl.col("status").is_in(list(RECENT_STATUS_COMPLETE)).sum().alias("completed_sources"),
            pl.col("documents_found").sum(),
            pl.col("records_promoted").sum(),
            pl.col("rejected_versions").sum(),
            pl.col("pdfs_archived").sum(),
        )
        .sort("city_name")
        if queue.height
        else pl.DataFrame()
    )
    source_coverage = (
        queue.group_by(["source_id", "source_role", "official_domain"])
        .agg(
            pl.len().alias("city_count"),
            pl.col("status").is_in(list(RECENT_STATUS_COMPLETE)).sum().alias("completed_cities"),
            pl.col("documents_found").sum(),
            pl.col("records_promoted").sum(),
            pl.col("rejected_versions").sum(),
            pl.col("pdfs_archived").sum(),
        )
        .sort(["source_role", "official_domain"])
        if queue.height
        else pl.DataFrame()
    )
    for frame, name in (
        (city_coverage, "RECENT_30D_CITY_COVERAGE.parquet"),
        (source_coverage, "RECENT_30D_SOURCE_COVERAGE.parquet"),
    ):
        frame.write_parquet(root / name, compression="zstd")
    failures = queue.filter(~pl.col("status").is_in(list(RECENT_STATUS_COMPLETE))) if queue.height else pl.DataFrame()
    failures.write_parquet(root / "RECENT_30D_FAILURES.parquet", compression="zstd")

    policy_path = settings.curated / "policy_document_versions.parquet"
    policy_rows = []
    if policy_path.exists() and queue.height:
        run_ids = [rid for value in queue.get_column("run_ids_json").to_list() for rid in json.loads(value or "[]")]
        if run_ids:
            ids = _run_version_ids(settings, sorted(set(run_ids)))
            versions = read_parquet_snapshot(policy_path).filter(pl.col("document_version_id").is_in(ids))
            items_path = settings.curated / "crawl_items.parquet"
            if items_path.exists() and "crawl_item_id" in versions.columns:
                items = read_parquet_snapshot(items_path)
                item_columns = [
                    column
                    for column in ("item_id", "run_id", "city_id", "candidate_date", "candidate_date_source")
                    if column in items.columns
                ]
                if "item_id" in item_columns:
                    versions = versions.join(
                        items.select(item_columns),
                        left_on="crawl_item_id",
                        right_on="item_id",
                        how="left",
                    )
            policy_rows = versions.to_dicts()
    policy_frame = (
        pl.DataFrame(policy_rows, infer_schema_length=None)
        if policy_rows
        else pl.DataFrame(
            schema={
                "document_version_id": pl.String,
                "record_id": pl.String,
                "source_id": pl.String,
                "canonical_url": pl.String,
                "title": pl.String,
                "extracted_text": pl.String,
            }
        )
    )
    atomic_write_parquet(
        policy_frame,
        root / "RECENT_30D_POLICY_LIST.parquet",
        {"module": "recent_priority", "operation": "policy_list"},
    )

    all_pdf_manifest: list[pl.DataFrame] = []
    if queue.height:
        run_ids = [rid for value in queue.get_column("run_ids_json").to_list() for rid in json.loads(value or "[]")]
        for run_id in sorted(set(run_ids)):
            ids = _run_version_ids(settings, [run_id])
            _, _, manifest = _pdf_counts(settings, ids)
            if manifest.height:
                all_pdf_manifest.append(manifest)
    pdf_manifest = pl.concat(all_pdf_manifest, how="diagonal_relaxed") if all_pdf_manifest else pl.DataFrame(
        schema={
            "policy_file_id": pl.String,
            "document_version_id": pl.String,
            "record_id": pl.String,
            "archive_relative_path": pl.String,
            "content_type": pl.String,
            "archive_status": pl.String,
        }
    )
    atomic_write_parquet(
        pdf_manifest,
        root / "RECENT_30D_PDF_MANIFEST.parquet",
        {"module": "recent_priority", "operation": "pdf_manifest"},
    )

    started = queue.filter(pl.col("attempts") > 0) if queue.height else queue
    latest_event_row = (
        started.sort("last_event_at", descending=True).row(0, named=True)
        if started.height and "last_event_at" in started.columns
        else {}
    )
    started_cities = sorted(started.get_column("city_name").unique().to_list()) if started.height else []
    started_sources = sorted(started.get_column("source_id").unique().to_list()) if started.height else []
    summary = {
        **state,
        "queue_size": queue.height,
        "completed_items": completed.height,
        "cities_started": sorted(queue.filter(pl.col("attempts") > 0).get_column("city_name").unique().to_list()) if queue.height else [],
        "sources_started": sorted(queue.filter(pl.col("attempts") > 0).get_column("source_id").unique().to_list()) if queue.height else [],
        "documents_found": int(queue.get_column("documents_found").sum()) if queue.height else 0,
        "document_versions": int(queue.get_column("document_versions").sum()) if queue.height else 0,
        "records_promoted": int(queue.get_column("records_promoted").sum()) if queue.height else 0,
        "rejected_versions": int(queue.get_column("rejected_versions").sum()) if queue.height else 0,
        "pdfs_found": int(queue.get_column("pdfs_found").sum()) if queue.height else 0,
        "pdfs_archived": int(queue.get_column("pdfs_archived").sum()) if queue.height else 0,
        "max_record_date_after": _max_record_date(settings),
        "updated_at": _utc_now(),
    }
    summary.update(
        {
            "RECENT_QUEUE_SIZE": queue.height,
            "RECENT_CITIES_STARTED": len(started_cities),
            "RECENT_CITIES_STARTED_LIST": started_cities,
            "RECENT_SOURCES_STARTED": len(started_sources),
            "RECENT_SOURCES_STARTED_LIST": started_sources,
            "LATEST_RECENT_EVENT": latest_event_row.get("last_event"),
            "LATEST_RECENT_EVENT_TIME": latest_event_row.get("last_event_at"),
            "RECENT_DOCUMENTS_FOUND": summary["documents_found"],
            "RECENT_DOCUMENT_VERSIONS": summary["document_versions"],
            "RECENT_RECORDS_PROMOTED": summary["records_promoted"],
            "RECENT_REJECTED_VERSIONS": summary["rejected_versions"],
            "MAX_RECORD_DATE_BEFORE": state.get("max_record_date_before"),
            "MAX_RECORD_DATE_AFTER": summary["max_record_date_after"],
            "RECENT_PDFS_FOUND": summary["pdfs_found"],
            "RECENT_PDFS_ARCHIVED": summary["pdfs_archived"],
        }
    )
    _atomic_json(root / "RECENT_30D_SUMMARY.json", summary)
    (root / "RECENT_30D_SUMMARY.md").write_text(
        "# Recent 30-day crawl summary\n\n"
        + "\n".join(f"- **{key}**: {value}" for key, value in summary.items())
        + "\n",
        encoding="utf-8",
    )
    _atomic_json(_state_path(settings), summary)
    return summary


def run_recent_30d(
    settings: Settings | None = None,
    *,
    config: Recent30DConfig | None = None,
) -> dict[str, Any]:
    """Run one bounded queue slice; safe to invoke repeatedly by the scheduler."""

    settings = settings or Settings.discover()
    config = config or Recent30DConfig.default()
    config.validate()
    settings.automation.mkdir(parents=True, exist_ok=True)
    lock = FileLock(str(settings.automation / "RECENT_30D_WRITER.lock"), timeout=0.2)
    with lock:
        prior_state_path = _state_path(settings)
        prior_state = (
            json.loads(prior_state_path.read_text(encoding="utf-8"))
            if prior_state_path.exists()
            else {}
        )
        queue = build_recent_queue(settings, config=config, resume=config.resume)
        # Keep the first baseline across scheduler invocations; otherwise a
        # later bounded slice would overwrite MAX_RECORD_DATE_BEFORE with the
        # previous slice's result and make the final audit non-comparable.
        before = prior_state.get("max_record_date_before") or _max_record_date(settings)
        state: dict[str, Any] = {
            "status": "RUNNING" if config.apply else "PLANNED",
            "start_date": config.start_date.isoformat(),
            "end_date": config.end_date.isoformat(),
            "max_record_date_before": before,
            "current_item": None,
            "updated_at": _utc_now(),
        }
        if not config.apply:
            summary = _write_outputs(settings, queue, state=state)
            return {**summary, "queue_size": queue.height, "processed_items": 0, "exit_code": 0}
        pending_indices = [
            index
            for index, row in enumerate(queue.iter_rows(named=True))
            if row.get("status") not in RECENT_STATUS_COMPLETE
        ][: config.max_items]
        processed = 0
        blocking_error: str | None = None
        for index in pending_indices:
            row = queue.row(index, named=True)
            item_id = str(row["item_id"])
            queue = queue.with_columns(
                pl.when(pl.col("item_id") == item_id).then(pl.lit("RUNNING")).otherwise(pl.col("status")).alias("status"),
                pl.when(pl.col("item_id") == item_id).then(pl.col("attempts") + 1).otherwise(pl.col("attempts")).alias("attempts"),
                pl.when(pl.col("item_id") == item_id).then(pl.lit(_utc_now())).otherwise(pl.col("started_at")).alias("started_at"),
            )
            state.update({"current_item": item_id, "processed_items": processed, "updated_at": _utc_now()})
            _write_queue(settings, queue)
            _atomic_json(_state_path(settings), state)
            try:
                crawler = ExhaustiveCrawler(settings)
                result = crawler.run_city(
                    str(row["city_id"]),
                    start_date=config.start_date,
                    end_date=config.end_date,
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
                for run in run_ids:
                    archive_document_versions(settings, run_id=run)
                for run in run_ids:
                    materialize_policy_identity(settings, run_id=run)
                build_database(settings, materialize_geography=False)
                documents = int(sum(result.get("run_metrics", {}).get(run, {}).get("fetched", 0) for run in run_ids))
                pdfs_found, pdfs_archived, _ = _pdf_counts(settings, version_ids)
                failed = int(sum(result.get("run_metrics", {}).get(run, {}).get("failed", 0) for run in run_ids))
                zero_confirmed = documents == 0 and failed == 0 and bool(result.get("processed_shards"))
                final_status = "ZERO_CONFIRMED" if zero_confirmed else ("SUCCESS" if documents else "LIST_CHECKED")
                if failed and not documents:
                    final_status = "RETRY_WAIT"
                update = {
                    "status": final_status,
                    "run_ids_json": json.dumps(run_ids, ensure_ascii=False),
                    "batch_ids_json": json.dumps([result.get("batch_id")] if result.get("batch_id") else [], ensure_ascii=False),
                    "documents_found": documents,
                    "document_versions": len(version_ids),
                    "records_promoted": int(promotion.get("promoted_records", 0)),
                    "rejected_versions": int(promotion.get("rejected_versions", 0)),
                    "pdfs_found": pdfs_found,
                    "pdfs_archived": pdfs_archived,
                    "list_checked": bool(result.get("processed_shards")),
                    "zero_confirmed": zero_confirmed,
                    "last_event": "ZERO_CONFIRMED" if zero_confirmed else "FORMAL_RECORDS_PROMOTED",
                    "last_event_at": _utc_now(),
                    "last_error": None if not failed else f"failed_fetches={failed}",
                    "completed_at": _utc_now(),
                    "updated_at": _utc_now(),
                }
                for column, value in update.items():
                    queue = queue.with_columns(
                        pl.when(pl.col("item_id") == item_id).then(pl.lit(value)).otherwise(pl.col(column)).alias(column)
                    )
            except Exception as exc:  # noqa: BLE001 - persisted retry state is the recovery contract
                if _is_blocking_exception(exc):
                    blocking_error = f"{type(exc).__name__}: {str(exc)[:500]}"
                update = {
                    "status": "RETRY_WAIT",
                    "last_event": "RECENT_ITEM_FAILED",
                    "last_event_at": _utc_now(),
                    "last_error": f"{type(exc).__name__}: {str(exc)[:500]}",
                    "updated_at": _utc_now(),
                }
                for column, value in update.items():
                    queue = queue.with_columns(
                        pl.when(pl.col("item_id") == item_id).then(pl.lit(value)).otherwise(pl.col(column)).alias(column)
                    )
            processed += 1
            _write_queue(settings, queue)
            state.update({"processed_items": processed, "current_item": None, "updated_at": _utc_now()})
            if blocking_error:
                state["latest_error"] = blocking_error
                state["status"] = "BLOCKED"
            _atomic_json(_state_path(settings), state)
            if blocking_error:
                break

        complete = bool(queue.height) and queue.get_column("status").is_in(list(RECENT_STATUS_COMPLETE)).all()
        state.update(
            {
                "status": "BLOCKED" if blocking_error else ("COMPLETE" if complete else "PARTIAL"),
                "current_item": None,
                "updated_at": _utc_now(),
            }
        )
        if blocking_error:
            state["blocking_reason"] = "RECENT_FORMAL_INGEST_BLOCKED"
        summary = _write_outputs(settings, queue, state=state)
        return {
            **summary,
            "queue_size": queue.height,
            "processed_items": processed,
            "blocking_error": blocking_error,
            "exit_code": 1 if blocking_error else 0,
        }


__all__ = ["Recent30DConfig", "build_recent_queue", "run_recent_30d"]
