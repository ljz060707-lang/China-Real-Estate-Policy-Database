"""Lightweight, read-only monitoring and audit helpers for the 930 episode.

The production crawler remains the owner of the queue and curated tables.  This
module only reads immutable snapshots/audits and writes two small operational
artifacts: ``930_MONITOR_SNAPSHOT.json`` and ``930_PROGRESS_HISTORY.parquet``.
It deliberately never calls a search provider, HTTP client, or database writer.
"""

from __future__ import annotations

import json
import os
import statistics
import uuid
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import polars as pl

from policydb.parquet_store import atomic_write_parquet, read_parquet_snapshot

EPISODE_ID = "EP_2016_930_TIGHTENING"
EXECUTION_MODE = "URGENT_CONVERGENCE"

PROGRESS_WEIGHTS: dict[str, float] = {
    "discovery": 0.20,
    "official_recovery": 0.10,
    "action_extraction": 0.10,
    "api_pass1": 0.10,
    "api_pass2": 0.08,
    "date_verification": 0.10,
    "parameter_extraction": 0.06,
    "attachment": 0.04,
    "dedup_quality": 0.06,
    "formal_promotion": 0.07,
    "gap_audit": 0.05,
    "export_dashboard": 0.04,
}

HISTORY_SCHEMA = {
    "snapshot_at": pl.String,
    "run_id": pl.String,
    "queue_completed": pl.Int64,
    "queue_total": pl.Int64,
    "search_items": pl.Int64,
    "search_results": pl.Int64,
    "http_requests": pl.Int64,
    "http_200": pl.Int64,
    "document_versions": pl.Int64,
    "documents": pl.Int64,
    "official_documents": pl.Int64,
    "actions": pl.Int64,
    "api_pass1": pl.Int64,
    "api_pass2": pl.Int64,
    "dates": pl.Int64,
    "parameters": pl.Int64,
    "attachments_resolved": pl.Int64,
    "promoted": pl.Int64,
    "critical_gaps": pl.Int64,
    "provenance_completed": pl.Int64,
    "provenance_pending": pl.Int64,
    "attachments_completed": pl.Int64,
    "attachment_pending": pl.Int64,
    "manual_review_completed": pl.Int64,
    "manual_review_pending": pl.Int64,
    "final_manifest_ready": pl.Int64,
}

QUEUE_TERMINAL_STATUSES = frozenset({"CRAWL_COMPLETED", "COMPLETED", "SUCCESS"})
QUEUE_RETRY_STATUSES = frozenset({"RETRY_WAIT", "RETRYABLE_FAILURE", "FAILED_RETRYABLE"})
QUEUE_ACTIVE_STATUSES = frozenset({"RUNNING", "CLAIMED", "IN_PROGRESS", "FETCHING", "PROCESSING"})


def now_utc() -> datetime:
    return datetime.now(UTC)


def iso_now() -> str:
    return now_utc().isoformat()


def read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def atomic_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, default=str) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def read_table(path: Path, columns: list[str] | None = None) -> pl.DataFrame:
    if not path.exists():
        return pl.DataFrame()
    try:
        return read_parquet_snapshot(path, columns=columns)
    except Exception:
        return pl.DataFrame()


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "y"}


def _upper(value: Any) -> str:
    return str(value or "").strip().upper()


def reconcile_queue(output: Path, progress: dict[str, Any]) -> dict[str, Any]:
    """Reconcile queue rows with progress leases without mutating production state.

    The queue row is authoritative for accounting.  Progress lease references are
    diagnostic only: a reference to a terminal row is reported as stale and is
    never counted as active work.  The mutually exclusive accounting buckets
    always sum to ``total`` when a readable queue snapshot exists.
    """

    queue = read_table(output / "930_TASK_QUEUE.parquet")
    lease = progress.get("lease") if isinstance(progress.get("lease"), dict) else {}
    lease_ids = [str(value) for value in (lease.get("queue_item_ids") or []) if value not in (None, "")]
    if queue.is_empty() or "status" not in queue.columns:
        total = _int(progress.get("queue_total"))
        completed = _int(progress.get("queue_completed"))
        retry = _int(progress.get("queue_retry"))
        active = _int(progress.get("queue_running"))
        pending = max(0, total - completed - retry - active)
        accounted = completed + retry + active + pending
        return {
            "source": "progress_fallback",
            "total": total,
            "accounted_total": accounted,
            "consistent": accounted == total,
            "accounted_statuses": {
                "completed": completed,
                "retry": retry,
                "leased": 0,
                "active": active,
                "pending": pending,
            },
            "active": active,
            "inflight": active,
            "leased": 0,
            "retry": retry,
            "stale_completed_lease_references": 0,
            "active_terminal_history_references": 0,
            "lease_reference_count": len(lease_ids),
            "lease_reference_items": [],
            "lease_ids": lease_ids,
        }

    rows = queue.to_dicts()
    by_id = {str(row.get("queue_item_id")): row for row in rows if row.get("queue_item_id") not in (None, "")}
    lease_reference_items: list[dict[str, Any]] = []
    stale_completed = 0
    active_terminal_history = 0
    nonterminal_lease_ids: set[str] = set()
    for item_id in lease_ids:
        row = by_id.get(item_id)
        status = _upper(row.get("status")) if row else "MISSING"
        if status in QUEUE_TERMINAL_STATUSES:
            if _upper(progress.get("status")) in {"RUNNING", "IN_PROGRESS"}:
                active_terminal_history += 1
                reference_state = "ACTIVE_RECOVERY_REFERENCE_TO_TERMINAL_HISTORY"
            else:
                stale_completed += 1
                reference_state = "STALE_COMPLETED_REFERENCE"
        elif row is None:
            reference_state = "MISSING_QUEUE_ROW"
        else:
            reference_state = "ACTIVE_LEASE_REFERENCE"
            nonterminal_lease_ids.add(item_id)
        compact = {
            "queue_item_id": item_id,
            "city": row.get("city") if row else None,
            "status": status,
            "execution_status": row.get("execution_status") if row else None,
            "fetch_status": row.get("fetch_status") if row else None,
            "result_status": row.get("result_status") if row else None,
            "document_version_id": row.get("document_version_id") if row else None,
            "updated_at": row.get("updated_at") if row else None,
            "reference_state": reference_state,
        }
        lease_reference_items.append(compact)

    completed = 0
    retry = 0
    leased = 0
    active = 0
    pending = 0
    for row in rows:
        status = _upper(row.get("status"))
        item_id = str(row.get("queue_item_id")) if row.get("queue_item_id") not in (None, "") else ""
        if status in QUEUE_TERMINAL_STATUSES:
            completed += 1
        elif status in QUEUE_RETRY_STATUSES:
            retry += 1
        elif item_id in nonterminal_lease_ids or row.get("lease_owner") not in (None, "") or row.get("lease_expires_at") not in (None, ""):
            leased += 1
        elif status in QUEUE_ACTIVE_STATUSES or _upper(row.get("execution_status")) in QUEUE_ACTIVE_STATUSES:
            active += 1
        else:
            pending += 1

    accounted_total = completed + retry + leased + active + pending
    return {
        "source": "atomic_queue_snapshot",
        "total": queue.height,
        "accounted_total": accounted_total,
        "consistent": accounted_total == queue.height,
        "accounted_statuses": {
            "completed": completed,
            "retry": retry,
            "leased": leased,
            "active": active,
            "pending": pending,
        },
        "active": active + leased,
        "inflight": active + leased,
        "leased": leased,
        "retry": retry,
        "stale_completed_lease_references": stale_completed,
        "active_terminal_history_references": active_terminal_history,
        "lease_reference_count": len(lease_ids),
        "lease_reference_items": lease_reference_items,
        "lease_ids": lease_ids,
    }


def queue_counts(output: Path, progress: dict[str, Any]) -> dict[str, Any]:
    """Prefer reconciled queue rows over stale progress counters."""

    reconciliation = reconcile_queue(output, progress)
    buckets = reconciliation["accounted_statuses"]
    return {
        "queue_total": _int(reconciliation["total"]),
        "queue_completed": _int(buckets["completed"]),
        "queue_pending": _int(buckets["pending"]),
        "queue_running": _int(buckets["active"] + buckets["leased"]),
        "queue_retry": _int(buckets["retry"]),
        "queue_reconciliation": reconciliation,
    }


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _safe_sum(frame: pl.DataFrame, column: str) -> int:
    if frame.is_empty() or column not in frame.columns:
        return 0
    try:
        return _int(frame.get_column(column).fill_null(0).sum())
    except Exception:
        return 0


def _count_non_null(frame: pl.DataFrame, column: str) -> int:
    if frame.is_empty() or column not in frame.columns:
        return 0
    return _int(frame.get_column(column).is_not_null().sum())


def _unique(frame: pl.DataFrame, column: str) -> int:
    if frame.is_empty() or column not in frame.columns:
        return 0
    return _int(frame.get_column(column).drop_nulls().n_unique())


def audit_crawl_artifacts(output: Path) -> dict[str, int]:
    """Read only the bounded queue audit files, never raw HTML or full crawls."""

    search = read_table(output / "930_QUEUE_SEARCH_EXECUTION.parquet")
    http = read_table(output / "930_QUEUE_HTTP_AUDIT.parquet")
    http_200 = 0
    if not http.is_empty() and "http_status" in http.columns:
        http_200 = _int((http.get_column("http_status") == 200).sum())
    real = 0
    if not http.is_empty() and "real_network_fetch" in http.columns:
        real = _int(http.get_column("real_network_fetch").fill_null(False).sum())
    return {
        "search_calls": _unique(search, "queue_item_id"),
        "search_results": search.height,
        "unique_urls": _unique(search, "result_url"),
        "http_requests": http.height,
        "http_200": http_200,
        "http_failures": max(0, http.height - http_200),
        "real_network_fetches": real,
        "response_bytes": _safe_sum(http, "response_bytes"),
        "document_versions": _count_non_null(http, "document_version_id"),
        "cache_hits": _safe_sum(http, "cache_hit"),
    }


def latest_formal_counts(output: Path) -> dict[str, int | str | None]:
    runs = sorted(
        (path for path in (output / "production_runs").glob("*/STATE.json")),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    for state_path in runs:
        state = read_json(state_path)
        if state.get("stage") == "FORMAL_IMPORT" and isinstance(state.get("rows"), dict):
            rows = state["rows"]
            return {
                "run_id": state_path.parent.name,
                "documents": _int(rows.get("documents")),
                "actions": _int(rows.get("actions")),
                "parameters": _int(rows.get("parameters")),
                "gaps": _int(rows.get("gaps")),
            }
    return {"run_id": None, "documents": 0, "actions": 0, "parameters": 0, "gaps": 0}


def _attachment_counts(progress: dict[str, Any]) -> tuple[int, int]:
    status = progress.get("attachment_status")
    if isinstance(status, dict):
        found = _int(status.get("attachments_found"))
        resolved = _int(status.get("attachments_archived")) + _int(status.get("already_present"))
        return found, resolved
    return _int(progress.get("pdfs_found")), _int(progress.get("pdfs_archived"))


def discovery_progress(output: Path, provenance: pl.DataFrame, queue_total: int) -> dict[str, Any]:
    """Calculate discovery credit without treating raw completion as discovery."""

    credit_classes = {
        "LIVE_SEARCH_AND_FETCH",
        "LIVE_SEARCH_NO_NEW_URL",
        "LIVE_SEARCH_CACHE_REUSE",
        "LOCAL_DB_REUSE",
        "CACHE_ONLY",
    }
    provenance_verified = 0
    false_candidates = 0
    if not provenance.is_empty() and "provenance_class" in provenance.columns:
        classes = provenance.get_column("provenance_class")
        provenance_verified = _int(classes.is_in(sorted(credit_classes)).sum())
        false_candidates = _int((classes == "SEARCH_NOT_EXECUTED").sum())
    eligibility = read_table(output / "930_FALSE_COMPLETION_ELIGIBILITY_AUDIT.parquet")
    legitimately_exempted = (
        _int(eligibility.get_column("discovery_credit").fill_null(False).sum())
        if not eligibility.is_empty() and "discovery_credit" in eligibility.columns
        else 0
    )
    recovery = read_table(output / "930_FALSE_COMPLETION_RECOVERY_QUEUE.parquet")
    if not recovery.is_empty() and "status" in recovery.columns:
        statuses = recovery.get_column("status").cast(pl.String)
        recovery_required = _int(
            statuses.is_in(["RECOVERY_REQUIRED", "PENDING", "IN_PROGRESS", "RUNNING", "RETRY_WAIT"]).sum()
        )
        recovery_completed = _int(
            statuses.is_in(["COMPLETED", "RECOVERED", "RECOVERY_COMPLETED"]).sum()
        )
    else:
        recovery_required = false_candidates
        recovery_completed = 0
    credit_completed = provenance_verified + legitimately_exempted + recovery_completed
    return {
        "raw_queue_completed": 0,
        "provenance_verified_completed": provenance_verified,
        "legitimately_exempted_completed": legitimately_exempted,
        "discovery_credit_completed": credit_completed,
        "false_completion_candidates": false_candidates,
        "false_completion_recovery_required": recovery_required,
        "recovery_completed": recovery_completed,
        "scope_total": queue_total,
        "progress_percent": round(100 * credit_completed / queue_total, 2) if queue_total else None,
        "progress_basis": "PROVENANCE_VERIFIED_OR_LEGITIMATELY_EXEMPTED",
    }


def analysis_ready_discovery_progress(
    output: Path,
    provenance: pl.DataFrame,
) -> dict[str, Any]:
    scope = read_json(output / "930_ANALYSIS_READY_SCOPE.json")
    scope_ids = {str(value) for value in (scope.get("queue_item_ids") or [])}
    if not scope_ids:
        return {
            "scope_status": "FROZEN_SCOPE_MISSING",
            "core_eligible_total": 0,
            "core_verified": 0,
            "core_recovery_required": 0,
            "core_coverage_percent": None,
            "scope_version": scope.get("scope_version"),
            "scope_hash": scope.get("scope_hash"),
        }
    credit_classes = {
        "LIVE_SEARCH_AND_FETCH",
        "LIVE_SEARCH_NO_NEW_URL",
        "LIVE_SEARCH_CACHE_REUSE",
        "LOCAL_DB_REUSE",
        "CACHE_ONLY",
    }
    verified_ids: set[str] = set()
    if not provenance.is_empty() and {"queue_item_id", "provenance_class"} <= set(provenance.columns):
        verified_ids = {
            str(row.get("queue_item_id"))
            for row in provenance.iter_rows(named=True)
            if str(row.get("queue_item_id")) in scope_ids
            and str(row.get("provenance_class")) in credit_classes
        }
    eligibility = read_table(output / "930_FALSE_COMPLETION_ELIGIBILITY_AUDIT.parquet")
    if not eligibility.is_empty() and {"queue_item_id", "discovery_credit"} <= set(eligibility.columns):
        verified_ids.update(
            str(row.get("queue_item_id"))
            for row in eligibility.iter_rows(named=True)
            if str(row.get("queue_item_id")) in scope_ids
            and _truthy(row.get("discovery_credit"))
        )
    recovery = read_table(output / "930_FALSE_COMPLETION_RECOVERY_QUEUE.parquet")
    recovery_required_ids: set[str] = set()
    recovery_completed_ids: set[str] = set()
    if not recovery.is_empty() and {"queue_item_id", "status"} <= set(recovery.columns):
        for row in recovery.iter_rows(named=True):
            queue_item_id = str(row.get("queue_item_id") or "")
            if queue_item_id not in scope_ids:
                continue
            status = _upper(row.get("status"))
            if status in {"COMPLETED", "RECOVERED", "RECOVERY_COMPLETED"}:
                recovery_completed_ids.add(queue_item_id)
            elif status in {"RECOVERY_REQUIRED", "PENDING", "IN_PROGRESS", "RUNNING", "RETRY_WAIT"}:
                recovery_required_ids.add(queue_item_id)
    verified_ids.update(recovery_completed_ids)
    total = len(scope_ids)
    return {
        "scope_status": "FROZEN",
        "core_eligible_total": total,
        "core_verified": len(verified_ids),
        "core_recovery_required": len(recovery_required_ids),
        "core_coverage_percent": round(100 * len(verified_ids) / total, 2) if total else None,
        "scope_version": scope.get("scope_version"),
        "scope_hash": scope.get("scope_hash"),
    }


def recovery_claim_metrics(
    output: Path,
    analysis_discovery: dict[str, Any],
) -> dict[str, Any]:
    """Expose deterministic hotfix claim evidence without scheduling work."""

    audit = read_table(output / "930_RECOVERY_CLAIM_AUDIT.parquet")
    recent_claims: list[dict[str, Any]] = []
    if not audit.is_empty():
        if "claimed_at" in audit.columns:
            audit = audit.sort("claimed_at", nulls_last=True)
        recent = audit.tail(10)
        for row in recent.iter_rows(named=True):
            recent_claims.append(
                {
                    key: row.get(key)
                    for key in (
                        "task_id",
                        "recovery_id",
                        "normalized_priority",
                        "priority_reason",
                        "core_scope_member",
                        "critical_gap_member",
                        "work_source",
                        "claimed_at",
                        "worker_pid",
                        "worker_generation",
                    )
                    if key in row
                }
            )
    recent_priority_counts = {str(priority): 0 for priority in range(3)}
    for row in recent_claims:
        priority_value = row.get("normalized_priority")
        priority = "" if priority_value is None else str(priority_value)
        if priority in recent_priority_counts:
            recent_priority_counts[priority] += 1
    recent_work_source_counts: dict[str, int] = {}
    for row in recent_claims:
        source = str(row.get("work_source") or "UNKNOWN")
        recent_work_source_counts[source] = recent_work_source_counts.get(source, 0) + 1

    scope = read_json(output / "930_ANALYSIS_READY_SCOPE.json")
    scope_ids = {str(value) for value in (scope.get("queue_item_ids") or [])}
    recovery = read_table(output / "930_FALSE_COMPLETION_RECOVERY_QUEUE.parquet")
    core_running = 0
    core_required = 0
    if not recovery.is_empty() and {"queue_item_id", "status"}.issubset(recovery.columns):
        core_rows = recovery.filter(pl.col("queue_item_id").cast(pl.String).is_in(sorted(scope_ids)))
        statuses = core_rows.get_column("status").cast(pl.String) if not core_rows.is_empty() else pl.Series([], dtype=pl.String)
        core_running = _int((statuses == "RUNNING").sum())
        core_required = _int((statuses == "RECOVERY_REQUIRED").sum())
    return {
        "audit_path": str(output / "930_RECOVERY_CLAIM_AUDIT.parquet"),
        "total_claims": audit.height,
        "recent_claims": recent_claims,
        "recent_10_priority_counts": recent_priority_counts,
        "recent_10_work_source_counts": recent_work_source_counts,
        "core_verified": _int(analysis_discovery.get("core_verified")),
        "core_running": core_running,
        "core_required": core_required,
        "core_coverage": analysis_discovery.get("core_coverage_percent"),
        "hotfix_version": "ONE-SHOT_RECOVERY_SCHEDULER_HOTFIX_V1",
    }


def _stage(completed: int, total: int, *, label: str) -> dict[str, Any]:
    completed = max(0, completed)
    total = max(0, total)
    percent = None if total == 0 else round(min(1.0, completed / total) * 100, 2)
    return {
        "completed": completed,
        "total": total,
        "percent": percent,
        "denominator": label,
        "raw_status": "CALIBRATING" if total == 0 else "COMPLETE" if completed >= total else "IN_PROGRESS",
        "progress_scope": "CURRENT_BATCH_PROGRESS",
    }


def no_api_success_for_15m(success_age_seconds: float | None) -> bool:
    """Health-only age boundary; this function never schedules an API call."""

    return success_age_seconds is None or float(success_age_seconds) >= 900.0


def gap_impact(gaps: pl.DataFrame) -> dict[str, int]:
    """Summarize one authoritative gap frame without inventing affected rows."""

    if gaps.is_empty():
        return {
            "blocking_gap_count": 0,
            "critical_severity_gap_count": 0,
            "affected_document_count": 0,
            "affected_action_count": 0,
            "affected_city_count": 0,
            "non_document_action_gap_count": 0,
        }

    def unique_nonempty(column: str) -> int:
        if column not in gaps.columns:
            return 0
        return len(
            {
                str(value).strip()
                for value in gaps.get_column(column).drop_nulls().to_list()
                if str(value).strip()
            }
        )

    has_document = (
        gaps.get_column("document_id").is_not_null()
        if "document_id" in gaps.columns
        else pl.Series([False] * gaps.height)
    )
    has_action = (
        gaps.get_column("action_id").is_not_null()
        if "action_id" in gaps.columns
        else pl.Series([False] * gaps.height)
    )
    critical = 0
    if "severity" in gaps.columns:
        critical = int(
            gaps.get_column("severity")
            .cast(pl.String, strict=False)
            .str.to_uppercase()
            .eq("HIGH")
            .sum()
        )
    return {
        "blocking_gap_count": gaps.height,
        "critical_severity_gap_count": critical,
        "affected_document_count": unique_nonempty("document_id"),
        "affected_action_count": unique_nonempty("action_id"),
        "affected_city_count": unique_nonempty("city_id") or unique_nonempty("city"),
        "non_document_action_gap_count": int((~has_document & ~has_action).sum()),
    }


def _parse_scope_date(value: Any) -> date | None:
    """Parse a scope/date value without making a missing date eligible."""

    if value in (None, ""):
        return None
    try:
        return date.fromisoformat(str(value)[:10])
    except (TypeError, ValueError):
        return None


def analysis_ready_scope_entities(output: Path) -> dict[str, Any]:
    """Return the immutable frozen core entities used by readiness metrics.

    The scope is authoritative for city and window membership.  Curated
    document/action tables only supply entities that have actually reached the
    data layer; this keeps a global document or gap from receiving core credit
    merely because it shares a city.
    """

    scope = read_json(output / "930_ANALYSIS_READY_SCOPE.json")
    core_city_ids = {
        str(value)
        for value in (scope.get("city_ids") or [])
        if value not in (None, "")
    }
    core_city_names = {
        str(value)
        for value in (scope.get("cities") or [])
        if value not in (None, "")
    }
    window = scope.get("episode_window") or []
    window_start = _parse_scope_date(window[0]) if len(window) > 0 else None
    window_end = _parse_scope_date(window[1]) if len(window) > 1 else None
    data_root = output.parents[2] if len(output.parents) >= 3 else output.parent
    curated = data_root / "curated"
    documents = read_table(curated / "policy_episode_documents.parquet")
    if not documents.is_empty() and "episode_id" in documents.columns:
        documents = documents.filter(pl.col("episode_id") == EPISODE_ID)

    def city_in_scope(row: dict[str, Any]) -> bool:
        city_id = str(row.get("city_id") or "")
        city = str(row.get("city") or "")
        if city_id:
            return city_id in core_city_ids
        return city in core_city_names

    core_document_rows: list[dict[str, Any]] = []
    for row in documents.iter_rows(named=True) if not documents.is_empty() else []:
        if "is_formal_eligible" in documents.columns and not _truthy(row.get("is_formal_eligible")):
            continue
        event_date = _parse_scope_date(row.get("announcement_date") or row.get("publication_date"))
        if not city_in_scope(row):
            continue
        if window_start is not None and (event_date is None or event_date < window_start):
            continue
        if window_end is not None and (event_date is None or event_date > window_end):
            continue
        core_document_rows.append(row)
    core_documents = (
        pl.DataFrame(core_document_rows, infer_schema_length=None)
        if core_document_rows
        else documents.head(0)
    )
    core_document_ids = {
        str(value)
        for value in core_documents.get_column("document_id").drop_nulls().to_list()
    } if not core_documents.is_empty() and "document_id" in core_documents.columns else set()

    actions = read_table(curated / "policy_episode_actions.parquet")
    if not actions.is_empty() and "episode_id" in actions.columns:
        actions = actions.filter(pl.col("episode_id") == EPISODE_ID)
    core_actions = (
        actions.filter(pl.col("document_id").cast(pl.String).is_in(sorted(core_document_ids)))
        if core_document_ids and not actions.is_empty() and "document_id" in actions.columns
        else actions.head(0)
    )
    core_action_ids = {
        str(value)
        for value in core_actions.get_column("action_id").drop_nulls().to_list()
    } if not core_actions.is_empty() and "action_id" in core_actions.columns else set()
    parameters = read_table(curated / "policy_episode_parameters.parquet")
    if not parameters.is_empty() and "episode_id" in parameters.columns:
        parameters = parameters.filter(pl.col("episode_id") == EPISODE_ID)
    core_parameters = (
        parameters.filter(pl.col("action_id").cast(pl.String).is_in(sorted(core_action_ids)))
        if core_action_ids and not parameters.is_empty() and "action_id" in parameters.columns
        else parameters.head(0)
    )
    return {
        "scope_version": scope.get("scope_version"),
        "scope_hash": scope.get("scope_hash"),
        "queue_item_ids": {
            str(value)
            for value in (scope.get("queue_item_ids") or [])
            if value not in (None, "")
        },
        "core_city_ids": core_city_ids,
        "core_city_names": core_city_names,
        "window_start": window_start,
        "window_end": window_end,
        "core_documents": core_documents,
        "core_actions": core_actions,
        "core_parameters": core_parameters,
        "core_document_ids": core_document_ids,
        "core_action_ids": core_action_ids,
    }


def split_gap_metrics(
    gaps: pl.DataFrame,
    *,
    core_document_ids: set[str],
    core_action_ids: set[str],
    core_city_ids: set[str],
    core_city_names: set[str],
) -> dict[str, Any]:
    """Split one global gap register into global and frozen-core metrics.

    Entity-linked rows must match the frozen core entity.  Rows without an
    entity are scoped by city because they represent city/policy-tool gaps.
    The returned ``core_gap_frame`` is for callers/tests and is never written
    as a replacement for the global register.
    """

    core_documents = {str(value) for value in core_document_ids if value}
    core_actions = {str(value) for value in core_action_ids if value}
    core_cities = {str(value) for value in core_city_ids if value}
    core_names = {str(value) for value in core_city_names if value}
    flags: list[bool] = []
    for row in gaps.iter_rows(named=True) if not gaps.is_empty() else []:
        document_id = str(row.get("document_id") or "").strip()
        action_id = str(row.get("action_id") or "").strip()
        if document_id or action_id:
            flags.append(document_id in core_documents or action_id in core_actions)
            continue
        city_id = str(row.get("city_id") or "").strip()
        city = str(row.get("city") or "").strip()
        flags.append(city_id in core_cities or city in core_names)
    core_frame = gaps.filter(pl.Series("_analysis_ready_core", flags)) if flags else gaps.head(0)
    return {
        "analysis_ready_core_blocking_gaps": gap_impact(core_frame),
        "global_final_blocking_gaps": gap_impact(gaps),
        "core_gap_frame": core_frame,
    }


def timeout_fingerprint(
    failures: pl.DataFrame,
    *,
    configured_read_timeout: float | None = None,
    recent_limit: int = 20,
) -> dict[str, Any]:
    """Detect configured-limit or SDK retry-chain read timeouts.

    ``None`` is intentional when the audit lacks enough timeout samples or a
    configured timeout.  A duration is never treated as proof of a timeout
    configuration by itself.  When persisted failure rows contain
    ``max_retries``, a repeated duration near ``read_timeout * (max_retries +
    1)`` is separately recorded as an SDK retry-chain fingerprint.  This is
    the narrow evidence gate used by the recovery controller to make the next
    SINGLE probe one transport attempt with its existing extended timeout
    policy; ordinary API calls remain unchanged.
    """

    if failures.is_empty():
        return {"CLIENT_READ_TIMEOUT_SUSPECTED": None, "sample_count": 0, "samples": [], "reason_code": "NO_FAILURE_SAMPLES"}
    frame = failures
    sort_column = "created_at" if "created_at" in frame.columns else "updated_at" if "updated_at" in frame.columns else None
    if sort_column:
        frame = frame.sort(sort_column, nulls_last=True).tail(max(1, int(recent_limit)))
    samples: list[dict[str, Any]] = []
    for row in frame.iter_rows(named=True):
        if _upper(row.get("failure_class")) != "READ_TIMEOUT":
            continue
        configured = row.get("configured_read_timeout") or row.get("read_timeout") or configured_read_timeout
        duration = row.get("duration_seconds")
        if duration in (None, "") and row.get("latency_ms") not in (None, ""):
            try:
                duration = float(row.get("latency_ms")) / 1000.0
            except (TypeError, ValueError):
                duration = None
        try:
            configured = float(configured) if configured not in (None, "") else None
        except (TypeError, ValueError):
            configured = None
        try:
            duration = float(duration) if duration not in (None, "") else None
        except (TypeError, ValueError):
            duration = None
        configured_max_retries = None
        try:
            if row.get("max_retries") not in (None, ""):
                configured_max_retries = int(float(row.get("max_retries")))
        except (TypeError, ValueError):
            configured_max_retries = None
        samples.append({
            "configured_read_timeout": configured,
            "duration_seconds": round(duration, 3) if duration is not None else None,
            "configured_max_retries": configured_max_retries,
            "failure_class": "READ_TIMEOUT",
        })
    known = [sample for sample in samples if sample["configured_read_timeout"] is not None and sample["duration_seconds"] is not None]
    if len(known) < 2:
        return {
            "CLIENT_READ_TIMEOUT_SUSPECTED": None,
            "sample_count": len(samples),
            "known_sample_count": len(known),
            "samples": samples,
            "reason_code": "INSUFFICIENT_CONFIGURED_TIMEOUT_SAMPLES",
        }
    concentrated = all(
        abs(sample["duration_seconds"] - sample["configured_read_timeout"])
        <= max(15.0, sample["configured_read_timeout"] * 0.10)
        for sample in known
    )
    retry_chain_matches = [
        sample
        for sample in known
        if sample.get("configured_max_retries") is not None
        and sample["configured_max_retries"] > 0
        and abs(
            sample["duration_seconds"]
            - sample["configured_read_timeout"] * (sample["configured_max_retries"] + 1)
        )
        <= max(
            15.0,
            sample["configured_read_timeout"]
            * (sample["configured_max_retries"] + 1)
            * 0.10,
        )
    ]
    sdk_retry_chain_suspected = len(retry_chain_matches) >= 2
    if sdk_retry_chain_suspected:
        reason_code = "SDK_RETRY_CHAIN_SUSPECTED"
    elif concentrated:
        reason_code = "READ_TIMEOUT_NEAR_CONFIGURED_LIMIT"
    else:
        reason_code = "READ_TIMEOUT_NOT_CONCENTRATED"
    return {
        "CLIENT_READ_TIMEOUT_SUSPECTED": bool(concentrated or sdk_retry_chain_suspected),
        "SDK_RETRY_CHAIN_SUSPECTED": sdk_retry_chain_suspected,
        "SDK_RETRY_CHAIN_MATCH_COUNT": len(retry_chain_matches),
        "sample_count": len(samples),
        "known_sample_count": len(known),
        "samples": samples,
        "reason_code": reason_code,
    }


def action_extraction_readiness(
    *,
    eligible_document_ids: set[str],
    completed_document_ids: set[str],
    analysis_scope_document_ids: set[str],
    excluded_with_reason: dict[str, str] | None = None,
) -> dict[str, Any]:
    eligible = {str(value) for value in eligible_document_ids if value}
    completed = {str(value) for value in completed_document_ids if value}
    core = eligible.intersection(
        {str(value) for value in analysis_scope_document_ids if value}
    )

    def metrics(scope: set[str]) -> dict[str, Any]:
        done = len(scope.intersection(completed))
        total = len(scope)
        return {
            "eligible_total": total,
            "completed": done,
            "remaining": max(0, total - done),
            "percent": None if total == 0 else round(100 * done / total, 2),
            "gate": "PASS" if total > 0 and done == total else "FAIL",
        }

    return {
        "global": metrics(eligible),
        "analysis_ready": metrics(core),
        "excluded_with_reason": dict(excluded_with_reason or {}),
    }


def _current_gap_frame(output: Path, progress: dict[str, Any]) -> tuple[pl.DataFrame, str | None]:
    run_id = str(progress.get("run_id") or "")
    candidates: list[Path] = []
    if run_id:
        run_dir = output / "production_runs" / run_id / "03_GAP_AUDIT"
        candidates.extend(
            [
                run_dir / "2016_930_GAP_REGISTER.parquet",
                run_dir / "2016_930_GAP_AUDIT_PASS_2.parquet",
            ]
        )
    candidates.extend(
        sorted(
            (output / "production_runs").glob(
                "*/03_GAP_AUDIT/2016_930_GAP_REGISTER.parquet"
            ),
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )
    )
    for path in candidates:
        frame = read_table(path)
        if not frame.is_empty():
            return frame, str(path)
    return pl.DataFrame(), None


def _global_gap_frame(output: Path) -> tuple[pl.DataFrame, str | None]:
    """Read the complete curated gap register for final-scope metrics."""

    data_root = output.parents[2] if len(output.parents) >= 3 else output.parent
    path = data_root / "curated" / "policy_episode_gaps.parquet"
    if not path.exists():
        return pl.DataFrame(), None
    frame = read_table(path)
    if not frame.is_empty() and "episode_id" in frame.columns:
        frame = frame.filter(pl.col("episode_id") == EPISODE_ID)
    return frame, str(path)


def _current_action_extraction(
    output: Path,
    progress: dict[str, Any],
    *,
    scope_entities: dict[str, Any] | None = None,
) -> dict[str, Any]:
    run_id = str(progress.get("run_id") or "")
    coverage_paths = sorted(
        (output / "production_runs").glob(
            "*/04_ACTION_EXTRACTION/2016_930_ACTION_EXTRACTION_COVERAGE.parquet"
        )
    )
    coverage_frames = [read_table(path) for path in coverage_paths]
    coverage_frames = [frame for frame in coverage_frames if not frame.is_empty()]
    coverage = (
        pl.concat(coverage_frames, how="diagonal_relaxed")
        .unique(subset=["document_id"], keep="last")
        if coverage_frames
        else pl.DataFrame()
    )
    scope_entities = scope_entities or analysis_ready_scope_entities(output)
    core_city_ids = scope_entities.get("core_city_ids") or set()
    core_city_names = scope_entities.get("core_city_names") or set()
    frozen_core_ids = scope_entities.get("core_document_ids") or set()
    if not coverage.is_empty() and "document_id" in coverage.columns:
        eligible_rows = (
            coverage.filter(pl.col("eligible").fill_null(False))
            if "eligible" in coverage.columns
            else coverage
        )
        coverage_eligible_ids = {
            str(value)
            for value in eligible_rows.get_column("document_id").drop_nulls().to_list()
        }
        completed_ids = {
            str(value)
            for value in eligible_rows.filter(pl.col("status") == "COMPLETED")
            .get_column("document_id")
            .drop_nulls()
            .to_list()
        }
        coverage_core_ids = {
            str(row.get("document_id"))
            for row in eligible_rows.iter_rows(named=True)
            if (
                str(row.get("city_id") or "") in core_city_ids
                if row.get("city_id") not in (None, "")
                else str(row.get("city") or "") in core_city_names
            )
            and row.get("document_id")
        }
        curated_documents = read_table(
            output.parents[2] / "curated" / "policy_episode_documents.parquet"
        )
        if not curated_documents.is_empty() and "episode_id" in curated_documents.columns:
            curated_documents = curated_documents.filter(pl.col("episode_id") == EPISODE_ID)
        if not curated_documents.is_empty() and "document_id" in curated_documents.columns:
            if "is_formal_eligible" in curated_documents.columns:
                curated_documents = curated_documents.filter(
                    pl.col("is_formal_eligible").fill_null(False)
                )
            eligible_ids = {
                str(value)
                for value in curated_documents.get_column("document_id").drop_nulls().to_list()
            }
            core_ids = set(frozen_core_ids)
            if not core_ids:
                core_ids = {
                    str(row.get("document_id"))
                    for row in curated_documents.iter_rows(named=True)
                    if (
                        str(row.get("city_id") or "") in core_city_ids
                        if row.get("city_id") not in (None, "")
                        else str(row.get("city") or "") in core_city_names
                    )
                    and row.get("document_id")
                }
        else:
            eligible_ids = coverage_eligible_ids
            core_ids = coverage_core_ids
        excluded = {
            str(row.get("document_id")): str(row.get("excluded_reason"))
            for row in eligible_rows.iter_rows(named=True)
            if row.get("document_id") and row.get("excluded_reason")
        }
        result = action_extraction_readiness(
            eligible_document_ids=eligible_ids,
            completed_document_ids=completed_ids,
            analysis_scope_document_ids=core_ids,
            excluded_with_reason=excluded,
        )
        result["source"] = [str(path) for path in coverage_paths]
        return result

    action_path = (
        output
        / "production_runs"
        / run_id
        / "04_ACTION_EXTRACTION"
        / "2016_930_ACTIONS.parquet"
    )
    actions = read_table(action_path)
    completed = (
        int(actions.get_column("document_id").drop_nulls().n_unique())
        if not actions.is_empty() and "document_id" in actions.columns
        else 0
    )
    total = max(0, _int(progress.get("official_documents") or progress.get("documents_found")))
    return {
        "global": {
            "eligible_total": total,
            "completed": min(completed, total),
            "remaining": max(0, total - completed),
            "percent": None if total == 0 else round(100 * min(completed, total) / total, 2),
            "gate": "PASS" if total > 0 and completed >= total else "FAIL",
        },
        "analysis_ready": {
            "eligible_total": 0,
            "completed": 0,
            "remaining": 0,
            "percent": None,
            "gate": "FAIL",
        },
        "excluded_with_reason": {},
        "source": str(action_path) if action_path.exists() else None,
    }


def stage_progress(progress: dict[str, Any], crawl: dict[str, int], formal: dict[str, Any], output: Path) -> dict[str, dict[str, Any]]:
    docs = _int(progress.get("documents_found"))
    actions = _int(progress.get("actions_extracted"))
    extraction = _current_action_extraction(output, progress)
    extraction_global = extraction.get("global") or {}
    api_docs = max(docs, _int(crawl.get("document_versions")), _int(progress.get("official_documents")))
    attachments, attachments_resolved = _attachment_counts(progress)
    queue_total = _int(progress.get("queue_total"))
    search_items = _int(crawl.get("search_calls"))
    export_exists = (output / "2016_930_FINAL_EXPORT.csv").exists()
    gap_frame, _gap_source = _current_gap_frame(output, progress)
    gap_count = gap_impact(gap_frame)["blocking_gap_count"] if not gap_frame.is_empty() else _int(progress.get("gaps_remaining"))
    return {
        "discovery": _stage(search_items, queue_total, label="queue-scoped search items / queue total"),
        "official_recovery": _stage(_int(progress.get("official_documents")), docs, label="official documents / documents"),
        "action_extraction": _stage(
            _int(extraction_global.get("completed")),
            _int(extraction_global.get("eligible_total")),
            label="documents with deterministic actions / eligible documents",
        ),
        "api_pass1": _stage(_int(progress.get("api_pass1_success")), api_docs, label="Pass1 documents / cached DocumentVersions"),
        "api_pass2": _stage(_int(progress.get("api_pass2_success")), api_docs, label="Pass2 documents / cached DocumentVersions"),
        "date_verification": _stage(_int(progress.get("dates_verified")), actions, label="actions with verified date state / actions"),
        "parameter_extraction": _stage(_int(progress.get("parameters_extracted")), actions, label="parameterized actions / actions"),
        "attachment": _stage(attachments_resolved, attachments, label="resolved attachments / known attachments"),
        "dedup_quality": _stage(_int(formal.get("documents")), _int(formal.get("documents")), label="deduplicated formal documents / formal documents"),
        "formal_promotion": _stage(_int(progress.get("formal_actions_promoted")), actions, label="promoted actions / extracted actions"),
        "gap_audit": _stage(1 if gap_count == 0 and docs else 0, 1, label="critical gap audit clear / audit run"),
        "export_dashboard": _stage(1 if export_exists else 0, 1, label="action export present / export gate"),
    }


def _parse_time(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _history_frame(path: Path) -> pl.DataFrame:
    frame = read_table(path)
    if frame.is_empty():
        return pl.DataFrame(schema=HISTORY_SCHEMA)
    for name, dtype in HISTORY_SCHEMA.items():
        if name not in frame.columns:
            frame = frame.with_columns(pl.lit(None).cast(dtype).alias(name))
    return frame.select(list(HISTORY_SCHEMA))


def append_history(output: Path, row: dict[str, Any], *, min_seconds: int = 30) -> pl.DataFrame:
    path = output / "930_PROGRESS_HISTORY.parquet"
    history = _history_frame(path)
    timestamp = _parse_time(row.get("snapshot_at")) or now_utc()
    if not history.is_empty():
        last = _parse_time(history.get_column("snapshot_at")[-1])
        if last and (timestamp - last).total_seconds() < min_seconds:
            return history
    incoming = pl.DataFrame([row], schema=HISTORY_SCHEMA)
    merged = pl.concat([history, incoming], how="vertical_relaxed") if not history.is_empty() else incoming
    merged = merged.unique(subset=["snapshot_at"], keep="last", maintain_order=True)
    atomic_write_parquet(merged, path, {"module": "episode_930_monitor", "artifact": "progress_history"}, key_columns=("snapshot_at",))
    return merged


def _rates(history: pl.DataFrame, counters: list[str]) -> dict[str, float | None]:
    if history.height < 5 or "snapshot_at" not in history.columns:
        return {name: None for name in counters}
    timestamps = [_parse_time(value) for value in history.get_column("snapshot_at").to_list()]
    valid = [value for value in timestamps if value]
    if len(valid) < 5 or (max(valid) - min(valid)).total_seconds() < 300:
        return {name: None for name in counters}
    result: dict[str, float | None] = {}
    recent = history.tail(min(20, history.height))
    for name in counters:
        if name not in recent.columns:
            result[name] = None
            continue
        values = recent.get_column(name).cast(pl.Float64, strict=False).fill_null(0).to_list()
        deltas = [
            max(0.0, float(b) - float(a))
            for a, b in zip(values, values[1:], strict=False)
        ]
        if not any(deltas):
            result[name] = None
            continue
        hours = max((valid[-1] - valid[0]).total_seconds() / 3600, 1e-9)
        overall = max(0.0, float(values[-1]) - float(values[0])) / hours
        positive = [value for value in deltas if value > 0]
        result[name] = round(0.7 * overall + 0.3 * (statistics.mean(positive) / max(hours / max(len(deltas), 1), 1e-9)), 2)
    return result


def _eta(remaining: int, rate: float | None, *, blocked: bool = False) -> str:
    if remaining <= 0:
        return "COMPLETE"
    if blocked:
        return "BLOCKED_PROVIDER"
    if not rate or rate <= 0:
        return "CALIBRATING"
    return (now_utc() + timedelta(hours=remaining / rate)).isoformat()


def _stage_eta(stage: dict[str, Any], rate: float | None, *, blocked: bool = False) -> str:
    """Return an ETA only when the stage has a meaningful denominator."""

    total = _int(stage.get("total"))
    if total <= 0:
        return "CALIBRATING"
    if blocked and _int(stage.get("completed")) < total:
        return "BLOCKED_BY_API"
    return _eta(max(0, total - _int(stage.get("completed"))), rate, blocked=blocked)


def _latest_time(values: list[Any]) -> datetime | None:
    parsed = [_parse_time(value) for value in values]
    parsed = [value for value in parsed if value is not None]
    return max(parsed) if parsed else None


def _api_classification_frame(output: Path) -> pl.DataFrame:
    frames = [
        read_table(path)
        for path in sorted(
            (output / "production_runs").glob(
                "*/05_API_CLASSIFICATION/2016_930_API_CLASSIFICATION.parquet"
            )
        )
    ]
    frames = [frame for frame in frames if not frame.is_empty()]
    if not frames:
        return pl.DataFrame()
    merged = pl.concat(frames, how="diagonal_relaxed")
    keys = [name for name in ("request_id", "action_id", "pass_name") if name in merged.columns]
    return merged.unique(subset=keys, keep="last") if keys else merged


def _core_api_backlog(
    core_document_ids: set[str],
    api_rows: pl.DataFrame,
) -> dict[str, int]:
    """Count API readiness only for frozen-core documents."""

    eligible = {str(value) for value in core_document_ids if value}
    pass1: set[str] = set()
    pass2: set[str] = set()
    if not api_rows.is_empty() and {"document_id", "pass_name"}.issubset(api_rows.columns):
        for row in api_rows.iter_rows(named=True):
            document_id = str(row.get("document_id") or "")
            if document_id not in eligible:
                continue
            if str(row.get("pass_name") or "") == "first_pass":
                pass1.add(document_id)
            elif str(row.get("pass_name") or "") == "second_review":
                pass2.add(document_id)
    pass1.intersection_update(eligible)
    pass2.intersection_update(pass1)
    return {
        "core_pass1_eligible": len(eligible),
        "core_pass1_waiting": max(0, len(eligible) - len(pass1)),
        "core_pass1_success": len(pass1),
        "core_pass2_not_eligible": max(0, len(eligible) - len(pass1)),
        "core_pass2_eligible": len(pass1),
        "core_pass2_waiting": max(0, len(pass1) - len(pass2)),
        "core_pass2_success": len(pass2),
    }


def api_health(
    output: Path,
    progress: dict[str, Any],
    crawl: dict[str, int],
    *,
    core_document_ids: set[str] | None = None,
    api_rows: pl.DataFrame | None = None,
) -> dict[str, Any]:
    """Describe API recovery state without making a provider call.

    The production runner owns the 1 -> 5 -> 20 cached-document probe.  The
    monitor only reads its recovery receipt and marks a prolonged no-success
    interval as stalled so that a heartbeat cannot masquerade as progress.
    """

    provider = read_json(output / "930_API_PROVIDER_STATUS.json")
    recovery = read_json(output / "930_API_RECOVERY_STATE.json")
    failures = read_table(output / "930_API_FAILURES.parquet")
    latest_failure: dict[str, Any] = {}
    if not failures.is_empty():
        if "created_at" in failures.columns:
            failures = failures.sort("created_at", nulls_last=True)
        latest_failure = failures.tail(1).to_dicts()[0]
    timeout_configured = recovery.get("configured_read_timeout")
    try:
        timeout_configured = float(timeout_configured) if timeout_configured not in (None, "") else None
    except (TypeError, ValueError):
        timeout_configured = None
    timeout_audit = timeout_fingerprint(
        failures,
        configured_read_timeout=timeout_configured,
    )
    api_rows = api_rows if api_rows is not None else _api_classification_frame(output)
    core_api = _core_api_backlog(core_document_ids or set(), api_rows)
    work_total = max(
        _int(progress.get("api_documents_total")),
        _int(progress.get("documents_found")),
        _int(progress.get("official_documents")),
        _int(crawl.get("document_versions")),
    )
    pass1 = _int(progress.get("api_pass1_success"))
    raw_pass2 = _int(progress.get("api_pass2_success"))
    pass1_waiting = max(0, work_total - pass1)
    pass2_eligible = min(work_total, pass1)
    pass2 = min(raw_pass2, pass2_eligible)
    orphan_pass2 = max(0, raw_pass2 - pass2)
    pass2_not_yet_eligible = max(0, work_total - pass2_eligible)
    pass2_waiting = max(0, pass2_eligible - pass2)
    provider_success_times: list[Any] = [
        provider.get("last_success_at"),
    ]
    if not failures.is_empty() and "recovered_at" in failures.columns:
        provider_success_times.extend(failures.get_column("recovered_at").drop_nulls().to_list())
    last_success = _latest_time(provider_success_times)
    recovery_last_success = _latest_time([recovery.get("last_success_at")])
    age_seconds = (now_utc() - last_success).total_seconds() if last_success else None
    recovery_success_age_seconds = (
        (now_utc() - recovery_last_success).total_seconds()
        if recovery_last_success
        else None
    )
    provider_status = _upper(provider.get("status") or progress.get("api_provider_status"))
    waiting = pass1_waiting > 0 or pass2_waiting > 0
    attempted = _int(recovery.get("last_attempted_documents"))
    successes = _int(recovery.get("last_success_documents"))
    success_rate = recovery.get("last_success_rate")
    if success_rate is None and attempted > 0:
        success_rate = round(successes / attempted, 4)
    try:
        success_rate = float(success_rate) if success_rate is not None else None
    except (TypeError, ValueError):
        success_rate = None
    phase = _upper(recovery.get("phase") or "SINGLE_PROBE")
    schema_valid = bool(recovery.get("schema_valid"))
    valid_probe_success = successes > 0 and schema_valid
    if phase in {"PROBE", "SINGLE_PROBE", "BACKOFF_SINGLE_PROBE"}:
        next_probe = "MICRO_5" if valid_probe_success else "BACKOFF_SINGLE_PROBE" if attempted > 0 else "SINGLE_PROBE"
    elif phase == "MICRO_5":
        next_probe = "MICRO_20" if schema_valid and success_rate is not None and success_rate >= 0.8 else "BACKOFF_SINGLE_PROBE"
    elif phase == "MICRO_20":
        next_probe = "STABLE_BACKLOG_CONSUMPTION" if schema_valid and success_rate is not None and success_rate >= 0.8 else "BACKOFF_SINGLE_PROBE"
    else:
        next_probe = "STABLE_BACKLOG_CONSUMPTION" if phase == "BACKLOG_CONSUMPTION" and schema_valid and success_rate is not None and success_rate >= 0.8 else "BACKOFF_SINGLE_PROBE"
    recovery_gate_blocked = waiting and (
        (phase in {"PROBE", "SINGLE_PROBE", "BACKOFF_SINGLE_PROBE"} and not valid_probe_success)
        or (phase in {"MICRO_5", "MICRO_20", "BACKLOG_CONSUMPTION"} and not (schema_valid and success_rate is not None and success_rate >= 0.8))
    )
    recovery_lane_missed_retry_window = (
        _upper(recovery.get("reason_code")) == "CACHE_REUSE_NOT_A_PROVIDER_PROBE"
        and attempted == 0
        and _int(recovery.get("provider_probe_attempted_documents")) == 0
        and _int(recovery.get("api_cache_hits")) > 0
    )
    no_success_for_15m = no_api_success_for_15m(age_seconds)
    if not waiting:
        health_status = "IDLE"
    elif no_success_for_15m:
        health_status = "STALLED"
    elif recovery_gate_blocked:
        health_status = "RECOVERING"
    elif provider_status in {"OPERATIONAL", "RECOVERED"}:
        health_status = "OPERATIONAL"
    elif provider_status in {"BLOCKED", "DISABLED", "UNAVAILABLE", "PERMANENT_FAILURE"}:
        health_status = "BLOCKED"
    else:
        health_status = "RECOVERING"
    return {
        "status": health_status,
        "provider_status": provider_status or "UNKNOWN",
        "pass1_success": pass1,
        "pass1_waiting": pass1_waiting,
        "pass2_success": pass2,
        "pass2_success_raw_observed": raw_pass2,
        "pass2_success_without_current_pass1_provenance": orphan_pass2,
        "pass2_not_yet_eligible": pass2_not_yet_eligible,
        "pass2_eligible": pass2_eligible,
        "pass2_waiting": pass2_waiting,
        "waiting_total": pass1_waiting + pass2_waiting,
        "unprocessed_total": pass1_waiting + pass2_waiting,
        "last_success_at": last_success.isoformat() if last_success else None,
        "last_success_age_seconds": round(age_seconds, 1) if age_seconds is not None else None,
        "recovery_last_success_at": recovery_last_success.isoformat() if recovery_last_success else None,
        "recovery_last_success_age_seconds": round(recovery_success_age_seconds, 1) if recovery_success_age_seconds is not None else None,
        "next_retry_at": recovery.get("next_retry_at"),
        "no_success_for_15m": bool(no_success_for_15m),
        "recovery_phase": phase,
        "schema_valid": schema_valid,
        "valid_probe_success": valid_probe_success,
        "recovery_gate_blocked": recovery_gate_blocked,
        "recovery_lane_missed_retry_window": recovery_lane_missed_retry_window,
        "probe_attempted_documents": attempted,
        "probe_success_documents": successes,
        "probe_success_rate": success_rate,
        "backlog_consumption_allowed": bool(
            phase == "BACKLOG_CONSUMPTION"
            and schema_valid
            and success_rate is not None
            and success_rate >= 0.8
        ),
        "recovery_gate": next_probe,
        "next_probe": next_probe,
        "probe_policy": "EXISTING_CACHED_DOCUMENT_VERSION_ONLY",
        "monitor_network_probe_executed": False,
        "latest_failure": {
            key: latest_failure.get(key)
            for key in (
                "failure_class", "transport_started", "dns_ok", "connect_ok",
                "http_status", "response_received", "response_bytes", "latency_ms",
                "timeout_type", "json_parse_ok", "schema_valid", "schema_errors",
                "provider_error_code", "provider_error_message_sanitized",
                "configured_read_timeout", "configured_connect_timeout",
            )
        },
        "timeout_fingerprint": timeout_audit,
        **core_api,
    }


def _combine_etas(values: dict[str, str]) -> str:
    """Combine independent dependency ETAs without treating unknown as complete."""

    if any(value == "BLOCKED_BY_API" for value in values.values()):
        return "BLOCKED_BY_API"
    blocked = [value for value in values.values() if value.startswith("BLOCKED_")]
    if blocked:
        return blocked[0]
    if any(value == "CALIBRATING" for value in values.values()):
        return "CALIBRATING"
    parsed = [_parse_time(value) for value in values.values()]
    parsed = [value for value in parsed if value is not None]
    return max(parsed).isoformat() if parsed else "CALIBRATING"


def build_monitor_snapshot(output: Path, *, write: bool = False) -> dict[str, Any]:
    progress = read_json(output / "930_PROGRESS_SNAPSHOT.json")
    queue = queue_counts(output, progress)
    queue_reconciliation = queue.get("queue_reconciliation") or {}
    progress = {**progress, **queue}
    crawl = audit_crawl_artifacts(output)
    formal = latest_formal_counts(output)
    stages = stage_progress(progress, crawl, formal, output)
    gap_frame, batch_gap_source = _current_gap_frame(output, progress)
    global_gap_frame, global_gap_source = _global_gap_frame(output)
    if global_gap_source is None:
        global_gap_frame = gap_frame
        global_gap_source = batch_gap_source
    gaps = gap_impact(global_gap_frame)
    if global_gap_source == batch_gap_source and global_gap_frame.is_empty() and _int(
        progress.get("gaps_remaining")
    ) > 0:
        gaps = {
            **gaps,
            "blocking_gap_count": _int(progress.get("gaps_remaining")),
            "non_document_action_gap_count": _int(progress.get("gaps_remaining")),
        }
    gap_source = global_gap_source
    scope_entities = analysis_ready_scope_entities(output)
    split_gaps = split_gap_metrics(
        global_gap_frame,
        core_document_ids=scope_entities.get("core_document_ids") or set(),
        core_action_ids=scope_entities.get("core_action_ids") or set(),
        core_city_ids=scope_entities.get("core_city_ids") or set(),
        core_city_names=scope_entities.get("core_city_names") or set(),
    )
    core_gaps = split_gaps["analysis_ready_core_blocking_gaps"]
    extraction_readiness = _current_action_extraction(
        output,
        progress,
        scope_entities=scope_entities,
    )
    rolling_core = read_json(output / "930_ANALYSIS_READY_ROLLING_METRICS.json")
    if not rolling_core:
        rolling_core = {
            "scope_version": scope_entities.get("scope_version"),
            "scope_hash": scope_entities.get("scope_hash"),
            "core_discovery_verified": _int(analysis_ready_discovery_progress(output, read_table(output / "930_COMPLETED_PROVENANCE_AUDIT.parquet")).get("core_verified")),
            "core_action_eligible": _int((extraction_readiness.get("analysis_ready") or {}).get("eligible_total")),
            "core_action_completed": _int((extraction_readiness.get("analysis_ready") or {}).get("completed")),
            "core_official_documents": len(scope_entities.get("core_document_ids") or set()),
            "core_date_resolved": 0,
            "core_parameters_processed": 0,
            "analysis_ready_core_blocking_gaps": core_gaps,
            "global_final_blocking_gaps": gaps,
            "updated_at": None,
        }
    history_path = output / "930_PROGRESS_HISTORY.parquet"
    history = _history_frame(history_path)
    attachments, attachments_resolved = _attachment_counts(progress)
    provenance_audit = read_table(output / "930_COMPLETED_PROVENANCE_AUDIT.parquet")
    provenance_pending = 0
    if not provenance_audit.is_empty() and "needs_recovery" in provenance_audit.columns:
        provenance_pending = _int(provenance_audit.get_column("needs_recovery").fill_null(False).sum())
    provenance_total = provenance_audit.height
    provenance_completed = max(0, provenance_total - provenance_pending)
    discovery = discovery_progress(output, provenance_audit, _int(progress.get("queue_total")))
    analysis_discovery = analysis_ready_discovery_progress(output, provenance_audit)
    rolling_core = {
        **rolling_core,
        "scope_version": scope_entities.get("scope_version"),
        "scope_hash": scope_entities.get("scope_hash"),
        "core_discovery_verified": _int(analysis_discovery.get("core_verified")),
        "core_discovery_total": _int(analysis_discovery.get("core_eligible_total")),
        "core_discovery_recovery_required": _int(analysis_discovery.get("core_recovery_required")),
        "core_action_eligible": _int((extraction_readiness.get("analysis_ready") or {}).get("eligible_total")),
        "core_action_completed": _int((extraction_readiness.get("analysis_ready") or {}).get("completed")),
        "core_official_documents": len(scope_entities.get("core_document_ids") or set()),
        "core_date_resolved": (
            scope_entities["core_actions"].filter(pl.col("effective_date").is_not_null())
            .get_column("action_id").n_unique()
            if isinstance(scope_entities.get("core_actions"), pl.DataFrame)
            and not scope_entities["core_actions"].is_empty()
            and {"effective_date", "action_id"}.issubset(scope_entities["core_actions"].columns)
            else 0
        ),
        "core_parameters_processed": (
            scope_entities["core_parameters"].get_column("action_id").drop_nulls().n_unique()
            if isinstance(scope_entities.get("core_parameters"), pl.DataFrame)
            and not scope_entities["core_parameters"].is_empty()
            and "action_id" in scope_entities["core_parameters"].columns
            else 0
        ),
        "analysis_ready_core_blocking_gaps": core_gaps,
        "global_final_blocking_gaps": gaps,
    }
    claim_metrics = recovery_claim_metrics(output, analysis_discovery)
    next_work_source = str(
        progress.get("work_source")
        or progress.get("next_work_source")
        or "UNKNOWN"
    )
    global_core_priority = progress.get("global_core_priority")
    discovery["raw_queue_completed"] = _int(progress.get("queue_completed"))
    stages["discovery"] = _stage(
        _int(discovery.get("discovery_credit_completed")),
        _int(discovery.get("scope_total")),
        label="provenance verified + legitimate exemptions / discovery scope",
    )
    stages["analysis_ready_discovery"] = _stage(
        _int(analysis_discovery.get("core_verified")),
        _int(analysis_discovery.get("core_eligible_total")),
        label="frozen core scope provenance verified / frozen core queue items",
    )
    stages["analysis_ready_discovery"]["progress_scope"] = "ANALYSIS_READY_SCOPE"
    stages["analysis_ready_gap_audit"] = _stage(
        1 if _int(core_gaps.get("blocking_gap_count")) == 0 and scope_entities.get("core_document_ids") else 0,
        1,
        label="frozen core critical gaps clear / core gap audit",
    )
    stages["analysis_ready_gap_audit"]["progress_scope"] = "ANALYSIS_READY_SCOPE"
    manual_value = progress.get("manual_review_pending")
    if manual_value is None and isinstance(progress.get("manual_review"), dict):
        manual_value = progress["manual_review"].get("pending")
    manual_known = manual_value is not None
    manual_pending = _int(manual_value) if manual_known else None
    manual_total = max(1, manual_pending) if manual_known else 0
    manual_completed = max(0, manual_total - (manual_pending or 0)) if manual_known else None
    final_manifest_paths = (
        output / "2016_930_FINAL_EXPORT_METADATA.json",
        output / "2016_930_FINAL_MANIFEST.json",
        output / "930_FINAL_MANIFEST.json",
        output / "930_FINAL_COVERAGE_AUDIT.json",
    )
    final_manifest_ready = int(any(path.exists() for path in final_manifest_paths))
    row = {
        "snapshot_at": iso_now(),
        "run_id": str(progress.get("run_id") or ""),
        "queue_completed": _int(progress.get("queue_completed")),
        "queue_total": _int(progress.get("queue_total")),
        "search_items": crawl["search_calls"],
        "search_results": crawl["search_results"],
        "http_requests": crawl["http_requests"],
        "http_200": crawl["http_200"],
        "document_versions": crawl["document_versions"],
        "documents": _int(progress.get("documents_found")),
        "official_documents": _int(progress.get("official_documents")),
        "actions": _int(progress.get("actions_extracted")),
        "api_pass1": _int(progress.get("api_pass1_success")),
        "api_pass2": _int(progress.get("api_pass2_success")),
        "dates": _int(progress.get("dates_verified")),
        "parameters": _int(progress.get("parameters_extracted")),
        "attachments_resolved": attachments_resolved,
        "promoted": _int(progress.get("formal_actions_promoted")),
        "critical_gaps": _int(gaps.get("blocking_gap_count")),
        "provenance_completed": provenance_completed,
        "provenance_pending": provenance_pending,
        "attachments_completed": attachments_resolved,
        "attachment_pending": max(0, attachments - attachments_resolved),
        "manual_review_completed": manual_completed,
        "manual_review_pending": manual_pending,
        "final_manifest_ready": final_manifest_ready,
    }
    if write:
        history = append_history(output, row)
    rates = _rates(history, [
        "search_items", "http_requests", "document_versions", "actions", "api_pass1", "api_pass2", "dates", "parameters", "promoted",
        "provenance_completed", "attachments_completed", "manual_review_completed", "final_manifest_ready",
    ])
    provider = read_json(output / "930_API_PROVIDER_STATUS.json")
    api_rows = _api_classification_frame(output)
    api = api_health(
        output,
        progress,
        crawl,
        core_document_ids=scope_entities.get("core_document_ids") or set(),
        api_rows=api_rows,
    )
    api_available = (
        _upper(provider.get("status") or progress.get("api_provider_status")) in {"OPERATIONAL", "RECOVERED"}
        and api["status"] == "OPERATIONAL"
        and not bool(api.get("recovery_gate_blocked"))
    )
    api_blocked = not api_available or api["status"] in {"STALLED", "BLOCKED", "RECOVERING"} or bool(api.get("recovery_gate_blocked"))
    stages["provenance"] = _stage(provenance_completed, provenance_total, label="provenance-audited completed / completed queue items")
    stages["provenance"]["readiness_gate"] = "PASS" if provenance_pending == 0 and provenance_total > 0 else "FAIL"
    stages["manual_review"] = _stage(manual_completed or 0, manual_total, label="resolved manual review / known manual review items")
    stages["manual_review"]["readiness_gate"] = "PASS" if manual_known and manual_pending == 0 else "UNKNOWN"
    stages["final_manifest"] = _stage(final_manifest_ready, 1, label="final manifest / final manifest gate")
    stages["final_manifest"]["readiness_gate"] = "PASS" if final_manifest_ready else "FAIL"
    stage_values = {name: (value.get("percent") or 0.0) / 100 for name, value in stages.items()}
    valid_weight = sum(PROGRESS_WEIGHTS.values())
    overall = round(100 * sum(PROGRESS_WEIGHTS[name] * stage_values.get(name, 0.0) for name in PROGRESS_WEIGHTS) / valid_weight, 2)
    stage_eta = {
        "discovery": _stage_eta(stages["discovery"], rates.get("search_items")),
        "official_recovery": _stage_eta(stages["official_recovery"], rates.get("document_versions")),
        "action_extraction": _stage_eta(stages["action_extraction"], rates.get("actions")),
        "api_pass1": _stage_eta(stages["api_pass1"], rates.get("api_pass1"), blocked=api_blocked),
        "api_pass2": _stage_eta(stages["api_pass2"], rates.get("api_pass2"), blocked=api_blocked),
        "date_verification": _stage_eta(stages["date_verification"], rates.get("dates")),
        "formal_promotion": _stage_eta(stages["formal_promotion"], rates.get("promoted")),
        "gap_audit": _stage_eta(stages["gap_audit"], None),
        "analysis_ready_gap_audit": _stage_eta(stages["analysis_ready_gap_audit"], None),
        "provenance": _stage_eta(stages.get("provenance", {}), rates.get("provenance_completed")),
        "attachment": _stage_eta(stages["attachment"], rates.get("attachments_completed")),
        "manual_review": _stage_eta(stages.get("manual_review", {}), rates.get("manual_review_completed")),
        "final_manifest": _stage_eta(stages.get("final_manifest", {}), rates.get("final_manifest_ready")),
        "analysis_ready_discovery": _stage_eta(
            stages["analysis_ready_discovery"],
            rates.get("provenance_completed"),
        ),
    }
    csv_ready = {
        "live_crawl": crawl["real_network_fetches"] > 0,
        "official_documents": _int(progress.get("official_documents")) > 0,
        "action_extraction": (extraction_readiness.get("analysis_ready") or {}).get("gate") == "PASS",
        "api_pass1": _int(progress.get("api_pass1_success")) > 0 and api_available,
        "api_pass2": _int(progress.get("api_pass2_success")) > 0 and api_available,
        "date_verification": _int(progress.get("dates_verified")) > 0,
        "formal_promotion": _int(progress.get("formal_actions_promoted")) > 0,
        "dashboard_export": (output / "2016_930_ANALYSIS_READY.csv").exists(),
        "critical_gaps": _int(core_gaps.get("blocking_gap_count")),
        "global_critical_gaps": _int(gaps.get("blocking_gap_count")),
        "analysis_ready_core_blocking_gaps": core_gaps,
        "global_final_blocking_gaps": gaps,
    }
    stage_gate_map = {
        "api_pass1": csv_ready["api_pass1"],
        "api_pass2": csv_ready["api_pass2"],
        "date_verification": csv_ready["date_verification"],
        "formal_promotion": csv_ready["formal_promotion"],
        "export_dashboard": csv_ready["dashboard_export"],
        "gap_audit": csv_ready["global_critical_gaps"] == 0,
        "analysis_ready_gap_audit": csv_ready["critical_gaps"] == 0,
    }
    for name, gate in stage_gate_map.items():
        if name not in stages:
            continue
        stage = stages[name]
        stage["readiness_gate"] = "PASS" if gate else "FAIL"
        if stage.get("raw_status") == "COMPLETE" and not gate:
            stage["status"] = "COMPLETE_BUT_GATE_FAIL"
        else:
            stage["status"] = stage.get("raw_status")
    blockers: list[dict[str, Any]] = []
    if api_blocked:
        if api.get("recovery_lane_missed_retry_window"):
            blocker_type = "API_RECOVERY_LANE_MISSED_RETRY_WINDOW"
        else:
            blocker_type = "TEMPORARY_PROVIDER_FAILURE" if api.get("recovery_gate_blocked") else str(provider.get("status") or "API_PROVIDER_UNAVAILABLE")
        blockers.append({
            "severity": "P0",
            "type": blocker_type,
            "runtime_state": api["status"],
            "recovery_gate": api.get("recovery_gate"),
            "affected_documents": api["waiting_total"],
            "status": "OPEN",
        })
    if _int(core_gaps.get("blocking_gap_count")) > 0:
        blockers.append({
            "severity": "P1",
            "type": "ANALYSIS_READY_CORE_GAPS",
            "scope": "930-analysis-ready-v1",
            "affected_documents": _int(core_gaps.get("affected_document_count")),
            "affected_actions": _int(core_gaps.get("affected_action_count")),
            "affected_cities": _int(core_gaps.get("affected_city_count")),
            "authoritative_source": gap_source,
            "status": "OPEN",
            "count": _int(core_gaps.get("blocking_gap_count")),
        })
    if _int(gaps.get("blocking_gap_count")) > 0:
        blockers.append({
            "severity": "P1",
            "type": "GLOBAL_CRITICAL_GAPS",
            "scope": "GLOBAL_FINAL",
            "affected_documents": _int(gaps.get("affected_document_count")),
            "affected_actions": _int(gaps.get("affected_action_count")),
            "affected_cities": _int(gaps.get("affected_city_count")),
            "non_document_action_gaps": _int(gaps.get("non_document_action_gap_count")),
            "non_entity_explanation": "city/policy-type/date coverage gap" if _int(gaps.get("non_document_action_gap_count")) > 0 else None,
            "authoritative_source": gap_source,
            "status": "OPEN",
            "count": _int(gaps.get("blocking_gap_count")),
        })
    if attachments > attachments_resolved:
        blockers.append({"severity": "P1", "type": "ATTACHMENT_RETRY", "affected_documents": attachments - attachments_resolved, "status": "OPEN"})
    gate_keys = {
        "live_crawl",
        "official_documents",
        "action_extraction",
        "api_pass1",
        "api_pass2",
        "date_verification",
        "formal_promotion",
        "dashboard_export",
    }
    ready = all(csv_ready[key] is True for key in gate_keys) and csv_ready["critical_gaps"] == 0
    current_batch_total = _int(progress.get("crawler_progress_total"))
    current_batch_completed = _int(progress.get("crawler_progress_current"))
    current_batch_progress = {
        "run_id": progress.get("run_id"),
        "stage": progress.get("stage"),
        "current_city": progress.get("current_city"),
        "current_source": progress.get("current_source"),
        "current_item": progress.get("current_item"),
        "completed": current_batch_completed,
        "total": current_batch_total,
        "percent": None if current_batch_total <= 0 else round(min(1.0, current_batch_completed / current_batch_total) * 100, 2),
        "lease_reference_ids": queue_reconciliation.get("lease_ids", []),
        "progress_scope": "CURRENT_BATCH_PROGRESS",
    }
    global_episode_progress = {
        "queue_total": _int(progress.get("queue_total")),
        "queue_completed": _int(progress.get("queue_completed")),
        "queue_pending": _int(progress.get("queue_pending")),
        "queue_accounted_total": _int(queue_reconciliation.get("accounted_total")),
        "queue_accounting_consistent": bool(queue_reconciliation.get("consistent")),
        "raw_queue_completed": _int(progress.get("queue_completed")),
        "provenance_verified_completed": _int(discovery.get("provenance_verified_completed")),
        "legitimately_exempted_completed": _int(discovery.get("legitimately_exempted_completed")),
        "false_completion_candidates": _int(discovery.get("false_completion_candidates")),
        "false_completion_recovery_required": _int(discovery.get("false_completion_recovery_required")),
        "recovery_completed": _int(discovery.get("recovery_completed")),
        "recovery_claims": claim_metrics,
        "next_work_source": next_work_source,
        "global_core_priority": global_core_priority,
        "analysis_ready_core_recovery_required": _int(
            claim_metrics.get("core_required")
        ),
        "discovery_progress_percent": discovery.get("progress_percent"),
        "analysis_ready_discovery": analysis_discovery,
        "final_discovery": discovery,
        "action_extraction": extraction_readiness,
        "core_rolling_postprocess": rolling_core,
        "authoritative_gap_impact": gaps,
        "analysis_ready_core_blocking_gaps": core_gaps,
        "global_final_blocking_gaps": gaps,
        "authoritative_gap_source": gap_source,
        "documents": _int(progress.get("documents_found")),
        "actions": _int(progress.get("actions_extracted")),
        "api_pass1": _int(progress.get("api_pass1_success")),
        "api_pass2": _int(progress.get("api_pass2_success")),
        "dates": _int(progress.get("dates_verified")),
        "promotion": _int(progress.get("formal_actions_promoted")),
        "overall_progress_percent": overall,
        "episode_complete": ready,
        "status": "COMPLETE" if ready else "IN_PROGRESS",
        "substage_100_does_not_imply_episode_complete": not ready,
        "progress_scope": "GLOBAL_EPISODE_PROGRESS",
    }
    failed_gates = [key for key in gate_keys if csv_ready[key] is not True]
    if csv_ready["critical_gaps"]:
        failed_gates.append("critical_gaps")
    csv_gate = csv_ready | {
        "status": "PASS" if ready else "FAIL",
        "failed_gates": failed_gates,
        "analysis_ready": ready,
        "gate_scope": "CSV_READINESS_GATE",
    }
    analysis_dependencies = {
        name: stage_eta[name]
        for name in ("analysis_ready_discovery", "official_recovery", "action_extraction", "api_pass1", "api_pass2", "date_verification", "formal_promotion", "analysis_ready_gap_audit")
    }
    final_dependencies = {
        name: stage_eta[name]
        for name in ("discovery", "official_recovery", "action_extraction", "api_pass1", "api_pass2", "date_verification", "formal_promotion", "gap_audit")
    }
    final_dependencies.update({
        "provenance": stage_eta["provenance"],
        "attachment": stage_eta["attachment"],
        "manual_review": stage_eta["manual_review"],
        "final_manifest": stage_eta["final_manifest"],
    })
    analysis_eta = _combine_etas(analysis_dependencies)
    final_eta = _combine_etas(final_dependencies)
    snapshot = {
        "updated_at": iso_now(),
        "episode_id": EPISODE_ID,
        "execution_mode": EXECUTION_MODE,
        "episode_status": progress.get("status", "UNKNOWN"),
        "pipeline_status": "PARTIAL / LIVE_CRAWL_VERIFIED" if not ready else "ANALYSIS_READY",
        "current_stage": progress.get("stage"),
        "run_id": progress.get("run_id"),
        "queue_total": _int(progress.get("queue_total")),
        "queue_completed": _int(progress.get("queue_completed")),
        "queue_pending": _int(progress.get("queue_pending")),
        "queue_running": _int(progress.get("queue_running")),
        "queue_retry": _int(progress.get("queue_retry")),
        "queue_reconciliation": queue_reconciliation,
        "raw_queue_completed": _int(progress.get("queue_completed")),
        "provenance_verified_completed": _int(discovery.get("provenance_verified_completed")),
        "false_completion_candidates": _int(discovery.get("false_completion_candidates")),
        "false_completion_recovery_required": _int(discovery.get("false_completion_recovery_required")),
        "recovery_completed": _int(discovery.get("recovery_completed")),
        "recovery_claim_audit": claim_metrics,
        "next_work_source": next_work_source,
        "global_core_priority": global_core_priority,
        "discovery_progress": discovery,
        "analysis_ready_discovery_progress": analysis_discovery,
        "final_discovery_progress": discovery,
        "action_extraction_readiness": extraction_readiness,
        "core_rolling_postprocess": rolling_core,
        "crawl": crawl,
        "formal": formal,
        "documents": _int(progress.get("documents_found")),
        "official_documents": _int(progress.get("official_documents")),
        "actions": _int(progress.get("actions_extracted")),
        "parameters": _int(progress.get("parameters_extracted")),
        "dates": _int(progress.get("dates_verified")),
        "api_pass1": _int(progress.get("api_pass1_success")),
        "api_pass2": _int(progress.get("api_pass2_success")),
        "attachments_found": attachments,
        "attachments_resolved": attachments_resolved,
        "promotion": _int(progress.get("formal_actions_promoted")),
        "gap_counts": gaps,
        "analysis_ready_core_blocking_gaps": core_gaps,
        "global_final_blocking_gaps": gaps,
        "analysis_ready_core_gap_counts": core_gaps,
        "global_final_gap_counts": gaps,
        "gap_authoritative_source": gap_source,
        "stage_progress": stages,
        "stage_throughput_per_hour": rates,
        "stage_eta": stage_eta,
        "overall_progress_percent": overall,
        "CURRENT_BATCH_PROGRESS": current_batch_progress,
        "GLOBAL_EPISODE_PROGRESS": global_episode_progress,
        "CSV_READINESS_GATE": csv_gate,
        "analysis_ready_eta": analysis_eta,
        "final_complete_eta": final_eta,
        "eta_dependencies": {"analysis_ready": analysis_dependencies, "final_complete": final_dependencies},
        "eta_confidence": "BLOCKED" if api_blocked else "MEDIUM" if history.height >= 5 else "CALIBRATING",
        "blockers": blockers[:5],
        "provider": provider,
        "api_health": api,
        "csv_readiness": csv_gate,
        "provenance_pending": provenance_pending,
        "manual_review_pending": manual_pending,
        "final_manifest_ready": bool(final_manifest_ready),
        "last_real_progress_at": progress.get("last_real_progress_at"),
        "heartbeat_at": progress.get("heartbeat_at"),
        "recent_events": [],
    }
    if write:
        atomic_json(output / "930_MONITOR_SNAPSHOT.json", snapshot)
    return snapshot


__all__ = [
    "EPISODE_ID",
    "EXECUTION_MODE",
    "PROGRESS_WEIGHTS",
    "audit_crawl_artifacts",
    "append_history",
    "analysis_ready_scope_entities",
    "api_health",
    "build_monitor_snapshot",
    "latest_formal_counts",
    "queue_counts",
    "reconcile_queue",
    "recovery_claim_metrics",
    "split_gap_metrics",
    "stage_progress",
    "timeout_fingerprint",
]
