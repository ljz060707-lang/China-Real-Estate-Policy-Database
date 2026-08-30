# ruff: noqa: E402, I001
"""Build bounded 930 audit/recovery queues without refetching any URL."""

from __future__ import annotations

import argparse
import hashlib
import sys
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from policydb.episode_930_monitor import read_json, read_table  # noqa: E402
from policydb.parquet_store import atomic_write_parquet  # noqa: E402


PROVENANCE_SCHEMA = {
    "queue_item_id": pl.String,
    "city": pl.String,
    "query": pl.String,
    "completed_at": pl.String,
    "search_provider_called": pl.Boolean,
    "search_result_count": pl.Int64,
    "live_http_requests": pl.Int64,
    "http_2xx": pl.Int64,
    "http_failures": pl.Int64,
    "response_bytes": pl.Int64,
    "new_content_hashes": pl.Int64,
    "document_versions": pl.Int64,
    "cache_hits": pl.Int64,
    "local_db_hits": pl.Int64,
    "search_executed": pl.Boolean,
    "fetch_executed": pl.Boolean,
    "provenance_class": pl.String,
    "provenance_reason": pl.String,
    "needs_recovery": pl.Boolean,
}

BLOCKER_SCHEMA = {
    "blocker_id": pl.String,
    "blocker_type": pl.String,
    "stage": pl.String,
    "severity": pl.String,
    "current_or_historical": pl.String,
    "first_seen_at": pl.String,
    "last_seen_at": pl.String,
    "affected_run_count": pl.Int64,
    "affected_document_count": pl.Int64,
    "affected_action_count": pl.Int64,
    "recoverable_without_refetch": pl.Boolean,
    "requires_code_fix": pl.Boolean,
    "requires_api": pl.Boolean,
    "requires_writer": pl.Boolean,
    "status": pl.String,
    "resolution": pl.String,
    "resolved_at": pl.String,
}

RECOVERY_SCHEMA = {
    "recovery_id": pl.String,
    "episode_id": pl.String,
    "run_id": pl.String,
    "stage": pl.String,
    "source": pl.String,
    "document_version_id": pl.String,
    "status": pl.String,
    "reason_code": pl.String,
    "recoverable_without_refetch": pl.Boolean,
    "requires_api": pl.Boolean,
    "created_at": pl.String,
    "updated_at": pl.String,
}

DATE_SCHEMA = {
    "date_recovery_id": pl.String,
    "episode_id": pl.String,
    "action_id": pl.String,
    "document_id": pl.String,
    "city": pl.String,
    "reason_code": pl.String,
    "status": pl.String,
    "created_at": pl.String,
}

FALSE_COMPLETION_SCHEMA = {
    "recovery_id": pl.String,
    "episode_id": pl.String,
    "queue_item_id": pl.String,
    "city": pl.String,
    "query": pl.String,
    "provenance_class": pl.String,
    "status": pl.String,
    "reason_code": pl.String,
    "recoverable_without_refetch": pl.Boolean,
    "requires_search": pl.Boolean,
    "requires_fetch": pl.Boolean,
    "created_at": pl.String,
    "updated_at": pl.String,
    "lease_owner": pl.String,
    "lease_acquired_at": pl.String,
    "lease_expires_at": pl.String,
    "completed_at": pl.String,
    "result_status": pl.String,
    "fetch_status": pl.String,
    "search_executed": pl.Boolean,
    "http_request_count": pl.Int64,
    "real_network_fetch": pl.Boolean,
    "document_version_id": pl.String,
    "failure_reason": pl.String,
}

ELIGIBILITY_SCHEMA = {
    "queue_item_id": pl.String,
    "episode_id": pl.String,
    "city_id": pl.String,
    "city": pl.String,
    "query_type": pl.String,
    "query": pl.String,
    "window_start": pl.String,
    "window_end": pl.String,
    "provenance_class": pl.String,
    "scope_status": pl.String,
    "legitimately_exempted": pl.Boolean,
    "duplicate_or_superseded": pl.Boolean,
    "existing_document_version_from_other_queue": pl.Boolean,
    "local_db_reuse_linked": pl.Boolean,
    "eligibility_status": pl.String,
    "discovery_credit": pl.Boolean,
    "recovery_status": pl.String,
    "reason_code": pl.String,
    "audited_at": pl.String,
}


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _frame(rows: list[dict[str, Any]], schema: dict[str, pl.DataType]) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame(schema=schema)
    return pl.DataFrame(rows, schema=schema, strict=False)


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _text(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return str(value)


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "y"}


def _has_local_reuse_marker(item: dict[str, Any], fetches: list[dict[str, Any]]) -> bool:
    values = [
        item.get("fetch_status"), item.get("result_status"), item.get("execution_status"),
        item.get("search_provider"), item.get("evidence_path"),
    ]
    for row in fetches:
        values.extend([row.get("network_source"), row.get("fetch_status"), row.get("error_type")])
    text = "|".join(str(value or "").upper() for value in values)
    return any(marker in text for marker in ("LOCAL_DB", "DATABASE_REUSE", "DB_REUSE", "CURATED_REUSE"))


def _classify_provenance(
    item: dict[str, Any],
    searches: list[dict[str, Any]],
    fetches: list[dict[str, Any]],
    cache_hits: int,
    local_db_hits: int,
) -> tuple[str, str, bool, bool]:
    search_executed = _truthy(item.get("search_executed")) or bool(searches) or _int(item.get("search_call_count")) > 0
    search_has_url = any(row.get("result_url") or row.get("canonical_url") for row in searches)
    explicit_fetch_status = str(item.get("fetch_status") or "").upper() not in {"", "NOT_ATTEMPTED", "PENDING"}
    fetch_executed = bool(fetches) or _int(item.get("http_request_count")) > 0 or explicit_fetch_status
    explicit_fetch = explicit_fetch_status or _truthy(item.get("real_network_fetch"))
    if local_db_hits:
        return "LOCAL_DB_REUSE", "explicit local database reuse marker", search_executed, fetch_executed
    if search_executed and fetches and any(_truthy(row.get("real_network_fetch")) for row in fetches):
        return "LIVE_SEARCH_AND_FETCH", "search audit plus live network fetch audit", search_executed, fetch_executed
    if search_executed and cache_hits and not any(_truthy(row.get("real_network_fetch")) for row in fetches):
        return "LIVE_SEARCH_CACHE_REUSE", "search executed and cached content reused", search_executed, fetch_executed
    if not search_executed and cache_hits:
        return "CACHE_ONLY", "cached content reused without a search call", search_executed, fetch_executed
    if search_executed and not search_has_url and not fetches:
        return "LIVE_SEARCH_NO_NEW_URL", "search executed but produced no URL to fetch", search_executed, fetch_executed
    if search_executed and search_has_url and not fetches:
        return "FETCH_NOT_EXECUTED", "search returned URL evidence but no fetch audit exists", search_executed, fetch_executed
    if not search_executed and not fetches and not explicit_fetch:
        return "SEARCH_NOT_EXECUTED", "completed row has no search or fetch evidence", search_executed, fetch_executed
    return "UNKNOWN_PROVENANCE", "queue fields and audit evidence are contradictory or incomplete", search_executed, fetch_executed


def _group(rows: pl.DataFrame, key: str) -> dict[str, list[dict[str, Any]]]:
    result: dict[str, list[dict[str, Any]]] = defaultdict(list)
    if rows.is_empty() or key not in rows.columns:
        return result
    for row in rows.to_dicts():
        value = row.get(key)
        if value not in (None, ""):
            result[str(value)].append(row)
    return result


def _count_distinct(rows: list[dict[str, Any]], key: str) -> int:
    return len({str(row.get(key)) for row in rows if row.get(key) not in (None, "")})


def build_completed_provenance(output: Path) -> pl.DataFrame:
    queue = read_table(output / "930_TASK_QUEUE.parquet")
    search = _group(read_table(output / "930_QUEUE_SEARCH_EXECUTION.parquet"), "queue_item_id")
    http = _group(read_table(output / "930_QUEUE_HTTP_AUDIT.parquet"), "queue_item_id")
    rows: list[dict[str, Any]] = []
    if not queue.is_empty():
        for item in queue.filter(pl.col("status").cast(pl.String).is_in(["CRAWL_COMPLETED", "COMPLETED"])).to_dicts():
            item_id = str(item.get("queue_item_id"))
            searches = search.get(item_id, [])
            fetches = http.get(item_id, [])
            two_xx = sum(1 for row in fetches if 200 <= _int(row.get("http_status")) < 300)
            cache_hits = sum(1 for row in fetches if _truthy(row.get("cache_hit"))) + int(_truthy(item.get("cache_hit")))
            local_db_hits = int(_has_local_reuse_marker(item, fetches))
            provenance, reason, search_executed, fetch_executed = _classify_provenance(item, searches, fetches, cache_hits, local_db_hits)
            rows.append({
                "queue_item_id": item_id,
                "city": item.get("city"),
                "query": item.get("query_text"),
                "completed_at": item.get("completed_at"),
                "search_provider_called": bool(searches),
                "search_result_count": len(searches),
                "live_http_requests": len(fetches),
                "http_2xx": two_xx,
                "http_failures": max(0, len(fetches) - two_xx),
                "response_bytes": sum(_int(row.get("response_bytes")) for row in fetches),
                "new_content_hashes": _count_distinct(fetches, "content_sha256"),
                "document_versions": _count_distinct(fetches, "document_version_id"),
                "cache_hits": cache_hits,
                "local_db_hits": local_db_hits,
                "search_executed": search_executed,
                "fetch_executed": fetch_executed,
                "provenance_class": provenance,
                "provenance_reason": reason,
                "needs_recovery": provenance == "SEARCH_NOT_EXECUTED",
            })
    return _frame(rows, PROVENANCE_SCHEMA)


def build_false_completion_eligibility(output: Path, provenance: pl.DataFrame) -> pl.DataFrame:
    """Audit false-completion rows before allowing them into recovery.

    This is deliberately conservative.  A document elsewhere in the curated
    database is not treated as satisfying a queue item unless there is an
    item-level link (same scope key and a different queue item).  The original
    queue is never mutated here.
    """

    queue = read_table(output / "930_TASK_QUEUE.parquet")
    if provenance.is_empty() or queue.is_empty() or "provenance_class" not in provenance.columns:
        return _frame([], ELIGIBILITY_SCHEMA)
    candidate_ids = {
        str(value)
        for value in provenance.filter(pl.col("provenance_class") == "SEARCH_NOT_EXECUTED").get_column("queue_item_id").to_list()
    }
    queue_rows = {str(row.get("queue_item_id")): row for row in queue.to_dicts()}
    scope_doc_keys: set[tuple[str, str, str, str]] = set()
    for row in queue.to_dicts():
        if row.get("document_version_id") not in (None, ""):
            scope_doc_keys.add((
                str(row.get("city_id") or ""),
                str(row.get("query_type") or ""),
                str(row.get("window_start") or ""),
                str(row.get("window_end") or ""),
            ))
    rows: list[dict[str, Any]] = []
    for item_id in sorted(candidate_ids):
        row = queue_rows.get(item_id, {})
        values_text = "|".join(str(value or "").upper() for value in row.values())
        duplicate_or_superseded = any(marker in values_text for marker in ("DUPLICATE", "SUPERSEDED", "EXCLUDED"))
        window_start = str(row.get("window_start") or "")
        window_end = str(row.get("window_end") or "")
        scope_status = "IN_SCOPE" if (
            str(row.get("episode_id") or "") == "EP_2016_930_TIGHTENING"
            and str(row.get("task_stage") or "") == "930_DISCOVERY"
            and row.get("query_text") not in (None, "")
            and window_start <= "2016-10-31"
            and window_end >= "2016-09-01"
        ) else "OUT_OF_SCOPE"
        scope_key = (
            str(row.get("city_id") or ""),
            str(row.get("query_type") or ""),
            window_start,
            window_end,
        )
        existing_document = row.get("document_version_id") not in (None, "") or scope_key in scope_doc_keys and any(
            str(other.get("queue_item_id")) != item_id
            and (str(other.get("city_id") or ""), str(other.get("query_type") or ""), str(other.get("window_start") or ""), str(other.get("window_end") or "")) == scope_key
            and other.get("document_version_id") not in (None, "")
            for other in queue.to_dicts()
        )
        local_linked = _has_local_reuse_marker(row, [])
        if scope_status != "IN_SCOPE":
            eligibility_status = "OUT_OF_SCOPE"
            reason = "queue item is outside EP_2016_930_TIGHTENING discovery scope"
        elif duplicate_or_superseded:
            eligibility_status = "DUPLICATE_OR_SUPERSEDED"
            reason = "explicit duplicate, superseded, or excluded marker"
        elif existing_document:
            eligibility_status = "ALREADY_SATISFIED"
            reason = "item-level DocumentVersion link exists from another queue item"
        elif local_linked:
            eligibility_status = "LOCAL_DB_REUSE_PENDING_LINK"
            reason = "local reuse marker exists but provenance link is incomplete"
        else:
            eligibility_status = "RECOVERY_REQUIRED"
            reason = "in-scope completed row has no legal search/fetch/reuse evidence"
        exempted = eligibility_status != "RECOVERY_REQUIRED"
        rows.append({
            "queue_item_id": item_id,
            "episode_id": row.get("episode_id"),
            "city_id": row.get("city_id"),
            "city": row.get("city"),
            "query_type": row.get("query_type"),
            "query": row.get("query_text"),
            "window_start": window_start,
            "window_end": window_end,
            "provenance_class": "SEARCH_NOT_EXECUTED",
            "scope_status": scope_status,
            "legitimately_exempted": exempted,
            "duplicate_or_superseded": duplicate_or_superseded,
            "existing_document_version_from_other_queue": existing_document,
            "local_db_reuse_linked": local_linked,
            "eligibility_status": eligibility_status,
            "discovery_credit": scope_status == "IN_SCOPE" and exempted,
            "recovery_status": eligibility_status,
            "reason_code": reason,
            "audited_at": _now(),
        })
    return _frame(rows, ELIGIBILITY_SCHEMA)


def build_false_completion_recovery(provenance_or_eligibility: pl.DataFrame) -> pl.DataFrame:
    """Create a queue only for rows whose eligibility is RECOVERY_REQUIRED."""

    rows: list[dict[str, Any]] = []
    if provenance_or_eligibility.is_empty():
        return _frame(rows, FALSE_COMPLETION_SCHEMA)
    if "eligibility_status" in provenance_or_eligibility.columns:
        source_rows = provenance_or_eligibility.filter(pl.col("eligibility_status") == "RECOVERY_REQUIRED").to_dicts()
    elif "needs_recovery" in provenance_or_eligibility.columns:
        source_rows = provenance_or_eligibility.filter(pl.col("needs_recovery").fill_null(False)).to_dicts()
    else:
        source_rows = []
    for item in source_rows:
        queue_item_id = str(item.get("queue_item_id") or "")
        recovery_id = "930FALSE_" + hashlib.sha256(queue_item_id.encode()).hexdigest()[:24]
        rows.append({
            "recovery_id": recovery_id,
            "episode_id": "EP_2016_930_TIGHTENING",
            "queue_item_id": queue_item_id,
            "city": item.get("city"),
            "query": item.get("query"),
            "provenance_class": item.get("provenance_class") or "SEARCH_NOT_EXECUTED",
            "status": "RECOVERY_REQUIRED",
            "reason_code": "SEARCH_AND_FETCH_EVIDENCE_MISSING",
            "recoverable_without_refetch": False,
            "requires_search": True,
            "requires_fetch": True,
            "created_at": _now(),
            "updated_at": _now(),
        })
    return _frame(rows, FALSE_COMPLETION_SCHEMA)


def preserve_recovery_runtime(fresh: pl.DataFrame, existing: pl.DataFrame) -> pl.DataFrame:
    """Keep recovery leases/results when the read-only eligibility audit reruns."""

    if fresh.is_empty() or existing.is_empty() or "queue_item_id" not in existing.columns:
        return fresh
    prior = {str(row.get("queue_item_id") or ""): row for row in existing.to_dicts()}
    runtime_fields = {
        "status", "lease_owner", "lease_acquired_at", "lease_expires_at",
        "completed_at", "result_status", "fetch_status", "search_executed",
        "http_request_count", "real_network_fetch", "document_version_id",
        "failure_reason", "updated_at",
    }
    rows = fresh.to_dicts()
    for row in rows:
        old = prior.get(str(row.get("queue_item_id") or ""))
        if not old or str(old.get("status") or "") == "RECOVERY_REQUIRED":
            continue
        for field in runtime_fields:
            if field in old:
                row[field] = old.get(field)
    return _frame(rows, FALSE_COMPLETION_SCHEMA)


def build_postprocess_recovery(output: Path) -> pl.DataFrame:
    rows: list[dict[str, Any]] = []
    active_run_id = str(read_json(output / "930_PROGRESS_SNAPSHOT.json").get("run_id") or "")
    for state_path in sorted((output / "production_runs").glob("*/STATE.json")):
        state = read_json(state_path)
        run_id = state_path.parent.name
        checkpoint = read_json(state_path.parent / "CHECKPOINT.json")
        checkpoint_status = str(checkpoint.get("status") or "").upper()
        if checkpoint_status == "PLANNED":
            continue
        if checkpoint_status in {"RUNNING", "STARTED", "IN_PROGRESS"} and run_id == active_run_id:
            continue
        summary_exists = (state_path.parent / "POSTPROCESS_SUMMARY.json").exists()
        stage = str(state.get("stage") or "UNKNOWN")
        if summary_exists and stage == "FORMAL_IMPORT":
            continue
        if stage in {"FINAL_COVERAGE_AUDIT", "COMPLETE"} and summary_exists:
            continue
        recovery_id = hashlib.sha256(f"EP_2016_930_TIGHTENING|{run_id}|{stage}".encode()).hexdigest()[:24]
        rows.append({
            "recovery_id": f"930POST_{recovery_id}",
            "episode_id": "EP_2016_930_TIGHTENING",
            "run_id": run_id,
            "stage": stage,
            "source": str(state_path.parent),
            "document_version_id": None,
            "status": "PENDING",
            "reason_code": "POSTPROCESS_NOT_FORMALLY_COMPLETED",
            "recoverable_without_refetch": True,
            "requires_api": "API" in stage or "CLASSIFY" in stage,
            "created_at": _now(),
            "updated_at": _now(),
        })
    return _frame(rows, RECOVERY_SCHEMA)


def build_date_recovery(output: Path) -> pl.DataFrame:
    actions = read_table(output.parent.parent / ".." / "curated" / "policy_episode_actions.parquet")
    # The canonical path above is normalized below for callers passing the
    # episode output directory rather than the data root.
    if actions.is_empty():
        data_root = output.parents[2]
        actions = read_table(data_root / "curated" / "policy_episode_actions.parquet")
    rows: list[dict[str, Any]] = []
    if not actions.is_empty() and "effective_date" in actions.columns:
        missing = actions.filter(pl.col("effective_date").is_null())
        for row in missing.to_dicts():
            action_id = str(row.get("action_id") or "")
            rows.append({
                "date_recovery_id": "930DATE_" + hashlib.sha256(action_id.encode()).hexdigest()[:24],
                "episode_id": "EP_2016_930_TIGHTENING",
                "action_id": action_id,
                "document_id": row.get("document_id"),
                "city": row.get("city"),
                "reason_code": "NO_EXPLICIT_EFFECTIVE_DATE",
                "status": "PENDING",
                "created_at": _now(),
            })
    return _frame(rows, DATE_SCHEMA)


def build_blockers(output: Path, provenance: pl.DataFrame, postprocess: pl.DataFrame, dates: pl.DataFrame) -> pl.DataFrame:
    now = _now()
    rows: list[dict[str, Any]] = []
    provider = read_json(output / "930_API_PROVIDER_STATUS.json")
    failures = read_table(output / "930_API_FAILURES.parquet")
    provider_status = str(provider.get("status") or "UNKNOWN").upper()
    if provider_status not in {"OPERATIONAL", "RECOVERED"}:
        underlying = "API_PROVIDER_UNAVAILABLE"
        if not failures.is_empty() and "error_type" in failures.columns:
            recent = failures.sort("created_at").tail(1)
            if recent.height:
                underlying = str(recent.item(0, "error_type") or underlying)
        rows.append({
            "blocker_id": "930BLK_API_PROVIDER",
            "blocker_type": underlying,
            "stage": "API_CLASSIFICATION",
            "severity": "P0",
            "current_or_historical": "CURRENT",
            "first_seen_at": provider.get("last_success_at"),
            "last_seen_at": provider.get("updated_at"),
            "affected_run_count": failures.height,
            "affected_document_count": _count_distinct(failures.to_dicts(), "document_id"),
            "affected_action_count": 0,
            "recoverable_without_refetch": True,
            "requires_code_fix": False,
            "requires_api": True,
            "requires_writer": False,
            "status": "OPEN",
            "resolution": "Wait for backoff and probe an existing cached document; never refetch.",
            "resolved_at": None,
        })
    state = read_json(output / "930_AUTORUN_STATE.json")
    if "Parquet key columns missing" in str(state.get("last_error") or ""):
        formal = any(read_json(path).get("stage") == "FORMAL_IMPORT" for path in (output / "production_runs").glob("*/STATE.json"))
        rows.append({
            "blocker_id": "930BLK_PARAMETER_SCHEMA_HISTORICAL",
            "blocker_type": "PARAMETER_SCHEMA_MISSING_KEYS",
            "stage": "POSTPROCESS",
            "severity": "P1",
            "current_or_historical": "HISTORICAL",
            "first_seen_at": None,
            "last_seen_at": state.get("heartbeat_at"),
            "affected_run_count": 1,
            "affected_document_count": 0,
            "affected_action_count": 0,
            "recoverable_without_refetch": True,
            "requires_code_fix": False,
            "requires_api": False,
            "requires_writer": True,
            "status": "RESOLVED" if formal else "OPEN",
            "resolution": "Fixed keyed empty schemas; later runs reached FORMAL_IMPORT." if formal else "Use keyed empty schemas before resuming.",
            "resolved_at": now if formal else None,
        })
    if postprocess.height:
        rows.append({
            "blocker_id": "930BLK_POSTPROCESS_RECOVERY",
            "blocker_type": "POSTPROCESS_RECOVERY_BACKLOG",
            "stage": "POSTPROCESS",
            "severity": "P1",
            "current_or_historical": "HISTORICAL",
            "first_seen_at": None,
            "last_seen_at": now,
            "affected_run_count": postprocess.height,
            "affected_document_count": 0,
            "affected_action_count": 0,
            "recoverable_without_refetch": True,
            "requires_code_fix": False,
            "requires_api": bool(postprocess.filter(pl.col("requires_api").fill_null(False)).height),
            "requires_writer": True,
            "status": "OPEN",
            "resolution": "Re-enter persisted DocumentVersion rows into postprocess; do not refetch.",
            "resolved_at": None,
        })
    if dates.height:
        rows.append({
            "blocker_id": "930BLK_DATE_RECOVERY",
            "blocker_type": "DATE_RECOVERY_BACKLOG",
            "stage": "DATE_VERIFICATION",
            "severity": "P2",
            "current_or_historical": "CURRENT",
            "first_seen_at": None,
            "last_seen_at": now,
            "affected_run_count": 0,
            "affected_document_count": _count_distinct(dates.to_dicts(), "document_id"),
            "affected_action_count": dates.height,
            "recoverable_without_refetch": True,
            "requires_code_fix": False,
            "requires_api": False,
            "requires_writer": False,
            "status": "OPEN",
            "resolution": "Run deterministic effective-date recovery over stored official text.",
            "resolved_at": None,
        })
    if provenance.height and provenance.filter(pl.col("needs_recovery").fill_null(False)).height:
        count = provenance.filter(pl.col("needs_recovery").fill_null(False))
        rows.append({
            "blocker_id": "930BLK_FALSE_COMPLETION_RECOVERY",
            "blocker_type": "FALSE_COMPLETION_PROVENANCE",
            "stage": "DISCOVERY",
            "severity": "P1",
            "current_or_historical": "HISTORICAL",
            "first_seen_at": None,
            "last_seen_at": now,
            "affected_run_count": count.height,
            "affected_document_count": _count_distinct(count.to_dicts(), "document_version_id"),
            "affected_action_count": 0,
            "recoverable_without_refetch": True,
            "requires_code_fix": False,
            "requires_api": False,
            "requires_writer": False,
            "status": "OPEN",
            "resolution": "Keep completed history; retry only rows without search/fetch evidence.",
            "resolved_at": None,
        })
    return _frame(rows, BLOCKER_SCHEMA)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-root", default=r"E:\Data Set\CRPD")
    args = parser.parse_args()
    output = Path(args.data_root) / "outputs" / "special_projects" / "2016_930"
    output.mkdir(parents=True, exist_ok=True)
    provenance = build_completed_provenance(output)
    eligibility = build_false_completion_eligibility(output, provenance)
    false_completion = build_false_completion_recovery(eligibility)
    existing_false_completion = read_table(output / "930_FALSE_COMPLETION_RECOVERY_QUEUE.parquet")
    false_completion = preserve_recovery_runtime(false_completion, existing_false_completion)
    postprocess = build_postprocess_recovery(output)
    dates = build_date_recovery(output)
    blockers = build_blockers(output, provenance, postprocess, dates)
    ctx = {"module": "audit_episode_930_blockers", "episode_id": "EP_2016_930_TIGHTENING"}
    atomic_write_parquet(provenance, output / "930_COMPLETED_PROVENANCE_AUDIT.parquet", ctx, key_columns=("queue_item_id",))
    atomic_write_parquet(eligibility, output / "930_FALSE_COMPLETION_ELIGIBILITY_AUDIT.parquet", ctx, key_columns=("queue_item_id",))
    atomic_write_parquet(false_completion, output / "930_FALSE_COMPLETION_RECOVERY_QUEUE.parquet", ctx, key_columns=("recovery_id",))
    atomic_write_parquet(postprocess, output / "930_POSTPROCESS_RECOVERY_QUEUE.parquet", ctx, key_columns=("recovery_id",))
    atomic_write_parquet(dates, output / "930_DATE_RECOVERY_QUEUE.parquet", ctx, key_columns=("date_recovery_id",))
    atomic_write_parquet(blockers, output / "930_BLOCKER_REGISTER.parquet", ctx, key_columns=("blocker_id",))
    print({
        "completed_provenance": provenance.height,
        "needs_recovery": provenance.filter(pl.col("needs_recovery").fill_null(False)).height if not provenance.is_empty() else 0,
        "eligibility_rows": eligibility.height,
        "recovery_required": false_completion.height,
        "eligibility_statuses": eligibility.get_column("eligibility_status").value_counts().to_dicts() if not eligibility.is_empty() else [],
        "false_completion_recovery": false_completion.height,
        "provenance_classes": provenance.get_column("provenance_class").value_counts().to_dicts() if not provenance.is_empty() else [],
        "postprocess_recovery": postprocess.height,
        "date_recovery": dates.height,
        "blockers": blockers.height,
    })
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
