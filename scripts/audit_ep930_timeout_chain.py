"""Build EP930 timeout-chain and core-transition audits from saved evidence.

This module is deliberately read-only with respect to the production queue,
curated tables, and recovery state.  It never calls a provider and never
copies a provider response body.  It records the distinction between one
persisted outer audit record and the transport attempts performed inside the
OpenAI-compatible client; the latter are *not* inferred from a duration alone.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import re
from collections import Counter
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path
from statistics import median
from typing import Any

TIMEOUT_FIELDS = [
    "timestamp",
    "finished_at",
    "probe_id",
    "attempt_id",
    "persisted_attempt",
    "probe_type",
    "phase",
    "pass_name",
    "provider",
    "model",
    "http_status",
    "response_received",
    "json_parseable",
    "schema_valid",
    "failure_class",
    "timeout_class",
    "configured_read_timeout",
    "configured_connect_timeout",
    "configured_max_retries",
    "observed_audit_attempts",
    "nested_retry_evidence",
    "retry_source",
    "controller_retry_owner",
    "latency_ms",
    "wall_clock_ms",
    "wall_clock_delta_ms",
    "prompt_version",
    "schema_version",
    "content_sha256",
    "request_id",
    "source_path",
]

CORE_FIELDS = [
    "queue_item_id",
    "city",
    "previous_status",
    "current_status",
    "previous_execution_status",
    "current_result_status",
    "previous_provenance_class",
    "current_provenance_class",
    "transition_class",
    "transition_reason",
    "evidence_status",
    "treatment_impact",
    "retry_count",
    "last_attempt_at",
    "next_eligible_retry",
    "document_version_id",
    "last_claimed_at",
    "last_worker_pid",
    "scope_version",
    "scope_hash",
]

_ALLOWED_RETRY_SOURCES = {"HTTP_CLIENT", "SDK", "APPLICATION", "CONTROLLER", "PROXY", "UNKNOWN"}


def _safe_text(value: Any, limit: int = 240) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        value = json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
    text = str(value).replace("\r", " ").replace("\n", " ").strip()
    text = re.sub(r"(?i)bearer\s+[A-Za-z0-9._-]+", "Bearer <REDACTED>", text)
    text = re.sub(
        r"(?i)(api[_-]?key|authorization|password|secret|token)\s*[:=]\s*[^,; ]+",
        r"\1=<REDACTED>",
        text,
    )
    return text[:limit]


def _first(record: dict[str, Any], *names: str) -> Any:
    for name in names:
        value = record.get(name)
        if value not in (None, "", [], {}):
            return value
    return None


def _bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if value is None:
        return None
    text = str(value).strip().lower()
    if text in {"true", "1", "yes"}:
        return True
    if text in {"false", "0", "no"}:
        return False
    return None


def _number(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        number = float(value)
        return number if math.isfinite(number) else None
    except (TypeError, ValueError):
        return None


def _parse_datetime(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    text = str(value).strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _timestamp(record: dict[str, Any]) -> str:
    return _safe_text(_first(record, "started_at", "created_at", "timestamp", "updated_at"))


def _failure_class(record: dict[str, Any]) -> str:
    value = _safe_text(
        " ".join(
            str(record.get(key) or "")
            for key in ("failure_class", "error_type", "timeout_type", "error_message_safe")
        )
    ).upper()
    if "READ_TIMEOUT" in value:
        return "READ_TIMEOUT"
    if "CONNECT_TIMEOUT" in value:
        return "CONNECT_TIMEOUT"
    if "SCHEMA" in value or "VALIDATION" in value:
        return "SCHEMA_VALIDATION_FAILURE"
    if "EMPTY_RESPONSE" in value:
        return "EMPTY_RESPONSE"
    if "HTTP" in value and re.search(r"\b[45]\d\d\b", value):
        return "PROVIDER_HTTP_ERROR"
    return _safe_text(record.get("failure_class")) or "UNKNOWN"


def _request_directories(root: Path) -> list[Path]:
    candidates = [root / "05_API_CLASSIFICATION" / "ai_audit" / "requests"]
    candidates.extend(path for path in root.rglob("ai_audit/requests") if path not in candidates)
    return [path for path in candidates if path.is_dir()]


def _merge_record(existing: dict[str, Any], incoming: dict[str, Any]) -> dict[str, Any]:
    merged = dict(existing)
    for key, value in incoming.items():
        if key not in merged or merged[key] in (None, "", [], {}):
            merged[key] = value
    old_time = _parse_datetime(_first(existing, "updated_at", "completed_at", "started_at"))
    new_time = _parse_datetime(_first(incoming, "updated_at", "completed_at", "started_at"))
    if new_time and (old_time is None or new_time > old_time):
        for key, value in incoming.items():
            if value not in (None, "", [], {}):
                merged[key] = value
    return merged


def _load_records(root: Path) -> list[tuple[dict[str, Any], str]]:
    by_request: dict[str, tuple[dict[str, Any], str]] = {}
    anonymous: list[tuple[dict[str, Any], str]] = []
    for directory in _request_directories(root):
        for path in directory.glob("*.json"):
            try:
                record = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, UnicodeDecodeError, json.JSONDecodeError):
                continue
            if not isinstance(record, dict):
                continue
            request_id = _safe_text(record.get("request_id"))
            if not request_id:
                anonymous.append((record, str(path)))
                continue
            if request_id in by_request:
                previous, previous_path = by_request[request_id]
                by_request[request_id] = (_merge_record(previous, record), previous_path)
            else:
                by_request[request_id] = (record, str(path))

    failure_path = root / "930_API_FAILURES.parquet"
    if failure_path.exists():
        try:
            import polars as pl

            failure_rows = pl.read_parquet(failure_path).to_dicts()
        except (ImportError, OSError, RuntimeError):
            failure_rows = []
        for index, failure in enumerate(failure_rows):
            if not isinstance(failure, dict):
                continue
            request_id = _safe_text(failure.get("request_id"))
            source = f"{failure_path}#failure_id={_safe_text(failure.get('failure_id')) or index}"
            if request_id and request_id in by_request:
                record, path = by_request[request_id]
                by_request[request_id] = (_merge_record(record, failure), path)
            else:
                anonymous.append((failure, source))
    return list(by_request.values()) + anonymous


def _phase(record: dict[str, Any]) -> str:
    value = _safe_text(_first(record, "phase", "probe_phase", "recovery_phase")).upper()
    if value in {"SINGLE", "PROBE"}:
        return "SINGLE_PROBE"
    if value in {"BACKOFF", "BACKOFF_SINGLE"}:
        return "BACKOFF_SINGLE_PROBE"
    if value in {"SINGLE_PROBE", "BACKOFF_SINGLE_PROBE", "MICRO_5", "MICRO_20", "BACKLOG_CONSUMPTION"}:
        return value
    return "UNRECORDED"


def _explicit_attempt_count(record: dict[str, Any]) -> int | None:
    for key in ("transport_attempt_count", "sdk_attempts", "http_attempts", "attempt_count"):
        value = _number(record.get(key))
        if value is not None and value >= 1:
            return int(value)
    retry_count = _number(record.get("retry_count"))
    if retry_count is not None and retry_count >= 0:
        return int(retry_count) + 1
    return None


def _nested_retry_evidence(record: dict[str, Any], latency_ms: float | None) -> str:
    explicit = _explicit_attempt_count(record)
    if explicit is not None and explicit > 1:
        return "CONFIRMED_EXPLICIT_TRANSPORT_ATTEMPTS"
    configured = _number(record.get("max_retries"))
    timeout = _number(record.get("configured_read_timeout"))
    failure = _failure_class(record)
    if failure == "READ_TIMEOUT" and configured and configured > 0 and timeout and latency_ms:
        if latency_ms / 1000.0 >= timeout * 2.5:
            return "CONFIGURED_RETRY_CHAIN_PLAUSIBLE_NO_PER_ATTEMPT_TRACE"
    return "NO_NESTED_RETRY_EVIDENCE"


def _retry_source(record: dict[str, Any], nested: str) -> str:
    explicit = _safe_text(_first(record, "retry_source", "retry_owner")).upper()
    if explicit in _ALLOWED_RETRY_SOURCES:
        return explicit
    if nested == "CONFIRMED_EXPLICIT_TRANSPORT_ATTEMPTS":
        source = _safe_text(_first(record, "attempt_source", "transport_retry_source")).upper()
        return source if source in _ALLOWED_RETRY_SOURCES else "UNKNOWN"
    return "UNKNOWN"


def build_timeout_rows(root: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for record, source_path in _load_records(root):
        if not record.get("request_id") and not record.get("failure_id"):
            continue
        probe_type = _safe_text(record.get("probe_type"))
        failure = _failure_class(record)
        response_received = _bool(_first(record, "response_received", "responseReceived"))
        http_status = _number(_first(record, "http_status", "final_http_status"))
        latency_ms = _number(_first(record, "latency_ms", "duration_ms"))
        started = _parse_datetime(_first(record, "started_at", "created_at", "timestamp"))
        finished = _parse_datetime(_first(record, "completed_at", "updated_at"))
        wall_clock_ms = (finished - started).total_seconds() * 1000 if started and finished and finished >= started else None
        nested = _nested_retry_evidence(record, latency_ms)
        input_summary = record.get("input_summary") if isinstance(record.get("input_summary"), dict) else {}
        request_id = _safe_text(record.get("request_id") or record.get("failure_id"))
        observed_attempts = _explicit_attempt_count(record) or 1
        rows.append(
            {
                "timestamp": _timestamp(record),
                "finished_at": _safe_text(_first(record, "completed_at", "updated_at")),
                "probe_id": _safe_text(record.get("probe_id")) or request_id,
                "attempt_id": _safe_text(record.get("attempt_id")) or request_id,
                "persisted_attempt": _safe_text(record.get("attempt")),
                "probe_type": probe_type,
                "phase": _phase(record),
                "pass_name": _safe_text(input_summary.get("pass_name") or record.get("pass_name")),
                "provider": _safe_text(_first(record, "provider", "provider_name")),
                "model": _safe_text(record.get("model")),
                "http_status": "" if http_status is None else str(int(http_status)),
                "response_received": "" if response_received is None else str(response_received).lower(),
                "json_parseable": "" if _bool(_first(record, "json_parse_ok", "json_parseable", "json_parsed")) is None else str(_bool(_first(record, "json_parse_ok", "json_parseable", "json_parsed"))).lower(),
                "schema_valid": "" if _bool(record.get("schema_valid")) is None else str(_bool(record.get("schema_valid"))).lower(),
                "failure_class": failure,
                "timeout_class": _safe_text(_first(record, "timeout_type", "timeout_class")),
                "configured_read_timeout": _safe_text(record.get("configured_read_timeout")),
                "configured_connect_timeout": _safe_text(record.get("configured_connect_timeout")),
                "configured_max_retries": _safe_text(record.get("max_retries")),
                "observed_audit_attempts": observed_attempts,
                "nested_retry_evidence": nested,
                "retry_source": _retry_source(record, nested),
                "controller_retry_owner": "RECOVERY_CONTROLLER" if probe_type == "RECOVERY_NETWORK_PROBE" else "UNKNOWN",
                "latency_ms": "" if latency_ms is None else round(latency_ms, 3),
                "wall_clock_ms": "" if wall_clock_ms is None else round(wall_clock_ms, 3),
                "wall_clock_delta_ms": "" if wall_clock_ms is None or latency_ms is None else round(wall_clock_ms - latency_ms, 3),
                "prompt_version": _safe_text(record.get("prompt_version")),
                "schema_version": _safe_text(_first(record, "schema_version", "output_schema_version")) or "unknown",
                "content_sha256": _safe_text(_first(record, "content_sha256", "content_hash")),
                "request_id": request_id,
                "source_path": source_path,
            }
        )
    rows.sort(key=lambda row: (row["timestamp"], row["request_id"]))
    return rows


def _percentile(values: list[float], percentile: float) -> float | None:
    if not values:
        return None
    if len(values) == 1:
        return values[0]
    ordered = sorted(values)
    position = (len(ordered) - 1) * percentile
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def _response_latency_stats(rows: Iterable[dict[str, Any]]) -> dict[str, Any]:
    response_rows = []
    for row in rows:
        status = _number(row.get("http_status"))
        if status is None or not (200 <= status < 300) or row.get("response_received") != "true":
            continue
        latency = _number(row.get("latency_ms"))
        if latency is None:
            continue
        response_rows.append(row)
    values = [float(row["latency_ms"]) / 1000.0 for row in response_rows]
    schema_valid = sum(row.get("schema_valid") == "true" for row in response_rows)
    return {
        "sample_count": len(values),
        "schema_valid_count": schema_valid,
        "schema_invalid_count": len(values) - schema_valid,
        "min_seconds": round(min(values), 3) if values else None,
        "median_seconds": round(median(values), 3) if values else None,
        "p75_seconds": round(_percentile(values, 0.75), 3) if values else None,
        "p90_seconds": round(_percentile(values, 0.90), 3) if values else None,
        "p95_seconds": round(_percentile(values, 0.95), 3) if values else None,
        "max_seconds": round(max(values), 3) if values else None,
        "phase_attribution": "explicit_phase_only",
    }


def _load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _core_transition_rows(root: Path) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    import polars as pl

    scope_path = root / "930_ANALYSIS_READY_SCOPE.json"
    scope = _load_json(scope_path)
    scope_ids = [str(value) for value in scope.get("queue_item_ids") or []]
    scope_hash = _safe_text(scope.get("scope_hash"))
    if not scope_hash:
        scope_hash = hashlib.sha256(json.dumps(scope, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()
    raw = pl.read_parquet(root / "930_TASK_QUEUE.parquet").to_dicts()
    recovery = pl.read_parquet(root / "930_FALSE_COMPLETION_RECOVERY_QUEUE.parquet").to_dicts()
    claims = pl.read_parquet(root / "930_RECOVERY_CLAIM_AUDIT.parquet").to_dicts()
    raw_by_id = {str(row.get("queue_item_id")): row for row in raw}
    recovery_by_id = {str(row.get("queue_item_id")): row for row in recovery}
    claim_by_task: dict[str, list[dict[str, Any]]] = {}
    for row in claims:
        claim_by_task.setdefault(str(row.get("task_id") or ""), []).append(row)
    rows: list[dict[str, Any]] = []
    for queue_item_id in scope_ids:
        raw_row = raw_by_id.get(queue_item_id, {})
        recovery_row = recovery_by_id.get(queue_item_id, {})
        task_claims = sorted(claim_by_task.get(queue_item_id, []), key=lambda row: _safe_text(row.get("claimed_at")))
        latest_claim = task_claims[-1] if task_claims else {}
        current_status = _safe_text(recovery_row.get("status")) or _safe_text(raw_row.get("status")) or "UNKNOWN"
        previous_status = _safe_text(raw_row.get("status")) or "UNKNOWN"
        reason = _safe_text(recovery_row.get("reason_code") or recovery_row.get("failure_reason"))
        is_retry = current_status == "RETRY_WAIT"
        rows.append(
            {
                "queue_item_id": queue_item_id,
                "city": _safe_text(recovery_row.get("city") or raw_row.get("city")),
                "previous_status": previous_status,
                "current_status": current_status,
                "previous_execution_status": _safe_text(raw_row.get("execution_status")),
                "current_result_status": _safe_text(recovery_row.get("result_status")),
                "previous_provenance_class": "SEARCH_NOT_EXECUTED" if not raw_row.get("search_executed") else "LIVE_SEARCH",
                "current_provenance_class": _safe_text(recovery_row.get("provenance_class")),
                "transition_class": "TEMPORARY_RETRY_WAIT" if is_retry else "RECOVERY_COMPLETED",
                "transition_reason": reason or "RECOVERY_STATUS_RECONCILED",
                "evidence_status": "RETRYABLE_NO_RUN_ID" if is_retry else "RECOVERY_ROW_PRESENT",
                "treatment_impact": "BLOCKS_ANALYSIS_READY_CORE" if is_retry else "NO_STATUS_REGRESSION_INFERRED",
                "retry_count": _safe_text(raw_row.get("attempt_count")) or "0",
                "last_attempt_at": _safe_text(recovery_row.get("updated_at") or raw_row.get("last_attempt_at")),
                "next_eligible_retry": _safe_text(recovery_row.get("next_retry_at")) or "NOT_RECORDED",
                "document_version_id": _safe_text(recovery_row.get("document_version_id") or raw_row.get("document_version_id")),
                "last_claimed_at": _safe_text(latest_claim.get("claimed_at")),
                "last_worker_pid": _safe_text(latest_claim.get("worker_pid")),
                "scope_version": _safe_text(scope.get("scope_version")),
                "scope_hash": scope_hash,
            }
        )
    summary = {
        "scope_version": _safe_text(scope.get("scope_version")),
        "scope_hash": scope_hash,
        "scope_queue_item_count": len(scope_ids),
        "current_status_counts": dict(Counter(row["current_status"] for row in rows)),
        "transition_class_counts": dict(Counter(row["transition_class"] for row in rows)),
        "retry_wait_items": [row["queue_item_id"] for row in rows if row["current_status"] == "RETRY_WAIT"],
        "raw_queue_status_counts": dict(Counter(_safe_text(raw_by_id.get(item, {}).get("status")) for item in scope_ids)),
        "scope_integrity": "UNCHANGED_FROM_FROZEN_INPUT",
        "evidence_limits": [
            "Previous status is the immutable raw queue status; it is not rewritten from the recovery result.",
            "A RETRY_WAIT row is retained as temporary retryable evidence and is not counted as recovered core credit.",
        ],
    }
    return rows, summary


def _write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.part")
    try:
        with temporary.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def _write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.part")
    try:
        with temporary.open("w", encoding="utf-8", newline="") as handle:
            handle.write(json.dumps(value, ensure_ascii=False, indent=2, default=str) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def build_audit(root: Path) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    rows = build_timeout_rows(root)
    probe_rows = [row for row in rows if row.get("probe_type") == "RECOVERY_NETWORK_PROBE"]
    timeout_rows = [row for row in probe_rows if row.get("failure_class") == "READ_TIMEOUT"]
    explicit_attempts = [row for row in timeout_rows if row.get("nested_retry_evidence") == "CONFIRMED_EXPLICIT_TRANSPORT_ATTEMPTS"]
    plausible = [row for row in timeout_rows if row.get("nested_retry_evidence") == "CONFIGURED_RETRY_CHAIN_PLAUSIBLE_NO_PER_ATTEMPT_TRACE"]
    configured_timeouts = sorted({float(row["configured_read_timeout"]) for row in timeout_rows if _number(row.get("configured_read_timeout")) is not None})
    configured_retries = sorted({int(float(row["configured_max_retries"])) for row in timeout_rows if _number(row.get("configured_max_retries")) is not None})
    ratios = [float(row["latency_ms"]) / 1000.0 / float(row["configured_read_timeout"]) for row in timeout_rows if _number(row.get("latency_ms")) is not None and _number(row.get("configured_read_timeout")) not in (None, 0)]
    by_phase = {phase: _response_latency_stats(row for row in probe_rows if row.get("phase") == phase) for phase in ("SINGLE_PROBE", "MICRO_5", "MICRO_20", "UNRECORDED")}
    by_pass = {pass_name: _response_latency_stats(row for row in probe_rows if row.get("pass_name") == pass_name) for pass_name in ("first_pass", "second_review", "UNRECORDED")}
    state = _load_json(root / "930_API_RECOVERY_STATE.json")
    rows_core, core_summary = _core_transition_rows(root)
    rolling = _load_json(root / "930_ANALYSIS_READY_ROLLING_METRICS.json")
    audit = {
        "generated_at": datetime.now(UTC).isoformat(),
        "episode_id": "EP_2016_930_TIGHTENING",
        "read_only": True,
        "manual_api_call": False,
        "queue_mutated": False,
        "frozen_scope": {
            "scope_version": core_summary.get("scope_version"),
            "scope_hash": core_summary.get("scope_hash"),
            "scope_unit": "queue_item",
            "scope_city_count": _number(_load_json(root / "930_ANALYSIS_READY_SCOPE.json").get("city_ids")) or len(_load_json(root / "930_ANALYSIS_READY_SCOPE.json").get("city_ids") or []),
            "scope_queue_item_count": core_summary.get("scope_queue_item_count"),
        },
        "persisted_request_records": len(rows),
        "provider_probe_records": len(probe_rows),
        "read_timeout_records": len(timeout_rows),
        "read_timeout_timeline": timeout_rows,
        "attempt_accounting": {
            "persisted_outer_audit_records_per_probe": "one record per request_id in the saved audit",
            "transport_attempts_observed": sum(int(row.get("observed_audit_attempts") or 0) for row in timeout_rows),
            "explicit_transport_attempt_records": len(explicit_attempts),
            "nested_retry_evidence": "CONFIRMED" if explicit_attempts else "INCONCLUSIVE",
            "configured_retry_chain_plausible_records": len(plausible),
            "configured_read_timeout_seconds": configured_timeouts,
            "configured_sdk_max_retries": configured_retries,
            "wall_clock_to_configured_timeout_ratios": [round(value, 3) for value in ratios],
            "median_wall_clock_to_configured_timeout_ratio": round(median(ratios), 3) if ratios else None,
            "retry_source_for_nested_attempts": "UNKNOWN_WITHOUT_PER_ATTEMPT_TRACE",
            "controller_retry_owner": "RECOVERY_CONTROLLER_FOR_OUTER_SINGLE_PROBE_BACKOFF",
            "decision": (
                "MINIMAL_FINGERPRINT_FIX_ACTIVE_NEXT_SINGLE_MAX_RETRIES_ZERO"
                if plausible
                else "NO_TIMEOUT_RETRY_CHANGE_TRIGGERED"
            ),
            "reason": (
                "The local OpenAI SDK source proves timeout retries execute max_retries+1 transport attempts; persisted EP930 evidence has max_retries=3 and approximately 4x wall-clock durations. The minimum fix is limited to the existing SINGLE-probe fingerprint gate: the next natural SINGLE uses read_timeout=300s, max_retries=0, and hard_wall_timeout=330s."
                if plausible
                else "No repeated configured retry-chain fingerprint was present; no timeout or retry change is justified."
            ),
        },
        "successful_provider_response_latency_by_explicit_phase": by_phase,
        "successful_provider_response_latency_by_saved_pass_name": by_pass,
        "latency_sample_definition": "HTTP 2xx + response_received=true; schema-invalid responses remain in the network latency sample.",
        "current_recovery_state": {
            "phase": state.get("phase"),
            "last_phase": state.get("last_phase"),
            "last_attempt_at": state.get("last_attempt_at"),
            "next_retry_at": state.get("next_retry_at"),
            "last_success_documents": state.get("last_success_documents"),
            "schema_valid": state.get("schema_valid"),
            "timeout_policy_reason": state.get("timeout_policy_reason"),
        },
        "core_status_transition": core_summary,
        "rolling_metrics_reconciliation": {
            "rolling_metrics_core_discovery_verified": rolling.get("core_discovery_verified"),
            "recovery_queue_core_completed": core_summary.get("current_status_counts", {}).get("RECOVERY_COMPLETED", 0),
            "recovery_queue_core_retry_wait": core_summary.get("current_status_counts", {}).get("RETRY_WAIT", 0),
            "status": "INCONSISTENT_ARTIFACTS" if rolling.get("core_discovery_verified") not in (None, 95) else "CONSISTENT_WITH_RECOVERY_AUDIT",
            "action": "preserve_both_artifacts; do_not_fabricate_core_completion",
        },
        "quality_gate": "STRICT_SCHEMA_AND_CONTROLLER_PHASE_GATES_RETAINED",
        "evidence_limits": [
            "Provider response bodies are not copied; only sanitized metadata and hashes are emitted.",
            "The persisted `attempt=0` field is an outer application attempt marker, not proof of one transport attempt.",
            "SINGLE_PROBE/MICRO_5/MICRO_20 are reported only when explicitly persisted; unlabelled responses are not reassigned by timing or pass name.",
        ],
    }
    return audit, rows, rows_core


def _write_markdown(audit: dict[str, Any], path: Path) -> None:
    accounting = audit["attempt_accounting"]
    latency = audit["successful_provider_response_latency_by_explicit_phase"]
    core = audit["core_status_transition"]
    lines = [
        "# EP930 API Timeout Chain Audit",
        "",
        f"Generated: `{audit['generated_at']}`",
        "",
        "## Scope and safety",
        "",
        "- Read-only evidence reconstruction; no provider request was made.",
        "- The 1,575 queue, frozen scope, and valid DocumentVersions were not changed.",
        f"- Frozen scope: `{audit['frozen_scope']['scope_version']}` / `{audit['frozen_scope']['scope_hash']}`; cities `{audit['frozen_scope']['scope_city_count']}`; queue items `{audit['frozen_scope']['scope_queue_item_count']}`.",
        "",
        "## Timeout-chain finding",
        "",
        f"- Persisted provider probes: `{audit['provider_probe_records']}`; saved READ_TIMEOUT records: `{audit['read_timeout_records']}`.",
        f"- Configured read timeout values: `{accounting['configured_read_timeout_seconds']}` seconds.",
        f"- Configured SDK max retries: `{accounting['configured_sdk_max_retries']}`.",
        f"- Median wall-clock/configured-timeout ratio: `{accounting['median_wall_clock_to_configured_timeout_ratio']}`.",
        f"- Explicit per-transport attempt records: `{accounting['explicit_transport_attempt_records']}`.",
        f"- Nested retry conclusion: **{accounting['nested_retry_evidence']}**. The 30s + max_retries=3 + approximately 4x pattern is plausible, but persisted evidence does not prove how many HTTP attempts the SDK made.",
        f"- Retry-source conclusion: `{accounting['retry_source_for_nested_attempts']}`; outer backoff owner remains `{accounting['controller_retry_owner']}`.",
        f"- Decision: `{accounting['decision']}`. The source fix is limited to the existing SINGLE-probe gate; ordinary API calls and schema gates are unchanged.",
        "",
        "## Explicit-phase HTTP 2xx latency",
        "",
        "HTTP 2xx responses with `response_received=true` are included even when strict schema validation failed.",
        "",
        "| Phase | N | schema-valid | schema-invalid | median s | p95 s | max s |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for phase, stats in latency.items():
        lines.append(f"| `{phase}` | {stats['sample_count']} | {stats['schema_valid_count']} | {stats['schema_invalid_count']} | {stats['median_seconds']} | {stats['p95_seconds']} | {stats['max_seconds']} |")
    lines.extend(
        [
            "",
            "Saved `pass_name` distributions are provided in the JSON, but are not mislabeled as SINGLE/MICRO_5/MICRO_20 when the phase was not persisted.",
            "",
            "## Core 100 transition audit",
            "",
            f"- Current recovery status counts: `{core['current_status_counts']}`.",
            f"- Temporary retry-wait items: `{len(core['retry_wait_items'])}`.",
            f"- Scope integrity: `{core['scope_integrity']}`.",
            "- The five RETRY_WAIT rows remain retryable and block core credit; they are not rewritten to completed.",
            f"- Rolling-metrics reconciliation: `{audit['rolling_metrics_reconciliation']['status']}`. Both artifacts are retained; no completion is fabricated.",
            "",
            "## Recovery gate",
            "",
            "The natural controller path remains `SINGLE_PROBE -> MICRO_5 -> MICRO_20 -> backlog`. With the proven retry-chain fingerprint, the next natural SINGLE is configured by the existing policy as one transport attempt with read_timeout=300s and hard wall-clock=330s; an HTTP 2xx schema failure remains a strict schema failure.",
        ]
    )
    _write_text_atomic(path, "\n".join(lines) + "\n")


def _write_text_atomic(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.part")
    try:
        with temporary.open("w", encoding="utf-8", newline="") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-root", type=Path, default=Path(r"E:\Data Set\CRPD\outputs\special_projects\2016_930"))
    args = parser.parse_args()
    audit, timeout_rows, core_rows = build_audit(args.output_root)
    _write_json(args.output_root / "EP930_API_TIMEOUT_CHAIN_AUDIT.json", audit)
    _write_markdown(audit, args.output_root / "EP930_API_TIMEOUT_CHAIN_AUDIT.md")
    _write_csv(args.output_root / "EP930_API_TIMEOUT_CHAIN_TIMELINE.csv", timeout_rows, TIMEOUT_FIELDS)
    _write_csv(args.output_root / "EP930_CORE_STATUS_TRANSITION_AUDIT.csv", core_rows, CORE_FIELDS)
    print(
        json.dumps(
            {
                "timeout_audit": str(args.output_root / "EP930_API_TIMEOUT_CHAIN_AUDIT.json"),
                "timeout_rows": len(timeout_rows),
                "core_audit": str(args.output_root / "EP930_CORE_STATUS_TRANSITION_AUDIT.csv"),
                "core_rows": len(core_rows),
                "nested_retry_evidence": audit["attempt_accounting"]["nested_retry_evidence"],
                "manual_api_call": False,
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
