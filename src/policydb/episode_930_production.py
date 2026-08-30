"""Production orchestration for the 2016 930 historical episode.

This module is intentionally an orchestrator, not a crawler.  It creates the
episode scope and durable queue, then delegates discovery, pagination, HTTP,
attachment handling, retry and document-version checkpointing to the existing
``CrawlService``/``CrawlPipeline``.  The post-fetch work uses the existing
archive, promotion and episode tables while a single ``PolicyWriteLock`` is
held by the job worker.

The older ``episode_930`` module remains available for inspecting the bounded
offline supplement.  This module never calls its direct search/recovery
methods, so an offline report cannot be mistaken for a production run.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import threading
from collections import Counter
from collections.abc import Callable, Mapping
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import polars as pl

from policydb.ai import validate_structured_payload
from policydb.archive import archive_document_versions
from policydb.crawl.dedup import canonicalize_url
from policydb.crawl.registry import load_registry
from policydb.crawl.service import CrawlService, commit_crawl_workspace
from policydb.episode_930 import (
    CORE_END,
    CORE_START,
    EPISODE_ID,
    EPISODE_NAME,
    PROVENANCE_END,
    PROVENANCE_START,
    SEARCH_TERMS,
    SEED_CITIES,
    ActionClassificationPayload,
    Episode930Pipeline,
    EpisodeConfig,
    _id,
    _parse_effective_evidence,
)
from policydb.episode_930_monitor import (
    EXECUTION_MODE,
    analysis_ready_discovery_progress,
    analysis_ready_scope_entities,
    build_monitor_snapshot,
    split_gap_metrics,
    timeout_fingerprint,
)
from policydb.ingest.promote_versions import promote_document_versions
from policydb.jobs.manager import JobManager, PolicyWriteLock
from policydb.jobs.models import CrawlJobRequest
from policydb.parquet_store import atomic_write_parquet, read_parquet_snapshot
from policydb.pdf_pipeline import PDFPipeline, load_pdf_config
from policydb.promotion_audit import build_promotion_gate_trace
from policydb.runtime_context import build_runtime_context
from policydb.scope import load_cities_105
from policydb.settings import Settings
from policydb.transform.normalization import stable_id

PRODUCTION_VERSION = "930-production-v1"
PRIORITY_HOTFIX_VERSION = "ONE-SHOT_RECOVERY_SCHEDULER_HOTFIX_V1"
RECOVERY_REQUIRED_STATUS = "RECOVERY_REQUIRED"
WORK_SOURCE_CORE_RECOVERY = "CORE_RECOVERY"
WORK_SOURCE_CRITICAL_GAP_RECOVERY = "CRITICAL_GAP_RECOVERY"
WORK_SOURCE_FINAL_RECOVERY = "FINAL_RECOVERY"
WORK_SOURCE_ORDINARY_RAW_PENDING = "ORDINARY_RAW_PENDING"
QUEUE_SCHEMA = {
    "queue_item_id": pl.String,
    "episode_id": pl.String,
    "city_id": pl.String,
    "city": pl.String,
    "province": pl.String,
    "task_stage": pl.String,
    "source_id": pl.String,
    "source_role": pl.String,
    "query_type": pl.String,
    "query_text": pl.String,
    "window_start": pl.Date,
    "window_end": pl.Date,
    "status": pl.String,
    "priority": pl.Int64,
    "attempt_count": pl.Int64,
    "lease_owner": pl.String,
    "lease_acquired_at": pl.String,
    "lease_expires_at": pl.String,
    "last_attempt_at": pl.String,
    "completed_at": pl.String,
    "documents_found": pl.Int64,
    "documents_recovered": pl.Int64,
    "actions_extracted": pl.Int64,
    "actions_classified": pl.Int64,
    "pdfs_found": pl.Int64,
    "pdfs_archived": pl.Int64,
    "failure_reason": pl.String,
    "updated_at": pl.String,
    # Derived execution/evidence fields.  The historical ``status`` column
    # remains unchanged so old queue history is not rewritten semantically.
    "execution_status": pl.String,
    "fetch_status": pl.String,
    "result_status": pl.String,
    "search_provider": pl.String,
    "search_executed": pl.Boolean,
    "search_call_count": pl.Int64,
    "search_result_count": pl.Int64,
    "http_request_count": pl.Int64,
    "real_network_fetch": pl.Boolean,
    "last_http_status": pl.Int64,
    "response_bytes": pl.Int64,
    "cache_hit": pl.Boolean,
    "content_sha256": pl.String,
    "crawl_run_id": pl.String,
    "crawl_item_id": pl.String,
    "document_version_id": pl.String,
    "evidence_path": pl.String,
}

SEARCH_PLAN_SCHEMA = {
    "search_plan_id": pl.String,
    "episode_id": pl.String,
    "city_id": pl.String,
    "city": pl.String,
    "province": pl.String,
    "window_name": pl.String,
    "window_start": pl.Date,
    "window_end": pl.Date,
    "query_type": pl.String,
    "query_text": pl.String,
    "keyword_group": pl.String,
    "expected_policy_category": pl.String,
    "official_source": pl.String,
    "fallback_discovery_query": pl.String,
    "priority": pl.Int64,
    "created_at": pl.String,
}

CERTIFICATION_BATCH_LEDGER_NAME = "930_API_CERTIFICATION_BATCH_LEDGER.parquet"
CERTIFICATION_ATTEMPT_LEDGER_NAME = "930_API_CERTIFICATION_ATTEMPTS.parquet"
CERTIFICATION_STAGE_REQUIREMENTS = {"SINGLE": 1, "MICRO_5": 5, "MICRO_20": 20}
CERTIFICATION_STAGE_THRESHOLDS = {"SINGLE": 1.0, "MICRO_5": 0.8, "MICRO_20": 0.8}

CERTIFICATION_BATCH_SCHEMA = {
    "certification_batch_id": pl.String,
    "stage": pl.String,
    "provider": pl.String,
    "model": pl.String,
    "started_at": pl.String,
    "completed_at": pl.String,
    "required_attempts": pl.Int64,
    "attempts_started": pl.Int64,
    "real_provider_attempts": pl.Int64,
    "cache_reuse_count": pl.Int64,
    "schema_valid_successes": pl.Int64,
    "schema_failures": pl.Int64,
    "provider_failures": pl.Int64,
    "timeouts": pl.Int64,
    "other_failures": pl.Int64,
    "pending_attempts": pl.Int64,
    "success_rate": pl.Float64,
    "threshold": pl.Float64,
    "batch_status": pl.String,
    "next_retry_at": pl.String,
    "remaining_slots": pl.Int64,
    "successes_needed": pl.Int64,
    "max_possible_valid": pl.Int64,
    "required_valid": pl.Int64,
    "pass_possible": pl.Boolean,
    "pass_possible_reason": pl.String,
    "run_id": pl.String,
    "updated_at": pl.String,
}

CERTIFICATION_ATTEMPT_SCHEMA = {
    "attempt_id": pl.String,
    "certification_batch_id": pl.String,
    "ordinal": pl.Int64,
    "started_at": pl.String,
    "finished_at": pl.String,
    "provider": pl.String,
    "model": pl.String,
    "input_id": pl.String,
    "request_fingerprint": pl.String,
    "cache_hit": pl.Boolean,
    "provider_attempt": pl.Boolean,
    "http_status": pl.Int64,
    "schema_valid": pl.Boolean,
    "failure_class": pl.String,
    "latency_ms": pl.Float64,
    "provider_request_id": pl.String,
    "prompt_tokens": pl.Int64,
    "completion_tokens": pl.Int64,
    "total_tokens": pl.Int64,
    "estimated_cost_usd": pl.Float64,
    "usage_status": pl.String,
    "status": pl.String,
    "run_id": pl.String,
}


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _now_plus(*, minutes: int) -> str:
    return (datetime.now(UTC) + timedelta(minutes=minutes)).isoformat()


def _atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def load_api_fast_lane_plan(output: Path) -> tuple[pl.DataFrame, Path | None]:
    """Load the newest scope-compatible treatment fast-lane overlay.

    The overlay is an optional, read-only scheduling artifact produced by the
    treatment-universe closure stage.  A missing or stale overlay must never
    block ordinary production work or change the frozen scope.
    """

    root = output / "treatment_universe_closure"
    if not root.exists():
        return pl.DataFrame(), None
    scope = _read_json(output / "930_ANALYSIS_READY_SCOPE.json")
    expected_hash = str(scope.get("scope_hash") or "")
    for candidate in sorted((path for path in root.iterdir() if path.is_dir()), reverse=True):
        plan_path = candidate / "EP930_API_FAST_LANE_PLAN.csv"
        manifest_path = candidate / "EP930_TREATMENT_UNIVERSE_CLOSURE_MANIFEST.json"
        if not plan_path.exists() or not manifest_path.exists():
            continue
        manifest = _read_json(manifest_path)
        manifest_scope = manifest.get("scope") if isinstance(manifest.get("scope"), dict) else {}
        if expected_hash and str(manifest_scope.get("scope_hash") or "") != expected_hash:
            continue
        try:
            return pl.read_csv(plan_path), plan_path
        except (OSError, pl.exceptions.PolarsError):
            continue
    return pl.DataFrame(), None


def api_fast_lane_document_priorities(output: Path) -> dict[str, int]:
    """Return deterministic document priorities for cached API recovery."""

    plan, _ = load_api_fast_lane_plan(output)
    if plan.is_empty() or not {"document_id", "priority"}.issubset(plan.columns):
        return {}
    priorities: dict[str, int] = {}
    for row in plan.iter_rows(named=True):
        document_id = str(row.get("document_id") or "")
        if not document_id:
            continue
        raw_priority = row.get("priority")
        priority = 2 if raw_priority is None else int(raw_priority)
        priorities[document_id] = min(priority, priorities.get(document_id, priority))
    return priorities


def select_api_fast_lane_inputs(
    documents: pl.DataFrame,
    actions: pl.DataFrame,
    plan: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame, dict[str, Any]]:
    """Restrict a paid main-classification call to relevant plan lineage.

    Direct IDs are preferred; URL/content-hash lineage is used for production
    namespaced document IDs.  If no lineage can be proved, the caller receives
    the original inputs and an explicit fallback reason rather than silently
    dropping documents.
    """

    base_metrics = {
        "enabled": False,
        "reason_code": "NO_FAST_LANE_PLAN",
        "plan_actions": 0,
        "plan_documents": 0,
        "selected_actions": actions.height,
        "selected_documents": documents.height,
    }
    if documents.is_empty() or plan.is_empty():
        return documents, actions, base_metrics
    plan_action_ids = {
        str(value)
        for value in plan.get_column("action_id").drop_nulls().cast(pl.String).to_list()
    } if "action_id" in plan.columns else set()
    plan_document_ids = {
        str(value)
        for value in plan.get_column("document_id").drop_nulls().cast(pl.String).to_list()
    } if "document_id" in plan.columns else set()
    plan_hashes = {
        str(value)
        for value in plan.get_column("content_hash").drop_nulls().cast(pl.String).to_list()
    } if "content_hash" in plan.columns else set()
    plan_urls = {
        canonicalize_url(str(value)) or str(value).strip().rstrip("/").lower()
        for value in plan.get_column("official_url").drop_nulls().cast(pl.String).to_list()
        if str(value).strip()
    } if "official_url" in plan.columns else set()
    selected_document_ids: set[str] = set()
    for row in documents.iter_rows(named=True):
        document_id = str(row.get("document_id") or "")
        content_hash = str(row.get("content_hash") or "")
        urls = {
            canonicalize_url(str(row.get(column))) or str(row.get(column)).strip().rstrip("/").lower()
            for column in ("official_url", "canonical_url", "final_url")
            if str(row.get(column) or "").strip()
        }
        if document_id in plan_document_ids or content_hash in plan_hashes or urls.intersection(plan_urls):
            selected_document_ids.add(document_id)
    if not actions.is_empty() and "document_id" in actions.columns:
        action_rows = actions
        if "action_id" in action_rows.columns and plan_action_ids:
            selected_document_ids.update(
                str(value)
                for value in action_rows.filter(
                    pl.col("action_id").cast(pl.String).is_in(sorted(plan_action_ids))
                ).get_column("document_id").drop_nulls().cast(pl.String).to_list()
            )
    selected_document_ids.discard("")
    if not selected_document_ids:
        return documents, actions, {
            **base_metrics,
            "reason_code": "NO_FAST_LANE_LINEAGE_MATCH",
            "plan_actions": plan.height,
            "plan_documents": len(plan_document_ids),
        }
    selected_documents = documents.filter(
        pl.col("document_id").cast(pl.String).is_in(sorted(selected_document_ids))
    ) if "document_id" in documents.columns else documents
    selected_actions = actions.filter(
        pl.col("document_id").cast(pl.String).is_in(sorted(selected_document_ids))
    ) if not actions.is_empty() and "document_id" in actions.columns else actions
    return selected_documents, selected_actions, {
        "enabled": True,
        "reason_code": "FAST_LANE_LINEAGE_MATCH",
        "plan_actions": plan.height,
        "plan_documents": len(plan_document_ids),
        "selected_actions": selected_actions.height,
        "selected_documents": selected_documents.height,
    }


QUEUE_DERIVED_DEFAULTS: dict[str, tuple[pl.DataType, object]] = {
    "execution_status": (pl.String, "PENDING"),
    "fetch_status": (pl.String, "NOT_ATTEMPTED"),
    "result_status": (pl.String, "NO_RESULT"),
    "search_provider": (pl.String, None),
    "search_executed": (pl.Boolean, False),
    "search_call_count": (pl.Int64, 0),
    "search_result_count": (pl.Int64, 0),
    "http_request_count": (pl.Int64, 0),
    "real_network_fetch": (pl.Boolean, False),
    "last_http_status": (pl.Int64, None),
    "response_bytes": (pl.Int64, 0),
    "cache_hit": (pl.Boolean, False),
    "content_sha256": (pl.String, None),
    "crawl_run_id": (pl.String, None),
    "crawl_item_id": (pl.String, None),
    "document_version_id": (pl.String, None),
    "evidence_path": (pl.String, None),
}


def _ensure_queue_columns(queue: pl.DataFrame) -> pl.DataFrame:
    """Upgrade old queue snapshots without changing historical status values."""

    result = queue
    for column, (dtype, default) in QUEUE_DERIVED_DEFAULTS.items():
        if column not in result.columns:
            result = result.with_columns(pl.lit(default).cast(dtype).alias(column))
    return result


def _date(value: object) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if value in (None, ""):
        return None
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def _certification_stage(value: object) -> str:
    normalized = str(value or "SINGLE").upper()
    return {
        "PROBE": "SINGLE",
        "SINGLE_PROBE": "SINGLE",
        "BACKOFF": "SINGLE",
        "BACKOFF_SINGLE_PROBE": "SINGLE",
    }.get(normalized, normalized)


def _certification_runtime_phase(value: object) -> str:
    """Map persisted batch stages to the recovery state-machine phase names."""

    stage = _certification_stage(value)
    return "SINGLE_PROBE" if stage == "SINGLE" else stage


def certification_batch_feasibility(
    stage: object,
    real_provider_attempts: int,
    schema_valid_successes: int,
) -> dict[str, Any]:
    """Expose whether a bounded certification batch can still pass.

    This is a derived audit invariant only.  The existing policy still
    requires all scheduled real attempts before a batch can be finalized;
    ``pass_possible=False`` therefore marks diagnostic-only continuation and
    does not terminate or reset the batch.
    """

    normalized = _certification_stage(stage)
    required_attempts = CERTIFICATION_STAGE_REQUIREMENTS.get(normalized)
    if required_attempts is None:
        return {
            "remaining_slots": 0,
            "successes_needed": 0,
            "max_possible_valid": 0,
            "required_valid": 0,
            "pass_possible": False,
            "pass_possible_reason": "UNKNOWN_CERTIFICATION_STAGE",
        }
    required_valid = int(required_attempts * CERTIFICATION_STAGE_THRESHOLDS[normalized])
    attempts = max(0, int(real_provider_attempts))
    valid = max(0, min(int(schema_valid_successes), attempts))
    remaining_slots = max(0, required_attempts - attempts)
    successes_needed = max(0, required_valid - valid)
    max_possible_valid = valid + remaining_slots
    pass_possible = successes_needed <= remaining_slots
    return {
        "remaining_slots": remaining_slots,
        "successes_needed": successes_needed,
        "max_possible_valid": max_possible_valid,
        "required_valid": required_valid,
        "pass_possible": pass_possible,
        "pass_possible_reason": (
            "PASS_STILL_POSSIBLE"
            if pass_possible
            else "PASS_THRESHOLD_MATHEMATICALLY_UNREACHABLE"
        ),
    }


def certification_batch_transition(
    stage: str,
    attempted_documents: int,
    successful_documents: int,
    schema_valid: bool,
    *,
    schema_valid_successes: int | None = None,
    cache_reuse_count: int = 0,
) -> dict[str, Any]:
    """Advance a certification batch only after its real-attempt quota is met.

    ``attempted_documents`` is deliberately the count of uncached provider
    requests.  Cache reuse is reported separately and can never fill a batch
    slot.  A partial micro batch remains in that stage; a failed attempt may
    enter backoff but keeps the same batch identity for resumption.
    """

    normalized = _certification_stage(stage)
    required = CERTIFICATION_STAGE_REQUIREMENTS.get(normalized)
    if required is None:
        return {
            "stage": normalized,
            "next_phase": "BACKOFF_SINGLE_PROBE",
            "batch_status": "FAIL",
            "reason_code": "UNKNOWN_CERTIFICATION_STAGE",
            "backoff": True,
            "required_attempts": 0,
            "attempts_started": 0,
            "real_provider_attempts": 0,
            "schema_valid_successes": 0,
            "pending_attempts": 0,
            "success_rate": 0.0,
            "threshold": 0.8,
            "cache_reuse_count": max(0, int(cache_reuse_count)),
        }

    attempted = max(0, int(attempted_documents))
    successful = max(0, min(int(successful_documents), attempted))
    valid_successful = (
        max(0, min(int(schema_valid_successes), attempted))
        if schema_valid_successes is not None
        else successful if schema_valid else 0
    )
    threshold = CERTIFICATION_STAGE_THRESHOLDS[normalized]
    rate = valid_successful / attempted if attempted else 0.0
    complete = attempted >= required
    passed = complete and valid_successful >= int(required * threshold) and (
        normalized != "SINGLE" or valid_successful == 1
    )
    pending = max(0, required - attempted)

    if normalized == "SINGLE":
        if passed:
            return {
                "stage": normalized,
                "next_phase": "MICRO_5",
                "batch_status": "PASS",
                "reason_code": "SINGLE_PROBE_SUCCESS_SCHEMA_VALID",
                "backoff": False,
                "required_attempts": required,
                "attempts_started": attempted,
                "real_provider_attempts": attempted,
                "schema_valid_successes": valid_successful,
                "pending_attempts": 0,
                "success_rate": rate,
                "threshold": threshold,
                "cache_reuse_count": max(0, int(cache_reuse_count)),
            }
        return {
            "stage": normalized,
            "next_phase": "BACKOFF_SINGLE_PROBE",
            "batch_status": "RETRY_WAIT" if attempted else "RUNNING",
            "reason_code": "SINGLE_PROBE_FAILED_OR_SCHEMA_INVALID",
            "backoff": bool(attempted),
        } | {
            "required_attempts": required,
            "attempts_started": attempted,
            "real_provider_attempts": attempted,
            "schema_valid_successes": valid_successful,
            "pending_attempts": pending,
            "success_rate": rate,
            "threshold": threshold,
            "cache_reuse_count": max(0, int(cache_reuse_count)),
        }

    if not complete:
        failed_attempt = attempted > 0 and (
            not schema_valid or valid_successful < successful
        )
        return {
            "stage": normalized,
            "next_phase": "BACKOFF_SINGLE_PROBE" if failed_attempt else normalized,
            "batch_status": "RETRY_WAIT" if failed_attempt else "RUNNING",
            "reason_code": f"{normalized}_ATTEMPTS_INCOMPLETE",
            "backoff": failed_attempt,
            "required_attempts": required,
            "attempts_started": attempted,
            "real_provider_attempts": attempted,
            "schema_valid_successes": valid_successful,
            "pending_attempts": pending,
            "success_rate": rate,
            "threshold": threshold,
            "cache_reuse_count": max(0, int(cache_reuse_count)),
        }

    if passed:
        next_phase = "MICRO_20" if normalized == "MICRO_5" else "BACKLOG_CONSUMPTION"
        reason = (
            "MICRO_5_SUCCESS_RATE_GE_80_SCHEMA_VALID"
            if normalized == "MICRO_5"
            else "MICRO_20_STABLE"
        )
        return {
            "stage": normalized,
            "next_phase": next_phase,
            "batch_status": "PASS",
            "reason_code": reason,
            "backoff": False,
            "required_attempts": required,
            "attempts_started": attempted,
            "real_provider_attempts": attempted,
            "schema_valid_successes": valid_successful,
            "pending_attempts": 0,
            "success_rate": rate,
            "threshold": threshold,
            "cache_reuse_count": max(0, int(cache_reuse_count)),
        }

    return {
        "stage": normalized,
        "next_phase": "BACKOFF_SINGLE_PROBE",
        "batch_status": "FAIL",
        "reason_code": (
            "MICRO_5_SUCCESS_RATE_BELOW_80_OR_SCHEMA_INVALID"
            if normalized == "MICRO_5"
            else "MICRO_20_UNSTABLE"
        ),
        "backoff": True,
        "required_attempts": required,
        "attempts_started": attempted,
        "real_provider_attempts": attempted,
        "schema_valid_successes": valid_successful,
        "pending_attempts": 0,
        "success_rate": rate,
        "threshold": threshold,
        "cache_reuse_count": max(0, int(cache_reuse_count)),
    }


def api_recovery_transition(
    phase: str,
    attempted_documents: int,
    successful_documents: int,
    schema_valid: bool,
    *,
    schema_valid_successes: int | None = None,
    cache_reuse_count: int = 0,
) -> dict[str, Any]:
    """Apply the bounded API recovery state machine without making a call."""

    normalized = str(phase or "SINGLE_PROBE").upper()
    normalized = {"PROBE": "SINGLE_PROBE", "BACKOFF": "BACKOFF_SINGLE_PROBE"}.get(normalized, normalized)
    if normalized in {"BACKLOG_CONSUMPTION", "STABLE_BACKLOG_CONSUMPTION"}:
        attempted = max(0, int(attempted_documents))
        successful = max(0, min(int(successful_documents), attempted))
        valid_successful = (
            max(0, min(int(schema_valid_successes), attempted))
            if schema_valid_successes is not None
            else successful if schema_valid else 0
        )
        rate = valid_successful / attempted if attempted else 0.0
        stable = bool(attempted and rate >= 0.8 and schema_valid)
        return {
            "stage": "BACKLOG_CONSUMPTION",
            "next_phase": "BACKLOG_CONSUMPTION" if stable else "BACKOFF_SINGLE_PROBE",
            "batch_status": "NOT_APPLICABLE" if stable else "RETRY_WAIT",
            "reason_code": "BACKLOG_CONSUMPTION_STABLE" if stable else "UNKNOWN_OR_UNSTABLE_RECOVERY_PHASE",
            "backoff": not stable,
            "required_attempts": 0,
            "attempts_started": attempted,
            "real_provider_attempts": attempted,
            "schema_valid_successes": valid_successful,
            "pending_attempts": 0,
            "success_rate": rate,
            "threshold": 0.8,
            "cache_reuse_count": max(0, int(cache_reuse_count)),
        }
    if normalized in {"SINGLE_PROBE", "BACKOFF_SINGLE_PROBE"}:
        return certification_batch_transition(
            "SINGLE",
            attempted_documents,
            successful_documents,
            schema_valid,
            schema_valid_successes=schema_valid_successes,
            cache_reuse_count=cache_reuse_count,
        )
    return certification_batch_transition(
        normalized,
        attempted_documents,
        successful_documents,
        schema_valid,
        schema_valid_successes=schema_valid_successes,
        cache_reuse_count=cache_reuse_count,
    )


def read_certification_ledger(output: Path) -> pl.DataFrame:
    """Read the append/replace certification ledger without inferring PASS."""

    path = Path(output) / CERTIFICATION_BATCH_LEDGER_NAME
    if not path.exists():
        return pl.DataFrame(schema=CERTIFICATION_BATCH_SCHEMA)
    try:
        return read_parquet_snapshot(path)
    except Exception:
        return pl.DataFrame(schema=CERTIFICATION_BATCH_SCHEMA)


def certification_gate_from_ledger(rows: pl.DataFrame | list[Mapping[str, Any]]) -> dict[str, Any]:
    """Compute certification strictly from complete persisted batches."""

    records = rows.to_dicts() if isinstance(rows, pl.DataFrame) else [dict(row) for row in rows]
    stages: dict[str, dict[str, Any]] = {}
    for stage, required in CERTIFICATION_STAGE_REQUIREMENTS.items():
        candidates = [row for row in records if _certification_stage(row.get("stage")) == stage]
        candidates.sort(key=lambda row: str(row.get("updated_at") or row.get("started_at") or ""))
        row = candidates[-1] if candidates else {}
        attempts = int(row.get("real_provider_attempts") or row.get("attempts_started") or 0)
        valid = int(row.get("schema_valid_successes") or 0)
        threshold = CERTIFICATION_STAGE_THRESHOLDS[stage]
        passed = (
            str(row.get("batch_status") or "") == "PASS"
            and attempts == required
            and valid >= int(required * threshold)
        )
        feasibility = certification_batch_feasibility(stage, attempts, valid)
        stages[stage] = {
            "certification_batch_id": row.get("certification_batch_id"),
            "required_attempts": required,
            "real_provider_attempts": attempts,
            "schema_valid_successes": valid,
            "cache_reuse_count": int(row.get("cache_reuse_count") or 0),
            "pending_attempts": max(0, required - attempts),
            **feasibility,
            "success_rate": valid / attempts if attempts else 0.0,
            "threshold": threshold,
            "batch_status": row.get("batch_status") or "NOT_STARTED",
            "passed": passed,
        }
    certified = all(stage["passed"] for stage in stages.values())
    return {
        "certification": "PASS" if certified else "BLOCKED_BY_CERTIFICATION_BATCH",
        "stages": stages,
        "required_stages": list(CERTIFICATION_STAGE_REQUIREMENTS),
        "reason_code": "ALL_CERTIFICATION_BATCHES_COMPLETE" if certified else "CERTIFICATION_BATCH_INCOMPLETE",
    }


def summarize_api_recovery_probe(
    document_ids: set[str],
    audit_rows: list[dict[str, Any]],
    classifier_metrics: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Separate real provider probe evidence from classifier cache reuse.

    Recovery state must be derived from persisted request records.  A cached
    classifier response can produce rows without creating a new request record,
    so it must never advance the ``SINGLE_PROBE`` gate or be reported as a
    provider attempt.
    """

    ids = {str(value) for value in document_ids if value}
    records = [row for row in audit_rows if str(row.get("slot_id") or "") in ids]
    success_rows = [row for row in records if row.get("status") == "response_completed"]
    attempted_ids = {str(row.get("slot_id")) for row in records if row.get("slot_id")}
    success_ids = {str(row.get("slot_id")) for row in success_rows if row.get("slot_id")}
    schema_valid_ids = {
        str(row.get("slot_id"))
        for row in success_rows
        if row.get("slot_id")
        and isinstance(row.get("response_payload"), dict)
        and isinstance((row.get("response_payload") or {}).get("actions"), list)
    }
    metrics = classifier_metrics or {}
    cache_reuse_ids = {
        str(value)
        for value in (metrics.get("api_cache_hit_document_ids") or [])
        if str(value) in ids
    }
    reason_code = "CACHE_REUSE_NOT_A_PROVIDER_PROBE" if cache_reuse_ids and not attempted_ids else None
    return {
        "attempted_ids": attempted_ids,
        "success_ids": success_ids,
        "schema_valid_ids": schema_valid_ids,
        "cache_reuse_ids": cache_reuse_ids,
        "reason_code": reason_code,
    }


def recovery_timeout_policy(
    phase: str,
    *,
    client_timeout_suspected: bool,
    connect_timeout: float,
) -> dict[str, Any]:
    """Return the only permitted timeout override for recovery probes.

    The override applies to every certification stage while the SDK retry-chain
    fingerprint shows the provider needs longer than the default 30s read
    timeout (observed SINGLE latency 69-157s).  Applying 30s + 3 SDK retries to
    MICRO_5/MICRO_20 makes every micro batch fail (~127s wall clock) after a
    successful SINGLE, so certification can never advance past the gate.
    """

    normalized = str(phase or "").upper()
    if normalized in {
        "PROBE",
        "BACKOFF",
        "BACKOFF_SINGLE_PROBE",
        "SINGLE_PROBE",
        "MICRO_5",
        "MICRO_20",
    } and client_timeout_suspected:
        return {
            "read_timeout": 300.0,
            "connect_timeout": float(connect_timeout),
            "max_retries": 0,
            "hard_wall_timeout_seconds": 330,
            "reason_code": "CLIENT_READ_TIMEOUT_SUSPECTED_SINGLE_PROBE",
        }
    return {
        "read_timeout": None,
        "connect_timeout": None,
        "max_retries": None,
        "hard_wall_timeout_seconds": None,
        "reason_code": "NORMAL_API_TIMEOUTS_UNCHANGED",
    }


def api_classification_allowed(
    recovery_metrics: dict[str, Any],
    *,
    recovery_queue_rows: int,
) -> bool:
    """Prevent the normal classification path from bypassing recovery gates."""

    if recovery_queue_rows <= 0:
        return True
    return str(recovery_metrics.get("recovery_gate") or "").upper() in {
        "BACKLOG_CONSUMPTION",
        "STABLE_BACKLOG_CONSUMPTION",
    }


def freeze_analysis_ready_scope(
    path: Path,
    queue: pl.DataFrame,
    *,
    created_at: str | None = None,
) -> dict[str, Any]:
    """Freeze an outcome-independent core scope exactly once."""

    existing = _read_json(path)
    if existing:
        return existing
    rows = []
    if not queue.is_empty():
        rows = [
            row
            for row in queue.iter_rows(named=True)
            if str(row.get("city") or "") in SEED_CITIES
            and int(row.get("priority") or 0) == 10
            and _date(row.get("window_start")) == CORE_START
            and _date(row.get("window_end")) == CORE_END
        ]
    city_pairs = sorted(
        {
            (str(row.get("city_id") or ""), str(row.get("city") or ""))
            for row in rows
            if row.get("city_id") and row.get("city")
        }
    )
    basis = {
        "scope_version": "930-analysis-ready-v1",
        "selection_rule": "predefined SEED_CITIES intersect priority=10 core-window queue; no outcome, regression, significance, or API-result fields",
        "city_ids": [city_id for city_id, _city in city_pairs],
        "cities": [city for _city_id, city in city_pairs],
        "queue_item_ids": sorted(
            str(row.get("queue_item_id"))
            for row in rows
            if row.get("queue_item_id")
        ),
        "document_eligibility_rule": "official formal document; city in frozen scope; announcement/publication date within core window; deterministic episode relevance",
        "episode_window": [CORE_START.isoformat(), CORE_END.isoformat()],
        "source_requirements": [
            "official evidence valid",
            "formal source eligibility",
            "document provenance retained",
        ],
    }
    scope_hash = analysis_ready_scope_hash(basis)
    payload = {
        **basis,
        "city_count": len(city_pairs),
        "created_at": created_at or _now(),
        "scope_hash": scope_hash,
        "frozen": True,
    }
    _atomic_json(path, payload)
    return payload


def analysis_ready_scope_hash(scope: dict[str, Any]) -> str:
    """Recompute the immutable hash from the frozen scope basis only."""

    basis = {
        key: scope.get(key)
        for key in (
            "scope_version",
            "selection_rule",
            "city_ids",
            "cities",
            "queue_item_ids",
            "document_eligibility_rule",
            "episode_window",
            "source_requirements",
        )
    }
    return hashlib.sha256(
        json.dumps(basis, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def validate_frozen_analysis_ready_scope(scope: dict[str, Any]) -> str | None:
    """Reject a changed frozen scope before it can affect scheduling."""

    stored = str(scope.get("scope_hash") or "")
    if not stored:
        return None
    computed = analysis_ready_scope_hash(scope)
    if computed != stored:
        raise ValueError(
            "frozen Analysis-ready scope hash mismatch: "
            f"stored={stored} computed={computed}"
        )
    return stored


def _safe_priority(value: object, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


_LINEAGE_HASH_FIELDS = ("content_hash", "content_sha256")
_LINEAGE_URL_FIELDS = ("canonical_url", "official_url", "final_url")


def _lineage_identity_tokens(row: Mapping[str, Any]) -> set[tuple[str, str]]:
    tokens: set[tuple[str, str]] = set()
    for field in _LINEAGE_HASH_FIELDS:
        value = str(row.get(field) or "").strip()
        if value:
            tokens.add(("hash", value))
    for field in _LINEAGE_URL_FIELDS:
        value = str(row.get(field) or "").strip()
        if value.startswith(("http://", "https://")):
            canonical = canonicalize_url(value)
            if canonical:
                tokens.add(("url", canonical))
    return tokens


def _lineage_city_matches(
    core_row: Mapping[str, Any], production_row: Mapping[str, Any]
) -> bool:
    core_city_id = str(core_row.get("city_id") or "")
    production_city_id = str(production_row.get("city_id") or "")
    if core_city_id and production_city_id and core_city_id != production_city_id:
        return False
    core_city = str(core_row.get("city") or "")
    production_city = str(production_row.get("city") or "")
    return not (core_city and production_city and core_city != production_city)


def build_core_document_lineage(
    scope_entities: dict[str, Any], production_documents: pl.DataFrame
) -> dict[str, str]:
    """Map production IDs to frozen IDs using exact stable identity only.

    Production and curated artifacts may use different document ID namespaces,
    but city membership alone is never a document lineage key.  Ambiguous or
    cross-city matches are intentionally omitted so the frozen scope receives
    no credit without deterministic evidence.
    """

    core_documents = scope_entities.get("core_documents")
    if not isinstance(core_documents, pl.DataFrame) or core_documents.is_empty():
        return {}
    if "document_id" not in core_documents.columns:
        return {}
    core_rows = core_documents.to_dicts()
    core_by_id = {
        str(row.get("document_id")): row
        for row in core_rows
        if row.get("document_id") not in (None, "")
    }
    identity_to_core_ids: dict[tuple[str, str], set[str]] = {}
    for core_id, row in core_by_id.items():
        for token in _lineage_identity_tokens(row):
            identity_to_core_ids.setdefault(token, set()).add(core_id)

    lineage: dict[str, str] = {}
    if production_documents.is_empty() or "document_id" not in production_documents.columns:
        return lineage
    for row in production_documents.to_dicts():
        production_id = str(row.get("document_id") or "")
        if not production_id:
            continue
        matched_core_ids: set[str] = set()
        for token in _lineage_identity_tokens(row):
            matched_core_ids.update(identity_to_core_ids.get(token, set()))
        if len(matched_core_ids) != 1:
            continue
        core_id = next(iter(matched_core_ids))
        if _lineage_city_matches(core_by_id[core_id], row):
            lineage[production_id] = core_id
    return lineage


def core_action_coverage_metrics(
    coverage: pl.DataFrame,
    scope_entities: dict[str, Any],
    *,
    document_lineage: Mapping[str, str] | None = None,
) -> tuple[int, int]:
    """Count core action coverage using direct IDs or exact document lineage.

    A production ``DOC930PROD_*`` row receives frozen-core credit only when
    ``document_lineage`` explicitly maps it to a frozen ``DOC930_*`` ID.
    Sharing a city is not sufficient evidence.
    """

    core_document_ids = {
        str(value)
        for value in (scope_entities.get("core_document_ids") or set())
        if value not in (None, "")
    }
    lineage = {
        str(source): str(target)
        for source, target in (document_lineage or {}).items()
        if source not in (None, "") and target not in (None, "")
    }
    if coverage.is_empty():
        return 0, 0

    eligible = 0
    completed = 0
    for row in coverage.iter_rows(named=True):
        document_id = str(row.get("document_id") or "")
        linked_document_id = lineage.get(document_id, document_id)
        is_core = linked_document_id in core_document_ids
        if not is_core or row.get("eligible") is False:
            continue
        eligible += 1
        if str(row.get("status") or "").upper() == "COMPLETED":
            completed += 1
    return eligible, completed


def load_authoritative_episode_gaps(
    curated_path: Path,
    fallback_paths: tuple[Path, ...] = (),
) -> pl.DataFrame:
    """Load the Curated gap register before run-local fallbacks.

    Rolling metrics and the Analysis-ready gate must use the same Curated
    register. A run-local register remains a compatibility fallback for
    isolated or historical runs where the Curated snapshot is unavailable.
    """

    for path in (curated_path, *fallback_paths):
        if not path.exists():
            continue
        frame = read_parquet_snapshot(path)
        if not frame.is_empty():
            return frame
    return pl.DataFrame()


def _active_lease(value: object, now: datetime) -> bool:
    if value in (None, ""):
        return False
    try:
        expires_at = datetime.fromisoformat(str(value))
    except (TypeError, ValueError):
        return False
    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=UTC)
    return expires_at > now


def _work_source_for_priority(priority: int) -> str:
    return {
        0: WORK_SOURCE_CORE_RECOVERY,
        1: WORK_SOURCE_CRITICAL_GAP_RECOVERY,
        2: WORK_SOURCE_FINAL_RECOVERY,
    }.get(_safe_priority(priority, 2), WORK_SOURCE_FINAL_RECOVERY)


def derive_recovery_priority(
    recovery: pl.DataFrame,
    queue: pl.DataFrame,
    scope: dict[str, Any],
    *,
    critical_city_ids: set[str] | None = None,
    critical_city_names: set[str] | None = None,
) -> pl.DataFrame:
    """Derive scheduling priority at read time from authoritative identities.

    ``priority_lane`` and ``priority_reason`` may be absent or stale in an
    overlay regenerated by an older runner.  The exact frozen queue-item set
    is the only source for P0; critical-gap membership is an explicit
    secondary source; everything else is P2.  The source frame is never
    filtered or rewritten by this function.
    """

    if recovery.is_empty():
        return recovery
    validate_frozen_analysis_ready_scope(scope)
    queue_rows = queue.to_dicts()
    queue_by_id = {
        str(row.get("queue_item_id") or ""): (index, row)
        for index, row in enumerate(queue_rows)
        if row.get("queue_item_id")
    }
    scope_queue_ids = {
        str(value)
        for value in (scope.get("queue_item_ids") or [])
        if value not in (None, "")
    }
    critical_ids = {str(value) for value in (critical_city_ids or set())}
    critical_names = {str(value) for value in (critical_city_names or set())}
    fallback_order = len(queue_rows)
    queue_city = {
        str(row.get("queue_item_id") or ""): str(row.get("city_id") or "")
        for row in queue_rows
    }
    rows: list[dict[str, Any]] = []
    for recovery_order, row in enumerate(recovery.iter_rows(named=True)):
        item = dict(row)
        queue_item_id = str(item.get("queue_item_id") or "")
        queue_index, queue_row = queue_by_id.get(queue_item_id, (fallback_order + recovery_order, {}))
        city_id = queue_city.get(queue_item_id, str(item.get("city_id") or ""))
        city = str(queue_row.get("city") or item.get("city") or "")
        core_member = queue_item_id in scope_queue_ids
        critical_member = city_id in critical_ids or city in critical_names
        normalized_priority = 0 if core_member else 1 if critical_member else 2
        reason = (
            "ANALYSIS_READY_CORE"
            if core_member
            else "CRITICAL_GAP_CLOSURE"
            if critical_member
            else "GLOBAL_FINAL_RECOVERY"
        )
        existing_secondary_priority = _safe_priority(
            queue_row.get("priority"),
            _safe_priority(item.get("priority"), 0),
        )
        item.update(
            {
                "city_id": city_id or item.get("city_id"),
                "city": city or item.get("city"),
                "normalized_priority": normalized_priority,
                # Keep the legacy column for old monitor/runner readers, but
                # never use its stored value as an input to this derivation.
                "priority_lane": normalized_priority,
                "priority_reason": reason,
                "core_scope_member": core_member,
                "critical_gap_member": critical_member,
                "existing_secondary_priority": existing_secondary_priority,
                "original_stable_order": queue_index,
            }
        )
        rows.append(item)
    return pl.DataFrame(rows, infer_schema_length=None).sort(
        ["normalized_priority", "existing_secondary_priority", "original_stable_order", "queue_item_id"]
    )


def prioritize_recovery_queue(
    recovery: pl.DataFrame,
    queue: pl.DataFrame,
    scope: dict[str, Any],
    *,
    critical_city_ids: set[str] | None = None,
    critical_city_names: set[str] | None = None,
) -> pl.DataFrame:
    """Backward-compatible name for the read-time priority derivation."""

    return derive_recovery_priority(
        recovery,
        queue,
        scope,
        critical_city_ids=critical_city_ids,
        critical_city_names=critical_city_names,
    )


def select_recovery_claim_rows(
    recovery: pl.DataFrame,
    queue: pl.DataFrame,
    scope: dict[str, Any],
    cities: list[str],
    *,
    critical_city_ids: set[str] | None = None,
    critical_city_names: set[str] | None = None,
    now: datetime | str | None = None,
) -> list[dict[str, Any]]:
    """Select only the highest currently-required recovery lane.

    A valid lease or any non-``RECOVERY_REQUIRED`` state is ineligible.  If
    P0 work exists for the selected cities, P1/P2 rows are deliberately not
    used to fill the remaining city slots.
    """

    if recovery.is_empty() or queue.is_empty() or not cities:
        return []
    if isinstance(now, str):
        current_time = datetime.fromisoformat(now)
    else:
        current_time = now or datetime.now(UTC)
    if current_time.tzinfo is None:
        current_time = current_time.replace(tzinfo=UTC)
    city_set = {str(city) for city in cities}
    prioritized = derive_recovery_priority(
        recovery,
        queue,
        scope,
        critical_city_ids=critical_city_ids,
        critical_city_names=critical_city_names,
    )
    queue_by_id = {
        str(row.get("queue_item_id") or ""): row
        for row in queue.iter_rows(named=True)
        if row.get("queue_item_id")
    }
    candidates: list[dict[str, Any]] = []
    for row in prioritized.iter_rows(named=True):
        queue_item_id = str(row.get("queue_item_id") or "")
        recovery_status = str(row.get("status") or "")
        if recovery_status != RECOVERY_REQUIRED_STATUS:
            continue
        if _active_lease(row.get("lease_expires_at"), current_time):
            continue
        raw = queue_by_id.get(queue_item_id)
        if raw is None:
            continue
        if str(raw.get("status") or "") == "RUNNING" and _active_lease(raw.get("lease_expires_at"), current_time):
            continue
        candidate = dict(row)
        candidate["raw_status"] = raw.get("status")
        candidate["raw_lease_expires_at"] = raw.get("lease_expires_at")
        candidate["recovery_status"] = recovery_status
        candidate["raw_city_id"] = str(raw.get("city_id") or "")
        candidates.append(candidate)
    if not candidates:
        return []
    minimum_priority = min(_safe_priority(row.get("normalized_priority"), 2) for row in candidates)
    selected: list[dict[str, Any]] = []
    seen_cities: set[str] = set()
    for row in sorted(
        (
            candidate
            for candidate in candidates
            if _safe_priority(candidate.get("normalized_priority"), 2) == minimum_priority
            and str(candidate.get("raw_city_id") or "") in city_set
        ),
        key=lambda candidate: (
            _safe_priority(candidate.get("normalized_priority"), 2),
            _safe_priority(candidate.get("existing_secondary_priority"), 0),
            _safe_priority(candidate.get("original_stable_order"), 0),
            str(candidate.get("queue_item_id") or ""),
        ),
    ):
        city_id = str(row.get("city_id") or "")
        if city_id in seen_cities:
            continue
        selected.append(row)
        seen_cities.add(city_id)
        if len(selected) >= len(city_set):
            break
    return selected


def select_next_work_source(
    recovery: pl.DataFrame,
    queue: pl.DataFrame,
    scope: dict[str, Any],
    *,
    city_limit: int,
    critical_city_ids: set[str] | None = None,
    critical_city_names: set[str] | None = None,
    now: datetime | str | None = None,
) -> dict[str, Any]:
    """Choose the next global lane before ordinary raw work is scheduled.

    Recovery priority is a derived scheduling property, not a stored overlay
    order.  A required higher lane therefore prevents a lower lane *and* the
    ordinary raw queue from being selected.  Active leases remain visible in
    the required counts but are never preempted or claimed again.
    """

    if isinstance(now, str):
        current_time = datetime.fromisoformat(now)
    else:
        current_time = now or datetime.now(UTC)
    if current_time.tzinfo is None:
        current_time = current_time.replace(tzinfo=UTC)

    base: dict[str, Any] = {
        "work_source": WORK_SOURCE_ORDINARY_RAW_PENDING,
        "normalized_priority": None,
        "priority_reason": "NO_RECOVERY_REQUIRED",
        "core_scope_member": False,
        "critical_gap_member": False,
        "queue_item_ids": [],
        "cities": [],
        "required_by_priority": {"0": 0, "1": 0, "2": 0},
        "eligible_by_priority": {"0": 0, "1": 0, "2": 0},
        "recovery_required": 0,
        "recovery_eligible": 0,
        "blocked_by_active_lease": False,
        "reason_code": "NO_RECOVERY_REQUIRED",
    }
    if recovery.is_empty() or queue.is_empty():
        return base

    prioritized = derive_recovery_priority(
        recovery,
        queue,
        scope,
        critical_city_ids=critical_city_ids,
        critical_city_names=critical_city_names,
    )
    queue_by_id = {
        str(row.get("queue_item_id") or ""): row
        for row in queue.iter_rows(named=True)
        if row.get("queue_item_id")
    }
    required_rows: list[dict[str, Any]] = []
    eligible_rows: list[dict[str, Any]] = []
    required_by_priority = {"0": 0, "1": 0, "2": 0}
    eligible_by_priority = {"0": 0, "1": 0, "2": 0}
    for row in prioritized.iter_rows(named=True):
        if str(row.get("status") or "") != RECOVERY_REQUIRED_STATUS:
            continue
        queue_item_id = str(row.get("queue_item_id") or "")
        raw = queue_by_id.get(queue_item_id)
        if raw is None:
            continue
        priority = min(2, max(0, _safe_priority(row.get("normalized_priority"), 2)))
        priority_key = str(priority)
        required_by_priority[priority_key] += 1
        candidate = dict(row)
        candidate["raw_status"] = raw.get("status")
        candidate["raw_city_id"] = str(raw.get("city_id") or "")
        required_rows.append(candidate)
        recovery_lease_active = _active_lease(row.get("lease_expires_at"), current_time)
        raw_lease_active = (
            str(raw.get("status") or "") == "RUNNING"
            and _active_lease(raw.get("lease_expires_at"), current_time)
        )
        if recovery_lease_active or raw_lease_active:
            continue
        eligible_by_priority[priority_key] += 1
        eligible_rows.append(candidate)

    base["required_by_priority"] = required_by_priority
    base["eligible_by_priority"] = eligible_by_priority
    base["recovery_required"] = len(required_rows)
    base["recovery_eligible"] = len(eligible_rows)
    if not required_rows:
        return base

    minimum_priority = min(
        _safe_priority(row.get("normalized_priority"), 2)
        for row in required_rows
    )
    minimum_candidates = [
        row
        for row in eligible_rows
        if _safe_priority(row.get("normalized_priority"), 2) == minimum_priority
    ]
    ordered = sorted(
        minimum_candidates,
        key=lambda row: (
            _safe_priority(row.get("existing_secondary_priority"), 0),
            _safe_priority(row.get("original_stable_order"), 0),
            str(row.get("queue_item_id") or ""),
        ),
    )
    selected: list[dict[str, Any]] = []
    seen_cities: set[str] = set()
    for row in ordered:
        city_id = str(row.get("raw_city_id") or row.get("city_id") or "")
        if not city_id or city_id in seen_cities:
            continue
        selected.append(row)
        seen_cities.add(city_id)
        if len(selected) >= max(1, int(city_limit)):
            break

    work_source = _work_source_for_priority(minimum_priority)
    base.update(
        {
            "work_source": work_source,
            "normalized_priority": minimum_priority,
            "priority_reason": (
                "ANALYSIS_READY_CORE"
                if minimum_priority == 0
                else "CRITICAL_GAP_CLOSURE"
                if minimum_priority == 1
                else "GLOBAL_FINAL_RECOVERY"
            ),
            "core_scope_member": minimum_priority == 0,
            "critical_gap_member": minimum_priority == 1,
            "queue_item_ids": [str(row.get("queue_item_id") or "") for row in selected],
            "cities": [str(row.get("raw_city_id") or row.get("city_id") or "") for row in selected],
            "blocked_by_active_lease": not bool(selected),
            "reason_code": (
                "ANALYSIS_READY_CORE_RECOVERY_REQUIRED"
                if minimum_priority == 0
                else "CRITICAL_GAP_RECOVERY_REQUIRED"
                if minimum_priority == 1
                else "FINAL_RECOVERY_REQUIRED"
            ),
        }
    )
    return base


def analysis_ready_decision(
    gates: dict[str, bool],
    *,
    pass1_success: int,
    pass1_total: int,
    pass2_success: int,
    final_recovery_remaining: int,
) -> dict[str, Any]:
    pass1_total = max(0, int(pass1_total))
    pass1_success = max(0, min(int(pass1_success), pass1_total))
    pass2_eligible = pass1_success
    pass2_success = max(0, min(int(pass2_success), pass2_eligible))
    pass2_waiting = max(0, pass2_eligible - pass2_success)
    required = (
        "core_discovery",
        "official_evidence",
        "action_extraction",
        "api_pass1",
        "api_pass2",
        "date_verification",
        "critical_dedup",
        "critical_gaps",
        "formal_promotion",
        "dashboard_action_export",
    )
    failed = [name for name in required if gates.get(name) is not True]
    ready = not failed
    return {
        "analysis_ready": ready,
        "export_required": ready,
        "final_ready": ready and int(final_recovery_remaining) == 0,
        "failed_gates": failed,
        "pass1_waiting": max(0, pass1_total - pass1_success),
        "pass2_not_yet_eligible": max(0, pass1_total - pass2_eligible),
        "pass2_eligible": pass2_eligible,
        "pass2_waiting": pass2_waiting,
        "pass2_success": pass2_success,
    }


def _text(value: object) -> str:
    return str(value or "").strip()


def _serialize_raw_response_payload(value: Any) -> str | None:
    """Keep heterogeneous provider payloads in one stable Parquet column."""

    if value is None:
        return None
    if isinstance(value, str):
        return value
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def _deserialize_raw_response_payload(value: Any) -> Any:
    """Decode the stable audit representation without weakening validation."""

    if not isinstance(value, str):
        return value
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value


def _count_path(path: Path, *, predicate=None) -> int:
    if not path.exists():
        return 0
    frame = read_parquet_snapshot(path)
    if predicate is not None and not frame.is_empty():
        frame = frame.filter(predicate)
    return frame.height


class Episode930ProductionController:
    """Plan and run one resumable, bounded episode job through CRPD services."""

    def __init__(
        self,
        settings: Settings | None = None,
        *,
        output: Path | None = None,
        run_id: str | None = None,
        city_limit: int = 5,
        max_ai_calls: int = 10,
    ) -> None:
        self.settings = settings or Settings.discover()
        self.output = (
            output
            or self.settings.outputs / "special_projects" / "2016_930"
        ).resolve()
        self.output.mkdir(parents=True, exist_ok=True)
        self.production_root = self.output / "production_runs"
        self.production_root.mkdir(parents=True, exist_ok=True)
        self.run_id = run_id or stable_id(
            EPISODE_ID, _now(), prefix="EP930RUN"
        )
        self.run_dir = self.production_root / self.run_id
        self.run_dir.mkdir(parents=True, exist_ok=True)
        self.city_limit = max(1, min(int(city_limit), 105))
        self.max_ai_calls = max(0, int(max_ai_calls))
        self.queue_path = self.output / "930_TASK_QUEUE.parquet"
        self.search_plan_path = self.output / "930_SEARCH_PLAN.parquet"
        self.snapshot_path = self.output / "930_PROGRESS_SNAPSHOT.json"
        self.provider_status_path = self.output / "930_API_PROVIDER_STATUS.json"
        self.recovery_state_path = self.output / "930_API_RECOVERY_STATE.json"
        self.certification_ledger_path = self.output / CERTIFICATION_BATCH_LEDGER_NAME
        self.certification_attempt_path = self.output / CERTIFICATION_ATTEMPT_LEDGER_NAME
        self.analysis_scope_path = self.output / "930_ANALYSIS_READY_SCOPE.json"
        self.false_recovery_path = self.output / "930_FALSE_COMPLETION_RECOVERY_QUEUE.parquet"
        self.priority_recovery_path = self.output / "930_FALSE_COMPLETION_PRIORITY_QUEUE.parquet"
        self.recovery_claim_audit_path = self.output / "930_RECOVERY_CLAIM_AUDIT.parquet"
        self.checkpoint_path = self.run_dir / "CHECKPOINT.json"
        self.handoff_path = self.run_dir / "HANDOFF.json"
        self._monitor_lock = threading.Lock()

    def _snapshot(self, **updates: Any) -> dict[str, Any]:
        current = _read_json(self.snapshot_path)
        if current.get("run_id") not in {None, self.run_id}:
            # The shared Dashboard snapshot can outlive a bounded run.  A new
            # run must begin from clean counters, while the immutable prior
            # run remains available under ``production_runs/<run_id>``.
            current = {
                "previous_run_id": current.get("run_id"),
                "previous_run_status": current.get("status"),
                "last_micro_batch_status": current.get("last_micro_batch_status") or current.get("status"),
            }
        base = {
            "episode_id": EPISODE_ID,
            "episode_name": EPISODE_NAME,
            "execution_mode": EXECUTION_MODE,
            "status": "INITIALIZING",
            "stage": "930_SCOPE_BUILD",
            "queue_total": 0,
            "queue_completed": 0,
            "queue_pending": 0,
            "queue_running": 0,
            "queue_failed": 0,
            "cities_discovered": 0,
            "cities_official_recovered": 0,
            "cities_unresolved": 0,
            "documents_found": 0,
            "official_documents": 0,
            "actions_extracted": 0,
            "actions_classified": 0,
            "dates_verified": 0,
            "parameters_extracted": 0,
            "pdfs_found": 0,
            "pdfs_archived": 0,
            "formal_actions_promoted": 0,
            "formal_documents_promoted": 0,
            "api_success": 0,
            "api_failed": 0,
            "api_deferred": 0,
            "api_attempts": 0,
            "api_in_flight": 0,
            "api_pass1_success": 0,
            "api_pass2_success": 0,
            "api_pass1_failed": 0,
            "api_pass2_failed": 0,
            "api_cache_hits": 0,
            "api_failure_rows": 0,
            "api_retryable_failures": 0,
            "api_recovery": {},
            "api_provider_status": _read_json(self.provider_status_path).get("status", "unknown"),
            "classified_rows": 0,
            "tokens": None,
            "cost": None,
            "usage_status": "unavailable",
            "api_status": "unknown",
            "gaps_remaining": 0,
            "gap_type_counts": [],
            "last_micro_batch_status": None,
            "next_batch_status": "PENDING",
            "current_city": None,
            "current_source": None,
            "current_item": None,
            "last_real_progress_at": None,
            "heartbeat_at": _now(),
            "run_id": self.run_id,
            "production_version": PRODUCTION_VERSION,
            "work_source": None,
            "work_source_priority": None,
            "work_source_reason": None,
            "global_core_priority": None,
            "analysis_ready_core_recovery_required": 0,
        }
        base.update(current)
        base.update(updates)
        # A shared snapshot is read by the Dashboard.  Do not let an older
        # planned run's ``run_id`` survive the merge above and make a live
        # production run look like a different run.
        base["run_id"] = self.run_id
        base["production_version"] = PRODUCTION_VERSION
        if updates.get("real_progress", False):
            base["last_real_progress_at"] = _now()
        base["heartbeat_at"] = _now()
        base.pop("real_progress", None)
        _atomic_json(self.snapshot_path, base)
        # Monitoring is deliberately best-effort and read-only with respect to
        # the production tables.  A metrics artifact must never abort a crawl
        # or postprocess transaction.
        try:
            with self._monitor_lock:
                build_monitor_snapshot(self.output, write=True)
        except Exception:
            pass
        return base

    def _api_audit_metrics(self) -> dict[str, Any]:
        """Summarise only this run's persisted API request records.

        The classifier's ``ai_calls`` is an attempt budget.  It is not a
        success count: failed and interrupted requests must remain visible in
        the production snapshot, and missing pricing must remain ``None``.
        """

        request_dir = self.run_dir / "05_API_CLASSIFICATION" / "ai_audit" / "requests"
        records: list[dict[str, Any]] = []
        if request_dir.exists():
            for path in sorted(request_dir.glob("*.json")):
                try:
                    value = json.loads(path.read_text(encoding="utf-8"))
                except (OSError, json.JSONDecodeError):
                    continue
                if isinstance(value, dict):
                    records.append(value)
        completed = [row for row in records if row.get("status") == "response_completed"]
        failed = [
            row
            for row in records
            if row.get("status") in {"response_failed", "interrupted"}
        ]
        started = [row for row in records if row.get("status") == "request_started"]
        token_values = [
            int(row["total_tokens"])
            for row in completed
            if isinstance(row.get("total_tokens"), (int, float))
        ]
        cost_values = [row.get("estimated_cost_usd") for row in completed]
        cost = (
            sum(float(value) for value in cost_values)
            if completed and all(value is not None for value in cost_values)
            else None
        )

        def pass_count(rows: list[dict[str, Any]], name: str) -> int:
            return sum(
                1
                for row in rows
                if str((row.get("input_summary") or {}).get("pass_name")) == name
            )

        return {
            "api_attempts": len(records),
            "api_success": len(completed),
            "api_failed": len(failed),
            # A failed request is deferred to the retry/recovery path; it is
            # not silently discarded and is not a successful classification.
            "api_deferred": len(failed),
            "api_in_flight": len(started),
            "api_pass1_success": pass_count(completed, "first_pass"),
            "api_pass2_success": pass_count(completed, "second_review"),
            "api_pass1_failed": pass_count(failed, "first_pass"),
            "api_pass2_failed": pass_count(failed, "second_review"),
            "api_cache_hits": sum(1 for row in completed if row.get("cache_hit") is True),
            "tokens": sum(token_values) if completed and len(token_values) == len(completed) else None,
            "cost": cost,
            "usage_status": "available" if completed and len(token_values) == len(completed) else "unavailable",
        }

    @staticmethod
    def _safe_api_error(value: object) -> str:
        text = str(value or "")[:500]
        text = re.sub(r"(?i)(api[_-]?key|authorization|bearer|token|password|secret)\s*[:=]\s*[^,\s]+", r"\1=[REDACTED]", text)
        text = re.sub(r"(?i)bearer\s+[^\s,]+", "Bearer [REDACTED]", text)
        text = re.sub(r"\bsk-[A-Za-z0-9_-]+", "[REDACTED]", text)
        return text

    @staticmethod
    def _api_failure_retryable(error_type: object, error_message: object) -> bool:
        value = f"{error_type or ''} {error_message or ''}".lower()
        terminal_markers = ("401", "402", "403", "invalid api key", "authentication", "context length", "content policy", "model rejection")
        if any(marker in value for marker in terminal_markers):
            return False
        retry_markers = ("timeout", "rate", "429", "500", "502", "503", "504", "connection", "network", "tls", "temporary", "json", "schema", "parse", "validation", "provider")
        return any(marker in value for marker in retry_markers)

    @staticmethod
    def _legacy_failure_diagnostics(error_type: object, error_message: object) -> dict[str, Any]:
        """Classify old audits only where their stored exception is decisive."""

        value = f"{error_type or ''} {error_message or ''}".lower()
        if "connecttimeout" in value:
            return {"failure_class": "CONNECT_TIMEOUT", "timeout_type": "connect", "transport_started": True, "connect_ok": False, "response_received": False}
        if "readtimeout" in value:
            return {"failure_class": "READ_TIMEOUT", "timeout_type": "read", "transport_started": True, "connect_ok": True, "response_received": False}
        if "apitimeouterror" in value or "timeoutexception" in value:
            return {"failure_class": "UNKNOWN_PROVIDER_FAILURE", "timeout_type": "unspecified", "transport_started": True, "response_received": False}
        if "validation_failed" in value or "schema" in value:
            return {"failure_class": "SCHEMA_VALIDATION_FAILURE", "transport_started": True, "response_received": True, "json_parse_ok": True, "schema_valid": False}
        for code in (401, 402, 403, 429):
            if str(code) in value:
                return {"failure_class": f"HTTP_{code}", "http_status": code, "transport_started": True, "response_received": True}
        if any(code in value for code in ("500", "502", "503", "504")):
            return {"failure_class": "HTTP_5XX", "transport_started": True, "response_received": True}
        if "connection" in value or "network" in value:
            return {"failure_class": "CONNECTION_ERROR", "transport_started": True, "response_received": False}
        return {"failure_class": "UNKNOWN_PROVIDER_FAILURE"}

    def _write_provider_status(self, records: list[dict[str, Any]]) -> dict[str, Any]:
        """Persist current provider health without replacing historical failures."""
        completed = [row for row in records if row.get("status") == "response_completed"]
        failures = [row for row in records if row.get("status") in {"response_failed", "interrupted"}]
        previous = _read_json(self.provider_status_path)
        if not completed and not failures and previous:
            return previous
        if completed:
            status = "OPERATIONAL"
            balance_status = "call_succeeded"
            representative = completed[-1]
        else:
            failure_classes = {
                str(row.get("failure_class") or "").upper()
                for row in failures
            }
            text = " ".join(
                f"{row.get('error_type') or ''} {row.get('error_message') or ''}"
                for row in failures
            ).lower()
            if failure_classes and failure_classes <= {"SCHEMA_VALIDATION_FAILURE"}:
                status, balance_status = "SCHEMA_VALIDATION_FAILURE", "call_succeeded"
            elif "HTTP_402" in failure_classes or "402" in text or "insufficient" in text:
                status, balance_status = "BLOCKED_EXTERNAL_402", "insufficient_balance"
            elif failure_classes.intersection({"HTTP_401", "HTTP_403"}) or "401" in text or "403" in text or "authentication" in text:
                status, balance_status = "AUTH_CONFIGURATION_ERROR", "auth_failed"
            elif "HTTP_429" in failure_classes or "429" in text or "rate" in text:
                status, balance_status = "RATE_LIMITED", "rate_limited"
            elif failures:
                status, balance_status = "TEMPORARY_PROVIDER_FAILURE", "unknown"
            else:
                status, balance_status = "UNKNOWN", "unknown"
            representative = failures[-1] if failures else {}
        payload = {
            "episode_id": EPISODE_ID,
            "status": status,
            "provider": representative.get("provider") or "siliconflow",
            "model": representative.get("model") or self.settings.siliconflow_chat_model,
            "api_balance_status": balance_status,
            "last_success_at": completed[-1].get("completed_at") if completed else _read_json(self.provider_status_path).get("last_success_at"),
            "last_request_id": representative.get("request_id"),
            "updated_at": _now(),
        }
        _atomic_json(self.provider_status_path, payload)
        return payload

    @staticmethod
    def _recovery_status_for(error_type: object, error_message: object, retryable: bool) -> str:
        value = f"{error_type or ''} {error_message or ''}".lower()
        if "402" in value or "insufficient" in value:
            return "PENDING_PROVIDER_RECOVERY"
        return "PENDING_RETRY" if retryable else "TERMINAL"

    def _write_api_failure_artifact(self, documents: pl.DataFrame) -> dict[str, Any]:
        """Persist retryable AI failures and expose a resumable recovery queue."""

        request_dir = self.run_dir / "05_API_CLASSIFICATION" / "ai_audit" / "requests"
        records: list[dict[str, Any]] = []
        if request_dir.exists():
            for path in sorted(request_dir.glob("*.json")):
                try:
                    value = json.loads(path.read_text(encoding="utf-8"))
                except (OSError, json.JSONDecodeError):
                    continue
                if isinstance(value, dict):
                    records.append(value)
        doc_hashes = {
            str(row.get("document_id")): row.get("content_hash")
            for row in documents.iter_rows(named=True)
            if row.get("document_id")
        }
        failure_path = self.output / "930_API_FAILURES.parquet"
        existing = read_parquet_snapshot(failure_path) if failure_path.exists() else pl.DataFrame()
        existing_rows = existing.to_dicts() if not existing.is_empty() else []
        attempt_counts = Counter(str(row.get("request_hash")) for row in existing_rows if row.get("request_hash"))
        existing_signatures = {(str(row.get("request_id") or ""), str(row.get("created_at") or "")) for row in existing_rows}
        rows = list(existing_rows)
        for row in rows:
            error_type = row.get("error_type")
            error_message = row.get("error_message_safe")
            retryable = bool(row.get("retryable"))
            row.setdefault("recovery_status", self._recovery_status_for(error_type, error_message, retryable))
            row.setdefault("recovered_at", None)
            row.setdefault("recovery_run_id", None)
            row.setdefault("final_http_status", None)
            row.setdefault("final_pass1_status", None)
            for name in (
                "failure_class", "transport_started", "dns_ok", "connect_ok",
                "http_status", "response_received", "response_bytes", "latency_ms",
                "timeout_type", "json_parse_ok", "schema_valid", "schema_errors",
                "provider_error_code", "provider_error_message_sanitized",
                "raw_response_hash", "raw_response_payload",
                "configured_read_timeout", "configured_connect_timeout", "max_retries",
            ):
                row.setdefault(name, None)
            if not row.get("failure_class"):
                for name, value in self._legacy_failure_diagnostics(error_type, error_message).items():
                    if row.get(name) is None:
                        row[name] = value
        for record in records:
            if record.get("status") not in {"response_failed", "interrupted"}:
                continue
            signature = (str(record.get("request_id") or ""), str(record.get("updated_at") or ""))
            if signature in existing_signatures:
                continue
            request_hash = str(record.get("request_hash") or "")
            attempt_counts[request_hash] += 1
            error_type = str(record.get("error_type") or "unknown")
            safe_message = self._safe_api_error(record.get("error_message"))
            retryable = self._api_failure_retryable(error_type, safe_message) and attempt_counts[request_hash] <= 2
            recovery_status = self._recovery_status_for(error_type, safe_message, retryable)
            rows.append({
                "failure_id": _id(EPISODE_ID, record.get("request_id"), attempt_counts[request_hash], prefix="AIF930"),
                "episode_id": EPISODE_ID,
                "run_id": self.run_id,
                "request_id": record.get("request_id"),
                "request_hash": request_hash,
                "document_id": record.get("slot_id"),
                "content_hash": doc_hashes.get(str(record.get("slot_id"))),
                "pass_name": (record.get("input_summary") or {}).get("pass_name"),
                "attempt": attempt_counts[request_hash],
                "error_type": error_type,
                "error_message_safe": safe_message,
                "failure_class": record.get("failure_class") or "UNKNOWN_PROVIDER_FAILURE",
                "transport_started": record.get("transport_started"),
                "dns_ok": record.get("dns_ok"),
                "connect_ok": record.get("connect_ok"),
                "http_status": record.get("http_status"),
                "response_received": record.get("response_received"),
                "response_bytes": record.get("response_bytes"),
                "latency_ms": record.get("latency_ms"),
                "timeout_type": record.get("timeout_type"),
                "json_parse_ok": record.get("json_parse_ok"),
                "schema_valid": record.get("schema_valid"),
                "schema_errors": record.get("schema_errors"),
                "provider_error_code": record.get("provider_error_code"),
                "provider_error_message_sanitized": record.get("provider_error_message_sanitized"),
                "raw_response_hash": record.get("raw_response_hash"),
                "raw_response_payload": record.get("raw_response_payload"),
                "configured_read_timeout": record.get("configured_read_timeout"),
                "configured_connect_timeout": record.get("configured_connect_timeout"),
                "max_retries": record.get("max_retries"),
                "retryable": retryable,
                "status": "RETRYABLE_FAILURE" if retryable else "AI_DEFERRED",
                "next_retry_at": (_now_plus(minutes=30) if retryable else None),
                "recovery_status": recovery_status,
                "recovered_at": None,
                "recovery_run_id": None,
                "final_http_status": None,
                "final_pass1_status": None,
                "created_at": record.get("updated_at") or _now(),
                "resolved_at": None,
            })
            existing_signatures.add(signature)
        # A completed global request resolves historical retry rows without
        # recharging the same content hash.
        completed_hashes: set[str] = set()
        global_dir = self.settings.outputs / "ai_audit" / "requests"
        if global_dir.exists():
            # The global audit store canonicalizes each request hash to its
            # filename.  Read only hashes already present in the failure
            # ledger; scanning every historical request made one recovery
            # ordinal spend minutes in post-processing after the provider had
            # already returned successfully.
            pending_hashes = {
                str(row.get("request_hash"))
                for row in rows
                if row.get("request_hash")
            }
            for request_hash in pending_hashes:
                safe_hash = "".join(
                    character
                    for character in request_hash
                    if character.isalnum() or character in "_-"
                )
                if not safe_hash:
                    continue
                path = global_dir / f"{safe_hash}.json"
                try:
                    record = json.loads(path.read_text(encoding="utf-8"))
                except (FileNotFoundError, OSError, json.JSONDecodeError):
                    continue
                if record.get("status") == "response_completed" and record.get("request_hash"):
                    completed_hashes.add(str(record["request_hash"]))
        for row in rows:
            if row.get("request_hash") in completed_hashes:
                row["status"] = "RESOLVED"
                row["retryable"] = False
                row["next_retry_at"] = None
                row["recovery_status"] = "RECOVERED"
                row["resolved_at"] = row.get("resolved_at") or _now()
        for row in rows:
            row["raw_response_payload"] = _serialize_raw_response_payload(
                row.get("raw_response_payload")
            )
        if rows:
            frame = pl.DataFrame(rows, infer_schema_length=None).unique(subset=["failure_id"], keep="last")
        else:
            frame = pl.DataFrame(schema={"failure_id": pl.String, "request_hash": pl.String, "retryable": pl.Boolean, "status": pl.String})
        atomic_write_parquet(frame, failure_path, {"module": "episode_930_production", "artifact": "api_failures"}, key_columns=("failure_id",))
        recovery = (
            frame.filter(
                (pl.col("status") == "RETRYABLE_FAILURE")
                | (pl.col("recovery_status") == "PENDING_PROVIDER_RECOVERY")
            )
            if not frame.is_empty() and {"status", "recovery_status"}.issubset(frame.columns)
            else frame
        )
        atomic_write_parquet(recovery, self.output / "930_API_RECOVERY_QUEUE.parquet", {"module": "episode_930_production", "artifact": "api_recovery_queue"}, key_columns=("failure_id",))
        provider_status = self._write_provider_status(records)
        return {
            "failure_rows": frame.height,
            "retryable_failures": int(recovery.filter(pl.col("status") == "RETRYABLE_FAILURE").height) if not recovery.is_empty() else 0,
            "provider_recovery_pending": int(recovery.filter(pl.col("recovery_status") == "PENDING_PROVIDER_RECOVERY").height) if not recovery.is_empty() and "recovery_status" in recovery.columns else 0,
            "terminal_failures": int(frame.filter(pl.col("status") == "AI_DEFERRED").height) if not frame.is_empty() and "status" in frame.columns else 0,
            "provider_status": provider_status.get("status"),
        }

    def _replay_schema_failures_locally(self, queue: pl.DataFrame) -> tuple[pl.DataFrame, dict[str, Any]]:
        """Revalidate cached 2xx JSON without making a provider request."""

        if queue.is_empty() or not {"failure_class", "raw_response_payload"}.issubset(queue.columns):
            return queue, {"attempted": 0, "recovered": 0, "failed": 0}
        rows = queue.to_dicts()
        replay_rows: list[dict[str, Any]] = []
        classification_rows: list[dict[str, Any]] = []
        attempted = recovered = failed = 0
        for row in rows:
            if str(row.get("failure_class") or "").upper() != "SCHEMA_VALIDATION_FAILURE":
                continue
            raw_payload = row.get("raw_response_payload")
            if raw_payload in (None, ""):
                continue
            payload = _deserialize_raw_response_payload(raw_payload)
            attempted += 1
            try:
                parsed = validate_structured_payload(payload, ActionClassificationPayload)
            except Exception as exc:  # deterministic replay must never call the provider
                failed += 1
                replay_rows.append({
                    "failure_id": row.get("failure_id"),
                    "document_id": row.get("document_id"),
                    "status": "LOCAL_REPLAY_SCHEMA_INVALID",
                    "error_type": type(exc).__name__,
                    "replayed_at": _now(),
                })
                continue
            recovered += 1
            row.update({
                "status": "RESOLVED",
                "retryable": False,
                "recovery_status": "RECOVERED_LOCAL_REPLAY",
                "recovered_at": _now(),
                "recovery_run_id": self.run_id,
                "final_http_status": row.get("http_status") or 200,
                "final_pass1_status": "response_completed_local_replay",
                "next_retry_at": None,
                "resolved_at": row.get("resolved_at") or _now(),
            })
            audit_record = {
                "slot_id": row.get("document_id"),
                "provider": "siliconflow",
                "model": self.settings.siliconflow_chat_model,
                "prompt_version": "episode_930_actions_v1",
            }
            for item in parsed.actions:
                classification_rows.append(
                    Episode930Pipeline._classification_row(
                        item.model_dump(mode="json"),
                        str(row.get("request_id") or row.get("failure_id") or ""),
                        str(row.get("request_hash") or ""),
                        str(row.get("pass_name") or "first_pass"),
                        audit_record,
                        cache_hit=True,
                    )
                )
            replay_rows.append({
                "failure_id": row.get("failure_id"),
                "document_id": row.get("document_id"),
                "status": "RECOVERED_LOCAL_REPLAY",
                "actions": len(parsed.actions),
                "replayed_at": _now(),
            })
        if replay_rows:
            atomic_write_parquet(
                pl.DataFrame(replay_rows, infer_schema_length=None),
                self.output / "930_API_SCHEMA_REPLAY_RECEIPT.parquet",
                {"module": "episode_930_production", "network_calls": 0},
                key_columns=("failure_id",),
            )
        if classification_rows:
            target = self.run_dir / "05_API_CLASSIFICATION" / "2016_930_API_CLASSIFICATION.parquet"
            current = read_parquet_snapshot(target) if target.exists() else pl.DataFrame()
            merged = pl.concat([current, pl.DataFrame(classification_rows, infer_schema_length=None)], how="diagonal_relaxed") if not current.is_empty() else pl.DataFrame(classification_rows, infer_schema_length=None)
            atomic_write_parquet(
                merged.unique(subset=["request_id", "action_id", "pass_name"], keep="last"),
                target,
                {"module": "episode_930_production", "artifact": "local_schema_replay"},
                key_columns=("request_id", "action_id", "pass_name"),
            )
        return pl.DataFrame(rows, infer_schema_length=None), {
            "attempted": attempted,
            "recovered": recovered,
            "failed": failed,
        }

    def _read_certification_attempts(self) -> pl.DataFrame:
        if not self.certification_attempt_path.exists():
            return pl.DataFrame(schema=CERTIFICATION_ATTEMPT_SCHEMA)
        try:
            return read_parquet_snapshot(self.certification_attempt_path)
        except Exception:
            return pl.DataFrame(schema=CERTIFICATION_ATTEMPT_SCHEMA)

    def _merge_certification_rows(
        self,
        path: Path,
        rows: list[dict[str, Any]],
        schema: Mapping[str, Any],
        key_column: str,
        artifact: str,
    ) -> pl.DataFrame:
        incoming = pl.DataFrame(rows, schema=schema, strict=False) if rows else pl.DataFrame(schema=schema)
        current = read_parquet_snapshot(path) if path.exists() else pl.DataFrame(schema=schema)
        if current.is_empty() and not current.columns:
            merged = incoming
        elif incoming.is_empty() and not incoming.columns:
            merged = current
        else:
            merged = pl.concat([current, incoming], how="diagonal_relaxed")
        if key_column in merged.columns:
            merged = merged.unique(subset=[key_column], keep="last", maintain_order=True)
        atomic_write_parquet(
            merged,
            path,
            {"module": "episode_930_production", "artifact": artifact, "run_id": self.run_id},
            key_columns=(key_column,),
        )
        return merged

    def _ensure_certification_batch(
        self,
        recovery_state: Mapping[str, Any],
        provider_state: Mapping[str, Any],
    ) -> tuple[pl.DataFrame, dict[str, Any] | None]:
        ledger = read_certification_ledger(self.output)
        active_rows = [
            row
            for row in ledger.to_dicts()
            if str(row.get("batch_status") or "") in {"RUNNING", "RETRY_WAIT"}
        ]
        active_rows.sort(key=lambda row: str(row.get("updated_at") or row.get("started_at") or ""))
        active_id = str(recovery_state.get("certification_batch_id") or "")
        active = next((row for row in active_rows if str(row.get("certification_batch_id")) == active_id), None)
        if active is None and active_rows:
            active = active_rows[-1]
        if active is not None:
            return ledger, active

        gate = certification_gate_from_ledger(ledger)
        if gate.get("certification") == "PASS":
            return ledger, None
        stage = next(
            (
                candidate
                for candidate in CERTIFICATION_STAGE_REQUIREMENTS
                if not bool((gate.get("stages") or {}).get(candidate, {}).get("passed"))
            ),
            "SINGLE",
        )
        now = _now()
        batch_id = stable_id(EPISODE_ID, stage, now, prefix="EP930CERT")
        active = {
            "certification_batch_id": batch_id,
            "stage": stage,
            "provider": provider_state.get("provider"),
            "model": provider_state.get("model"),
            "started_at": now,
            "completed_at": None,
            "required_attempts": CERTIFICATION_STAGE_REQUIREMENTS[stage],
            "attempts_started": 0,
            "real_provider_attempts": 0,
            "cache_reuse_count": 0,
            "schema_valid_successes": 0,
            "schema_failures": 0,
            "provider_failures": 0,
            "timeouts": 0,
            "other_failures": 0,
            "pending_attempts": CERTIFICATION_STAGE_REQUIREMENTS[stage],
            "success_rate": 0.0,
            "threshold": CERTIFICATION_STAGE_THRESHOLDS[stage],
            "batch_status": "RUNNING",
            "next_retry_at": None,
            "run_id": self.run_id,
            "updated_at": now,
        }
        active.update(certification_batch_feasibility(stage, 0, 0))
        ledger = self._merge_certification_rows(
            self.certification_ledger_path,
            [active],
            CERTIFICATION_BATCH_SCHEMA,
            "certification_batch_id",
            "certification_batch_started",
        )
        return ledger, active

    def _record_certification_attempts(
        self,
        batch: Mapping[str, Any],
        audit_rows: list[dict[str, Any]],
        cache_reuse_ids: set[str],
        input_ids: set[str],
    ) -> pl.DataFrame:
        batch_id = str(batch.get("certification_batch_id") or "")
        existing = self._read_certification_attempts()
        existing_ids = {str(value) for value in existing.get_column("attempt_id").to_list()} if "attempt_id" in existing.columns else set()
        real_existing = existing.filter(
            (pl.col("certification_batch_id") == batch_id) & pl.col("provider_attempt")
        ) if not existing.is_empty() and "provider_attempt" in existing.columns else pl.DataFrame()
        next_ordinal = real_existing.height + 1
        incoming: list[dict[str, Any]] = []
        for row in audit_rows:
            input_id = str(row.get("slot_id") or "")
            if input_id not in input_ids or str((row.get("input_summary") or {}).get("pass_name") or "") != "first_pass":
                continue
            request_id = str(row.get("request_id") or "")
            attempt_id = request_id or stable_id(batch_id, input_id, row.get("started_at"), prefix="EP930ATT")
            if attempt_id in existing_ids:
                continue
            status = str(row.get("status") or "")
            cache_hit = bool(row.get("cache_hit") is True)
            provider_attempt = bool(
                not cache_hit
                and (
                    status.startswith("response_")
                    or row.get("transport_started") is True
                    or row.get("provider_request_id")
                )
            )
            ordinal = next_ordinal if provider_attempt else None
            if provider_attempt:
                next_ordinal += 1
            incoming.append(
                {
                    "attempt_id": attempt_id,
                    "certification_batch_id": batch_id,
                    "ordinal": ordinal,
                    "started_at": row.get("started_at"),
                    "finished_at": row.get("completed_at") or row.get("updated_at"),
                    "provider": row.get("provider"),
                    "model": row.get("model"),
                    "input_id": input_id,
                    "request_fingerprint": row.get("request_hash"),
                    "cache_hit": cache_hit,
                    "provider_attempt": provider_attempt,
                    "http_status": row.get("http_status"),
                    "schema_valid": bool(row.get("schema_valid") is True and status == "response_completed"),
                    "failure_class": row.get("failure_class"),
                    "latency_ms": row.get("latency_ms"),
                    "provider_request_id": row.get("request_id"),
                    "prompt_tokens": row.get("prompt_tokens"),
                    "completion_tokens": row.get("completion_tokens"),
                    "total_tokens": row.get("total_tokens"),
                    "estimated_cost_usd": row.get("estimated_cost_usd"),
                    "usage_status": row.get("usage_status"),
                    "status": status,
                    "run_id": self.run_id,
                }
            )
        for input_id in sorted(cache_reuse_ids):
            attempt_id = stable_id(batch_id, "CACHE", input_id, self.run_id, prefix="EP930CACHE")
            if attempt_id in existing_ids:
                continue
            now = _now()
            incoming.append(
                {
                    "attempt_id": attempt_id,
                    "certification_batch_id": batch_id,
                    "ordinal": None,
                    "started_at": now,
                    "finished_at": now,
                    "provider": None,
                    "model": None,
                    "input_id": input_id,
                    "request_fingerprint": None,
                    "cache_hit": True,
                    "provider_attempt": False,
                    "http_status": None,
                    "schema_valid": False,
                    "failure_class": "CACHE_REUSE_NOT_A_PROVIDER_PROBE",
                    "latency_ms": None,
                    "provider_request_id": None,
                    "prompt_tokens": None,
                    "completion_tokens": None,
                    "total_tokens": None,
                    "estimated_cost_usd": None,
                    "usage_status": "unavailable",
                    "status": "cache_reused",
                    "run_id": self.run_id,
                }
            )
        return self._merge_certification_rows(
            self.certification_attempt_path,
            incoming,
            CERTIFICATION_ATTEMPT_SCHEMA,
            "attempt_id",
            "certification_attempt_recorded",
        )

    def _update_certification_batch(
        self,
        batch: Mapping[str, Any],
        attempts: pl.DataFrame,
        *,
        state_reason: str | None = None,
    ) -> tuple[pl.DataFrame, dict[str, Any]]:
        batch_id = str(batch.get("certification_batch_id") or "")
        scoped = attempts.filter(pl.col("certification_batch_id") == batch_id) if "certification_batch_id" in attempts.columns else pl.DataFrame(schema=CERTIFICATION_ATTEMPT_SCHEMA)
        real = scoped.filter(pl.col("provider_attempt")) if not scoped.is_empty() else scoped
        real_count = real.height
        valid_count = int(real.filter(pl.col("schema_valid")).height) if not real.is_empty() else 0
        cache_count = int(scoped.filter(pl.col("cache_hit")).height) if not scoped.is_empty() else 0
        failures = [str(value or "").upper() for value in (real.get_column("failure_class").to_list() if not real.is_empty() and "failure_class" in real.columns else [])]
        schema_failures = sum("SCHEMA" in value for value in failures)
        timeouts = sum("TIMEOUT" in value for value in failures)
        provider_failures = sum(any(token in value for token in ("PROVIDER", "HTTP_", "CONNECTION", "CONNECT_TIMEOUT", "402", "5XX")) for value in failures)
        other_failures = max(0, real_count - valid_count - schema_failures - timeouts - provider_failures)
        transition = certification_batch_transition(
            str(batch.get("stage") or ""),
            real_count,
            real_count,
            bool(real_count and valid_count == real_count),
            schema_valid_successes=valid_count,
            cache_reuse_count=cache_count,
        )
        feasibility = certification_batch_feasibility(
            batch.get("stage"), real_count, valid_count
        )
        transition = {**transition, **feasibility}
        now = _now()
        updated = {
            **dict(batch),
            "attempts_started": real_count,
            "real_provider_attempts": real_count,
            "cache_reuse_count": cache_count,
            "schema_valid_successes": valid_count,
            "schema_failures": schema_failures,
            "provider_failures": provider_failures,
            "timeouts": timeouts,
            "other_failures": other_failures,
            "pending_attempts": transition.get("pending_attempts", 0),
            "success_rate": transition.get("success_rate", 0.0),
            "threshold": transition.get("threshold", CERTIFICATION_STAGE_THRESHOLDS.get(str(batch.get("stage")), 0.8)),
            "batch_status": transition.get("batch_status", "RUNNING"),
            "completed_at": now if transition.get("batch_status") in {"PASS", "FAIL"} else batch.get("completed_at"),
            "next_retry_at": _now_plus(minutes=30) if transition.get("backoff") else None,
            **feasibility,
            "updated_at": now,
            "run_id": self.run_id,
        }
        ledger = self._merge_certification_rows(
            self.certification_ledger_path,
            [updated],
            CERTIFICATION_BATCH_SCHEMA,
            "certification_batch_id",
            "certification_batch_updated",
        )
        if state_reason:
            transition["reason_code"] = state_reason
        return ledger, transition

    def _recover_api_failures(
        self,
        *,
        core_document_ids: set[str] | None = None,
        fast_lane_document_priorities: Mapping[str, int] | None = None,
    ) -> dict[str, Any]:
        """Retry cached-document API failures through a strict bounded gate."""

        path = self.output / "930_API_RECOVERY_QUEUE.parquet"
        if not path.exists():
            return {"recovery_attempted": 0, "recovery_success": 0, "recovery_deferred": 0, "recovery_gate": "NO_RECOVERY_BACKLOG"}
        queue = read_parquet_snapshot(path)
        if queue.is_empty() or "status" not in queue.columns:
            return {"recovery_attempted": 0, "recovery_success": 0, "recovery_deferred": 0, "recovery_gate": "NO_RECOVERY_BACKLOG"}

        queue, local_replay = self._replay_schema_failures_locally(queue)
        if local_replay["recovered"]:
            remaining = queue.filter(
                (pl.col("status") == "RETRYABLE_FAILURE")
                | (pl.col("recovery_status") == "PENDING_PROVIDER_RECOVERY")
            )
            atomic_write_parquet(
                remaining,
                path,
                {"module": "episode_930_production", "artifact": "api_recovery_after_local_replay"},
                key_columns=("failure_id",),
            )
            queue = remaining
        if queue.is_empty():
            return {
                "recovery_attempted": 0,
                "recovery_success": 0,
                "recovery_deferred": 0,
                "recovery_gate": "NO_RECOVERY_BACKLOG",
                "local_schema_replay": local_replay,
            }

        provider_status = _read_json(self.provider_status_path)
        provider_status_name = str(provider_status.get("status") or "").upper()
        provider_recovered = provider_status_name in {"RECOVERED", "OPERATIONAL"}
        schema_retry_eligible = (
            provider_status_name == "SCHEMA_VALIDATION_FAILURE"
            and str(provider_status.get("api_balance_status") or "").lower() == "call_succeeded"
            and provider_status.get("primary_provider_unavailable") is not True
        )
        # A 2xx/provider response with a schema mismatch is a retryable
        # certification result, not evidence that the provider is unavailable.
        # Transport, auth, quota, and 5xx failures remain blocked here.
        provider_recovered = provider_recovered or schema_retry_eligible
        recovery_state = _read_json(self.recovery_state_path)
        raw_phase = str(recovery_state.get("phase") or "SINGLE_PROBE").upper()
        phase = {"PROBE": "SINGLE_PROBE", "BACKOFF": "BACKOFF_SINGLE_PROBE"}.get(raw_phase, raw_phase)
        now = datetime.now(UTC)
        next_retry_at = recovery_state.get("next_retry_at")
        if next_retry_at:
            try:
                if datetime.fromisoformat(str(next_retry_at).replace("Z", "+00:00")) > now:
                    return {
                        "recovery_attempted": 0,
                        "recovery_success": 0,
                        "recovery_deferred": queue.height,
                        "recovery_phase": raw_phase,
                        "recovery_gate": "BACKOFF_SINGLE_PROBE",
                        "reason_code": "RECOVERY_BACKOFF_WINDOW",
                        "next_retry_at": next_retry_at,
                    }
            except (TypeError, ValueError):
                pass

        previous_success = int(recovery_state.get("last_success_documents") or 0)
        previous_rate = float(recovery_state.get("last_success_rate") or 0.0)
        previous_schema_valid = bool(recovery_state.get("schema_valid"))
        if phase == "MICRO_5" and not (previous_success > 0 and previous_schema_valid):
            phase = "BACKOFF_SINGLE_PROBE"
        elif phase == "MICRO_20" and not (str(recovery_state.get("last_phase") or "").upper() == "MICRO_5" and previous_rate >= 0.8 and previous_schema_valid):
            phase = "BACKOFF_SINGLE_PROBE"
        elif phase == "BACKLOG_CONSUMPTION" and not (str(recovery_state.get("last_phase") or "").upper() == "MICRO_20" and previous_rate >= 0.8 and previous_schema_valid):
            phase = "BACKOFF_SINGLE_PROBE"
        certification_ledger, certification_batch = self._ensure_certification_batch(
            recovery_state,
            provider_status,
        )
        if certification_batch is not None:
            active_stage = str(certification_batch.get("stage") or "")
            if active_stage in CERTIFICATION_STAGE_REQUIREMENTS:
                phase = _certification_runtime_phase(active_stage)
        certification_retry_due = bool(
            certification_batch is not None
            and active_stage in CERTIFICATION_STAGE_REQUIREMENTS
            and str(certification_batch.get("batch_status") or "").upper()
            in {"RUNNING", "RETRY_WAIT"}
        )
        if (
            phase not in {"SINGLE_PROBE", "BACKOFF_SINGLE_PROBE"}
            and not provider_recovered
            and not certification_retry_due
        ):
            return {
                "recovery_attempted": 0,
                "recovery_success": 0,
                "recovery_deferred": queue.height,
                "recovery_phase": phase,
                "recovery_gate": "BLOCKED_PROVIDER",
                "reason_code": "PROVIDER_NOT_RECOVERED_FOR_MICRO_BATCH",
            }

        retryable_all = queue.filter(pl.col("status") == "RETRYABLE_FAILURE")
        retryable_rows = []
        for row in retryable_all.iter_rows(named=True):
            retry_at = row.get("next_retry_at")
            if not retry_at:
                retryable_rows.append(row)
                continue
            try:
                if datetime.fromisoformat(str(retry_at).replace("Z", "+00:00")) <= now:
                    retryable_rows.append(row)
            except (TypeError, ValueError):
                retryable_rows.append(row)
        retryable = pl.DataFrame(retryable_rows, schema=retryable_all.schema) if retryable_rows else retryable_all.head(0)
        provider_rows = queue.filter(pl.col("recovery_status") == "PENDING_PROVIDER_RECOVERY") if "recovery_status" in queue.columns else pl.DataFrame()
        frames = [frame for frame in (retryable, provider_rows) if not frame.is_empty()]
        if not frames:
            return {"recovery_attempted": 0, "recovery_success": 0, "recovery_deferred": queue.height, "recovery_phase": phase, "reason_code": "NO_DUE_RECOVERY_ROWS"}
        due = pl.concat(frames, how="diagonal_relaxed").unique(subset=["failure_id"], keep="first")
        core_document_ids = {str(value) for value in (core_document_ids or set()) if value}
        fast_lane_document_priorities = {
            str(key): int(value)
            for key, value in (fast_lane_document_priorities or {}).items()
            if key
        }
        if (core_document_ids or fast_lane_document_priorities) and "document_id" in due.columns:
            ordered_due = sorted(
                due.to_dicts(),
                key=lambda row: (
                    -1 if str(row.get("document_id") or "") in core_document_ids
                    else fast_lane_document_priorities.get(str(row.get("document_id") or ""), 99),
                    str(row.get("created_at") or ""),
                    str(row.get("failure_id") or ""),
                ),
            )
            due = pl.DataFrame(ordered_due, schema=due.schema)
        document_path = self.settings.curated / "policy_episode_documents.parquet"
        action_path = self.settings.curated / "policy_episode_actions.parquet"
        if not document_path.exists() or not action_path.exists():
            return {"recovery_attempted": 0, "recovery_success": 0, "recovery_deferred": due.height, "recovery_phase": phase, "reason_code": "CACHED_DOCUMENT_TABLE_UNAVAILABLE"}
        documents = read_parquet_snapshot(document_path)
        actions = read_parquet_snapshot(action_path)
        certification_stage = str(certification_batch.get("stage") or "") if certification_batch is not None else ""
        certification_attempts = self._read_certification_attempts() if certification_batch is not None else pl.DataFrame(schema=CERTIFICATION_ATTEMPT_SCHEMA)
        if certification_batch is not None and certification_stage in CERTIFICATION_STAGE_REQUIREMENTS:
            batch_id = str(certification_batch.get("certification_batch_id") or "")
            existing_real = certification_attempts.filter(
                (pl.col("certification_batch_id") == batch_id)
                & pl.col("provider_attempt")
            ) if not certification_attempts.is_empty() else certification_attempts
            remaining_attempts = max(
                0,
                CERTIFICATION_STAGE_REQUIREMENTS[certification_stage] - existing_real.height,
            )
            document_limit = min(remaining_attempts, max(1, self.max_ai_calls))
            call_limit = document_limit
            effective_phase = "SINGLE_PROBE" if certification_stage == "SINGLE" else certification_stage
        elif phase in {"SINGLE_PROBE", "BACKOFF_SINGLE_PROBE"}:
            document_limit, call_limit = 1, 1
            effective_phase = "SINGLE_PROBE"
        elif phase == "MICRO_5":
            document_limit, call_limit = 5, min(5, max(1, self.max_ai_calls))
            effective_phase = phase
        else:
            document_limit, call_limit = 20, min(20, max(1, self.max_ai_calls))
            effective_phase = phase
        if document_limit <= 0 and certification_batch is not None:
            certification_ledger, transition = self._update_certification_batch(
                certification_batch,
                certification_attempts,
                state_reason="CERTIFICATION_BATCH_RECONCILED_FROM_LEDGER",
            )
            return {
                "recovery_attempted": 0,
                "recovery_success": 0,
                "recovery_success_rate": transition.get("success_rate", 0.0),
                "schema_valid": bool(transition.get("schema_valid_successes", 0)),
                "recovery_phase": transition["next_phase"],
                "recovery_gate": transition["next_phase"],
                "reason_code": transition.get("reason_code"),
                "recovery_deferred": due.height,
                "certification_batch": transition,
                "certification_gate": certification_gate_from_ledger(certification_ledger),
            }
        ids = set(due.get_column("document_id").drop_nulls().cast(pl.String).head(document_limit).to_list()) if "document_id" in due.columns else set()
        if not ids:
            return {"recovery_attempted": 0, "recovery_success": 0, "recovery_deferred": due.height, "recovery_phase": phase, "reason_code": "NO_CACHED_DOCUMENT_IDS"}
        failure_frame = read_parquet_snapshot(self.output / "930_API_FAILURES.parquet") if (self.output / "930_API_FAILURES.parquet").exists() else pl.DataFrame()
        timeout_audit = timeout_fingerprint(failure_frame)
        timeout_policy = recovery_timeout_policy(
            effective_phase,
            client_timeout_suspected=timeout_audit.get("CLIENT_READ_TIMEOUT_SUSPECTED") is True,
            connect_timeout=float(self.settings.connect_timeout),
        )
        extended_single_probe = timeout_policy["read_timeout"] is not None
        read_timeout_override = timeout_policy["read_timeout"]
        connect_timeout_override = timeout_policy["connect_timeout"]
        documents = documents.filter(pl.col("document_id").cast(pl.String).is_in(ids))
        actions = actions.filter(pl.col("document_id").cast(pl.String).is_in(ids))
        pipeline = Episode930Pipeline(
            self.settings,
            config=EpisodeConfig(
                max_ai_calls=call_limit,
                run_search=False,
                run_ai=True,
                apply=False,
                ai_request_timeout_override=read_timeout_override,
                ai_connect_timeout_override=connect_timeout_override,
                ai_max_retries_override=0 if extended_single_probe else None,
                bypass_ai_cache=effective_phase in {"SINGLE_PROBE", "MICRO_5", "MICRO_20"},
            ),
            output=self.run_dir,
        )
        _, ai_metrics = pipeline.classify_actions(documents, actions)
        failures = self._write_api_failure_artifact(documents)
        request_dir = self.run_dir / "05_API_CLASSIFICATION" / "ai_audit" / "requests"
        audit_rows = []
        for path_json in request_dir.glob("*.json"):
            try:
                value = json.loads(path_json.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            if str(value.get("slot_id")) in ids and str((value.get("input_summary") or {}).get("pass_name")) == "first_pass":
                audit_rows.append(value)
        probe_summary = summarize_api_recovery_probe(ids, audit_rows, ai_metrics)
        success_ids = probe_summary["success_ids"]
        attempted_ids = probe_summary["attempted_ids"]
        schema_valid_ids = probe_summary["schema_valid_ids"]
        cache_reuse_ids = probe_summary["cache_reuse_ids"]
        if success_ids:
            failure_path = self.output / "930_API_FAILURES.parquet"
            failure_frame = read_parquet_snapshot(failure_path) if failure_path.exists() else pl.DataFrame()
            if not failure_frame.is_empty() and "document_id" in failure_frame.columns:
                updated_rows = failure_frame.to_dicts()
                for row in updated_rows:
                    if str(row.get("document_id")) in success_ids and str(row.get("pass_name")) == "first_pass":
                        row.update({"status": "RESOLVED", "retryable": False, "recovery_status": "RECOVERED", "recovered_at": _now(), "recovery_run_id": self.run_id, "final_http_status": 200, "final_pass1_status": "response_completed", "next_retry_at": None, "resolved_at": row.get("resolved_at") or _now()})
                updated = pl.DataFrame(updated_rows, infer_schema_length=None).unique(subset=["failure_id"], keep="last")
                atomic_write_parquet(updated, failure_path, {"module": "episode_930_production", "artifact": "api_failures_recovery_update"}, key_columns=("failure_id",))
                remaining = updated.filter((pl.col("status") == "RETRYABLE_FAILURE") | (pl.col("recovery_status") == "PENDING_PROVIDER_RECOVERY"))
                atomic_write_parquet(remaining, self.output / "930_API_RECOVERY_QUEUE.parquet", {"module": "episode_930_production", "artifact": "api_recovery_queue"}, key_columns=("failure_id",))
        schema_valid = bool(success_ids) and schema_valid_ids == success_ids
        primary_provider_unavailable = bool(
            extended_single_probe
            and not schema_valid
            and any(
                str(row.get("failure_class") or "").upper()
                in {"READ_TIMEOUT", "CONNECT_TIMEOUT", "HTTP_5XX", "CONNECTION_ERROR", "UNKNOWN_PROVIDER_FAILURE"}
                for row in audit_rows
            )
        )
        if primary_provider_unavailable:
            current_provider = _read_json(self.provider_status_path)
            _atomic_json(
                self.provider_status_path,
                {
                    **current_provider,
                    "episode_id": EPISODE_ID,
                    "status": "PRIMARY_PROVIDER_UNAVAILABLE",
                    "primary_provider_unavailable": True,
                    "updated_at": _now(),
                },
            )
        if certification_batch is not None:
            certification_attempts = self._record_certification_attempts(
                certification_batch,
                audit_rows,
                cache_reuse_ids,
                ids,
            )
            certification_ledger, transition = self._update_certification_batch(
                certification_batch,
                certification_attempts,
                state_reason=probe_summary["reason_code"],
            )
            certification_gate = certification_gate_from_ledger(certification_ledger)
        else:
            transition = api_recovery_transition(
                effective_phase,
                len(attempted_ids),
                len(success_ids),
                schema_valid,
                schema_valid_successes=len(schema_valid_ids),
                cache_reuse_count=len(cache_reuse_ids),
            )
            certification_gate = None
        reason_code = probe_summary["reason_code"] or transition["reason_code"]
        state = {
            "episode_id": EPISODE_ID,
            "phase": transition["next_phase"],
            "last_phase": effective_phase,
            "last_attempted_documents": len(attempted_ids),
            "last_success_documents": len(success_ids),
            "last_success_rate": transition["success_rate"],
            "schema_valid": schema_valid,
            "reason_code": reason_code,
            "last_attempt_at": _now(),
            "last_success_at": _now() if success_ids else recovery_state.get("last_success_at"),
            "next_retry_at": _now_plus(minutes=30) if transition["backoff"] else None,
            "configured_read_timeout": read_timeout_override,
            "configured_connect_timeout": connect_timeout_override,
            "hard_wall_timeout_seconds": timeout_policy["hard_wall_timeout_seconds"],
            "timeout_fingerprint": timeout_audit,
            "timeout_policy_reason": timeout_policy["reason_code"],
            "primary_provider_unavailable": primary_provider_unavailable,
            "api_classification_status": ai_metrics.get("ai_status"),
            "api_cache_hits": int(ai_metrics.get("api_cache_hits") or 0),
            "cache_reuse_documents": len(cache_reuse_ids),
            "cache_reuse_document_ids": sorted(cache_reuse_ids),
            "provider_probe_attempted_documents": len(attempted_ids),
            "certification_batch_id": certification_batch.get("certification_batch_id") if certification_batch is not None else None,
            "certification_stage": certification_batch.get("stage") if certification_batch is not None else None,
            "certification_batch": transition if certification_batch is not None else None,
            "certification_gate": certification_gate,
            "updated_at": _now(),
        }
        _atomic_json(self.recovery_state_path, state)
        return {
            "recovery_attempted": len(attempted_ids),
            "recovery_success": len(success_ids),
            "recovery_success_rate": transition["success_rate"],
            "schema_valid": schema_valid,
            "recovery_phase": transition["next_phase"],
            "recovery_gate": transition["next_phase"],
            "reason_code": reason_code,
            "recovery_deferred": int(failures.get("provider_recovery_pending", 0) or 0),
            "core_priority_document": bool(ids.intersection(core_document_ids)),
            "fast_lane_priority_document": bool(ids.intersection(fast_lane_document_priorities)),
            "extended_single_probe": extended_single_probe,
            "timeout_fingerprint": timeout_audit,
            "timeout_policy_reason": timeout_policy["reason_code"],
            "primary_provider_unavailable": primary_provider_unavailable,
            "cache_reuse_documents": len(cache_reuse_ids),
            "api_classification_status": ai_metrics.get("ai_status"),
            "certification_batch": transition if certification_batch is not None else None,
            "certification_gate": certification_gate,
        }

    def _attachment_metrics(self, crawl_run_id: str) -> dict[str, Any]:
        """Count attachment discovery and successful archive rows for a run."""

        path = self.settings.curated / "attachments.parquet"
        if not path.exists():
            return {
                "attachments_found": 0,
                "pdfs_found": 0,
                "attachments_archived": 0,
                "pdfs_archived": 0,
                "pending": 0,
                "failed": 0,
                "already_present": 0,
                "retryable_failure": 0,
                "unrecoverable": 0,
            }
        frame = read_parquet_snapshot(path)
        if frame.is_empty() or "run_id" not in frame.columns:
            return {
                "attachments_found": 0,
                "pdfs_found": 0,
                "attachments_archived": 0,
                "pdfs_archived": 0,
                "pending": 0,
                "failed": 0,
                "already_present": 0,
                "retryable_failure": 0,
                "unrecoverable": 0,
            }
        frame = frame.filter(pl.col("run_id") == crawl_run_id)
        if frame.is_empty():
            return {
                "attachments_found": 0,
                "pdfs_found": 0,
                "attachments_archived": 0,
                "pdfs_archived": 0,
                "pending": 0,
                "failed": 0,
                "already_present": 0,
                "retryable_failure": 0,
                "unrecoverable": 0,
            }
        url = pl.col("url").cast(pl.String).str.to_lowercase()
        pdfs = frame.filter(url.str.contains(r"\.pdf(?:$|[?#])"))
        archived = frame.filter(
            (pl.col("status").is_in(["FETCHED", "ARCHIVED"]))
            & pl.col("content_sha256").is_not_null()
            & (pl.col("content_sha256").cast(pl.String).str.len_chars() > 0)
            & pl.col("local_path").is_not_null()
            & (pl.col("local_path").cast(pl.String).str.len_chars() > 0)
        )
        archived_pdfs = archived.filter(
            pl.col("url").cast(pl.String).str.to_lowercase().str.contains(r"\.pdf(?:$|[?#])")
        )
        return {
            "attachments_found": frame.height,
            "pdfs_found": pdfs.height,
            "attachments_archived": archived.height,
            "pdfs_archived": archived_pdfs.height,
            "pending": frame.filter(pl.col("status") == "PENDING_ATTACHMENT").height,
            "failed": frame.filter(pl.col("status") == "FAILED").height,
            "already_present": frame.filter(pl.col("status") == "ALREADY_PRESENT").height,
            "retryable_failure": frame.filter(pl.col("status") == "FAILED").height,
            "unrecoverable": frame.filter(pl.col("status") == "UNRECOVERABLE_WITH_REASON").height,
        }

    def _write_core_rolling_metrics(self) -> dict[str, Any]:
        """Publish deterministic frozen-core progress after each postprocess.

        This is an aggregation over already archived/curated artifacts.  It
        never fetches, calls an API, claims a queue item, or changes the frozen
        scope.  A later worker can therefore resume from the same checkpoint
        while the Dashboard sees incremental core progress.
        """

        scope_entities = analysis_ready_scope_entities(self.output)
        provenance = (
            read_parquet_snapshot(self.output / "930_COMPLETED_PROVENANCE_AUDIT.parquet")
            if (self.output / "930_COMPLETED_PROVENANCE_AUDIT.parquet").exists()
            else pl.DataFrame()
        )
        discovery = analysis_ready_discovery_progress(self.output, provenance)
        coverage_paths = sorted(
            (self.output / "production_runs").glob(
                "*/04_ACTION_EXTRACTION/2016_930_ACTION_EXTRACTION_COVERAGE.parquet"
            )
        )
        coverage_frames = [
            read_parquet_snapshot(path)
            for path in coverage_paths
            if path.exists()
        ]
        coverage_frames = [frame for frame in coverage_frames if not frame.is_empty()]
        coverage = (
            pl.concat(coverage_frames, how="diagonal_relaxed").unique(
                subset=["document_id"], keep="last"
            )
            if coverage_frames
            else pl.DataFrame()
        )
        production_document_frames = []
        for coverage_path in coverage_paths:
            document_path = (
                coverage_path.parent.parent
                / "07_DEDUP"
                / "2016_930_DOCUMENTS_DEDUP.parquet"
            )
            if not document_path.exists():
                continue
            document_frame = read_parquet_snapshot(document_path)
            if not document_frame.is_empty() and "document_id" in document_frame.columns:
                production_document_frames.append(document_frame)
        production_documents = (
            pl.concat(production_document_frames, how="diagonal_relaxed").unique(
                subset=["document_id"], keep="last"
            )
            if production_document_frames
            else pl.DataFrame()
        )
        document_lineage = build_core_document_lineage(
            scope_entities, production_documents
        )
        core_action_eligible, core_action_completed = core_action_coverage_metrics(
            coverage,
            scope_entities,
            document_lineage=document_lineage,
        )

        documents = scope_entities.get("core_documents")
        actions = scope_entities.get("core_actions")
        parameters = scope_entities.get("core_parameters")
        if not isinstance(documents, pl.DataFrame):
            documents = pl.DataFrame()
        if not isinstance(actions, pl.DataFrame):
            actions = pl.DataFrame()
        if not isinstance(parameters, pl.DataFrame):
            parameters = pl.DataFrame()
        date_resolved = 0
        if not actions.is_empty() and "action_id" in actions.columns:
            date_resolved = actions.filter(
                pl.col("effective_date").is_not_null()
                if "effective_date" in actions.columns
                else pl.lit(False)
            ).get_column("action_id").n_unique()
        parameter_count = (
            parameters.get_column("action_id").drop_nulls().n_unique()
            if not parameters.is_empty() and "action_id" in parameters.columns
            else 0
        )
        current_gap_paths = (
            self.run_dir / "03_GAP_AUDIT" / "2016_930_GAP_REGISTER.parquet",
            self.run_dir / "03_GAP_AUDIT" / "2016_930_GAP_AUDIT_PASS_2.parquet",
        )
        gaps = load_authoritative_episode_gaps(
            self.settings.curated / "policy_episode_gaps.parquet",
            current_gap_paths,
        )
        if not gaps.is_empty() and "episode_id" in gaps.columns:
            gaps = gaps.filter(pl.col("episode_id") == EPISODE_ID)
        gap_metrics = split_gap_metrics(
            gaps,
            core_document_ids=scope_entities.get("core_document_ids") or set(),
            core_action_ids=scope_entities.get("core_action_ids") or set(),
            core_city_ids=scope_entities.get("core_city_ids") or set(),
            core_city_names=scope_entities.get("core_city_names") or set(),
        )
        metrics = {
            "episode_id": EPISODE_ID,
            "scope_version": scope_entities.get("scope_version"),
            "scope_hash": scope_entities.get("scope_hash"),
            "run_id": self.run_id,
            "core_discovery_verified": int(discovery.get("core_verified") or 0),
            "core_discovery_total": int(discovery.get("core_eligible_total") or 0),
            "core_discovery_recovery_required": int(discovery.get("core_recovery_required") or 0),
            "core_action_eligible": int(core_action_eligible),
            "core_action_completed": int(core_action_completed),
            "core_official_documents": documents.height,
            "core_date_resolved": int(date_resolved),
            "core_parameters_processed": int(parameter_count),
            "analysis_ready_core_blocking_gaps": gap_metrics["analysis_ready_core_blocking_gaps"],
            "global_final_blocking_gaps": gap_metrics["global_final_blocking_gaps"],
            "updated_at": _now(),
        }
        _atomic_json(self.output / "930_ANALYSIS_READY_ROLLING_METRICS.json", metrics)
        return metrics

    def _maybe_export_analysis_ready(self) -> dict[str, Any]:
        """Evaluate the frozen lane and emit its CSV only when every gate passes."""

        scope = _read_json(self.analysis_scope_path)
        scope_entities = analysis_ready_scope_entities(self.output)
        documents_path = self.settings.curated / "policy_episode_documents.parquet"
        actions_path = self.settings.curated / "policy_episode_actions.parquet"
        parameters_path = self.settings.curated / "policy_episode_parameters.parquet"
        gaps_path = self.settings.curated / "policy_episode_gaps.parquet"
        documents = read_parquet_snapshot(documents_path) if documents_path.exists() else pl.DataFrame()
        actions = read_parquet_snapshot(actions_path) if actions_path.exists() else pl.DataFrame()
        parameters = read_parquet_snapshot(parameters_path) if parameters_path.exists() else pl.DataFrame()
        gaps = read_parquet_snapshot(gaps_path) if gaps_path.exists() else pl.DataFrame()
        for name, frame in (("documents", documents), ("actions", actions), ("parameters", parameters), ("gaps", gaps)):
            if not frame.is_empty() and "episode_id" in frame.columns:
                filtered = frame.filter(pl.col("episode_id") == EPISODE_ID)
                if name == "documents":
                    documents = filtered
                elif name == "actions":
                    actions = filtered
                elif name == "parameters":
                    parameters = filtered
                else:
                    gaps = filtered
        core_documents = scope_entities.get("core_documents")
        if not isinstance(core_documents, pl.DataFrame):
            core_documents = documents.head(0)
        core_doc_ids = scope_entities.get("core_document_ids") or set()
        core_actions = scope_entities.get("core_actions")
        if not isinstance(core_actions, pl.DataFrame):
            core_actions = actions.head(0)
        core_action_ids = scope_entities.get("core_action_ids") or set()
        core_parameters = scope_entities.get("core_parameters")
        if not isinstance(core_parameters, pl.DataFrame):
            core_parameters = parameters.head(0)
        split_gaps = split_gap_metrics(
            gaps,
            core_document_ids=core_doc_ids,
            core_action_ids=core_action_ids,
            core_city_ids=scope_entities.get("core_city_ids") or set(),
            core_city_names=scope_entities.get("core_city_names") or set(),
        )
        core_gaps = split_gaps["core_gap_frame"]
        global_gap_metrics = split_gaps["global_final_blocking_gaps"]

        api_frames = [
            read_parquet_snapshot(path)
            for path in sorted((self.output / "production_runs").glob("*/05_API_CLASSIFICATION/2016_930_API_CLASSIFICATION.parquet"))
        ]
        api_frames = [frame for frame in api_frames if not frame.is_empty()]
        api_rows = (
            pl.concat(api_frames, how="diagonal_relaxed").unique(
                subset=["request_id", "action_id", "pass_name"], keep="last"
            )
            if api_frames
            else pl.DataFrame()
        )
        api_core = (
            api_rows.filter(pl.col("document_id").cast(pl.String).is_in(sorted(core_doc_ids)))
            if core_doc_ids and not api_rows.is_empty() and "document_id" in api_rows.columns
            else api_rows.head(0)
        )
        pass1_docs = {
            str(value)
            for value in api_core.filter(pl.col("pass_name") == "first_pass")
            .get_column("document_id").drop_nulls().to_list()
        } if not api_core.is_empty() and {"pass_name", "document_id"}.issubset(api_core.columns) else set()
        pass2_docs = {
            str(value)
            for value in api_core.filter(pl.col("pass_name") == "second_review")
            .get_column("document_id").drop_nulls().to_list()
        } if not api_core.is_empty() and {"pass_name", "document_id"}.issubset(api_core.columns) else set()
        action_doc_ids = {
            str(value)
            for value in core_actions.get_column("document_id").drop_nulls().to_list()
        } if not core_actions.is_empty() and "document_id" in core_actions.columns else set()
        date_state_complete = bool(core_action_ids) and all(
            row.get("effective_date_basis") not in (None, "")
            for row in core_actions.iter_rows(named=True)
        )
        official_complete = bool(core_doc_ids) and all(
            bool(row.get("is_formal_eligible"))
            for row in core_documents.iter_rows(named=True)
        )
        provenance = (
            read_parquet_snapshot(self.output / "930_COMPLETED_PROVENANCE_AUDIT.parquet")
            if (self.output / "930_COMPLETED_PROVENANCE_AUDIT.parquet").exists()
            else pl.DataFrame()
        )
        core_discovery = analysis_ready_discovery_progress(self.output, provenance)
        recovery = (
            read_parquet_snapshot(self.false_recovery_path)
            if self.false_recovery_path.exists()
            else pl.DataFrame()
        )
        final_recovery_remaining = (
            recovery.filter(
                pl.col("status").is_in(["RECOVERY_REQUIRED", "PENDING", "IN_PROGRESS", "RUNNING", "RETRY_WAIT"])
            ).height
            if not recovery.is_empty() and "status" in recovery.columns
            else 0
        )
        gates = {
            "core_discovery": (
                int(core_discovery.get("core_eligible_total") or 0) > 0
                and int(core_discovery.get("core_verified") or 0)
                >= int(core_discovery.get("core_eligible_total") or 0)
            ),
            "official_evidence": official_complete,
            "action_extraction": bool(core_doc_ids) and core_doc_ids <= action_doc_ids,
            "api_pass1": bool(core_doc_ids) and core_doc_ids <= pass1_docs,
            "api_pass2": bool(core_doc_ids) and core_doc_ids <= pass2_docs,
            "date_verification": date_state_complete,
            "critical_dedup": (
                len(core_doc_ids) == core_documents.height
                and len(core_action_ids) == core_actions.height
            ),
            "critical_gaps": core_gaps.is_empty(),
            "formal_promotion": bool(core_doc_ids) and bool(core_action_ids),
            "dashboard_action_export": True,
        }
        decision = analysis_ready_decision(
            gates,
            pass1_success=len(pass1_docs.intersection(core_doc_ids)),
            pass1_total=len(core_doc_ids),
            pass2_success=len(pass2_docs.intersection(core_doc_ids)),
            final_recovery_remaining=final_recovery_remaining,
        )
        result = {
            **decision,
            "scope_hash": scope.get("scope_hash"),
            "scope_version": scope.get("scope_version"),
            "core_documents": len(core_doc_ids),
            "core_actions": len(core_action_ids),
            "core_gaps": core_gaps.height,
            "analysis_ready_core_blocking_gaps": split_gaps["analysis_ready_core_blocking_gaps"],
            "global_final_blocking_gaps": global_gap_metrics,
            "evaluated_at": _now(),
        }
        if decision["export_required"]:
            exporter = Episode930Pipeline(
                self.settings,
                config=EpisodeConfig(run_search=False, run_ai=False, apply=False),
                output=self.output,
            )
            export = exporter.final_export(
                core_documents,
                core_actions,
                core_parameters,
                pl.DataFrame(),
                pl.DataFrame(),
                core_gaps,
                {
                    "analysis_ready": True,
                    "export_status": "ANALYSIS_READY",
                    "source_run_ids": sorted(path.parent.name for path in (self.output / "production_runs").glob("*/POSTPROCESS_SUMMARY.json")),
                },
                api_rows=api_core,
            )
            result["export"] = export
            result["export_verified"] = (self.output / "2016_930_ANALYSIS_READY.csv").exists()
        _atomic_json(self.output / "930_ANALYSIS_READY_GATE.json", result)
        return result

    def _write_checkpoint(self, *, stage: str, status: str, **data: Any) -> dict[str, Any]:
        episode_status = str(data.pop("episode_status", status))
        last_micro_batch_status = str(data.pop("last_micro_batch_status", status))
        payload = {
            "episode_id": EPISODE_ID,
            "run_id": self.run_id,
            "stage": stage,
            "status": status,
            "episode_status": episode_status,
            "last_micro_batch_status": last_micro_batch_status,
            "updated_at": _now(),
            **data,
        }
        _atomic_json(self.checkpoint_path, payload)
        transition_id = stable_id(
            EPISODE_ID,
            self.run_id,
            stage,
            status,
            prefix="EP930TRANS",
        )
        transition_path = self.run_dir / "STAGE_TRANSITIONS.jsonl"
        existing_ids: set[str] = set()
        if transition_path.exists():
            for line in transition_path.read_text(encoding="utf-8").splitlines():
                try:
                    item = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if item.get("transition_id"):
                    existing_ids.add(str(item["transition_id"]))
        if transition_id not in existing_ids:
            with transition_path.open("a", encoding="utf-8") as stream:
                stream.write(
                    json.dumps(
                        {
                            "transition_id": transition_id,
                            "idempotency_key": transition_id,
                            "episode_id": EPISODE_ID,
                            "run_id": self.run_id,
                            "stage": stage,
                            "status": status,
                            "reason_code": str(data.get("reason_code") or status.lower()),
                            "timestamp": payload["updated_at"],
                        },
                        ensure_ascii=False,
                    )
                    + "\n"
                )
        self._snapshot(stage=stage, status=episode_status, last_micro_batch_status=last_micro_batch_status, **data)
        return payload

    def build_plan(self) -> dict[str, Any]:
        cities = load_cities_105(self.settings)
        now = _now()
        windows = (
            ("core", CORE_START, CORE_END),
            ("extended", date(2016, 9, 20), date(2016, 10, 15)),
            ("provenance", PROVENANCE_START, PROVENANCE_END),
        )
        categories = (
            ("market", "房地产市场平稳健康发展 房地产市场调控"),
            ("purchase", "限购 购房资格 非本市户籍"),
            ("credit", "首付款 首付比例 差别化住房信贷 住房贷款 认房认贷"),
            ("supply", "限售 土地供应 住宅用地 预售监管 价格备案"),
            ("supervision", "捂盘惜售 中介监管 行政规范性文件 政府公报"),
        )
        plan_rows: list[dict[str, Any]] = []
        queue_rows: list[dict[str, Any]] = []
        registry = load_registry(self.settings)
        for city_row in cities.iter_rows(named=True):
            city_id = _text(city_row.get("city_id"))
            city = _text(city_row.get("city_name"))
            province = _text(city_row.get("province_name"))
            official_domains = sorted(
                {
                    _text(source.domain)
                    for source in registry
                    if source.crawl_enabled
                    and (
                        source.scope_type == "national"
                        or city_id in source.city_ids
                        or _text(city_row.get("province_code")) in source.province_codes
                    )
                    and _text(source.domain)
                }
            )
            official_source = ";".join(official_domains) or None
            for window_name, window_start, window_end in windows:
                for query_type, query_text in categories:
                    search_plan_id = stable_id(
                        EPISODE_ID, city_id, window_name, query_type,
                        prefix="PLAN930",
                    )
                    fallback = f"{city} {query_text} 2016 site:gov.cn"
                    plan_rows.append(
                        {
                            "search_plan_id": search_plan_id,
                            "episode_id": EPISODE_ID,
                            "city_id": city_id,
                            "city": city,
                            "province": province,
                            "window_name": window_name,
                            "window_start": window_start,
                            "window_end": window_end,
                            "query_type": query_type,
                            "query_text": f"{city} {query_text}",
                            "keyword_group": query_type,
                            "expected_policy_category": query_type,
                            "official_source": official_source,
                            "fallback_discovery_query": fallback,
                            "priority": 10 if window_name == "core" else 20 if window_name == "extended" else 30,
                            "created_at": now,
                        }
                    )
                    queue_rows.append(
                        {
                            "queue_item_id": stable_id(
                                EPISODE_ID, city_id, window_name, query_type,
                                prefix="QUEUE930",
                            ),
                            "episode_id": EPISODE_ID,
                            "city_id": city_id,
                            "city": city,
                            "province": province,
                            "task_stage": "930_DISCOVERY",
                            "source_id": None,
                            "source_role": None,
                            "query_type": query_type,
                            "query_text": f"{city} {query_text}",
                            "window_start": window_start,
                            "window_end": window_end,
                            "status": "PENDING",
                            "priority": 10 if window_name == "core" else 20 if window_name == "extended" else 30,
                            "attempt_count": 0,
                            "lease_owner": None,
                            "lease_acquired_at": None,
                            "lease_expires_at": None,
                            "last_attempt_at": None,
                            "completed_at": None,
                            "documents_found": 0,
                            "documents_recovered": 0,
                            "actions_extracted": 0,
                            "actions_classified": 0,
                            "pdfs_found": 0,
                            "pdfs_archived": 0,
                            "failure_reason": None,
                            "updated_at": now,
                            "execution_status": "PENDING",
                            "fetch_status": "NOT_ATTEMPTED",
                            "result_status": "NO_RESULT",
                            "search_provider": None,
                            "search_executed": False,
                            "search_call_count": 0,
                            "search_result_count": 0,
                            "http_request_count": 0,
                            "real_network_fetch": False,
                            "last_http_status": None,
                            "response_bytes": 0,
                            "cache_hit": False,
                            "content_sha256": None,
                            "crawl_run_id": None,
                            "crawl_item_id": None,
                            "document_version_id": None,
                            "evidence_path": None,
                        }
                    )
        plan = pl.DataFrame(plan_rows, schema=SEARCH_PLAN_SCHEMA).sort(
            ["priority", "city_id", "window_start", "query_type"]
        )
        generated_queue = pl.DataFrame(queue_rows, schema=QUEUE_SCHEMA).sort(
            ["priority", "city_id", "window_start", "query_type"]
        )
        queue = generated_queue
        immutable_existing_queue = False
        if self.queue_path.exists():
            existing = read_parquet_snapshot(self.queue_path)
            if not existing.is_empty() and "queue_item_id" in existing.columns:
                # The deployed 1575-row queue is historical state, not a
                # template.  Once present it is read as-is and never rebuilt
                # by a later bounded run.
                queue = _ensure_queue_columns(existing)
                immutable_existing_queue = True
        # A completed queue item is terminal for this episode plan.  Only
        # pending/retry items can be claimed by a later bounded run; the
        # crawler may still be asked to resume its own crawl checkpoint, but
        # the episode queue must not report old work as newly claimed.
        if not self.search_plan_path.exists():
            atomic_write_parquet(
                plan,
                self.search_plan_path,
                {"module": "episode_930_production", "run_id": self.run_id},
                key_columns=("search_plan_id",),
            )
        if not immutable_existing_queue:
            atomic_write_parquet(
                queue,
                self.queue_path,
                {"module": "episode_930_production", "run_id": self.run_id},
                key_columns=("queue_item_id",),
            )
        analysis_scope = freeze_analysis_ready_scope(
            self.analysis_scope_path,
            queue,
            created_at=now,
        )
        if self.false_recovery_path.exists():
            recovery = read_parquet_snapshot(self.false_recovery_path)
            critical_city_ids, critical_city_names = self._critical_gap_members()
            prioritized = prioritize_recovery_queue(
                recovery,
                queue,
                analysis_scope,
                critical_city_ids=critical_city_ids,
                critical_city_names=critical_city_names,
            )
            atomic_write_parquet(
                prioritized,
                self.priority_recovery_path,
                {
                    "module": "episode_930_production",
                    "artifact": "analysis_ready_priority_overlay",
                    "scope_hash": analysis_scope.get("scope_hash"),
                },
                key_columns=("recovery_id",),
            )
        self._write_checkpoint(
            stage="930_SCOPE_BUILD",
            status="PLANNED",
            queue_total=queue.height,
            queue_completed=int(
                queue.filter(pl.col("status") != "PENDING").height
            ),
            queue_pending=int(queue.filter(pl.col("status") == "PENDING").height),
            queue_running=0,
            queue_failed=0,
            search_plan_rows=plan.height,
            analysis_ready_scope_version=analysis_scope.get("scope_version"),
            analysis_ready_scope_hash=analysis_scope.get("scope_hash"),
            analysis_ready_scope_cities=analysis_scope.get("city_count"),
        )
        _atomic_json(
            self.run_dir / "SCOPE.json",
            {
                "episode_id": EPISODE_ID,
                "episode_name": EPISODE_NAME,
                "core_window": [CORE_START.isoformat(), CORE_END.isoformat()],
                "provenance_window": [PROVENANCE_START.isoformat(), PROVENANCE_END.isoformat()],
                "city_count": cities.height,
                "search_plan_rows": plan.height,
                "queue_rows": queue.height,
                "generated_at": now,
            },
        )
        return {
            "episode_id": EPISODE_ID,
            "run_id": self.run_id,
            "search_plan_rows": plan.height,
            "queue_total": queue.height,
            "cities": cities.height,
            "search_plan": str(self.search_plan_path),
            "queue": str(self.queue_path),
            "checkpoint": str(self.checkpoint_path),
        }

    def _selected_cities(self, requested: list[str] | None) -> list[str]:
        cities = load_cities_105(self.settings)
        if requested:
            wanted = {str(value).strip() for value in requested if str(value).strip()}
            matched = cities.filter(
                pl.col("city_id").cast(pl.String).is_in(sorted(wanted))
                | pl.col("city_name").cast(pl.String).is_in(sorted(wanted))
                | pl.col("city_name_short").cast(pl.String).is_in(sorted(wanted))
            )
            return [str(value) for value in matched.get_column("city_id").to_list()]

        # With no explicit scope, rotate across the 105-city queue.  A
        # bounded run should not spend every invocation exhausting the first
        # five cities while untouched cities wait behind them.  Completed
        # queue items are the durable progress signal; the queue itself is
        # still the source of truth for what can be claimed.
        if self.queue_path.exists():
            queue = read_parquet_snapshot(self.queue_path)
            queue = _ensure_queue_columns(queue)
            if not queue.is_empty():
                recovery_ids: set[str] = set()
                recovery_path = (
                    self.priority_recovery_path
                    if self.priority_recovery_path.exists()
                    else self.false_recovery_path
                )
                if recovery_path.exists():
                    recovery = read_parquet_snapshot(recovery_path)
                    if not recovery.is_empty() and {"queue_item_id", "status"}.issubset(recovery.columns):
                        recovery_ids = {
                            str(value)
                            for value in recovery.filter(
                                pl.col("status").is_in(["RECOVERY_REQUIRED", "RETRY_WAIT"])
                            ).get_column("queue_item_id").to_list()
                        }
                city_order = {
                    str(value): index
                    for index, value in enumerate(cities.get_column("city_id").to_list())
                }
                eligible = queue.filter(
                    pl.col("status").is_in(["PENDING", "RETRY_WAIT"])
                    | (
                        pl.col("status").is_in(["CRAWL_COMPLETED", "COMPLETED"])
                        & pl.col("queue_item_id").cast(pl.String).is_in(sorted(recovery_ids))
                    )
                )
                if not eligible.is_empty():
                    progress = (
                        queue.with_columns(
                            pl.col("status")
                            .is_in(["CRAWL_COMPLETED", "COMPLETED"])
                            .cast(pl.Int64)
                            .alias("_completed")
                        )
                        .group_by("city_id")
                        .agg(pl.col("_completed").sum().alias("completed_items"))
                        .to_dicts()
                    )
                    completed_by_city = {
                        str(row.get("city_id")): int(row.get("completed_items") or 0)
                        for row in progress
                    }
                    pending_ids = {
                        str(value)
                        for value in eligible.get_column("city_id").unique().to_list()
                    }
                    selected = sorted(
                        pending_ids,
                        key=lambda city_id: (
                            completed_by_city.get(city_id, 0),
                            city_order.get(city_id, 10_000),
                        ),
                    )[: self.city_limit]
                    if selected:
                        return selected
        return [
            str(value)
            for value in cities.head(self.city_limit).get_column("city_id").to_list()
        ]

    def _next_work_source(self, requested: list[str] | None = None) -> dict[str, Any]:
        """Resolve the global lane before ordinary city rotation is applied."""

        queue = (
            _ensure_queue_columns(read_parquet_snapshot(self.queue_path))
            if self.queue_path.exists()
            else pl.DataFrame()
        )
        recovery_path = (
            self.false_recovery_path
            if self.false_recovery_path.exists()
            else self.priority_recovery_path
        )
        recovery = read_parquet_snapshot(recovery_path) if recovery_path.exists() else pl.DataFrame()
        scope = _read_json(self.analysis_scope_path)
        critical_city_ids, critical_city_names = self._critical_gap_members()
        decision = select_next_work_source(
            recovery,
            queue,
            scope,
            city_limit=self.city_limit,
            critical_city_ids=critical_city_ids,
            critical_city_names=critical_city_names,
        )
        if decision["work_source"] == WORK_SOURCE_ORDINARY_RAW_PENDING:
            decision["cities"] = self._selected_cities(requested)
            decision["reason_code"] = "ORDINARY_RAW_PENDING_AFTER_RECOVERY_LANES"
        return decision

    def _update_recovery_overlay(
        self,
        queue_item_ids: set[str],
        updates: dict[str, Any],
    ) -> None:
        """Update recovery runtime state without mutating terminal raw queue rows."""

        if not queue_item_ids:
            return
        for path in (self.false_recovery_path, self.priority_recovery_path):
            if not path.exists():
                continue
            frame = read_parquet_snapshot(path)
            if frame.is_empty() or "queue_item_id" not in frame.columns:
                continue
            rows = frame.to_dicts()
            for row in rows:
                if str(row.get("queue_item_id") or "") in queue_item_ids:
                    row.update(updates)
                    row["updated_at"] = _now()
            updated = pl.DataFrame(rows, infer_schema_length=None)
            key = "recovery_id" if "recovery_id" in updated.columns else "queue_item_id"
            atomic_write_parquet(
                updated,
                path,
                {"module": "episode_930_production", "artifact": "recovery_runtime_overlay"},
                key_columns=(key,),
            )

    def _critical_gap_members(self) -> tuple[set[str], set[str]]:
        """Read current authoritative critical-gap city membership only."""

        monitor = _read_json(self.output / "930_MONITOR_SNAPSHOT.json")
        progress = monitor.get("GLOBAL_EPISODE_PROGRESS") or {}
        source_value = progress.get("authoritative_gap_source") or monitor.get("gap_authoritative_source")
        candidates: list[Path] = []
        if source_value:
            candidates.append(Path(str(source_value)))
        production_root = self.output / "production_runs"
        if production_root.exists():
            candidates.extend(
                sorted(
                    production_root.glob("*/03_GAP_AUDIT/*GAP_REGISTER.parquet"),
                    key=lambda path: path.stat().st_mtime,
                    reverse=True,
                )
            )
        seen: set[str] = set()
        for path in candidates:
            path_key = str(path)
            if path_key in seen or not path.exists():
                continue
            seen.add(path_key)
            try:
                gaps = read_parquet_snapshot(path)
            except Exception:
                continue
            if gaps.is_empty() or "severity" not in gaps.columns:
                continue
            critical = gaps.filter(
                pl.col("severity")
                .cast(pl.String)
                .str.to_uppercase()
                .is_in(["HIGH", "CRITICAL"])
            )
            city_ids = (
                {
                    str(value)
                    for value in critical.get_column("city_id").drop_nulls().to_list()
                    if str(value).strip()
                }
                if "city_id" in critical.columns
                else set()
            )
            city_names = (
                {
                    str(value)
                    for value in critical.get_column("city").drop_nulls().to_list()
                    if str(value).strip()
                }
                if "city" in critical.columns
                else set()
            )
            return city_ids, city_names
        return set(), set()

    def _append_recovery_claim_audit(
        self,
        claims: list[dict[str, Any]],
        *,
        job_id: str,
        claimed_at: str,
        scope_hash: str | None,
        work_source: str | None = None,
    ) -> None:
        if not claims:
            return
        rows = []
        for row in claims:
            queue_item_id = str(row.get("queue_item_id") or "")
            recovery_id = str(row.get("recovery_id") or "")
            normalized_priority = _safe_priority(row.get("normalized_priority"), 2)
            rows.append(
                {
                    "claim_id": stable_id(
                        EPISODE_ID,
                        self.run_id,
                        job_id,
                        queue_item_id,
                        claimed_at,
                        prefix="CLAIM930",
                    ),
                    "task_id": queue_item_id,
                    "recovery_id": recovery_id,
                    "normalized_priority": normalized_priority,
                    "priority_reason": str(row.get("priority_reason") or "GLOBAL_FINAL_RECOVERY"),
                    "core_scope_member": bool(row.get("core_scope_member")),
                    "critical_gap_member": bool(row.get("critical_gap_member")),
                    "work_source": str(work_source or _work_source_for_priority(normalized_priority)),
                    "claimed_at": claimed_at,
                    "worker_pid": os.getpid(),
                    "worker_generation": "POST_PRIORITY_HOTFIX_WORKER",
                    "priority_hotfix_version": PRIORITY_HOTFIX_VERSION,
                    "job_id": job_id,
                    "run_id": self.run_id,
                    "scope_hash": scope_hash,
                }
            )
        new_audit = pl.DataFrame(rows)
        if self.recovery_claim_audit_path.exists():
            existing = read_parquet_snapshot(self.recovery_claim_audit_path)
            audit = pl.concat([existing, new_audit], how="diagonal_relaxed")
        else:
            audit = new_audit
        audit = audit.unique(subset=["claim_id"], keep="last", maintain_order=True)
        atomic_write_parquet(
            audit,
            self.recovery_claim_audit_path,
            {
                "module": "episode_930_production",
                "artifact": "recovery_claim_audit",
                "run_id": self.run_id,
                "job_id": job_id,
                "priority_hotfix_version": PRIORITY_HOTFIX_VERSION,
            },
            key_columns=("claim_id",),
        )

    def _inner_request(
        self,
        request: CrawlJobRequest,
        cities: list[str],
        *,
        queue_item_ids: list[str] | None = None,
    ) -> CrawlJobRequest:
        drain = bool(request.drain_selected_batch)
        candidate_limit = (
            min(request.max_candidates, request.max_candidates_total or request.max_candidates)
            if drain
            else 40
        )
        return CrawlJobRequest(
            mode="historical_episode_930",
            episode_id=EPISODE_ID,
            episode_run_id=self.run_id,
            episode_queue_path=str(self.queue_path),
            episode_output_path=str(self.output),
            episode_queue_item_ids=list(queue_item_ids or []),
            start_date=PROVENANCE_START,
            end_date=PROVENANCE_END,
            cities=cities,
            topics=list(SEARCH_TERMS),
            max_candidates=candidate_limit,
            max_candidates_total=candidate_limit,
            max_candidates_per_source=request.max_candidates_per_source if drain else 20,
            max_pages_per_source=request.max_pages_per_source if drain else 5,
            batch_size=request.batch_size if drain else 20,
            global_safety_limit=request.global_safety_limit if drain else 40,
            resume=True,
            max_fetches=request.max_fetches,
            drain_selected_batch=drain,
            max_attachment_attempts=request.max_attachment_attempts,
            enabled_only=True,
            run_glm=False,
            run_verification=False,
            rebuild_database=False,
            run_validation=False,
            official_first=True,
            runtime_mode=request.runtime_mode,
            production_write_allowed=request.production_write_allowed,
            processing_mode="staged_only",
        )

    def _update_queue(
        self,
        *,
        cities: list[str],
        status: str,
        metrics: dict[str, Any],
        error: str | None = None,
        queue_item_ids: list[str] | None = None,
    ) -> None:
        if not self.queue_path.exists():
            return
        queue = read_parquet_snapshot(self.queue_path)
        queue = _ensure_queue_columns(queue)
        if queue.is_empty():
            return
        target_ids = {
            str(value) for value in (queue_item_ids or []) if value
        }
        if not target_ids:
            target_ids = {
                str(value)
                for value in queue.filter(pl.col("city_id").is_in(cities))["queue_item_id"].to_list()
            }
        http_path_text = str(metrics.get("queue_http_audit_path") or "").strip()
        search_path_text = str(metrics.get("queue_search_audit_path") or "").strip()
        http_path = Path(http_path_text) if http_path_text else None
        search_path = Path(search_path_text) if search_path_text else None
        http_rows = []
        search_rows = []
        if http_path is not None and http_path.is_file():
            http_frame = read_parquet_snapshot(http_path)
            if "queue_item_id" in http_frame.columns:
                http_rows = http_frame.filter(
                    pl.col("queue_item_id").cast(pl.String).is_in(sorted(target_ids))
                ).to_dicts()
        if search_path is not None and search_path.is_file():
            search_frame = read_parquet_snapshot(search_path)
            if "queue_item_id" in search_frame.columns:
                search_rows = search_frame.filter(
                    pl.col("queue_item_id").cast(pl.String).is_in(sorted(target_ids))
                ).to_dicts()
        by_http: dict[str, list[dict]] = {}
        by_search: dict[str, list[dict]] = {}
        for row in http_rows:
            by_http.setdefault(str(row.get("queue_item_id") or ""), []).append(row)
        for row in search_rows:
            by_search.setdefault(str(row.get("queue_item_id") or ""), []).append(row)
        now = _now()
        updated_rows: list[dict[str, Any]] = []
        terminal_updates: dict[str, dict[str, Any]] = {}
        for row in queue.iter_rows(named=True):
            item_id = str(row.get("queue_item_id") or "")
            if item_id not in target_ids:
                updated_rows.append(row)
                continue
            item_http = by_http.get(item_id, [])
            item_search = by_search.get(item_id, [])
            result_urls = [value for value in (entry.get("result_url") for entry in item_search) if value]
            successful_http = [
                entry
                for entry in item_http
                if bool(entry.get("real_network_fetch"))
                and str(entry.get("fetch_status") or "") in {"LIVE_FETCH_SUCCESS", "CACHE_HIT"}
            ]
            versions = [
                entry for entry in item_http if entry.get("document_version_id")
            ]
            live_success = bool(successful_http)
            if live_success:
                derived_status = "CRAWL_COMPLETED"
                fetch_status = "CACHE_HIT" if all(
                    str(entry.get("fetch_status") or "") == "CACHE_HIT" for entry in successful_http
                ) else "LIVE_FETCH_SUCCESS"
                result_status = "DOCUMENT_FOUND" if versions else "DISCOVERY_ONLY"
                failure_reason = None
            else:
                # Keep the old historical status untouched in the audit trail,
                # but make the current queue retryable until a live response
                # is persisted.  This prevents another false completion.
                derived_status = "RETRY_WAIT"
                fetch_status = (
                    "NETWORK_FAILED" if item_http else "NOT_ATTEMPTED"
                )
                result_status = "DISCOVERY_ONLY" if result_urls else "NO_RESULT"
                failure_reason = error or (
                    "queue_scoped_search_or_http_evidence_missing"
                )
            if str(row.get("status") or "") in {"CRAWL_COMPLETED", "COMPLETED"}:
                # The original completed status is historical audit evidence.
                # Recovery outcome belongs only to the independent overlay.
                terminal_updates[item_id] = {
                    "status": "RECOVERY_COMPLETED" if live_success else "RETRY_WAIT",
                    "result_status": result_status,
                    "fetch_status": fetch_status,
                    "search_executed": bool(item_search),
                    "http_request_count": len(item_http),
                    "real_network_fetch": live_success,
                    "document_version_id": next(
                        (str(entry.get("document_version_id")) for entry in reversed(item_http) if entry.get("document_version_id")),
                        None,
                    ),
                    "failure_reason": failure_reason,
                    "lease_owner": None,
                    "lease_acquired_at": None,
                    "lease_expires_at": None,
                    "completed_at": now if live_success else None,
                }
                updated_rows.append(row)
                continue
            row.update(
                {
                    "status": derived_status,
                    "execution_status": "TASK_COMPLETED" if item_search or item_http else "TASK_FAILED",
                    "fetch_status": fetch_status,
                    "result_status": result_status,
                    "search_provider": str(item_search[0].get("provider") or "") if item_search else None,
                    "search_executed": bool(item_search),
                    "search_call_count": 1 if item_search else 0,
                    "search_result_count": len(result_urls),
                    "http_request_count": len(item_http),
                    "real_network_fetch": live_success,
                    "last_http_status": next(
                        (entry.get("http_status") for entry in reversed(item_http) if entry.get("http_status") is not None),
                        None,
                    ),
                    "response_bytes": sum(int(entry.get("response_bytes") or 0) for entry in item_http),
                    "cache_hit": any(bool(entry.get("cache_hit")) for entry in item_http),
                    "content_sha256": next(
                        (str(entry.get("content_sha256")) for entry in reversed(item_http) if entry.get("content_sha256")),
                        None,
                    ),
                    "crawl_run_id": next(
                        (str(entry.get("crawl_run_id")) for entry in item_http if entry.get("crawl_run_id")),
                        str(metrics.get("run_id") or "") or None,
                    ),
                    "crawl_item_id": next(
                        (str(entry.get("crawl_item_id")) for entry in item_http if entry.get("crawl_item_id")),
                        None,
                    ),
                    "document_version_id": next(
                        (str(entry.get("document_version_id")) for entry in reversed(item_http) if entry.get("document_version_id")),
                        None,
                    ),
                    "evidence_path": str(http_path) if http_path is not None and http_path.exists() else None,
                    "attempt_count": int(row.get("attempt_count") or 0) + 1,
                    "updated_at": now,
                    "completed_at": now if live_success else None,
                    "documents_found": len({str(entry.get("document_version_id")) for entry in versions}),
                    "documents_recovered": len(versions),
                    "failure_reason": failure_reason,
                    "lease_owner": None,
                    "lease_expires_at": None,
                }
            )
            updated_rows.append(row)
        queue = pl.DataFrame(updated_rows, infer_schema_length=None)
        for column, dtype in QUEUE_SCHEMA.items():
            if column not in queue.columns:
                queue = queue.with_columns(pl.lit(None).cast(dtype).alias(column))
            else:
                queue = queue.with_columns(pl.col(column).cast(dtype, strict=False).alias(column))
        queue = queue.select(list(QUEUE_SCHEMA))
        atomic_write_parquet(
            queue,
            self.queue_path,
            {"module": "episode_930_production", "run_id": self.run_id},
            key_columns=("queue_item_id",),
        )
        for item_id, recovery_update in terminal_updates.items():
            self._update_recovery_overlay({item_id}, recovery_update)

    def _claim_queue(
        self,
        cities: list[str],
        *,
        job_id: str,
        work_source: str | None = None,
    ) -> dict[str, Any]:
        if not self.queue_path.exists():
            return {"claimed": 0, "queue_item_ids": [], "work_source": work_source}
        queue = read_parquet_snapshot(self.queue_path)
        queue = _ensure_queue_columns(queue)
        if queue.is_empty():
            return {"claimed": 0, "queue_item_ids": [], "work_source": work_source}
        now = datetime.now(UTC)
        lease_expires_at = datetime.fromtimestamp(
            now.timestamp() + 3600, UTC
        ).isoformat()
        city_set = {str(city) for city in cities}
        recovery = pl.DataFrame()
        if self.false_recovery_path.exists():
            # The false-completion queue is authoritative for recovery
            # status/lease.  The priority overlay is only an optional
            # compatibility artifact and is never trusted for those fields.
            recovery = read_parquet_snapshot(self.false_recovery_path)
        elif self.priority_recovery_path.exists():
            recovery = read_parquet_snapshot(self.priority_recovery_path)
        scope = _read_json(self.analysis_scope_path)
        scope_hash = validate_frozen_analysis_ready_scope(scope)
        critical_city_ids, critical_city_names = self._critical_gap_members()
        recovery_claim_rows = select_recovery_claim_rows(
            recovery,
            queue,
            scope,
            cities,
            critical_city_ids=critical_city_ids,
            critical_city_names=critical_city_names,
            now=now,
        )
        recovery_ids = {
            str(row.get("queue_item_id") or "")
            for row in recovery.iter_rows(named=True)
            if str(row.get("status") or "") == RECOVERY_REQUIRED_STATUS
            and not _active_lease(row.get("lease_expires_at"), now)
        } if not recovery.is_empty() and {"queue_item_id", "status"}.issubset(recovery.columns) else set()
        if (
            (recovery_ids and not recovery_claim_rows)
            or (
                work_source
                and work_source != WORK_SOURCE_ORDINARY_RAW_PENDING
                and not recovery_claim_rows
            )
        ):
            # A lower lane must never fill a batch while a higher lane exists
            # globally.  The runner will rotate naturally until the selected
            # city set contains the global minimum lane.
            return {
                "claimed": 0,
                "queue_item_ids": [],
                "recovery_claimed": 0,
                "recovery_priority_blocked": True,
                "work_source": work_source,
                "priority_hotfix_version": PRIORITY_HOTFIX_VERSION,
            }
        queue_by_id = {
            str(row.get("queue_item_id") or ""): row
            for row in queue.iter_rows(named=True)
            if row.get("queue_item_id")
        }
        if recovery_claim_rows:
            selected_rows = [
                queue_by_id[str(row.get("queue_item_id") or "")]
                for row in recovery_claim_rows
                if str(row.get("queue_item_id") or "") in queue_by_id
            ]
        else:
            eligible_rows: list[dict[str, Any]] = []
            for row in queue.iter_rows(named=True):
                if str(row.get("city_id") or "") not in city_set:
                    continue
                status = str(row.get("status") or "")
                eligible = status in {"PENDING", "RETRY_WAIT"}
                if status in {"CRAWL_COMPLETED", "COMPLETED"}:
                    # A task completion is not a network completion.  Old
                    # rows remain historical evidence; recovery status lives
                    # in the independent overlay.
                    eligible = str(row.get("queue_item_id") or "") in recovery_ids
                if status == "RUNNING":
                    raw_expiry = row.get("lease_expires_at")
                    try:
                        expiry = datetime.fromisoformat(str(raw_expiry))
                        if expiry.tzinfo is None:
                            expiry = expiry.replace(tzinfo=UTC)
                        eligible = expiry <= now or str(row.get("lease_owner") or "") == job_id
                    except (TypeError, ValueError):
                        eligible = True
                if eligible:
                    eligible_rows.append(row)
            eligible_rows.sort(
                key=lambda row: (
                    int(row.get("priority") or 0),
                    str(row.get("city_id") or ""),
                    str(row.get("window_start") or ""),
                    str(row.get("query_type") or ""),
                )
            )
            selected_rows = []
            seen_cities: set[str] = set()
            for row in eligible_rows:
                city_id = str(row.get("city_id") or "")
                if city_id in seen_cities:
                    continue
                selected_rows.append(row)
                seen_cities.add(city_id)
                if len(selected_rows) >= len(cities):
                    break
        ids = [str(row["queue_item_id"]) for row in selected_rows]
        if not ids:
            return {"claimed": 0, "queue_item_ids": [], "work_source": work_source}
        terminal_ids = {
            str(row.get("queue_item_id"))
            for row in selected_rows
            if str(row.get("status") or "") in {"CRAWL_COMPLETED", "COMPLETED"}
        }
        raw_claim_ids = [item_id for item_id in ids if item_id not in terminal_ids]
        queue = queue.with_columns(
            pl.when(pl.col("queue_item_id").is_in(raw_claim_ids))
            .then(pl.lit("RUNNING"))
            .otherwise(pl.col("status"))
            .alias("status"),
            pl.when(pl.col("queue_item_id").is_in(raw_claim_ids))
            .then(pl.lit(job_id))
            .otherwise(pl.col("lease_owner"))
            .alias("lease_owner"),
            pl.when(pl.col("queue_item_id").is_in(raw_claim_ids))
            .then(pl.lit(now.isoformat()))
            .otherwise(pl.col("lease_acquired_at"))
            .alias("lease_acquired_at"),
            pl.when(pl.col("queue_item_id").is_in(raw_claim_ids))
            .then(pl.lit(lease_expires_at))
            .otherwise(pl.col("lease_expires_at"))
            .alias("lease_expires_at"),
            pl.when(pl.col("queue_item_id").is_in(raw_claim_ids))
            .then(pl.lit(now.isoformat()))
            .otherwise(pl.col("updated_at"))
            .alias("updated_at"),
        )
        atomic_write_parquet(
            queue,
            self.queue_path,
            {"module": "episode_930_production", "run_id": self.run_id, "job_id": job_id},
            key_columns=("queue_item_id",),
        )
        recovery_claim_ids = {
            str(row.get("queue_item_id") or "")
            for row in recovery_claim_rows
        }
        self._update_recovery_overlay(
            recovery_claim_ids,
            {
                "status": "RUNNING",
                "lease_owner": job_id,
                "lease_acquired_at": now.isoformat(),
                "lease_expires_at": lease_expires_at,
            },
        )
        self._append_recovery_claim_audit(
            recovery_claim_rows,
            job_id=job_id,
            claimed_at=now.isoformat(),
            scope_hash=scope_hash,
            work_source=work_source,
        )
        selected_priority = (
            min(
                _safe_priority(row.get("normalized_priority"), 2)
                for row in recovery_claim_rows
            )
            if recovery_claim_rows
            else None
        )
        return {
            "claimed": len(ids),
            "queue_item_ids": ids,
            "lease_expires_at": lease_expires_at,
            "recovery_overlay_claimed": len(terminal_ids),
            "recovery_claimed": len(recovery_claim_rows),
            "recovery_claim_priorities": {
                str(row.get("queue_item_id") or ""): _safe_priority(row.get("normalized_priority"), 2)
                for row in recovery_claim_rows
            },
            "work_source": work_source or (
                _work_source_for_priority(selected_priority)
                if selected_priority is not None
                else WORK_SOURCE_ORDINARY_RAW_PENDING
            ),
            "work_source_priority": selected_priority,
            "priority_hotfix_version": PRIORITY_HOTFIX_VERSION,
        }

    def _versions_for_run(self, crawl_run_id: str) -> pl.DataFrame:
        versions_path = self.settings.curated / "policy_document_versions.parquet"
        items_path = self.settings.curated / "crawl_items.parquet"
        if not versions_path.exists() or not items_path.exists():
            return pl.DataFrame()
        versions = read_parquet_snapshot(versions_path)
        items = read_parquet_snapshot(items_path)
        if versions.is_empty() or items.is_empty():
            return pl.DataFrame()
        item_cols = [
            col for col in ("item_id", "run_id", "city_id", "candidate_date", "candidate_date_source")
            if col in items.columns
        ]
        if "crawl_item_id" not in versions.columns or "item_id" not in item_cols:
            return versions.head(0)
        return versions.join(
            items.select(item_cols),
            left_on="crawl_item_id",
            right_on="item_id",
            how="inner",
        ).filter(pl.col("run_id") == crawl_run_id)

    def _episode_documents(self, versions: pl.DataFrame) -> pl.DataFrame:
        if versions.is_empty():
            return pl.DataFrame()
        cities = {
            str(row["city_id"]): row
            for row in load_cities_105(self.settings).iter_rows(named=True)
        }
        sources = {str(source.source_id): source for source in load_registry(self.settings)}
        rows: list[dict[str, Any]] = []
        for row in versions.iter_rows(named=True):
            status = int(row.get("http_status") or 0)
            text = _text(row.get("extracted_text"))
            if status != 200 or not text:
                continue
            city = cities.get(_text(row.get("city_id"))) or {}
            source = sources.get(_text(row.get("source_id")))
            official = bool(
                source is not None
                and str(getattr(source, "official_status", ""))
                in {"official", "official_reprint"}
            )
            publication_date = _date(row.get("candidate_date"))
            effective_date, date_confidence, effective_date_basis, date_evidence_text = _parse_effective_evidence(text, publication_date)
            rows.append(
                {
                    "episode_id": EPISODE_ID,
                    "episode_name": EPISODE_NAME,
                    "document_id": _id(EPISODE_ID, row.get("document_version_id"), prefix="DOC930PROD"),
                    "record_id": row.get("record_id"),
                    "city_id": row.get("city_id"),
                    "city": city.get("city_name"),
                    "province": city.get("province_name"),
                    "document_title": row.get("title"),
                    "document_number": None,
                    "issuer": None,
                    "document_type": "OFFICIAL_POLICY" if official else "DISCOVERY_EVIDENCE",
                    "official_url": row.get("final_url") or row.get("canonical_url"),
                    "canonical_url": row.get("canonical_url"),
                    "final_url": row.get("final_url"),
                    "official_source": official,
                    "official_evidence_status": "LIVE_HTTP_200" if official else "LIVE_HTTP_200_NON_OFFICIAL_DISCOVERY",
                    "live_status": "recovered_by_crawl_pipeline" if official else "discovery_only_non_official",
                    "http_status": status,
                    "content_type": row.get("content_type"),
                    "content_hash": row.get("content_sha256"),
                    "raw_path": row.get("local_path"),
                    "official_text": text,
                    "publication_date": publication_date,
                    "announcement_date": publication_date,
                    "effective_date": effective_date,
                    "implementation_date": effective_date,
                    "date_confidence": date_confidence,
                    "effective_date_basis": effective_date_basis,
                    "date_evidence_text": date_evidence_text,
                    "expiry_date": None,
                    "source_confidence": 0.95 if official else 0.25,
                    "retrieved_at": row.get("updated_at") or _now(),
                    "error_type": None,
                    "is_formal_eligible": official,
                    "created_at": _now(),
                }
            )
        return pl.DataFrame(rows, infer_schema_length=None) if rows else pl.DataFrame()

    def _run_postprocess(self, crawl_run_id: str, request: CrawlJobRequest, *, progress=None) -> dict[str, Any]:
        versions = self._versions_for_run(crawl_run_id)
        self._write_checkpoint(
            stage="930_ATTACHMENT_ARCHIVE",
            status="RUNNING",
            documents_found=versions.height,
            reason_code="immutable_archive_hash_gate",
            real_progress=versions.height > 0,
        )
        archive = archive_document_versions(self.settings, run_id=crawl_run_id)
        pdf_pipeline = PDFPipeline(self.settings, config=load_pdf_config(self.settings))
        pdf_discovery = pdf_pipeline.discover(
            run_id=crawl_run_id,
            limit=max(1, request.max_attachment_attempts * 10),
        )
        pdf_archive = pdf_pipeline.archive(
            run_id=crawl_run_id,
            limit=max(1, request.max_attachment_attempts * 10),
        )
        pdf_download = pdf_pipeline.download(
            run_id=crawl_run_id,
            limit=request.max_attachment_attempts,
        )
        pdf_parse = pdf_pipeline.parse(
            run_id=crawl_run_id,
            limit=request.max_attachment_attempts,
        )
        pdf_match = pdf_pipeline.match(
            run_id=crawl_run_id,
            limit=request.max_attachment_attempts,
        )
        attachment_metrics = {
            **self._attachment_metrics(crawl_run_id),
            "pdf_pipeline": {
                "discovery": pdf_discovery,
                "archive": pdf_archive,
                "download": pdf_download,
                "parse": pdf_parse,
                "match": pdf_match,
            },
        }
        official_source_ids = {
            str(source.source_id)
            for source in load_registry(self.settings)
            if str(source.official_status) in {"official", "official_reprint"}
        }
        official_version_ids = (
            versions.filter(pl.col("source_id").cast(pl.String).is_in(sorted(official_source_ids)))
            .get_column("document_version_id")
            .to_list()
            if not versions.is_empty() and official_source_ids
            else []
        )
        promoted = promote_document_versions(
            self.settings,
            run_id=crawl_run_id,
            document_version_ids=official_version_ids,
            apply=True,
        )
        # Re-read versions after promotion so episode rows carry the formal
        # record_id linkage created by the existing importer.
        versions = self._versions_for_run(crawl_run_id)
        all_documents = self._episode_documents(versions)
        documents = (
            all_documents.filter(pl.col("is_formal_eligible"))
            if not all_documents.is_empty() and "is_formal_eligible" in all_documents.columns
            else all_documents
        )
        pipeline = Episode930Pipeline(
            self.settings,
            config=EpisodeConfig(
                max_ai_calls=self.max_ai_calls,
                run_search=False,
                run_ai=self.max_ai_calls > 0,
                apply=True,
            ),
            output=self.run_dir,
        )
        pipeline.scope()
        frozen_scope_entities = analysis_ready_scope_entities(self.output)
        api_fast_lane_plan, api_fast_lane_path = load_api_fast_lane_plan(self.output)
        api_fast_lane_priorities = api_fast_lane_document_priorities(self.output)
        api_recovery_metrics = self._recover_api_failures(
            core_document_ids=frozen_scope_entities.get("core_document_ids") or set(),
            fast_lane_document_priorities=api_fast_lane_priorities,
        )
        city_rows = []
        discovered_ids = set(documents.get_column("city_id").drop_nulls().to_list()) if not documents.is_empty() and "city_id" in documents.columns else set()
        selected_cities = [
            str(value)
            for value in (_read_json(self.snapshot_path).get("selected_cities") or [])
        ]
        unresolved_cities = max(
            0,
            len(set(selected_cities)) - len(discovered_ids.intersection(selected_cities)),
        )
        for row in load_cities_105(self.settings).iter_rows(named=True):
            city_rows.append({
                "city_id": row.get("city_id"),
                "city": row.get("city_name"),
                "province": row.get("province_name"),
                "mentioned_as_930_city": str(row.get("city_id")) in {str(value) for value in discovered_ids},
            })
        scope_path = pipeline.phase_dirs["01_DISCOVERY"] / "2016_930_CITY_DISCOVERY.parquet"
        atomic_write_parquet(
            pl.DataFrame(city_rows),
            scope_path,
            {"module": "episode_930_production", "stage": "930_DISCOVERY"},
            key_columns=("city_id",),
        )
        self._write_checkpoint(
            stage="930_GAP_AUDIT_1",
            status="RUNNING",
            documents_found=documents.height,
            official_documents=documents.height,
            cities_discovered=len(discovered_ids),
            cities_unresolved=unresolved_cities,
            reason_code="deterministic_post_fetch_audit",
            real_progress=documents.height > 0,
        )
        matrix1, gap1, gap1_metrics = pipeline.gap_audit(documents, pass_number=1)
        self._write_checkpoint(
            stage="930_TARGETED_RECOVERY",
            status="DEFERRED_TO_NEXT_BOUNDED_CRAWL",
            gaps_remaining=gap1.height,
            reason_code="formal_crawler_first_bounded_pass_complete",
        )
        self._write_checkpoint(
            stage="930_GAP_AUDIT_2",
            status="RUNNING",
            gaps_remaining=gap1.height,
            reason_code="deterministic_recheck_without_manual_search",
        )
        matrix2, gap2, gap2_metrics = pipeline.gap_audit(documents, pass_number=2)
        if documents.is_empty():
            core_rolling = self._write_core_rolling_metrics()
            postprocess = {
                "archive": archive,
                "promotion": promoted,
                "extraction": {"actions_extracted": 0, "parameterized_actions": 0},
                "ai": {
                    "ai_status": "deferred_no_documents",
                    "ai_first_pass": 0,
                    "ai_second_pass": 0,
                    "ai_conflicts": 0,
                    "ai_calls": 0,
                    "tokens": None,
                    "cost": None,
                    "usage_status": "unavailable",
                },
                "dates": {"date_rows": 0, "high_confidence_dates": 0},
                "dedup": {"documents": 0, "actions": 0},
                "formal_import": {"formal_import": "NO_NEW_DOCUMENTS", "rows": {}},
                "final_export": {"export_rows": 0},
                "golden_path": {
                    "status": "FAIL",
                    "episode_id": EPISODE_ID,
                    "run_id": self.run_id,
                    "crawl_run_id": crawl_run_id,
                    "document_version_ids": [],
                    "document_ids": [],
                    "action_ids": [],
                    "api_pass1": 0,
                    "api_pass2": 0,
                    "date_rows": 0,
                    "attachment_archive": archive,
                    "formal_promotion": promoted,
                    "dashboard_filter": "FAIL",
                    "action_export": "FAIL",
                    "created_at": _now(),
                },
                "documents": 0,
                "actions": 0,
                "parameters": 0,
                "manual_review": 0,
                "api_failures": {"failure_rows": 0, "retryable_failures": 0, "terminal_failures": 0},
                "core_rolling": core_rolling,
            }
            _atomic_json(self.output / "930_GOLDEN_PATH_EVIDENCE.json", postprocess["golden_path"])
            _atomic_json(self.run_dir / "POSTPROCESS_SUMMARY.json", postprocess)
            return postprocess
        self._write_checkpoint(
            stage="930_ACTION_EXTRACTION",
            status="RUNNING",
            documents_found=documents.height,
            official_documents=documents.height,
            cities_discovered=len(discovered_ids),
            cities_unresolved=unresolved_cities,
            reason_code="deterministic_clause_extraction",
            real_progress=documents.height > 0,
        )
        actions, params, extraction = pipeline.extract_actions(documents)
        audit_documents = documents
        audit_actions = actions
        prior_documents_path = self.settings.curated / "policy_episode_documents.parquet"
        prior_actions_path = self.settings.curated / "policy_episode_actions.parquet"
        if prior_documents_path.exists():
            prior_documents = read_parquet_snapshot(prior_documents_path)
            if not prior_documents.is_empty():
                audit_documents = pl.concat([prior_documents, documents], how="diagonal_relaxed").unique(subset=["document_id"], keep="last")
        if prior_actions_path.exists():
            prior_actions = read_parquet_snapshot(prior_actions_path)
            if not prior_actions.is_empty():
                audit_actions = pl.concat([prior_actions, actions], how="diagonal_relaxed").unique(subset=["action_id"], keep="last")
        action_count_audit = pipeline.action_count_audit(audit_documents, audit_actions)
        self._write_checkpoint(
            stage="930_API_CLASSIFY_PASS1",
            status="RUNNING",
            actions_extracted=actions.height,
            parameters_extracted=params.height,
            reason_code="bounded_structured_api_pass",
            real_progress=actions.height > 0,
        )
        recovery_queue_path = self.output / "930_API_RECOVERY_QUEUE.parquet"
        recovery_queue = (
            read_parquet_snapshot(recovery_queue_path)
            if recovery_queue_path.exists()
            else pl.DataFrame()
        )
        main_api_allowed = api_classification_allowed(
            api_recovery_metrics,
            recovery_queue_rows=recovery_queue.height,
        )
        heartbeat_stop = threading.Event()

        def api_heartbeat() -> None:
            while not heartbeat_stop.wait(30):
                self._snapshot(
                    stage="930_API_CLASSIFY_PASS1",
                    status="RUNNING",
                    current_item="api_classification",
                )

        classification_path = (
            pipeline.phase_dirs["05_API_CLASSIFICATION"]
            / "2016_930_API_CLASSIFICATION.parquet"
        )
        if main_api_allowed:
            api_documents, api_actions, api_fast_lane_metrics = select_api_fast_lane_inputs(
                documents,
                actions,
                api_fast_lane_plan,
            )
            heartbeat_thread = threading.Thread(
                target=api_heartbeat,
                name="episode-930-api-heartbeat",
                daemon=True,
            )
            heartbeat_thread.start()
            try:
                ai_rows, ai_metrics = pipeline.classify_actions(api_documents, api_actions)
            finally:
                heartbeat_stop.set()
                heartbeat_thread.join(timeout=2)
        else:
            # Recovery owns all paid calls until SINGLE -> 5 -> 20 reaches
            # stable backlog consumption.  Deterministic downstream work is
            # intentionally allowed to continue with cached classifications.
            ai_rows = (
                read_parquet_snapshot(classification_path)
                if classification_path.exists()
                else pl.DataFrame()
            )
            ai_metrics = {
                "ai_status": "deferred_recovery_gate",
                "ai_first_pass": 0,
                "ai_second_pass": 0,
                "ai_conflicts": 0,
                "ai_calls": 0,
                "tokens": None,
                "cost": None,
                "usage_status": "unavailable",
                "recovery_gate": api_recovery_metrics.get("recovery_gate"),
                "reason_code": "MAIN_CLASSIFICATION_BLOCKED_BY_RECOVERY_GATE",
            }
            api_fast_lane_metrics = {
                "enabled": False,
                "reason_code": "MAIN_CLASSIFICATION_BLOCKED_BY_RECOVERY_GATE",
                "plan_actions": api_fast_lane_plan.height,
                "plan_documents": len(api_fast_lane_priorities),
                "selected_actions": 0,
                "selected_documents": 0,
            }
        api_audit = self._api_audit_metrics()
        api_failures = self._write_api_failure_artifact(documents)
        ai_metrics = {
            **ai_metrics,
            "ai_status": (
                "operational"
                if api_audit["api_success"] > 0 and api_audit["api_failed"] == 0
                else "partial"
                if api_audit["api_success"] > 0
                else "deferred"
            ),
            "api_attempts": api_audit["api_attempts"],
            "api_success": api_audit["api_success"],
            "api_failed": api_audit["api_failed"],
            "api_deferred": api_audit["api_deferred"],
            "api_in_flight": api_audit["api_in_flight"],
            "api_pass1_success": api_audit["api_pass1_success"],
            "api_pass2_success": api_audit["api_pass2_success"],
            "api_pass1_failed": api_audit["api_pass1_failed"],
            "api_pass2_failed": api_audit["api_pass2_failed"],
            "api_cache_hits": api_audit["api_cache_hits"],
            "tokens": api_audit["tokens"],
            "cost": api_audit["cost"],
            "usage_status": api_audit["usage_status"],
            "classified_rows": ai_rows.height,
            "api_failure_rows": api_failures["failure_rows"],
            "api_retryable_failures": api_failures["retryable_failures"],
            "main_classification_allowed": main_api_allowed,
            "recovery_gate": api_recovery_metrics.get("recovery_gate"),
            "api_fast_lane": {
                **api_fast_lane_metrics,
                "plan_path": str(api_fast_lane_path) if api_fast_lane_path else None,
            },
            "pass2_not_yet_eligible": max(
                0,
                documents.height - api_audit["api_pass1_success"],
            ),
            "pass2_eligible": api_audit["api_pass1_success"],
            "pass2_waiting": max(
                0,
                api_audit["api_pass1_success"] - api_audit["api_pass2_success"],
            ),
        }
        self._snapshot(
            stage="930_DATE_VERIFICATION",
            status="RUNNING",
            actions_classified=int(ai_rows.height),
            api_success=api_audit["api_success"],
            api_failed=api_audit["api_failed"],
            api_deferred=api_audit["api_deferred"],
            api_status=ai_metrics["ai_status"],
            tokens=api_audit["tokens"],
            cost=api_audit["cost"],
            usage_status=api_audit["usage_status"],
            gaps_remaining=gap2.height,
            cities_discovered=len(discovered_ids),
            cities_unresolved=unresolved_cities,
        )
        dates, date_metrics = pipeline.date_audit(documents, actions)
        dedup_docs, dedup_actions, dedup_metrics = pipeline.deduplicate(documents, actions)
        gap_register, gap_register_metrics = pipeline.build_gap_register(
            dedup_docs,
            dedup_actions,
            params,
            gap2,
            attachment_metrics=attachment_metrics,
            ai_rows=ai_rows,
        )
        timeline, manual = pipeline.timeline_and_manual_queue(dedup_docs, dedup_actions, gap_register, ai_rows)
        self._write_checkpoint(
            stage="930_DEDUP",
            status="COMPLETED",
            reason_code="deterministic_episode_dedup",
        )
        self._write_checkpoint(
            stage="930_FORMAL_PROMOTION",
            status="RUNNING",
            reason_code="formal_episode_import_under_single_writer",
        )
        pre_trace = build_promotion_gate_trace(
            dedup_actions,
            dedup_docs,
            verified_action_ids=(
                dedup_actions.get_column("action_id").to_list()
                if not dedup_actions.is_empty()
                else ()
            ),
            dedup_action_ids=(
                dedup_actions.get_column("action_id").to_list()
                if not dedup_actions.is_empty()
                else ()
            ),
        )
        trace_path = self.run_dir / "09_PROMOTION" / "CRPD_PROMOTION_GATE_TRACE_PRE_IMPORT.csv"
        trace_path.parent.mkdir(parents=True, exist_ok=True)
        pre_trace.write_csv(trace_path)
        promotable_ids = (
            set(pre_trace.filter(pl.col("eligible_for_import")).get_column("action_id").to_list())
            if not pre_trace.is_empty()
            else set()
        )
        promotable_actions = (
            dedup_actions.filter(pl.col("action_id").is_in(sorted(promotable_ids)))
            if promotable_ids
            else dedup_actions.head(0)
        )
        promotable_params = (
            params.filter(pl.col("action_id").is_in(sorted(promotable_ids)))
            if promotable_ids and not params.is_empty() and "action_id" in params.columns
            else params.head(0)
        )
        import_metrics = pipeline.formal_import(
            dedup_docs,
            promotable_actions,
            promotable_params,
            gap_register,
            matrix2,
        )
        persisted_actions_path = self.settings.curated / "policy_episode_actions.parquet"
        persisted_actions = (
            read_parquet_snapshot(persisted_actions_path)
            if persisted_actions_path.exists()
            else pl.DataFrame()
        )
        persisted_ids = (
            set(
                persisted_actions.get_column("action_id")
                .drop_nulls()
                .cast(pl.String)
                .to_list()
            )
            if not persisted_actions.is_empty() and "action_id" in persisted_actions.columns
            else set()
        )
        final_trace = build_promotion_gate_trace(
            dedup_actions,
            dedup_docs,
            verified_action_ids=(
                dedup_actions.get_column("action_id").to_list()
                if not dedup_actions.is_empty()
                else ()
            ),
            dedup_action_ids=(
                dedup_actions.get_column("action_id").to_list()
                if not dedup_actions.is_empty()
                else ()
            ),
            database_action_ids=persisted_ids,
        )
        final_trace_path = self.run_dir / "09_PROMOTION" / "CRPD_PROMOTION_GATE_TRACE.csv"
        final_trace.write_csv(final_trace_path)
        import_metrics["promotion_gate_trace"] = str(final_trace_path)
        import_metrics["promotion_candidates"] = (
            int(final_trace.filter(pl.col("eligible_for_import")).height)
            if not final_trace.is_empty()
            else 0
        )
        import_metrics["new_action_rows"] = int(import_metrics.get("new_action_rows", 0))
        core_rolling = self._write_core_rolling_metrics()
        self._write_checkpoint(
            stage="930_ANALYSIS_READY_ROLLING",
            status="RUNNING",
            core_discovery_verified=core_rolling["core_discovery_verified"],
            core_action_eligible=core_rolling["core_action_eligible"],
            core_action_completed=core_rolling["core_action_completed"],
            core_official_documents=core_rolling["core_official_documents"],
            core_date_resolved=core_rolling["core_date_resolved"],
            core_parameters_processed=core_rolling["core_parameters_processed"],
            core_blocking_gaps=core_rolling["analysis_ready_core_blocking_gaps"].get("blocking_gap_count", 0),
            global_final_blocking_gaps=core_rolling["global_final_blocking_gaps"].get("blocking_gap_count", 0),
            reason_code="incremental_core_postprocess_after_micro_batch",
            real_progress=True,
        )
        analysis_ready_gate = self._maybe_export_analysis_ready()
        analysis_ready = bool(analysis_ready_gate.get("analysis_ready"))
        final = pipeline.final_export(dedup_docs, dedup_actions, params, dates, manual["manual_frame"], gap_register, {
            **date_metrics,
            **ai_metrics,
            "formal_import": import_metrics.get("formal_import"),
            "dashboard_episode_filter": "available",
            "gap_register": gap_register_metrics,
            "analysis_ready": analysis_ready,
            "source_run_ids": [self.run_id],
            "analysis_ready_gate": analysis_ready_gate,
        }, api_rows=ai_rows)
        dashboard_actions = int(import_metrics.get("new_action_rows", 0))
        golden_requirements = {
            "real_documents": dedup_docs.height > 0,
            "real_actions": dashboard_actions > 0,
            "api_pass1": api_audit["api_pass1_success"] > 0,
            "api_pass2": api_audit["api_pass2_success"] > 0,
            "high_confidence_effective_date": date_metrics.get("high_confidence_dates", 0) > 0,
            "attachment_discovery": attachment_metrics["attachments_found"] > 0,
            "attachment_archive": attachment_metrics["attachments_archived"] > 0,
            "formal_promotion": dashboard_actions > 0,
            "dashboard_filter": dashboard_actions > 0,
            "action_export": Path(final.get("final_export", "")).exists(),
        }
        golden = {
            "status": "PASS" if all(golden_requirements.values()) else "FAIL",
            "requirements": golden_requirements,
            "episode_id": EPISODE_ID,
            "run_id": self.run_id,
            "crawl_run_id": crawl_run_id,
            "document_version_ids": [str(value) for value in versions.get_column("document_version_id").to_list()] if "document_version_id" in versions.columns else [],
            "document_ids": [str(value) for value in dedup_docs.get_column("document_id").to_list()] if "document_id" in dedup_docs.columns else [],
            "action_ids": [str(value) for value in dedup_actions.get_column("action_id").to_list()] if "action_id" in dedup_actions.columns else [],
            "api_pass1": int(api_audit["api_pass1_success"]),
            "api_pass2": int(api_audit["api_pass2_success"]),
            "date_rows": int(date_metrics.get("date_rows", 0)),
            "high_confidence_effective_dates": int(date_metrics.get("high_confidence_dates", 0)),
            "attachment_archive": attachment_metrics,
            "formal_promotion": {**promoted, "new_action_rows": dashboard_actions},
            "dashboard_filter": "PASS" if dashboard_actions > 0 else "FAIL",
            "action_export": "PASS" if Path(final.get("final_export", "")).exists() else "FAIL",
            "created_at": _now(),
        }
        _atomic_json(self.output / "930_GOLDEN_PATH_EVIDENCE.json", golden)
        _atomic_json(self.run_dir / "POSTPROCESS_SUMMARY.json", {
            "archive": archive,
            "attachments": attachment_metrics,
            "promotion": promoted,
            "extraction": extraction,
            "action_count_audit": action_count_audit,
            "gap_register": gap_register_metrics,
            "api_failures": api_failures,
            "api_recovery": api_recovery_metrics,
            "core_rolling": core_rolling,
            "ai": ai_metrics,
            "api_audit": api_audit,
            "dates": date_metrics,
            "gap_audit_1": gap1_metrics,
            "gap_audit_2": {**gap2_metrics, "gap_rows_before_typed_register": gap2.height},
            "dedup": dedup_metrics,
            "formal_import": import_metrics,
            "analysis_ready_gate": analysis_ready_gate,
            "final_export": final,
            "golden_path": golden,
        })
        return {
            "archive": archive,
            "attachments": attachment_metrics,
            "promotion": promoted,
            "extraction": extraction,
            "ai": ai_metrics,
            "dates": date_metrics,
            "gap_audit_1": gap1_metrics,
            "gap_audit_2": gap2_metrics,
            "dedup": dedup_metrics,
            "formal_import": import_metrics,
            "analysis_ready_gate": analysis_ready_gate,
            "final_export": final,
            "golden_path": golden,
            "documents": documents.height,
            "actions": dedup_actions.height,
            "formal_actions_promoted": dashboard_actions,
            "parameters": params.height,
            "manual_review": int(manual.get("manual_review_pending", 0)),
            "api_audit": api_audit,
            "api_failures": api_failures,
            "api_recovery": api_recovery_metrics,
            "core_rolling": core_rolling,
            "action_count_audit": action_count_audit,
            "gap_register": gap_register_metrics,
            "cities_discovered": len(discovered_ids),
            "cities_unresolved": unresolved_cities,
        }

    def _run_cached_convergence(
        self,
        job_id: str,
        request: CrawlJobRequest,
        *,
        plan: Mapping[str, Any],
        work_source: Mapping[str, Any],
        claim: Mapping[str, Any],
        progress: Callable[[str, int, int, str, dict[str, Any]], None] | None,
    ) -> dict[str, Any]:
        """Advance cached recovery when no new raw queue item was claimed.

        A completed raw queue still leaves cached API recovery work to do.  In
        that state the formal worker must use the existing recovery controller
        under the single writer lock, without constructing a new crawler or
        rewriting the immutable raw queue.
        """

        queue = read_parquet_snapshot(self.queue_path) if self.queue_path.exists() else pl.DataFrame()
        queue_total = queue.height
        queue_completed = int(queue.filter(pl.col("status") == "CRAWL_COMPLETED").height) if not queue.is_empty() and "status" in queue.columns else 0
        queue_pending = int(queue.filter(pl.col("status") == "PENDING").height) if not queue.is_empty() and "status" in queue.columns else 0
        queue_running = int(queue.filter(pl.col("status") == "RUNNING").height) if not queue.is_empty() and "status" in queue.columns else 0
        queue_failed = int(queue.filter(pl.col("status") == "FAILED").height) if not queue.is_empty() and "status" in queue.columns else 0
        self._write_checkpoint(
            stage="930_API_CLASSIFY_PASS1",
            status="RUNNING",
            episode_status="PARTIAL",
            last_micro_batch_status="CACHED_CONVERGENCE_RUNNING",
            reason_code="cached_convergence_without_new_queue_claim",
            crawl_run_id=None,
            work_source=work_source.get("work_source"),
            work_source_priority=work_source.get("normalized_priority"),
            work_source_reason=work_source.get("reason_code"),
            queue_total=queue_total,
            queue_completed=queue_completed,
            queue_pending=queue_pending,
            queue_running=queue_running,
            queue_failed=queue_failed,
            real_progress=False,
        )
        if progress:
            progress(
                "enriching",
                1,
                1,
                "930 cached convergence delegated to the recovery controller",
                {
                    "episode_id": EPISODE_ID,
                    "queue_completed": queue_completed,
                    "queue_total": queue_total,
                    "work_source": work_source.get("work_source"),
                },
            )
        scope_entities = analysis_ready_scope_entities(self.output)
        api_recovery: dict[str, Any]
        with PolicyWriteLock(self.settings, job_id):
            api_recovery = self._recover_api_failures(
                core_document_ids=scope_entities.get("core_document_ids") or set(),
                fast_lane_document_priorities=api_fast_lane_document_priorities(self.output),
            )
        final = self._write_checkpoint(
            stage="930_FINAL_AUDIT",
            status="COMPLETED_WITH_WARNINGS",
            episode_status="PARTIAL",
            last_micro_batch_status="COMPLETED_WITH_WARNINGS",
            reason_code="cached_convergence_without_new_queue_claim",
            crawl_run_id=None,
            work_source=work_source.get("work_source"),
            work_source_priority=work_source.get("normalized_priority"),
            work_source_reason=work_source.get("reason_code"),
            next_batch_status="NOT_REQUIRED",
            next_batch_autostart=False,
            queue_total=queue_total,
            queue_completed=queue_completed,
            queue_pending=queue_pending,
            queue_running=queue_running,
            queue_failed=queue_failed,
            api_recovery=api_recovery,
            real_progress=bool(api_recovery.get("recovery_success", 0)),
        )
        handoff = {
            "episode_id": EPISODE_ID,
            "run_id": self.run_id,
            "job_id": job_id,
            "work_source": work_source.get("work_source"),
            "work_source_priority": work_source.get("normalized_priority"),
            "work_source_reason": work_source.get("reason_code"),
            "crawl_run_id": None,
            "status": "COMPLETED_WITH_WARNINGS",
            "episode_status": "PARTIAL",
            "last_micro_batch_status": "COMPLETED_WITH_WARNINGS",
            "next_batch_autostart": False,
            "checkpoint": str(self.checkpoint_path),
            "progress_snapshot": str(self.snapshot_path),
            "queue": str(self.queue_path),
            "search_plan": str(self.search_plan_path),
            "postprocess": {"api_recovery": api_recovery, "cached_convergence": True},
            "updated_at": _now(),
        }
        _atomic_json(self.handoff_path, handoff)
        return {
            "plan": plan,
            "cached_convergence": True,
            "status": "COMPLETED_WITH_WARNINGS",
            "episode_status": "PARTIAL",
            "run_id": self.run_id,
            "crawler": {"status": "NOT_RUN", "reason_code": "NO_NEW_QUEUE_CLAIM"},
            "postprocess": {"api_recovery": api_recovery, "cached_convergence": True},
            "checkpoint": final,
        }

    def run_job(
        self,
        job_id: str,
        request: CrawlJobRequest,
        *,
        progress: Callable[[str, int, int, str, dict], None] | None = None,
        cancel_check: Callable[[], bool] | None = None,
    ) -> dict[str, Any]:
        plan = self.build_plan()
        work_source = self._next_work_source(request.cities)
        cities = [str(value) for value in work_source.get("cities") or []]
        if not cities and not bool(work_source.get("blocked_by_active_lease")):
            cities = self._selected_cities(request.cities)
        claim = self._claim_queue(
            cities,
            job_id=job_id,
            work_source=str(work_source.get("work_source") or WORK_SOURCE_ORDINARY_RAW_PENDING),
        )
        claim["work_source"] = claim.get("work_source") or work_source.get("work_source")
        claim["work_source_priority"] = work_source.get("normalized_priority")
        claim["work_source_reason"] = work_source.get("reason_code")
        self._write_checkpoint(
            stage="930_DISCOVERY",
            status="RUNNING",
            current_city=cities[0] if cities else None,
            selected_cities=cities,
            lease=claim,
            work_source=work_source.get("work_source"),
            work_source_priority=work_source.get("normalized_priority"),
            work_source_reason=work_source.get("reason_code"),
            global_core_priority=work_source.get("normalized_priority"),
            analysis_ready_core_recovery_required=(work_source.get("required_by_priority") or {}).get("0", 0),
            reason_code="episode_queue_claimed",
        )
        if progress:
            progress(
                "discovering",
                1,
                5,
                "930 discovery delegated to CrawlService",
                {
                    "episode_id": EPISODE_ID,
                    "selected_cities": len(cities),
                    "work_source": work_source.get("work_source"),
                    "work_source_priority": work_source.get("normalized_priority"),
                },
            )
        if cancel_check and cancel_check():
            raise InterruptedError("930 task cancellation requested")
        queue = read_parquet_snapshot(self.queue_path) if self.queue_path.exists() else pl.DataFrame()
        unfinished_statuses = {"PENDING", "RETRY_WAIT", "RUNNING", "LEASED", "INFLIGHT"}
        has_unfinished_raw = (
            not queue.is_empty()
            and "status" in queue.columns
            and bool(queue.get_column("status").cast(pl.String).is_in(sorted(unfinished_statuses)).any())
        )
        if (
            int(claim.get("claimed", 0) or 0) == 0
            and not bool(claim.get("recovery_priority_blocked"))
            and not bool(work_source.get("blocked_by_active_lease"))
            and not has_unfinished_raw
        ):
            return self._run_cached_convergence(
                job_id,
                request,
                plan=plan,
                work_source=work_source,
                claim=claim,
                progress=progress,
            )
        inner = self._inner_request(
            request,
            cities,
            queue_item_ids=claim.get("queue_item_ids") or [],
        )
        def crawl_progress(
            stage: str,
            current: int,
            total: int,
            message: str,
            counters: dict[str, Any],
        ) -> None:
            details = dict(counters or {})
            current_source = _text(details.get("_source_id")) or None
            current_item = _text(details.get("_current_url")) or None
            processed = int(details.get("processed") or 0)
            self._snapshot(
                stage="930_DISCOVERY" if stage in {"discovering", "fetching"} else "930_OFFICIAL_RECOVERY",
                status="RUNNING",
                current_city=cities[0] if cities else None,
                current_source=current_source,
                current_item=current_item,
                queue_running=claim.get("claimed", 0),
                last_crawler_stage=stage,
                last_crawler_message=message,
                crawler_progress_current=current,
                crawler_progress_total=total,
                real_progress=stage == "fetching" and processed > 0,
            )
            if progress:
                progress(stage, current, total, message, counters)

        service = CrawlService(
            self.settings,
            workspace=JobManager(self.settings).workspace_dir(job_id),
        )
        result = service.execute(
            inner,
            progress=crawl_progress,
            cancel_check=cancel_check,
        )
        crawl_run_id = str(result.get("run_id") or "")
        self._update_queue(
            cities=cities,
            queue_item_ids=claim.get("queue_item_ids"),
            status="CRAWL_COMPLETED" if crawl_run_id else "FAILED",
            metrics=result.get("metrics", {}),
            error=None if crawl_run_id else "crawler did not return run_id",
        )
        self._write_checkpoint(stage="930_OFFICIAL_RECOVERY", status="RUNNING", crawl_run_id=crawl_run_id, crawler_metrics=result.get("metrics", {}), real_progress=int(result.get("metrics", {}).get("fetched", 0) or 0) > 0)
        if not crawl_run_id:
            self._write_checkpoint(stage="930_FINAL_AUDIT", status="BLOCKED", blocker="crawler_missing_run_id")
            return {"plan": plan, "crawler": result, "status": "BLOCKED", "run_id": self.run_id}
        if cancel_check and cancel_check():
            raise InterruptedError("930 task cancellation requested")
        with PolicyWriteLock(self.settings, job_id):
            merge = commit_crawl_workspace(self.settings, JobManager(self.settings).workspace_dir(job_id), job_id)
            self._write_checkpoint(stage="930_FORMAL_PROMOTION", status="RUNNING", merge_manifest=merge)
            postprocess = self._run_postprocess(crawl_run_id, request, progress=progress)
            if request.rebuild_database:
                try:
                    from policydb.query.database import build_database_atomic

                    build_database_atomic(self.settings, job_id)
                except Exception as exc:
                    postprocess["database_error"] = type(exc).__name__
        queue = read_parquet_snapshot(self.queue_path)
        completed = int(queue.filter(pl.col("status") == "CRAWL_COMPLETED").height) if not queue.is_empty() else 0
        pending = int(queue.filter(pl.col("status") == "PENDING").height) if not queue.is_empty() else 0
        last_batch_status = "COMPLETED" if postprocess.get("golden_path", {}).get("status") == "PASS" else "COMPLETED_WITH_WARNINGS"
        episode_status = (
            "RUNNING"
            if pending > 0 or int(queue.filter(pl.col("status") == "RUNNING").height) > 0
            else "COMPLETE_WITH_REVIEW_PENDING"
            if last_batch_status == "COMPLETED"
            else "PARTIAL"
        )
        final = self._write_checkpoint(
            stage="930_FINAL_COVERAGE_AUDIT",
            status=last_batch_status,
            episode_status=episode_status,
            last_micro_batch_status=last_batch_status,
            work_source=work_source.get("work_source"),
            work_source_priority=work_source.get("normalized_priority"),
            work_source_reason=work_source.get("reason_code"),
            next_batch_status="PENDING" if pending > 0 else "NOT_REQUIRED",
            queue_total=queue.height,
            queue_completed=completed,
            queue_pending=pending,
            queue_running=0,
            queue_failed=int(queue.filter(pl.col("status") == "FAILED").height) if not queue.is_empty() else 0,
            documents_found=postprocess.get("documents", 0),
            official_documents=postprocess.get("documents", 0),
            actions_extracted=postprocess.get("actions", 0),
            parameters_extracted=postprocess.get("parameters", 0),
            formal_documents_promoted=postprocess.get("promotion", {}).get("new_records", 0),
            formal_actions_promoted=postprocess.get("formal_actions_promoted", 0),
            dates_verified=postprocess.get("dates", {}).get("high_confidence_dates", 0),
            cities_discovered=postprocess.get("cities_discovered", 0),
            cities_unresolved=postprocess.get("cities_unresolved", 0),
            pdfs_found=postprocess.get("attachments", {}).get("pdfs_found", 0),
            pdfs_archived=postprocess.get("attachments", {}).get("pdfs_archived", 0),
            api_success=postprocess.get("api_audit", {}).get("api_success", 0),
            api_failed=postprocess.get("api_audit", {}).get("api_failed", 0),
            api_deferred=postprocess.get("api_audit", {}).get("api_deferred", 0),
            api_attempts=postprocess.get("api_audit", {}).get("api_attempts", 0),
            api_in_flight=postprocess.get("api_audit", {}).get("api_in_flight", 0),
            api_pass1_success=postprocess.get("api_audit", {}).get("api_pass1_success", 0),
            api_pass2_success=postprocess.get("api_audit", {}).get("api_pass2_success", 0),
            api_pass1_failed=postprocess.get("api_audit", {}).get("api_pass1_failed", 0),
            api_pass2_failed=postprocess.get("api_audit", {}).get("api_pass2_failed", 0),
            api_cache_hits=postprocess.get("api_audit", {}).get("api_cache_hits", 0),
            api_failure_rows=postprocess.get("api_failures", {}).get("failure_rows", 0),
            api_retryable_failures=postprocess.get("api_failures", {}).get("retryable_failures", 0),
            api_recovery=postprocess.get("api_recovery", {}),
            classified_rows=postprocess.get("ai", {}).get("classified_rows", 0),
            tokens=postprocess.get("api_audit", {}).get("tokens"),
            cost=postprocess.get("api_audit", {}).get("cost"),
            usage_status=postprocess.get("api_audit", {}).get("usage_status", "unavailable"),
            gap_type_counts=postprocess.get("gap_register", {}).get("gap_types", []),
            gaps_remaining=postprocess.get("gap_register", {}).get("gap_rows", postprocess.get("gap_audit_2", {}).get("gap_rows", 0)),
            attachment_status=postprocess.get("attachments", {}),
            effective_date_metrics=postprocess.get("dates", {}),
            action_count_audit=postprocess.get("action_count_audit", {}),
            real_progress=bool(postprocess.get("documents") or postprocess.get("actions")),
        )
        handoff = {
            "episode_id": EPISODE_ID,
            "run_id": self.run_id,
            "job_id": job_id,
            "work_source": work_source.get("work_source"),
            "work_source_priority": work_source.get("normalized_priority"),
            "work_source_reason": work_source.get("reason_code"),
            "crawl_run_id": crawl_run_id,
            "status": last_batch_status,
            "episode_status": episode_status,
            "last_micro_batch_status": last_batch_status,
            "next_batch_autostart": pending > 0,
            "checkpoint": str(self.checkpoint_path),
            "progress_snapshot": str(self.snapshot_path),
            "queue": str(self.queue_path),
            "search_plan": str(self.search_plan_path),
            "postprocess": postprocess,
            "updated_at": _now(),
        }
        _atomic_json(self.handoff_path, handoff)
        return {"plan": plan, "crawler": result, "merge_manifest": merge, "postprocess": postprocess, "status": last_batch_status, "episode_status": episode_status, "run_id": self.run_id, "checkpoint": final}


def run_episode_930_job(
    job_id: str,
    request: CrawlJobRequest,
    settings: Settings,
    *,
    progress: Callable[[str, int, int, str, dict], None] | None = None,
    cancel_check: Callable[[], bool] | None = None,
) -> dict[str, Any]:
    if request.runtime_mode != "UNSPECIFIED":
        build_runtime_context(
            settings,
            run_mode=request.runtime_mode,
            run_id=job_id,
            production_write_allowed=request.production_write_allowed,
        )
    controller = Episode930ProductionController(
        settings,
        city_limit=request.episode_city_limit,
        max_ai_calls=request.episode_max_ai_calls,
    )
    return controller.run_job(job_id, request, progress=progress, cancel_check=cancel_check)


__all__ = [
    "EPISODE_ID",
    "EPISODE_NAME",
    "Episode930ProductionController",
    "PRODUCTION_VERSION",
    "WORK_SOURCE_CORE_RECOVERY",
    "WORK_SOURCE_CRITICAL_GAP_RECOVERY",
    "WORK_SOURCE_FINAL_RECOVERY",
    "WORK_SOURCE_ORDINARY_RAW_PENDING",
    "run_episode_930_job",
    "select_next_work_source",
]
