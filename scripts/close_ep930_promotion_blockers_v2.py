"""Create the EP930 blocker-closure V2 release from existing isolated evidence.

This command is intentionally offline.  It consumes the completed bounded
rehearsal artifacts, derives only conservative direction/geography fields, and
uses ``Episode930Pipeline.formal_import`` against a newly-created rehearsal
root.  It never starts a crawler, searches the network, calls an API, or writes
the production database/raw/PDF roots.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from policydb.episode_930 import EPISODE_ID, Episode930Pipeline, EpisodeConfig  # noqa: E402
from policydb.episode_930_gate_closure import close_action_gates  # noqa: E402
from policydb.parquet_store import read_parquet_snapshot  # noqa: E402
from policydb.promotion_audit import build_promotion_gate_trace  # noqa: E402
from policydb.runtime_context import build_runtime_context  # noqa: E402
from policydb.settings import Settings  # noqa: E402

DATA_ROOT = Path(r"E:\Data Set\CRPD")
OLD_RUN_ROOT = DATA_ROOT / "promotion_rehearsal" / "CRPD_PROMOTION_20260820T165634Z_BLOCKER_CLOSURE"
OLD_RUN_ID = "EP930RUN_A7871E389A3F2046900A"
OLD_CRAWL_RUN_ID = "CRAWLRUN_E3E6ED8C962011910A0E"
ISOLATION_RUN_ROOT = DATA_ROOT / "promotion_rehearsal" / "CRPD_PROMOTION_20260821T075856Z_ISOLATION_CONFIRMATION"
PRODUCTION_ROOT = DATA_ROOT
PRODUCTION_DB = DATA_ROOT / "database" / "policydb.duckdb"
PRODUCTION_PDF = DATA_ROOT / "raw" / "pdf" / "objects" / "01" / (
    "011dfd02191e5909ade25c97fc46f4211dc7c30095027f3a6afdffe55a98ac67.pdf"
)
TARGET_SHA256 = "011dfd02191e5909ade25c97fc46f4211DC7C30095027F3A6AFDFFE55A98AC67".lower()
SCOPE_PATH = DATA_ROOT / "outputs" / "special_projects" / "2016_930" / "930_ANALYSIS_READY_SCOPE.json"


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def _atomic_csv(frame: pl.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    frame.write_csv(temporary)
    os.replace(temporary, path)


def _read_parquet(path: Path) -> pl.DataFrame:
    if not path.exists():
        return pl.DataFrame()
    return read_parquet_snapshot(path)


def _text(value: object) -> str:
    return str(value or "").strip()


def _count_values(frame: pl.DataFrame, column: str, value: str) -> int:
    if frame.is_empty() or column not in frame.columns:
        return 0
    return int(frame.select(pl.col(column).cast(pl.String).fill_null("").eq(value).sum()).item())


def _new_run_root() -> Path:
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    root = DATA_ROOT / "promotion_rehearsal" / f"CRPD_PROMOTION_{stamp}_CLOSURE_V2"
    if root.exists():
        raise FileExistsError(f"refusing to overwrite an existing V2 run: {root}")
    root.mkdir(parents=True)
    return root


def _copy_formal_import_inputs(run_root: Path) -> Path:
    target = run_root / "data" / "curated"
    target.mkdir(parents=True, exist_ok=True)
    names = (
        "policy_episode_documents.parquet",
        "policy_episode_actions.parquet",
        "policy_episode_parameters.parquet",
        "policy_episode_gaps.parquet",
        "policy_episode_city_policy_matrix.parquet",
        "policy_episode_index.parquet",
    )
    for name in names:
        source = OLD_RUN_ROOT / "data" / "curated" / name
        if source.exists():
            shutil.copy2(source, target / name)
    return target


def _read_selected_run() -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    output = OLD_RUN_ROOT / "data" / "outputs" / "special_projects" / "2016_930" / "production_runs" / OLD_RUN_ID
    documents = _read_parquet(output / "07_DEDUP" / "2016_930_DOCUMENTS_DEDUP.parquet")
    actions = _read_parquet(output / "07_DEDUP" / "2016_930_ACTIONS_DEDUP.parquet")
    parameters = _read_parquet(output / "04_ACTION_EXTRACTION" / "2016_930_PARAMETERS.parquet")
    gaps = _read_parquet(output / "03_GAP_AUDIT" / "2016_930_GAP_REGISTER.parquet")
    matrix = _read_parquet(output / "03_GAP_AUDIT" / "2016_930_CITY_POLICY_MATRIX_PASS_2.parquet")
    trace = pl.read_csv(OLD_RUN_ROOT / "release" / "CRPD_PROMOTION_GATE_TRACE.csv")
    action_ids = trace.get_column("action_id").cast(pl.String).to_list()
    actions = actions.filter(pl.col("action_id").cast(pl.String).is_in(action_ids))
    parameters = parameters.filter(pl.col("action_id").cast(pl.String).is_in(action_ids)) if not parameters.is_empty() else parameters
    return documents, actions, parameters, gaps, matrix, trace


def _audit_pdf_reference() -> dict[str, Any]:
    """Audit the known production PDF without deleting or moving it."""

    references: list[dict[str, Any]] = []
    db = PRODUCTION_DB
    try:
        import duckdb

        connection = duckdb.connect(str(db), read_only=True)
        for table in ("attachments", "policy_document_versions", "pdf_assets", "pdf_download_audit", "pdf_discovery_evidence"):
            try:
                columns = [row[0] for row in connection.execute(f'describe "{table}"').fetchall()]
            except Exception:
                continue
            predicates: list[str] = []
            params: list[str] = []
            for column in columns:
                lower = column.lower()
                if lower in {"sha256", "content_sha256", "content_hash", "file_sha256"}:
                    predicates.append(f'lower(cast("{column}" as varchar)) = ?')
                    params.append(TARGET_SHA256)
                elif lower in {"local_path", "archive_path", "raw_path", "path", "url", "source_url"}:
                    predicates.append(f'lower(cast("{column}" as varchar)) like ?')
                    params.append(f"%{PRODUCTION_PDF.name.lower()}%")
            if not predicates:
                continue
            count = int(connection.execute(f'select count(*) from "{table}" where ' + " or ".join(predicates), params).fetchone()[0])
            if count:
                references.append({"source": "duckdb", "table": table, "count": count})
        connection.close()
    except Exception as exc:
        references.append({"source": "duckdb", "status": "AUDIT_ERROR", "error_type": type(exc).__name__})

    curated_names = (
        "attachments.parquet",
        "pdf_assets.parquet",
        "pdf_download_audit.parquet",
        "pdf_discovery_evidence.parquet",
        "pdf_text_versions.parquet",
        "policy_document_versions.parquet",
        "policy_episode_documents.parquet",
        "policy_files.parquet",
    )
    for name in curated_names:
        path = DATA_ROOT / "curated" / name
        if not path.exists():
            continue
        try:
            schema = pl.read_parquet_schema(path)
            columns = list(schema)
            expressions: list[pl.Expr] = []
            for column in columns:
                lower = column.lower()
                if lower in {"sha256", "content_sha256", "content_hash", "file_sha256"}:
                    expressions.append(pl.col(column).cast(pl.String).str.to_lowercase().eq(TARGET_SHA256))
                elif lower in {"local_path", "archive_path", "raw_path", "path", "url", "source_url"}:
                    expressions.append(pl.col(column).cast(pl.String).str.to_lowercase().str.contains(PRODUCTION_PDF.name.lower(), literal=True))
            if not expressions:
                continue
            predicate = expressions[0]
            for expression in expressions[1:]:
                predicate = predicate | expression
            count = int(pl.scan_parquet(path).filter(predicate).select(pl.len()).collect().item())
            if count:
                references.append({"source": "curated_parquet", "path": str(path), "count": count})
        except Exception as exc:
            references.append({"source": "curated_parquet", "path": str(path), "status": "AUDIT_ERROR", "error_type": type(exc).__name__})

    registered = any(int(item.get("count", 0)) > 0 for item in references)
    return {
        "audited_at": _now(),
        "target_sha256": TARGET_SHA256,
        "target_path": str(PRODUCTION_PDF),
        "target_exists": PRODUCTION_PDF.exists(),
        "target_size_bytes": PRODUCTION_PDF.stat().st_size if PRODUCTION_PDF.exists() else None,
        "target_actual_sha256": _sha256(PRODUCTION_PDF) if PRODUCTION_PDF.exists() else None,
        "classification": "VALID_EXISTING_PRODUCTION_ASSET" if registered else "ISOLATION_ORPHAN_ASSET",
        "registered_reference": registered,
        "raw_pdf_mirror": "UNREGISTERED_MIRROR_OF_REGISTERED_ASSET" if registered else "UNREGISTERED",
        "references": references,
        "statement": "The file was not deleted, moved, rewritten, or used as a new V2 fetch result.",
    }


def _recovery_disposition() -> pl.DataFrame:
    source = pl.read_csv(OLD_RUN_ROOT / "release" / "CRPD_PENDING_50_DISPOSITION.csv")
    required = source.filter(pl.col("recovery_required") == True)  # noqa: E712
    rows: list[dict[str, Any]] = []
    for row in required.iter_rows(named=True):
        rows.append(
            {
                **row,
                "v2_disposition": "PREFERRED_SOURCE_RETRY",
                "next_action": "existing_recovery_controller_retry",
                "network_executed_in_v2": False,
                "evidence_status": "NO_DOCUMENT_VERSION_IN_SELECTED_RUN",
                "reason": "The bounded rehearsal recorded search evidence but no fetch; no new network request was made in V2.",
            }
        )
    return pl.DataFrame(rows, infer_schema_length=None) if rows else pl.DataFrame()


def _scope_split(
    actions_before: pl.DataFrame,
    actions_after: pl.DataFrame,
    trace_after: pl.DataFrame,
    recovery: pl.DataFrame,
    old_validation: dict[str, Any],
) -> pl.DataFrame:
    old_report = old_validation.get("report") if isinstance(old_validation.get("report"), dict) else old_validation
    rows = [
        {"scope": "EP930_SELECTED_20_CITY", "metric": "action_rows", "count": actions_after.height, "blocking": False, "source": "old production run 04/07_DEDUP"},
        {"scope": "EP930_SELECTED_20_CITY", "metric": "direction_unresolved_before", "count": _count_values(actions_before, "direction_state", "UNKNOWN"), "blocking": True, "source": "V2 deterministic closure"},
        {"scope": "EP930_SELECTED_20_CITY", "metric": "direction_unresolved_after", "count": _count_values(actions_after, "direction_state", "UNKNOWN"), "blocking": True, "source": "V2 deterministic closure"},
        {"scope": "EP930_SELECTED_20_CITY", "metric": "geography_unresolved_after", "count": _count_values(actions_after, "geography_state", "UNKNOWN"), "blocking": True, "source": "V2 deterministic closure"},
        {"scope": "EP930_SELECTED_20_CITY", "metric": "date_gate_fail", "count": _count_values(trace_after, "date_gate", "FAIL"), "blocking": True, "source": "promotion gate trace"},
        {"scope": "EP930_SELECTED_20_CITY", "metric": "recovery_required_13", "count": recovery.height, "blocking": True, "source": "selected queue disposition"},
        {"scope": "EP930_SELECTED_20_CITY", "metric": "api_pass1_success", "count": 0, "blocking": True, "source": "no manual/API call; controller gate"},
        {"scope": "EP930_SELECTED_20_CITY", "metric": "api_pass2_success", "count": 0, "blocking": True, "source": "Pass2 requires Pass1"},
        {"scope": "GLOBAL_ONLY", "metric": "release_records", "count": int(old_report.get("record_count", 0)), "blocking": False, "source": "prior release validator"},
        {"scope": "GLOBAL_ONLY", "metric": "missing_full_text", "count": int(old_report.get("missing_full_text_count_all_records", 0)), "blocking": True, "source": "prior release validator"},
        {"scope": "GLOBAL_ONLY", "metric": "missing_url", "count": int(old_report.get("missing_url_count_all_records", 0)), "blocking": True, "source": "prior release validator"},
        {"scope": "GLOBAL_ONLY", "metric": "unclassified_records", "count": int(old_report.get("collection_unclassified_record_count", 0)), "blocking": True, "source": "prior release validator"},
    ]
    return pl.DataFrame(rows)


def _api_state() -> dict[str, Any]:
    output = DATA_ROOT / "outputs" / "special_projects" / "2016_930"
    state = _json(output / "930_API_RECOVERY_STATE.json")
    provider = _json(output / "930_API_PROVIDER_STATUS.json")
    failures_path = output / "930_API_FAILURES.parquet"
    failures = _read_parquet(failures_path)
    latest_failure = {}
    if not failures.is_empty() and "created_at" in failures.columns:
        latest = failures.sort("created_at", descending=True).row(0, named=True)
        latest_failure = {
            key: latest.get(key)
            for key in ("created_at", "failure_class", "http_status", "response_received", "schema_valid", "configured_read_timeout", "configured_connect_timeout")
            if key in latest
        }
    return {
        "generated_at": _now(),
        "provider": provider.get("provider"),
        "model": provider.get("model"),
        "provider_status_from_last_controller_artifact": provider.get("status"),
        "phase": state.get("phase"),
        "last_probe_at": state.get("last_attempt_at"),
        "next_retry_at": state.get("next_retry_at"),
        "last_success_at": state.get("last_success_at"),
        "last_success_rate": state.get("last_success_rate"),
        "schema_valid": state.get("schema_valid"),
        "failure_class": latest_failure.get("failure_class"),
        "failure_evidence": latest_failure,
        "pass1_success_selected_v2": 0,
        "pass2_success_selected_v2": 0,
        "tokens": None,
        "cost": None,
        "usage_status": "unavailable",
        "manual_api_calls": 0,
        "v2_controller_api_calls": 0,
        "certification": "BLOCKED_BY_EXISTING_RECOVERY_GATE",
        "gate": "SINGLE_PROBE -> MICRO_5 -> MICRO_20 -> backlog remains controller-owned",
        "failure_rows_observed": failures.height,
    }


def _isolation_state() -> dict[str, Any]:
    artifact = ISOLATION_RUN_ROOT / "release" / "EP930_ISOLATION_RUNTIME_VALIDATION.json"
    payload = _json(artifact)
    return {
        "validated_at": _now(),
        "status": payload.get("status", "MISSING"),
        "source_artifact": str(artifact),
        "source_artifact_sha256": _sha256(artifact) if artifact.exists() else None,
        "source_run_root": str(ISOLATION_RUN_ROOT),
        "new_production_file_writes": payload.get("new_production_file_writes"),
        "production_raw_pdf_file_count_before": (payload.get("before") or {}).get("file_count"),
        "production_raw_pdf_file_count_after": (payload.get("after") or {}).get("file_count"),
        "network_confirmation": "REAL_NETWORK_FETCH_1_CITY_3_DOCUMENTS",
        "manual_api_calls": 0,
        "v2_network_requests": 0,
        "v2_statement": "V2 closure consumed existing artifacts only; it did not start a crawler or fetch a document.",
    }


def _write_gate_state(
    before: pl.DataFrame,
    after: pl.DataFrame,
    actions: pl.DataFrame,
    final: pl.DataFrame,
    imported_ids: set[str],
    recovery_count: int,
    api: dict[str, Any],
) -> pl.DataFrame:
    before_by_id = {str(row["action_id"]): row for row in before.iter_rows(named=True)}
    action_by_id = {str(row["action_id"]): row for row in actions.iter_rows(named=True)}
    final_by_id = {str(row["action_id"]): row for row in final.iter_rows(named=True)}
    rows: list[dict[str, Any]] = []
    for row in actions.iter_rows(named=True):
        action_id = str(row.get("action_id") or "")
        old = before_by_id.get(action_id, {})
        new = action_by_id.get(action_id, {})
        current = final_by_id.get(action_id, {})
        rows.append(
            {
                "action_id": action_id,
                "document_id": row.get("document_id"),
                "direction_state": new.get("direction_state"),
                "direction_source": new.get("direction_source"),
                "direction_evidence": new.get("direction_evidence"),
                "direction_confidence": new.get("direction_confidence"),
                "geography_state": new.get("geography_state"),
                "geography_source": new.get("geography_source"),
                "geography_evidence": new.get("geography_evidence"),
                "geography_confidence": new.get("geography_confidence"),
                "pre_direction_gate": old.get("direction_gate"),
                "post_direction_gate": current.get("direction_gate"),
                "pre_geography_gate": old.get("geography_gate"),
                "post_geography_gate": current.get("geography_gate"),
                "date_gate": current.get("date_gate"),
                "dedup_gate": current.get("dedup_gate"),
                "database_gate": current.get("database_gate"),
                "promotion_gate": current.get("promotion_gate"),
                "first_failed_gate": current.get("first_failed_gate"),
                "formal_import_requested": action_id in imported_ids,
                "api_pass1_status": "DEFERRED_RECOVERY_GATE" if api["pass1_success_selected_v2"] == 0 else "SUCCESS",
                "api_pass2_status": "NOT_ELIGIBLE" if api["pass2_success_selected_v2"] == 0 else "SUCCESS",
                "recovery_required_count": recovery_count,
            }
        )
    return pl.DataFrame(rows, infer_schema_length=None)


def main() -> int:
    if not OLD_RUN_ROOT.exists():
        raise FileNotFoundError(f"required completed rehearsal is missing: {OLD_RUN_ROOT}")
    scope = _json(SCOPE_PATH)
    scope_hash = _text(scope.get("scope_hash"))
    if scope_hash != "a751e9a99d405bc91dc4a5c4a19f900b398400e1e88b43f724e034209807a90d":
        raise RuntimeError("frozen Analysis-ready scope hash does not match the registered value")

    run_root = _new_run_root()
    release = run_root / "release"
    release.mkdir(parents=True)
    curated = _copy_formal_import_inputs(run_root)
    documents, actions, parameters, gaps, matrix, old_trace = _read_selected_run()
    closed_actions = close_action_gates(actions, documents)
    pre_import_trace = build_promotion_gate_trace(
        closed_actions,
        documents,
        verified_action_ids=closed_actions.get_column("action_id").to_list(),
        dedup_action_ids=closed_actions.get_column("action_id").to_list(),
    )
    requested_ids = set(pre_import_trace.filter(pl.col("eligible_for_import")).get_column("action_id").to_list())
    requested_actions = closed_actions.filter(pl.col("action_id").is_in(sorted(requested_ids)))
    requested_parameters = parameters.filter(pl.col("action_id").is_in(sorted(requested_ids))) if not parameters.is_empty() else parameters

    settings = Settings(
        root=REPO_ROOT,
        data_root_path=run_root / "data",
        database_path=run_root / "data" / "database" / "policydb.duckdb",
        curated_path=curated,
        outputs_path=run_root / "data" / "outputs",
    )
    context = build_runtime_context(
        settings,
        run_mode="REHEARSAL",
        run_id=run_root.name,
        production_write_allowed=False,
        expected_production_database=PRODUCTION_DB,
        release_root=release,
    )
    pipeline = Episode930Pipeline(
        settings,
        config=EpisodeConfig(run_search=False, run_ai=False, apply=True),
        output=run_root / "data" / "outputs" / "special_projects" / "2016_930",
    )
    import_metrics = pipeline.formal_import(
        documents,
        requested_actions,
        requested_parameters,
        gaps,
        matrix,
    )
    persisted = _read_parquet(curated / "policy_episode_actions.parquet")
    persisted_ids = set(persisted.get_column("action_id").cast(pl.String).to_list()) if not persisted.is_empty() and "action_id" in persisted.columns else set()
    final_trace = build_promotion_gate_trace(
        closed_actions,
        documents,
        verified_action_ids=closed_actions.get_column("action_id").to_list(),
        dedup_action_ids=closed_actions.get_column("action_id").to_list(),
        database_action_ids=persisted_ids,
    )
    recovery = _recovery_disposition()
    api = _api_state()
    pdf_audit = _audit_pdf_reference()
    isolation = _isolation_state()
    old_validation = _json(OLD_RUN_ROOT / "release" / "CRPD_RELEASE_VALIDATION_REPORT.json")
    gate_state = _write_gate_state(
        old_trace,
        pre_import_trace,
        closed_actions,
        final_trace,
        requested_ids,
        recovery.height,
        api,
    )
    _atomic_csv(gate_state, release / "EP930_ACTION_GATE_STATE.csv")
    _atomic_csv(recovery, release / "EP930_RECOVERY_13_DISPOSITION.csv")
    _atomic_csv(
        _scope_split(actions, closed_actions, final_trace, recovery, old_validation),
        release / "EP930_COMPLETENESS_SCOPE_SPLIT.csv",
    )
    _atomic_csv(
        gate_state.select(
            [
                "action_id",
                "document_id",
                "promotion_gate",
                "first_failed_gate",
                "formal_import_requested",
                "post_direction_gate",
                "post_geography_gate",
                "date_gate",
                "database_gate",
                "api_pass1_status",
                "api_pass2_status",
            ]
        ),
        release / "EP930_PROMOTION_RESULT.csv",
    )
    _atomic_json(release / "EP930_PDF_REFERENCE_AUDIT.json", pdf_audit)
    _atomic_json(release / "EP930_ISOLATION_RUNTIME_VALIDATION.json", isolation)
    _atomic_json(release / "EP930_API_CERTIFICATION_STATE.json", api)
    release_validation = {
        "generated_at": _now(),
        "status": "FAIL",
        "scope": {"episode_id": EPISODE_ID, "scope_version": scope.get("scope_version"), "scope_hash": scope_hash, "scope_unit": "queue_item", "scope_city_count": scope.get("city_count"), "scope_queue_item_count": len(scope.get("queue_item_ids") or [])},
        "runtime": {"run_mode": context.run_mode, "data_root": str(context.data_root), "production_write_allowed": context.production_write_allowed, "production_database_unchanged_target": str(PRODUCTION_DB)},
        "selected_rows": actions.height,
        "post_closure": {"direction_gate_fail": _count_values(final_trace, "direction_gate", "FAIL"), "geography_gate_fail": _count_values(final_trace, "geography_gate", "FAIL"), "date_gate_fail": _count_values(final_trace, "date_gate", "FAIL"), "eligible_for_import": int(pre_import_trace.filter(pl.col("eligible_for_import")).height), "formal_import_requested": len(requested_ids), "formal_import_new_actions": int(import_metrics.get("new_action_rows", 0))},
        "api": {"certification": api.get("certification"), "pass1_success": api.get("pass1_success_selected_v2"), "pass2_success": api.get("pass2_success_selected_v2"), "tokens": None, "cost": None},
        "recovery": {"required": recovery.height, "network_executed_in_v2": 0},
        "pdf": {"classification": pdf_audit.get("classification"), "isolation_status": isolation.get("status")},
        "global_reference_validation": old_validation,
        "blocking_reasons": ["API_CERTIFICATION_BLOCKED", "DATE_OR_EPISODE_MEMBERSHIP_GATE_OPEN", "RECOVERY_13_REMAINING"],
    }
    _atomic_json(release / "EP930_RELEASE_VALIDATION.json", release_validation)
    formal_new = int(import_metrics.get("new_action_rows", 0))
    root_blockers = [
        {"code": "API_CERTIFICATION_BLOCKED", "evidence": "Existing controller remains gated; V2 made zero API calls."},
        {"code": "DATE_OR_EPISODE_MEMBERSHIP_GATE_OPEN", "evidence": f"date_gate_fail={_count_values(final_trace, 'date_gate', 'FAIL')} of {final_trace.height}; no date was guessed."},
        {"code": "RECOVERY_13_REMAINING", "evidence": f"recovery_required={recovery.height}; no new fetch was started."},
    ]
    decision = {
        "generated_at": _now(),
        "decision": "PROMOTE" if formal_new > 0 and not recovery.height and api.get("certification") == "CERTIFIED" and _count_values(final_trace, "date_gate", "FAIL") == 0 else "DO_NOT_PROMOTE",
        "run_root": str(run_root),
        "scope": {"scope_version": scope.get("scope_version"), "scope_hash": scope_hash, "scope_unit": "queue_item", "scope_city_count": scope.get("city_count"), "scope_queue_item_count": len(scope.get("queue_item_ids") or [])},
        "formal_actions_promoted": formal_new,
        "gates": {"runtime_isolated": context.run_mode == "REHEARSAL" and not context.production_write_allowed, "isolation_runtime": isolation.get("status") == "PASS", "pdf_reference_audited": pdf_audit.get("classification") == "VALID_EXISTING_PRODUCTION_ASSET", "direction_geography_closure": _count_values(final_trace, "direction_gate", "FAIL") == 0 and _count_values(final_trace, "geography_gate", "FAIL") == 0, "promotion_trace_has_eligible": bool(requested_ids), "formal_actions_promoted": formal_new > 0, "api_certified": api.get("certification") == "CERTIFIED", "recovery_13_closed": recovery.height == 0, "release_validation": release_validation["status"] == "PASS"},
        "root_blockers": root_blockers[:3],
        "statement": "This decision is diagnostic and not a production promotion authorization unless every gate is PASS.",
    }
    _atomic_json(release / "EP930_PROMOTION_DECISION.json", decision)
    report = "\n".join(
        [
            "# EP930 Promotion Blocker Closure V2",
            "",
            f"- run_root: `{run_root}`",
            f"- decision: **{decision['decision']}**",
            f"- scope: `{scope.get('scope_version')}` / `{scope_hash}` / `queue_item` / `{scope.get('city_count')} cities` / `{len(scope.get('queue_item_ids') or [])} queue items`",
            "",
            "## Evidence",
            "",
            f"- Existing 1-city/3-document network isolation confirmation: `{isolation['status']}`; new production PDF writes: `{isolation.get('new_production_file_writes')}`; V2 network/API calls: `0/0`.",
            f"- Target PDF classification: `{pdf_audit['classification']}`; actual SHA matches target: `{pdf_audit.get('target_actual_sha256') == TARGET_SHA256}`.",
            f"- Selected actions: `{actions.height}`; direction gate failures after closure: `{_count_values(final_trace, 'direction_gate', 'FAIL')}`; geography failures: `{_count_values(final_trace, 'geography_gate', 'FAIL')}`; date failures: `{_count_values(final_trace, 'date_gate', 'FAIL')}`.",
            f"- Formal importer requested `{len(requested_ids)}` actions and produced `{formal_new}` new formal action rows; no direct table update was used.",
            f"- Recovery 13 remaining: `{recovery.height}`; all V2 recovery network executions: `0`.",
            f"- API phase from existing controller artifact: `{api.get('phase')}`; certification: `{api.get('certification')}`; selected V2 Pass1/Pass2: `0/0`; tokens/cost: `null/null`.",
            "",
            "## Decision",
            "",
            "The release remains `DO_NOT_PROMOTE`.  No date, episode membership, API result, or recovery completion was guessed.  Existing production data and the leaked PDF were preserved.",
            "",
            "### Root blockers",
            "",
            *[f"- `{item['code']}` — {item['evidence']}" for item in root_blockers],
        ]
    ) + "\n"
    report_path = release / "EP930_BLOCKER_CLOSURE_V2_REPORT.md"
    report_path.write_text(report, encoding="utf-8")
    files = []
    for path in sorted(release.iterdir()):
        if path.is_file() and path.name != "SHA256_MANIFEST.json":
            files.append({"path": path.name, "size_bytes": path.stat().st_size, "sha256": _sha256(path)})
    manifest = {"generated_at": _now(), "run_root": str(run_root), "files": files, "manifest_scope": {"scope_version": scope.get("scope_version"), "scope_hash": scope_hash}, "statement": "Manifest covers V2 release files; the manifest file itself is excluded to avoid recursive hashing."}
    _atomic_json(release / "SHA256_MANIFEST.json", manifest)
    _atomic_json(run_root / "V2_RUN_SUMMARY.json", {"run_root": str(run_root), "release": str(release), "decision": decision, "formal_import": import_metrics, "pdf_reference_audit": pdf_audit, "isolation": isolation, "api": api})
    print(json.dumps({"run_root": str(run_root), "release": str(release), "decision": decision["decision"], "actions": actions.height, "direction_fail": _count_values(final_trace, "direction_gate", "FAIL"), "geography_fail": _count_values(final_trace, "geography_gate", "FAIL"), "date_fail": _count_values(final_trace, "date_gate", "FAIL"), "formal_new_actions": formal_new, "recovery_required": recovery.height, "api_certification": api.get("certification"), "pdf_classification": pdf_audit.get("classification")}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
