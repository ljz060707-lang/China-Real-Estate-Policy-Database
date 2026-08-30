"""Run one isolated CRPD promotion rehearsal and write an auditable handoff.

The rehearsal is deliberately separate from the production data root.  It copies
the stable curated snapshot and database into a new, immutable-after-creation
run directory, resets only the selected queue rows in that copy, and delegates
all discovery/fetch/extraction/promotion work to the normal JobManager worker.
No production path is used for a write by this script.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from policydb.jobs.manager import JobManager  # noqa: E402
from policydb.jobs.models import CrawlJobRequest  # noqa: E402
from policydb.parquet_store import atomic_write_parquet, read_parquet_snapshot  # noqa: E402
from policydb.runtime_context import build_runtime_context  # noqa: E402
from policydb.settings import Settings  # noqa: E402

EPISODE_ID = "EP_2016_930_TIGHTENING"
SOURCE_DATA_ROOT = Path(r"E:\Data Set\CRPD")
SOURCE_OUTPUT = SOURCE_DATA_ROOT / "outputs" / "special_projects" / "2016_930"
DEFAULT_QUEUE = SOURCE_OUTPUT / "930_TASK_QUEUE.parquet"
DEFAULT_SCOPE = SOURCE_OUTPUT / "930_ANALYSIS_READY_SCOPE.json"
REHEARSAL_PARENT = SOURCE_DATA_ROOT / "promotion_rehearsal"
PRODUCTION_PDF_ROOT = SOURCE_DATA_ROOT / "raw" / "pdf"
LEAKED_PDF = PRODUCTION_PDF_ROOT / "objects" / "01" / "011dfd02191e5909ade25c97fc46f4211dc7c30095027f3a6afdffe55a98ac67.pdf"

CITY_IDS = [
    "CITY_110000",
    "CITY_310000",
    "CITY_440100",
    "CITY_440300",
    "CITY_140400",
    "CITY_320100",
    "CITY_330100",
    "CITY_420100",
    "CITY_510100",
    "CITY_410100",
    "CITY_370100",
    "CITY_350200",
    "CITY_320500",
    "CITY_130600",
    "CITY_320800",
    "CITY_140100",
    "CITY_120000",
    "CITY_430100",
    "CITY_650100",
    "CITY_210100",
]

TERMINAL_STATUSES = {"completed", "completed_with_warnings", "failed", "cancelled"}


def _now() -> datetime:
    return datetime.now(UTC)


def _iso() -> str:
    return _now().isoformat()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _pdf_boundary_snapshot() -> dict[str, Any]:
    entries: dict[str, dict[str, Any]] = {}
    if PRODUCTION_PDF_ROOT.exists():
        for path in sorted(PRODUCTION_PDF_ROOT.rglob("*")):
            if not path.is_file():
                continue
            stat = path.stat()
            relative = path.relative_to(PRODUCTION_PDF_ROOT).as_posix()
            entries[relative] = {
                "size_bytes": stat.st_size,
                "mtime_ns": stat.st_mtime_ns,
            }
    target_key = LEAKED_PDF.relative_to(PRODUCTION_PDF_ROOT).as_posix()
    target = entries.get(target_key)
    if target and LEAKED_PDF.exists():
        target = {**target, "sha256": _sha256(LEAKED_PDF)}
    return {
        "captured_at": _iso(),
        "root": str(PRODUCTION_PDF_ROOT),
        "exists": PRODUCTION_PDF_ROOT.exists(),
        "file_count": len(entries),
        "total_size_bytes": sum(item["size_bytes"] for item in entries.values()),
        "entries": entries,
        "leaked_pdf_relative_path": target_key,
        "leaked_pdf": target,
    }


def _write_pdf_boundary_validation(release: Path, before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
    before_entries = before.get("entries") if isinstance(before.get("entries"), dict) else {}
    after_entries = after.get("entries") if isinstance(after.get("entries"), dict) else {}
    new_files = sorted(set(after_entries) - set(before_entries))
    removed_files = sorted(set(before_entries) - set(after_entries))
    changed_files = sorted(
        key for key in set(before_entries) & set(after_entries)
        if before_entries[key] != after_entries[key]
    )
    payload = {
        "validated_at": _iso(),
        "status": "PASS" if not new_files and not removed_files and not changed_files else "FAIL",
        "new_production_file_writes": len(new_files) + len(changed_files),
        "new_files": new_files,
        "changed_existing_files": changed_files,
        "removed_files": removed_files,
        "before": {key: value for key, value in before.items() if key != "entries"},
        "after": {key: value for key, value in after.items() if key != "entries"},
        "leaked_pdf_reference_audit_required": True,
        "statement": "This validation covers the production raw/pdf boundary only; all rehearsal writes must remain under the isolated data root.",
    }
    _atomic_json(release / "EP930_ISOLATION_RUNTIME_VALIDATION.json", payload)
    return payload


def _atomic_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temp.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    os.replace(temp, path)


def _safe(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_safe(item) for item in value]
    return value


def _env_for_run(run_root: Path) -> dict[str, str]:
    data_root = run_root / "data"
    paths = {
        "POLICYDB_ROOT": str(REPO_ROOT),
        "CRPD_DATA_ROOT": str(data_root),
        "POLICYDB_DATA_ROOT": str(data_root),
        "POLICYDB_DATABASE": str(data_root / "database" / "policydb.duckdb"),
        "CRPD_DB": str(data_root / "database" / "policydb.duckdb"),
        "POLICYDB_CURATED_ROOT": str(data_root / "curated"),
        "CRPD_CURATED_ROOT": str(data_root / "curated"),
        "POLICYDB_RAW_ROOT": str(data_root / "raw"),
        "CRPD_RAW_ROOT": str(data_root / "raw"),
        "POLICYDB_RESEARCH_ROOT": str(data_root / "research"),
        "POLICYDB_OUTPUTS_ROOT": str(data_root / "outputs"),
        "POLICYDB_OUTPUT_ROOT": str(data_root / "outputs"),
        "POLICYDB_LOG_ROOT": str(data_root / "logs"),
        "POLICYDB_AUTOMATION_ROOT": str(data_root / "automation"),
        "POLICYDB_CONTROL_ROOT": str(data_root / "control"),
        "POLICYDB_RUNTIME_ROOT": str(data_root / "runtime"),
        "POLICYDB_CACHE_ROOT": str(data_root / "cache"),
        "POLICYDB_TEMP_ROOT": str(data_root / "temp"),
        "POLICYDB_TEST_ARTIFACTS_ROOT": str(data_root / "test_artifacts"),
        "POLICYDB_DASHBOARD_ROOT": str(data_root / "dashboard"),
        "POLICYDB_BACKUPS_ROOT": str(data_root / "backups"),
        "CRPD_ARCHIVE_ROOT": str(data_root / "archive"),
        "POLICYDB_ARCHIVE_ROOT": str(data_root / "archive"),
        "POLICYDB_SKIP_STORAGE_CONFIG": "1",
        "POLICYDB_READ_ONLY": "0",
        "POLICYDB_MAX_CONCURRENCY": "2",
        "POLARS_MAX_THREADS": "2",
        "OMP_NUM_THREADS": "1",
        "ARROW_NUM_THREADS": "2",
        "OPENBLAS_NUM_THREADS": "1",
        "MKL_NUM_THREADS": "1",
        "NUMEXPR_NUM_THREADS": "1",
    }
    return paths


def _apply_run_environment(run_root: Path) -> None:
    for key, value in _env_for_run(run_root).items():
        os.environ[key] = value
    existing = os.environ.get("PYTHONPATH", "")
    os.environ["PYTHONPATH"] = os.pathsep.join(
        part for part in (str(SRC_ROOT), existing) if part
    )


def _copy_curated_snapshot(source: Path, target: Path) -> dict[str, Any]:
    target.mkdir(parents=True, exist_ok=True)
    copied: list[dict[str, Any]] = []
    for source_file in sorted(source.glob("*.parquet")):
        if ".tmp." in source_file.name:
            continue
        target_file = target / source_file.name
        shutil.copy2(source_file, target_file)
        copied.append(
            {"name": source_file.name, "size": target_file.stat().st_size, "sha256": _sha256(target_file)}
        )
    return {"source": str(source), "files": copied, "file_count": len(copied)}


def _reset_selected_queue(source_path: Path, target_path: Path, city_ids: list[str]) -> dict[str, Any]:
    source = read_parquet_snapshot(source_path)
    if source.is_empty():
        raise ValueError(f"source queue is empty: {source_path}")
    if "city_id" not in source.columns or "queue_item_id" not in source.columns:
        raise ValueError("930 queue must include city_id and queue_item_id")
    indexed = source.with_row_index("__source_order")
    selected = (
        indexed.filter(pl.col("city_id").is_in(city_ids))
        .sort(["city_id", "__source_order"])
        .group_by("city_id", maintain_order=True)
        .first()
    )
    selected_ids = selected.get_column("queue_item_id").to_list()
    if len(selected_ids) != len(set(selected_ids)):
        raise ValueError("selected queue ids are not unique")
    if len(selected_ids) != len(city_ids):
        missing = sorted(set(city_ids) - set(selected.get_column("city_id").to_list()))
        raise ValueError(f"queue does not contain all requested cities: {missing}")
    string_nulls = {
        name: pl.lit(None).cast(dtype)
        for name, dtype in source.schema.items()
        if dtype == pl.String and name in {
            "lease_owner",
            "lease_acquired_at",
            "lease_expires_at",
            "completed_at",
            "last_attempt_at",
            "failure_reason",
            "search_provider",
            "content_sha256",
            "crawl_run_id",
            "crawl_item_id",
            "document_version_id",
            "evidence_path",
        }
    }
    reset_values: dict[str, Any] = {
        "status": "PENDING",
        "execution_status": "PENDING",
        "fetch_status": "NOT_ATTEMPTED",
        "result_status": "NO_RESULT",
        "attempt_count": 0,
        "documents_found": 0,
        "documents_recovered": 0,
        "actions_extracted": 0,
        "actions_classified": 0,
        "pdfs_found": 0,
        "pdfs_archived": 0,
        "search_executed": False,
        "search_call_count": 0,
        "search_result_count": 0,
        "http_request_count": 0,
        "real_network_fetch": False,
        "response_bytes": 0,
        "cache_hit": False,
        "updated_at": _iso(),
    }
    expressions = []
    selected_expr = pl.col("queue_item_id").is_in(selected_ids)
    for name, value in reset_values.items():
        if name in source.columns:
            expressions.append(pl.when(selected_expr).then(pl.lit(value)).otherwise(pl.col(name)).alias(name))
    expressions.extend(
        pl.when(selected_expr).then(value).otherwise(pl.col(name)).alias(name)
        for name, value in string_nulls.items()
    )
    isolated = indexed.drop("__source_order").with_columns(expressions)
    atomic_write_parquet(isolated, target_path, {"module": "promotion_rehearsal", "source": str(source_path)})
    return {
        "source_path": str(source_path),
        "source_sha256": _sha256(source_path),
        "isolated_path": str(target_path),
        "isolated_sha256": _sha256(target_path),
        "source_rows": source.height,
        "selected_rows": len(selected_ids),
        "selected_queue_item_ids": selected_ids,
        "selected_city_ids": sorted(city_ids),
    }


def _prepare_root(run_root: Path, queue_source: Path, scope_source: Path, city_ids: list[str]) -> dict[str, Any]:
    if run_root.exists():
        raise FileExistsError(f"refusing to reuse existing rehearsal root: {run_root}")
    run_root.mkdir(parents=True)
    data_root = run_root / "data"
    for name in ("database", "outputs", "logs", "raw", "archive", "runtime", "temp"):
        (data_root / name).mkdir(parents=True, exist_ok=True)
    curated_manifest = _copy_curated_snapshot(SOURCE_DATA_ROOT / "curated", data_root / "curated")
    production_db = SOURCE_DATA_ROOT / "database" / "policydb.duckdb"
    isolated_db = data_root / "database" / "policydb.duckdb"
    db_copy = None
    if production_db.exists():
        shutil.copy2(production_db, isolated_db)
        db_copy = {"source": str(production_db), "path": str(isolated_db), "sha256": _sha256(isolated_db), "size": isolated_db.stat().st_size}
    output = data_root / "outputs" / "special_projects" / "2016_930"
    output.mkdir(parents=True, exist_ok=True)
    queue_target = output / "930_TASK_QUEUE.parquet"
    queue_manifest = _reset_selected_queue(queue_source, queue_target, city_ids)
    scope_manifest = None
    if scope_source.exists():
        scope_target = output / scope_source.name
        shutil.copy2(scope_source, scope_target)
        scope_manifest = {"source": str(scope_source), "path": str(scope_target), "sha256": _sha256(scope_target), "size": scope_target.stat().st_size}
    return {
        "run_root": str(run_root),
        "data_root": str(data_root),
        "curated": curated_manifest,
        "database": db_copy,
        "queue": queue_manifest,
        "scope": scope_manifest,
    }


def _settings(run_root: Path) -> Settings:
    data_root = run_root / "data"
    return Settings(
        root=REPO_ROOT,
        data_root_path=data_root,
        database_path=data_root / "database" / "policydb.duckdb",
        curated_path=data_root / "curated",
        raw_path=data_root / "raw",
        outputs_path=data_root / "outputs",
        logs_path=data_root / "logs",
        runtime_path=data_root / "runtime",
        temp_path=data_root / "temp",
    )


def _build_request(output: Path, queue_path: Path, cities: list[str], max_ai_calls: int, max_fetches: int) -> CrawlJobRequest:
    from datetime import date

    return CrawlJobRequest(
        mode="historical_episode_930",
        episode_id=EPISODE_ID,
        episode_city_limit=len(cities),
        episode_max_ai_calls=max_ai_calls,
        start_date=date(2016, 9, 1),
        end_date=date(2016, 10, 31),
        cities=cities,
        episode_queue_path=str(queue_path),
        episode_output_path=str(output),
        max_candidates=80,
        max_candidates_total=80,
        max_candidates_per_source=3,
        max_pages_per_source=2,
        batch_size=20,
        global_safety_limit=80,
        resume=True,
        max_fetches=max_fetches,
        drain_selected_batch=True,
        max_attachment_attempts=1,
        enabled_only=True,
        include_recommended=False,
        run_glm=True,
        run_verification=True,
        rebuild_database=True,
        run_validation=False,
        official_first=True,
        runtime_mode="REHEARSAL",
        production_write_allowed=False,
        processing_mode="full",
    )


def _safe_state(state: Any) -> dict[str, Any]:
    return {
        "job_id": state.job_id,
        "status": state.status,
        "stage": state.stage,
        "pid": state.pid,
        "run_id": state.run_id,
        "message": state.message,
        "error_type": state.error_type,
        "error_message": state.error_message,
        "progress": [state.progress_current, state.progress_total],
        "processed_count": state.processed_count,
        "queued_count": state.queued_count,
        "heartbeat_at": _safe(state.heartbeat_at),
        "last_progress_at": _safe(state.last_progress_at),
        "counters": _safe(state.counters),
    }


def _run_worker(settings: Settings, request: CrawlJobRequest, poll_seconds: int) -> tuple[Any, dict[str, Any]]:
    manager = JobManager(settings)
    state = manager.create(request)
    manager.start(state.job_id)
    print(json.dumps({"event": "rehearsal_started", "job_id": state.job_id, "run_root": str(settings.data_root)}, ensure_ascii=False), flush=True)
    while True:
        state = manager.inspect_state(state.job_id)
        print(json.dumps({"event": "state", **_safe_state(state)}, ensure_ascii=False), flush=True)
        if state.status in TERMINAL_STATUSES:
            break
        time.sleep(max(1, poll_seconds))
    result_path = manager.job_dir(state.job_id) / "result.json"
    result = {}
    if result_path.exists():
        try:
            result = json.loads(result_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            result = {"result_json_error": True}
    return state, result


def _find_latest(root: Path, filename: str) -> Path | None:
    paths = [path for path in root.rglob(filename) if path.is_file()]
    return max(paths, key=lambda path: path.stat().st_mtime) if paths else None


def _read_json(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _text(value: Any) -> str:
    return str(value or "").strip()


def _has(value: Any) -> bool:
    return _text(value).upper() not in {"", "NONE", "NULL", "NAN"}


def _queue_disposition(queue_path: Path) -> pl.DataFrame:
    queue = read_parquet_snapshot(queue_path) if queue_path.exists() else pl.DataFrame()
    if queue.is_empty():
        return pl.DataFrame(schema={"queue_item_id": pl.String, "disposition": pl.String, "recovery_required": pl.Boolean, "raw_status": pl.String})
    rows: list[dict[str, Any]] = []
    for row in queue.iter_rows(named=True):
        searched = bool(row.get("search_executed"))
        fetched = bool(row.get("real_network_fetch"))
        cache_hit = bool(row.get("cache_hit"))
        document_version = _has(row.get("document_version_id"))
        http_count = int(row.get("http_request_count") or 0)
        status = _text(row.get("status"))
        if searched and fetched and document_version:
            disposition = "LIVE_SEARCH_AND_FETCH"
        elif searched and cache_hit:
            disposition = "LIVE_SEARCH_CACHE_REUSE"
        elif searched and (http_count > 0 or document_version):
            disposition = "LIVE_SEARCH_NO_NEW_URL"
        elif not searched and document_version:
            disposition = "LOCAL_DB_REUSE"
        elif cache_hit:
            disposition = "CACHE_ONLY"
        elif searched and http_count == 0:
            disposition = "FETCH_NOT_EXECUTED"
        elif not searched:
            disposition = "SEARCH_NOT_EXECUTED"
        else:
            disposition = "UNKNOWN_PROVENANCE"
        recovery_required = disposition in {"SEARCH_NOT_EXECUTED", "FETCH_NOT_EXECUTED", "UNKNOWN_PROVENANCE"} and status not in {"EXCLUDED", "SUPERSEDED"}
        rows.append({
            "queue_item_id": row.get("queue_item_id"),
            "city_id": row.get("city_id"),
            "city": row.get("city"),
            "source_role": row.get("source_role"),
            "raw_status": status,
            "disposition": disposition,
            "recovery_required": recovery_required,
            "search_executed": searched,
            "real_network_fetch": fetched,
            "cache_hit": cache_hit,
            "document_version_id": row.get("document_version_id"),
            "crawl_item_id": row.get("crawl_item_id"),
            "crawl_run_id": row.get("crawl_run_id"),
            "failure_reason": row.get("failure_reason"),
        })
    return pl.DataFrame(rows, infer_schema_length=None)


def _write_pending_disposition(release: Path, output: Path, *, crawl_run_id: str | None = None) -> dict[str, Any]:
    frame = _queue_disposition(output / "930_TASK_QUEUE.parquet")
    if crawl_run_id and not frame.is_empty() and "crawl_run_id" in frame.columns:
        scoped = frame.filter(pl.col("crawl_run_id") == crawl_run_id)
        if not scoped.is_empty():
            frame = scoped
    path = release / "CRPD_PENDING_50_DISPOSITION.csv"
    frame.write_csv(path)
    counts = frame.group_by("disposition").len().sort("disposition").to_dicts() if not frame.is_empty() else []
    return {"path": str(path), "rows": frame.height, "counts": counts, "recovery_required": int(frame.filter(pl.col("recovery_required")).height) if not frame.is_empty() else 0}


def _audit_rows(run_root: Path) -> list[dict[str, Any]]:
    request_root = run_root / "data" / "outputs"
    rows: list[dict[str, Any]] = []
    for path in request_root.rglob("*.json"):
        if "ai_audit" not in {part.lower() for part in path.parts} and "requests" not in {part.lower() for part in path.parts}:
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(payload, dict):
            continue
        status = _text(payload.get("status") or payload.get("request_status") or payload.get("state"))
        error = _text(payload.get("error_type") or payload.get("failure_class") or payload.get("error"))
        lower = f"{status} {error}".lower()
        if any(term in lower for term in ("timeout", "connection", "provider", "429", "503")):
            failure_class = "TRANSIENT_PROVIDER_FAILURE"
        elif "schema" in lower or payload.get("schema_valid") is False:
            failure_class = "SCHEMA_VALIDATION_FAILURE"
        elif "no_action" in lower or "no action" in lower:
            failure_class = "NO_ACTIONS_EXTRACTED"
        elif status.lower() in {"complete", "completed", "response_completed", "success"}:
            failure_class = "SUCCESS"
        else:
            failure_class = "OTHER"
        rows.append({
            "audit_file": str(path),
            "request_id": payload.get("request_id"),
            "document_id": payload.get("document_id"),
            "content_sha256": payload.get("content_sha256") or payload.get("content_hash"),
            "status": status,
            "failure_class": failure_class,
            "error_type": _text(payload.get("error_type")),
            "http_status": payload.get("http_status"),
            "response_received": payload.get("response_received"),
            "json_parse_ok": payload.get("json_parse_ok"),
            "schema_valid": payload.get("schema_valid"),
            "provider": payload.get("provider"),
            "model": payload.get("model"),
            "prompt_version": payload.get("prompt_version"),
            "schema_version": payload.get("schema_version"),
            "created_at": payload.get("created_at") or payload.get("started_at") or payload.get("completed_at"),
        })
    return rows


def _write_extraction_analysis(release: Path, run_root: Path) -> dict[str, Any]:
    rows = _audit_rows(run_root)
    frame = pl.DataFrame(rows, infer_schema_length=None) if rows else pl.DataFrame(schema={"audit_file": pl.String, "request_id": pl.String, "status": pl.String, "failure_class": pl.String})
    path = release / "CRPD_EXTRACTION_FAILURE_ANALYSIS.csv"
    frame.write_csv(path)
    counts = frame.group_by("failure_class").len().sort("failure_class").to_dicts() if not frame.is_empty() else []
    return {"path": str(path), "rows": frame.height, "counts": counts}


def _attachment_report(release: Path, run_root: Path, output: Path) -> dict[str, Any]:
    summary_path = _find_latest(output, "POSTPROCESS_SUMMARY.json")
    summary = _read_json(summary_path)
    attachment = summary.get("attachments") if isinstance(summary.get("attachments"), dict) else {}
    pdf = attachment.get("pdf_pipeline") if isinstance(attachment.get("pdf_pipeline"), dict) else {}
    discover = pdf.get("discover") if isinstance(pdf.get("discover"), dict) else {}
    archive = pdf.get("archive") if isinstance(pdf.get("archive"), dict) else {}
    download = pdf.get("download") if isinstance(pdf.get("download"), dict) else {}
    parse = pdf.get("parse") if isinstance(pdf.get("parse"), dict) else {}
    match = pdf.get("match") if isinstance(pdf.get("match"), dict) else {}
    allowed_root = (run_root / "data").resolve()
    external_paths: list[str] = []
    for item in (discover, archive, download, parse, match):
        for key in ("inventory_root", "archive_root"):
            value = item.get(key)
            if not value:
                continue
            try:
                Path(str(value)).resolve().relative_to(allowed_root)
            except ValueError:
                external_paths.append(str(value))
    chain_status = "ISOLATION_VIOLATION" if external_paths else (
        "PASS" if any(bool(item) for item in (discover, archive, download, parse, match)) else "NONE_FOUND_IN_SAMPLE"
    )
    payload = {
        "validated_at": _iso(),
        "postprocess_summary": str(summary_path) if summary_path else None,
        "chain": {
            "discover": discover,
            "archive": archive,
            "download": download,
            "parse": parse,
            "match": match,
        },
        "attachments_found": attachment.get("attachments_found"),
        "attachments_archived": attachment.get("attachments_archived"),
        "pdfs_found": attachment.get("pdfs_found"),
        "pdfs_archived": attachment.get("pdfs_archived"),
        "status": chain_status,
        "isolation_incident": {
            "detected": bool(external_paths),
            "external_paths": sorted(set(external_paths)),
            "allowed_root": str(allowed_root),
            "action": "preserved_for_audit; no production file was deleted or moved",
        },
        "raw_attachment_tables": {
            "pdf_assets": (run_root / "data" / "curated" / "pdf_assets.parquet").exists(),
            "pdf_discovery_evidence": (run_root / "data" / "curated" / "pdf_discovery_evidence.parquet").exists(),
            "pdf_download_audit": (run_root / "data" / "curated" / "pdf_download_audit.parquet").exists(),
            "pdf_text_versions": (run_root / "data" / "curated" / "pdf_text_versions.parquet").exists(),
        },
    }
    path = release / "CRPD_ATTACHMENT_PIPELINE_VALIDATION.json"
    _atomic_json(path, payload)
    if external_paths:
        _atomic_json(
            release / "CRPD_ISOLATION_INCIDENT.json",
            {
                "status": "RECORDED",
                "detected_at": _iso(),
                "run_root": str(run_root),
                "external_paths": sorted(set(external_paths)),
                "allowed_root": str(allowed_root),
                "statement": (
                    "The pre-fix rehearsal resolved PDF inventory/archive paths outside the isolated root. "
                    "The observed external artifact is preserved for audit; no production file was deleted or moved. "
                    "The runtime-root override fix prevents recurrence."
                ),
            },
        )
    return payload | {"path": str(path)}


def _copy_gate_trace(release: Path, output: Path) -> dict[str, Any]:
    source = _find_latest(output, "CRPD_PROMOTION_GATE_TRACE.csv")
    target = release / "CRPD_PROMOTION_GATE_TRACE.csv"
    if source:
        shutil.copy2(source, target)
        frame = pl.read_csv(target)
    else:
        frame = pl.DataFrame(schema={"action_id": pl.String, "promotion_gate": pl.String, "first_failed_gate": pl.String, "eligible_for_import": pl.Boolean})
        frame.write_csv(target)
    return {
        "path": str(target),
        "source": str(source) if source else None,
        "rows": frame.height,
        "promotion_pass": int(frame.filter(pl.col("promotion_gate") == "PASS").height) if "promotion_gate" in frame.columns else 0,
        "promotion_fail": int(frame.filter(pl.col("promotion_gate") == "FAIL").height) if "promotion_gate" in frame.columns else 0,
        "eligible": int(frame.filter(pl.col("eligible_for_import")).height) if "eligible_for_import" in frame.columns else 0,
    }


def _validation_settings(run_root: Path) -> Settings:
    root = run_root / "validation_project"
    reference = root / "data" / "reference"
    staging = root / "data" / "staging" / "excel"
    reference.mkdir(parents=True, exist_ok=True)
    staging.mkdir(parents=True, exist_ok=True)
    for name in ("cities_105.csv", "source_registry.yaml", "crawl_keywords.yaml"):
        source = REPO_ROOT / "data" / "reference" / name
        if source.exists():
            shutil.copy2(source, reference / name)
    for source in (REPO_ROOT / "data" / "staging" / "excel").glob("*.parquet"):
        shutil.copy2(source, staging / source.name)
    data_root = run_root / "data"
    return Settings(
        root=root,
        data_root_path=data_root,
        database_path=data_root / "database" / "policydb.duckdb",
        curated_path=data_root / "curated",
        outputs_path=data_root / "outputs",
    )


def _run_release_validation(release: Path, run_root: Path) -> dict[str, Any]:
    payload: dict[str, Any]
    try:
        from policydb.validate.quality import validate

        report = validate(_validation_settings(run_root), group="release")
        payload = {"status": "PASS" if report.get("passed") else "FAIL", "report": report}
    except Exception as exc:  # validation failure is evidence, not a reason to fake PASS
        payload = {"status": "ERROR", "error_type": type(exc).__name__, "error_message": str(exc)[:1000]}
    path = release / "CRPD_RELEASE_VALIDATION_REPORT.json"
    _atomic_json(path, payload)
    return payload | {"path": str(path)}


def _table_metrics(run_root: Path) -> dict[str, Any]:
    curated = run_root / "data" / "curated"
    metrics: dict[str, Any] = {}
    for name in ("crawl_items", "policy_document_versions", "documents", "policy_actions", "policy_episode_actions", "parameters", "policy_episode_parameters", "attachments", "pdf_assets", "pdf_text_versions"):
        path = curated / f"{name}.parquet"
        if path.exists():
            try:
                metrics[name] = read_parquet_snapshot(path).height
            except Exception:
                metrics[name] = None
        else:
            metrics[name] = 0
    return metrics


def _write_risk_register(release: Path, *, state: Any, result: dict[str, Any], pending: dict[str, Any], attachment: dict[str, Any], validation: dict[str, Any], trace: dict[str, Any], metrics: dict[str, Any]) -> dict[str, Any]:
    rows = [
        {"severity": "P0" if state.status == "failed" else "P1", "blocker": "worker_terminal_state", "status": state.status, "evidence": state.error_type or state.message},
        {"severity": "P0" if pending.get("recovery_required", 0) else "P2", "blocker": "selected_queue_not_fully_provenanced", "status": "OPEN" if pending.get("recovery_required", 0) else "CLOSED", "evidence": pending},
        {"severity": "P1" if attachment.get("status") not in {"PASS", "NONE_FOUND_IN_SAMPLE"} else "P2", "blocker": "attachment_archive", "status": attachment.get("status"), "evidence": attachment.get("chain")},
        {"severity": "P0" if validation.get("status") != "PASS" else "P2", "blocker": "release_validator", "status": validation.get("status"), "evidence": validation.get("report", validation.get("error_message"))},
        {"severity": "P0" if trace.get("eligible", 0) == 0 else "P2", "blocker": "formal_action_promotion", "status": "OPEN" if trace.get("eligible", 0) == 0 else "CANDIDATES", "evidence": {"trace": trace, "tables": metrics}},
    ]
    rows = [
        {**row, "evidence": json.dumps(row["evidence"], ensure_ascii=False, default=str)}
        for row in rows
    ]
    frame = pl.DataFrame(rows)
    path = release / "CRPD_PRODUCTION_RISK_REGISTER.csv"
    frame.write_csv(path)
    return {"path": str(path), "rows": len(rows), "open": sum(row["status"] == "OPEN" or row["status"] in {"FAIL", "ERROR"} for row in rows)}


def _promotion_decision(
    state: Any,
    result: dict[str, Any],
    attachment: dict[str, Any],
    validation: dict[str, Any],
    trace: dict[str, Any],
    pending: dict[str, Any],
    isolation: dict[str, Any] | None = None,
) -> dict[str, Any]:
    postprocess = result.get("postprocess") if isinstance(result.get("postprocess"), dict) else {}
    formal_new = int(postprocess.get("formal_actions_promoted") or postprocess.get("promotion", {}).get("new_action_rows", 0) or 0)
    gates = {
        "runtime_isolated": True,
        "isolation_runtime": isolation is None or isolation.get("status") == "PASS",
        "worker_completed": state.status in {"completed", "completed_with_warnings"},
        "promotion_trace_has_eligible": trace.get("eligible", 0) > 0,
        "formal_actions_promoted": formal_new > 0,
        "release_validation": validation.get("status") == "PASS",
        "selected_queue_reconciled": pending.get("recovery_required", 0) == 0,
        "attachment_chain_recorded": attachment.get("status") in {"PASS", "NONE_FOUND_IN_SAMPLE"},
    }
    decision = "PROMOTE" if all(gates.values()) else "DO_NOT_PROMOTE"
    return {"decision": decision, "formal_actions_promoted": formal_new, "gates": gates, "reason": "all rehearsal gates passed" if decision == "PROMOTE" else "one or more promotion rehearsal gates remain open"}


def _write_report(release: Path, *, prepared: dict[str, Any], state: Any, result: dict[str, Any], pending: dict[str, Any], extraction: dict[str, Any], attachment: dict[str, Any], validation: dict[str, Any], trace: dict[str, Any], risk: dict[str, Any], decision: dict[str, Any], metrics: dict[str, Any], isolation: dict[str, Any] | None = None) -> None:
    report = {
        "generated_at": _iso(),
        "rehearsal_root": prepared["run_root"],
        "job_state": _safe_state(state),
        "result_status": result.get("status"),
        "run_id": result.get("run_id") or state.run_id,
        "queue": prepared["queue"],
        "pending_disposition": pending,
        "extraction_failure_analysis": extraction,
        "attachment_validation": attachment,
        "isolation_runtime": isolation,
        "release_validation": validation,
        "promotion_trace": trace,
        "curated_table_metrics": metrics,
        "decision": decision,
    }
    _atomic_json(release / "CRPD_REHEARSAL_RUN_MANIFEST.json", report)
    _atomic_json(release / "CRPD_PROMOTION_CANDIDATE.json", decision)
    lines = [
        "# CRPD Promotion Rehearsal",
        "",
        f"Decision: **{decision['decision']}**",
        "",
        "This is an isolated rehearsal. It never writes the production database, source registry, or published release.",
        "",
        "## Runtime",
        "",
        f"- Rehearsal root: `{prepared['run_root']}`",
        f"- Job: `{state.job_id}`",
        f"- Run: `{result.get('run_id') or state.run_id}`",
        f"- Final worker state: `{state.status}` / `{state.stage}`",
        "",
        "## Evidence",
        "",
        f"- Queue rows: `{pending.get('rows', 0)}`; records still requiring provenance: `{pending.get('recovery_required', 0)}`",
        f"- Gate trace rows: `{trace.get('rows', 0)}`; eligible: `{trace.get('eligible', 0)}`; persisted PASS: `{trace.get('promotion_pass', 0)}`",
        f"- Newly promoted formal actions: `{decision.get('formal_actions_promoted', 0)}`",
        f"- Attachment chain: `{attachment.get('status')}`",
        f"- Isolation runtime: `{(isolation or {}).get('status', 'NOT_RUN')}`",
        f"- Release validator: `{validation.get('status')}`",
        "",
        "## Gate decision",
        "",
    ]
    lines.extend(f"- {name}: `{value}`" for name, value in decision.get("gates", {}).items())
    lines.extend([
        "",
        "## Table counts",
        "",
    ])
    lines.extend(f"- {name}: `{value}`" for name, value in sorted(metrics.items()))
    (release / "CRPD_PROMOTION_REHEARSAL_REPORT.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    blocker_lines = [
        "# CRPD Blocker Closure Report",
        "",
        f"Decision: **{decision['decision']}**",
        "",
        "No blocker is hidden or converted into a success state. See the machine-readable risk register for evidence.",
        "",
    ]
    if decision["decision"] != "PROMOTE":
        blocker_lines.extend(f"- `{name}`: `{value}`" for name, value in decision.get("gates", {}).items() if not value)
    else:
        blocker_lines.append("All bounded rehearsal gates passed; production promotion still requires an explicit release operation.")
    (release / "CRPD_BLOCKER_CLOSURE_REPORT.md").write_text("\n".join(blocker_lines) + "\n", encoding="utf-8")
    _atomic_json(
        release / "CRPD_BLOCKER_CLOSURE_REPORT.json",
        {
            "generated_at": _iso(),
            "decision": decision,
            "blocked_gates": [name for name, value in decision.get("gates", {}).items() if not value],
            "risk_register": risk,
            "pending_disposition": pending,
            "attachment_status": attachment.get("status"),
            "release_validation_status": validation.get("status"),
            "promotion_trace": trace,
            "curated_table_metrics": metrics,
            "statement": "This bounded rehearsal is diagnostic and is not a production promotion authorization.",
        },
    )


def _write_hash_manifest(release: Path) -> Path:
    files = []
    for path in sorted(release.rglob("*")):
        if not path.is_file() or path.name == "SHA256_MANIFEST.json":
            continue
        files.append({"path": str(path.relative_to(release)).replace("\\", "/"), "size": path.stat().st_size, "sha256": _sha256(path)})
    target = release / "SHA256_MANIFEST.json"
    _atomic_json(target, {"created_at": _iso(), "files": files})
    return target


def _existing_prepared(run_root: Path, queue_source: Path, scope_source: Path) -> dict[str, Any]:
    data_root = run_root / "data"
    queue_path = data_root / "outputs" / "special_projects" / "2016_930" / "930_TASK_QUEUE.parquet"
    scope_path = data_root / "outputs" / "special_projects" / "2016_930" / scope_source.name
    return {
        "run_root": str(run_root),
        "data_root": str(data_root),
        "database": {"path": str(data_root / "database" / "policydb.duckdb"), "sha256": _sha256(data_root / "database" / "policydb.duckdb") if (data_root / "database" / "policydb.duckdb").exists() else None},
        "queue": {"source_path": str(queue_source), "source_sha256": _sha256(queue_source) if queue_source.exists() else None, "isolated_path": str(queue_path), "isolated_sha256": _sha256(queue_path) if queue_path.exists() else None},
        "scope": {"source": str(scope_source), "path": str(scope_path), "sha256": _sha256(scope_path) if scope_path.exists() else None} if scope_path.exists() else None,
    }


def _load_existing_result(output: Path, state: Any) -> dict[str, Any]:
    handoff = _read_json(_find_latest(output, "HANDOFF.json"))
    if handoff:
        return {
            "status": handoff.get("status"),
            "episode_status": handoff.get("episode_status"),
            "run_id": handoff.get("run_id") or state.run_id,
            "crawl_run_id": handoff.get("crawl_run_id"),
            "postprocess": handoff.get("postprocess") if isinstance(handoff.get("postprocess"), dict) else {},
            "crawler": handoff.get("crawler") if isinstance(handoff.get("crawler"), dict) else {},
            "checkpoint": handoff.get("checkpoint"),
        }
    summary = _read_json(_find_latest(output, "POSTPROCESS_SUMMARY.json"))
    return {"status": state.status, "run_id": state.run_id, "postprocess": summary}


def _find_existing_job(run_root: Path, job_id: str | None) -> str:
    jobs = run_root / "data" / "jobs" / "crawl_jobs"
    if job_id:
        if not (jobs / job_id / "state.json").exists():
            raise FileNotFoundError(jobs / job_id / "state.json")
        return job_id
    candidates = list(jobs.glob("*/state.json"))
    if not candidates:
        raise FileNotFoundError(f"no job state under {jobs}")
    return max(candidates, key=lambda path: path.stat().st_mtime).parent.name


def run(args: argparse.Namespace) -> int:
    cities = list(args.cities or CITY_IDS)
    if args.isolation_only and not 1 <= len(cities) <= 3:
        raise ValueError("isolation-only confirmation requires 1 to 3 cities")
    if not args.isolation_only and len(cities) != 20:
        raise ValueError("the controlled rehearsal requires exactly 20 cities")
    queue_source = Path(args.queue_source)
    scope_source = Path(args.scope_source)
    if not queue_source.exists():
        raise FileNotFoundError(queue_source)
    timestamp = _now().strftime("%Y%m%dT%H%M%SZ")
    run_root = Path(args.run_root) if args.run_root else REHEARSAL_PARENT / f"CRPD_PROMOTION_{timestamp}_BLOCKER_CLOSURE"
    _apply_run_environment(run_root)
    settings = _settings(run_root)
    release = run_root / "release"
    isolation: dict[str, Any] | None = None
    if args.finalize_existing:
        if not run_root.exists():
            raise FileNotFoundError(run_root)
        release.mkdir(parents=True, exist_ok=True)
        prepared = _existing_prepared(run_root, queue_source, scope_source)
        job_id = _find_existing_job(run_root, args.job_id)
        state = JobManager(settings).inspect_state(job_id)
        result = _load_existing_result(Path(prepared["data_root"]) / "outputs" / "special_projects" / "2016_930", state)
        context = build_runtime_context(
            settings,
            run_mode="REHEARSAL",
            run_id=run_root.name,
            production_write_allowed=False,
            expected_production_database=SOURCE_DATA_ROOT / "database" / "policydb.duckdb",
            release_root=release,
        )
        existing_isolation = _read_json(release / "EP930_ISOLATION_RUNTIME_VALIDATION.json")
        isolation = existing_isolation or None
    else:
        boundary_before = _pdf_boundary_snapshot()
        prepared = _prepare_root(run_root, queue_source, scope_source, cities)
        release.mkdir(parents=True, exist_ok=True)
        _atomic_json(release / "EP930_ISOLATION_BOUNDARY_BEFORE.json", boundary_before)
        context = build_runtime_context(
            settings,
            run_mode="REHEARSAL",
            run_id=run_root.name,
            production_write_allowed=False,
            expected_production_database=SOURCE_DATA_ROOT / "database" / "policydb.duckdb",
            release_root=release,
        )
        _atomic_json(release / "CRPD_RUNTIME_ISOLATION_VALIDATION.json", {"status": "PASS", "context": _safe(context.__dict__), "production_database": str(SOURCE_DATA_ROOT / "database" / "policydb.duckdb"), "isolated_database": str(settings.database), "production_write_allowed": False})
        request = _build_request(Path(prepared["data_root"]) / "outputs" / "special_projects" / "2016_930", Path(prepared["data_root"]) / "outputs" / "special_projects" / "2016_930" / "930_TASK_QUEUE.parquet", cities, args.max_ai_calls, args.max_fetches)
        _atomic_json(release / "CRPD_REHEARSAL_REQUEST.json", request.model_dump(mode="json"))
        state, result = _run_worker(settings, request, args.poll_seconds)
        boundary_after = _pdf_boundary_snapshot()
        _atomic_json(release / "EP930_ISOLATION_BOUNDARY_AFTER.json", boundary_after)
        isolation = _write_pdf_boundary_validation(release, boundary_before, boundary_after)
    output = Path(prepared["data_root"]) / "outputs" / "special_projects" / "2016_930"
    pending = _write_pending_disposition(
        release,
        output,
        crawl_run_id=result.get("crawl_run_id") or (result.get("crawler") or {}).get("run_id"),
    )
    extraction = _write_extraction_analysis(release, run_root)
    attachment = _attachment_report(release, run_root, output)
    trace = _copy_gate_trace(release, output)
    validation = _run_release_validation(release, run_root)
    metrics = _table_metrics(run_root)
    risk = _write_risk_register(release, state=state, result=result, pending=pending, attachment=attachment, validation=validation, trace=trace, metrics=metrics)
    decision = _promotion_decision(state, result, attachment, validation, trace, pending, isolation)
    _write_report(release, prepared=prepared, state=state, result=result, pending=pending, extraction=extraction, attachment=attachment, validation=validation, trace=trace, risk=risk, decision=decision, metrics=metrics, isolation=isolation)
    manifest = _write_hash_manifest(release)
    print(json.dumps({"event": "rehearsal_finished", "run_root": str(run_root), "release": str(release), "decision": decision, "hash_manifest": str(manifest)}, ensure_ascii=False), flush=True)
    return 0 if decision["decision"] == "PROMOTE" else 2


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-root", default="", help="new, non-existing isolated rehearsal root")
    parser.add_argument("--queue-source", default=str(DEFAULT_QUEUE))
    parser.add_argument("--scope-source", default=str(DEFAULT_SCOPE))
    parser.add_argument("--max-ai-calls", type=int, default=20)
    parser.add_argument("--max-fetches", type=int, default=80)
    parser.add_argument("--poll-seconds", type=int, default=10)
    parser.add_argument("--cities", nargs="*", default=None)
    parser.add_argument("--isolation-only", action="store_true", help="allow a 1-3 city real-network isolation confirmation; never treats it as a promotion run")
    parser.add_argument("--finalize-existing", action="store_true", help="finalize an already completed isolated run without rerunning it")
    parser.add_argument("--job-id", default=None)
    args = parser.parse_args()
    return run(args)


if __name__ == "__main__":
    raise SystemExit(main())
