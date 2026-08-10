"""Read-only validation and atomic publication for a private DuckDB candidate."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any
from uuid import uuid4

import duckdb

from policydb.query.database import build_database
from policydb.settings import Settings

OLD_D_ROOT_MARKER = "d:/data set/crpd"

_COUNT_RELATIONS = {
    "records": ("records",),
    "policy_document_versions": ("policy_document_versions",),
    "crawl_items": ("crawl_items",),
    "source_sync_state": ("source_sync_state",),
    "geographies": ("geographies", "record_geographies_normalized", "record_jurisdictions"),
    "source_slots": ("source_requirement_slots", "v_source_requirement_slots"),
}


def _json_safe(value: Any) -> Any:
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    return str(value)


def _sql_path(path: Path) -> str:
    return str(path).replace("\\", "/").replace("'", "''")


def _available_relations(connection: duckdb.DuckDBPyConnection) -> set[str]:
    tables = connection.execute(
        """SELECT table_name FROM information_schema.tables
           WHERE table_schema='main'"""
    ).fetchall()
    views = connection.execute(
        """SELECT view_name FROM duckdb_views()
           WHERE schema_name='main' AND NOT internal"""
    ).fetchall()
    return {str(row[0]) for row in tables} | {str(row[0]) for row in views}


def _choose_relation(available: set[str], candidates: tuple[str, ...]) -> str | None:
    return next((name for name in candidates if name in available), None)


def _missing_query(name: str) -> dict[str, Any]:
    return {
        "name": name,
        "relation": None,
        "sql": None,
        "ok": False,
        "required": True,
        "error_type": "RequiredRelationMissing",
        "error": f"Required relation is missing: {name}",
    }


def _run_query(
    connection: duckdb.DuckDBPyConnection,
    name: str,
    sql: str,
    *,
    relation: str | None = None,
    limit: int = 10,
) -> dict[str, Any]:
    try:
        result = connection.execute(sql)
        columns = [item[0] for item in result.description]
        rows = result.fetchmany(limit)
        return {
            "name": name,
            "relation": relation,
            "sql": sql,
            "ok": True,
            "columns": columns,
            "rows": [_json_safe(row) for row in rows],
        }
    except Exception as exc:  # DuckDB errors are part of the health result.
        return {
            "name": name,
            "relation": relation,
            "sql": sql,
            "ok": False,
            "error_type": type(exc).__name__,
            "error": str(exc),
        }


def _scan_views(connection: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    rows = connection.execute(
        """SELECT view_name, sql FROM duckdb_views()
           WHERE schema_name='main' AND NOT internal
           ORDER BY view_name"""
    ).fetchall()
    views = []
    old_path_references = []
    sql_by_name: dict[str, str] = {}
    for view_name, sql in rows:
        name = str(view_name)
        sql_text = str(sql or "")
        normalized = sql_text.replace("\\", "/").lower()
        entry = {"view_name": name, "sql": sql_text}
        sql_by_name[name] = sql_text
        if OLD_D_ROOT_MARKER in normalized:
            old_path_references.append(entry)
        views.append(entry)
    return {
        "ok": not old_path_references,
        "view_count": len(views),
        "views": views,
        "sql_by_name": sql_by_name,
        "old_d_root_references": old_path_references,
    }


def _curated_parquet(label: str, curated_path: Path) -> Path:
    if label == "geographies":
        for name in ("geographies", "record_geographies_normalized", "record_jurisdictions"):
            path = curated_path / f"{name}.parquet"
            if path.exists():
                return path
    if label == "source_slots":
        return curated_path / "source_requirement_slots.parquet"
    return curated_path / f"{label}.parquet"


def _curated_consistency(
    connection: duckdb.DuckDBPyConnection,
    curated_path: Path | None,
    available: set[str],
) -> dict[str, Any]:
    if curated_path is None:
        return {"available": False, "reason": "curated_path_not_supplied", "checks": {}}

    checks: dict[str, Any] = {}
    for label, candidates in _COUNT_RELATIONS.items():
        relation = _choose_relation(available, candidates)
        parquet = _curated_parquet(label, curated_path)
        if relation is None or not parquet.exists():
            checks[label] = {
                "relation": relation,
                "parquet": str(parquet),
                "available": False,
                "reason": "relation_or_parquet_missing",
            }
            continue
        try:
            candidate_count = int(connection.execute(f"SELECT count(*) FROM {relation}").fetchone()[0])
            curated_count = int(
                connection.execute(
                    f"SELECT count(*) FROM read_parquet('{_sql_path(parquet)}')"
                ).fetchone()[0]
            )
            checks[label] = {
                "relation": relation,
                "parquet": str(parquet),
                "available": True,
                "candidate_count": candidate_count,
                "curated_count": curated_count,
                "matches": candidate_count == curated_count,
            }
        except Exception as exc:
            checks[label] = {
                "relation": relation,
                "parquet": str(parquet),
                "available": True,
                "matches": False,
                "error_type": type(exc).__name__,
                "error": str(exc),
            }
    return {
        "available": True,
        "checks": checks,
        "all_available_checks_match": all(
            item.get("matches", True) for item in checks.values() if item.get("available")
        ),
    }


def _audit_entry(
    label: str,
    query: dict[str, Any],
    consistency: dict[str, Any],
    *,
    database_path: Path,
    curated_path: Path | None,
    view_sql: dict[str, str],
) -> dict[str, Any]:
    rows = query.get("rows") or []
    duckdb_count = int(rows[0][0]) if query.get("ok") and rows and rows[0] else None
    check = (consistency.get("checks") or {}).get(label, {})
    curated_count = check.get("curated_count")
    difference = (
        duckdb_count - int(curated_count)
        if duckdb_count is not None and curated_count is not None
        else None
    )
    relation = query.get("relation")
    sql = view_sql.get(str(relation)) if relation else None
    uses_external = bool(sql and "read_parquet" in sql.lower())
    if not query.get("ok"):
        status = "MISSING_REQUIRED_RELATION" if query.get("error_type") == "RequiredRelationMissing" else "QUERY_FAILED"
    elif difference not in (None, 0):
        status = "CURATED_COUNT_MISMATCH"
    else:
        status = "MATCH"
    return {
        "object": label,
        "duckdb_count": duckdb_count,
        "curated_count": curated_count,
        "difference": difference,
        "query_ok": bool(query.get("ok")),
        "external_source": str(_curated_parquet(label, curated_path)) if curated_path else None,
        "expected_root": str(curated_path) if curated_path else None,
        "actual_root": str(curated_path) if uses_external and curated_path else str(database_path),
        "status": status,
        "relation": relation,
        "required": True,
    }


def validate_database_interface(
    database_path: str | Path,
    *,
    curated_path: str | Path | None = None,
) -> dict[str, Any]:
    """Validate a database file without opening it for writes."""

    database_path = Path(database_path)
    curated = Path(curated_path) if curated_path is not None else None
    result: dict[str, Any] = {
        "database_path": str(database_path),
        "curated_path": str(curated) if curated else None,
        "file_exists": database_path.is_file(),
        "connect": {"ok": False},
        "query_ok": False,
        "view_scan": {"ok": False, "view_count": 0, "old_d_root_references": []},
        "representative_queries": {},
        "curated_consistency": {"available": False, "checks": {}},
        "DATABASE_INTERFACE_VALIDATION": [],
    }
    if not result["file_exists"]:
        result["status"] = "missing_file"
        result["passed"] = False
        return result

    try:
        connection = duckdb.connect(str(database_path), read_only=True)
    except Exception as exc:
        result["connect"] = {
            "ok": False,
            "error_type": type(exc).__name__,
            "error": str(exc),
        }
        result["status"] = "connect_failed"
        result["passed"] = False
        return result

    result["connect"] = {"ok": True}
    try:
        available = _available_relations(connection)
        count_queries: dict[str, dict[str, Any]] = {}
        for label, candidates in _COUNT_RELATIONS.items():
            relation = _choose_relation(available, candidates)
            if relation is None:
                count_queries[label] = _missing_query(label)
            else:
                count_queries[label] = _run_query(
                    connection,
                    label,
                    f"SELECT count(*) FROM {relation}",
                    relation=relation,
                    limit=1,
                )

        if "v_policy_master" in available:
            policy_list_sql = (
                "SELECT record_id,title,record_date FROM v_policy_master "
                "ORDER BY record_date DESC NULLS LAST LIMIT 5"
            )
            policy_detail_sql = "SELECT * FROM v_policy_master LIMIT 1"
        else:
            policy_list_sql = "SELECT record_id,title,record_date FROM records LIMIT 5"
            policy_detail_sql = "SELECT * FROM records LIMIT 1"
        representative = {
            **count_queries,
            "policy_list": _run_query(connection, "policy_list", policy_list_sql),
            "policy_detail": _run_query(connection, "policy_detail", policy_detail_sql),
            "date_range": _run_query(
                connection,
                "date_range",
                "SELECT min(record_date),max(record_date) FROM records",
                limit=1,
            ),
            "source_summary": _run_query(
                connection,
                "source_summary",
                "SELECT count(DISTINCT primary_source_url) FROM records",
                limit=1,
            ),
            "quality_summary": _run_query(
                connection,
                "quality_summary",
                "SELECT * FROM v_data_quality",
                limit=1,
            ),
        }
        geography_relation = _choose_relation(available, _COUNT_RELATIONS["geographies"])
        if geography_relation is None:
            representative["city_summary"] = _missing_query("city_summary")
        else:
            city_column = "jurisdiction_name" if geography_relation == "record_jurisdictions" else "city_name"
            representative["city_summary"] = _run_query(
                connection,
                "city_summary",
                f"SELECT count(DISTINCT {city_column}) FROM {geography_relation}",
                relation=geography_relation,
                limit=1,
            )
        result["representative_queries"] = representative
        result["view_scan"] = _scan_views(connection)
        result["curated_consistency"] = _curated_consistency(connection, curated, available)
        result["DATABASE_INTERFACE_VALIDATION"] = [
            _audit_entry(
                label,
                representative[label],
                result["curated_consistency"],
                database_path=database_path,
                curated_path=curated,
                view_sql=result["view_scan"].get("sql_by_name", {}),
            )
            for label in _COUNT_RELATIONS
        ]
    finally:
        connection.close()

    result["query_ok"] = all(
        item.get("ok", False) for item in result["representative_queries"].values()
    )
    consistency_ok = result["curated_consistency"].get("all_available_checks_match", True)
    view_ok = result["view_scan"].get("ok", False)
    result["status"] = (
        "healthy"
        if result["query_ok"] and consistency_ok and view_ok
        else "old_path_reference"
        if not view_ok
        else "query_unavailable"
        if not result["query_ok"]
        else "curated_mismatch"
    )
    result["passed"] = bool(result["query_ok"] and consistency_ok and view_ok)
    return result


def default_candidate_path(settings: Settings) -> Path:
    return settings.database_root / "policydb_interface_candidate.duckdb"


def build_candidate_database(
    settings: Settings | None = None,
    *,
    candidate_path: str | Path | None = None,
) -> dict[str, Any]:
    """Build to a private same-directory temporary DB, then publish atomically."""

    settings = settings or Settings.discover()
    candidate = Path(candidate_path) if candidate_path is not None else default_candidate_path(settings)
    candidate = candidate.expanduser()
    if candidate.resolve() == settings.database.resolve():
        raise ValueError("Candidate database must not equal the configured production database")

    candidate.parent.mkdir(parents=True, exist_ok=True)
    temporary = candidate.with_name(f".{candidate.name}.{uuid4().hex}.tmp.duckdb")
    try:
        candidate_settings = settings.model_copy(update={"database_path": temporary})
        build_database(candidate_settings, materialize_geography=False)
        validation = validate_database_interface(temporary, curated_path=settings.curated)
        validation.update(
            {
                "candidate_path": str(candidate),
                "temporary_path": str(temporary),
                "production_database": str(settings.database),
                "candidate_replaced": False,
                "production_database_touched": False,
            }
        )
        if not validation["passed"]:
            return validation
        os.replace(temporary, candidate)
        validation["candidate_replaced"] = True
        return validation
    finally:
        temporary.unlink(missing_ok=True)


build_and_validate_candidate_database = build_candidate_database


def database_switch_blockers(
    *,
    crawler_writer_active: bool,
    legacy_supervisor_writer_active: bool,
    checkpoint_safe: bool,
    candidate_validation_passed: bool,
    dashboard_smoke_passed: bool,
) -> list[str]:
    """Return fail-closed blockers for publishing a candidate as the formal DB.

    Process discovery stays outside this pure gate so callers must provide a
    fresh production snapshot instead of relying on a stale PID.  This helper
    performs no filesystem or database mutation.
    """

    blockers: list[str] = []
    if crawler_writer_active:
        blockers.append("ACTIVE_CRAWLER_WRITER")
    if legacy_supervisor_writer_active:
        blockers.append("ACTIVE_LEGACY_SUPERVISOR_WRITER")
    if not checkpoint_safe:
        blockers.append("CHECKPOINT_NOT_SAFE")
    if not candidate_validation_passed:
        blockers.append("CANDIDATE_VALIDATION_FAILED")
    if not dashboard_smoke_passed:
        blockers.append("DASHBOARD_SMOKE_FAILED")
    return blockers
