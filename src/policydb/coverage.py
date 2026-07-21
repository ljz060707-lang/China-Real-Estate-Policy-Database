from __future__ import annotations

import json
from datetime import UTC, date, datetime

import polars as pl

from policydb.crawl.checkpoint import append_unique
from policydb.crawl.registry import load_registry
from policydb.scope import load_cities_105
from policydb.settings import Settings
from policydb.transform.normalization import stable_id

COVERAGE_STATUSES = {
    "not_scanned",
    "partial",
    "failed",
    "complete_policy_found",
    "complete_confirmed_zero",
}


def build_source_matrix(settings: Settings | None = None) -> pl.DataFrame:
    settings = settings or Settings.discover()
    curated_cities = settings.curated / "cities_105.parquet"
    cities = pl.read_parquet(curated_cities) if curated_cities.exists() else load_cities_105(settings)
    rows: list[dict] = []
    for source in load_registry(settings):
        if not source.is_valid:
            continue
        city_ids = source.city_ids
        if source.scope_type == "national":
            city_ids = cities["city_id"].to_list()
        elif source.scope_type == "provincial" and source.province_codes:
            province_column = "province_code" if "province_code" in cities.columns else None
            if province_column:
                city_ids = cities.filter(pl.col(province_column).is_in(source.province_codes))["city_id"].to_list()
        for city_id in city_ids:
            rows.append(
                {
                    "source_id": source.source_id,
                    "source_name": source.source_name,
                    "city_id": city_id,
                    "scope_type": source.scope_type,
                    "agency_type": source.agency_type,
                    "required_level": source.required_level,
                    "crawl_enabled": source.crawl_enabled,
                    "coverage_start_date": source.coverage_start_date,
                    "coverage_end_date": source.coverage_end_date,
                    "expected_frequency": source.expected_frequency,
                }
            )
    schema = {
        "source_id": pl.String, "source_name": pl.String, "city_id": pl.String,
        "scope_type": pl.String, "agency_type": pl.String, "required_level": pl.String,
        "crawl_enabled": pl.Boolean, "coverage_start_date": pl.Date,
        "coverage_end_date": pl.Date, "expected_frequency": pl.String,
    }
    return pl.DataFrame(rows, schema=schema, infer_schema_length=None)


def record_source_window(
    *, run_id: str, source_id: str, period_start: date, period_end: date,
    scan_method: str, candidate_count: int, fetched_count: int, policy_count: int,
    error_count: int, page_count: int = 0, city_id: str | None = None,
    completion_evidence: dict | None = None, settings: Settings | None = None,
) -> dict:
    settings = settings or Settings.discover()
    evidence = completion_evidence or {}
    complete = error_count == 0 and page_count >= 1 and evidence.get("exhaustive") is True
    if not complete:
        status = "failed" if error_count and fetched_count == 0 else "partial"
    elif policy_count:
        status = "complete_policy_found"
    else:
        status = "complete_confirmed_zero"
    now = datetime.now(UTC).isoformat()
    row = {
        "window_id": stable_id(source_id, city_id or "", period_start.isoformat(), period_end.isoformat(), scan_method, prefix="WINDOW"),
        "run_id": run_id, "source_id": source_id, "city_id": city_id,
        "period_start": period_start.isoformat(), "period_end": period_end.isoformat(),
        "scan_method": scan_method, "coverage_status": status,
        "candidate_count": candidate_count, "fetched_count": fetched_count,
        "policy_count": policy_count, "error_count": error_count, "page_count": page_count,
        "is_complete": complete,
        "completion_evidence": json.dumps(evidence, ensure_ascii=False, sort_keys=True),
        "started_at": now, "finished_at": now, "created_at": now, "updated_at": now,
    }
    append_unique(settings.curated / "crawl_source_windows.parquet", [row], "window_id")
    return row


def coverage_summary(settings: Settings | None = None) -> dict:
    settings = settings or Settings.discover()
    matrix = build_source_matrix(settings)
    windows_path = settings.curated / "crawl_source_windows.parquet"
    windows = pl.read_parquet(windows_path) if windows_path.exists() else pl.DataFrame()
    return {
        "matrix_rows": matrix.height,
        "covered_cities": matrix["city_id"].n_unique() if matrix.height else 0,
        "window_count": windows.height,
        "complete_windows": windows.filter(pl.col("is_complete")).height if windows.height else 0,
    }
