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


def build_city_source_month_coverage(
    settings: Settings | None = None,
    *,
    start: date = date(2018, 1, 1),
    end: date | None = None,
) -> dict:
    settings = settings or Settings.discover()
    end = end or date.today()
    cities = (
        pl.read_parquet(settings.curated / "cities_105.parquet")
        if (settings.curated / "cities_105.parquet").exists()
        else load_cities_105(settings)
    )
    source_matrix = build_source_matrix(settings)
    roles = {
        "government_gazette": {"central_government", "local_government"},
        "housing_department": {"housing"},
        "provident_fund": {"provident_fund"},
        "natural_resources": {"natural_resources"},
    }
    source_lookup: dict[tuple[str, str], list[str]] = {}
    for row in source_matrix.iter_rows(named=True):
        role = next(
            (
                name
                for name, agency_types in roles.items()
                if row.get("agency_type") in agency_types
            ),
            None,
        )
        if role:
            source_lookup.setdefault((row["city_id"], role), []).append(row["source_id"])
    windows_path = settings.curated / "crawl_source_windows.parquet"
    windows = pl.read_parquet(windows_path) if windows_path.exists() else pl.DataFrame()
    window_lookup = {}
    for row in windows.iter_rows(named=True):
        month = str(row.get("period_start") or "")[:7]
        window_lookup[(row.get("city_id"), row.get("source_id"), month)] = row
    months = []
    current = start.replace(day=1)
    final = end.replace(day=1)
    while current <= final:
        months.append(current.strftime("%Y-%m"))
        current = (
            date(current.year + 1, 1, 1)
            if current.month == 12
            else date(current.year, current.month + 1, 1)
        )
    rows = []
    for city in cities.iter_rows(named=True):
        for role in roles:
            source_ids = source_lookup.get((city["city_id"], role), [])
            for month in months:
                matched = [
                    window_lookup[(city["city_id"], source_id, month)]
                    for source_id in source_ids
                    if (city["city_id"], source_id, month) in window_lookup
                ]
                complete = [item for item in matched if item.get("is_complete")]
                status = (
                    "complete_policy_found"
                    if any(item.get("policy_count", 0) > 0 for item in complete)
                    else "complete_confirmed_zero"
                    if complete
                    else "failed"
                    if matched and all(item.get("coverage_status") == "failed" for item in matched)
                    else "partial"
                    if matched
                    else "not_scanned"
                )
                rows.append(
                    {
                        "city_id": city["city_id"],
                        "city_name": city["city_name"],
                        "province_name": city["province_name"],
                        "source_role": role,
                        "month": month,
                        "registered_source_count": len(source_ids),
                        "coverage_status": status,
                        "complete_window_count": len(complete),
                        "policy_count": sum(int(item.get("policy_count", 0)) for item in complete),
                    }
                )
    frame = pl.DataFrame(rows)
    output = settings.root / "outputs/coverage"
    output.mkdir(parents=True, exist_ok=True)
    frame.write_csv(output / "city_source_month_coverage.csv")
    gaps = frame.filter(
        pl.col("coverage_status").is_in(["not_scanned", "partial", "failed"])
    )
    summary = (
        gaps.group_by(["city_name", "province_name"])
        .agg(
            pl.len().alias("gap_cells"),
            pl.col("registered_source_count").sum().alias("registered_source_links"),
        )
        .sort("gap_cells", descending=True)
    )
    (output / "105_city_gap_report.md").write_text(
        "# 105 城来源—月份缺口\n\n"
        f"- 范围：{months[0]} 至 {months[-1]}\n"
        f"- 覆盖单元：{frame.height}\n"
        f"- 完整窗口：{frame.filter(pl.col('coverage_status').str.starts_with('complete_')).height}\n"
        f"- 未扫描/部分/失败：{gaps.height}\n"
        f"- 已登记至少一个核心来源的城市："
        f"{frame.filter(pl.col('registered_source_count')>0)['city_id'].n_unique()}\n\n"
        "未扫描、部分扫描和失败均不计为零政策。\n\n"
        + summary.head(105).write_csv(),
        encoding="utf-8",
    )
    return {
        "coverage_cells": frame.height,
        "complete_cells": frame.filter(
            pl.col("coverage_status").str.starts_with("complete_")
        ).height,
        "gap_cells": gaps.height,
        "cities_with_core_source": frame.filter(
            pl.col("registered_source_count") > 0
        )["city_id"].n_unique(),
    }
