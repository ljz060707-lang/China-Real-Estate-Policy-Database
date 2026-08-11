from __future__ import annotations

from datetime import date

import polars as pl

import policydb.dashboard_policy_data as dashboard_policy_data
from policydb.dashboard_policy_data import DashboardPolicyData
from policydb.settings import Settings


def _settings(tmp_path):
    curated = tmp_path / "data" / "curated"
    curated.mkdir(parents=True)
    pl.DataFrame(
        [
            {
                "record_id": "POLICY_1",
                "record_date": date(2024, 1, 2),
                "title": "测试政策",
                "summary": "测试摘要",
                "official_status": "official",
                "manual_review_status": "approved",
                "primary_source_url": "https://example.gov.cn/policy/1",
            }
        ]
    ).write_parquet(curated / "records.parquet")
    pl.DataFrame(
        [
            {
                "record_id": "POLICY_1",
                "province_name": "测试省",
                "city_name": "测试市",
                "county_name": None,
            }
        ]
    ).write_parquet(curated / "record_geographies_normalized.parquet")
    return Settings(
        root=tmp_path,
        curated_path=curated,
        database_path=tmp_path / "data" / "database" / "policydb.duckdb",
    )


def test_filter_options_falls_back_when_duckdb_auxiliary_query_has_schema_error(
    monkeypatch, tmp_path
):
    settings = _settings(tmp_path)
    monkeypatch.setattr(
        dashboard_policy_data,
        "database_health",
        lambda _settings: {
            "status": "HEALTHY",
            "queryable": True,
            "fallback_available": True,
        },
    )
    monkeypatch.setattr(dashboard_policy_data, "PolicyDB", lambda _settings: object())

    def fail_filter_options(_db):
        raise RuntimeError("Conversion Error: record_id VARCHAR to INT32")

    monkeypatch.setattr(dashboard_policy_data, "duckdb_filter_options", fail_filter_options)

    service = DashboardPolicyData(settings)
    options = service.filter_options()

    assert service.mode == "duckdb"
    assert options["provinces"] == ["测试省"]
    assert options["cities"] == ["测试市"]
    assert service.query_failures[0]["operation"] == "filter_options"


def test_policy_search_and_detail_keep_using_curated_records_after_duckdb_failure(
    monkeypatch, tmp_path
):
    settings = _settings(tmp_path)
    monkeypatch.setattr(
        dashboard_policy_data,
        "database_health",
        lambda _settings: {
            "status": "HEALTHY",
            "queryable": True,
            "fallback_available": True,
        },
    )
    monkeypatch.setattr(dashboard_policy_data, "PolicyDB", lambda _settings: object())
    monkeypatch.setattr(
        dashboard_policy_data,
        "duckdb_policy_list",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("stale view")),
    )
    monkeypatch.setattr(
        dashboard_policy_data,
        "duckdb_policy_detail",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("stale view")),
    )

    service = DashboardPolicyData(settings)
    rows, total = service.search(
        {"start_date": date(2018, 1, 1), "end_date": date.today()},
        page=1,
        page_size=20,
    )
    detail = service.detail("POLICY_1")

    assert total == 1
    assert rows.height == 1
    assert detail["policy"]["record_id"] == "POLICY_1"
    assert service.display_mode == "mixed"
    assert {row["operation"] for row in service.query_failures} == {"search", "detail"}
