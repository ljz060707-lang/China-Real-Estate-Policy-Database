from __future__ import annotations

from pathlib import Path

import polars as pl

from policydb.dashboard_formatting import (
    format_datetime,
    format_percentage,
    format_status,
    format_value,
)
from policydb.dashboard_live_state import _database_error_status, load_dashboard_snapshot
from policydb.settings import Settings


def _settings(tmp_path: Path) -> Settings:
    curated = tmp_path / "data" / "curated"
    curated.mkdir(parents=True)
    return Settings(
        root=tmp_path,
        curated_path=curated,
        database_path=tmp_path / "data" / "database" / "policydb.duckdb",
    )


def _write_live_fixture(settings: Settings, *, city_count: int = 1) -> None:
    slot_rows = []
    roles = (
        "municipal_government",
        "government_gazette",
        "housing_department",
        "natural_resources_department",
        "provident_fund_center",
    )
    for city_index in range(city_count):
        city_id = f"CITY_{city_index:06d}"
        for role_index, role in enumerate(roles):
            slot_rows.append(
                {
                    "slot_id": f"SLOT_{city_index}_{role_index}",
                    "city_id": city_id,
                    "city_name": f"城市{city_index}",
                    "province_name": "测试省",
                    "source_role": role,
                    "status": "enabled",
                    "candidate_count": 1,
                    "verified_candidate_count": 1,
                    "enabled_source_count": 1,
                    "enabled_pending_verification_count": 0,
                }
            )
    pl.DataFrame(slot_rows).write_parquet(settings.curated / "source_requirement_slots.parquet")
    pl.DataFrame(
        [
            {
                "event_id": "E1",
                "batch_id": "B1",
                "shard_id": "SHARD_1",
                "stage": "discovering",
                "message": "城市0 2018-01",
                "counts_json": "{}",
                "created_at": "2026-08-10T02:45:44+00:00",
            }
        ]
    ).write_parquet(settings.curated / "pipeline_progress_events.parquet")
    pl.DataFrame(
        [
            {
                "shard_id": "SHARD_1",
                "batch_id": "B1",
                "city_id": "CITY_000000",
                "city_name": "城市0",
                "source_id": "SRC_1",
                "source_role": "municipal_government",
                "start_date": "2018-01-01",
                "end_date": "2018-01-31",
                "status": "pending",
                "fetched": 0,
                "failed": 0,
            }
        ]
    ).write_parquet(settings.curated / "crawl_shards.parquet")
    pl.DataFrame(
        [
            {
                "record_id": "R1",
                "record_date": None,
                "title": None,
                "full_text": None,
                "primary_source_url": None,
                "content_hash": None,
                "official_status": "official",
                "manual_review_status": "pending",
                "updated_at": "2026-08-10T02:45:44+00:00",
            }
        ]
    ).write_parquet(settings.curated / "records.parquet")
    pl.DataFrame(
        [
            {
                "record_id": "R1",
                "city_id": "CITY_000000",
                "city_name": "城市0",
                "province_name": "测试省",
            }
        ]
    ).write_parquet(settings.curated / "record_geographies_normalized.parquet")


def test_formatting_preserves_unknown_and_missing_semantics() -> None:
    assert format_datetime("2026-08-10T02:45:44+00:00") == "2026-08-10 10:45:44"
    assert format_status("discovering") == "正在发现政策链接"
    assert format_status("NEW_INTERNAL_STATE") == "未识别状态"
    assert format_value(None) == "暂无数据"
    assert format_percentage(1, 4) == "1 / 4（25.0%）"
    assert format_percentage(0, None) == "暂无数据"


def test_snapshot_joins_latest_event_to_current_shard(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    _write_live_fixture(settings)
    snapshot = load_dashboard_snapshot(settings)
    assert snapshot.crawler["city_name"] == "城市0"
    assert snapshot.crawler["start_date"] == "2018-01-01"
    assert snapshot.crawler["stage"] == "discovering"
    assert snapshot.frames["recent_events"].height == 1
    assert snapshot.documents["records"] == 1
    assert snapshot.quality["missing_title"] == 1
    assert snapshot.system["database"]["status"] == "CURATED_FALLBACK"
    assert snapshot.system["database"]["formal_status"] == "QUERY_UNAVAILABLE"


def test_database_health_distinguishes_stale_index_from_active_write() -> None:
    stale = OSError(
        'IO Error: No files found that match the pattern "D:/Data Set/CRPD/curated/records.parquet"'
    )
    locked = OSError("database file is being used by another process: lock conflict")
    unknown = OSError("TLS storage adapter failed")

    assert _database_error_status(stale) == "INDEX_REFRESH_PENDING"
    assert _database_error_status(locked) == "DATABASE_UPDATING"
    assert _database_error_status(unknown) == "QUERY_UNAVAILABLE"


def test_snapshot_missing_interfaces_degrade_without_zero_fabrication(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    snapshot = load_dashboard_snapshot(settings)
    assert snapshot.system["database"]["queryable"] is False
    assert snapshot.crawler["city_name"] is None
    assert snapshot.coverage["total_slots"] is None
    assert snapshot.coverage["metrics"]["verified_slots"].value is None
    assert snapshot.availability["pipeline_progress_events"]["status"] == "unavailable"
    assert snapshot.availability["automation/MASTER_STATE"]["status"] == "unavailable"


def test_snapshot_uses_105_city_and_525_slot_denominators(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    _write_live_fixture(settings, city_count=105)
    snapshot = load_dashboard_snapshot(settings)
    assert snapshot.frames["source_slots"].height == 525
    assert snapshot.coverage["total_slots"] == 525
    assert snapshot.coverage["verified_slots"] == 525
    assert snapshot.coverage["metrics"]["city_live_progress"].denominator == 105
