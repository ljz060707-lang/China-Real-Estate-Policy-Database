from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import polars as pl

import policydb.dashboard_live_state as dashboard_live_state
from policydb.dashboard_formatting import (
    format_datetime,
    format_percentage,
    format_status,
    format_value,
)
from policydb.dashboard_live_state import (
    DashboardSnapshot,
    _automation_live_state,
    _database_error_status,
    _read_frame,
    clear_dashboard_caches,
    load_dashboard_snapshot,
)
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
    event_at = datetime.now(UTC).isoformat()
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
                "created_at": event_at,
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
    automation = settings.data_root / "automation"
    automation.mkdir(parents=True, exist_ok=True)
    (automation / "MASTER_STATE.json").write_text(
        json.dumps(
            {
                "automation_id": "AUTO_TEST",
                "status": "RUNNING",
                "stage": "CRAWL",
                "run_id": "RUN_MASTER",
                "worker_pid": 1234,
                "next_stage": "NORMALIZE",
                "last_heartbeat_at": event_at,
                "current_run_active": False,
            }
        ),
        encoding="utf-8",
    )


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
    assert snapshot.crawler["stage"] == "CRAWL"
    assert snapshot.crawler["run_id"] == "RUN_MASTER"
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


def test_master_state_hides_stale_event_as_current_position() -> None:
    now = datetime(2026, 8, 10, 12, 0, tzinfo=UTC)
    master = {
        "automation_id": "AUTO_TEST",
        "status": "READY_FOR_NEXT_STAGE",
        "stage": "CRAWL_AGAIN",
        "run_id": "RUN_CURRENT",
        "worker_pid": None,
        "next_stage": "COVERAGE_AUDIT",
        "last_heartbeat_at": "2026-08-10T11:59:00+00:00",
        "current_run_active": False,
    }
    latest = {
        "batch_id": "B1",
        "shard_id": "S1",
        "shard_city_name": "南京市",
        "shard_source_role": "housing_department",
        "shard_start_date": "2018-01-01",
        "shard_end_date": "2018-01-31",
        "created_at": (now - timedelta(minutes=10)).isoformat(),
    }
    state = _automation_live_state(master, latest, now=now)
    assert state["status"] == "READY_FOR_NEXT_STAGE"
    assert state["stage"] == "CRAWL_AGAIN"
    assert state["current_position_available"] is False
    assert state["current_position"] == {}
    assert state["last_crawl_position"]["city_name"] == "南京市"
    assert state["heartbeat_age_seconds"] == 60


def test_master_stage_is_authoritative_over_fresh_non_crawl_event() -> None:
    now = datetime(2026, 8, 10, 12, 0, tzinfo=UTC)
    state = _automation_live_state(
        {
            "status": "READY_FOR_NEXT_STAGE",
            "stage": "ARCHIVE",
            "run_id": "RUN_ARCHIVE",
            "last_heartbeat_at": now.isoformat(),
        },
        {
            "stage": "discovering",
            "shard_city_name": "南京市",
            "created_at": now.isoformat(),
        },
        now=now,
    )
    assert state["stage"] == "ARCHIVE"
    assert state["current_position_available"] is False
    assert state["last_crawl_position"]["city_name"] == "南京市"


def test_parquet_read_retries_an_atomic_replace_before_reporting_unavailable(
    monkeypatch, tmp_path: Path
) -> None:
    settings = _settings(tmp_path)
    path = settings.curated / "records.parquet"
    pl.DataFrame([{"record_id": "R1"}]).write_parquet(path)
    calls = {"count": 0}
    original = dashboard_live_state._cached_parquet

    def flaky_read(path_text, mtime_ns, size, columns):
        calls["count"] += 1
        if calls["count"] == 1:
            raise OSError("file replaced during read")
        return original(path_text, mtime_ns, size, columns)

    monkeypatch.setattr(dashboard_live_state, "_cached_parquet", flaky_read)
    frame, meta = _read_frame(settings, "records")

    assert calls["count"] == 2
    assert meta["status"] == "available"
    assert frame[0, "record_id"] == "R1"


def test_snapshot_serves_last_good_when_a_refresh_fails(monkeypatch, tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    clear_dashboard_caches()
    first = DashboardSnapshot(
        generated_at="2026-08-10T15:00:00+00:00",
        data_root=str(settings.data_root),
        database_path=str(settings.database),
        system={"database": {"status": "HEALTHY"}},
        frames={"records": pl.DataFrame([{"record_id": "R1"}])},
        availability={"records": {"status": "available"}},
    )
    monkeypatch.setattr(
        dashboard_live_state,
        "_build_dashboard_snapshot",
        lambda _settings, event_limit=20: first,
    )
    assert load_dashboard_snapshot(settings).system["snapshot_status"] == "FRESH"

    def fail_build(_settings, event_limit=20):
        raise RuntimeError("temporary schema refresh")

    monkeypatch.setattr(dashboard_live_state, "_build_dashboard_snapshot", fail_build)
    fallback = load_dashboard_snapshot(settings)

    assert fallback.frames["records"].height == 1
    assert fallback.system["snapshot_status"] == "LAST_GOOD"
    assert fallback.availability["snapshot"]["status"] == "last_good"


def test_snapshot_keeps_records_when_versions_and_gaps_are_unavailable(
    monkeypatch, tmp_path: Path
) -> None:
    settings = _settings(tmp_path)
    _write_live_fixture(settings)
    original_read = dashboard_live_state._read_frame

    def read_with_missing_auxiliary(settings_arg, name, *, columns=None):
        if name == "policy_document_versions":
            return pl.DataFrame(), {
                "path": str(settings_arg.curated / "policy_document_versions.parquet"),
                "status": "unavailable",
                "error_type": "temporary_replace",
            }
        if name == "coverage_gaps":
            return pl.DataFrame(), {
                "path": str(settings_arg.curated / "coverage_gaps.parquet"),
                "status": "unavailable",
                "error_type": "temporary_replace",
            }
        return original_read(settings_arg, name, columns=columns)

    monkeypatch.setattr(dashboard_live_state, "_read_frame", read_with_missing_auxiliary)
    snapshot = load_dashboard_snapshot(settings)

    assert snapshot.documents["records"] == 1
    assert snapshot.documents["document_versions"] is None
    assert snapshot.coverage["open_gaps"] is None
    assert snapshot.coverage["critical_gaps"] is None
