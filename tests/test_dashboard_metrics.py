from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl

from policydb.dashboard_metrics import (
    city_role_matrix,
    city_year_coverage,
    document_quality,
    gold_placeholder,
    overview_metrics,
)
from policydb.settings import Settings


def _settings(tmp_path: Path) -> Settings:
    curated = tmp_path / "curated"
    curated.mkdir()
    return Settings(root=tmp_path, curated_path=curated)


def _write_fixture(settings: Settings) -> None:
    pl.DataFrame(
        [
            {"slot_id": "S1", "city_id": "CITY_A", "city_name": "A", "province_name": "P", "source_role": "municipal_government", "required": True, "status": "enabled", "candidate_count": 1, "verified_candidate_count": 1, "enabled_source_count": 1, "preferred_source_id": "SRC1"},
            {"slot_id": "S2", "city_id": "CITY_A", "city_name": "A", "province_name": "P", "source_role": "housing_department", "required": True, "status": "candidate", "candidate_count": 1, "verified_candidate_count": 0, "enabled_source_count": 0, "preferred_source_id": "SRC2"},
        ]
    ).write_parquet(settings.curated / "source_requirement_slots.parquet")
    pl.DataFrame([{"source_id": "SRC1", "city_id": "CITY_A"}, {"source_id": "SRC2", "city_id": "CITY_A"}]).write_parquet(settings.curated / "source_registry.parquet")
    pl.DataFrame([{"source_id": "SRC1", "slot_id": "S1", "city_id": "CITY_A", "source_role": "municipal_government", "source_status": "COMPLETE_WITH_GAPS", "backfill_status": "complete_with_gaps", "updated_at": "2026-08-03T00:00:00+00:00"}]).write_parquet(settings.curated / "source_sync_state.parquet")
    pl.DataFrame([{"record_id": "R1", "record_date": date(2018, 1, 2), "title": "t", "full_text": "long text", "primary_source_url": "https://a.gov.cn/1", "content_hash": "H1"}, {"record_id": "R2", "record_date": date(2020, 1, 2), "title": "t2", "full_text": "long text", "primary_source_url": "https://a.gov.cn/2", "content_hash": "H1"}]).write_parquet(settings.curated / "records.parquet")
    pl.DataFrame([{"record_id": "R1", "city_id": "CITY_A", "city_name": "A", "province_name": "P"}, {"record_id": "R2", "city_id": "CITY_A", "city_name": "A", "province_name": "P"}]).write_parquet(settings.curated / "record_geographies_normalized.parquet")
    pl.DataFrame([{"gap_id": "G1", "city_id": "CITY_A", "severity": "high", "status": "open"}]).write_parquet(settings.curated / "coverage_gaps.parquet")


def test_metrics_use_required_slot_denominator_and_dynamic_years(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    _write_fixture(settings)
    data = overview_metrics(settings)
    assert data["kpis"]["source_slots"]["denominator"] == 5
    assert data["kpis"]["enabled_slots"]["numerator"] == 1
    assert data["open_gaps"] == 1
    years = city_year_coverage(settings, start_year=2018, end_year=2020)
    assert set(years["year"].to_list()) == {2018, 2020}


def test_partial_source_and_gold_placeholder_are_explicit(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    _write_fixture(settings)
    matrix = city_role_matrix(settings)
    assert matrix.filter(pl.col("display_status") == "PARTIAL_BUT_USABLE").height == 1
    gold = gold_placeholder(settings)
    assert gold["enabled"] is False
    assert gold["policy_intensity_calls"] == 0
    assert document_quality(settings)["duplicate_hash"] == 1
