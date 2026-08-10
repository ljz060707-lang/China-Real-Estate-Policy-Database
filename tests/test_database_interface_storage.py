from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb
import polars as pl
import pytest

from policydb.query import database_validation as validation
from policydb.query.database import curated_dataset_parquets
from policydb.settings import Settings


def _write_curated(curated: Path, *, omit: str | None = None) -> None:
    curated.mkdir(parents=True, exist_ok=True)
    frames = {
        "records": pl.DataFrame(
            [
                {
                    "record_id": "R1",
                    "title": "Policy",
                    "record_date": date(2020, 1, 2),
                    "primary_source_url": "https://example.gov.cn/policy",
                    "full_text": "policy text",
                    "manual_review_status": "approved",
                    "record_type": "policy",
                    "official_status": "official",
                    "source_quality": 5,
                    "legacy_category": "housing",
                    "source_sheet": "sheet",
                    "direction": "neutral",
                    "summary": "summary",
                }
            ]
        ),
        "policy_document_versions": pl.DataFrame([{"document_version_id": "V1"}]),
        "crawl_items": pl.DataFrame([{"item_id": "I1"}]),
        "source_sync_state": pl.DataFrame([{"source_id": "S1"}]),
        "record_geographies_normalized": pl.DataFrame(
            [
                {
                    "record_id": "R1",
                    "city_name": "Test City",
                    "province_name": "Test Province",
                    "parent_city_name": None,
                    "county_name": None,
                    "geography_original": "Test City",
                    "city_code": "C1",
                    "jurisdiction_level": "city",
                }
            ]
        ),
        "source_requirement_slots": pl.DataFrame([{"slot_id": "SLOT1"}]),
    }
    for name, frame in frames.items():
        if name != omit:
            frame.write_parquet(curated / f"{name}.parquet")


def _create_database(
    database_path: Path,
    curated: Path,
    *,
    old_d_view: bool = False,
    omit: str | None = None,
) -> None:
    _write_curated(curated, omit=omit)
    with duckdb.connect(str(database_path)) as connection:
        for relation in (
            "records",
            "policy_document_versions",
            "crawl_items",
            "source_sync_state",
            "record_geographies_normalized",
            "source_requirement_slots",
        ):
            parquet = curated / f"{relation}.parquet"
            if parquet.exists():
                sql_path = str(parquet).replace("\\", "/").replace("'", "''")
                connection.execute(
                    f"CREATE TABLE {relation} AS SELECT * FROM read_parquet('{sql_path}')"
                )
        connection.execute("CREATE VIEW v_policy_master AS SELECT * FROM records")
        connection.execute(
            """CREATE VIEW v_data_quality AS
               SELECT count(*) AS record_count,
                      count(*) FILTER (WHERE title IS NULL) AS missing_title_count,
                      count(*) FILTER (WHERE full_text IS NULL) AS missing_full_text_count,
                      count(*) FILTER (WHERE primary_source_url IS NULL) AS missing_url_count,
                      count(*) FILTER (WHERE manual_review_status='pending') AS pending_review_count
               FROM records"""
        )
        if old_d_view:
            connection.execute(
                "CREATE VIEW v_old_path AS "
                "SELECT 'D:/Data Set/CRPD/curated/records.parquet' AS external_source"
            )
        else:
            sql_path = str(curated / "records.parquet").replace("\\", "/").replace("'", "''")
            connection.execute(
                "CREATE VIEW v_curated_path AS "
                f"SELECT * FROM read_parquet('{sql_path}')"
            )


def _settings(tmp_path: Path, curated: Path, production: Path) -> Settings:
    return Settings(
        root=tmp_path,
        data_root_path=tmp_path / "data",
        curated_path=curated,
        database_path=production,
    )


def test_database_builder_ignores_atomic_parquet_artifacts(tmp_path: Path) -> None:
    curated = tmp_path / "curated"
    curated.mkdir()
    formal = curated / "records.parquet"
    hidden_temporary = curated / ".crawl_items.parquet.run.append.tmp.parquet"
    invalid_identifier = curated / "bad-name.parquet"
    for path in (formal, hidden_temporary, invalid_identifier):
        path.write_bytes(b"placeholder")

    assert curated_dataset_parquets(curated) == [formal]


def test_required_relation_missing_is_a_query_failure(tmp_path: Path) -> None:
    database = tmp_path / "candidate.duckdb"
    curated = tmp_path / "curated"
    _create_database(database, curated, omit="crawl_items")

    result = validation.validate_database_interface(database, curated_path=curated)

    assert result["query_ok"] is False
    assert result["passed"] is False
    assert result["representative_queries"]["crawl_items"]["ok"] is False
    assert result["representative_queries"]["crawl_items"]["sql"] is None
    audit = next(item for item in result["DATABASE_INTERFACE_VALIDATION"] if item["object"] == "crawl_items")
    assert audit["query_ok"] is False
    assert audit["status"] == "MISSING_REQUIRED_RELATION"


def test_old_d_view_path_fails_validation(tmp_path: Path) -> None:
    database = tmp_path / "candidate.duckdb"
    curated = tmp_path / "curated"
    _create_database(database, curated, old_d_view=True)

    result = validation.validate_database_interface(database, curated_path=curated)

    assert result["passed"] is False
    assert result["status"] == "old_path_reference"
    assert result["view_scan"]["old_d_root_references"]


def test_e_curated_views_representative_queries_and_counts_pass(tmp_path: Path) -> None:
    database = tmp_path / "candidate.duckdb"
    curated = tmp_path / "curated"
    _create_database(database, curated)

    result = validation.validate_database_interface(database, curated_path=curated)

    assert result["query_ok"] is True
    assert result["passed"] is True
    assert result["status"] == "healthy"
    assert result["view_scan"]["old_d_root_references"] == []
    assert all(item["difference"] == 0 for item in result["DATABASE_INTERFACE_VALIDATION"])
    assert result["curated_consistency"]["all_available_checks_match"] is True


def test_candidate_path_cannot_equal_production(tmp_path: Path) -> None:
    curated = tmp_path / "curated"
    production = tmp_path / "production.duckdb"
    settings = _settings(tmp_path, curated, production)

    with pytest.raises(ValueError, match="must not equal"):
        validation.build_candidate_database(settings, candidate_path=production)


def test_active_writer_blocks_formal_database_switch() -> None:
    blockers = validation.database_switch_blockers(
        crawler_writer_active=True,
        legacy_supervisor_writer_active=False,
        checkpoint_safe=True,
        candidate_validation_passed=True,
        dashboard_smoke_passed=True,
    )

    assert blockers == ["ACTIVE_CRAWLER_WRITER"]


def test_formal_database_switch_requires_all_five_gates() -> None:
    assert validation.database_switch_blockers(
        crawler_writer_active=False,
        legacy_supervisor_writer_active=False,
        checkpoint_safe=True,
        candidate_validation_passed=True,
        dashboard_smoke_passed=True,
    ) == []


def test_candidate_build_uses_same_directory_and_does_not_touch_production(
    tmp_path: Path, monkeypatch
) -> None:
    curated = tmp_path / "curated"
    production = tmp_path / "production.duckdb"
    candidate = tmp_path / "candidate.duckdb"
    production.write_bytes(b"production-before")
    candidate.write_bytes(b"candidate-before")
    settings = _settings(tmp_path, curated, production)
    observed: list[Path] = []

    def fake_build(candidate_settings: Settings, *, materialize_geography: bool) -> None:
        assert materialize_geography is False
        observed.append(candidate_settings.database)
        _create_database(candidate_settings.database, curated)

    monkeypatch.setattr(validation, "build_database", fake_build)

    result = validation.build_candidate_database(settings, candidate_path=candidate)

    assert result["passed"] is True
    assert result["candidate_replaced"] is True
    assert production.read_bytes() == b"production-before"
    assert observed and observed[0].parent == candidate.parent
    assert observed[0] != candidate
    assert not observed[0].exists()


def test_failed_candidate_validation_preserves_existing_candidate(
    tmp_path: Path, monkeypatch
) -> None:
    curated = tmp_path / "curated"
    production = tmp_path / "production.duckdb"
    candidate = tmp_path / "candidate.duckdb"
    production.write_bytes(b"production-before")
    candidate.write_bytes(b"candidate-before")
    settings = _settings(tmp_path, curated, production)
    observed: list[Path] = []

    def fake_build(candidate_settings: Settings, *, materialize_geography: bool) -> None:
        observed.append(candidate_settings.database)
        _create_database(candidate_settings.database, curated, omit="crawl_items")

    monkeypatch.setattr(validation, "build_database", fake_build)

    result = validation.build_candidate_database(settings, candidate_path=candidate)

    assert result["passed"] is False
    assert result["candidate_replaced"] is False
    assert candidate.read_bytes() == b"candidate-before"
    assert production.read_bytes() == b"production-before"
    assert observed and not observed[0].exists()
