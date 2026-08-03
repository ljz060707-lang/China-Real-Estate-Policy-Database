from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from policydb.dashboard_jobs import enqueue_job, validate_job_request
from policydb.settings import Settings


def _settings(tmp_path: Path) -> Settings:
    curated = tmp_path / "curated"
    curated.mkdir()
    pl.DataFrame([{"city_id": "CITY_A"}]).write_parquet(curated / "source_requirement_slots.parquet")
    return Settings(root=tmp_path, curated_path=curated)


def test_dashboard_job_validates_city_and_confirmation(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    with pytest.raises(ValueError, match="confirmation"):
        validate_job_request(settings, "city_fast_ingest", {"cities": ["CITY_A"]})
    with pytest.raises(ValueError, match="unknown city"):
        validate_job_request(settings, "city_fast_ingest", {"cities": ["CITY_X"]}, confirmed=True)
    request = validate_job_request(settings, "city_fast_ingest", {"cities": ["CITY_A"]}, confirmed=True)
    assert request["action"] == "city_fast_ingest"


def test_duplicate_active_dashboard_job_is_blocked(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    first = enqueue_job(settings, "city_fast_ingest", {"cities": ["CITY_A"]}, confirmed=True)
    assert first["status"] == "QUEUED"
    with pytest.raises(RuntimeError, match="already active"):
        enqueue_job(settings, "city_fast_ingest", {"cities": ["CITY_A"]}, confirmed=True)
