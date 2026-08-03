from __future__ import annotations

import json
from pathlib import Path

import pytest

from policydb.fast_bulk_ingest import (
    FastBulkConfig,
    FastBulkIngestController,
    _status_from_summary,
    select_city_source_queue,
)
from policydb.settings import Settings


def test_fast_defaults_keep_gold_disabled() -> None:
    config = FastBulkConfig()
    config.validate()
    assert config.mode == "FAST_BULK_INGEST"
    assert config.max_minutes_per_source == 10
    assert config.max_documents_per_source == 300
    assert config.gold_enabled is False


def test_city_role_selection_is_round_robin(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    settings = Settings(root=tmp_path, curated_path=tmp_path / "curated")
    metrics = {
        "CITY_A": {"city_id": "CITY_A", "city_name": "A", "province_name": "P", "documents": 1, "missing_years": 1, "missing_roles": 2},
        "CITY_B": {"city_id": "CITY_B", "city_name": "B", "province_name": "P", "documents": 0, "missing_years": 9, "missing_roles": 5},
    }
    ready = [
        {"source_id": "SRC_A_M", "source_name": "A-m", "agency_type": "municipal_government", "source_role": "municipal_government", "city_ids": ["CITY_A"], "crawl_enabled": True},
        {"source_id": "SRC_B_M", "source_name": "B-m", "agency_type": "municipal_government", "source_role": "municipal_government", "city_ids": ["CITY_B"], "crawl_enabled": True},
        {"source_id": "SRC_A_H", "source_name": "A-h", "agency_type": "housing_department", "source_role": "housing_department", "city_ids": ["CITY_A"], "crawl_enabled": True},
        {"source_id": "SRC_B_H", "source_name": "B-h", "agency_type": "housing_department", "source_role": "housing_department", "city_ids": ["CITY_B"], "crawl_enabled": True},
    ]
    monkeypatch.setattr("policydb.fast_bulk_ingest._city_metrics", lambda _settings: metrics)
    monkeypatch.setattr("policydb.fast_bulk_ingest._ready_source_rows", lambda _settings, _config: ready)
    config = FastBulkConfig(source_roles=("municipal_government", "housing_department"), max_cities=2)
    queue = select_city_source_queue(settings, config)
    assert [row["source_role"] for row in queue["tasks"]] == ["municipal_government", "housing_department", "municipal_government", "housing_department"]
    assert [row["city_id"] for row in queue["tasks"]] == ["CITY_B", "CITY_B", "CITY_A", "CITY_A"]


def test_status_mapping_keeps_partial_usable_distinct() -> None:
    assert _status_from_summary({"source_results": [{"status": "partial", "fetched": 2}]}) == "PARTIAL_BUT_USABLE"
    assert _status_from_summary({"source_results": [{"status": "partial", "fetched": 0}]}) == "PARTIAL_EMPTY"
    assert _status_from_summary({"source_results": [{"status": "skipped_dependency"}]}) == "SKIPPED_DEPENDENCY"


def test_resume_does_not_repeat_completed_source(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    settings = Settings(root=tmp_path, curated_path=tmp_path / "curated")
    task = {"task_id": "TASK1", "city_id": "CITY_A", "city_name": "A", "province_name": "P", "source_id": "SRC1", "source_role": "housing_department", "source_name": "A housing"}
    monkeypatch.setattr("policydb.fast_bulk_ingest.select_city_source_queue", lambda _settings, _config, city_ids=None: {"cities": [{"city_id": "CITY_A", "city_name": "A", "province_name": "P"}], "tasks": [task], "city_count": 1, "source_count": 1, "role_order": []})
    calls: list[object] = []

    class FakeController:
        def __init__(self, _settings, *, config, output, run_id):
            calls.append(config)

        def run(self, *, command):
            return {"exit_code": 0, "source_results": [{"status": "completed", "status_category": "SUCCESS", "fetched": 1}]}

    monkeypatch.setattr("policydb.fast_bulk_ingest.FullSyncController", FakeController)
    config = FastBulkConfig(apply=True, output=tmp_path / "run")
    first = FastBulkIngestController(settings, config=config, output=tmp_path / "run", run_id="FAST_TEST").run()
    second = FastBulkIngestController(settings, config=config, output=tmp_path / "run", run_id="FAST_TEST").run()
    assert first["documents_added"] == 1
    assert second["processed_sources"] == 1
    assert len(calls) == 1
    checkpoints = (tmp_path / "run" / "fast_bulk_checkpoints.jsonl").read_text(encoding="utf-8").splitlines()
    assert len([json.loads(line) for line in checkpoints if json.loads(line)["checkpoint_type"] == "SOURCE_COMPLETED"]) == 1
    passed = calls[0]
    assert passed.max_minutes_per_source == 10
    assert passed.max_list_pages_per_source == 30
    assert passed.max_attachment_attempts == 1


def test_resume_can_record_completion_after_an_attempt(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    settings = Settings(root=tmp_path, curated_path=tmp_path / "curated")
    task = {"task_id": "TASK1", "city_id": "CITY_A", "city_name": "A", "province_name": "P", "source_id": "SRC1", "source_role": "housing_department", "source_name": "A housing"}
    monkeypatch.setattr("policydb.fast_bulk_ingest.select_city_source_queue", lambda _settings, _config, city_ids=None: {"cities": [{"city_id": "CITY_A", "city_name": "A", "province_name": "P"}], "tasks": [task], "city_count": 1, "source_count": 1, "role_order": []})
    outcomes = iter(({"exit_code": 1, "source_results": [{"status": "failed", "fetched": 0}]}, {"exit_code": 0, "source_results": [{"status": "completed", "status_category": "SUCCESS", "fetched": 1}]}))

    class FakeController:
        def __init__(self, _settings, *, config, output, run_id):
            pass

        def run(self, *, command):
            return next(outcomes)

    monkeypatch.setattr("policydb.fast_bulk_ingest.FullSyncController", FakeController)
    config = FastBulkConfig(apply=True, output=tmp_path / "run")
    first = FastBulkIngestController(settings, config=config, output=tmp_path / "run", run_id="FAST_TEST").run()
    second = FastBulkIngestController(settings, config=config, output=tmp_path / "run", run_id="FAST_TEST").run()
    assert first["source_results"][-1]["status"] == "FAILED_TERMINAL"
    assert second["source_results"][-1]["status"] == "SUCCESS"
    rows = [json.loads(line) for line in (tmp_path / "run" / "fast_bulk_checkpoints.jsonl").read_text(encoding="utf-8").splitlines()]
    assert [row["checkpoint_type"] for row in rows] == ["SOURCE_ATTEMPTED", "SOURCE_COMPLETED"]
