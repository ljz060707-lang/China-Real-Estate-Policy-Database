from __future__ import annotations

import json
from pathlib import Path

import polars as pl

from policydb.autopilot import AutopilotConfig, AutopilotController, AutopilotStateStore, _gate
from policydb.settings import Settings


def _settings(tmp_path: Path) -> Settings:
    return Settings(root=tmp_path, curated_path=tmp_path / "curated", database_path=tmp_path / "db.duckdb")


def _queue() -> pl.DataFrame:
    return pl.DataFrame([
        {"slot_id": "S1", "city_id": "C1", "city_name": "示例市", "source_role": "housing_department", "work_status": "candidate_ready_for_probe", "best_candidate_id": "CAND1", "health_probe_success_count": 0},
        {"slot_id": "S2", "city_id": "C1", "city_name": "示例市", "source_role": "government_gazette", "work_status": "no_candidate_manual_research", "best_candidate_id": None, "health_probe_success_count": 0},
    ])


def _blocked_audit(settings):
    return {"required_slots": 525, "slots_verified": 1, "slots_enabled": 1, "slots_direct_healthy": 1, "slots_parser_ready": 1, "slots_unresolved": 524, "enabled_unverified_slots": 0}


def test_config_has_safe_caps_and_requires_two_probes(tmp_path):
    path = tmp_path / "autopilot.yaml"
    path.write_text("max_slots_per_batch: 20\nconcurrency: 4\nprobe_rounds: 2\n", encoding="utf-8")
    config = AutopilotConfig.load(path)
    assert config.max_slots_per_batch == 20
    assert config.per_domain_concurrency == 1
    assert config.probe_rounds == 2


def test_state_store_writes_atomic_status_and_transition(tmp_path):
    store = AutopilotStateStore(tmp_path / "run")
    store.write({"run_id": "R1", "status": "SOURCE_COMPLETION"})
    state = store.transition(new_status="AI_PLANNED", reason_code="planned", slot_id="S1")
    assert state["status"] == "AI_PLANNED"
    event = json.loads(store.events_path.read_text(encoding="utf-8").splitlines()[0])
    assert event["slot_id"] == "S1"
    assert event["idempotency_key"]


def test_plan_is_dry_and_builds_go_gate(monkeypatch, tmp_path):
    settings = _settings(tmp_path)
    monkeypatch.setattr("policydb.autopilot.build_slot_work_queue", lambda settings: _queue())
    monkeypatch.setattr("policydb.autopilot.audit_525", _blocked_audit)
    result = AutopilotController(settings, output=tmp_path / "out" / "run").plan()
    assert result["go_no_go"]["status"] == "BLOCKED"
    assert result["source_plan"]["execution_started"] is False
    assert (tmp_path / "out" / "run" / "full_crawl_plan_dry_run.json").exists()


def test_run_without_apply_never_calls_source_runner(monkeypatch, tmp_path):
    settings = _settings(tmp_path)
    monkeypatch.setattr("policydb.autopilot.build_slot_work_queue", lambda settings: _queue())
    monkeypatch.setattr("policydb.autopilot.audit_525", _blocked_audit)
    called = []
    controller = AutopilotController(settings, output=tmp_path / "out" / "run", source_runner=lambda *args, **kwargs: called.append(1))
    result = controller.run(apply=False)
    assert result["status"] == "PLANNED"
    assert called == []


def test_stop_and_retry_are_persistent(monkeypatch, tmp_path):
    settings = _settings(tmp_path)
    monkeypatch.setattr("policydb.autopilot.build_slot_work_queue", lambda settings: _queue())
    monkeypatch.setattr("policydb.autopilot.audit_525", _blocked_audit)
    controller = AutopilotController(settings, output=tmp_path / "out" / "run")
    controller.plan()
    stopped = controller.stop()
    assert stopped["status"] == "STOPPED"
    assert controller.store.stop_requested()
    resumed = controller.retry()
    assert resumed["status"] == "RETRY_WAIT"
    assert not controller.store.stop_requested()


def test_go_gate_requires_all_deterministic_checks():
    audit = {"required_slots": 525, "slots_verified": 525, "slots_enabled": 525, "slots_direct_healthy": 525, "slots_parser_ready": 525, "slots_unresolved": 0, "enabled_unverified_slots": 0}
    assert _gate(audit, tests_passed=True)["status"] == "GO"
    assert _gate(audit, tests_passed=False)["status"] == "BLOCKED"
