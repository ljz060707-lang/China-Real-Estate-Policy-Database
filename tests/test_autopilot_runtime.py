from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import polars as pl

from policydb.autopilot import AutopilotConfig
from policydb.autopilot_runtime import (
    EXIT_GO_BLOCKED,
    BoundedAutopilotController,
    build_go_gate,
    exit_code_for,
    select_source_slots,
    select_top_candidates,
    usage_summary,
)
from policydb.settings import Settings

NANJING_VERIFIED_SLOT = "SLOT_B608F9073953B62B5B33"


def _settings(tmp_path: Path) -> Settings:
    return Settings(root=tmp_path, curated_path=tmp_path / "curated", database_path=tmp_path / "db.duckdb")


def _row(slot_id: str, **updates: object) -> dict[str, object]:
    row: dict[str, object] = {
        "slot_id": slot_id,
        "city_id": f"CITY_{slot_id}",
        "city_name": f"City {slot_id}",
        "province_name": "Province",
        "source_role": "housing_department",
        "work_status": "no_candidate",
        "coverage_status": "no_candidate",
        "candidate_count": 0,
        "best_candidate_id": None,
        "health_probe_success_count": 0,
        "verified_candidate_count": 0,
        "enabled_source_count": 0,
        "is_verified": False,
        "is_enabled": False,
        "manual_review_status": None,
        "next_retry_at": None,
        "active_batch_id": None,
        "claim_status": None,
    }
    row.update(updates)
    return row


def _blocked_audit() -> dict[str, int]:
    return {
        "required_slots": 525,
        "slots_verified": 1,
        "slots_enabled": 1,
        "slots_direct_healthy": 1,
        "slots_parser_ready": 1,
        "slots_unresolved": 524,
        "enabled_unverified_slots": 0,
    }


def test_verified_enabled_review_retry_and_active_slots_are_not_claimed() -> None:
    future = (datetime.now(UTC) + timedelta(hours=1)).isoformat()
    queue = pl.from_dicts(
        [
            _row(NANJING_VERIFIED_SLOT, verified_candidate_count=1),
            _row("ENABLED", enabled_source_count=1),
            _row("REVIEW", work_status="HUMAN_REVIEW"),
            _row("RETRY", work_status="RETRY_WAIT", next_retry_at=future),
            _row("ACTIVE", active_batch_id="OTHER"),
            _row("DOUBLE", health_probe_success_count=2),
            _row("CANDIDATE", candidate_count=2, best_candidate_id="C1"),
            _row("CONTENT", coverage_status="content_evidence_only"),
            _row("EMPTY"),
        ],
        infer_schema_length=None,
    )

    selected = select_source_slots(queue, max_slots=10)

    assert selected.get_column("slot_id").to_list() == ["DOUBLE", "CANDIDATE", "CONTENT", "EMPTY"]
    assert NANJING_VERIFIED_SLOT not in selected.get_column("slot_id").to_list()


def test_active_and_completed_claim_sets_are_excluded() -> None:
    queue = pl.from_dicts([_row("A"), _row("B"), _row("C")], infer_schema_length=None)
    selected = select_source_slots(
        queue,
        max_slots=3,
        active_claimed_ids={"A"},
        completed_slot_ids={"B"},
    )
    assert selected.get_column("slot_id").to_list() == ["C"]


def test_top_three_are_formal_candidates_and_remainder_is_search_evidence() -> None:
    proposals = pl.from_dicts(
        [
            {
                "proposal_id": f"P{index}",
                "candidate_url": f"https://example{index}.gov.cn/list/",
                "ai_confidence": 1 - index / 10,
            }
            for index in range(5)
        ]
    )
    selected, evidence = select_top_candidates(proposals)
    assert selected.height == 3
    assert evidence.filter(pl.col("selection_status") == "selected_top3").height == 3
    assert evidence.filter(pl.col("selection_status") == "search_evidence_only").height == 2


def test_usage_missing_is_null_and_go_blocked_has_dedicated_exit_code() -> None:
    usage = usage_summary(
        [
            {
                "status": "response_completed",
                "cache_hit": False,
                "total_tokens": None,
                "estimated_cost_usd": 0,
            }
        ]
    )
    assert usage["tokens"] is None
    assert usage["cost"] is None
    assert usage["usage_status"] == "unavailable"
    assert exit_code_for(batch_success=True, go_status="BLOCKED") == EXIT_GO_BLOCKED


def test_full_test_result_is_tri_state_and_only_passed_allows_full_gate() -> None:
    complete = {
        "required_slots": 525,
        "slots_verified": 525,
        "slots_enabled": 525,
        "slots_direct_healthy": 525,
        "slots_parser_ready": 525,
        "slots_unresolved": 0,
        "enabled_unverified_slots": 0,
    }
    unknown = build_go_gate(complete)
    passed = build_go_gate(
        complete,
        test_evidence={
            "test_commit_sha": "abc",
            "test_timestamp": "2026-08-02T00:00:00Z",
            "test_result": "passed",
            "test_suite": "full pytest",
        },
    )
    assert unknown["status"] == "BLOCKED"
    assert unknown["test_result"] == "unknown"
    assert passed["status"] == "GO"


def test_bounded_run_caps_candidates_syncs_status_and_records_transitions(monkeypatch, tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    queue = pl.from_dicts([_row("SLOT_1")], infer_schema_length=None)
    monkeypatch.setattr("policydb.autopilot_runtime.build_slot_work_queue", lambda _settings: queue)
    monkeypatch.setattr("policydb.autopilot_runtime.audit_525", lambda _settings: _blocked_audit())

    def fake_ai_batch(_settings: Settings, *, output: Path, **_kwargs: object) -> dict[str, int]:
        output.mkdir(parents=True, exist_ok=True)
        pl.from_dicts(
            [
                {
                    "proposal_id": f"P{index}",
                    "slot_id": "SLOT_1",
                    "city_id": "CITY_SLOT_1",
                    "city_name": "Test City",
                    "source_role": "housing_department",
                    "candidate_url": f"https://agency{index}.gov.cn/list/",
                    "candidate_snippet": f"candidate {index}",
                    "ai_confidence": 0.9 - index / 100,
                    "ai_recommended_action": "pending_probe",
                }
                for index in range(5)
            ],
            infer_schema_length=None,
        ).write_parquet(output / "candidate_proposals.parquet")
        return {"ai_calls": 1, "ai_attempts": 1}

    class FakeAuditStore:
        def __init__(self, _path: Path) -> None:
            pass

        def records(self) -> list[dict[str, object]]:
            return [
                {
                    "request_id": "REQ1",
                    "status": "response_completed",
                    "cache_hit": False,
                    "total_tokens": None,
                    "estimated_cost_usd": 0,
                }
            ]

    applied: list[dict[str, object]] = []
    probed: list[str] = []

    def fake_upsert(rows: list[dict[str, object]], _settings: Settings) -> None:
        applied.extend(rows)

    def fake_probe(*, candidate_id: str, rounds: int, settings: Settings) -> dict[str, object]:
        assert rounds == 2
        assert settings is not None
        probed.append(candidate_id)
        return {"checked": 1, "verification": {"verified": 0}}

    monkeypatch.setattr("policydb.autopilot_runtime.run_ai_batch", fake_ai_batch)
    monkeypatch.setattr("policydb.autopilot_runtime.AIAuditStore", FakeAuditStore)
    monkeypatch.setattr("policydb.autopilot_runtime.upsert_candidates", fake_upsert)
    monkeypatch.setattr("policydb.autopilot_runtime.probe_candidates", fake_probe)
    monkeypatch.setattr(
        "policydb.autopilot_runtime.verify_candidates",
        lambda **_kwargs: {"checked": 3, "verified": 0, "enabled": 0},
    )
    monkeypatch.setattr(
        "policydb.autopilot_runtime.promote_verified_candidates",
        lambda **_kwargs: {"promoted_candidates": 0, "source_ids": []},
    )
    monkeypatch.setattr("policydb.autopilot_runtime.enable_source_strict", lambda *_args, **_kwargs: None)

    config = replace(
        AutopilotConfig(),
        max_slots_per_batch=1,
        max_ai_calls_per_batch=1,
        max_candidates_per_slot=3,
        concurrency=1,
    )
    controller = BoundedAutopilotController(
        settings,
        config=config,
        output=tmp_path / "run",
        run_id="RUNTIME_TEST",
    )
    result = controller.run(apply=True)

    assert result["exit_code"] == EXIT_GO_BLOCKED
    assert result["status"] == "GO_BLOCKED"
    assert result["candidate_proposals"] == 5
    assert result["applied_candidates"] == 3
    assert result["probed_candidates"] == 3
    assert result["human_review"] == 3
    assert result["slot_results"] == [
        {
            "slot_id": "SLOT_1",
            "candidate_proposals": 5,
            "applied_candidates": 3,
            "probed_candidates": 3,
            "human_review": 3,
            "completed": True,
        }
    ]
    assert len(applied) == 3
    assert len(probed) == 3
    assert result["provider_status"] == "operational"
    assert result["api_balance_status"] == "call_succeeded"
    assert result["tokens"] is None
    assert result["cost"] is None
    assert result["full_run_started"] is False

    status = json.loads((tmp_path / "run" / "current_status.json").read_text(encoding="utf-8"))
    assert status["human_review"] == status["current_batch"]["human_review"] == 3
    assert status["provider_status"] == "operational"
    assert status["api_balance_status"] == "call_succeeded"
    assert status["tokens"] is None
    assert status["cost"] is None
    assert status["usage_status"] == result["usage_status"] == "unavailable"
    assert status["cost_status"] == result["cost_status"] == "unavailable"
    assert status["full_tests_status"] == "unknown"

    events = [
        json.loads(line)
        for line in (tmp_path / "run" / "state_transitions.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    required = {
        "batch_claimed",
        "slot_claimed",
        "ai_request_started",
        "ai_request_completed",
        "search_started",
        "search_completed",
        "candidates_ranked",
        "probe_started",
        "probe_completed",
        "verify_completed",
        "strict_enable_completed",
        "batch_completed",
        "go_gate_evaluated",
        "go_gate_blocked",
    }
    assert required <= {event["event_type"] for event in events}
    assert all(event["run_id"] == "RUNTIME_TEST" for event in events)
    assert all(event["reason_code"] and event["timestamp"] and event["idempotency_key"] for event in events)

    resumed = controller.run(apply=False, resume=True)
    assert resumed["planned_slots"] == 0
    assert resumed["slot_ids"] == []


def test_ai_call_cap_also_caps_planned_slots(monkeypatch, tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    queue = pl.from_dicts([_row(f"S{index}") for index in range(5)], infer_schema_length=None)
    monkeypatch.setattr("policydb.autopilot_runtime.build_slot_work_queue", lambda _settings: queue)
    config = replace(AutopilotConfig(), max_slots_per_batch=3, max_ai_calls_per_batch=2)
    result = BoundedAutopilotController(settings, config=config, output=tmp_path / "plan").run(apply=False)
    assert result["planned_slots"] == 2
    assert not (tmp_path / "plan" / "slot_claims.json").exists()