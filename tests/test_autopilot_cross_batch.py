from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import polars as pl

from policydb.ai_audit import AIAuditStore
from policydb.autopilot_checkpoints import GlobalSlotCheckpointStore, is_slot_claimable
from policydb.autopilot_runtime import select_source_slots
from policydb.source_completion_ai_workflow import SourceAIAssessment, _call_ai


def _slot(slot_id: str = "SLOT_1") -> dict[str, object]:
    return {
        "slot_id": slot_id,
        "city_id": "CITY_1",
        "city_name": "Test City",
        "province_name": "Test Province",
        "source_role": "housing_department",
        "work_status": "no_candidate",
        "candidate_count": 0,
        "verified_candidate_count": 0,
        "enabled_source_count": 0,
        "is_verified": False,
        "is_enabled": False,
    }


def _audit_payload(run_id: str) -> dict[str, object]:
    return {
        "request_id": "REQ_CROSS_BATCH",
        "run_id": run_id,
        "stage": "source_discovery",
        "schema": "SourceAIAssessment",
        "slot_id": "SLOT_1",
        "provider": "test",
        "model": "model",
        "prompt_version": "test-v1",
        "prompt_hash": "prompt-hash",
        "request_hash": "request-hash",
        "cache_key": "request-hash",
    }


def test_global_claim_is_atomic_and_only_one_runtime_wins(tmp_path: Path) -> None:
    store = GlobalSlotCheckpointStore(tmp_path)
    slot = _slot()

    def claim(run_id: str) -> bool:
        return store.claim(slot, run_id=run_id)[0]

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(claim, ["RUN_A", "RUN_B"]))

    assert sorted(results) == [False, True]
    assert store.snapshot()["SLOT_1"]["status"] == "CLAIMED"


def test_terminal_checkpoint_is_non_regressing_and_sticky(tmp_path: Path) -> None:
    store = GlobalSlotCheckpointStore(tmp_path)
    slot = _slot()
    assert store.claim(slot, run_id="RUN_A")[0]
    store.terminal(
        slot,
        status="HUMAN_REVIEW",
        run_id="RUN_A",
        terminal_outcome="ambiguous",
        ai_call_persisted=True,
    )

    ignored = store.terminal(
        slot,
        status="COMPLETED",
        run_id="RUN_B",
        terminal_outcome="retry_attempt",
        ai_call_persisted=False,
    )
    assert ignored["event"] == "CHECKPOINT_NON_REGRESSION_IGNORED"
    assert store.snapshot()["SLOT_1"]["status"] == "HUMAN_REVIEW"
    assert not is_slot_claimable(slot, store.snapshot()["SLOT_1"])

    store.terminal(
        slot,
        status="VERIFIED",
        run_id="RUN_C",
        terminal_outcome="strict_verified",
        ai_call_persisted=True,
    )
    ignored = store.terminal(
        slot,
        status="HUMAN_REVIEW",
        run_id="RUN_D",
        terminal_outcome="stale_rebuild",
        ai_call_persisted=False,
    )
    assert ignored["event"] == "CHECKPOINT_NON_REGRESSION_IGNORED"
    assert store.snapshot()["SLOT_1"]["status"] == "VERIFIED"


def test_retry_wait_and_active_batch_are_not_claimable(tmp_path: Path) -> None:
    store = GlobalSlotCheckpointStore(tmp_path)
    future = (datetime.now(UTC) + timedelta(hours=1)).isoformat()
    retry_slot = _slot("RETRY")
    store.terminal(
        retry_slot,
        status="RETRY_WAIT",
        run_id="RUN_A",
        terminal_outcome="network_retry",
        ai_call_persisted=False,
        next_retry_at=future,
    )
    assert not is_slot_claimable(retry_slot, store.snapshot()["RETRY"])
    assert not is_slot_claimable({**_slot("ACTIVE"), "active_batch_id": "RUN_A"})


def test_historical_duplicate_backfill_is_read_only_then_idempotent(tmp_path: Path) -> None:
    root = tmp_path / "autopilot"
    first = root / "20260802T031259Z"
    second = root / "20260802T032314Z"
    first.mkdir(parents=True)
    second.mkdir(parents=True)
    summary = {
        "run_dir": str(first),
        "ai_calls": 1,
        "persisted_ai_calls": 1,
        "slot_results": [{"slot_id": "SLOT_1", "completed": True, "human_review": 3}],
    }
    (first / "run_summary.json").write_text(json.dumps(summary), encoding="utf-8")
    (second / "run_summary.json").write_text(json.dumps({**summary, "run_dir": str(second)}), encoding="utf-8")
    store = GlobalSlotCheckpointStore(root)

    dry = store.backfill_from_run_dirs(root, apply=False)
    assert dry["backfilled"] == 0
    assert len(dry["proposals"]) == 1
    assert len(dry["duplicates"]) == 1
    assert not (root / "slot_checkpoints.jsonl").exists()

    applied = store.backfill_from_run_dirs(root, apply=True)
    assert applied["backfilled"] == 1
    repeated = store.backfill_from_run_dirs(root, apply=True)
    assert repeated["backfilled"] == 0
    assert store.snapshot()["SLOT_1"]["status"] == "HUMAN_REVIEW"


def test_plan_run_resume_share_checkpoint_predicate() -> None:
    queue = pl.from_dicts([_slot("A"), _slot("B"), _slot("C")], infer_schema_length=None)
    checkpoints = {"A": {"status": "HUMAN_REVIEW"}, "B": {"status": "COMPLETED"}}
    plan = select_source_slots(queue, max_slots=3, checkpoint_records=checkpoints)
    run = select_source_slots(queue, max_slots=3, checkpoint_records=checkpoints)
    resume = select_source_slots(queue, max_slots=3, checkpoint_records=checkpoints)
    assert plan.get_column("slot_id").to_list() == run.get_column("slot_id").to_list() == resume.get_column("slot_id").to_list() == ["C"]


def test_ai_completed_request_is_reused_across_run_directories(tmp_path: Path) -> None:
    class Provider:
        calls = 0

        def structured(self, **_kwargs):
            self.calls += 1
            return SourceAIAssessment(search_queries=["official query"], confidence=0.8), SimpleNamespace(prompt_tokens=4, completion_tokens=2)

    provider = Provider()
    global_root = tmp_path / "global"
    first = AIAuditStore(tmp_path / "run_a", global_root=global_root)
    value, trace, error = _call_ai(
        provider,
        "model",
        "system",
        "user",
        audit=first,
        audit_payload=_audit_payload("RUN_A"),
    )
    assert value is not None and trace is not None and error is None
    assert provider.calls == 1

    second = AIAuditStore(tmp_path / "run_b", global_root=global_root)
    value, trace, error = _call_ai(
        provider,
        "model",
        "system",
        "user",
        audit=second,
        audit_payload=_audit_payload("RUN_B"),
    )
    assert value is not None and trace is None and error is None
    assert provider.calls == 1
    record = second.records()[0]
    assert record["status"] == "response_completed"
    assert record["cache_hit"] is True
    assert record["reused_ai_call"] is True
    assert record["total_tokens"] == 6
