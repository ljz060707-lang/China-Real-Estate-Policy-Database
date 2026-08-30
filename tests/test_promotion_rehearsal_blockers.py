from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from policydb.crawl.service import selected_batch_fetch_limit
from policydb.jobs.models import CrawlJobRequest
from policydb.promotion_audit import build_promotion_gate_trace
from policydb.runtime_context import RuntimeContextError, build_runtime_context
from policydb.settings import Settings


def _settings(root: Path) -> Settings:
    data_root = root / "data"
    return Settings(
        root=root,
        data_root_path=data_root,
        database_path=data_root / "database" / "policydb.duckdb",
        curated_path=data_root / "curated",
        outputs_path=data_root / "outputs",
    )


def test_rehearsal_runtime_requires_an_isolated_promotion_root(tmp_path: Path) -> None:
    settings = _settings(tmp_path / "ordinary-run")
    with pytest.raises(RuntimeContextError, match="promotion_rehearsal"):
        build_runtime_context(
            settings,
            run_mode="REHEARSAL",
            run_id="RUN_BAD",
            production_write_allowed=False,
        )


def test_rehearsal_runtime_accepts_explicit_isolated_root(tmp_path: Path) -> None:
    root = tmp_path / "promotion_rehearsal" / "RUN_GOOD"
    settings = _settings(root)
    context = build_runtime_context(
        settings,
        run_mode="REHEARSAL",
        run_id="RUN_GOOD",
        production_write_allowed=False,
    )
    assert context.run_mode == "REHEARSAL"
    assert context.production_write_allowed is False
    assert context.database_path == settings.database.resolve()


def test_selected_batch_can_be_drained_without_removing_global_cap() -> None:
    request = CrawlJobRequest(
        mode="historical_episode_930",
        max_fetches=30,
        max_candidates_total=80,
        drain_selected_batch=True,
    )
    assert selected_batch_fetch_limit(request, planned_item_count=80) == 80
    assert selected_batch_fetch_limit(request, planned_item_count=120) == 80


def test_promotion_trace_exposes_first_failed_gate_and_passes_valid_action() -> None:
    documents = pl.DataFrame(
        [
            {
                "document_id": "DOC1",
                "is_formal_eligible": True,
                "official_url": "https://beijing.gov.cn/policy/1",
                "effective_date_basis": "EXPLICIT_EFFECTIVE_DATE",
            }
        ]
    )
    actions = pl.DataFrame(
        [
            {
                "action_id": "ACT1",
                "document_id": "DOC1",
                "action_text": "提高首付款比例",
                "policy_type": "LIMIT_PURCHASE",
                "action_direction": "TIGHTEN",
                "geographic_scope": "全市",
                "effective_date": "2016-09-30",
                "effective_date_basis": "ACTION_SPECIFIC_EFFECTIVE_DATE",
            },
            {
                "action_id": "ACT2",
                "document_id": "DOC1",
                "action_text": "提高首付款比例",
                "policy_type": "LIMIT_PURCHASE",
                "action_direction": "UNKNOWN",
                "geographic_scope": "全市",
                "effective_date": "2016-09-30",
                "effective_date_basis": "ACTION_SPECIFIC_EFFECTIVE_DATE",
            },
        ]
    )
    trace = build_promotion_gate_trace(
        actions,
        documents,
        verified_action_ids={"ACT1", "ACT2"},
        dedup_action_ids={"ACT1", "ACT2"},
        database_action_ids={"ACT1", "ACT2"},
    )
    passed = trace.filter(pl.col("action_id") == "ACT1").row(0, named=True)
    failed = trace.filter(pl.col("action_id") == "ACT2").row(0, named=True)
    assert passed["promotion_gate"] == "PASS"
    assert passed["first_failed_gate"] is None
    assert failed["promotion_gate"] == "FAIL"
    assert failed["first_failed_gate"] == "direction_gate"
