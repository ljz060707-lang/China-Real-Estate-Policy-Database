from __future__ import annotations

from datetime import UTC, datetime

import polars as pl

from policydb.source_completion_recovery import (
    classify_recovery_slot,
    manual_research_queue,
)
from policydb.source_discovery import build_source_discovery_query_specs


def test_levelled_query_specs_include_deep_routes_and_skip_cached_hash() -> None:
    row = {
        "city_name": "Test City",
        "city_name_short": "Test",
        "aliases": "TC|Historic Test",
        "source_role": "housing_department",
        "best_candidate_url": "https://housing.test.gov.cn/",
        "historical_institution_names": "旧住房局|合并住建局",
    }
    specs = build_source_discovery_query_specs(row, "housing_department", max_queries=18)
    levels = {int(item["discovery_level"]) for item in specs}
    assert {1, 2, 3, 4, 5, 6, 7, 8, 9} <= levels
    cached = {str(specs[0]["query_hash"])}
    remaining = build_source_discovery_query_specs(
        row,
        "housing_department",
        max_queries=18,
        existing_query_hashes=cached,
    )
    assert str(specs[0]["query"]) not in {str(item["query"]) for item in remaining}


def test_recovery_classification_preserves_ambiguity_and_cooldown() -> None:
    row = classify_recovery_slot(
        {
            "slot_id": "SLOT_1",
            "city_id": "CITY_1",
            "city_name": "Test City",
            "source_role": "housing_department",
            "work_status": "failed_recoverable",
            "proposal_count": 4,
            "best_candidate_url": "https://housing.gov.cn/",
            "city_evidence_fail_count": 4,
            "role_evidence_fail_count": 4,
            "network_fail_count": 0,
            "dominant_failure_reason": "city_evidence_missing",
            "consecutive_zero_yield": 1,
            "prefilter_reason_counts": {"city_evidence_missing": 4},
        },
        now=datetime(2026, 8, 6, tzinfo=UTC),
    )
    assert row["recovery_class"] == "F_EXISTING_CANDIDATE_EVIDENCE_INCOMPLETE"
    assert row["cooldown_until"] is not None
    assert "page-enrich" in str(row["next_strategy"])


def test_manual_queue_only_keeps_unresolved_official_decisions() -> None:
    recovery = pl.from_dicts(
        [
            {"slot_id": "A", "city_id": "CA", "city_name": "A", "source_role": "housing_department", "recovery_class": "F_EXISTING_CANDIDATE_EVIDENCE_INCOMPLETE", "reason": "evidence", "evidence_ids": "[]", "next_strategy": "enrich"},
            {"slot_id": "B", "city_id": "CB", "city_name": "B", "source_role": "housing_department", "recovery_class": "J_CROSS_JURISDICTION_CONFLICT", "reason": "conflict", "evidence_ids": "[]", "next_strategy": "review"},
        ],
        infer_schema_length=None,
    )
    queue = manual_research_queue(recovery)
    assert queue["slot_id"].to_list() == ["B"]
