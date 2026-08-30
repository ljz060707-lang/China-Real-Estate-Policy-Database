from __future__ import annotations

from datetime import date

import polars as pl

from policydb.episode_930_final_closure import (
    api_certification_summary,
    build_root_objects,
    build_stale_lock_audit,
    derive_date_v3,
    derive_direction_v3,
    root_counts,
)
from scripts.close_ep930_promotion_blockers_v3 import _inputs, _read_parquet


def test_direction_v3_uses_same_unit_parameter_transition() -> None:
    result = derive_direction_v3(
        {
            "policy_type": "PF_DOWNPAYMENT",
            "old_value": "20",
            "new_value": "30",
            "unit": "%",
            "action_text": "调整首付款比例",
        }
    )

    assert result["direction_state"] == "PASS"
    assert result["direction"] == "TIGHTENING"
    assert result["direction_method"] == "PARAMETER_TRANSITION"


def test_date_v3_does_not_invent_2016_effective_date() -> None:
    result = derive_date_v3(
        {"action_text": "住房政策调整"},
        {"publication_date": date(2026, 8, 7), "document_title": "2026年政策"},
    )

    assert result["effective_date"] is None
    assert result["date_source"] == "PUBLICATION_DATE"
    assert result["date_basis"] == "PUBLICATION_DATE_FALLBACK"
    assert result["episode_membership_state"] == "OUTSIDE_FROZEN_EPISODE_WINDOW"


def test_root_counts_deduplicates_independent_units_and_documents() -> None:
    root_objects = pl.DataFrame(
        [
            {
                "root_object_type": "ACTION",
                "root_object_id": "A1",
                "action_id": "A1",
                "evidence_unit_id": "U1",
                "document_id": "D1",
            },
            {
                "root_object_type": "ACTION",
                "root_object_id": "A2",
                "action_id": "A2",
                "evidence_unit_id": "U1",
                "document_id": "D1",
            },
            {
                "root_object_type": "RECOVERY_ITEM",
                "root_object_id": "Q1",
                "action_id": None,
                "evidence_unit_id": None,
                "document_id": None,
            },
        ]
    )

    assert root_counts(root_objects) == {
        "actions": 2,
        "evidence_units": 1,
        "documents": 1,
        "recovery_items": 1,
    }


def test_root_objects_keep_api_blocker_separate_from_non_api_roots() -> None:
    actions = pl.DataFrame(
        [{"action_id": "A1", "document_id": "D1", "city": "示例市", "action_text": "调整政策"}]
    )
    documents = pl.DataFrame(
        [{"document_id": "D1", "document_title": "示例", "is_formal_eligible": True}]
    )
    gate = pl.DataFrame(
        [{"action_id": "A1", "dedup_gate": "PASS", "promotion_gate": "READY"}]
    )
    direction = pl.DataFrame(
        [{"action_id": "A1", "direction_state": "PASS"}]
    )
    dates = pl.DataFrame(
        [{"action_id": "A1", "date_state": "PASS", "episode_membership_state": "WITHIN_OR_UNRESOLVED"}]
    )
    recovery = pl.DataFrame(
        [{"queue_item_id": "Q1", "recovery_required": False}]
    )

    root = build_root_objects(actions, documents, gate, direction, dates, recovery)
    row = root.row(0, named=True)
    assert row["root_blockers"] == "API_CERTIFICATION_BLOCKED"
    assert row["root_blocker_count"] == 1


def test_stale_lock_audit_is_read_only_and_does_not_infer_process_from_hash() -> None:
    result = build_stale_lock_audit([], exclude_pid=999999)

    assert result["mutation_performed"] is False
    assert result["locks"] == []
    assert result["active_ep930_runner_count"] == 0
    assert result["production_writer_count"] == 0


def test_api_artifact_does_not_certify_from_provider_status_or_cache_reuse() -> None:
    result = api_certification_summary(
        {"phase": "BACKOFF_SINGLE_PROBE", "schema_valid": False},
        {"provider": "SiliconFlow", "model": "zai-org/GLM-5.2", "status": "OPERATIONAL"},
        pass1_success=0,
        pass2_success=0,
    )

    assert result["certification"] == "BLOCKED_BY_CERTIFICATION_SEQUENCE"
    assert result["manual_api_calls_this_v3"] == 0
    assert result["cache_reuse_counts_as_probe"] is False


def test_v3_reader_uses_csv_format_for_csv_recovery_artifact(tmp_path) -> None:
    path = tmp_path / "recovery.csv"
    path.write_text("queue_item_id,status\nQ1,RETRY_WAIT\n", encoding="utf-8")

    result = _read_parquet(path)

    assert result.to_dicts() == [{"queue_item_id": "Q1", "status": "RETRY_WAIT"}]


def test_v3_inputs_use_gate_action_ids_instead_of_full_curated_table(tmp_path) -> None:
    curated = tmp_path / "data" / "curated"
    release = tmp_path / "release"
    curated.mkdir(parents=True)
    release.mkdir()
    pl.DataFrame(
        [
            {"action_id": "A1", "document_id": "D1"},
            {"action_id": "A2", "document_id": "D2"},
        ]
    ).write_parquet(curated / "policy_episode_actions.parquet")
    pl.DataFrame(
        [
            {"document_id": "D1", "is_formal_eligible": True},
            {"document_id": "D2", "is_formal_eligible": True},
        ]
    ).write_parquet(curated / "policy_episode_documents.parquet")
    for name in ("policy_episode_parameters.parquet", "policy_episode_gaps.parquet", "policy_episode_city_policy_matrix.parquet"):
        pl.DataFrame().write_parquet(curated / name)
    pl.DataFrame(
        [
            {"action_id": "A2", "document_id": "D2", "post_geography_gate": "PASS"},
            {"action_id": "A3", "document_id": "D3", "post_geography_gate": "FAIL"},
        ]
    ).write_csv(release / "EP930_ACTION_GATE_STATE.csv")

    actions, documents, *_ = _inputs(tmp_path)

    assert actions.get_column("action_id").to_list() == ["A2", "A3"]
    assert documents.get_column("document_id").to_list() == ["D2"]
