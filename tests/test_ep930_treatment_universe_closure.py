from __future__ import annotations

import pandas as pd

from scripts.close_ep930_treatment_universe import (
    EPISODE_ID,
    _reference_closure_status,
    build_api_fast_lane_plan,
    build_reference_reconciliation,
    build_reference_recovery_queue,
    classify_action,
)


def _row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "episode_id": EPISODE_ID,
        "document_id": "D1",
        "action_id": "A1",
        "city": "北京市",
        "province": "北京市",
        "doc_title": "住房限购政策通知",
        "action_text": "暂停向拥有两套住房的家庭出售新建商品住房",
        "policy_type": "LIMIT_PURCHASE",
        "action_direction": "TIGHTENING",
        "geographic_scope": "全市",
        "announcement_date": "2016-09-30",
        "publication_date": "2016-09-30",
        "dedup_status": "canonical",
        "doc_official_source": True,
        "doc_official_evidence_status": "LIVE_HTTP_200",
        "doc_official_url": "https://www.example.gov.cn/policy/1",
    }
    row.update(overrides)
    return row


def test_2017_policy_is_deterministically_excluded() -> None:
    result = classify_action(_row(announcement_date="2017-01-01", publication_date="2017-01-01"), set(), set())

    assert result["triage_state"] == "EXCLUDE_2017_POLICY"


def test_duplicate_is_deterministically_excluded_before_api() -> None:
    result = classify_action(_row(dedup_status="duplicate_action"), set(), set())

    assert result["triage_state"] == "EXCLUDE_DUPLICATE"


def test_official_late_reprint_with_underlying_2016_date_is_retained() -> None:
    result = classify_action(
        _row(
            announcement_date=None,
            publication_date=None,
            doc_announcement_date=None,
            doc_publication_date="2020-01-01",
            action_text="转载2016年9月30日住房限购政策通知",
        ),
        set(),
        set(),
    )

    assert result["triage_state"] == "KEEP_FOR_EP930_REVIEW"
    assert result["official_reprint_of_2016_policy"] is True
    assert result["date_state"] == "OFFICIAL_REPRINT_DATE_NOT_POLICY_DATE"


def test_missing_date_and_direction_are_evidence_recovery_not_silent_exclusion() -> None:
    result = classify_action(
        _row(announcement_date=None, publication_date=None, action_direction="UNKNOWN"),
        set(),
        set(),
    )

    assert result["triage_state"] == "NEEDS_EVIDENCE"
    assert result["evidence_recovery_required"] is True


def test_reference_reconciliation_has_no_unknown_status() -> None:
    discovery = pd.DataFrame(
        [
            {
                "city": "北京市",
                "province": "北京市",
                "earliest_known_policy_date": "2016-09-30",
                "expected_policy_types": '["LIMIT_PURCHASE"]',
                "official_policy_found": True,
                "official_policy_count": 1,
            }
        ]
    )
    triage = pd.DataFrame([classify_action(_row(), set(), set())])

    result = build_reference_reconciliation(discovery, triage)

    assert result.loc[0, "resolution_status"] != "UNKNOWN"
    assert result.loc[0, "episode_membership"] == "CONFIRMED_INCLUDED"


def test_reference_reconciliation_exposes_allowed_closure_status() -> None:
    discovery = pd.DataFrame(
        [
            {
                "city": "测试市",
                "province": "测试省",
                "earliest_known_policy_date": "2016-09-30",
                "expected_policy_types": ["限购"],
                "official_policy_found": True,
                "official_policy_count": 1,
            }
        ]
    )
    triage = pd.DataFrame(
        [
            {
                "city": "测试市",
                "document_id": "DOC1",
                "action_id": "ACT1",
                "triage_state": "KEEP_FOR_EP930_REVIEW",
                "official_evidence": True,
            }
        ]
    )

    result = build_reference_reconciliation(discovery, triage)

    assert result.loc[0, "closure_status"] == "CONFIRMED_INCLUDED"
    assert set(result["closure_status"]).issubset(
        {
            "CONFIRMED_INCLUDED",
            "CONFIRMED_EXCLUDED",
            "INSUFFICIENT_EVIDENCE",
            "MANUAL_REVIEW_REQUIRED",
        }
    )
    assert _reference_closure_status("INSUFFICIENT_EVIDENCE", "MANUAL_REVIEW_REQUIRED") == "INSUFFICIENT_EVIDENCE"


def test_evidence_fast_lane_preserves_reference_and_keep_priority() -> None:
    matched = classify_action(
        _row(document_id="D_MATCH", action_id="A_MATCH"), set(), set()
    )
    keep = classify_action(
        _row(document_id="D_KEEP", action_id="A_KEEP"), set(), set()
    )
    triage = pd.DataFrame([matched, keep])
    reference = pd.DataFrame(
        [
            {
                "reference_event_id": "REF_MATCH",
                "city": "CITY_MATCH",
                "manual_review_required": False,
                "matched_action_ids": "A_MATCH",
            }
        ]
    )

    plan = build_api_fast_lane_plan(triage, reference)

    assert plan.loc[plan["action_id"].eq("A_MATCH"), "priority"].item() == 0
    assert plan.loc[plan["action_id"].eq("A_KEEP"), "priority"].item() == 2
    assert plan.loc[plan["action_id"].eq("A_MATCH"), "priority_reason"].item() == "REFERENCE_MATCHED_ACTION"


def test_unresolved_reference_is_recovery_required_without_unknown() -> None:
    triage = pd.DataFrame(
        [classify_action(_row(city="CITY_UNRESOLVED", action_id="A_CANDIDATE"), set(), set())]
    )
    reference = pd.DataFrame(
        [
            {
                "reference_event_id": "REF_UNRESOLVED",
                "episode_id": EPISODE_ID,
                "city": "CITY_UNRESOLVED",
                "province": "广东省",
                "reference_date": "2016-10-01",
                "reference_policy_type": "LIMIT_PURCHASE",
                "matched_action_ids": "",
                "resolution_status": "MANUAL_REVIEW_REQUIRED",
                "resolution_reason": "official_evidence_not_closed",
                "official_evidence": False,
                "manual_review_required": True,
            }
        ]
    )

    queue = build_reference_recovery_queue(reference, triage)

    assert len(queue) == 1
    assert queue.loc[0, "status"] == "RECOVERY_REQUIRED"
    assert queue.loc[0, "recovery_class"] == "OFFICIAL_RECOVERY_REQUIRED"
    assert queue.loc[0, "resolution_status"] != "UNKNOWN"
