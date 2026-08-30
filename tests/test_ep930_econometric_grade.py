from __future__ import annotations

import pandas as pd

from scripts.build_ep930_econometric_grade import (
    EPISODE_ID,
    _date_value,
    _normalize_candidates,
)


def _scope() -> dict[str, dict[str, str]]:
    return {
        "core_window": {"start": "2016-09-25", "end": "2016-10-10"},
        "extended_window": {"start": "2016-09-20", "end": "2016-10-15"},
        "provenance_window": {"start": "2016-09-01", "end": "2016-10-31"},
    }


def _actions(**overrides: object) -> pd.DataFrame:
    row: dict[str, object] = {
        "episode_id": EPISODE_ID,
        "document_id": "D1",
        "action_id": "A1",
        "city": "北京",
        "province": "北京",
        "action_text": "提高二套房首付比例",
        "policy_type": "COMMERCIAL_DOWNPAYMENT",
        "action_direction": "TIGHTENING",
        "geographic_scope": "全市",
        "announcement_date": "2016-09-30",
        "dedup_status": "canonical",
    }
    row.update(overrides)
    return pd.DataFrame([row])


def _documents() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "episode_id": EPISODE_ID,
                "document_id": "D1",
                "official_url": "https://example.gov.cn/policy/1",
                "official_source": "OFFICIAL_POLICY",
                "official_evidence_status": "CURATED_OFFICIAL",
            }
        ]
    )


def _api_rows() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"action_id": "A1", "pass_name": "first_pass", "status": "SUCCESS"},
            {"action_id": "A1", "pass_name": "second_pass", "status": "SUCCESS"},
        ]
    )


def _normalize(actions: pd.DataFrame, api: pd.DataFrame | None = None) -> pd.DataFrame:
    return _normalize_candidates(
        actions,
        _documents(),
        pd.DataFrame(),
        pd.DataFrame(),
        _api_rows() if api is None else api,
        pd.DataFrame(),
        _scope(),
    )


def test_date_value_preserves_invalid_or_missing_as_none() -> None:
    assert _date_value(None) is None
    assert _date_value("not-a-date") is None
    assert _date_value("2016-09-30") == "2016-09-30"


def test_missing_api_passes_cannot_enter_formal_master() -> None:
    result = _normalize(_actions(), api=pd.DataFrame())

    assert result.loc[0, "treatment_status"] == "MANUAL_REVIEW_REQUIRED"
    assert "API_PASS1_INCOMPLETE" in result.loc[0, "exclusion_reason"]
    assert "API_PASS2_INCOMPLETE" in result.loc[0, "exclusion_reason"]


def test_both_api_passes_and_deterministic_gates_allow_formal_candidate() -> None:
    result = _normalize(_actions())

    assert result.loc[0, "treatment_status"] == "CONFIRMED_INCLUDED"
    assert result.loc[0, "date_state"] == "NO_EXPLICIT_EFFECTIVE_DATE"
    assert bool(result.loc[0, "official_evidence"])


def test_later_policy_is_excluded_from_2016_episode() -> None:
    result = _normalize(_actions(announcement_date="2017-01-05"))

    assert result.loc[0, "treatment_status"] == "MANUAL_REVIEW_REQUIRED"
    assert "2017_POLICY" in result.loc[0, "exclusion_reason"]


def test_missing_event_date_is_explicit_unknown_state() -> None:
    result = _normalize(_actions(announcement_date=None))

    assert result.loc[0, "date_state"] == "UNKNOWN_DATE_STATE"
    assert "UNKNOWN_DATE_STATE" in result.loc[0, "exclusion_reason"]
