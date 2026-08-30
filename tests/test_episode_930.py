from datetime import date

import polars as pl

from policydb.dashboard_policy_data import DashboardPolicyData
from policydb.episode_930 import (
    CORE_END,
    CORE_START,
    EPISODE_DIRECTION,
    EPISODE_ID,
    POLICY_TOOLS,
    _action_direction,
    _extract_number_pair,
    _mechanisms,
    _official_url,
    _parse_effective,
)


def test_episode_scope_and_policy_tool_contract() -> None:
    assert EPISODE_ID == "EP_2016_930_TIGHTENING"
    assert EPISODE_DIRECTION == "TIGHTENING"
    assert CORE_START == date(2016, 9, 25)
    assert CORE_END == date(2016, 10, 10)
    assert len(POLICY_TOOLS) == 9
    assert "LIMIT_PURCHASE" in POLICY_TOOLS
    assert "MARKET_SUPERVISION" in POLICY_TOOLS


def test_official_domain_gate_does_not_accept_media_or_invalid_urls() -> None:
    assert _official_url("https://www.beijing.gov.cn/policy/1")
    assert _official_url("http://gov.cn/policy/1")
    assert not _official_url("https://example.com/policy/1")
    assert not _official_url("not-a-url")


def test_action_can_have_multiple_mechanisms_but_one_episode_direction() -> None:
    text = "提高二套住房首付款比例，加强预售资金监管"
    mechanisms = _mechanisms(text)
    assert "COMMERCIAL_DOWNPAYMENT" in mechanisms
    assert "MARKET_SUPERVISION" in mechanisms
    assert _action_direction(text) == "TIGHTENING"
    assert EPISODE_DIRECTION != _action_direction("增加住宅用地供应")


def test_effective_date_requires_explicit_wording() -> None:
    publication = date(2016, 10, 1)
    effective, confidence = _parse_effective("本通知自2016年10月8日起施行", publication)
    assert effective == date(2016, 10, 8)
    assert confidence == "HIGH"
    no_date, low_confidence = _parse_effective("本通知发布后请认真执行", publication)
    assert no_date is None
    assert low_confidence == "LOW"


def test_old_new_parser_preserves_unit_and_does_not_infer_missing_pair() -> None:
    old_value, new_value, unit = _extract_number_pair("首付比例由30%提高至40%")
    assert old_value == "30%"
    assert new_value == "40%"
    assert unit == "%"
    assert _extract_number_pair("只规定首付比例提高") == (None, None, None)


def test_dashboard_episode_filter_exposes_historical_records() -> None:
    data = object.__new__(DashboardPolicyData)
    data.mode = "curated_fallback"
    data.db = None
    data.query_failures = []
    data.read_failures = []
    data.query_modes = {}
    data.used_last_good_snapshot = False
    data._curated_index = lambda: pl.DataFrame(
        {
            "record_id": ["R930"],
            "record_date": [date(2016, 10, 1)],
            "title": ["930调控通知"],
            "summary": [""],
            "official_status": ["official"],
            "manual_review_status": [None],
            "primary_source_url": ["https://www.example.gov.cn/930"],
            "province": ["北京"],
            "city": ["北京市"],
            "district": [None],
            "primary_category_code": [None],
            "secondary_category_code": [None],
            "instrument_type": [None],
            "direction": ["TIGHTENING"],
            "classification_confidence": [None],
            "classification_review_status": [None],
            "has_pdf": [False],
            "episode_id": [EPISODE_ID],
            "episode_name": ["2016年930楼市调控潮"],
        }
    )

    frame, total = data.search({"episode_id": EPISODE_ID}, page=1, page_size=10)

    assert total == 1
    assert frame.get_column("record_id").to_list() == ["R930"]
