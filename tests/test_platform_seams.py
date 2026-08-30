"""CRPD platform seams — resolution probe + deterministic adapter behavior.

Import-only for DB/network-touching seams (probe covers resolution); pure
deterministic adapters are executed directly.
"""
from __future__ import annotations

from datetime import date

from policydb.crawl.models import RegisteredSource
from policydb.platform.seams import (
    SEAM_MAP,
    classify_actions,
    deduplicate,
    deduplicate_pair,
    discover_sources,
    extract_actions,
    probe_seams,
    seam_status,
)


def _source() -> RegisteredSource:
    return RegisteredSource(
        source_id="TEST_SRC_001",
        source_name="测试市住房和城乡建设局",
        domain="zjj.test.example.gov.cn",
        source_type="government",
        source_role="official",
        official_status="official",
        list_page_urls=[
            "https://zjj.test.example.gov.cn/zcfg/list.html",
            "https://zjj.test.example.gov.cn/zcfg/2022/index.html",
        ],
    )


def test_probe_resolves_all_implemented_seams():
    report = probe_seams()
    for name, entry in report.items():
        if entry["status"] == "IMPLEMENTED":
            assert entry["resolves"], f"{name} did not resolve: {entry}"
        else:
            assert entry["resolves"] is False
            assert not entry.get("missing_symbols")


def test_implemented_seam_count():
    status = seam_status()
    assert status["extract_actions"] == "IMPLEMENTED"
    assert sum(v == "IMPLEMENTED" for v in status.values()) == 12
    assert sum(v == "PARTIAL" for v in status.values()) == 0


def test_extract_actions_empty_document_returns_no_candidates():
    assert extract_actions({"document_type": "html"}) == []


def test_extract_actions_returns_deterministic_multi_action_candidates_without_ai():
    rows = extract_actions(
        {
            "record_id": "R_MULTI",
            "document_version_id": "DV_MULTI",
            "title": "关于调整住房政策的通知",
            "official_status": "official",
            "full_text": (
                "本通知自2026年9月1日起施行。购买首套住房最低首付款比例提高至20%；"
                "发放购房补贴每套2万元。"
            ),
        }
    )

    assert len(rows) == 2
    assert {row["instrument"] for row in rows} == {
        "mortgage_downpayment",
        "purchase_subsidy",
    }
    assert all(row["extraction_method"] == "deterministic_rule" for row in rows)
    assert all(row["evidence_text"] for row in rows)


def test_classify_actions_deterministic_keyword_rule():
    rows = classify_actions(
        [
            {
                "action_id": "A1",
                "record_id": "R1",
                "instrument": "mortgage_downpayment",
                "clause_text": "预售资金监管比例调整为30%",
            },
            {
                "action_id": "A2",
                "record_id": "R1",
                "instrument": "mortgage_downpayment",
                "clause_text": "购买首套住房最低首付比例调整为20%",
            },
        ]
    )
    assert rows[0]["primary_category"] == "F"
    assert rows[0]["secondary_category"] == "F06"
    assert rows[0]["classification_source"] == "deterministic_rule"
    assert rows[1]["primary_category"] == "D"
    assert rows[1]["secondary_category"] == "D04"
    assert rows[1]["confidence"] == 0.90


def test_classify_actions_unmapped_keeps_empty_categories():
    rows = classify_actions(
        [{"action_id": "A3", "record_id": "R2", "instrument": "", "clause_text": "其他事项"}]
    )
    assert rows[0]["primary_category"] is None
    assert rows[0]["confidence"] == 0.0
    assert rows[0]["decision_reason"] == "unmapped"


def test_deduplicate_pair_identical_content():
    decision = deduplicate_pair("关于促进房地产市场平稳健康发展的通知", "关于促进房地产市场平稳健康发展的通知")
    assert decision.decision == "duplicate_content"
    assert decision.level == "L4"


def test_deduplicate_pair_numeric_conflict_is_material_change():
    decision = deduplicate_pair(
        "首套住房首付比例调整为20%",
        "首套住房首付比例调整为30%",
        left_numbers=["20%"],
        right_numbers=["30%"],
    )
    assert decision.decision == "material_change"
    assert decision.level == "L6"


def test_deduplicate_driver_canonicalizes_and_keys():
    rows = deduplicate(
        [
            {
                "item_id": "I1",
                "url": "https://www.zjj.test.example.gov.cn/zcfg/2022/a.html?utm_source=x",
                "title": "关于XX的通知",
                "document_number": "建房〔2022〕1号",
            }
        ]
    )
    assert rows[0]["canonical_url"] == "https://zjj.test.example.gov.cn/zcfg/2022/a.html"
    assert len(rows[0]["identity_key"]) == 64
    assert all(c in "0123456789abcdef" for c in rows[0]["identity_key"])
    assert rows[0]["item_id"] == "I1"


def test_discover_sources_seed_items_shape_and_window_filter():
    items = discover_sources(
        _source(), "RUN_TEST", city_id="110100", end_date=date(2021, 12, 31)
    )
    urls = {item["url"] for item in items}
    # 2022 index page must be excluded by the end-date window; list page kept.
    assert "https://zjj.test.example.gov.cn/zcfg/list.html" in urls
    assert "https://zjj.test.example.gov.cn/zcfg/2022/index.html" not in urls
    for item in items:
        assert item["item_id"].startswith("CRAWLITEM")
        assert item["status"] == "pending"
        assert item["canonical_url"] == item["url"].lower().replace("www.", "")


def test_seam_map_has_exactly_twelve_core_interfaces():
    assert len(SEAM_MAP) == 12
    expected = {
        "discover_sources",
        "validate_source",
        "plan_crawl",
        "fetch_document",
        "extract_document",
        "extract_actions",
        "classify_actions",
        "deduplicate",
        "evaluate_coverage",
        "recover_gaps",
        "promote",
        "release",
    }
    assert set(SEAM_MAP) == expected
