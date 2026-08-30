"""CRPD deterministic extract_actions regressions.

Covers the required gates: real-shaped documents, multi-action documents,
candidate extraction without AI, evidence spans, date/geography/parameter
linkage, negation/reversal context, no-action documents, duplicate control,
and AI-unavailable graceful degradation (the extractor never calls AI).

Clause excerpts are real clauses from official policy texts (six-city
closeout evidence and 930-class official notices).
"""
from __future__ import annotations

from pathlib import Path

import pytest

from policydb.intensity.models import PolicyAction
from policydb.intensity.rules import DeterministicPolicyRules, split_clauses
from policydb.platform.seams import extract_actions as seam_extract_actions

REFERENCE = Path(__file__).resolve().parents[1] / "data" / "reference"

# Real clauses from official documents (six-city closeout evidence).
REAL_LIULIANG = "缴存人连续足额缴存住房公积金6个月(含)以上，即可申请住房公积金贷款；取消夫妻离异一年内购买住房不得申请住房公积金提取和贷款的限制"
REAL_CHANGZHI = "取消“账户余额≥6个月缴存额”贷款条件，二手房认定标准调整"
REAL_XINZHOU = "第(十九)条 首套商贷最低首付20%；第(二十)条 首套新建商品房契税补贴（90㎡以下全额/90-143㎡ 80%/143㎡+ 50%）；第(二十三)(二十四)条 公积金贷款最高额度提高至80万、公积金首付20%"
REAL_LINFEN = "本通知自2022年6月3日起施行。对购买首套新建商品住房的购房人，给予契税补贴。"
REAL_NO_ACTION = "各区人民政府，市政府各部门：现将《关于成立市房地产工作领导小组的通知》印发给你们，请认真贯彻执行。"


@pytest.fixture(scope="module")
def rules() -> DeterministicPolicyRules:
    return DeterministicPolicyRules(REFERENCE)


def _extract(rules, text: str, *, title: str = "关于促进房地产市场平稳健康发展的通知",
             official: str = "official", record_id: str = "R_VAL") -> list[PolicyAction]:
    return rules.extract_actions(
        record_id=record_id,
        text=text,
        title=title,
        official_status=official,
    )


def test_gate_candidate_extraction_without_ai(rules):
    """The deterministic layer emits candidates with no AI involvement."""
    actions = _extract(rules, REAL_LIULIANG + "。" + REAL_CHANGZHI)
    assert actions, "expected candidates from real clauses"
    assert all(a.extraction_method == "rule_v1" for a in actions)
    assert all(a.rules_version for a in actions)


def test_gate_multi_action_document(rules):
    actions = _extract(rules, REAL_XINZHOU)
    instruments = {a.instrument for a in actions}
    assert len(actions) >= 3, f"expected >=3 candidates, got {len(actions)}"
    assert "mortgage_downpayment" in instruments
    assert "provident_fund" in instruments
    assert "purchase_subsidy" in instruments


def test_gate_evidence_spans_and_mentions(rules):
    actions = _extract(rules, REAL_LINFEN)
    assert actions
    for action in actions:
        assert action.evidence_end > action.evidence_start
        assert action.evidence_start >= 0
        assert action.evidence_text
        # date mention linkage (施行 date present in the document)
        if "契税补贴" in action.clause_text:
            assert any(m["kind"] == "date" for m in action.mentions)


def test_gate_negation_context(rules):
    """取消限购 must NOT be read as tightening; negation terms recorded."""
    text = "取消限购政策，不再执行住房套数限制。"
    actions = _extract(rules, text)
    assert actions
    restriction = next(a for a in actions if a.instrument == "purchase_restriction")
    assert restriction.direction != "tightening"
    assert restriction.negation_terms, "negation context must be recorded"
    assert any("取消" in term or "不再" in term for term in restriction.negation_terms)


def test_gate_quota_raise_is_supportive(rules):
    text = "住房公积金贷款最高额度提高至80万元。"
    actions = _extract(rules, text)
    assert actions
    fund = next(a for a in actions if a.instrument == "provident_fund")
    assert fund.direction == "supportive"
    assert any(m["kind"] == "parameter" and "80" in m["text"] for m in fund.mentions)


def test_gate_no_action_document(rules):
    actions = _extract(rules, REAL_NO_ACTION)
    assert actions == []


def test_gate_duplicate_candidate_control(rules):
    text = "首套住房最低首付比例调整为20%。首套住房最低首付比例调整为20%。"
    actions = _extract(rules, text)
    downpayments = [a for a in actions if a.instrument == "mortgage_downpayment"]
    assert len(downpayments) == 1, f"duplicate candidate not controlled: {len(downpayments)}"


def test_gate_numbered_clause_splitting(rules):
    clauses = split_clauses("（一）首付比例调整为20%。（二）公积金额度提高至80万元。", record_id="R_NUM")
    numbers = [c.number for c in clauses if c.number]
    assert numbers, "numbered markers must be recognized"
    assert any("（一）" in n for n in numbers)
    assert any("（二）" in n for n in numbers)


def test_gate_ai_unavailable_graceful_degradation():
    """The seam runs with no AI configuration at all — degradation is inherent."""
    rows = seam_extract_actions(
        {
            "record_id": "R_DEGRADE",
            "document_version_id": "DV_1",
            "title": "关于调整住房公积金政策的通知",
            "official_status": "official",
            "full_text": "住房公积金贷款最高额度提高至80万元。",
        }
    )
    assert rows, "seam must produce candidates without AI"
    assert all(row["extraction_method"] == "deterministic_rule" for row in rows)


def test_gate_single_action_policy(rules):
    text = "自2026年1月1日起，首套住房商业贷款最低首付比例调整为15%。"
    actions = _extract(rules, text)
    assert len(actions) == 1
    assert actions[0].instrument == "mortgage_downpayment"
