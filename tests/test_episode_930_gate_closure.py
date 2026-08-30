from __future__ import annotations

import polars as pl

from policydb.episode_930_gate_closure import close_action_gates, derive_direction, derive_geography


def test_direction_uses_explicit_lexicon_with_negation() -> None:
    assert derive_direction({"action_text": "提高首付款比例"})["direction_state"] == "TIGHTENING"
    assert derive_direction({"action_text": "不得降低首付款比例"})["direction_state"] == "TIGHTENING"
    assert derive_direction({"action_text": "降低首付款比例"})["direction_state"] == "SUPPORTIVE"


def test_direction_keeps_mixed_evidence_unresolved() -> None:
    result = derive_direction({"action_text": "提高首付款，同时降低利率"})
    assert result["direction_state"] == "UNKNOWN"
    assert result["direction_source"] == "LEXICAL_CONFLICT"


def test_direction_parameter_comparison_requires_same_unit() -> None:
    resolved = derive_direction(
        {
            "policy_type": "PF_DOWNPAYMENT",
            "old_value": "20",
            "new_value": "30",
            "unit": "%",
            "action_text": "调整首付款比例",
        }
    )
    assert resolved["direction_state"] == "TIGHTENING"
    ambiguous = derive_direction(
        {
            "policy_type": "PF_DOWNPAYMENT",
            "old_value": "20",
            "new_value": "30万元",
            "unit": "%",
            "action_text": "调整首付款比例",
        }
    )
    assert ambiguous["direction_state"] == "UNKNOWN"


def test_geography_prefers_explicit_action_scope() -> None:
    result = derive_geography(
        {
            "action_text": "中心城区执行本条规定",
        },
        {"city": "示例市", "official_url": "https://example.gov.cn/policy"},
    )
    assert result["geography_state"] == "CITY_LEVEL"
    assert result["geography_source"] == "EXPLICIT_ACTION"


def test_close_action_gates_adds_auditable_fields_without_external_calls() -> None:
    actions = pl.DataFrame(
        [
            {
                "action_id": "ACT1",
                "document_id": "DOC1",
                "action_text": "调整首付款比例",
                "action_direction": "UNKNOWN",
                "geographic_scope": None,
                "policy_type": "PF_DOWNPAYMENT",
                "old_value": "20",
                "new_value": "30",
                "unit": "%",
            }
        ]
    )
    documents = pl.DataFrame(
        [
            {
                "document_id": "DOC1",
                "city": "示例市",
                "document_title": "示例市住房政策",
                "issuer": "示例市住房和城乡建设局",
                "official_url": "https://example.gov.cn/policy",
                "official_text": "本市执行相关规定",
            }
        ]
    )
    result = close_action_gates(actions, documents).row(0, named=True)
    assert result["action_direction"] == "TIGHTENING"
    assert result["geographic_scope"] == "CITY_LEVEL"
    assert result["direction_source"] == "PARAMETER_COMPARISON_SAME_UNIT"
    assert result["geography_source"] == "EXPLICIT_ACTION_OR_DOCUMENT"
