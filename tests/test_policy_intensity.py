from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest

from policydb.crawl.models import RegisteredSource
from policydb.intensity.aggregate import (
    apply_revision_stock,
    coverage_intensity_value,
    deduplicate_policy_actions,
    unique_model_input_count,
)
from policydb.intensity.glm import GLMActionAssessment, _cache_key, _validate_spans
from policydb.intensity.models import DecisionCandidate, GLMIntensityRubric
from policydb.intensity.router import HybridDecisionRouter
from policydb.intensity.rules import DeterministicPolicyRules, classify_text_completeness
from policydb.intensity.scoring import aggregate_action_score, score_dimensions
from policydb.settings import Settings


@pytest.fixture(scope="module")
def rules(root):
    return DeterministicPolicyRules(root / "data" / "reference")


def _action(rules, text, *, title="关于优化住房政策的通知", record_id="R1"):
    actions = rules.extract_actions(
        record_id=record_id,
        text=text,
        title=title,
        official_status="official",
    )
    assert actions
    return actions[0]


def _candidate(method, value, confidence=0.9):
    return DecisionCandidate(
        method=method,
        value=value,
        confidence=confidence,
        evidence_text="首付由30%降至20%",
        evidence_start=0,
        evidence_end=12,
    )


def test_rule_svm_transformer_and_glm_consensus():
    decision = HybridDecisionRouter().route(
        record_id="R1",
        task_name="direction",
        candidates=[
            _candidate("rule_direction", "loosening"),
            _candidate("model:svm", "loosening"),
            _candidate("model:transformer", "loosening"),
            _candidate("model:glm", "loosening"),
        ],
    )
    assert decision.accepted_value == "loosening"
    assert decision.agreement == 1
    assert not decision.review_required


def test_glm_conflict_cannot_override_numeric_rule():
    decision = HybridDecisionRouter().route(
        record_id="R1",
        task_name="numeric_pair",
        candidates=[_candidate("rule_numeric", [0.3, 0.2]), _candidate("model:glm", [0.3, 0.25])],
    )
    assert decision.accepted_value == [0.3, 0.2]
    assert decision.accepted_method == "deterministic_rule"
    assert decision.review_required


def test_transformer_glm_direction_conflict_requires_review():
    decision = HybridDecisionRouter().route(
        record_id="R1",
        task_name="direction",
        candidates=[_candidate("model:transformer", "loosening"), _candidate("model:glm", "tightening")],
    )
    assert decision.review_required
    assert decision.decision_reason == "model_disagreement"


def test_glm_rubric_without_evidence_is_invalid():
    with pytest.raises(ValueError, match="evidence"):
        GLMIntensityRubric(
            dimension_code="D1", rubric_value=2, confidence=0.9
        )
    with pytest.raises(ValueError, match="evidence"):
        GLMActionAssessment(is_policy_action=True, confidence=0.9)


def test_glm_offset_is_repaired_only_for_unique_verbatim_evidence():
    assessment = GLMActionAssessment(
        is_policy_action=True,
        evidence_text="降低首付",
        evidence_start=99,
        evidence_end=103,
        confidence=0.9,
    )
    _validate_spans("本市决定降低首付比例。", assessment)
    assert assessment.evidence_start == 4
    assert assessment.evidence_end == 8
    ambiguous = assessment.model_copy(update={"evidence_start": 99, "evidence_end": 103})
    with pytest.raises(ValueError, match="unique"):
        _validate_spans("降低首付，并继续降低首付。", ambiguous)


def test_one_document_can_contain_multiple_actions(rules):
    text = "本市降低首套住房首付比例。提高住房公积金贷款额度。"
    actions = rules.extract_actions(
        record_id="R-MULTI", text=text, title="通知", official_status="official"
    )
    assert len(actions) == 2
    assert {item.instrument for item in actions} == {"mortgage_downpayment", "provident_fund"}


def test_policy_interpretation_is_negative_example(rules):
    actions = rules.extract_actions(
        record_id="R-NEWS",
        text="有关负责人介绍，本次政策降低首付比例。",
        title="政策解读：关于住房政策的说明",
        official_status="official",
    )
    assert actions == []


def test_official_reprint_does_not_duplicate_action():
    actions = [
        {"action_id": "A1", "record_id": "R1", "policy_entity_id": "P1", "instrument": "mortgage", "direction": "loosening", "clause_text": "降低首付", "source_priority": 5},
        {"action_id": "A2", "record_id": "R2", "policy_entity_id": "P1", "instrument": "mortgage", "direction": "loosening", "clause_text": "降低首付", "source_priority": 4},
    ]
    result = deduplicate_policy_actions(actions)
    assert len(result) == 1
    assert result[0]["action_id"] == "A1"


def test_revision_updates_stock_instead_of_adding_duplicate():
    result = apply_revision_stock([
        {"action_id": "A1", "action_family_id": "F1", "effective_date": "2024-01-01", "intensity": 0.4, "status": "active"},
        {"action_id": "A2", "action_family_id": "F2", "target_action_family_id": "F1", "relation_type": "replaces", "effective_date": "2024-02-01", "intensity": 0.7, "status": "active"},
    ])
    assert result[-1]["active_action_count"] == 1
    assert result[-1]["stock_intensity"] == pytest.approx(0.7)


@pytest.mark.parametrize(
    ("text", "measure", "old", "new", "direction"),
    [
        ("最低首付由30%降至20%。", "mortgage_downpayment", 0.30, 0.20, "loosening"),
        ("公积金贷款额度从60万元提高至100万元。", "provident_fund_quota", 600000, 1000000, "supportive"),
        ("购房所需社保由5年降至3年。", "social_security_years", 5, 3, "loosening"),
    ],
)
def test_numeric_calibration_pairs(rules, text, measure, old, new, direction):
    action = _action(rules, text)
    calibration = rules.extract_calibrations(action)[0]
    assert calibration.measure_type == measure
    assert calibration.old_value == pytest.approx(old)
    assert calibration.new_value == pytest.approx(new)
    assert calibration.direction == direction
    assert calibration.magnitude is not None


def test_stronger_wording_without_numeric_change_has_no_calibration_magnitude(rules):
    action = _action(rules, "必须严格执行最低首付比例30%。")
    calibration = rules.extract_calibrations(action)[0]
    assert calibration.pairing_status == "single_value"
    assert calibration.magnitude is None


def test_not_applicable_dimensions_are_excluded_from_denominator(rules):
    action = _action(rules, "支持降低首套住房首付比例。")
    dimensions = score_dimensions(action, [])
    score = aggregate_action_score(action, dimensions, source_authority=1.0, data_quality=1.0)
    applicable = [d.mapped_score for d in dimensions if d.dimension_code != "D8" and d.applicable]
    assert score.textual_policy_design_intensity == pytest.approx(sum(applicable) / len(applicable))


def test_same_rule_model_run_is_stable(rules):
    kwargs = dict(record_id="R-STABLE", text="降低首套住房首付比例。", title="通知", official_status="official")
    first = rules.extract_actions(**kwargs)
    second = rules.extract_actions(**kwargs)
    assert [item.action_id for item in first] == [item.action_id for item in second]


def test_prompt_version_changes_cache_key():
    assert _cache_key("正文", "glm", "prompt-v1", "1") != _cache_key("正文", "glm", "prompt-v2", "1")


def test_model_upgrade_does_not_share_prediction_key():
    assert _cache_key("正文", "glm-4", "p", "1") != _cache_key("正文", "glm-5", "p", "1")


def test_city_expansion_does_not_repeat_model_calls():
    actions = [{"action_id": "A1"}, {"action_id": "A2"}]
    links = [{"action_id": "A1", "city_id": f"C{i}"} for i in range(105)]
    assert unique_model_input_count(actions, links) == 2


def test_not_scanned_intensity_is_null():
    assert coverage_intensity_value("not_scanned", action_count=0, gross_intensity=0) is None


def test_confirmed_zero_intensity_is_zero():
    assert coverage_intensity_value("confirmed_zero", action_count=0, gross_intensity=None) == 0


def test_all_curated_sources_validate_current_pydantic(root):
    frame = pl.read_parquet(root / "data" / "curated" / "source_registry.parquet")
    for row in frame.iter_rows(named=True):
        RegisteredSource.model_validate({key: value for key, value in row.items() if key in RegisteredSource.model_fields})
    assert frame.height == 816


def test_dashboard_does_not_import_training_framework(root):
    dashboard = (root / "app" / "dashboard.py").read_text(encoding="utf-8")
    assert "import torch" not in dashboard
    assert "import transformers" not in dashboard
    assert "train_transformer(" not in dashboard


def test_text_completeness_is_conservative():
    assert classify_text_completeness("", official_status="official") == "missing_text"
    assert classify_text_completeness("简短摘要", official_status="official") == "title_abstract_only"
    assert classify_text_completeness("媒体摘要" * 1000, official_status="general_media") == "third_party_summary"


def test_model_prediction_value_is_json_serializable():
    value = json.dumps({"direction": "loosening"}, ensure_ascii=False)
    assert json.loads(value)["direction"] == "loosening"


def test_settings_discovery_does_not_require_ml_dependency(root):
    settings = Settings.discover(root)
    assert settings.root == Path(root)
