from __future__ import annotations

import hashlib
import re
from datetime import UTC, datetime

from policydb.intensity.models import (
    ActionCalibration,
    DimensionScore,
    PolicyAction,
    PolicyIntensityScore,
)

DIMENSIONS = {
    "D1": "objective_specificity",
    "D2": "scope",
    "D3": "integration_coordination",
    "D4": "resource_commitment",
    "D5": "implementation_procedure",
    "D6": "monitoring_accountability",
    "D7": "bindingness",
    "D8": "calibration_magnitude",
}


def _id(*parts: object, prefix: str) -> str:
    digest = hashlib.sha256("|".join(str(part or "") for part in parts).encode()).hexdigest()[:24]
    return f"{prefix}_{digest}"


def _rubric(action: PolicyAction, code: str) -> tuple[int | None, bool, str | None]:
    text = action.clause_text
    if code == "D1":
        specific = bool(re.search(r"(目标|达到|完成|实现).{0,16}\d", text))
        scoped = any(word in text for word in ("家庭", "居民", "人才", "项目", "企业", "住房"))
        return (3 if specific and scoped else 2 if specific or scoped else 1, True, text)
    if code == "D2":
        groups = sum(word in text for word in ("本市", "全市", "区", "县", "家庭", "居民", "人才", "首套", "二套"))
        return (min(3, groups) if groups else 1, True, text)
    if code == "D3":
        agencies = len(re.findall(r"[、和及](?:住建|财政|自然资源|税务|银行|公积金)|部门联动|协调机制", text))
        if not agencies and not any(word in text for word in ("协同", "联合", "联动", "协调")):
            return (None, False, None)
        return (3 if "分工" in text and agencies else 2 if agencies else 1, True, text)
    if code == "D4":
        has_resource = bool(re.search(r"\d+(?:\.\d+)?\s*(?:%|％|万?元|亿?元|套|户|平方米)", text))
        resource_type = any(word in text for word in ("资金", "额度", "土地", "住房", "授信", "补贴"))
        if not resource_type and not has_resource:
            return (None, False, None)
        return (3 if has_resource and resource_type else 2, True, text)
    if code == "D5":
        steps = sum(word in text for word in ("申请", "审核", "审批", "办理", "申报", "实施", "负责"))
        deadline = bool(re.search(r"(?:自|于|截至).{0,12}(?:年|月|日)|\d+个工作日", text))
        if not steps and not deadline:
            return (None, False, None)
        return (3 if steps >= 2 and deadline else 2 if steps or deadline else 1, True, text)
    if code == "D6":
        monitors = sum(word in text for word in ("监测", "监督", "报告", "考核", "问责", "评估", "通报"))
        if not monitors:
            return (None, False, None)
        return (3 if monitors >= 2 and any(word in text for word in ("考核", "问责")) else 2, True, text)
    if code == "D7":
        if any(word in text for word in ("必须", "不得", "严禁", "一律", "严格执行")):
            return (3, True, text)
        if any(word in text for word in ("应当", "需要", "按照", "落实")):
            return (2, True, text)
        if any(word in text for word in ("鼓励", "支持", "引导", "推动", "探索")):
            return (1, True, text)
        return (0, True, text)
    raise ValueError(f"unsupported dimension: {code}")


def score_dimensions(action: PolicyAction, calibrations: list[ActionCalibration]) -> list[DimensionScore]:
    now = datetime.now(UTC).isoformat()
    scores: list[DimensionScore] = []
    for code in ["D1", "D2", "D3", "D4", "D5", "D6", "D7"]:
        value, applicable, evidence = _rubric(action, code)
        scores.append(
            DimensionScore(
                score_id=_id(action.action_id, code, "0.1.0-experimental", prefix="DIM"),
                action_id=action.action_id,
                record_id=action.record_id,
                dimension_code=code,
                dimension_name=DIMENSIONS[code],
                rubric_value=value,
                mapped_score=value / 3 if value is not None else None,
                applicable=applicable,
                evidence_text=evidence,
                evidence_start=action.evidence_start if evidence else None,
                evidence_end=action.evidence_end if evidence else None,
                scoring_method="deterministic_rubric_v1",
                decision_confidence=0.85 if evidence else 0.75,
                review_required=False,
                created_at=now,
            )
        )
    paired = [item for item in calibrations if item.pairing_status == "paired" and item.magnitude is not None]
    scores.append(
        DimensionScore(
            score_id=_id(action.action_id, "D8", "0.1.0-experimental", prefix="DIM"),
            action_id=action.action_id,
            record_id=action.record_id,
            dimension_code="D8",
            dimension_name=DIMENSIONS["D8"],
            rubric_value=None,
            mapped_score=max((item.magnitude or 0 for item in paired), default=None),
            applicable=bool(paired),
            evidence_text="；".join(item.evidence_text for item in paired) or None,
            evidence_start=min((item.evidence_start for item in paired), default=None),
            evidence_end=max((item.evidence_end for item in paired), default=None),
            scoring_method="deterministic_calibration_v1",
            decision_confidence=0.95 if paired else 0.8,
            review_required=any(item.review_required for item in calibrations),
            created_at=now,
        )
    )
    return scores


def aggregate_action_score(
    action: PolicyAction,
    dimensions: list[DimensionScore],
    *,
    source_authority: float,
    data_quality: float,
) -> PolicyIntensityScore:
    now = datetime.now(UTC).isoformat()
    qualitative = [
        item.mapped_score for item in dimensions if item.dimension_code != "D8" and item.applicable and item.mapped_score is not None
    ]
    calibration = next((item.mapped_score for item in dimensions if item.dimension_code == "D8" and item.applicable), None)
    qualitative_score = sum(qualitative) / len(qualitative) if qualitative else None
    implementation_items = [
        item.mapped_score
        for item in dimensions
        if item.dimension_code in {"D4", "D5", "D6", "D7"} and item.applicable and item.mapped_score is not None
    ]
    implementation = sum(implementation_items) / len(implementation_items) if implementation_items else None
    if qualitative_score is None:
        design = None
    elif calibration is None:
        design = qualitative_score
    else:
        design = 0.75 * qualitative_score + 0.25 * calibration
    formal = action.formal_eligible and design is not None
    confidence = min((item.decision_confidence for item in dimensions if item.applicable), default=0.0)
    return PolicyIntensityScore(
        score_id=_id(action.action_id, "composite", "0.1.0-experimental", prefix="SCORE"),
        record_id=action.record_id,
        action_id=action.action_id,
        textual_policy_design_intensity=design,
        textual_implementation_commitment_intensity=implementation,
        instrument_calibration_intensity=calibration,
        authority_adjusted_intensity=design * source_authority if design is not None else None,
        quality_adjusted_intensity=design * data_quality if design is not None else None,
        qualitative_dimension_count=len(qualitative),
        calibration_applicable=calibration is not None,
        formal_status="formal" if formal else "provisional" if design is not None else "not_scored",
        text_completeness=action.text_completeness,
        decision_confidence=confidence,
        review_required=any(item.review_required for item in dimensions),
        created_at=now,
    )
