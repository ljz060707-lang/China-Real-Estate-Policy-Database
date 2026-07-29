from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator

TextCompleteness = Literal[
    "full_official_text",
    "partial_official_text",
    "title_abstract_only",
    "third_party_summary",
    "missing_text",
]
DecisionMethod = Literal[
    "deterministic_rule",
    "model_consensus",
    "task_priority",
    "manual",
    "unresolved",
]


class PolicyAction(BaseModel):
    action_id: str
    record_id: str
    document_version_id: str | None = None
    clause_id: str
    clause_text: str
    evidence_start: int
    evidence_end: int
    instrument: str
    direction: Literal["tightening", "loosening", "supportive", "neutral", "mixed", "unknown"]
    action_status: Literal["active", "amended", "repealed", "expired", "provisional"] = "provisional"
    text_completeness: TextCompleteness
    formal_eligible: bool = False
    extraction_method: str = "rule_v1"
    rules_version: str = "1.0.0"
    evidence_text: str
    created_at: str
    updated_at: str


class ActionCalibration(BaseModel):
    calibration_id: str
    action_id: str
    record_id: str
    measure_type: str
    old_value: float | None = None
    new_value: float | None = None
    unit: str
    standardized_change: float | None = None
    magnitude: float | None = Field(default=None, ge=0, le=1)
    direction: str = "unknown"
    pairing_status: Literal["paired", "single_value", "ambiguous", "not_applicable"]
    evidence_text: str
    evidence_start: int
    evidence_end: int
    extraction_method: str = "deterministic_rule"
    rules_version: str = "1.0.0"
    review_required: bool = False
    created_at: str


class DimensionScore(BaseModel):
    score_id: str
    action_id: str
    record_id: str
    dimension_code: Literal["D1", "D2", "D3", "D4", "D5", "D6", "D7", "D8"]
    dimension_name: str
    rubric_value: int | None = Field(default=None, ge=0, le=3)
    mapped_score: float | None = Field(default=None, ge=0, le=1)
    applicable: bool = True
    evidence_text: str | None = None
    evidence_start: int | None = None
    evidence_end: int | None = None
    scoring_method: str
    model_agreement: float | None = Field(default=None, ge=0, le=1)
    decision_confidence: float = Field(ge=0, le=1)
    review_required: bool = False
    score_version: str = "0.1.0-experimental"
    created_at: str


class PolicyIntensityScore(BaseModel):
    score_id: str
    record_id: str
    action_id: str
    score_scope: Literal["action", "document"] = "action"
    textual_policy_design_intensity: float | None = Field(default=None, ge=0, le=1)
    textual_implementation_commitment_intensity: float | None = Field(default=None, ge=0, le=1)
    instrument_calibration_intensity: float | None = Field(default=None, ge=0, le=1)
    authority_adjusted_intensity: float | None = Field(default=None, ge=0, le=1)
    quality_adjusted_intensity: float | None = Field(default=None, ge=0, le=1)
    qualitative_dimension_count: int = 0
    calibration_applicable: bool = False
    weight_version: str = "equal_v1"
    score_version: str = "0.1.0-experimental"
    formal_status: Literal["formal", "provisional", "not_scored"] = "provisional"
    text_completeness: TextCompleteness
    decision_confidence: float = Field(ge=0, le=1)
    review_required: bool = False
    created_at: str


class ModelPrediction(BaseModel):
    prediction_id: str
    action_id: str | None = None
    record_id: str
    task_name: str
    model_name: str
    model_version: str
    prompt_version: str | None = None
    schema_version: str = "1.0.0"
    predicted_value: str | None = None
    confidence: float = Field(ge=0, le=1)
    evidence_text: str | None = None
    evidence_start: int | None = None
    evidence_end: int | None = None
    cache_key: str
    created_at: str


class DecisionCandidate(BaseModel):
    method: str
    value: Any = None
    confidence: float = Field(ge=0, le=1)
    evidence_text: str | None = None
    evidence_start: int | None = None
    evidence_end: int | None = None

    @property
    def has_evidence(self) -> bool:
        return bool(self.evidence_text) and self.evidence_start is not None and self.evidence_end is not None


class ModelDecision(BaseModel):
    decision_id: str
    action_id: str | None = None
    record_id: str
    task_name: str
    accepted_value: Any = None
    accepted_method: DecisionMethod
    agreement: float = Field(ge=0, le=1)
    decision_confidence: float = Field(ge=0, le=1)
    review_required: bool
    decision_reason: str
    router_version: str
    candidate_methods: list[str]
    fallback_path: str
    created_at: str


class GLMIntensityRubric(BaseModel):
    """Strict GLM output: rubric and evidence only, never a continuous index."""

    dimension_code: Literal["D1", "D2", "D3", "D4", "D5", "D6", "D7"]
    rubric_value: int | None = Field(default=None, ge=0, le=3)
    applicable: bool = True
    evidence_text: str | None = None
    evidence_start: int | None = None
    evidence_end: int | None = None
    confidence: float = Field(ge=0, le=1)
    needs_review: bool = False

    @model_validator(mode="after")
    def evidence_required_for_value(self) -> GLMIntensityRubric:
        if self.rubric_value is not None and not (
            self.evidence_text and self.evidence_start is not None and self.evidence_end is not None
        ):
            raise ValueError("rubric_value requires a verbatim evidence span")
        return self
