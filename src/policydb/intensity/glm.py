from __future__ import annotations

import hashlib
import json
import re
from datetime import UTC, datetime

import httpx
import polars as pl
from pydantic import BaseModel, Field, ValidationError, model_validator

from policydb.intensity.models import GLMIntensityRubric, ModelPrediction
from policydb.intensity.storage import upsert_parquet
from policydb.settings import Settings


class GLMActionAssessment(BaseModel):
    is_policy_action: bool
    instrument: str | None = None
    direction: str | None = None
    rubrics: list[GLMIntensityRubric] = Field(default_factory=list)
    evidence_text: str | None = None
    evidence_start: int | None = None
    evidence_end: int | None = None
    confidence: float = Field(ge=0, le=1)
    needs_review: bool = False

    @model_validator(mode="after")
    def action_requires_evidence(self) -> GLMActionAssessment:
        if self.is_policy_action and not (
            self.evidence_text and self.evidence_start is not None and self.evidence_end is not None
        ):
            raise ValueError("policy action requires verbatim evidence")
        return self


class GLMIndependentVerification(BaseModel):
    all_fields_have_evidence: bool
    action_supported: bool
    direction_supported: bool
    scope_over_inferred: bool = False
    numeric_conflict: bool = False
    conflicts: list[str] = Field(default_factory=list)
    evidence_text: str | None = None
    evidence_start: int | None = None
    evidence_end: int | None = None
    confidence: float = Field(ge=0, le=1)


SYSTEM_PROMPT = """你是政策文本证据抽取器。只输出符合schema的JSON。只使用输入原文，不能补写政策事实、数值、日期、机构或链接。rubric只能是0/1/2/3/NA；每个非NA值必须提供逐字证据和字符偏移。你不能生成连续政策强度总分。"""
VERIFY_PROMPT = """你是独立复核器。逐字段核验第一次结果是否有输入原文证据，检查政策动作、方向、适用范围过度推断和数值冲突。只报告证据和冲突，不能决定最终通过，不能生成连续强度分数。"""


def _cache_key(text: str, model: str, prompt_version: str, schema_version: str) -> str:
    content = "|".join((hashlib.sha256(text.encode()).hexdigest(), model, prompt_version, schema_version))
    return hashlib.sha256(content.encode()).hexdigest()


def _validate_spans(text: str, assessment: GLMActionAssessment) -> None:
    spans = [assessment, *assessment.rubrics]
    for item in spans:
        excerpt = item.evidence_text
        start = item.evidence_start
        end = item.evidence_end
        if excerpt is None:
            continue
        if start is not None and end is not None and start >= 0 and end <= len(text) and text[start:end] == excerpt:
            continue
        occurrences = [match.start() for match in re.finditer(re.escape(excerpt), text)]
        if len(occurrences) != 1:
            raise ValueError("GLM evidence span does not match a unique source span")
        item.evidence_start = occurrences[0]
        item.evidence_end = occurrences[0] + len(excerpt)


class GLMIntensityClient:
    schema_version = "1.0.0"
    extract_prompt_version = "intensity-extract-2026-07-22-v1"
    verify_prompt_version = "intensity-verify-2026-07-22-v1"

    def __init__(self, settings: Settings | None = None, *, client: httpx.Client | None = None) -> None:
        self.settings = settings or Settings.discover()
        self.api_key = self.settings.glm_api_key
        self.model = self.settings.glm_model
        self.client = client or httpx.Client(timeout=self.settings.request_timeout)
        self.predictions_path = self.settings.curated / "policy_model_predictions.parquet"
        self.usage = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}

    def _post(self, system: str, schema: dict, payload: str) -> dict:
        if not self.api_key:
            raise RuntimeError("GLM_API_KEY is not configured")
        response = self.client.post(
            self.settings.glm_base_url,
            headers={"Authorization": f"Bearer {self.api_key}"},
            json={
                "model": self.model,
                "response_format": {"type": "json_object"},
                "messages": [
                    {"role": "system", "content": system},
                    {"role": "user", "content": "schema=" + json.dumps(schema, ensure_ascii=False) + "\n" + payload},
                ],
            },
        )
        response.raise_for_status()
        payload = response.json()
        for key in self.usage:
            self.usage[key] += int((payload.get("usage") or {}).get(key) or 0)
        return json.loads(payload["choices"][0]["message"]["content"])

    def extract_action(self, *, record_id: str, action_id: str, text: str) -> GLMActionAssessment:
        result = GLMActionAssessment.model_validate(
            self._post(SYSTEM_PROMPT, GLMActionAssessment.model_json_schema(), "原文=" + text)
        )
        _validate_spans(text, result)
        now = datetime.now(UTC).isoformat()
        cache = _cache_key(text, self.model, self.extract_prompt_version, self.schema_version)
        rows = []
        for index, rubric in enumerate(result.rubrics):
            rows.append(
                ModelPrediction(
                    prediction_id=f"PRED_{cache[:20]}_{index}",
                    action_id=action_id,
                    record_id=record_id,
                    task_name=f"rubric_{rubric.dimension_code}",
                    model_name=self.model,
                    model_version=self.model,
                    prompt_version=self.extract_prompt_version,
                    predicted_value=json.dumps(rubric.rubric_value),
                    confidence=rubric.confidence,
                    evidence_text=rubric.evidence_text,
                    evidence_start=rubric.evidence_start,
                    evidence_end=rubric.evidence_end,
                    cache_key=cache,
                    created_at=now,
                )
            )
        rows.append(
            ModelPrediction(
                prediction_id=f"PRED_{cache[:20]}_assessment",
                action_id=action_id,
                record_id=record_id,
                task_name="action_assessment",
                model_name=self.model,
                model_version=self.model,
                prompt_version=self.extract_prompt_version,
                predicted_value=result.model_dump_json(),
                confidence=result.confidence,
                evidence_text=result.evidence_text,
                evidence_start=result.evidence_start,
                evidence_end=result.evidence_end,
                cache_key=cache,
                created_at=now,
            )
        )
        if rows:
            upsert_parquet(
                pl.DataFrame([row.model_dump(mode="json") for row in rows], infer_schema_length=None),
                self.predictions_path,
                "prediction_id",
            )
        return result

    def verify_action(self, text: str, assessment: GLMActionAssessment) -> GLMIndependentVerification:
        payload = "第一次抽取=" + assessment.model_dump_json() + "\n原文=" + text
        return GLMIndependentVerification.model_validate(
            self._post(VERIFY_PROMPT, GLMIndependentVerification.model_json_schema(), payload)
        )


def glm_extract_pending(settings: Settings | None = None, *, limit: int = 50) -> dict:
    settings = settings or Settings.discover()
    if not settings.glm_api_key:
        return {"status": "awaiting_api_key", "processed": 0, "failed": 0, "token_usage": None, "cost": None}
    path = settings.curated / "policy_actions.parquet"
    if not path.exists():
        return {"status": "blocked_missing_actions", "processed": 0, "failed": 0}
    actions = pl.read_parquet(path).filter(pl.col("formal_eligible")).head(limit)
    client = GLMIntensityClient(settings)
    processed = failed = 0
    errors: dict[str, int] = {}
    for row in actions.iter_rows(named=True):
        try:
            client.extract_action(record_id=row["record_id"], action_id=row["action_id"], text=row["clause_text"])
            processed += 1
        except (httpx.HTTPError, ValidationError, ValueError, KeyError, json.JSONDecodeError) as exc:
            failed += 1
            if isinstance(exc, httpx.HTTPStatusError):
                error_type = f"http_{exc.response.status_code}"
            elif isinstance(exc, ValidationError):
                error_type = "schema_validation"
            elif isinstance(exc, json.JSONDecodeError):
                error_type = "invalid_json"
            elif isinstance(exc, ValueError):
                error_type = "evidence_span_validation"
            else:
                error_type = type(exc).__name__
            errors[error_type] = errors.get(error_type, 0) + 1
    return {"status": "completed" if not failed else "completed_with_warnings", "processed": processed, "failed": failed, "error_types": errors, "token_usage": client.usage, "cost": None}


def glm_verify_pending(settings: Settings | None = None, *, limit: int = 50) -> dict:
    settings = settings or Settings.discover()
    if not settings.glm_api_key:
        return {"status": "awaiting_api_key", "processed": 0, "failed": 0, "token_usage": None, "cost": None}
    predictions_path = settings.curated / "policy_model_predictions.parquet"
    actions_path = settings.curated / "policy_actions.parquet"
    if not predictions_path.exists() or not actions_path.exists():
        return {"status": "blocked_missing_extractions", "processed": 0, "failed": 0}
    predictions = pl.read_parquet(predictions_path).filter(pl.col("task_name") == "action_assessment").head(limit)
    actions = pl.read_parquet(actions_path).select("action_id", "clause_text")
    work = predictions.join(actions, on="action_id", how="inner")
    client = GLMIntensityClient(settings)
    processed = failed = 0
    errors: dict[str, int] = {}
    rows = []
    now = datetime.now(UTC).isoformat()
    for row in work.iter_rows(named=True):
        try:
            assessment = GLMActionAssessment.model_validate_json(row["predicted_value"])
            result = client.verify_action(row["clause_text"], assessment)
            cache = _cache_key(row["clause_text"], client.model, client.verify_prompt_version, client.schema_version)
            rows.append(
                ModelPrediction(
                    prediction_id=f"PRED_{cache[:20]}_verification",
                    action_id=row["action_id"],
                    record_id=row["record_id"],
                    task_name="independent_verification",
                    model_name=client.model,
                    model_version=client.model,
                    prompt_version=client.verify_prompt_version,
                    predicted_value=result.model_dump_json(),
                    confidence=result.confidence,
                    evidence_text=result.evidence_text,
                    evidence_start=result.evidence_start,
                    evidence_end=result.evidence_end,
                    cache_key=cache,
                    created_at=now,
                )
            )
            processed += 1
        except (httpx.HTTPError, ValidationError, ValueError, KeyError, json.JSONDecodeError) as exc:
            failed += 1
            error_type = f"http_{exc.response.status_code}" if isinstance(exc, httpx.HTTPStatusError) else type(exc).__name__
            errors[error_type] = errors.get(error_type, 0) + 1
    if rows:
        upsert_parquet(
            pl.DataFrame([row.model_dump(mode="json") for row in rows], infer_schema_length=None),
            predictions_path,
            "prediction_id",
        )
    return {"status": "completed" if not failed else "completed_with_warnings", "processed": processed, "failed": failed, "error_types": errors, "token_usage": client.usage, "cost": None}
