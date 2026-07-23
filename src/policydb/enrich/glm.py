from __future__ import annotations

import json
from datetime import UTC, date, datetime
from typing import Literal

import httpx
import polars as pl
from pydantic import BaseModel, Field, ValidationError, model_validator

from policydb.crawl.checkpoint import append_unique
from policydb.crawl.dedup import glm_cache_key, normalized_text_hash
from policydb.settings import Settings
from policydb.transform.normalization import stable_id


class DemandFlags(BaseModel):
    purchase_limit: bool = False
    sale_limit: bool = False
    commercial_mortgage: bool = False
    hpf_down_payment: bool = False
    hpf_loan_quota: bool = False
    other_hpf: bool = False
    talent_or_hukou: bool = False
    purchase_subsidy_or_other: bool = False


class EvidenceSpan(BaseModel):
    field: str
    excerpt: str


class AIActionClassification(BaseModel):
    action_text: str
    primary_category: Literal["D", "S", "F", "H", "G"]
    secondary_category: str
    instrument_type: Literal[
        "tax",
        "subsidy",
        "public_spending",
        "public_provision",
        "credit_finance",
        "regulation",
        "land_planning",
        "administrative_service",
        "information_disclosure",
        "coordination",
    ]
    direction: Literal[
        "tightening",
        "loosening",
        "supportive",
        "risk_strengthening",
        "streamlining",
        "withdrawal_repeal",
        "neutral",
        "mixed",
        "uncertain",
    ]
    target_groups: list[str] = Field(default_factory=list)
    lifecycle: list[str] = Field(default_factory=list)
    evidence_excerpt: str
    evidence_start: int = Field(ge=0)
    evidence_end: int = Field(gt=0)
    confidence: float = Field(ge=0, le=1)

    @model_validator(mode="after")
    def secondary_belongs_to_primary(self):
        if not self.secondary_category.startswith(self.primary_category):
            raise ValueError("secondary_category must belong to primary_category")
        if self.evidence_end <= self.evidence_start:
            raise ValueError("invalid evidence offsets")
        return self


class GLMExtraction(BaseModel):
    is_relevant: bool
    policy_title: str = ""
    document_number: str | None = None
    issuing_agencies: list[str] = Field(default_factory=list)
    jurisdiction_candidates: list[str] = Field(default_factory=list)
    applicable_city_candidates: list[str] = Field(default_factory=list)
    primary_collection_id: str = ""
    secondary_category_ids: list[str] = Field(default_factory=list)
    policy_direction: Literal["easing", "tightening", "neutral", "mixed"] = "neutral"
    demand_flags: DemandFlags = Field(default_factory=DemandFlags)
    supply_flags: list[str] = Field(default_factory=list)
    target_groups: list[str] = Field(default_factory=list)
    effective_date_candidate: date | None = None
    summary: str = ""
    evidence_spans: list[EvidenceSpan] = Field(default_factory=list)
    policy_actions: list[AIActionClassification] = Field(default_factory=list)
    confidence: float = Field(ge=0, le=1)
    needs_review: bool = False


class GLMVerification(BaseModel):
    field_evidence_valid: bool
    segmentation_complete: bool
    city_scope_supported: bool
    classification_supported: bool
    direction_supported: bool
    strength_supported: bool
    source_refetch_required: bool = False
    evidence_spans: list[EvidenceSpan] = Field(default_factory=list)
    conflicts: list[str] = Field(default_factory=list)
    confidence: float = Field(ge=0, le=1)


SYSTEM_PROMPT = """你是中国房地产政策资料结构化助手。只输出符合给定JSON schema的JSON。
不得根据常识编造发布日期、机构、链接、行政区划代码或文件真实性。
综合文件必须拆为多个policy_actions，每个动作只能有一个D/S/F/H/G一级分类。
每个动作必须给出正文逐字证据和字符位置；证据不足时不输出该动作并设置needs_review=true。"""

VERIFICATION_PROMPT = """你是独立的政策数据复核助手。只核对输入正文与第一次抽取，输出JSON。
逐字段给出正文证据，检查文本切割、城市过度推断、分类、方向和强度；不得补写正文中不存在的内容。
你只能报告证据与冲突，最终是否通过由确定性程序决定。"""

_UNSET = object()


class GLMEnricher:
    schema_version = "1.0.0"
    prompt_version = "2026-07-14-v1"

    def __init__(
        self,
        settings: Settings | None = None,
        *,
        client: httpx.Client | None = None,
        api_key: str | None | object = _UNSET,
        model: str | None = None,
        retries: int = 2,
    ) -> None:
        self.settings = settings or Settings.discover()
        resolved_key = self.settings.glm_api_key if api_key is _UNSET else api_key
        self.api_key: str | None = resolved_key if isinstance(resolved_key, str) else None
        self.model = model or self.settings.glm_model
        self.base_url = self.settings.glm_base_url
        self.client = client or httpx.Client(timeout=self.settings.request_timeout)
        self.retries = retries
        self.cache_path = self.settings.curated / "llm_extractions.parquet"
        self.verification_cache_path = self.settings.curated / "llm_verifications.parquet"

    @staticmethod
    def chunks(text: str, size: int = 12000) -> list[str]:
        return [text[index : index + size] for index in range(0, len(text), size)] or [""]

    def _call(self, text: str) -> GLMExtraction:
        if not self.api_key:
            raise RuntimeError("GLM_API_KEY is not configured")
        error: Exception | None = None
        for _ in range(self.retries + 1):
            try:
                response = self.client.post(
                    self.base_url,
                    headers={"Authorization": f"Bearer {self.api_key}"},
                    json={
                        "model": self.model,
                        "response_format": {"type": "json_object"},
                        "messages": [
                            {"role": "system", "content": SYSTEM_PROMPT},
                            {
                                "role": "user",
                                "content": "schema="
                                + json.dumps(GLMExtraction.model_json_schema(), ensure_ascii=False)
                                + "\n正文="
                                + text,
                            },
                        ],
                    },
                )
                response.raise_for_status()
                content = response.json()["choices"][0]["message"]["content"]
                result = GLMExtraction.model_validate_json(content)
                self._validate_action_evidence(text, result)
                return result
            except (httpx.HTTPError, KeyError, TypeError, json.JSONDecodeError, ValidationError) as exc:
                error = exc
        assert error is not None
        raise ValueError(f"GLM structured output validation failed: {type(error).__name__}")

    @staticmethod
    def _validate_action_evidence(text: str, result: GLMExtraction) -> None:
        for action in result.policy_actions:
            excerpt = text[action.evidence_start : action.evidence_end]
            if excerpt != action.evidence_excerpt or text.count(action.evidence_excerpt) != 1:
                raise ValueError("policy action evidence must uniquely match source text")

    def extract(self, content_sha256: str, text: str) -> GLMExtraction | None:
        text_hash = normalized_text_hash(text)
        cache_key = glm_cache_key(text_hash, self.model, self.prompt_version, self.schema_version)
        if self.cache_path.exists():
            cached = pl.read_parquet(self.cache_path).filter(
                (pl.col("cache_key") == cache_key)
                & (pl.col("model_name") == self.model)
                & (pl.col("prompt_version") == self.prompt_version)
                & (pl.col("schema_version") == self.schema_version)
                & (pl.col("status") == "complete")
            )
            if cached.height:
                return GLMExtraction.model_validate_json(cached[-1, "output_json"])
        now = datetime.now(UTC).isoformat()
        if not self.api_key:
            result = None
            status = "awaiting_api_key"
            output = None
            error_type = None
        else:
            try:
                # The first version limits paid calls to one validated aggregate input.
                combined = "\n\n".join(self.chunks(text)[:4])
                result = self._call(combined)
                status = "complete"
                output = result.model_dump_json()
                error_type = None
            except ValueError as exc:
                result = None
                status = "failed_validation"
                output = None
                error_type = str(exc)
        row = {
            "extraction_id": stable_id(cache_key, prefix="LLM"),
            "content_sha256": content_sha256,
            "model_name": self.model,
            "prompt_version": self.prompt_version,
            "schema_version": self.schema_version,
            "status": status,
            "output_json": output,
            "confidence": result.confidence if result else None,
            "needs_review": bool(result.needs_review or result.confidence < 0.65)
            if result
            else True,
            "error_type": error_type,
            "called_at": now if self.api_key else None,
            "created_at": now,
            "updated_at": now,
            "cache_key": cache_key,
        }
        append_unique(self.cache_path, [row], "extraction_id")
        return result

    def verify(
        self,
        content_sha256: str,
        text: str,
        extraction: GLMExtraction,
    ) -> GLMVerification | None:
        text_hash = normalized_text_hash(text)
        cache_key = glm_cache_key(
            text_hash, self.model, self.prompt_version + ":verify", self.schema_version
        )
        verification_id = stable_id(cache_key, prefix="LLMVERIFY")
        if self.verification_cache_path.exists():
            cached = pl.read_parquet(self.verification_cache_path).filter(
                (pl.col("verification_id") == verification_id)
                & (pl.col("status") == "complete")
            )
            if cached.height:
                return GLMVerification.model_validate_json(cached[-1, "output_json"])
        now = datetime.now(UTC).isoformat()
        result: GLMVerification | None = None
        error_type = None
        status = "awaiting_api_key"
        if self.api_key:
            error: Exception | None = None
            for _ in range(self.retries + 1):
                try:
                    response = self.client.post(
                        self.base_url,
                        headers={"Authorization": f"Bearer {self.api_key}"},
                        json={
                            "model": self.model,
                            "response_format": {"type": "json_object"},
                            "messages": [
                                {"role": "system", "content": VERIFICATION_PROMPT},
                                {
                                    "role": "user",
                                    "content": "schema="
                                    + json.dumps(
                                        GLMVerification.model_json_schema(), ensure_ascii=False
                                    )
                                    + "\n第一次抽取="
                                    + extraction.model_dump_json()
                                    + "\n正文="
                                    + "\n\n".join(self.chunks(text)[:4]),
                                },
                            ],
                        },
                    )
                    response.raise_for_status()
                    content = response.json()["choices"][0]["message"]["content"]
                    result = GLMVerification.model_validate_json(content)
                    status = "complete"
                    break
                except (httpx.HTTPError, KeyError, TypeError, ValidationError) as exc:
                    error = exc
            if result is None:
                status = "failed_validation"
                error_type = type(error).__name__ if error else "unknown"
        row = {
            "verification_id": verification_id,
            "content_sha256": content_sha256,
            "model_name": self.model,
            "prompt_version": self.prompt_version,
            "status": status,
            "output_json": result.model_dump_json() if result else None,
            "confidence": result.confidence if result else None,
            "error_type": error_type,
            "called_at": now if self.api_key else None,
            "created_at": now,
            "updated_at": now,
            "cache_key": cache_key,
        }
        append_unique(self.verification_cache_path, [row], "verification_id")
        return result

    @staticmethod
    def _likely_relevant(text: str) -> bool:
        return any(
            term in text
            for term in ("房地产", "住房", "楼市", "购房", "土地", "公积金", "城市更新", "保交", "房企")
        )

    def _pending_versions(
        self,
        run_id: str | None = None,
        document_version_ids: list[str] | None = None,
    ) -> pl.DataFrame:
        versions_path = self.settings.curated / "policy_document_versions.parquet"
        if not versions_path.exists():
            return pl.DataFrame()
        versions = pl.read_parquet(versions_path)
        if document_version_ids:
            versions = versions.filter(
                pl.col("document_version_id").is_in(document_version_ids)
            )
        if run_id:
            items_path = self.settings.curated / "crawl_items.parquet"
            if not items_path.exists():
                return versions.head(0)
            items = pl.read_parquet(items_path).filter(pl.col("run_id") == run_id).select(
                "item_id"
            )
            versions = versions.join(
                items, left_on="crawl_item_id", right_on="item_id", how="inner"
            )
        return versions

    def enrich_pending(
        self,
        run_id: str | None = None,
        document_version_ids: list[str] | None = None,
    ) -> dict:
        versions = self._pending_versions(run_id, document_version_ids)
        if versions.is_empty():
            return {"pending": 0, "completed": 0, "awaiting_api_key": 0, "failed": 0, "irrelevant": 0}
        completed = awaiting = failed = irrelevant = 0
        for row in versions.iter_rows(named=True):
            text = row.get("extracted_text") or ""
            if not self._likely_relevant(text):
                irrelevant += 1
                continue
            result = self.extract(row["content_sha256"], text)
            if result:
                completed += 1
            elif self.api_key:
                failed += 1
            else:
                awaiting += 1
        return {
            "pending": versions.height,
            "completed": completed,
            "awaiting_api_key": awaiting,
            "failed": failed,
            "irrelevant": irrelevant,
        }

    def verify_pending(
        self,
        run_id: str | None = None,
        document_version_ids: list[str] | None = None,
    ) -> dict:
        """Run an independent evidence check for completed first-pass extractions."""
        versions_path = self.settings.curated / "policy_document_versions.parquet"
        if not versions_path.exists() or not self.cache_path.exists():
            return {"pending": 0, "completed": 0, "awaiting_api_key": 0, "failed": 0}
        versions = self._pending_versions(run_id, document_version_ids).select(
            "content_sha256", "extracted_text"
        ).unique("content_sha256", keep="last")
        extractions = pl.read_parquet(self.cache_path).filter(
            (pl.col("status") == "complete") & pl.col("output_json").is_not_null()
        )
        work = extractions.join(versions, on="content_sha256", how="inner")
        completed = awaiting = failed = 0
        for row in work.iter_rows(named=True):
            extraction = GLMExtraction.model_validate_json(row["output_json"])
            result = self.verify(
                row["content_sha256"], row.get("extracted_text") or "", extraction
            )
            if result:
                completed += 1
            elif self.api_key:
                failed += 1
            else:
                awaiting += 1
        return {
            "pending": work.height,
            "completed": completed,
            "awaiting_api_key": awaiting,
            "failed": failed,
        }
