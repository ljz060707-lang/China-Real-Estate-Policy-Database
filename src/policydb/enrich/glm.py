from __future__ import annotations

import json
import os
from datetime import UTC, date, datetime
from typing import Literal

import httpx
import polars as pl
from pydantic import BaseModel, Field, ValidationError

from policydb.crawl.checkpoint import append_unique
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
    confidence: float = Field(ge=0, le=1)
    needs_review: bool = False


SYSTEM_PROMPT = """你是中国房地产政策资料结构化助手。只输出符合给定JSON schema的JSON。
不得根据常识编造发布日期、机构、链接、行政区划代码或文件真实性。
证据不足时保留空值并设置needs_review=true。"""


class GLMEnricher:
    schema_version = "1.0.0"
    prompt_version = "2026-07-14-v1"

    def __init__(
        self,
        settings: Settings | None = None,
        *,
        client: httpx.Client | None = None,
        api_key: str | None = None,
        model: str | None = None,
        retries: int = 2,
    ) -> None:
        self.settings = settings or Settings.discover()
        self.api_key = api_key if api_key is not None else os.getenv("GLM_API_KEY")
        self.model = model or os.getenv("GLM_MODEL", "glm-4-flash")
        self.client = client or httpx.Client(timeout=90)
        self.retries = retries
        self.cache_path = self.settings.curated / "llm_extractions.parquet"

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
                    "https://open.bigmodel.cn/api/paas/v4/chat/completions",
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
                return GLMExtraction.model_validate_json(content)
            except (httpx.HTTPError, KeyError, TypeError, json.JSONDecodeError, ValidationError) as exc:
                error = exc
        assert error is not None
        raise ValueError(f"GLM structured output validation failed: {type(error).__name__}")

    def extract(self, content_sha256: str, text: str) -> GLMExtraction | None:
        if self.cache_path.exists():
            cached = pl.read_parquet(self.cache_path).filter(
                (pl.col("content_sha256") == content_sha256)
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
            "extraction_id": stable_id(
                content_sha256, self.model, self.prompt_version, self.schema_version, prefix="LLM"
            ),
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
        }
        append_unique(self.cache_path, [row], "extraction_id")
        return result

    def enrich_pending(self) -> dict:
        versions_path = self.settings.curated / "policy_document_versions.parquet"
        if not versions_path.exists():
            return {"pending": 0, "completed": 0, "awaiting_api_key": 0, "failed": 0}
        versions = pl.read_parquet(versions_path)
        completed = awaiting = failed = 0
        for row in versions.iter_rows(named=True):
            result = self.extract(row["content_sha256"], row.get("extracted_text") or "")
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
        }
