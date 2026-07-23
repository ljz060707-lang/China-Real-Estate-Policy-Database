from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import TypeVar

import httpx
from pydantic import BaseModel

from policydb.settings import Settings

T = TypeVar("T", bound=BaseModel)


@dataclass(frozen=True)
class AITrace:
    provider: str
    model: str
    trace_id: str | None
    prompt_tokens: int | None
    completion_tokens: int | None
    latency_seconds: float


class SiliconFlowProvider:
    """Thin OpenAI-compatible adapter; policy decisions stay in deterministic callers."""

    def __init__(self, settings: Settings | None = None, *, client=None) -> None:
        self.settings = settings or Settings.discover()
        self.api_key = self.settings.siliconflow_api_key
        self.base_url = self.settings.siliconflow_base_url
        self._injected_client = client

    def _client(self):
        if self._injected_client is not None:
            return self._injected_client
        if not self.api_key:
            raise RuntimeError("SILICONFLOW_API_KEY is not configured")
        from openai import OpenAI

        return OpenAI(
            api_key=self.api_key,
            base_url=self.base_url,
            timeout=self.settings.request_timeout,
            max_retries=self.settings.max_retries,
        )

    def models(self) -> list[str]:
        return sorted(model.id for model in self._client().models.list().data)

    def test(self) -> dict:
        configured = {
            "chat": self.settings.siliconflow_chat_model,
            "verify": self.settings.siliconflow_verify_model,
            "embedding": self.settings.siliconflow_embedding_model,
            "rerank": self.settings.siliconflow_rerank_model,
        }
        try:
            models = self.models()
        except Exception as exc:
            status_code = getattr(exc, "status_code", None)
            error_type = (
                "authentication_failed"
                if status_code in {401, 403}
                else "quota_or_rate_limit"
                if status_code == 429
                else "connection_failed"
            )
            return {
                "provider": "siliconflow",
                "connected": False,
                "error_type": error_type,
                "status_code": status_code,
                "configured_models": configured,
                "unavailable_models": [],
            }
        unavailable = sorted({name for name in configured.values() if name and name not in models})
        return {
            "provider": "siliconflow",
            "connected": True,
            "model_count": len(models),
            "configured_models": configured,
            "unavailable_models": unavailable,
        }

    def structured(
        self,
        *,
        model: str,
        system: str,
        user: str,
        schema: type[T],
    ) -> tuple[T, AITrace]:
        if not model:
            raise RuntimeError("SiliconFlow chat model is not configured")
        started = time.perf_counter()
        response = self._client().chat.completions.create(
            model=model,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
        )
        content = response.choices[0].message.content
        if not content:
            raise ValueError("AI returned empty structured content")
        value = schema.model_validate(json.loads(content))
        usage = getattr(response, "usage", None)
        return value, AITrace(
            provider="siliconflow",
            model=model,
            trace_id=getattr(response, "_request_id", None),
            prompt_tokens=getattr(usage, "prompt_tokens", None),
            completion_tokens=getattr(usage, "completion_tokens", None),
            latency_seconds=time.perf_counter() - started,
        )

    def embeddings(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []
        response = self._client().embeddings.create(
            model=self.settings.siliconflow_embedding_model,
            input=texts,
        )
        return [item.embedding for item in response.data]

    def rerank(self, query: str, documents: list[str]) -> list[dict]:
        if not documents:
            return []
        client = self._client()
        response = client.post(
            "/rerank",
            cast_to=httpx.Response,
            body={
                "model": self.settings.siliconflow_rerank_model,
                "query": query,
                "documents": documents,
            },
        )
        data = response.json()
        return list(data.get("results", []))


def get_ai_provider(settings: Settings | None = None) -> SiliconFlowProvider:
    settings = settings or Settings.discover()
    if settings.ai_provider != "siliconflow":
        raise ValueError(f"Unsupported AI_PROVIDER: {settings.ai_provider}")
    return SiliconFlowProvider(settings)
