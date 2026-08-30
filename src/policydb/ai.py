from __future__ import annotations

import hashlib
import json
import re
import time
from dataclasses import dataclass
from typing import Any

import httpx
from pydantic import BaseModel

from policydb.settings import Settings


@dataclass(frozen=True)
class AITrace:
    provider: str
    model: str
    trace_id: str | None
    prompt_tokens: int | None
    completion_tokens: int | None
    latency_seconds: float
    raw_response_hash: str | None = None
    raw_fields: tuple[str, ...] = ()
    transport_started: bool = True
    http_status: int | None = 200
    response_received: bool = True
    response_bytes: int | None = None
    json_parse_ok: bool = True
    schema_valid: bool = True
    configured_read_timeout: float | None = None
    configured_connect_timeout: float | None = None
    max_retries: int | None = None


class AIStructuredOutputError(ValueError):
    """Safe structured-output error carrying only non-sensitive diagnostics."""

    def __init__(
        self,
        message: str,
        *,
        parse_status: str,
        raw_response_hash: str | None,
        raw_fields: tuple[str, ...] = (),
        raw_payload: Any = None,
        http_status: int | None = 200,
        response_bytes: int | None = None,
        schema_errors: tuple[str, ...] = (),
    ) -> None:
        super().__init__(message)
        self.parse_status = parse_status
        self.raw_response_hash = raw_response_hash
        self.raw_fields = raw_fields
        self.raw_payload = raw_payload
        self.http_status = http_status
        self.response_bytes = response_bytes
        self.schema_errors = schema_errors


def _safe_provider_message(value: object) -> str:
    text = str(value or "")[:500]
    text = re.sub(
        r"(?i)(api[_-]?key|authorization|bearer|token|password|secret)\s*[:=]\s*[^,\s]+",
        r"\1=[REDACTED]",
        text,
    )
    text = re.sub(r"(?i)bearer\s+[^\s,]+", "Bearer [REDACTED]", text)
    text = re.sub(r"\bsk-[A-Za-z0-9_-]+", "[REDACTED]", text)
    return text


def _exception_names(exc: BaseException) -> set[str]:
    names: set[str] = set()
    current: BaseException | None = exc
    while current is not None and type(current).__name__ not in names:
        names.add(type(current).__name__)
        current = current.__cause__ or current.__context__
    return names


def _provider_error_code(exc: BaseException) -> str | None:
    direct = getattr(exc, "code", None)
    if direct not in (None, ""):
        return str(direct)[:100]
    body = getattr(exc, "body", None)
    if isinstance(body, dict):
        value = body.get("code")
        if value in (None, "") and isinstance(body.get("error"), dict):
            value = body["error"].get("code")
        if value not in (None, ""):
            return str(value)[:100]
    return None


def classify_ai_failure(exc: BaseException, *, latency_ms: float | None = None) -> dict[str, Any]:
    """Return a secret-safe transport/HTTP/parse/schema failure diagnosis."""

    response = getattr(exc, "response", None)
    status = getattr(exc, "http_status", None) or getattr(exc, "status_code", None)
    if status is None and response is not None:
        status = getattr(response, "status_code", None)
    try:
        http_status = int(status) if status is not None else None
    except (TypeError, ValueError):
        http_status = None
    names = _exception_names(exc)
    parse_status = str(getattr(exc, "parse_status", "") or "").lower()
    response_bytes = getattr(exc, "response_bytes", None)
    if response_bytes is None and response is not None:
        content = getattr(response, "content", None)
        if isinstance(content, (bytes, bytearray)):
            response_bytes = len(content)
    timeout_type = None
    if "ConnectTimeout" in names:
        failure_class = "CONNECT_TIMEOUT"
        timeout_type = "connect"
    elif "ReadTimeout" in names:
        failure_class = "READ_TIMEOUT"
        timeout_type = "read"
    elif "APITimeoutError" in names or "TimeoutException" in names:
        failure_class = "UNKNOWN_PROVIDER_FAILURE"
        timeout_type = "unspecified"
    elif parse_status == "empty_response":
        failure_class = "EMPTY_RESPONSE"
    elif parse_status == "truncated_response":
        failure_class = "TRUNCATED_RESPONSE"
    elif parse_status == "parse_failed":
        failure_class = "INVALID_JSON"
    elif parse_status == "validation_failed":
        failure_class = "SCHEMA_VALIDATION_FAILURE"
    elif http_status in {401, 402, 403, 429}:
        failure_class = f"HTTP_{http_status}"
    elif http_status is not None and 500 <= http_status <= 599:
        failure_class = "HTTP_5XX"
    elif "APIConnectionError" in names or "ConnectError" in names or "ConnectionError" in names:
        failure_class = "CONNECTION_ERROR"
    elif not getattr(exc, "response_received", False) and http_status is None:
        failure_class = "CONNECTION_ERROR"
    else:
        failure_class = "UNKNOWN_PROVIDER_FAILURE"
    response_received = bool(
        getattr(exc, "response_received", False)
        or http_status is not None
        or isinstance(exc, AIStructuredOutputError)
    )
    json_parse_ok = True if parse_status == "validation_failed" else False if parse_status in {"empty_response", "truncated_response", "parse_failed"} else None
    schema_valid = False if parse_status == "validation_failed" else None
    return {
        "transport_started": True,
        "dns_ok": True if http_status is not None else None,
        "connect_ok": True if http_status is not None else False if failure_class == "CONNECT_TIMEOUT" else None,
        "http_status": http_status,
        "response_received": response_received,
        "response_bytes": int(response_bytes) if response_bytes is not None else None,
        "latency_ms": round(float(latency_ms), 3) if latency_ms is not None else None,
        "timeout_type": timeout_type,
        "json_parse_ok": json_parse_ok,
        "schema_valid": schema_valid,
        "schema_errors": list(getattr(exc, "schema_errors", ()) or ()),
        "provider_error_code": _provider_error_code(exc),
        "provider_error_message_sanitized": _safe_provider_message(exc),
        "failure_class": failure_class,
        "raw_response_hash": getattr(exc, "raw_response_hash", None),
        "raw_fields": list(getattr(exc, "raw_fields", ()) or ()),
        "raw_response_payload": getattr(exc, "raw_payload", None),
    }


def normalize_structured_payload[T: BaseModel](payload: Any, schema: type[T]) -> Any:
    """Normalize common OpenAI-compatible wrappers without weakening schema validation."""

    fields = getattr(schema, "model_fields", {})
    if "actions" not in fields:
        return payload
    candidate = payload
    if isinstance(candidate, list):
        return {"actions": candidate}
    if not isinstance(candidate, dict):
        return candidate
    # SiliconFlow has returned these two structural envelopes in saved
    # production probes.  Unwrap the envelope only; field names and types are
    # still checked by the strict Pydantic schema below.
    for wrapper in ("result", "data", "output", "classification_output"):
        wrapped = candidate.get(wrapper)
        if isinstance(wrapped, (dict, list)):
            candidate = wrapped
            break
    if isinstance(candidate, list):
        return {"actions": candidate}
    if not isinstance(candidate, dict):
        return candidate
    classified_actions = candidate.get("classified_actions")
    if isinstance(classified_actions, (dict, list)):
        candidate = classified_actions
        if isinstance(candidate, list):
            return {"actions": candidate}
        if isinstance(candidate, dict):
            return {"actions": [candidate]}
    if "actions" in candidate:
        actions = candidate["actions"]
        if isinstance(actions, dict):
            return {**candidate, "actions": [actions]}
        return candidate
    if candidate.get("action_id") not in (None, ""):
        return {"actions": [candidate]}
    return candidate


def validate_structured_payload[T: BaseModel](payload: Any, schema: type[T]) -> T:
    return schema.model_validate(normalize_structured_payload(payload, schema))


class SiliconFlowProvider:
    """Thin OpenAI-compatible adapter; policy decisions stay in deterministic callers."""

    def __init__(
        self,
        settings: Settings | None = None,
        *,
        client=None,
        request_timeout_override: float | None = None,
        connect_timeout_override: float | None = None,
        max_retries_override: int | None = None,
    ) -> None:
        self.settings = settings or Settings.discover()
        self.api_key = self.settings.siliconflow_api_key
        self.base_url = self.settings.siliconflow_base_url
        self._injected_client = client
        self.request_timeout_override = request_timeout_override
        self.connect_timeout_override = connect_timeout_override
        self.max_retries_override = max_retries_override

    @property
    def configured_read_timeout(self) -> float:
        return float(self.request_timeout_override if self.request_timeout_override is not None else self.settings.request_timeout)

    @property
    def configured_connect_timeout(self) -> float:
        return float(self.connect_timeout_override if self.connect_timeout_override is not None else self.settings.connect_timeout)

    @property
    def configured_max_retries(self) -> int:
        return int(self.max_retries_override if self.max_retries_override is not None else self.settings.max_retries)

    def _client(self):
        if self._injected_client is not None:
            return self._injected_client
        if not self.api_key:
            raise RuntimeError("SILICONFLOW_API_KEY is not configured")
        from openai import OpenAI

        timeout = self.settings.request_timeout
        if self.request_timeout_override is not None:
            timeout = httpx.Timeout(
                timeout=self.request_timeout_override,
                connect=self.configured_connect_timeout,
                read=self.request_timeout_override,
                write=self.request_timeout_override,
                pool=self.configured_connect_timeout,
            )
        return OpenAI(
            api_key=self.api_key,
            base_url=self.base_url,
            timeout=timeout,
            max_retries=self.configured_max_retries,
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

    def structured[T: BaseModel](
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
            raise AIStructuredOutputError(
                "AI returned empty structured content",
                parse_status="empty_response",
                raw_response_hash=None,
                http_status=getattr(response, "status_code", None) or 200,
                response_bytes=0,
            )
        response_bytes = len(content.encode("utf-8"))
        raw_response_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()
        try:
            payload = json.loads(content)
        except json.JSONDecodeError as exc:
            truncated = exc.pos >= max(0, len(content) - 2) or "unterminated" in exc.msg.lower()
            raise AIStructuredOutputError(
                "AI returned truncated JSON" if truncated else "AI returned invalid JSON",
                parse_status="truncated_response" if truncated else "parse_failed",
                raw_response_hash=raw_response_hash,
                http_status=getattr(response, "status_code", None) or 200,
                response_bytes=response_bytes,
            ) from exc
        raw_fields = tuple(sorted(payload)) if isinstance(payload, dict) else ()
        try:
            value = validate_structured_payload(payload, schema)
        except Exception as exc:
            errors = ()
            if hasattr(exc, "errors"):
                errors = tuple(
                    f"{'.'.join(map(str, item.get('loc') or ())) or '<root>'}: {item.get('msg') or item.get('type')}"
                    for item in exc.errors()
                )
            raise AIStructuredOutputError(
                "AI structured output failed schema validation",
                parse_status="validation_failed",
                raw_response_hash=raw_response_hash,
                raw_fields=raw_fields,
                raw_payload=payload,
                http_status=getattr(response, "status_code", None) or 200,
                response_bytes=response_bytes,
                schema_errors=errors,
            ) from exc
        usage = getattr(response, "usage", None)
        return value, AITrace(
            provider="siliconflow",
            model=model,
            trace_id=getattr(response, "_request_id", None),
            prompt_tokens=getattr(usage, "prompt_tokens", None),
            completion_tokens=getattr(usage, "completion_tokens", None),
            latency_seconds=time.perf_counter() - started,
            raw_response_hash=raw_response_hash,
            raw_fields=raw_fields,
            http_status=getattr(response, "status_code", None) or 200,
            response_bytes=response_bytes,
            configured_read_timeout=self.configured_read_timeout,
            configured_connect_timeout=self.configured_connect_timeout,
            max_retries=self.configured_max_retries,
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


def get_ai_provider(
    settings: Settings | None = None,
    *,
    request_timeout_override: float | None = None,
    connect_timeout_override: float | None = None,
    max_retries_override: int | None = None,
) -> SiliconFlowProvider:
    settings = settings or Settings.discover()
    if settings.ai_provider != "siliconflow":
        raise ValueError(f"Unsupported AI_PROVIDER: {settings.ai_provider}")
    return SiliconFlowProvider(
        settings,
        request_timeout_override=request_timeout_override,
        connect_timeout_override=connect_timeout_override,
        max_retries_override=max_retries_override,
    )
