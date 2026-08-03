from __future__ import annotations

import argparse
import hashlib
import json
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Literal

import polars as pl
from pydantic import BaseModel, Field

from policydb.ai import get_ai_provider
from policydb.ai_audit import AIAuditStore
from policydb.budget import BudgetExceeded
from policydb.config.providers import build_search_fallback, build_search_provider
from policydb.crawl.dedup import canonicalize_url
from policydb.parquet_store import atomic_write_parquet, read_parquet_snapshot
from policydb.settings import Settings
from policydb.source_completion import build_slot_work_queue
from policydb.source_slots import audit_525, probe_candidates, upsert_candidates
from policydb.transform.normalization import stable_id

PROMPT_VERSION = "source-completion-v1"


class AICallBudgetExceeded(BudgetExceeded):
    """Raised before an LLM request when the actual-attempt cap is exhausted."""


class SearchCallBudgetExceeded(BudgetExceeded):
    """Raised before a search request when the actual-attempt cap is exhausted."""


class SourceAIAssessment(BaseModel):
    search_queries: list[str] = Field(default_factory=list, max_length=8)
    institution_aliases: list[str] = Field(default_factory=list, max_length=8)
    entry_type_hint: Literal["list_entry", "homepage", "detail_page", "pdf", "unknown"] = "unknown"
    pagination_hint: str = ""
    confidence: float = Field(ge=0, le=1, default=0)
    recommended_action: Literal["proposed", "pending_probe", "requires_human_review", "rejected"] = "proposed"
    human_question: str = ""


class SourceAIRanking(BaseModel):
    ranked_candidate_ids: list[str] = Field(default_factory=list, max_length=50)
    likely_page_type: Literal["list_entry", "homepage", "detail_page", "pdf", "unknown"] = "unknown"
    likely_role: str = ""
    official_evidence_summary: str = ""
    recommended_top3: list[str] = Field(default_factory=list, max_length=3)
    ambiguity_reason: str = ""
    ddg_research_needed: bool = False


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _sha(value: object) -> str:
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True, default=str).encode()).hexdigest()


def _json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")


def _empty(columns: list[str]) -> pl.DataFrame:
    return pl.DataFrame({name: pl.Series(name, [], dtype=pl.String) for name in columns})


def interface_audit() -> str:
    return """# AI接口审计

## 复用入口

- 统一Provider：`src/policydb/ai.py:get_ai_provider`；当前实现为 `SiliconFlowProvider`。
- 结构化调用：`SiliconFlowProvider.structured`，使用OpenAI兼容 `chat.completions` 和 JSON response format，并由Pydantic校验。
- 兼容旧接口：`src/policydb/enrich/glm.py:GLMEnricher`，已有Parquet缓存、缓存键和失败状态；本来源任务不另建客户端。
- 搜索：`src/policydb/config/providers.py` 的 `build_search_fallback`，支持配置Provider和无密钥的DuckDuckGo HTML fallback。

## 能力边界

- 当前AI Provider本身只提供普通结构化LLM调用，不声明具备联网搜索或浏览器工具能力。
- URL发现必须由现有搜索Provider、政府站内搜索或HTTP流程产生；AI只能生成查询、别名、初步页面类型和排序建议。
- 现有Provider由OpenAI客户端的 `max_retries` 提供基础重试；本流程再做有限的调用级重试。
- GLM/AI已有内容缓存；本流程使用 `slot_id + city + role + prompt_version + model + context_hash` 作为独立缓存键。
- 现有结构化输出和token usage trace可复用；费用单价未配置时只记录token，估算费用保持为空。
- 密钥来自既有Keyring/环境变量解析，不写入请求、响应、Parquet或报告；请求头不保存。
- 政府真实探测仍由 `source_slots.probe_candidates` 执行，保持直连、双探测、解析和分页门槛。

## 复用模块

`policydb.ai.get_ai_provider`、`policydb.config.providers.build_search_fallback`、`policydb.source_slots.upsert_candidates`、`probe_candidates`、`verify_candidates` 和 `audit_525`。
"""


def _ai_status(row: dict) -> str:
    raw = str(row.get("work_status") or "")
    state_values = {
        str(row.get(name) or "").strip().lower()
        for name in ("state", "status", "work_status", "manual_review_status", "slot_state")
    }
    if row.get("slots_verified") is True or row.get("is_verified") is True:
        return "A_verified_enabled"
    if raw == "verified_enabled":
        return "A_verified_enabled"
    if state_values & {"human_review", "pending_human_review", "retry_wait"}:
        return "E_ambiguous_human_review" if "human_review" in state_values or "pending_human_review" in state_values else "A_retry_wait"
    if row.get("best_candidate_id") and (row.get("health_probe_success_count") or 0) < 2:
        if row.get("role_confidence") is None and row.get("city_confidence") is None:
            return "B_candidate_exists_needs_probe"
        if raw not in {"blocked_role_conflict", "blocked_network"}:
            return "B_candidate_exists_needs_probe"
    if raw in {"blocked_role_conflict", "candidate_failed_ambiguous"}:
        return "E_ambiguous_human_review"
    if raw in {"blocked_network"}:
        return "F_blocked_network"
    if raw in {"blocked_parser"}:
        return "G_blocked_parser"
    if raw in {"blocked_pagination"}:
        return "G_blocked_parser"
    if raw.startswith("no_candidate"):
        return "D_no_candidate_ai_discoverable"
    return "C_candidate_failed_fixable"


def build_ai_plan(settings: Settings, *, city: str | None = None, city_id: str | None = None, source_role: str | None = None, slot_id: str | None = None, max_slots: int = 50, audit_existing: bool = False) -> pl.DataFrame:
    if max_slots < 1 or max_slots > 50:
        raise ValueError("max_slots must be between 1 and 50")
    base = build_slot_work_queue(settings)
    queue = base.with_columns(
        pl.Series("ai_status", [_ai_status(row) for row in base.iter_rows(named=True)])
    )
    # Source discovery must never reclaim a solved or manually adjudicated
    # slot.  The deterministic verification and crawl stages own those
    # records; this planner only supplies unresolved/recoverable work.
    # ``audit_existing`` is a read/search-only audit path used by a scoped
    # acceptance run.  It is deliberately explicit so it cannot silently
    # turn the normal source-completion planner into a reclaimer.
    queue = queue.filter(~pl.col("ai_status").is_in(["A_retry_wait", "E_ambiguous_human_review"]))
    if not audit_existing:
        queue = queue.filter(~pl.col("ai_status").is_in(["A_verified_enabled"]))
        if "verified_candidate_count" in queue.columns:
            queue = queue.filter(pl.col("verified_candidate_count").fill_null(0) <= 0)
        if "enabled_source_count" in queue.columns:
            queue = queue.filter(pl.col("enabled_source_count").fill_null(0) <= 0)
        for column in ("slots_verified", "is_verified", "verified", "source_verified", "crawl_enabled", "enabled"):
            if column in queue.columns:
                queue = queue.filter(~pl.col(column).fill_null(False).cast(pl.Boolean))
    if city:
        queue = queue.filter(pl.col("city_name") == city)
    if city_id:
        queue = queue.filter(pl.col("city_id") == city_id)
    if source_role:
        queue = queue.filter(pl.col("source_role") == source_role)
    if slot_id:
        queue = queue.filter(pl.col("slot_id") == slot_id)
    priority = {"B_candidate_exists_needs_probe": 0, "C_candidate_failed_fixable": 1, "D_no_candidate_ai_discoverable": 2, "F_blocked_network": 3, "G_blocked_parser": 4}
    queue = queue.with_columns(pl.col("ai_status").replace(priority, default=99).cast(pl.Int64).alias("ai_priority"))
    return queue.sort(["ai_priority", "province_name", "city_name", "source_role", "slot_id"]).head(max_slots)


def _queries(row: dict) -> list[str]:
    city = str(row["city_name"])
    role = str(row["source_role"])
    labels = {
        "municipal_government": "人民政府 政策",
        "government_gazette": "政府公报",
        "housing_department": "住房和城乡建设局 政策",
        "provident_fund_center": "住房公积金管理中心 政策",
        "natural_resources_department": "自然资源和规划局 政策",
    }
    return [f"{city} {labels.get(role, role)} 官网", f"site:gov.cn {city} {labels.get(role, role)} 栏目"]


def _assessment_prompt(row: dict, queries: list[str], search_rows: list[dict[str, Any]] | None = None) -> tuple[str, str]:
    system = "你是来源发现辅助器。只输出符合JSON schema的结构化建议。不要伪造URL，不要声称已联网，不要决定官方认证、HTTP健康、分页或is_verified。"
    user = json.dumps({"city": row["city_name"], "city_id": row["city_id"], "role": row["source_role"], "existing_candidate": row.get("best_candidate_url"), "queries": queries, "instruction": "生成更好的官方检索词、机构别名、页面类型和人工问题。"}, ensure_ascii=False)
    if search_rows:
        payload = json.loads(user)
        payload["search_evidence"] = [
            {
                "url": item.get("url"),
                "title": item.get("title"),
                "snippet": item.get("snippet"),
                "provider": item.get("provider"),
                "query": item.get("query"),
            }
            for item in search_rows
            if item.get("url")
        ]
        user = json.dumps(payload, ensure_ascii=False)
    return system, user


def _call_ai(
    provider,
    model: str,
    system: str,
    user: str,
    *,
    audit: AIAuditStore | None = None,
    audit_payload: dict | None = None,
    max_attempts: int = 3,
    schema: type[BaseModel] = SourceAIAssessment,
    attempt_callback: Callable[[dict[str, Any]], Any] | None = None,
) -> tuple[BaseModel | None, object | None, str | None]:
    request_id = str((audit_payload or {}).get("request_id", ""))
    if audit is not None and audit_payload is not None:
        action, existing = audit.reserve(audit_payload)
        if action == "reused":
            payload = (existing or {}).get("response_payload")
            if payload:
                try:
                    value = schema.model_validate(payload)
                except Exception as exc:
                    audit.start(audit_payload)
                    audit.fail(
                        request_id,
                        error_type="reused_response_invalid",
                        error_message=type(exc).__name__,
                    )
                    return None, None, "reused_response_invalid"
                audit.reuse(audit_payload, existing)
                return value, None, None
            audit.start(audit_payload)
            audit.fail(
                request_id,
                error_type="reused_without_payload",
                error_message="completed cross-run audit has no structured response payload",
            )
            return None, None, "reused_without_payload"
        if action == "in_flight":
            audit.start(audit_payload)
            audit.fail(
                request_id,
                error_type="duplicate_request_in_flight",
                error_message="another run currently owns the normalized request",
            )
            return None, None, "duplicate_request_in_flight"
        audit.start(audit_payload)
    last_error = None
    last_trace = None
    for attempt in range(max_attempts):
        if audit is not None and request_id:
            audit.update(request_id, attempt=attempt + 1)
        attempt_id = None
        try:
            if attempt_callback is not None:
                attempt_id = attempt_callback(
                    {
                        "phase": "before",
                        "stage": "ai",
                        "request_id": request_id,
                        "attempt": attempt + 1,
                        "model": model,
                    }
                )
            value, trace = provider.structured(model=model, system=system, user=user, schema=schema)
            if attempt_callback is not None:
                attempt_callback(
                    {
                        "phase": "after",
                        "attempt_id": attempt_id,
                        "stage": "ai",
                        "request_id": request_id,
                        "attempt": attempt + 1,
                        "status": "completed",
                        "prompt_tokens": getattr(trace, "prompt_tokens", None),
                        "completion_tokens": getattr(trace, "completion_tokens", None),
                        "estimated_cost_usd": getattr(trace, "estimated_cost_usd", None),
                    }
                )
            if audit is not None and request_id:
                prompt_tokens = getattr(trace, "prompt_tokens", None)
                completion_tokens = getattr(trace, "completion_tokens", None)
                total_tokens = (
                    int(prompt_tokens) + int(completion_tokens)
                    if prompt_tokens is not None and completion_tokens is not None
                    else None
                )
                audit.complete(
                    request_id,
                    response_hash=_sha(value.model_dump(mode="json")),
                    response_payload=value.model_dump(mode="json"),
                    prompt_tokens=prompt_tokens,
                    completion_tokens=completion_tokens,
                    total_tokens=total_tokens,
                    estimated_cost_usd=None,
                    cache_hit=False,
                    ai_parse_status="parsed",
                    ai_parse_error=None,
                    ai_raw_response_hash=getattr(trace, "raw_response_hash", None),
                    ai_fields_defaulted=[
                        name
                        for name in schema.model_fields
                        if name not in set(getattr(trace, "raw_fields", ()))
                    ] if getattr(trace, "raw_fields", None) is not None else [],
                )
            return value, trace, None
        except (AICallBudgetExceeded, SearchCallBudgetExceeded):
            if audit is not None and request_id:
                audit.fail(request_id, error_type="budget_exhausted", error_message="actual call budget exhausted before request")
            raise
        except Exception as exc:
            if attempt_callback is not None and attempt_id is not None:
                attempt_callback(
                    {
                        "phase": "after",
                        "attempt_id": attempt_id,
                        "stage": "ai",
                        "request_id": request_id,
                        "attempt": attempt + 1,
                        "status": "failed",
                        "error_type": type(exc).__name__,
                        "error_message": str(exc),
                    }
                )
            parse_status = getattr(exc, "parse_status", "call_failed")
            last_error = str(parse_status)
            last_trace = SimpleNamespace(
                prompt_tokens=None,
                completion_tokens=None,
                ai_parse_status=parse_status,
                ai_parse_error=type(exc).__name__,
                ai_raw_response_hash=getattr(exc, "raw_response_hash", None),
                ai_fields_defaulted=[],
            )
            if audit is not None and request_id and attempt == max_attempts - 1:
                audit.fail(request_id, error_type=last_error, error_message=type(exc).__name__)
                audit.update(
                    request_id,
                    ai_parse_status=parse_status,
                    ai_parse_error=type(exc).__name__,
                    ai_raw_response_hash=getattr(exc, "raw_response_hash", None),
                    ai_fields_defaulted=[],
                )
            if attempt < max_attempts - 1:
                time.sleep(0.5 * (attempt + 1))
    return None, last_trace, last_error

def _search(
    provider,
    queries: list[str],
    *,
    attempt_callback: Callable[[dict[str, Any]], Any] | None = None,
) -> tuple[list[dict], str]:
    found: list[dict] = []
    seen: set[str] = set()
    provider_name = getattr(provider, "name", type(provider).__name__)
    for query in queries:
        attempt_id = None
        try:
            if getattr(provider, "name", "") == "Fallback" and attempt_callback is not None:
                results = provider.search(query, max_results=8, _crpd_attempt_callback=attempt_callback)
            else:
                if attempt_callback is not None:
                    attempt_id = attempt_callback({"phase": "before", "stage": "search", "query": query, "provider": provider_name})
                results = provider.search(query, max_results=8)
                if attempt_callback is not None:
                    attempt_callback({"phase": "after", "attempt_id": attempt_id, "stage": "search", "query": query, "provider": provider_name, "status_code": 200})
        except SearchCallBudgetExceeded:
            raise
        except Exception as exc:
            if attempt_callback is not None and attempt_id is not None:
                attempt_callback({"phase": "after", "attempt_id": attempt_id, "stage": "search", "query": query, "provider": provider_name, "error_type": type(exc).__name__, "error_message": str(exc)})
            found.append({"query": query, "error_type": type(exc).__name__})
            continue
        for item in results:
            url = canonicalize_url(item.url)
            if url in seen:
                continue
            seen.add(url)
            found.append({"query": query, "url": url, "title": item.title, "snippet": item.snippet, "provider": provider_name})
    return found, provider_name


def _ai_mapping_metadata(
    assessment: SourceAIAssessment | None,
    trace: object | None,
    error_type: str | None,
    *,
    fallback: bool = False,
    cache_hit: bool = False,
) -> dict:
    """Make model-field absence, explicit zeroes, and fallbacks distinguishable."""
    if fallback:
        return {
            "ai_parse_status": "default_fallback",
            "ai_parse_error": error_type or "no_model_response",
            "ai_raw_response_hash": None,
            "ai_fields_defaulted": sorted(SourceAIAssessment.model_fields),
        }
    if cache_hit:
        return {
            "ai_parse_status": "cache_replayed",
            "ai_parse_error": error_type,
            "ai_raw_response_hash": getattr(trace, "ai_raw_response_hash", None) if trace else None,
            "ai_fields_defaulted": [],
        }
    if trace is None:
        return {
            "ai_parse_status": "trace_metadata_missing",
            "ai_parse_error": error_type,
            "ai_raw_response_hash": None,
            "ai_fields_defaulted": [],
        }
    raw_fields_value = getattr(trace, "raw_fields", None)
    raw_fields = set(raw_fields_value or ())
    defaulted = [name for name in SourceAIAssessment.model_fields if raw_fields_value is not None and name not in raw_fields]
    return {
        "ai_parse_status": "cache_replayed" if cache_hit else str(getattr(trace, "ai_parse_status", "parsed")),
        "ai_parse_error": getattr(trace, "ai_parse_error", None) or error_type,
        "ai_raw_response_hash": getattr(trace, "ai_raw_response_hash", None) or getattr(trace, "raw_response_hash", None),
        "ai_fields_defaulted": sorted(defaulted),
    }


def _trace_value(trace: object | None, name: str) -> object | None:
    """Read provider trace metadata without assuming a concrete trace class."""
    if trace is None:
        return None
    if isinstance(trace, dict):
        return trace.get(name)
    return getattr(trace, name, None)


def _write_tables(run_dir: Path, requests: list[dict], responses: list[dict], proposals: list[dict], verification: list[dict]) -> None:
    for filename, rows in (("ai_requests.parquet", requests), ("ai_responses.parquet", responses), ("candidate_proposals.parquet", proposals), ("deterministic_verification.parquet", verification)):
        frame = pl.DataFrame(rows) if rows else _empty(["status"])
        atomic_write_parquet(frame, run_dir / filename, {"job_id": f"ai-table-{filename}"})


def _run_ai_batch_v2(
    settings: Settings,
    *,
    output: Path | None,
    max_slots: int,
    max_ai_calls: int,
    max_search_calls: int | None,
    concurrency: int,
    dry_run: bool,
    apply: bool,
    resume: bool,
    city: str | None,
    city_id: str | None,
    source_role: str | None,
    slot_id: str | None,
    global_audit_root: Path | None,
    discovery_mode: str,
    audit_existing: bool,
    ai_call_callback: Callable[[dict[str, Any]], Any] | None,
    search_call_callback: Callable[[dict[str, Any]], Any] | None,
) -> dict:
    """Run one finite, mode-aware discovery batch.

    Search evidence is append-only evidence; only the deterministic top three
    rows per slot can become formal candidates.  The LLM is never given a
    write path to verification or enablement.
    """
    if apply and dry_run:
        raise ValueError("--apply and --dry-run are mutually exclusive")
    if max_slots < 1 or max_slots > 50 or max_ai_calls < 0 or concurrency > 4:
        raise ValueError("limits exceed this finite run")
    if max_search_calls is None:
        max_search_calls = max(1, min(100, max_slots * 2))
    if max_search_calls < 0:
        raise ValueError("max_search_calls must be non-negative")
    requested_mode = str(discovery_mode or "AUTO").upper()
    if requested_mode not in {"AUTO", "DISABLED", "SEARCH_ONLY", "AI_ONLY", "SEARCH_AND_AI"}:
        raise ValueError("invalid discovery mode")

    run_dir = output or settings.outputs / "source_completion_ai" / datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "AI_INTERFACE_AUDIT.md").write_text(interface_audit(), encoding="utf-8")
    plan = build_ai_plan(settings, city=city, city_id=city_id, source_role=source_role, slot_id=slot_id, max_slots=max_slots, audit_existing=audit_existing)
    atomic_write_parquet(plan, run_dir / "ai_slot_plan.parquet", {"job_id": "ai-slot-plan"})

    requests: list[dict[str, Any]] = []
    responses: list[dict[str, Any]] = []
    proposals: list[dict[str, Any]] = []
    search_evidence: list[dict[str, Any]] = []
    verification: list[dict[str, Any]] = []
    cache_path = run_dir / "ai_cache.json"
    cache = json.loads(cache_path.read_text(encoding="utf-8")) if resume and cache_path.exists() else {}
    search_cache_path = run_dir / "search_cache.json"
    search_cache = json.loads(search_cache_path.read_text(encoding="utf-8")) if resume and search_cache_path.exists() else {}
    audit = AIAuditStore(run_dir, global_root=global_audit_root)
    recovered_audit = audit.recover_interrupted() if resume else []

    provider = None
    model = ""
    try:
        provider = get_ai_provider(settings)
        model = settings.siliconflow_chat_model or settings.glm_model
    except Exception:
        provider = None
    search_provider = build_search_fallback(settings)
    if getattr(search_provider, "name", "None") == "None":
        search_provider = build_search_provider("ddg", None)
    ddg_provider = build_search_provider("ddg", None)
    search_available = getattr(search_provider, "name", "None") != "None"
    existing_candidates = any(bool(row.get("best_candidate_url") or row.get("best_candidate_id")) for row in plan.iter_rows(named=True))
    effective_mode = requested_mode
    if requested_mode == "AUTO":
        if provider is not None and max_ai_calls > 0 and max_search_calls > 0:
            effective_mode = "SEARCH_AND_AI"
        elif max_search_calls > 0 and search_available:
            effective_mode = "SEARCH_ONLY"
        elif provider is not None and max_ai_calls > 0 and existing_candidates:
            effective_mode = "AI_ONLY"
        else:
            effective_mode = "DISABLED"
    elif requested_mode == "SEARCH_AND_AI":
        if not search_available or max_search_calls == 0:
            effective_mode = "AI_ONLY" if provider is not None and max_ai_calls > 0 and existing_candidates else "DISABLED"
        elif provider is None or max_ai_calls == 0:
            effective_mode = "SEARCH_ONLY"
    elif requested_mode == "SEARCH_ONLY" and (not search_available or max_search_calls == 0):
        effective_mode = "DISABLED"
    elif requested_mode == "AI_ONLY" and (provider is None or max_ai_calls == 0):
        effective_mode = "DISABLED"

    actual_ai_attempts = 0
    actual_search_attempts = 0
    cache_hits = 0
    reused_ai_calls = 0
    prevented_duplicate_calls = 0

    def ai_attempt(event: dict[str, Any]) -> Any:
        nonlocal actual_ai_attempts
        if event.get("phase") == "before":
            if actual_ai_attempts >= max_ai_calls:
                raise AICallBudgetExceeded("ai_calls budget exhausted")
            result = ai_call_callback(event) if ai_call_callback is not None else None
            actual_ai_attempts += 1
            return result or f"AIATTEMPT_{actual_ai_attempts:06d}"
        if ai_call_callback is not None:
            return ai_call_callback(event)
        return None

    def search_attempt(event: dict[str, Any]) -> Any:
        nonlocal actual_search_attempts
        if event.get("phase") == "before":
            if actual_search_attempts >= max_search_calls:
                raise SearchCallBudgetExceeded("search_calls budget exhausted")
            result = search_call_callback(event) if search_call_callback is not None else None
            actual_search_attempts += 1
            return result or f"SEARCHATTEMPT_{actual_search_attempts:06d}"
        if search_call_callback is not None:
            return search_call_callback(event)
        return None

    def search_queries(queries: list[str]) -> list[dict[str, Any]]:
        found: list[dict[str, Any]] = []
        seen: set[str] = set()
        for query in queries:
            key = _sha({"provider": getattr(search_provider, "name", type(search_provider).__name__), "query": query})
            if resume and key in search_cache:
                rows = list(search_cache[key])
            else:
                rows, _ = _search(search_provider, [query], attempt_callback=search_attempt)
                if not rows and getattr(search_provider, "name", "") != "DuckDuckGoHTML" and actual_search_attempts < max_search_calls:
                    fallback_rows, _ = _search(ddg_provider, [query], attempt_callback=search_attempt)
                    rows = fallback_rows
                search_cache[key] = rows
            for row in rows:
                item = {**row, "search_cache_key": key}
                url = canonicalize_url(str(item.get("url") or "")) if item.get("url") else ""
                if not url or url in seen:
                    if item.get("error_type"):
                        search_evidence.append({"query": query, **item, "evidence_type": "provider_error"})
                    continue
                seen.add(url)
                item["url"] = url
                found.append(item)
                search_evidence.append({"query": query, **item, "evidence_type": "search_result"})
        return found

    for row in plan.iter_rows(named=True):
        queries = _queries(row)
        search_rows: list[dict[str, Any]] = []
        if not dry_run and effective_mode in {"SEARCH_ONLY", "SEARCH_AND_AI"}:
            search_rows = search_queries(queries)
        context = {
            "stage": "source_discovery",
            "schema": "SourceAIAssessment",
            "provider": "siliconflow" if provider else "unavailable",
            "slot_id": row["slot_id"],
            "city": row["city_name"],
            "role": row["source_role"],
            "queries": queries,
            "search_result_count": len(search_rows),
        }
        prompt_hash = _sha(_assessment_prompt(row, queries, search_rows))
        request_hash = _sha({"stage": "source_discovery", "schema": "SourceAIAssessment", "provider": "siliconflow" if provider else "unavailable", "prompt_version": PROMPT_VERSION, "model": model, "prompt_hash": prompt_hash, "payload": context})
        request = {
            "request_id": f"REQ_{request_hash[:20]}",
            "run_id": run_dir.name,
            "stage": "source_discovery",
            "schema": "SourceAIAssessment",
            "slot_id": row["slot_id"],
            "city_id": row["city_id"],
            "source_role": row["source_role"],
            "provider": "siliconflow" if provider else "unavailable",
            "model": model or None,
            "model_version": model or None,
            "prompt_version": PROMPT_VERSION,
            "prompt_hash": prompt_hash,
            "request_hash": request_hash,
            "cache_key": request_hash,
            "input_summary": f"{row['city_name']} / {row['source_role']}",
            "created_at": _now(),
            "cache_hit": request_hash in cache,
            "search_queries": queries,
            "candidate_urls": [item.get("url") for item in search_rows if item.get("url")],
        }
        assessment: SourceAIAssessment | None = None
        trace = None
        error_type: str | None = None
        request_action = "none"
        if not dry_run and effective_mode in {"AI_ONLY", "SEARCH_AND_AI"} and provider is not None and (existing_candidates or search_rows or effective_mode == "SEARCH_AND_AI"):
            if request_hash in cache:
                assessment = SourceAIAssessment.model_validate(cache[request_hash])
                trace = SimpleNamespace(prompt_tokens=None, completion_tokens=None, ai_parse_status="cache_replayed", ai_parse_error=None, ai_raw_response_hash=_sha(cache[request_hash]), ai_fields_defaulted=[])
                audit.start(request)
                audit.complete(request["request_id"], response_hash=_sha(assessment.model_dump(mode="json")), response_payload=assessment.model_dump(mode="json"), prompt_tokens=None, completion_tokens=None, total_tokens=None, estimated_cost_usd=None, cache_hit=True, ai_parse_status="cache_replayed", ai_parse_error=None, ai_raw_response_hash=trace.ai_raw_response_hash, ai_fields_defaulted=[])
                cache_hits += 1
                reused_ai_calls += 1
                request_action = "local_cache"
            else:
                system, user = _assessment_prompt(row, queries, search_rows)
                try:
                    assessment, trace, error_type = _call_ai(provider, model, system, user, audit=audit, audit_payload=request, attempt_callback=ai_attempt)
                except AICallBudgetExceeded:
                    raise
                request_action = getattr(audit, "last_reservation_action", "claimed")
                if request_action == "reused":
                    reused_ai_calls += 1
                elif request_action == "in_flight":
                    prevented_duplicate_calls += 1
                if assessment is not None:
                    cache[request_hash] = assessment.model_dump(mode="json")
        elif not dry_run and effective_mode in {"AI_ONLY", "SEARCH_AND_AI"} and provider is None:
            audit.start(request)
            audit.fail(request["request_id"], error_type="provider_unavailable", error_message="AI provider unavailable; no request was sent")
            error_type = "provider_unavailable"
            request_action = "provider_unavailable"

        if assessment is None:
            assessment = SourceAIAssessment(search_queries=queries, confidence=0, recommended_action="requires_human_review" if error_type else "proposed", human_question="AI未返回结果；请依据搜索证据和确定性门禁复核。")
        request["cache_hit"] = request_action in {"reused", "local_cache"}
        request["reused_ai_call"] = request["cache_hit"]
        request.update(_ai_mapping_metadata(assessment, trace, error_type, fallback=trace is None and error_type is None and effective_mode not in {"AI_ONLY", "SEARCH_AND_AI"}, cache_hit=request["cache_hit"]))
        requests.append(request)
        if trace is not None or effective_mode in {"AI_ONLY", "SEARCH_AND_AI"}:
            responses.append({
                **request,
                "search_queries": assessment.search_queries,
                "institution_aliases": assessment.institution_aliases,
                "entry_type_hint": assessment.entry_type_hint,
                "pagination_hint": assessment.pagination_hint,
                "confidence": assessment.confidence,
                "recommended_action": assessment.recommended_action,
                "human_question": assessment.human_question,
                "prompt_tokens": getattr(trace, "prompt_tokens", None),
                "completion_tokens": getattr(trace, "completion_tokens", None),
                "total_tokens": (
                    int(_trace_value(trace, "prompt_tokens")) + int(_trace_value(trace, "completion_tokens"))
                    if _trace_value(trace, "prompt_tokens") is not None and _trace_value(trace, "completion_tokens") is not None
                    else None
                ),
                "estimated_cost_usd": None,
                "error_type": error_type,
                "status": "response_completed" if trace is not None and error_type is None else "response_failed" if error_type else "search_only",
                "provider_has_search": False,
            })
        for found in search_rows:
            url = str(found.get("url") or "")
            if not url:
                continue
            proposals.append({
                "proposal_id": f"PROP_{_sha((row['slot_id'], url))[:20]}",
                "slot_id": row["slot_id"],
                "city_id": row["city_id"],
                "city_name": row["city_name"],
                "source_role": row["source_role"],
                "candidate_url": url,
                "candidate_title": found.get("title"),
                "candidate_snippet": found.get("snippet"),
                "discovery_method": "search_provider" if effective_mode == "SEARCH_ONLY" else "ai_assisted_search",
                "search_query": found.get("query"),
                "discovery_provider": found.get("provider") or getattr(search_provider, "name", type(search_provider).__name__),
                "ai_confidence": assessment.confidence,
                "ai_recommended_action": assessment.recommended_action,
                "candidate_kind": "search_result_pending_review",
                "entry_eligible_guess": False,
                "status": "proposed",
                "created_at": _now(),
            })

    if search_cache:
        _json(search_cache_path, search_cache)
    if cache:
        _json(cache_path, cache)

    proposals_by_slot: dict[str, list[dict[str, Any]]] = {}
    for item in proposals:
        proposals_by_slot.setdefault(str(item["slot_id"]), []).append(item)
    selected_items: list[dict[str, Any]] = []
    for _slot, rows in proposals_by_slot.items():
        rows.sort(key=lambda item: (0 if ".gov.cn" in str(item.get("candidate_url") or "").lower() else 1, -float(item.get("ai_confidence") or 0), str(item.get("candidate_url") or "")))
        for rank, item in enumerate(rows[:3], 1):
            item["selection_status"] = "selected_top3"
            item["selection_rank"] = rank
            selected_items.append(item)
        for item in rows[3:]:
            item["selection_status"] = "search_evidence_only"
            item["selection_rank"] = None

    applied = 0
    probed = 0
    if apply:
        for item in selected_items:
            url = canonicalize_url(str(item["candidate_url"]))
            candidate_kind = "official_entry_candidate" if ".gov.cn" in url.lower() else "search_result_pending_review"
            item["candidate_kind"] = candidate_kind
            item["entry_eligible_guess"] = candidate_kind == "official_entry_candidate"
            item["candidate_id"] = stable_id(item["slot_id"], url, candidate_kind, prefix="SRCCAND")
            upsert_candidates([{"candidate_id": item["candidate_id"], "city_id": item["city_id"], "source_role": item["source_role"], "candidate_url": url, "discovery_method": item["discovery_method"], "discovery_evidence_url": url, "discovery_evidence_text": item.get("candidate_snippet"), "official_domain_evidence": "search provider evidence; deterministic gate pending", "city_match_evidence": None, "role_match_evidence": None, "is_verified": False, "is_enabled": False, "manual_review_status": "pending_probe", "generation_batch_id": run_dir.name}], settings)
            applied += 1
            if candidate_kind == "official_entry_candidate":
                try:
                    probe_result = probe_candidates(candidate_id=item["candidate_id"], rounds=2, settings=settings)
                    probed += 1
                    verification.append({"candidate_id": item["candidate_id"], "probe": probe_result, "status": "probe_completed"})
                except Exception as exc:
                    probed += 1
                    verification.append({"candidate_id": item["candidate_id"], "status": "probe_failed", "error_type": type(exc).__name__})

    evidence_frame = pl.DataFrame(search_evidence, infer_schema_length=None) if search_evidence else _empty(["query", "url", "evidence_type"])
    proposal_frame = pl.DataFrame(proposals, infer_schema_length=None) if proposals else _empty(["proposal_id", "slot_id", "candidate_url"])
    storage_frame = proposal_frame
    atomic_write_parquet(evidence_frame, run_dir / "search_evidence.parquet", {"job_id": "source-search-evidence"})
    atomic_write_parquet(storage_frame, run_dir / "candidate_proposals.parquet", {"job_id": "source-candidate-proposals"})
    _write_tables(run_dir, requests, [record for record in (audit.records() or responses) if record.get("status") != "request_started"], proposals, verification)
    before = {"status": "captured", "queue_rows": int(plan.height), "verified_enabled_in_plan": int(plan.filter(pl.col("ai_status") == "A_verified_enabled").height) if "ai_status" in plan.columns else 0}
    after_audit = audit_525(settings) if apply else {"status": "not_applied"}
    _json(run_dir / "slot_audit_before.json", before)
    _json(run_dir / "slot_audit_after.json", after_audit)

    audit_records = audit.records()
    billable = [record for record in audit_records if record.get("status") == "response_completed" and not record.get("cache_hit")]
    prompt_values = [record.get("prompt_tokens") for record in billable]
    completion_values = [record.get("completion_tokens") for record in billable]
    total_values = [record.get("total_tokens") for record in billable]
    cost_values = [record.get("estimated_cost_usd") for record in billable]
    usage_complete = bool(billable) and all(value is not None for value in total_values)
    cost_complete = usage_complete and all(value is not None for value in cost_values)
    _json(run_dir / "ai_cost_summary.json", {"ai_calls": actual_ai_attempts, "persisted_ai_calls": len(billable), "reused_ai_calls": reused_ai_calls, "prevented_duplicate_calls": prevented_duplicate_calls, "prompt_tokens": sum(int(value) for value in prompt_values) if billable and all(value is not None for value in prompt_values) else None, "completion_tokens": sum(int(value) for value in completion_values) if billable and all(value is not None for value in completion_values) else None, "total_tokens": sum(int(value) for value in total_values) if usage_complete else None, "estimated_cost_usd": sum(float(value) for value in cost_values) if cost_complete else None, "usage_status": "available" if usage_complete else "unavailable", "cost_status": "available" if cost_complete else "unavailable", "cache_entries": len(cache), "recovered_interrupted": len(recovered_audit)})

    review_rows = [item for item in selected_items if float(item.get("ai_confidence") or 0) < 0.7 or not item.get("entry_eligible_guess")]
    review = pl.DataFrame(review_rows) if review_rows else _empty(["review_id", "city", "role", "candidate_url", "exact_question_for_human", "allowed_decisions", "machine_recommendation", "impact", "priority"])
    if review_rows:
        review = review.with_columns([pl.col("proposal_id").alias("review_id"), pl.col("city_name").alias("city"), pl.col("source_role").alias("role"), pl.lit("请确认该 URL 是否为对应城市和角色的官方栏目入口，并检查页面类型与分页证据。").alias("exact_question_for_human"), pl.lit(json.dumps(["approve_primary", "approve_alternative", "reject_all", "change_role", "defer", "quarantine"], ensure_ascii=False)).alias("allowed_decisions"), pl.col("ai_recommended_action").alias("machine_recommendation"), pl.lit("人工决定会影响该槽位是否进入确定性探测和后续准入；不会直接写入 verified。").alias("impact"), pl.lit(1).alias("priority")]).select(["review_id", "city", "role", "candidate_url", "exact_question_for_human", "allowed_decisions", "machine_recommendation", "impact", "priority"])
    review.write_excel(run_dir / "HUMAN_REVIEW_QUEUE.xlsx", autofit=True)
    (run_dir / "HUMAN_REVIEW_GUIDE.md").write_text("# AI来源人工复核指南\n\nAI只产生查询、搜索证据和排序建议；人工决定也必须经过确定性双探测、解析和分页门禁。不得直接修改 Parquet、YAML、verified 或 enabled。\n", encoding="utf-8")
    _json(run_dir / "pytest_summary.json", {"status": "unknown"})
    _json(run_dir / "secret_scan.json", {"status": "pending", "keys_redacted": True})
    _json(run_dir / "github_publish_result.json", {"status": "not_requested"})
    _json(run_dir / "blockers.json", {"go_no_go": "BLOCKED", "blockers": ["AI_RESULTS_REQUIRE_DETERMINISTIC_GATES"], "full_run_started": False, "full_ai_started": False})
    (run_dir / "NEXT_AI_BATCH_COMMAND.ps1").write_text("$ErrorActionPreference='Stop'\n$env:CRPD_DATA_ROOT='D:\\Data Set\\CRPD'\n.\\.venv\\Scripts\\python.exe -m policydb.source_completion_ai_workflow batch --discovery-mode SEARCH_AND_AI --max-slots 50 --max-ai-calls 100 --max-search-calls 100 --concurrency 4 --apply --resume --output '" + str(run_dir) + "'\n", encoding="utf-8")

    provider_operational = any(record.get("status") == "response_completed" and not record.get("cache_hit") for record in audit_records)
    report = {
        "run_dir": str(run_dir),
        "planned_slots": int(plan.height),
        "effective_discovery_mode": effective_mode,
        "ai_calls": actual_ai_attempts,
        "ai_attempts": actual_ai_attempts,
        "persisted_ai_calls": len(billable),
        "reused_ai_calls": reused_ai_calls,
        "prevented_duplicate_calls": prevented_duplicate_calls,
        "recovered_interrupted": len(recovered_audit),
        "search_calls": actual_search_attempts,
        "candidate_proposals": len(proposals),
        "applied_candidates": applied,
        "probed_candidates": probed,
        "strict_verified_added": 0,
        "strict_enabled_added": 0,
        "human_review": len(review_rows),
        "dry_run": dry_run,
        "apply": apply,
        "search_provider": getattr(search_provider, "name", type(search_provider).__name__),
        "ai_provider": "siliconflow" if provider else "unavailable",
        "provider_status": "operational" if provider_operational else "configured" if provider else "unavailable",
        "api_balance_status": "call_succeeded" if provider_operational else "unknown",
        "tokens": sum(int(value) for value in total_values) if usage_complete else None,
        "cost": sum(float(value) for value in cost_values) if cost_complete else None,
        "usage_status": "available" if usage_complete else "unavailable",
        "concurrency_cap": concurrency,
        "audit_existing": audit_existing,
    }
    _json(run_dir / "run_summary.json", report)
    (run_dir / "AI_SOURCE_COMPLETION_REPORT.md").write_text("# AI来源补齐有限批次报告\n\n" + json.dumps(report, ensure_ascii=False, indent=2) + "\n\nAI结果不能直接写入 verified 或 enabled；所有 URL 仍需确定性门禁。\n", encoding="utf-8")
    return report


def run_ai_batch(settings: Settings, *, output: Path | None = None, max_slots: int = 50, max_ai_calls: int = 100, max_search_calls: int | None = None, concurrency: int = 4, dry_run: bool = True, apply: bool = False, resume: bool = False, city: str | None = None, city_id: str | None = None, source_role: str | None = None, slot_id: str | None = None, global_audit_root: Path | None = None, discovery_mode: str = "AUTO", audit_existing: bool = False, ai_call_callback: Callable[[dict[str, Any]], Any] | None = None, search_call_callback: Callable[[dict[str, Any]], Any] | None = None) -> dict:
    return _run_ai_batch_v2(settings, output=output, max_slots=max_slots, max_ai_calls=max_ai_calls, max_search_calls=max_search_calls, concurrency=concurrency, dry_run=dry_run, apply=apply, resume=resume, city=city, city_id=city_id, source_role=source_role, slot_id=slot_id, global_audit_root=global_audit_root, discovery_mode=discovery_mode, audit_existing=audit_existing, ai_call_callback=ai_call_callback, search_call_callback=search_call_callback)
    if apply and dry_run:
        raise ValueError("--apply and --dry-run are mutually exclusive")
    if max_slots > 50 or max_ai_calls > 100 or concurrency > 4:
        raise ValueError("limits exceed this finite run: max_slots<=50, max_ai_calls<=100, concurrency<=4")
    run_dir = output or settings.outputs / "source_completion_ai" / datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "AI_INTERFACE_AUDIT.md").write_text(interface_audit(), encoding="utf-8")
    plan = build_ai_plan(settings, city=city, city_id=city_id, source_role=source_role, slot_id=slot_id, max_slots=max_slots)
    atomic_write_parquet(plan, run_dir / "ai_slot_plan.parquet", {"job_id": "ai-slot-plan"})
    requests: list[dict] = []
    responses: list[dict] = []
    proposals: list[dict] = []
    verification: list[dict] = []
    cache_path = run_dir / "ai_cache.json"
    cache = json.loads(cache_path.read_text(encoding="utf-8")) if resume and cache_path.exists() else {}
    audit = AIAuditStore(run_dir, global_root=global_audit_root)
    recovered_audit = audit.recover_interrupted() if resume else []
    provider = None
    model = ""
    ai_calls = 0
    reused_ai_calls = 0
    prevented_duplicate_calls = 0
    token_prompt = token_completion = 0
    try:
        provider = get_ai_provider(settings)
        model = settings.siliconflow_chat_model or settings.glm_model
    except Exception:
        provider = None
    search_provider = build_search_fallback(settings)
    if getattr(search_provider, "name", "None") == "None":
        search_provider = build_search_provider("ddg", None)
    for row in plan.iter_rows(named=True):
        queries = _queries(row)
        context = {"stage": "source_discovery", "schema": "SourceAIAssessment", "provider": "siliconflow" if provider else "unavailable", "slot_id": row["slot_id"], "city": row["city_name"], "role": row["source_role"], "queries": queries}
        prompt_hash = _sha(_assessment_prompt(row, queries))
        request_hash = _sha({"stage": "source_discovery", "schema": "SourceAIAssessment", "provider": "siliconflow" if provider else "unavailable", "prompt_version": PROMPT_VERSION, "model": model, "prompt_hash": prompt_hash, "payload": context})
        request = {"request_id": f"REQ_{request_hash[:20]}", "run_id": run_dir.name, "stage": "source_discovery", "schema": "SourceAIAssessment", "slot_id": row["slot_id"], "city_id": row["city_id"], "source_role": row["source_role"], "provider": "siliconflow" if provider else "unavailable", "model": model or None, "model_version": model or None, "prompt_version": PROMPT_VERSION, "prompt_hash": prompt_hash, "request_hash": request_hash, "cache_key": request_hash, "input_summary": f"{row['city_name']} / {row['source_role']}", "created_at": _now(), "cache_hit": request_hash in cache}
        requests.append(request)
        assessment = None
        trace = None
        error_type = None
        request_action = "none"
        if not dry_run and provider and ai_calls < max_ai_calls and request_hash not in cache:
            system, user = _assessment_prompt(row, queries)
            assessment, trace, error_type = _call_ai(
                provider,
                model,
                system,
                user,
                audit=audit,
                audit_payload=request,
            )
            request_action = getattr(audit, "last_reservation_action", "claimed")
            if request_action == "reused":
                reused_ai_calls += 1
            elif request_action == "in_flight":
                prevented_duplicate_calls += 1
            else:
                ai_calls += 1
            if assessment:
                cache[request_hash] = assessment.model_dump(mode="json")
        elif request_hash in cache:
            assessment = SourceAIAssessment.model_validate(cache[request_hash])
            trace = SimpleNamespace(
                prompt_tokens=None,
                completion_tokens=None,
                ai_parse_status="cache_replayed",
                ai_parse_error=None,
                ai_raw_response_hash=_sha(cache[request_hash]),
                ai_fields_defaulted=[],
            )
            audit.start(request)
            audit.complete(
                request["request_id"],
                response_hash=_sha(assessment.model_dump(mode="json")),
                prompt_tokens=None,
                completion_tokens=None,
                total_tokens=None,
                estimated_cost_usd=None,
                cache_hit=True,
                response_payload=assessment.model_dump(mode="json"),
                ai_parse_status="cache_replayed",
                ai_parse_error=None,
                ai_raw_response_hash=trace.ai_raw_response_hash,
                ai_fields_defaulted=[],
            )
            reused_ai_calls += 1
            request_action = "local_cache"
        elif not dry_run:
            audit.start(request)
            audit.fail(
                request["request_id"],
                error_type="provider_unavailable",
                error_message="AI provider unavailable; no request was sent",
            )
            request_action = "provider_unavailable"
        request["cache_hit"] = request_action in {"reused", "local_cache"}
        request["reused_ai_call"] = request_action in {"reused", "local_cache"}
        # A failed parse/call must remain distinguishable from a dry-run or
        # unavailable-provider fallback.  The fallback assessment below is
        # only a display value; it must not erase the failure metadata.
        used_default_fallback = assessment is None and error_type is None
        if assessment is None:
            assessment = SourceAIAssessment(search_queries=queries, confidence=0, recommended_action="requires_human_review" if not provider else "proposed", human_question="AI不可用或处于dry-run；请使用真实搜索证据和确定性门禁。")
        mapping = _ai_mapping_metadata(
            assessment,
            trace,
            error_type,
            fallback=used_default_fallback,
            cache_hit=request_action in {"reused", "local_cache"},
        )
        request.update(mapping)
        if trace:
            token_prompt += int(getattr(trace, "prompt_tokens", 0) or 0)
            token_completion += int(getattr(trace, "completion_tokens", 0) or 0)
        responses.append({**request, "search_queries": assessment.search_queries, "institution_aliases": assessment.institution_aliases, "entry_type_hint": assessment.entry_type_hint, "pagination_hint": assessment.pagination_hint, "confidence": assessment.confidence, "recommended_action": assessment.recommended_action, "human_question": assessment.human_question, "prompt_tokens": getattr(trace, "prompt_tokens", None), "completion_tokens": getattr(trace, "completion_tokens", None), "estimated_cost_usd": None, "error_type": error_type, "status": "dry_run" if dry_run else ("completed" if assessment is not None and error_type is None else "awaiting_api_key"), "provider_has_search": False, "cache_hit": bool(request.get("cache_hit")), "reused_ai_call": bool(request.get("reused_ai_call"))})
        if responses:
            responses[-1]["status"] = (
                "dry_run"
                if dry_run
                else "response_completed"
                if not used_default_fallback and error_type is None
                else "response_failed"
            )
        search_rows, search_name = _search(search_provider, assessment.search_queries or queries)
        for found in search_rows:
            if not found.get("url"):
                continue
            candidate_kind = "search_result_pending_review"
            proposals.append({"proposal_id": f"PROP_{_sha((row['slot_id'], found['url']))[:20]}", "slot_id": row["slot_id"], "city_id": row["city_id"], "city_name": row["city_name"], "source_role": row["source_role"], "candidate_url": found["url"], "candidate_title": found.get("title"), "candidate_snippet": found.get("snippet"), "discovery_method": "ai_assisted_search", "search_query": found.get("query"), "discovery_provider": found.get("provider", search_name), "ai_confidence": assessment.confidence, "ai_recommended_action": assessment.recommended_action, "candidate_kind": candidate_kind, "entry_eligible_guess": False, "status": "proposed", "created_at": _now()})
    cache_path.write_text(json.dumps(cache, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    applied = 0
    probed = 0
    if apply and proposals:
        for item in proposals:
            item["candidate_kind"] = "official_entry_candidate" if ".gov.cn" in item["candidate_url"] else "search_result_pending_review"
            item["entry_eligible_guess"] = item["candidate_kind"] == "official_entry_candidate"
            item["candidate_id"] = stable_id(item["slot_id"], canonicalize_url(item["candidate_url"]), item["candidate_kind"], prefix="SRCCAND")
            upsert_candidates([{"candidate_id": item["candidate_id"], "city_id": item["city_id"], "source_role": item["source_role"], "candidate_url": item["candidate_url"], "discovery_method": item["discovery_method"], "discovery_evidence_url": item["candidate_url"], "discovery_evidence_text": item.get("candidate_snippet"), "official_domain_evidence": "search result only; pending deterministic verification", "city_match_evidence": None, "role_match_evidence": None, "is_verified": False, "is_enabled": False, "manual_review_status": "pending_probe", "generation_batch_id": run_dir.name}], settings)
            applied += 1
            if item["entry_eligible_guess"] and ".gov.cn" in item["candidate_url"]:
                try:
                    result = probe_candidates(candidate_id=item["candidate_id"], rounds=2, settings=settings)
                    probed += int(result.get("checked", 0))
                except Exception as exc:
                    verification.append({"candidate_id": item["candidate_id"], "status": "probe_failed", "error_type": type(exc).__name__})
        candidates_path = settings.curated / "source_candidates.parquet"
        if candidates_path.exists():
            candidates = read_parquet_snapshot(candidates_path)
            verification.extend(candidates.filter(pl.col("candidate_id").is_in([item.get("candidate_id") for item in proposals if item.get("candidate_id")])).select(["candidate_id", "is_verified", "is_enabled", "http_status", "health_probe_success_count", "parser_status", "pagination_strategy"]).to_dicts())
    audit_records = audit.records()
    _write_tables(
        run_dir,
        audit_records,
        [record for record in audit_records if record.get("status") != "request_started"],
        proposals,
        verification,
    )
    before = {"status": "captured", "queue_rows": int(plan.height), "verified_enabled_in_plan": int(plan.filter(pl.col("ai_status") == "A_verified_enabled").height)}
    after_audit = audit_525(settings) if apply else {"status": "not_applied"}
    _json(run_dir / "slot_audit_before.json", before)
    _json(run_dir / "slot_audit_after.json", after_audit)
    billable_records = [record for record in audit_records if record.get("status") == "response_completed" and not record.get("cache_hit")]
    billable_tokens = [record.get("total_tokens") for record in billable_records]
    billable_costs = [record.get("estimated_cost_usd") for record in billable_records]
    _json(run_dir / "ai_cost_summary.json", {"ai_calls": len(billable_records), "reused_ai_calls": reused_ai_calls, "prevented_duplicate_calls": prevented_duplicate_calls, "prompt_tokens": sum(int(record.get("prompt_tokens") or 0) for record in billable_records) if billable_records and all(record.get("prompt_tokens") is not None for record in billable_records) else None, "completion_tokens": sum(int(record.get("completion_tokens") or 0) for record in billable_records) if billable_records and all(record.get("completion_tokens") is not None for record in billable_records) else None, "total_tokens": sum(int(value) for value in billable_tokens) if billable_records and all(value is not None for value in billable_tokens) else None, "estimated_cost_usd": sum(float(value) for value in billable_costs) if billable_records and all(value is not None for value in billable_costs) else None, "cost_status": "token_totals_persisted_pricing_unconfigured", "usage_status": "available" if billable_records and all(value is not None for value in billable_tokens) else "unavailable", "cache_entries": len(cache), "recovered_interrupted": len(recovered_audit)})
    review_rows = [item for item in proposals if float(item.get("ai_confidence") or 0) < 0.7 or item.get("entry_eligible_guess") is False]
    review = pl.DataFrame(review_rows) if review_rows else _empty(["review_id", "city", "role", "candidate_url", "exact_question_for_human", "allowed_decisions", "machine_recommendation", "impact", "priority"])
    if review_rows:
        review = review.with_columns([pl.col("proposal_id").alias("review_id"), pl.col("city_name").alias("city"), pl.col("source_role").alias("role"), pl.lit("该候选是否为正式官方栏目入口？请根据页面、角色和分页证据裁决。").alias("exact_question_for_human"), pl.lit(json.dumps(["approve_primary", "approve_alternative", "reject_all", "change_role", "accept_municipal_substitute", "defer", "quarantine"], ensure_ascii=False)).alias("allowed_decisions"), pl.col("ai_recommended_action").alias("machine_recommendation"), pl.lit("影响对应城市和角色的必需来源槽位").alias("impact"), pl.lit(1).alias("priority")]).select(["review_id", "city", "role", "candidate_url", "exact_question_for_human", "allowed_decisions", "machine_recommendation", "impact", "priority"])
    review.write_excel(run_dir / "HUMAN_REVIEW_QUEUE.xlsx", autofit=True)
    (run_dir / "HUMAN_REVIEW_GUIDE.md").write_text("# AI来源人工复核指南\n\nAI只产生查询和候选建议；人工不得直接修改Parquet或YAML。使用结构化decision导入，所有候选仍须通过确定性双探测和verify。\n", encoding="utf-8")
    _json(run_dir / "pytest_summary.json", {"status": "pending"})
    _json(run_dir / "secret_scan.json", {"status": "pending", "scan_scope": "repository source and staged files", "keys_redacted": True})
    _json(run_dir / "github_publish_result.json", {"status": "pending"})
    _json(run_dir / "blockers.json", {"go_no_go": "BLOCKED", "blockers": ([] if provider else ["AI_PROVIDER_UNAVAILABLE"]), "full_run_started": False, "full_ai_started": False})
    (run_dir / "NEXT_AI_BATCH_COMMAND.ps1").write_text("$ErrorActionPreference='Stop'\n$env:CRPD_DATA_ROOT='D:\\Data Set\\CRPD'\n.\\.venv\\Scripts\\python.exe -m policydb.source_completion_ai_workflow batch --max-slots 50 --max-ai-calls 100 --concurrency 4 --apply --output '" + str(run_dir) + "'\n", encoding="utf-8")
    report = {
        "run_dir": str(run_dir),
        "planned_slots": plan.height,
        "ai_calls": ai_calls,
        "ai_attempts": ai_calls,
        "persisted_ai_calls": len([record for record in audit_records if record.get("status") == "response_completed" and not record.get("cache_hit")]),
        "reused_ai_calls": reused_ai_calls,
        "prevented_duplicate_calls": prevented_duplicate_calls,
        "recovered_interrupted": len(recovered_audit),
        "candidate_proposals": len(proposals),
        "applied_candidates": applied,
        "probed_candidates": probed,
        "strict_verified_added": 0,
        "strict_enabled_added": 0,
        "human_review": len(review_rows),
        "dry_run": dry_run,
        "apply": apply,
        "search_provider": getattr(search_provider, "name", type(search_provider).__name__),
        "ai_provider": "siliconflow" if provider else "unavailable",
        "concurrency_cap": concurrency,
    }
    _json(run_dir / "run_summary.json", report)
    (run_dir / "AI_SOURCE_COMPLETION_REPORT.md").write_text("# AI来源补齐有限批次报告\n\n" + json.dumps(report, ensure_ascii=False, indent=2) + "\n\nAI结果永不直接写入verified；所有URL仍需确定性验证。\n", encoding="utf-8")
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="CRPD finite AI-assisted source completion")
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("plan", "batch"):
        command = sub.add_parser(name)
        command.add_argument("--city")
        command.add_argument("--city-id")
        command.add_argument("--source-role")
        command.add_argument("--slot-id")
        command.add_argument("--max-slots", type=int, default=50)
        command.add_argument("--max-ai-calls", type=int, default=100)
        command.add_argument("--max-search-calls", type=int)
        command.add_argument("--concurrency", type=int, default=4)
        command.add_argument("--discovery-mode", choices=["AUTO", "DISABLED", "SEARCH_ONLY", "AI_ONLY", "SEARCH_AND_AI"], default="AUTO")
        command.add_argument("--audit-existing", action="store_true")
        command.add_argument("--apply", action="store_true")
        command.add_argument("--dry-run", action="store_true")
        command.add_argument("--resume", action="store_true")
        command.add_argument("--output", type=Path)
    args = parser.parse_args()
    settings = Settings.discover()
    if args.command == "plan":
        run_dir = args.output or settings.outputs / "source_completion_ai" / datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        run_dir.mkdir(parents=True, exist_ok=True)
        (run_dir / "AI_INTERFACE_AUDIT.md").write_text(interface_audit(), encoding="utf-8")
        plan = build_ai_plan(settings, city=args.city, city_id=args.city_id, source_role=args.source_role, slot_id=args.slot_id, max_slots=args.max_slots, audit_existing=args.audit_existing)
        atomic_write_parquet(plan, run_dir / "ai_slot_plan.parquet", {"job_id": "ai-slot-plan"})
        print(json.dumps({"run_dir": str(run_dir), "planned_slots": plan.height}, ensure_ascii=False))
    else:
        print(json.dumps(run_ai_batch(settings, output=args.output, max_slots=args.max_slots, max_ai_calls=args.max_ai_calls, max_search_calls=args.max_search_calls, concurrency=args.concurrency, dry_run=args.dry_run or not args.apply, apply=args.apply, resume=args.resume, city=args.city, city_id=args.city_id, source_role=args.source_role, slot_id=args.slot_id, discovery_mode=args.discovery_mode, audit_existing=args.audit_existing), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
