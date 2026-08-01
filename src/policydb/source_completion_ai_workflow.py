from __future__ import annotations

import argparse
import hashlib
import json
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

import polars as pl
from pydantic import BaseModel, Field

from policydb.ai import get_ai_provider
from policydb.ai_audit import AIAuditStore
from policydb.config.providers import build_search_fallback, build_search_provider
from policydb.crawl.dedup import canonicalize_url
from policydb.settings import Settings
from policydb.source_completion import build_slot_work_queue
from policydb.source_slots import audit_525, probe_candidates, upsert_candidates
from policydb.transform.normalization import stable_id

PROMPT_VERSION = "source-completion-v1"


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
    if raw == "verified_enabled":
        return "A_verified_enabled"
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


def build_ai_plan(settings: Settings, *, city: str | None = None, city_id: str | None = None, source_role: str | None = None, slot_id: str | None = None, max_slots: int = 50) -> pl.DataFrame:
    if max_slots < 1 or max_slots > 50:
        raise ValueError("max_slots must be between 1 and 50")
    base = build_slot_work_queue(settings)
    queue = base.with_columns(
        pl.Series("ai_status", [_ai_status(row) for row in base.iter_rows(named=True)])
    )
    if city:
        queue = queue.filter(pl.col("city_name") == city)
    if city_id:
        queue = queue.filter(pl.col("city_id") == city_id)
    if source_role:
        queue = queue.filter(pl.col("source_role") == source_role)
    if slot_id:
        queue = queue.filter(pl.col("slot_id") == slot_id)
    priority = {"A_verified_enabled": 0, "B_candidate_exists_needs_probe": 1, "C_candidate_failed_fixable": 2, "D_no_candidate_ai_discoverable": 3, "E_ambiguous_human_review": 4, "F_blocked_network": 5, "G_blocked_parser": 6}
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


def _assessment_prompt(row: dict, queries: list[str]) -> tuple[str, str]:
    system = "你是来源发现辅助器。只输出符合JSON schema的结构化建议。不要伪造URL，不要声称已联网，不要决定官方认证、HTTP健康、分页或is_verified。"
    user = json.dumps({"city": row["city_name"], "city_id": row["city_id"], "role": row["source_role"], "existing_candidate": row.get("best_candidate_url"), "queries": queries, "instruction": "生成更好的官方检索词、机构别名、页面类型和人工问题。"}, ensure_ascii=False)
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
) -> tuple[BaseModel | None, object | None, str | None]:
    if audit is not None and audit_payload is not None:
        audit.start(audit_payload)
    request_id = str((audit_payload or {}).get("request_id", ""))
    last_error = None
    for attempt in range(max_attempts):
        if audit is not None and request_id:
            audit.update(request_id, attempt=attempt + 1)
        try:
            value, trace = provider.structured(model=model, system=system, user=user, schema=schema)
            if audit is not None and request_id:
                audit.complete(
                    request_id,
                    response_hash=_sha(value.model_dump(mode="json")),
                    prompt_tokens=getattr(trace, "prompt_tokens", None),
                    completion_tokens=getattr(trace, "completion_tokens", None),
                    total_tokens=(getattr(trace, "prompt_tokens", 0) or 0) + (getattr(trace, "completion_tokens", 0) or 0),
                    estimated_cost_usd=None,
                    cache_hit=False,
                )
            return value, trace, None
        except Exception as exc:
            last_error = type(exc).__name__
            if audit is not None and request_id and attempt == max_attempts - 1:
                audit.fail(request_id, error_type=last_error, error_message=str(exc))
            if attempt < max_attempts - 1:
                time.sleep(0.5 * (attempt + 1))
    return None, None, last_error


def _search(provider, queries: list[str]) -> tuple[list[dict], str]:
    found: list[dict] = []
    seen: set[str] = set()
    provider_name = getattr(provider, "name", type(provider).__name__)
    for query in queries:
        try:
            results = provider.search(query, max_results=8)
        except Exception as exc:
            found.append({"query": query, "error_type": type(exc).__name__})
            continue
        for item in results:
            url = canonicalize_url(item.url)
            if url in seen:
                continue
            seen.add(url)
            found.append({"query": query, "url": url, "title": item.title, "snippet": item.snippet, "provider": provider_name})
    return found, provider_name


def _write_tables(run_dir: Path, requests: list[dict], responses: list[dict], proposals: list[dict], verification: list[dict]) -> None:
    for filename, rows in (("ai_requests.parquet", requests), ("ai_responses.parquet", responses), ("candidate_proposals.parquet", proposals), ("deterministic_verification.parquet", verification)):
        frame = pl.DataFrame(rows) if rows else _empty(["status"])
        frame.write_parquet(run_dir / filename, compression="zstd")


def run_ai_batch(settings: Settings, *, output: Path | None = None, max_slots: int = 50, max_ai_calls: int = 100, concurrency: int = 4, dry_run: bool = True, apply: bool = False, resume: bool = False, city: str | None = None, city_id: str | None = None, source_role: str | None = None, slot_id: str | None = None) -> dict:
    if apply and dry_run:
        raise ValueError("--apply and --dry-run are mutually exclusive")
    if max_slots > 50 or max_ai_calls > 100 or concurrency > 4:
        raise ValueError("limits exceed this finite run: max_slots<=50, max_ai_calls<=100, concurrency<=4")
    run_dir = output or settings.outputs / "source_completion_ai" / datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "AI_INTERFACE_AUDIT.md").write_text(interface_audit(), encoding="utf-8")
    plan = build_ai_plan(settings, city=city, city_id=city_id, source_role=source_role, slot_id=slot_id, max_slots=max_slots)
    plan.write_parquet(run_dir / "ai_slot_plan.parquet", compression="zstd")
    requests: list[dict] = []
    responses: list[dict] = []
    proposals: list[dict] = []
    verification: list[dict] = []
    cache_path = run_dir / "ai_cache.json"
    cache = json.loads(cache_path.read_text(encoding="utf-8")) if resume and cache_path.exists() else {}
    audit = AIAuditStore(run_dir)
    recovered_audit = audit.recover_interrupted() if resume else []
    provider = None
    model = ""
    ai_calls = 0
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
        context = {"slot_id": row["slot_id"], "city": row["city_name"], "role": row["source_role"], "queries": queries}
        request_hash = _sha({"prompt_version": PROMPT_VERSION, "model": model, "context": context})
        request = {"request_id": f"REQ_{request_hash[:20]}", "slot_id": row["slot_id"], "city_id": row["city_id"], "source_role": row["source_role"], "provider": "siliconflow" if provider else "unavailable", "model": model or None, "model_version": model or None, "prompt_version": PROMPT_VERSION, "prompt_hash": _sha(_assessment_prompt(row, queries)), "request_hash": request_hash, "cache_key": request_hash, "input_summary": f"{row['city_name']} / {row['source_role']}", "created_at": _now(), "cache_hit": request_hash in cache}
        requests.append(request)
        assessment = None
        trace = None
        error_type = None
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
            ai_calls += 1
            if assessment:
                cache[request_hash] = assessment.model_dump(mode="json")
        elif request_hash in cache:
            assessment = SourceAIAssessment.model_validate(cache[request_hash])
            audit.start(request)
            audit.complete(
                request["request_id"],
                response_hash=_sha(assessment.model_dump(mode="json")),
                prompt_tokens=0,
                completion_tokens=0,
                total_tokens=0,
                estimated_cost_usd=0,
                cache_hit=True,
            )
        elif not dry_run:
            audit.start(request)
            audit.fail(
                request["request_id"],
                error_type="provider_unavailable",
                error_message="AI provider unavailable; no request was sent",
            )
        if assessment is None:
            assessment = SourceAIAssessment(search_queries=queries, confidence=0, recommended_action="requires_human_review" if not provider else "proposed", human_question="AI不可用或处于dry-run；请使用真实搜索证据和确定性门禁。")
        if trace:
            token_prompt += int(getattr(trace, "prompt_tokens", 0) or 0)
            token_completion += int(getattr(trace, "completion_tokens", 0) or 0)
        responses.append({**request, "search_queries": assessment.search_queries, "institution_aliases": assessment.institution_aliases, "entry_type_hint": assessment.entry_type_hint, "pagination_hint": assessment.pagination_hint, "confidence": assessment.confidence, "recommended_action": assessment.recommended_action, "human_question": assessment.human_question, "prompt_tokens": getattr(trace, "prompt_tokens", None), "completion_tokens": getattr(trace, "completion_tokens", None), "estimated_cost_usd": None, "error_type": error_type, "status": "dry_run" if dry_run else ("completed" if trace or request_hash in cache else "awaiting_api_key"), "provider_has_search": False})
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
            candidates = pl.read_parquet(candidates_path)
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
    completed_records = [record for record in audit_records if record.get("status") == "response_completed"]
    _json(run_dir / "ai_cost_summary.json", {"ai_calls": len([record for record in completed_records if not record.get("cache_hit")]), "prompt_tokens": sum(int(record.get("prompt_tokens") or 0) for record in completed_records), "completion_tokens": sum(int(record.get("completion_tokens") or 0) for record in completed_records), "total_tokens": sum(int(record.get("total_tokens") or 0) for record in completed_records), "estimated_cost_usd": sum(float(record.get("estimated_cost_usd") or 0) for record in completed_records), "cost_status": "token_totals_persisted_pricing_unconfigured", "cache_entries": len(cache), "recovered_interrupted": len(recovered_audit)})
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
    report = {"run_dir": str(run_dir), "planned_slots": plan.height, "ai_calls": ai_calls, "persisted_ai_calls": len([record for record in audit_records if record.get("status") == "response_completed" and not record.get("cache_hit")]), "recovered_interrupted": len(recovered_audit), "candidate_proposals": len(proposals), "applied_candidates": applied, "probed_candidates": probed, "strict_verified_added": 0, "strict_enabled_added": 0, "human_review": len(review_rows), "dry_run": dry_run, "apply": apply, "search_provider": getattr(search_provider, "name", type(search_provider).__name__), "ai_provider": "siliconflow" if provider else "unavailable", "concurrency_cap": concurrency}
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
        command.add_argument("--concurrency", type=int, default=4)
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
        plan = build_ai_plan(settings, city=args.city, city_id=args.city_id, source_role=args.source_role, slot_id=args.slot_id, max_slots=args.max_slots)
        plan.write_parquet(run_dir / "ai_slot_plan.parquet", compression="zstd")
        print(json.dumps({"run_dir": str(run_dir), "planned_slots": plan.height}, ensure_ascii=False))
    else:
        print(json.dumps(run_ai_batch(settings, output=args.output, max_slots=args.max_slots, max_ai_calls=args.max_ai_calls, concurrency=args.concurrency, dry_run=args.dry_run or not args.apply, apply=args.apply, resume=args.resume, city=args.city, city_id=args.city_id, source_role=args.source_role, slot_id=args.slot_id), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
