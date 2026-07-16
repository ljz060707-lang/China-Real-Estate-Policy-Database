from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from urllib.parse import urlsplit

import polars as pl

from policydb.config.providers import build_search_provider
from policydb.crawl.checkpoint import append_unique
from policydb.crawl.dedup import canonicalize_url
from policydb.crawl.health import evaluate_sources
from policydb.crawl.models import DiscoveryCandidate
from policydb.crawl.pipeline import CrawlPipeline
from policydb.crawl.registry import (
    load_registry,
    materialize_seed_record_links,
    set_sources_enabled,
)
from policydb.enrich.glm import GLMEnricher
from policydb.jobs.models import CrawlJobRequest
from policydb.query.database import build_database
from policydb.recovery import recover_review_sources
from policydb.settings import Settings
from policydb.transform.normalization import stable_id
from policydb.validate.quality import validate

QUERY_ACTIONS = ("通知", "政策", "实施细则", "调整", "优化", "取消", "提高", "降低")


class CrawlService:
    """One business service shared by the web worker and CLI."""

    def __init__(self, settings: Settings | None = None, pipeline: CrawlPipeline | None = None) -> None:
        self.settings = settings or Settings.discover()
        self.pipeline = pipeline or CrawlPipeline(self.settings)

    def _notify(self, callback, stage: str, current: int, total: int, message: str, counters: dict | None = None) -> None:
        if callback:
            callback(stage, current, total, message, counters or {})

    def estimate(self, request: CrawlJobRequest) -> dict:
        cities = len(request.cities) or (105 if request.mode == "historical_105" else 1)
        topics = len(request.topics) or 1
        sources = load_registry(self.settings)
        enabled = [source for source in sources if source.crawl_enabled]
        queries = cities * topics * len(QUERY_ACTIONS) if request.mode in {"web_discovery", "historical_105", "smart"} else 0
        return {
            "city_count": cities,
            "topic_count": topics,
            "source_count": len(enabled),
            "query_count": min(queries, request.max_candidates),
            "max_pages": request.max_fetches,
            "possible_api_calls": min(queries, request.max_candidates),
        }

    def execute(self, request: CrawlJobRequest, *, progress=None, cancel_check=None) -> dict:
        if request.demo_mode:
            return self._demo_result(progress)
        today = date.today()
        start = request.start_date or today - timedelta(days=3)
        end = request.end_date or today
        if request.confirmed_recommended_source_ids:
            set_sources_enabled(request.confirmed_recommended_source_ids, True, self.settings)
        if request.mode == "source_health":
            self._notify(progress, "discovering", 1, 3, "正在检测来源入口和解析能力")
            health = evaluate_sources(self.settings, limit=request.max_fetches)
            return {
                "metrics": {"source_count": health["evaluated"], "candidate_count": 0, "fetched": 0, "failed": health["unhealthy"], "document_versions": 0},
                "source_health": self._parquet_rows("source_health"),
                "recommendations": [f"发现 {health['recommended']} 个高置信推荐来源；启用前请人工确认名单。"],
            }
        if request.mode == "web_discovery":
            return self._web_discovery(request, progress)
        if request.mode == "recover_missing":
            self._notify(progress, "discovering", 1, 3, "正在恢复缺失或失效来源")
            recovered = recover_review_sources(settings=self.settings, limit=request.max_fetches)
            return {
                "metrics": {"source_count": 0, "candidate_count": int(recovered.get("processed", 0)), "fetched": int(recovered.get("recovered", 0)), "failed": int(recovered.get("failed", 0)), "document_versions": int(recovered.get("recovered", 0))},
                "recovered_sources": self._parquet_rows("source_recovery_attempts"),
                "recommendations": [],
            }
        run_type = request.mode
        seed_mode = request.mode == "seed_backtrack"
        official_mode = request.mode in {"official_update", "historical_105"}
        self._notify(progress, "discovering", 1, 8, "正在发现政策详情页")
        if seed_mode:
            materialize_seed_record_links(self.settings)
        plan = self.pipeline.plan(
            run_type=run_type,
            start_date=start,
            end_date=end,
            official_first=request.official_first,
            include_disabled_seed=seed_mode,
            max_items=request.max_fetches,
            official_only_sources=official_mode,
        )
        if plan["status"] == "blocked_no_enabled_sources" and request.mode == "smart":
            materialize_seed_record_links(self.settings)
            plan = self.pipeline.plan(
                run_type="seed_backtrack",
                start_date=start,
                end_date=end,
                include_disabled_seed=True,
                max_items=request.max_fetches,
            )
        if plan["status"] == "blocked_no_enabled_sources":
            return {
                "run_id": plan["run_id"],
                "warning": True,
                "metrics": {"source_count": 0, "candidate_count": 0, "fetched": 0, "failed": 0, "document_versions": 0},
                "recommendations": ["本次没有启用来源，请先运行来源体检。"],
            }
        self._notify(progress, "fetching", 2, 8, f"已发现 {plan['item_count']} 个候选，开始抓取", {"discovered": plan["item_count"]})
        fetched = self.pipeline.run(plan["run_id"], cancel_check=cancel_check)
        if fetched.get("cancelled"):
            raise InterruptedError("任务已按用户请求安全停止")
        self._notify(progress, "deduplicating", 4, 8, "正在识别内容哈希和网页版本")
        glm_result = verify_result = {}
        if request.run_glm and self.settings.glm_api_key:
            self._notify(progress, "enriching", 5, 8, "正在处理本次新增或变化文档")
            glm_result = GLMEnricher(self.settings).enrich_pending(run_id=plan["run_id"])
            if request.run_verification:
                self._notify(progress, "verifying", 6, 8, "正在执行独立证据复核")
                verify_result = GLMEnricher(self.settings).verify_pending(run_id=plan["run_id"])
        if request.rebuild_database:
            self._notify(progress, "rebuilding", 7, 8, "正在重建 DuckDB 查询层")
            build_database(self.settings)
        validation = None
        if request.run_validation:
            self._notify(progress, "validating", 8, 8, "正在执行数据验证")
            validation = validate(self.settings)
        versions = self._run_versions(plan["run_id"])
        errors = [row for row in self._parquet_rows("fetch_errors") if row.get("run_id") == plan["run_id"]]
        recommendations = []
        if errors:
            recommendations.append(f"{len(errors)} 个页面抓取失败，请在错误表中按类型处理。")
        if request.run_glm and not self.settings.glm_api_key:
            recommendations.append("尚未配置 GLM；本次已完成抓取与解析，未调用付费模型。")
        return {
            "run_id": plan["run_id"],
            "warning": bool(errors) or bool(validation and not validation.get("passed")),
            "metrics": {
                "source_count": plan["source_count"],
                "candidate_count": plan["item_count"],
                "fetched": fetched["fetched"],
                "failed": fetched["failed"],
                "document_versions": len(versions),
                "glm_completed": int(glm_result.get("completed", 0)),
                "glm_failed": int(glm_result.get("failed", 0)),
                "auto_verified": int(verify_result.get("completed", 0)),
                "manual_review": int(verify_result.get("failed", 0)),
            },
            "discovered_candidates": [row for row in self._parquet_rows("crawl_items") if row.get("run_id") == plan["run_id"]],
            "fetched_documents": versions,
            "errors": errors,
            "recommendations": recommendations,
        }

    def _web_discovery(self, request: CrawlJobRequest, progress=None) -> dict:
        provider = build_search_provider(self.settings.search_provider, self.settings.search_api_key, base_url=self.settings.search_base_url)
        if provider.name == "None":
            return {
                "warning": True,
                "metrics": {"source_count": 0, "candidate_count": 0, "fetched": 0, "failed": 0, "document_versions": 0},
                "recommendations": ["全网发现需要配置搜索服务 API；官方来源增量抓取和中金链接回溯仍可运行。"],
            }
        run_id = stable_id("web_discovery", date.today().isoformat(), prefix="CRAWLRUN")
        queries = [f"{city} {topic} {action} site:gov.cn" for city in (request.cities or [""]) for topic in (request.topics or ["房地产"]) for action in QUERY_ACTIONS]
        queries = queries[: request.max_candidates]
        rows = []
        for index, query in enumerate(queries, 1):
            self._notify(progress, "discovering", index, len(queries), f"正在执行政策线索查询 {index}/{len(queries)}")
            for item in provider.search(query, start_date=request.start_date, end_date=request.end_date, max_results=min(10, request.max_candidates - len(rows))):
                canonical = canonicalize_url(item.url)
                official = urlsplit(canonical).netloc.endswith(".gov.cn") or urlsplit(canonical).netloc == "gov.cn"
                candidate = DiscoveryCandidate(
                    candidate_id=stable_id(run_id, canonical, prefix="CAND"), run_id=run_id, discovery_mode="web_discovery", url=item.url, canonical_url=canonical, title_hint=item.title, city_hint=None, source_role="canonical_candidate" if official else "discovery_lead", discovered_at=datetime.now(UTC), discovery_score=0.75 if official else 0.4
                )
                rows.append(candidate.model_dump(mode="json"))
                if len(rows) >= request.max_candidates:
                    break
            if len(rows) >= request.max_candidates:
                break
        if rows:
            append_unique(self.settings.curated / "discovery_candidates.parquet", rows, "candidate_id")
        return {
            "run_id": run_id,
            "metrics": {"source_count": 0, "candidate_count": len(rows), "fetched": 0, "failed": 0, "document_versions": 0, "media_leads": sum(row["source_role"] == "discovery_lead" for row in rows)},
            "discovered_candidates": rows,
            "recommendations": ["媒体结果仅保存为线索；须反查官方原文后才能成为 canonical source。"],
        }

    def _parquet_rows(self, name: str) -> list[dict]:
        path = self.settings.curated / f"{name}.parquet"
        return pl.read_parquet(path).to_dicts() if path.exists() else []

    def _run_versions(self, run_id: str) -> list[dict]:
        versions = self._parquet_rows("policy_document_versions")
        item_ids = {row["item_id"] for row in self._parquet_rows("crawl_items") if row.get("run_id") == run_id}
        return [row for row in versions if row.get("crawl_item_id") in item_ids]

    @staticmethod
    def _demo_result(progress=None) -> dict:
        if progress:
            for index, stage in enumerate(("preparing", "discovering", "fetching", "parsing", "deduplicating", "reporting"), 1):
                progress(stage, index, 6, f"本地演示：{stage}", {"discovered": 5, "fetched": min(index, 3)})
        return {
            "run_id": "MOCK_LOCAL_5_URLS",
            "warning": True,
            "metrics": {"source_count": 3, "candidate_count": 5, "fetched": 3, "failed": 1, "document_versions": 3, "duplicate_count": 1, "candidate_conflict": 1, "auto_verified": 2, "manual_review": 1},
            "discovered_candidates": [{"url": f"https://fixture.local/policy/{index}", "status": "pending"} for index in range(1, 6)],
            "fetched_documents": [{"url": f"https://fixture.local/policy/{index}", "status": "fetched"} for index in range(1, 4)],
            "errors": [{"url": "https://fixture.local/policy/4", "error_type": "Http404"}],
            "recovered_sources": [{"original_url": "https://fixture.local/old", "status": "candidate_conflict"}],
            "recommendations": ["1 条候选来源存在冲突，已保留为人工兜底，未自动采用。"],
        }
