from __future__ import annotations

import json
import os
import time
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
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
from policydb.settings import Settings
from policydb.transform.normalization import stable_id

QUERY_ACTIONS = ("通知", "政策", "实施细则", "调整", "优化", "取消", "提高", "降低")


class CrawlService:
    """One business service shared by the web worker and CLI."""

    def __init__(
        self,
        settings: Settings | None = None,
        pipeline: CrawlPipeline | None = None,
        workspace: Path | None = None,
    ) -> None:
        self.settings = settings or Settings.discover()
        self.workspace = workspace
        self.work_settings = (
            self.settings.model_copy(update={"curated_path": workspace})
            if workspace is not None
            else self.settings
        )
        self.pipeline = pipeline or CrawlPipeline(self.work_settings)

    def _notify(self, callback, stage: str, current: int, total: int, message: str, counters: dict | None = None) -> None:
        if callback:
            callback(stage, current, total, message, counters or {})

    def estimate(self, request: CrawlJobRequest) -> dict:
        enabled = sum(source.crawl_enabled for source in load_registry(self.settings))
        return request.estimate(enabled)

    def execute(self, request: CrawlJobRequest, *, progress=None, cancel_check=None) -> dict:
        if request.demo_mode:
            return self._demo_result(request, progress, cancel_check)
        today = date.today()
        start = request.start_date or today - timedelta(days=3)
        end = request.end_date or today
        if request.confirmed_recommended_source_ids:
            set_sources_enabled(request.confirmed_recommended_source_ids, True, self.settings)
        if request.mode == "source_health":
            self._notify(progress, "discovering", 1, 3, "正在检测来源入口和解析能力")
            health = evaluate_sources(self.work_settings, limit=request.max_fetches)
            return {
                "metrics": {"source_count": health["evaluated"], "candidate_count": 0, "fetched": 0, "failed": health["unhealthy"], "document_versions": 0},
                "source_health": self._parquet_rows("source_health"),
                "recommendations": [f"发现 {health['recommended']} 个高置信推荐来源；启用前请人工确认名单。"],
            }
        if request.mode == "web_discovery":
            return self._web_discovery(request, progress)
        if request.mode == "recover_missing":
            from policydb.recovery import recover_review_sources

            self._notify(progress, "discovering", 1, 3, "正在恢复缺失或失效来源")
            recovered = recover_review_sources(
                settings=self.work_settings, limit=request.max_fetches
            )
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
            materialize_seed_record_links(self.work_settings)
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
            materialize_seed_record_links(self.work_settings)
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
        fetched = self.pipeline.run(
            plan["run_id"], cancel_check=cancel_check, progress=progress
        )
        if fetched.get("cancelled"):
            raise InterruptedError("任务已按用户请求安全停止")
        self._notify(progress, "deduplicating", 4, 8, "正在识别内容哈希和网页版本")
        glm_result = verify_result = {}
        if request.run_glm and self.settings.glm_api_key:
            self._notify(progress, "enriching", 5, 8, "正在处理本次新增或变化文档")
            glm_result = GLMEnricher(self.work_settings).enrich_pending(
                run_id=plan["run_id"]
            )
            if request.run_verification:
                self._notify(progress, "verifying", 6, 8, "正在执行独立证据复核")
                verify_result = GLMEnricher(self.work_settings).verify_pending(
                    run_id=plan["run_id"]
                )
        version_count, versions_preview = self._table_summary(
            "policy_document_versions"
        )
        error_count, errors_preview = self._table_summary(
            "fetch_errors", pl.col("run_id") == plan["run_id"]
        )
        recommendations = []
        if error_count:
            recommendations.append(f"{error_count} 个页面抓取失败，请在错误表中按类型处理。")
        if request.run_glm and not self.settings.glm_api_key:
            recommendations.append("尚未配置 GLM；本次已完成抓取与解析，未调用付费模型。")
        return {
            "run_id": plan["run_id"],
            "warning": bool(error_count),
            "metrics": {
                "source_count": plan["source_count"],
                "candidate_count": plan["item_count"],
                "fetched": fetched["fetched"],
                "failed": fetched["failed"],
                "document_versions": version_count,
                "glm_completed": int(glm_result.get("completed", 0)),
                "glm_failed": int(glm_result.get("failed", 0)),
                "auto_verified": int(verify_result.get("completed", 0)),
                "manual_review": int(verify_result.get("failed", 0)),
            },
            "table_paths": self._result_paths(),
            "previews": {
                "discovered_candidates": [
                    row
                    for row in self._parquet_rows("crawl_items")
                    if row.get("run_id") == plan["run_id"]
                ][:20],
                "fetched_documents": versions_preview,
                "errors": errors_preview,
            },
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
            append_unique(
                self.work_settings.curated / "discovery_candidates.parquet",
                rows,
                "candidate_id",
            )
        return {
            "run_id": run_id,
            "metrics": {"source_count": 0, "candidate_count": len(rows), "fetched": 0, "failed": 0, "document_versions": 0, "media_leads": sum(row["source_role"] == "discovery_lead" for row in rows)},
            "table_paths": self._result_paths(),
            "previews": {"discovered_candidates": rows[:20]},
            "recommendations": ["媒体结果仅保存为线索；须反查官方原文后才能成为 canonical source。"],
        }

    def _parquet_rows(self, name: str) -> list[dict]:
        path = self.work_settings.curated / f"{name}.parquet"
        return pl.read_parquet(path).to_dicts() if path.exists() else []

    def _table_summary(self, name: str, predicate=None) -> tuple[int, list[dict]]:
        path = self.work_settings.curated / f"{name}.parquet"
        if not path.exists():
            return 0, []
        query = pl.scan_parquet(path)
        if predicate is not None:
            query = query.filter(predicate)
        count = query.select(pl.len()).collect().item()
        preview = query.head(20).collect().to_dicts()
        return int(count), preview

    def _result_paths(self) -> dict[str, str]:
        names = ("crawl_items", "policy_document_versions", "fetch_errors", "attachments")
        return {
            name: str(self.work_settings.curated / f"{name}.parquet")
            for name in names
            if (self.work_settings.curated / f"{name}.parquet").exists()
        }

    def _demo_result(self, request, progress=None, cancel_check=None) -> dict:
        candidate_count = (
            request.max_fetches if request.max_candidates == 200 else request.max_candidates
        )
        fetched_count = min(request.max_fetches, candidate_count)
        now = datetime.now(UTC).isoformat()
        run_id = f"MOCK_LOCAL_{candidate_count}_{fetched_count}"
        candidates = []
        versions = []
        for index in range(1, candidate_count + 1):
            if cancel_check and cancel_check():
                raise InterruptedError("任务已按用户请求安全停止")
            url = f"https://fixture.local/policy/{index}"
            candidates.append(
                {
                    "item_id": f"MOCK_ITEM_{index}",
                    "run_id": run_id,
                    "source_id": "fixture_local",
                    "url": url,
                    "canonical_url": url,
                    "status": "fetched" if index <= fetched_count else "pending",
                    "first_seen_at": now,
                    "last_seen_at": now,
                    "created_at": now,
                    "updated_at": now,
                }
            )
            if index <= fetched_count:
                versions.append(
                    {
                        "document_version_id": f"MOCK_VERSION_{index}",
                        "crawl_item_id": f"MOCK_ITEM_{index}",
                        "source_id": "fixture_local",
                        "canonical_url": url,
                        "final_url": url,
                        "content_sha256": f"mock-{index:08d}",
                        "local_path": f"data/work/mock/{index}.html",
                        "content_type": "text/html",
                        "http_status": 200,
                        "title": f"本地模拟政策 {index}",
                        "extracted_text": "本地夹具正文",
                        "parse_status": "parsed",
                        "is_material_change": False,
                        "first_seen_at": now,
                        "last_seen_at": now,
                        "created_at": now,
                        "updated_at": now,
                    }
                )
                if progress:
                    progress(
                        "fetching",
                        index,
                        max(fetched_count, 1),
                        f"本地夹具抓取 {index}/{fetched_count}",
                        {
                            "discovered": candidate_count,
                            "fetched": index,
                            "processed": index,
                            "queued": fetched_count - index,
                            "_current_url": url,
                            "_source_id": "fixture_local",
                        },
                    )
                time.sleep(0.015)
        if candidates:
            append_unique(
                self.work_settings.curated / "crawl_items.parquet",
                candidates,
                "item_id",
            )
        if versions:
            append_unique(
                self.work_settings.curated / "policy_document_versions.parquet",
                versions,
                "document_version_id",
            )
        glm_completed = 0
        auto_verified = 0
        if request.processing_mode in {"glm", "glm_verify"}:
            glm_completed = fetched_count
            if progress:
                progress(
                    "enriching",
                    fetched_count,
                    max(fetched_count, 1),
                    "本地夹具 GLM 抽取完成",
                    {"glm_completed": glm_completed},
                )
        if request.processing_mode == "glm_verify":
            auto_verified = fetched_count
            if progress:
                progress(
                    "verifying",
                    fetched_count,
                    max(fetched_count, 1),
                    "本地夹具独立复核完成",
                    {"auto_verified": auto_verified},
                )
        return {
            "run_id": run_id,
            "warning": True,
            "metrics": {
                "source_count": 1,
                "candidate_count": candidate_count,
                "fetched": fetched_count,
                "failed": 0,
                "document_versions": fetched_count,
                "glm_completed": glm_completed,
                "auto_verified": auto_verified,
                "manual_review": 0,
            },
            "table_paths": self._result_paths(),
            "previews": {
                "discovered_candidates": candidates[:20],
                "fetched_documents": versions[:20],
            },
            "recommendations": ["本次使用本地夹具，未访问真实网站。"],
        }


CURATED_MERGE_KEYS = {
    "crawl_runs": "run_id",
    "crawl_items": "item_id",
    "crawl_checkpoints": "checkpoint_id",
    "fetch_errors": "error_id",
    "policy_document_versions": "document_version_id",
    "llm_extractions": "extraction_id",
    "llm_verifications": "verification_id",
    "discovery_candidates": "candidate_id",
    "attachments": "attachment_id",
}


def commit_crawl_workspace(settings: Settings, workspace: Path, job_id: str) -> dict:
    """Validate all deltas first, then atomically replace individual stable tables."""
    prepared: list[tuple[Path, Path, int]] = []
    for name, key in CURATED_MERGE_KEYS.items():
        delta_path = workspace / f"{name}.parquet"
        if not delta_path.exists():
            continue
        delta = pl.read_parquet(delta_path)
        if delta.is_empty():
            continue
        target = settings.curated / f"{name}.parquet"
        current = pl.read_parquet(target) if target.exists() else pl.DataFrame()
        merged = pl.concat([current, delta], how="diagonal_relaxed").unique(
            subset=[key], keep="last", maintain_order=True
        )
        if merged.select(pl.col(key).n_unique()).item() != merged.height:
            raise ValueError(f"{name} 主键校验失败")
        temp = target.with_suffix(f".{job_id}.tmp.parquet")
        target.parent.mkdir(parents=True, exist_ok=True)
        merged.write_parquet(temp, compression="zstd")
        pl.read_parquet(temp, n_rows=1)
        prepared.append((temp, target, delta.height))
    manifest = {
        "job_id": job_id,
        "status": "prepared",
        "tables": {target.stem: count for _, target, count in prepared},
        "created_at": datetime.now(UTC).isoformat(),
    }
    manifest_path = workspace / "merge_manifest.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    for temp, target, _ in prepared:
        os.replace(temp, target)
    manifest["status"] = "committed"
    manifest["committed_at"] = datetime.now(UTC).isoformat()
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return manifest
