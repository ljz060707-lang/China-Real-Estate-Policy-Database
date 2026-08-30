from __future__ import annotations

import json
import time
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from urllib.parse import urlsplit

import polars as pl

from policydb.archive import archive_document_versions
from policydb.config.providers import build_search_fallback, build_search_provider
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
from policydb.parquet_store import atomic_write_parquet, read_parquet_snapshot
from policydb.settings import Settings
from policydb.transform.normalization import stable_id

QUERY_ACTIONS = ("通知", "政策", "实施细则", "调整", "优化", "取消", "提高", "降低")


def selected_batch_fetch_limit(request: CrawlJobRequest, planned_item_count: int) -> int:
    """Resolve the fetch cap for a selected rehearsal without bypassing safety caps."""

    planned = max(0, int(planned_item_count))
    safety = min(
        int(request.global_safety_limit),
        int(request.max_candidates_total or request.max_candidates),
    )
    if request.drain_selected_batch:
        return min(planned, safety)
    return min(int(request.max_fetches), safety)


def _coerce_date(value: object) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value or "")[:10]
    try:
        return date.fromisoformat(text) if text else None
    except ValueError:
        return None


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
        if request.mode == "historical_episode_930":
            return self._historical_episode_930(
                request,
                progress=progress,
                cancel_check=cancel_check,
            )
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
            official_only_sources=official_mode,
            cities=request.cities,
            provinces=request.provinces,
            topics=request.topics,
            source_ids=request.source_ids,
            source_roles=request.source_roles,
            max_candidates_total=request.max_candidates_total
            or request.max_candidates,
            max_candidates_per_source=request.max_candidates_per_source,
            max_pages_per_source=request.max_pages_per_source,
            batch_size=request.batch_size,
            global_safety_limit=request.global_safety_limit,
            resume=request.resume,
        )
        if plan["status"] == "blocked_no_enabled_sources" and request.mode == "smart":
            materialize_seed_record_links(self.work_settings)
            plan = self.pipeline.plan(
                run_type="seed_backtrack",
                start_date=start,
                end_date=end,
                include_disabled_seed=True,
                cities=request.cities,
                provinces=request.provinces,
                topics=request.topics,
                source_ids=request.source_ids,
                source_roles=request.source_roles,
                max_candidates_total=request.max_candidates_total
                or request.max_candidates,
                max_candidates_per_source=request.max_candidates_per_source,
                max_pages_per_source=request.max_pages_per_source,
                batch_size=request.batch_size,
                global_safety_limit=request.global_safety_limit,
                resume=request.resume,
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
            plan["run_id"],
            max_fetches=request.max_fetches,
            max_attachment_attempts=request.max_attachment_attempts,
            cancel_check=cancel_check,
            progress=progress,
            fetch_concurrency=request.fetch_concurrency or self.settings.max_concurrency,
            per_host_concurrency=request.per_host_concurrency,
        )
        if fetched.get("cancelled"):
            raise InterruptedError("任务已按用户请求安全停止")
        self._notify(progress, "deduplicating", 4, 8, "正在识别内容哈希和网页版本")
        archive_result = {}
        glm_result = verify_result = {}
        if request.run_glm and self.settings.glm_api_key:
            # GLMEnricher intentionally requires an immutable archive/hash
            # record.  Complete that gate for this run before asking it to
            # classify; otherwise a normal crawl reports zero AI work and
            # leaves the just-fetched versions stranded until a later manual
            # archive command.
            self.work_settings.archive_root.mkdir(parents=True, exist_ok=True)
            archive_result = archive_document_versions(
                self.work_settings,
                archive_root=self.work_settings.archive_root,
                run_id=plan["run_id"],
            )
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
            "archive": archive_result,
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

    def _historical_episode_930(self, request: CrawlJobRequest, *, progress=None, cancel_check=None) -> dict:
        """Run queue-scoped 930 search and fetch through the normal crawler.

        The old episode path silently substituted ``historical_105`` and never
        passed the queue query to a search provider.  This adapter keeps the
        existing single writer and ``CrawlPipeline`` archive/version path, but
        makes every search result and HTTP response explicitly linkable to a
        930 queue item.
        """

        queue_path = Path(request.episode_queue_path or (
            self.settings.outputs / "special_projects" / "2016_930" / "930_TASK_QUEUE.parquet"
        ))
        output = Path(request.episode_output_path or queue_path.parent)
        output.mkdir(parents=True, exist_ok=True)
        if not queue_path.exists():
            return {
                "warning": True,
                "metrics": {
                    "search_calls": 0,
                    "search_results": 0,
                    "http_requests": 0,
                    "real_network_fetches": 0,
                    "fetched": 0,
                    "failed": 0,
                    "document_versions": 0,
                },
                "recommendations": ["930 queue is missing; existing task was not rebuilt"],
            }

        queue = read_parquet_snapshot(queue_path)
        requested_ids = {str(value) for value in request.episode_queue_item_ids if value}
        if requested_ids:
            selected = queue.filter(pl.col("queue_item_id").cast(pl.String).is_in(sorted(requested_ids)))
        else:
            selected = queue.filter(pl.col("status").is_in(["RUNNING", "RETRY_WAIT", "PENDING"])).head(
                request.episode_city_limit
            )
        selected_rows = selected.to_dicts()
        if not selected_rows:
            return {
                "warning": False,
                "metrics": {
                    "search_calls": 0,
                    "search_results": 0,
                    "http_requests": 0,
                    "real_network_fetches": 0,
                    "fetched": 0,
                    "failed": 0,
                    "document_versions": 0,
                },
                "recommendations": ["no claimed 930 queue items"],
            }

        registry = load_registry(self.settings)

        def _domain(value: object) -> str:
            text = str(value or "").strip().lower()
            if "://" not in text:
                text = "https://" + text
            return urlsplit(text).netloc.split(":", 1)[0].removeprefix("www.")

        def _source_for_url(url: str, city_id: str | None):
            host = _domain(url)
            candidates = []
            for source in registry:
                source_host = _domain(source.domain)
                if not source_host or not (host == source_host or host.endswith("." + source_host)):
                    continue
                if city_id and source.city_ids and city_id not in {str(value) for value in source.city_ids}:
                    continue
                candidates.append(source)
            candidates.sort(key=lambda source: (not source.crawl_enabled, source.priority, source.source_id))
            return candidates[0] if candidates else None

        provider = build_search_fallback(self.settings)
        if getattr(provider, "name", "None") == "None":
            # The project already ships a keyless DDG HTML provider.  Use it
            # as the bounded episode fallback so a missing paid-search
            # preference cannot silently turn queue execution into a no-op.
            provider = build_search_provider("DuckDuckGoHTML", None)
        search_path = output / "930_QUEUE_SEARCH_EXECUTION.parquet"
        http_path = output / "930_QUEUE_HTTP_AUDIT.parquet"
        search_rows: list[dict] = []
        candidates: list[dict] = []
        search_calls = 0
        now = datetime.now(UTC)
        for index, row in enumerate(selected_rows, 1):
            if cancel_check and cancel_check():
                raise InterruptedError("930 task cancellation requested")
            queue_item_id = str(row.get("queue_item_id") or "")
            query = str(row.get("query_text") or "").strip()
            started = datetime.now(UTC)
            search_calls += 1
            results = []
            error_type = None
            error_message = None
            try:
                if progress:
                    progress("discovering", index, len(selected_rows), f"930 search {index}/{len(selected_rows)}", {"queue_item_id": queue_item_id})
                results = provider.search(
                    query,
                    start_date=_coerce_date(row.get("window_start")),
                    end_date=_coerce_date(row.get("window_end")),
                    max_results=min(5, request.max_candidates_per_source),
                )
            except Exception as exc:
                error_type = type(exc).__name__
                error_message = str(exc)[:500]
            finished = datetime.now(UTC)
            attempts_json = json.dumps(getattr(provider, "last_attempts", []), ensure_ascii=False)
            if not results:
                search_rows.append(
                    {
                        "search_execution_id": stable_id(request.episode_id, queue_item_id, started.isoformat(), prefix="930SEARCH"),
                        "episode_id": request.episode_id or "EP_2016_930_TIGHTENING",
                        "queue_item_id": queue_item_id,
                        "city_id": row.get("city_id"),
                        "query_type": row.get("query_type"),
                        "query_text": query,
                        "provider": getattr(provider, "name", "unknown"),
                        "provider_attempts_json": attempts_json,
                        "status": "FAILED" if error_type else "NO_RESULTS",
                        "result_index": 0,
                        "result_url": None,
                        "canonical_url": None,
                        "title": None,
                        "snippet": None,
                        "search_started_at": started.isoformat(),
                        "search_finished_at": finished.isoformat(),
                        "error_type": error_type,
                        "error_message": error_message,
                    }
                )
            for result_index, result in enumerate(results, 1):
                url = str(getattr(result, "url", "") or "").strip()
                if not url.startswith(("http://", "https://")):
                    continue
                canonical = canonicalize_url(url)
                source = _source_for_url(url, str(row.get("city_id") or "") or None)
                source_id = source.source_id if source else f"930_DISCOVERY_{_domain(url)}"
                search_rows.append(
                    {
                        "search_execution_id": stable_id(request.episode_id, queue_item_id, canonical, prefix="930SEARCH"),
                        "episode_id": request.episode_id or "EP_2016_930_TIGHTENING",
                        "queue_item_id": queue_item_id,
                        "city_id": row.get("city_id"),
                        "query_type": row.get("query_type"),
                        "query_text": query,
                        "provider": getattr(provider, "name", "unknown"),
                        "provider_attempts_json": attempts_json,
                        "status": "RESULT",
                        "result_index": result_index,
                        "result_url": url,
                        "canonical_url": canonical,
                        "title": str(getattr(result, "title", "") or ""),
                        "snippet": str(getattr(result, "snippet", "") or ""),
                        "search_started_at": started.isoformat(),
                        "search_finished_at": finished.isoformat(),
                        "error_type": None,
                        "error_message": None,
                    }
                )
                candidates.append(
                    {
                        "queue_item_id": queue_item_id,
                        "episode_id": request.episode_id or "EP_2016_930_TIGHTENING",
                        "city_id": row.get("city_id"),
                        "query_type": row.get("query_type"),
                        "url": url,
                        "canonical_url": canonical,
                        "source_id": source_id,
                        "published_at": getattr(result, "published_at", None),
                    }
                )

        if search_rows:
            append_unique(search_path, search_rows, "search_execution_id")
        # Keep one URL per queue item in the formal crawl input.  Search
        # evidence remains complete in the execution table above.
        unique_candidates = {}
        for candidate in candidates:
            unique_candidates.setdefault(
                (candidate["queue_item_id"], candidate["canonical_url"]), candidate
            )
        candidates = list(unique_candidates.values())
        if request.max_candidates_total is not None:
            candidates = candidates[: min(request.max_candidates_total, request.global_safety_limit)]
        crawl_run_id = stable_id(
            request.episode_id or "EP_2016_930_TIGHTENING",
            request.episode_run_id or "",
            now.isoformat(),
            prefix="CRAWLRUN",
        )
        planned = self.pipeline.plan_external_candidates(
            crawl_run_id,
            candidates,
            start_date=_coerce_date(request.start_date) or date(2016, 9, 1),
            end_date=_coerce_date(request.end_date) or date(2016, 10, 31),
        )
        audit_context = {
            str(link["crawl_item_id"]): {
                "episode_id": request.episode_id or "EP_2016_930_TIGHTENING",
                "queue_item_id": link.get("queue_item_id"),
                "city_id": link.get("city_id"),
            }
            for link in planned.get("links", [])
        }
        if planned.get("links"):
            append_unique(output / "930_QUEUE_CRAWL_LINKS.parquet", planned["links"], "link_id")
        fetched = self.pipeline.run(
            crawl_run_id,
            max_fetches=selected_batch_fetch_limit(request, len(planned.get("links", []))),
            max_attachment_attempts=request.max_attachment_attempts,
            cancel_check=cancel_check,
            progress=progress,
            audit_path=http_path,
            audit_context=audit_context,
            fetch_concurrency=request.fetch_concurrency or self.settings.max_concurrency,
            per_host_concurrency=request.per_host_concurrency,
        )
        http = read_parquet_snapshot(http_path) if http_path.exists() else pl.DataFrame()
        if not http.is_empty() and "crawl_run_id" in http.columns:
            http = http.filter(pl.col("crawl_run_id").cast(pl.String) == crawl_run_id)
        real_fetches = int(http.filter(pl.col("real_network_fetch")).height) if http.height and "real_network_fetch" in http.columns else 0
        successful = int(http.filter((pl.col("http_status") == 200) & (pl.col("response_bytes") > 0)).height) if http.height else 0
        document_versions = int(http.filter(pl.col("document_version_id").is_not_null()).height) if http.height and "document_version_id" in http.columns else 0
        metrics = {
            **dict(fetched),
            "search_calls": search_calls,
            "search_results": len(candidates),
            "http_requests": http.height,
            "real_network_fetches": real_fetches,
            "successful_http_200": successful,
            "cache_hits": int(http.filter(pl.col("cache_hit")).height) if http.height and "cache_hit" in http.columns else 0,
            "document_versions": document_versions,
            "queue_http_audit_path": str(http_path),
            "queue_search_audit_path": str(search_path),
            "queue_crawl_links_path": str(output / "930_QUEUE_CRAWL_LINKS.parquet"),
            "queue_item_ids": [str(row.get("queue_item_id")) for row in selected_rows],
        }
        return {
            "run_id": crawl_run_id,
            "metrics": metrics,
            "search": {"provider": getattr(provider, "name", "unknown"), "calls": search_calls, "results": len(candidates)},
            "planned": planned,
            "table_paths": self._result_paths(),
            "recommendations": [] if successful or real_fetches else ["930 search/fetch produced no live HTTP response; queue remains retryable"],
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
        return read_parquet_snapshot(path).to_dicts() if path.exists() else []

    def _table_summary(self, name: str, predicate=None) -> tuple[int, list[dict]]:
        path = self.work_settings.curated / f"{name}.parquet"
        if not path.exists():
            return 0, []
        query = read_parquet_snapshot(path)
        if predicate is not None:
            query = query.filter(predicate)
        count = query.height
        preview = query.head(20).to_dicts()
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
    prepared: list[tuple[pl.DataFrame, Path, int]] = []
    for name, key in CURATED_MERGE_KEYS.items():
        delta_path = workspace / f"{name}.parquet"
        if not delta_path.exists():
            continue
        delta = read_parquet_snapshot(delta_path)
        if delta.is_empty():
            continue
        target = settings.curated / f"{name}.parquet"
        current = read_parquet_snapshot(target) if target.exists() else pl.DataFrame()
        merged = pl.concat([current, delta], how="diagonal_relaxed").unique(
            subset=[key], keep="last", maintain_order=True
        )
        if merged.select(pl.col(key).n_unique()).item() != merged.height:
            raise ValueError(f"{name} 主键校验失败")
        prepared.append((merged, target, delta.height))
    manifest = {
        "job_id": job_id,
        "status": "prepared",
        "tables": {target.stem: count for _, target, count in prepared},
        "created_at": datetime.now(UTC).isoformat(),
    }
    manifest_path = workspace / "merge_manifest.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    for merged, target, _ in prepared:
        atomic_write_parquet(
            merged,
            target,
            {"module": "crawl.service", "job_id": job_id},
            key_columns=(CURATED_MERGE_KEYS[target.stem],),
        )
    manifest["status"] = "committed"
    manifest["committed_at"] = datetime.now(UTC).isoformat()
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return manifest
