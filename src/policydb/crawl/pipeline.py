from __future__ import annotations

import json
import os
from collections import deque
from datetime import UTC, date, datetime
from pathlib import Path

import polars as pl
import yaml

from policydb.budget import HttpBudgetExceeded
from policydb.coverage import record_source_window
from policydb.crawl.checkpoint import append_unique, ensure_crawl_storage
from policydb.crawl.dedup import (
    RULES_VERSION,
    content_sha256,
    normalized_text_hash,
    policy_identity_key,
    simhash64,
)
from policydb.crawl.discovery import (
    ListPageDiscovery,
    discover_search_items,
    discover_seed_items,
)
from policydb.crawl.fetcher import PermissionErrorLocal, RespectfulFetcher
from policydb.crawl.models import DiscoveryRequest
from policydb.crawl.parser import extract_pdf_embedded, parse_document
from policydb.crawl.registry import load_registry
from policydb.parquet_store import atomic_write_parquet, read_parquet_snapshot
from policydb.scope import load_cities_105
from policydb.settings import Settings
from policydb.transform.normalization import stable_id


def _province_matches(name: object, code: object, requested: set[str]) -> bool:
    full_name = str(name or "")
    aliases = {full_name, str(code or "")}
    for suffix in ("省", "市", "自治区", "壮族自治区", "回族自治区", "维吾尔自治区"):
        if full_name.endswith(suffix):
            aliases.add(full_name.removesuffix(suffix))
    return bool(aliases & requested)


class CrawlPipeline:
    def __init__(
        self,
        settings: Settings | None = None,
        fetcher: RespectfulFetcher | None = None,
    ) -> None:
        self.settings = settings or Settings.discover()
        self.fetcher = fetcher or RespectfulFetcher()
        ensure_crawl_storage(self.settings.curated)

    def _path(self, name: str) -> Path:
        return self.settings.curated / f"{name}.parquet"

    def _stored_path(self, path: Path) -> str:
        try:
            return str(path.relative_to(self.settings.root))
        except ValueError:
            return str(path)

    def plan(
        self,
        *,
        run_type: str,
        start_date: date,
        end_date: date,
        official_first: bool = True,
        include_disabled_seed: bool = False,
        max_items: int | None = None,
        official_only_sources: bool = False,
        cities: list[str] | None = None,
        provinces: list[str] | None = None,
        topics: list[str] | None = None,
        source_ids: list[str] | None = None,
        source_roles: list[str] | None = None,
        max_candidates_total: int | None = None,
        max_candidates_per_source: int = 50,
        max_pages_per_source: int = 20,
        batch_size: int = 50,
        global_safety_limit: int = 10000,
        resume: bool = True,
    ) -> dict:
        now = datetime.now(UTC)
        run_id = stable_id(run_type, now.isoformat(), prefix="CRAWLRUN")
        all_sources = load_registry(self.settings)
        sources = (
            [source for source in all_sources if source.seed_urls]
            if include_disabled_seed or run_type == "seed_backtrack"
            else [source for source in all_sources if source.crawl_enabled]
        )
        if official_only_sources:
            sources = [
                source
                for source in sources
                if source.official_status in {"official", "official_reprint"}
            ]
        if source_ids:
            selected_source_ids = set(source_ids)
            sources = [
                source for source in sources if source.source_id in selected_source_ids
            ]
        if source_roles:
            selected_source_roles = set(source_roles)
            sources = [
                source
                for source in sources
                if source.source_role in selected_source_roles
                or source.agency_type in selected_source_roles
            ]
        requested_cities = {
            str(city).strip() for city in (cities or []) if str(city).strip()
        }
        requested_provinces = {
            str(province).strip()
            for province in (provinces or [])
            if str(province).strip()
        }
        city_frame = pl.DataFrame()
        selected_city_ids: set[str] = set()
        if requested_cities or requested_provinces:
            city_frame = load_cities_105(self.settings)
            if requested_cities:
                city_frame = city_frame.filter(
                    pl.struct(
                        "city_id", "city_name", "city_name_short", "aliases"
                    ).map_elements(
                        lambda row: bool(
                            requested_cities
                            & {
                                str(row["city_id"]),
                                str(row["city_name"]),
                                str(row["city_name_short"]),
                                *str(row["aliases"] or "").split("|"),
                            }
                        ),
                        return_dtype=pl.Boolean,
                    )
                )
            if requested_provinces:
                city_frame = city_frame.filter(
                    pl.struct("province_name", "province_code").map_elements(
                        lambda row: _province_matches(
                            row["province_name"],
                            row["province_code"],
                            requested_provinces,
                        ),
                        return_dtype=pl.Boolean,
                    )
                )
            selected_city_ids = set(city_frame["city_id"].to_list())
            selected_province_codes = {
                str(value) for value in city_frame["province_code"].to_list()
            }
            sources = [
                source
                for source in sources
                if source.scope_type == "national"
                or bool(selected_city_ids & set(source.city_ids))
                or bool(selected_province_codes & set(source.province_codes))
            ]
        if official_first:
            sources.sort(key=lambda item: item.priority)
        search_sources = [source for source in sources if source.search_url_template]
        keyword_groups: dict[str, list[str]] = {}
        if search_sources:
            keyword_config = yaml.safe_load(
                (
                    self.settings.root
                    / "data"
                    / "reference"
                    / "crawl_keywords.yaml"
                ).read_text(encoding="utf-8")
            )
            all_keyword_groups = {
                name: value["terms"]
                for name, value in keyword_config.get("groups", {}).items()
            }
            requested_topics = {
                str(topic).strip() for topic in (topics or []) if str(topic).strip()
            }
            if requested_topics:
                keyword_groups = {
                    name: terms
                    for name, terms in all_keyword_groups.items()
                    if name in requested_topics
                    or any(
                        requested in str(term) or str(term) in requested
                        for requested in requested_topics
                        for term in terms
                    )
                }
                mapped_topics = {
                    requested
                    for requested in requested_topics
                    if any(
                        requested == name
                        or any(
                            requested in str(term) or str(term) in requested
                            for term in terms
                        )
                        for name, terms in keyword_groups.items()
                    )
                }
                keyword_groups.update(
                    {
                        f"topic:{topic}": [topic]
                        for topic in sorted(requested_topics - mapped_topics)
                    }
                )
            else:
                keyword_groups = all_keyword_groups
            if not requested_cities and not requested_provinces:
                city_frame = load_cities_105(self.settings)
        years = range(start_date.year, end_date.year + 1)
        source_buckets: list[deque[dict]] = []
        discovery_errors: list[dict] = []
        discovery_scans: list[dict] = []
        previous_scans = (
            read_parquet_snapshot(self._path("crawl_discovery_scans"))
            if resume and self._path("crawl_discovery_scans").exists()
            else pl.DataFrame()
        )
        for source in sources:
            source_items: list[dict] = []
            scoped_ids = selected_city_ids & set(source.city_ids)
            if not scoped_ids and city_frame.height == 1:
                scoped_ids = {str(city_frame[0, "city_id"])}
            scoped_city_id = next(iter(scoped_ids)) if len(scoped_ids) == 1 else None
            seed_source = source.model_copy(
                update={"list_page_urls": []}
            )
            source_items.extend(
                discover_seed_items(
                    seed_source,
                    run_id,
                    city_id=scoped_city_id,
                    start_date=start_date,
                    end_date=end_date,
                )
            )
            if source.list_page_urls and run_type != "seed_backtrack":
                try:
                    discovery = ListPageDiscovery(self.fetcher)
                    resume_scan = None
                    if previous_scans.height and "source_id" in previous_scans.columns:
                        candidates = previous_scans.filter(
                            (pl.col("source_id").cast(pl.String) == source.source_id)
                            & (~pl.col("pagination_exhausted").fill_null(False))
                        )
                        if candidates.height:
                            resume_scan = candidates.sort("created_at").row(-1, named=True)
                    candidates = discovery.discover(
                        DiscoveryRequest(
                            run_id=run_id,
                            mode=run_type,
                            start_date=start_date,
                            end_date=end_date,
                            max_candidates=max_candidates_per_source,
                            max_pages=max_pages_per_source,
                        ),
                        source,
                        resume_from=resume_scan,
                    )
                    now_text = now.isoformat()
                    source_items.extend(
                        {
                            "item_id": stable_id(source.source_id, item.canonical_url, prefix="CRAWLITEM"),
                            "run_id": run_id,
                            "source_id": source.source_id,
                            "url": item.url,
                            "canonical_url": item.canonical_url,
                            "status": "pending",
                            "city_id": scoped_city_id,
                            "query_year": item.date_hint.year if item.date_hint else None,
                            "candidate_date": (
                                item.date_hint.isoformat()
                                if item.date_hint
                                else None
                            ),
                            "candidate_date_source": (
                                "list_item" if item.date_hint else "unknown"
                            ),
                            "candidate_date_confidence": (
                                0.99 if item.date_hint else 0.0
                            ),
                            "period_decision": None,
                            "keyword_group": item.keyword_group,
                            "retry_count": 0,
                            "first_seen_at": now_text,
                            "last_seen_at": now_text,
                            "created_at": now_text,
                            "updated_at": now_text,
                        }
                        for item in candidates
                    )
                    discovery_scans.append(
                        {
                            "scan_id": stable_id(
                                run_id, source.source_id, prefix="DISCOVERYSCAN"
                            ),
                            "run_id": run_id,
                            "source_id": source.source_id,
                            "pages_scanned": discovery.last_scan["pages_scanned"],
                            "pagination_exhausted": discovery.last_scan[
                                "pagination_exhausted"
                            ],
                            "stop_reason": discovery.last_scan["stop_reason"],
                            "candidate_count": discovery.last_scan["candidate_count"],
                            "max_pages": discovery.last_scan["max_pages"],
                            "max_candidates": discovery.last_scan[
                                "max_candidates"
                            ],
                            "last_page": discovery.last_scan.get("last_page"),
                            "next_page": discovery.last_scan.get("next_page"),
                            "last_seen_date": discovery.last_scan.get("last_seen_date"),
                            "cursor": discovery.last_scan.get("cursor"),
                            "visited_urls": json.dumps(discovery.last_scan.get("visited_urls") or [], ensure_ascii=False),
                            "resumed_from": discovery.last_scan.get("resumed_from"),
                            "created_at": now.isoformat(),
                        }
                    )
                except HttpBudgetExceeded:
                    raise
                except Exception as exc:
                    discovery_errors.append(
                        {"source_id": source.source_id, "error_type": type(exc).__name__}
                    )
            source_items.extend(
                discover_search_items(
                    source, run_id, city_frame, years, keyword_groups
                )
            )
            unique_source_items = {
                item["canonical_url"]: item for item in source_items
            }
            source_buckets.append(
                deque(list(unique_source_items.values())[:max_candidates_per_source])
            )
        total_limit = min(
            max_candidates_total or max_items or global_safety_limit,
            global_safety_limit,
        )
        items: list[dict] = []
        active = deque(bucket for bucket in source_buckets if bucket)
        while active and len(items) < total_limit:
            for _ in range(min(max(1, batch_size), len(active))):
                if len(items) >= total_limit:
                    break
                bucket = active.popleft()
                items.append(bucket.popleft())
                if bucket:
                    active.append(bucket)
        prepared: dict[str, dict] = {}
        completed_windows: set[tuple[str, str | None]] = set()
        windows_path = self._path("crawl_source_windows")
        if resume and windows_path.exists():
            windows = read_parquet_snapshot(windows_path).filter(
                (pl.col("period_start").cast(pl.String) == start_date.isoformat())
                & (pl.col("period_end").cast(pl.String) == end_date.isoformat())
                & pl.col("coverage_status").cast(pl.String).str.starts_with("complete_")
            )
            completed_windows = {
                (str(row["source_id"]), row.get("city_id"))
                for row in windows.iter_rows(named=True)
            }
        for item in items:
            if (str(item["source_id"]), item.get("city_id")) in completed_windows:
                continue
            task_key = stable_id(
                item["source_id"], item["canonical_url"], start_date.isoformat(),
                end_date.isoformat(), run_type, prefix="TASK",
            )
            item.update(
                {
                    "item_id": stable_id(run_id, task_key, prefix="CRAWLITEM"),
                    "task_key": task_key,
                    "scan_method": run_type,
                    "requested_url": item["url"],
                    "final_url": None,
                    "etag": None,
                    "last_modified": None,
                    "last_checked_at": None,
                    "next_check_at": None,
                }
            )
            prepared[task_key] = item
        items = list(prepared.values())
        if items and self._path("crawl_items").exists():
            existing_rows = read_parquet_snapshot(self._path("crawl_items")).iter_rows(named=True)
            existing = {row["canonical_url"]: row for row in existing_rows}
            for item in items:
                previous = existing.get(item["canonical_url"])
                if previous:
                    item["first_seen_at"] = previous["first_seen_at"]
                    item["retry_count"] = previous["retry_count"]
                    item["etag"] = previous.get("etag")
                    item["last_modified"] = previous.get("last_modified")
                    item["resume_from_item_id"] = previous.get("item_id")
                    item["resume_previous_run_id"] = previous.get("run_id")
                    if str(previous.get("status") or "") in {"fetched", "unchanged"}:
                        # Preserve a fresh pending row so a resumed/repeated
                        # crawl records a new deterministic dedup decision.
                        # The append-only version store prevents duplicate
                        # inserts; avoiding the request here would erase the
                        # evidence that the source was checked again.
                        item["resume_skipped_http"] = False
                    else:
                        item["resume_skipped_http"] = False
        if items:
            append_unique(self._path("crawl_items"), items, "item_id")
        if discovery_scans:
            append_unique(
                self._path("crawl_discovery_scans"),
                discovery_scans,
                "scan_id",
            )
        runs = [
            {
                "run_id": run_id,
                "run_type": run_type,
                "scope_id": "large-cities-105",
                "period_start": start_date.isoformat(),
                "period_end": end_date.isoformat(),
                "status": "planned" if sources else "blocked_no_enabled_sources",
                "source_count": len(sources),
                "item_count": len(items),
                "fetched_count": 0,
                "failed_count": 0,
                "started_at": now.isoformat(),
                "finished_at": None,
                "created_at": now.isoformat(),
                "updated_at": now.isoformat(),
            }
        ]
        append_unique(self._path("crawl_runs"), runs, "run_id")
        return {
            "run_id": run_id,
            "source_count": len(sources),
            "item_count": len(items),
            "status": "planned" if sources else "blocked_no_enabled_sources",
            "diagnostic": None
            if sources
            else "当前没有已启用来源；请先运行来源体检并审核推荐来源。",
            "discovery_errors": discovery_errors,
        }

    @staticmethod
    def _atomic_write(path: Path, payload: bytes) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        temp = path.with_suffix(path.suffix + ".tmp")
        try:
            temp.write_bytes(payload)
            os.replace(temp, path)
        except PermissionError as exc:
            temp.unlink(missing_ok=True)
            raise PermissionErrorLocal(f"local write denied: {path}") from exc

    @classmethod
    def _atomic_parquet(cls, path: Path, frame: pl.DataFrame) -> None:
        atomic_write_parquet(frame, path, {"module": "crawl.pipeline"})

    def _finalize_run(
        self,
        run_id: str,
        *,
        cancelled: bool = False,
        fallback_fetched: int = 0,
        fallback_failed: int = 0,
        last_item_id: str | None = None,
        processed_count: int = 0,
        budget_paused: bool = False,
    ) -> dict:
        """Recompute run/checkpoint counts from this run's persisted items.

        The finalizer is deliberately idempotent and refuses to turn a blocked
        plan into a completed run.  Local loop counters are retained only for
        the CLI result; persisted counts always come from crawl_items.
        """
        runs_path = self._path("crawl_runs")
        items_path = self._path("crawl_items")
        checkpoints_path = self._path("crawl_checkpoints")
        if not runs_path.exists():
            result = {"run_id": run_id, "fetched": fallback_fetched, "failed": fallback_failed}
            if cancelled:
                result["cancelled"] = True
            return result
        runs = read_parquet_snapshot(runs_path)
        match = runs.filter(pl.col("run_id") == run_id)
        if match.height != 1:
            result = {"run_id": run_id, "fetched": fallback_fetched, "failed": fallback_failed}
            if cancelled:
                result["cancelled"] = True
            return result
        run_row = match.row(0, named=True)
        if str(run_row.get("status") or "") == "blocked_no_enabled_sources":
            return {"run_id": run_id, "status": "blocked_no_enabled_sources", "fetched": 0, "failed": 0}
        items = (
            read_parquet_snapshot(items_path).filter(pl.col("run_id") == run_id)
            if items_path.exists()
            else pl.DataFrame()
        )
        item_count = items.height
        fetched_count = (
            items.filter(pl.col("status").is_in(["fetched", "unchanged"])).height
            if item_count
            else 0
        )
        failed_count = (
            items.filter(pl.col("status") == "failed").height if item_count else 0
        )
        pending_count = (
            items.filter(pl.col("status") == "pending").height if item_count else 0
        )
        if budget_paused:
            status = "paused_budget"
        elif cancelled:
            status = "cancelled"
        elif pending_count:
            status = "partial"
        elif str(run_row.get("status") or "") in {"cancelled", "failed"}:
            status = str(run_row["status"])
        elif failed_count:
            status = "partial"
        else:
            status = "complete"
        now = datetime.now(UTC).isoformat()
        checkpoints = (
            read_parquet_snapshot(checkpoints_path)
            if checkpoints_path.exists()
            else pl.DataFrame()
        )
        checkpoint_id = stable_id(run_id, prefix="CHECKPOINT")
        old_checkpoint = (
            checkpoints.filter(pl.col("checkpoint_id") == checkpoint_id)
            if checkpoints.height
            else pl.DataFrame()
        )
        checkpoint_created = (
            old_checkpoint[0, "created_at"]
            if old_checkpoint.height and "created_at" in old_checkpoint.columns
            else now
        )
        terminal_ids = (
            items.filter(pl.col("status").is_in(["fetched", "unchanged", "failed"]))
            .select("item_id")["item_id"].to_list()
            if item_count
            else []
        )
        checkpoint = {
            "checkpoint_id": checkpoint_id,
            "run_id": run_id,
            "last_item_id": last_item_id or (terminal_ids[-1] if terminal_ids else None),
            "status": status,
            "processed_count": max(processed_count, len(terminal_ids)),
            "created_at": checkpoint_created,
            "updated_at": now,
        }
        append_unique(checkpoints_path, [checkpoint], "checkpoint_id")
        run_update = dict(run_row)
        run_update.update(
            {
                "status": status,
                "item_count": item_count,
                "fetched_count": fetched_count,
                "failed_count": failed_count,
                "finished_at": now if status in {"complete", "cancelled"} else run_row.get("finished_at"),
                "updated_at": now,
            }
        )
        append_unique(runs_path, [run_update], "run_id")
        result = {
            "run_id": run_id,
            "fetched": fallback_fetched,
            "failed": max(fallback_failed, failed_count),
        }
        if budget_paused:
            result["budget_paused"] = True
        if cancelled:
            result["cancelled"] = True
        return result

    def run(
        self,
        run_id: str,
        *,
        max_fetches: int | None = None,
        max_attachment_attempts: int | None = None,
        cancel_check=None,
        progress=None,
    ) -> dict:
        items_path = self._path("crawl_items")
        if not items_path.exists():
            return {"run_id": run_id, "status": "missing_items", "fetched": 0, "failed": 0}
        items = read_parquet_snapshot(items_path)
        pending = items.filter((pl.col("run_id") == run_id) & (pl.col("status") == "pending"))
        if max_fetches is not None:
            pending = pending.head(max_fetches)
        source_index = {source.source_id: source for source in load_registry(self.settings)}
        versions_path = self._path("policy_document_versions")
        existing_versions = (
            read_parquet_snapshot(versions_path)
            if versions_path.exists()
            else None
        )
        existing_version_ids = (
            set(existing_versions["document_version_id"].to_list())
            if existing_versions is not None
            else set()
        )
        versions: list[dict] = []
        dedup_decisions: list[dict] = []
        errors: list[dict] = []
        attachment_records: dict[str, dict] = {}
        fetched = 0
        cancelled = False
        budget_paused = False
        processed_count = 0
        last_item_id: str | None = None
        for item in pending.iter_rows(named=True):
            if cancel_check and cancel_check():
                cancelled = True
                break
            processed_count += 1
            last_item_id = item["item_id"]
            if progress:
                progress(
                    "fetching",
                    processed_count,
                    max(pending.height, 1),
                    f"正在抓取 {processed_count}/{pending.height}",
                    {
                        "processed": processed_count,
                        "queued": max(pending.height - processed_count, 0),
                        "_current_url": item["url"],
                        "_source_id": item["source_id"],
                    },
                )
            now = datetime.now(UTC)
            try:
                source = source_index[item["source_id"]]
                self.fetcher.rate_limit = source.rate_limit
                result = self.fetcher.fetch(
                    item["url"],
                    etag=item.get("etag"),
                    last_modified=item.get("last_modified"),
                )
                if result.not_modified:
                    items = items.with_columns(
                        pl.when(pl.col("item_id") == item["item_id"])
                        .then(pl.lit("unchanged"))
                        .otherwise(pl.col("status"))
                        .alias("status")
                    )
                    items = items.with_columns(
                        pl.when(pl.col("item_id") == item["item_id"])
                        .then(pl.lit(now.isoformat()))
                        .otherwise(pl.col("last_checked_at"))
                        .alias("last_checked_at")
                    )
                    dedup_decisions.append(
                        {
                            "decision_id": stable_id(run_id, item["item_id"], "L2", prefix="DEDUP"),
                            "run_id": run_id, "crawl_item_id": item["item_id"],
                            "document_version_id": None, "candidate_document_version_id": None,
                            "dedup_level": "L2", "decision": "unchanged",
                            "reason": "HTTP 304 conditional request", "score": 1.0,
                            "threshold": 1.0, "rules_version": RULES_VERSION,
                            "evidence_json": json.dumps({"etag": bool(item.get("etag")), "last_modified": bool(item.get("last_modified"))}),
                            "created_at": now.isoformat(),
                        }
                    )
                    continue
                parsed = parse_document(result.body, result.content_type, result.final_url)
                text_hash = normalized_text_hash(parsed["full_text"] or "")
                text_simhash = simhash64(parsed["full_text"] or "")
                identity_key = policy_identity_key(title=parsed["title"])
                extension = ".pdf" if parsed["document_type"] == "pdf" else ".html"
                archive_kind = "pdf" if extension == ".pdf" else "html"
                raw_dir = self.settings.archive_root / archive_kind / result.response_sha256[:2]
                raw_dir.mkdir(parents=True, exist_ok=True)
                raw_path = raw_dir / f"{result.response_sha256}{extension}"
                if not raw_path.exists():
                    self._atomic_write(raw_path, result.body)
                text_path = None
                if parsed["full_text"]:
                    text_bytes = parsed["full_text"].encode("utf-8")
                    text_digest = content_sha256(text_bytes)
                    text_path = (
                        self.settings.archive_root
                        / "text"
                        / text_digest[:2]
                        / f"{text_digest}.txt"
                    )
                    if not text_path.exists():
                        self._atomic_write(text_path, text_bytes)
                metadata_path = (
                    self.settings.archive_root
                    / "metadata"
                    / result.response_sha256[:2]
                    / f"{result.response_sha256}.json"
                )
                if not metadata_path.exists():
                    self._atomic_write(
                        metadata_path,
                        json.dumps(
                            {
                                "requested_url": result.requested_url,
                                "final_url": result.final_url,
                                "status_code": result.status_code,
                                "content_type": result.content_type,
                                "retrieved_at": result.retrieved_at.isoformat(),
                                "response_sha256": result.response_sha256,
                                "text_path": str(text_path) if text_path else None,
                                "protocol": result.protocol,
                                "network_route": result.network_route,
                                "redirect_chain": result.redirect_chain,
                                "resolved_addresses": result.resolved_addresses,
                                "fallback_used": result.fallback_used,
                            },
                            ensure_ascii=False,
                            indent=2,
                        ).encode("utf-8"),
                    )
                version_id = stable_id(
                    item["canonical_url"], result.response_sha256, prefix="DOCVER"
                )
                version_row = {
                        "document_version_id": version_id,
                        "record_id": None,
                        "crawl_item_id": item["item_id"],
                        "source_id": item["source_id"],
                        "canonical_url": item["canonical_url"],
                        "final_url": result.final_url,
                        "content_sha256": result.response_sha256,
                        "local_path": self._stored_path(raw_path),
                        "content_type": result.content_type,
                        "http_status": result.status_code,
                        "title": parsed["title"],
                        "extracted_text": parsed["full_text"],
                        "parse_status": parsed["parse_status"],
                        "is_material_change": any(
                            row["canonical_url"] == item["canonical_url"]
                            and row["content_sha256"] != result.response_sha256
                            for row in (
                                existing_versions.iter_rows(named=True)
                                if existing_versions is not None
                                else []
                            )
                        ),
                        "first_seen_at": now.isoformat(),
                        "last_seen_at": now.isoformat(),
                        "created_at": now.isoformat(),
                        "updated_at": now.isoformat(),
                        "normalized_text_hash": text_hash,
                        "simhash64": text_simhash,
                        "policy_identity_key": identity_key,
                        "parser_version": "2",
                        "network_route": result.network_route,
                        "redirect_chain_json": json.dumps(
                            result.redirect_chain, ensure_ascii=False
                        ),
                        "protocol": result.protocol,
                    }
                if version_id in existing_version_ids and existing_versions is not None:
                    existing_versions = existing_versions.with_columns(
                        pl.when(pl.col("document_version_id") == version_id)
                        .then(pl.lit(now.isoformat()))
                        .otherwise(pl.col("last_seen_at"))
                        .alias("last_seen_at")
                    )
                    item_status = "unchanged"
                    decision = "duplicate_content"
                else:
                    versions.append(version_row)
                    existing_version_ids.add(version_id)
                    item_status = "fetched"
                    decision = "new_document"
                dedup_decisions.append(
                    {
                        "decision_id": stable_id(run_id, item["item_id"], version_id, "L3", prefix="DEDUP"),
                        "run_id": run_id, "crawl_item_id": item["item_id"],
                        "document_version_id": version_id, "candidate_document_version_id": None,
                        "dedup_level": "L3", "decision": decision,
                        "reason": "binary SHA-256 comparison", "score": 1.0,
                        "threshold": 1.0, "rules_version": RULES_VERSION,
                        "evidence_json": json.dumps({"content_sha256": result.response_sha256, "normalized_text_hash": text_hash}),
                        "created_at": now.isoformat(),
                    }
                )
                for attachment in parsed.get("attachments", []):
                    if cancel_check and cancel_check():
                        cancelled = True
                        break
                    attachment_url = attachment.get("url")
                    if not attachment_url:
                        if attachment.get("source") == "pdf_embedded":
                            embedded = dict(extract_pdf_embedded(result.body)).get(
                                attachment.get("label")
                            )
                            if embedded is not None:
                                digest = content_sha256(embedded)
                                suffix = Path(str(attachment.get("label") or "")).suffix or ".bin"
                                attachment_dir = (
                                    self.settings.archive_root / "attachments" / digest[:2]
                                )
                                attachment_dir.mkdir(parents=True, exist_ok=True)
                                embedded_path = attachment_dir / f"{digest}{suffix[:10]}"
                                if not embedded_path.exists():
                                    self._atomic_write(embedded_path, embedded)
                        continue
                    attachment_item_id = stable_id(item["item_id"], attachment_url, prefix="ATTACH")
                    attachment_records[attachment_item_id] = {
                        "attachment_id": attachment_item_id,
                        "run_id": run_id,
                        "parent_item_id": item["item_id"],
                        "url": attachment_url,
                        "local_path": None,
                        "content_sha256": None,
                        "status": "PENDING_ATTACHMENT",
                    }
                    if max_attachment_attempts is not None and sum(
                        row["status"] != "PENDING_ATTACHMENT"
                        for row in attachment_records.values()
                    ) >= max_attachment_attempts:
                        continue
                    try:
                        attachment_result = self.fetcher.fetch(attachment_url)
                        attachment_parsed = parse_document(
                            attachment_result.body,
                            attachment_result.content_type,
                            attachment_result.final_url,
                        )
                        attachment_extension = (
                            ".pdf"
                            if attachment_parsed["document_type"] == "pdf"
                            else Path(attachment_result.final_url).suffix or ".bin"
                        )
                        attachment_dir = (
                            self.settings.archive_root
                            / "attachments"
                            / attachment_result.response_sha256[:2]
                        )
                        attachment_dir.mkdir(parents=True, exist_ok=True)
                        attachment_path = (
                            attachment_dir
                            / f"{attachment_result.response_sha256}{attachment_extension[:10]}"
                        )
                        if not attachment_path.exists():
                            self._atomic_write(attachment_path, attachment_result.body)
                        attachment_records[attachment_item_id].update(
                            {
                                "local_path": self._stored_path(attachment_path),
                                "content_sha256": attachment_result.response_sha256,
                                "status": "FETCHED",
                            }
                        )
                        attachment_version_id = stable_id(
                            attachment_url,
                            attachment_result.response_sha256,
                            prefix="DOCVER",
                        )
                        if attachment_version_id not in existing_version_ids:
                            versions.append(
                                {
                                    "document_version_id": attachment_version_id,
                                    "record_id": None,
                                    "crawl_item_id": attachment_item_id,
                                    "source_id": item["source_id"],
                                    "canonical_url": attachment_url,
                                    "final_url": attachment_result.final_url,
                                    "content_sha256": attachment_result.response_sha256,
                                    "local_path": self._stored_path(attachment_path),
                                    "content_type": attachment_result.content_type,
                                    "http_status": attachment_result.status_code,
                                    "title": attachment.get("label") or attachment_parsed["title"],
                                    "extracted_text": attachment_parsed["full_text"],
                                    "parse_status": attachment_parsed["parse_status"],
                                    "is_material_change": False,
                                    "first_seen_at": now.isoformat(),
                                    "last_seen_at": now.isoformat(),
                                    "created_at": now.isoformat(),
                                    "updated_at": now.isoformat(),
                                    "normalized_text_hash": normalized_text_hash(attachment_parsed["full_text"] or ""),
                                    "simhash64": simhash64(attachment_parsed["full_text"] or ""),
                                    "policy_identity_key": policy_identity_key(title=attachment.get("label") or attachment_parsed["title"]),
                                    "parser_version": "2",
                                }
                            )
                            existing_version_ids.add(attachment_version_id)
                    except HttpBudgetExceeded:
                        raise
                    except Exception as attachment_error:
                        attachment_records[attachment_item_id]["status"] = "FAILED"
                        errors.append(
                            {
                                "error_id": stable_id(
                                    attachment_item_id, now.isoformat(), prefix="FETCHERR"
                                ),
                                "run_id": run_id,
                                "item_id": attachment_item_id,
                                "source_id": item["source_id"],
                                "url": attachment_url,
                                "error_type": type(attachment_error).__name__,
                                "error_message": str(attachment_error)[:1000],
                                "retryable": True,
                                "requested_protocol": str(attachment_url).split(
                                    ":", 1
                                )[0].lower(),
                                "network_route": "direct",
                                "redirect_chain_json": "[]",
                                "created_at": now.isoformat(),
                                "updated_at": now.isoformat(),
                            }
                        )
                items = items.with_columns(
                    pl.when(pl.col("item_id") == item["item_id"])
                    .then(pl.lit(item_status))
                    .otherwise(pl.col("status"))
                    .alias("status")
                )
                items = items.with_columns(
                    pl.when(pl.col("item_id") == item["item_id"])
                    .then(pl.lit(result.final_url)).otherwise(pl.col("final_url")).alias("final_url"),
                    pl.when(pl.col("item_id") == item["item_id"])
                    .then(pl.lit(result.etag)).otherwise(pl.col("etag")).alias("etag"),
                    pl.when(pl.col("item_id") == item["item_id"])
                    .then(pl.lit(result.last_modified)).otherwise(pl.col("last_modified")).alias("last_modified"),
                    pl.when(pl.col("item_id") == item["item_id"])
                    .then(pl.lit(now.isoformat())).otherwise(pl.col("last_checked_at")).alias("last_checked_at"),
                )
                fetched += 1
            except HttpBudgetExceeded:
                budget_paused = True
                break
            except Exception as exc:  # failure is persisted; prior data remains untouched
                errors.append(
                    {
                        "error_id": stable_id(item["item_id"], now.isoformat(), prefix="FETCHERR"),
                        "run_id": run_id,
                        "item_id": item["item_id"],
                        "source_id": item["source_id"],
                        "url": item["url"],
                        "error_type": type(exc).__name__,
                        "error_message": str(exc)[:1000],
                        "retryable": bool(getattr(exc, "retryable", True)),
                        "requested_protocol": getattr(
                            exc,
                            "requested_protocol",
                            str(item["url"]).split(":", 1)[0].lower(),
                        ),
                        "network_route": getattr(exc, "network_route", "direct"),
                        "redirect_chain_json": json.dumps(
                            getattr(exc, "redirect_chain", []),
                            ensure_ascii=False,
                        ),
                        "created_at": now.isoformat(),
                        "updated_at": now.isoformat(),
                    }
                )
                items = items.with_columns(
                    pl.when(pl.col("item_id") == item["item_id"])
                    .then(pl.lit("failed"))
                    .otherwise(pl.col("status"))
                    .alias("status")
                )
        self._atomic_parquet(items_path, items)
        version_rows = existing_versions.to_dicts() if existing_versions is not None else []
        version_rows.extend(versions)
        if version_rows:
            append_unique(
                self._path("policy_document_versions"),
                version_rows,
                "document_version_id",
            )
        if errors:
            append_unique(self._path("fetch_errors"), errors, "error_id")
        if dedup_decisions:
            append_unique(self._path("dedup_decisions"), dedup_decisions, "decision_id")
        if attachment_records:
            append_unique(self._path("attachments"), list(attachment_records.values()), "attachment_id")
        final_result = self._finalize_run(
            run_id,
            cancelled=cancelled,
            fallback_fetched=fetched,
            fallback_failed=len(errors),
            last_item_id=last_item_id,
            processed_count=processed_count,
            budget_paused=budget_paused,
        )
        # Read the finalized run row.  Reading it before _finalize_run leaves
        # status=planned here and incorrectly marks a fully terminal window as
        # transaction_uncommitted.
        run_rows = read_parquet_snapshot(self._path("crawl_runs")).filter(pl.col("run_id") == run_id)
        if run_rows.height:
            run_row = run_rows.row(0, named=True)
            scans_path = self._path("crawl_discovery_scans")
            scans = (
                read_parquet_snapshot(scans_path).filter(pl.col("run_id") == run_id)
                if scans_path.exists()
                else pl.DataFrame()
            )
            termination_map = {
                "pagination_exhausted": "END_OF_PAGINATION",
                "stable_before_start_date": "DATE_BOUNDARY_REACHED",
                "next_page_absent": "END_OF_PAGINATION",
                "explicit_last_page_reached": "OFFICIAL_EXPLICIT_LAST_PAGE",
                "archive_start_reached": "DATE_BOUNDARY_REACHED",
                "configured_start_date_reached": "DATE_BOUNDARY_REACHED",
                "source_declared_end_reached": "OFFICIAL_EXPLICIT_LAST_PAGE",
                "consecutive_duplicate_pages_threshold": "COMPLETE_WITH_GAPS",
                "empty_terminal_page": "EMPTY_TERMINAL_PAGE",
            }
            for source_id in pending["source_id"].unique().to_list():
                source_items = items.filter(
                    (pl.col("run_id") == run_id) & (pl.col("source_id") == source_id)
                )
                source_errors = sum(row["source_id"] == source_id for row in errors)
                source_scans = (
                    scans.filter(pl.col("source_id") == source_id)
                    if scans.height
                    else pl.DataFrame()
                )
                pages_scanned = (
                    int(source_scans["pages_scanned"].sum())
                    if source_scans.height
                    else 0
                )
                pagination_exhausted = bool(
                    source_scans.height
                    and source_scans["pagination_exhausted"].all()
                )
                stop_reasons = (
                    source_scans["stop_reason"].unique().to_list()
                    if source_scans.height
                    else []
                )
                mapped_reasons = [
                    termination_map.get(str(reason))
                    for reason in stop_reasons
                    if termination_map.get(str(reason))
                ]
                termination_reason = mapped_reasons[0] if len(set(mapped_reasons)) == 1 else None
                termination_evidence_ids = [
                    str(value)
                    for value in source_scans.get_column("scan_id").to_list()
                    if value
                ] if source_scans.height and "scan_id" in source_scans.columns else []
                finalized_status = str(run_row.get("status") or "")
                pagination_complete = bool(
                    source_scans.height
                    and source_scans["pagination_exhausted"].fill_null(False).all()
                )
                if termination_reason and source_errors and pagination_complete:
                    termination_reason = "COMPLETE_WITH_GAPS"
                transaction_committed = bool(
                    finalized_status in {"complete", "partial", "complete_with_gaps"}
                    and not final_result.get("budget_paused")
                )
                checkpoint_persisted = False
                checkpoints_path = self._path("crawl_checkpoints")
                if checkpoints_path.exists():
                    checkpoint_rows = read_parquet_snapshot(checkpoints_path)
                    checkpoint_persisted = bool(
                        checkpoint_rows.height
                        and checkpoint_rows.filter(pl.col("run_id") == run_id).height
                    )
                completion_invariants_passed = bool(
                    pagination_complete
                    and termination_reason
                    and termination_evidence_ids
                    and transaction_committed
                    and checkpoint_persisted
                    and not source_items.filter(pl.col("status") == "pending").height
                )
                record_source_window(
                    run_id=run_id,
                    source_id=source_id,
                    period_start=date.fromisoformat(run_row["period_start"]),
                    period_end=date.fromisoformat(run_row["period_end"]),
                    scan_method=str(run_row["run_type"]),
                    candidate_count=source_items.height,
                    fetched_count=source_items.filter(pl.col("status").is_in(["fetched", "unchanged"])).height,
                    policy_count=source_items.filter(
                        pl.col("status").is_in(["fetched", "unchanged"])
                    ).height,
                    error_count=source_errors,
                    page_count=pages_scanned,
                    completion_evidence={
                        "strict_completion": True,
                        "pagination_exhausted": pagination_exhausted,
                        "pagination_complete": pagination_complete,
                        "termination_reason": termination_reason,
                        "termination_evidence_ids": termination_evidence_ids,
                        "stop_reasons": stop_reasons,
                        "transaction_committed": transaction_committed,
                        "checkpoint_persisted": checkpoint_persisted,
                        "completion_invariants_passed": completion_invariants_passed,
                        "completion_status": "COMPLETE_WITH_GAPS" if source_errors and completion_invariants_passed else "COMPLETE" if completion_invariants_passed else "PARTIAL",
                        "allow_article_gaps": bool(source_errors and completion_invariants_passed),
                        "exhaustive": completion_invariants_passed,
                        "reason": (
                            "strict pagination and transaction evidence persisted"
                            if completion_invariants_passed
                            else "pagination completion evidence is incomplete"
                        ),
                    },
                    settings=self.settings,
                )
        return final_result

    def audit(self) -> dict:
        def count(name: str) -> int:
            path = self._path(name)
            return read_parquet_snapshot(path).height if path.exists() else 0

        return {
            "registered_sources": len(load_registry(self.settings)),
            "enabled_sources": sum(source.crawl_enabled for source in load_registry(self.settings)),
            "crawl_runs": count("crawl_runs"),
            "crawl_items": count("crawl_items"),
            "document_versions": count("policy_document_versions"),
            "fetch_errors": count("fetch_errors"),
        }
