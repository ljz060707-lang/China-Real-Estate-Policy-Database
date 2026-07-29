from __future__ import annotations

import json
import os
from datetime import UTC, date, datetime
from pathlib import Path

import polars as pl
import yaml

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
from policydb.scope import load_cities_105
from policydb.settings import Settings
from policydb.transform.normalization import stable_id


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
        if official_first:
            sources.sort(key=lambda item: item.priority)
        search_sources = [source for source in sources if source.search_url_template]
        keyword_groups: dict[str, list[str]] = {}
        cities = pl.DataFrame()
        if search_sources:
            keyword_config = yaml.safe_load(
                (
                    self.settings.root
                    / "data"
                    / "reference"
                    / "crawl_keywords.yaml"
                ).read_text(encoding="utf-8")
            )
            keyword_groups = {
                name: value["terms"]
                for name, value in keyword_config.get("groups", {}).items()
            }
            cities = load_cities_105(self.settings)
        years = range(start_date.year, end_date.year + 1)
        items = []
        discovery_errors: list[dict] = []
        for source in sources:
            seed_source = source.model_copy(
                update={"list_page_urls": []}
            )
            items.extend(discover_seed_items(seed_source, run_id))
            if source.list_page_urls and run_type != "seed_backtrack":
                try:
                    candidates = ListPageDiscovery(self.fetcher).discover(
                        DiscoveryRequest(
                            run_id=run_id,
                            mode=run_type,
                            start_date=start_date,
                            end_date=end_date,
                            max_candidates=max_items or 200,
                        ),
                        source,
                    )
                    now_text = now.isoformat()
                    items.extend(
                        {
                            "item_id": stable_id(source.source_id, item.canonical_url, prefix="CRAWLITEM"),
                            "run_id": run_id,
                            "source_id": source.source_id,
                            "url": item.url,
                            "canonical_url": item.canonical_url,
                            "status": "pending",
                            "city_id": None,
                            "query_year": item.date_hint.year if item.date_hint else None,
                            "keyword_group": item.keyword_group,
                            "retry_count": 0,
                            "first_seen_at": now_text,
                            "last_seen_at": now_text,
                            "created_at": now_text,
                            "updated_at": now_text,
                        }
                        for item in candidates
                    )
                except Exception as exc:
                    discovery_errors.append(
                        {"source_id": source.source_id, "error_type": type(exc).__name__}
                    )
            items.extend(
                discover_search_items(
                    source, run_id, cities, years, keyword_groups
                )
            )
            if max_items and len(items) >= max_items:
                items = items[:max_items]
                break
        prepared: dict[str, dict] = {}
        for item in items:
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
            existing_rows = pl.read_parquet(self._path("crawl_items")).iter_rows(named=True)
            existing = {row["canonical_url"]: row for row in existing_rows}
            for item in items:
                previous = existing.get(item["canonical_url"])
                if previous:
                    item["first_seen_at"] = previous["first_seen_at"]
                    item["retry_count"] = previous["retry_count"]
                    item["etag"] = previous.get("etag")
                    item["last_modified"] = previous.get("last_modified")
        if items:
            append_unique(self._path("crawl_items"), items, "item_id")
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

    def run(self, run_id: str, *, cancel_check=None, progress=None) -> dict:
        items_path = self._path("crawl_items")
        if not items_path.exists():
            return {"run_id": run_id, "fetched": 0, "failed": 0}
        items = pl.read_parquet(items_path)
        pending = items.filter((pl.col("run_id") == run_id) & (pl.col("status") == "pending"))
        source_index = {source.source_id: source for source in load_registry(self.settings)}
        versions_path = self._path("policy_document_versions")
        existing_versions = (
            pl.read_parquet(versions_path)
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
        fetched = 0
        cancelled = False
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
                    item["url"], etag=item.get("etag"), last_modified=item.get("last_modified")
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
                raw_dir = (
                    self.settings.root
                    / "data"
                    / "raw"
                    / "webpages"
                    / now.strftime("%Y")
                    / now.strftime("%m")
                )
                raw_dir.mkdir(parents=True, exist_ok=True)
                raw_path = raw_dir / f"{result.response_sha256}{extension}"
                if not raw_path.exists():
                    self._atomic_write(raw_path, result.body)
                metadata_path = raw_path.with_suffix(raw_path.suffix + ".metadata.json")
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
                        "local_path": str(raw_path.relative_to(self.settings.root)),
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
                                attachment_dir = (
                                    self.settings.root
                                    / "data"
                                    / "raw"
                                    / "documents"
                                    / now.strftime("%Y")
                                    / now.strftime("%m")
                                )
                                attachment_dir.mkdir(parents=True, exist_ok=True)
                                digest = content_sha256(embedded)
                                suffix = Path(str(attachment.get("label") or "")).suffix or ".bin"
                                embedded_path = attachment_dir / f"{digest}{suffix[:10]}"
                                if not embedded_path.exists():
                                    self._atomic_write(embedded_path, embedded)
                        continue
                    attachment_item_id = stable_id(item["item_id"], attachment_url, prefix="ATTACH")
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
                            self.settings.root
                            / "data"
                            / "raw"
                            / "documents"
                            / now.strftime("%Y")
                            / now.strftime("%m")
                        )
                        attachment_dir.mkdir(parents=True, exist_ok=True)
                        attachment_path = (
                            attachment_dir
                            / f"{attachment_result.response_sha256}{attachment_extension[:10]}"
                        )
                        if not attachment_path.exists():
                            self._atomic_write(attachment_path, attachment_result.body)
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
                                    "local_path": str(attachment_path.relative_to(self.settings.root)),
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
                    except Exception as attachment_error:
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
        items.write_parquet(items_path, compression="zstd")
        if existing_versions is not None:
            existing_versions.write_parquet(versions_path, compression="zstd")
        if versions:
            append_unique(self._path("policy_document_versions"), versions, "document_version_id")
        if errors:
            append_unique(self._path("fetch_errors"), errors, "error_id")
        if dedup_decisions:
            append_unique(self._path("dedup_decisions"), dedup_decisions, "decision_id")
        run_rows = pl.read_parquet(self._path("crawl_runs")).filter(pl.col("run_id") == run_id)
        if run_rows.height:
            run_row = run_rows.row(0, named=True)
            for source_id in pending["source_id"].unique().to_list():
                source_items = items.filter(
                    (pl.col("run_id") == run_id) & (pl.col("source_id") == source_id)
                )
                source_errors = sum(row["source_id"] == source_id for row in errors)
                record_source_window(
                    run_id=run_id,
                    source_id=source_id,
                    period_start=date.fromisoformat(run_row["period_start"]),
                    period_end=date.fromisoformat(run_row["period_end"]),
                    scan_method=str(run_row["run_type"]),
                    candidate_count=source_items.height,
                    fetched_count=source_items.filter(pl.col("status").is_in(["fetched", "unchanged"])).height,
                    policy_count=0,
                    error_count=source_errors,
                    page_count=0,
                    completion_evidence={
                        "reason": "detail fetch evidence only; exhaustive list-page coverage not proven"
                    },
                    settings=self.settings,
                )
        checkpoint = {
            "checkpoint_id": stable_id(run_id, prefix="CHECKPOINT"),
            "run_id": run_id,
            "last_item_id": last_item_id,
            "status": "cancelled" if cancelled else "complete",
            "processed_count": processed_count,
            "created_at": datetime.now(UTC).isoformat(),
            "updated_at": datetime.now(UTC).isoformat(),
        }
        append_unique(self._path("crawl_checkpoints"), [checkpoint], "checkpoint_id")
        result = {
            "run_id": run_id,
            "fetched": fetched,
            "failed": len(errors),
        }
        if cancelled:
            result["cancelled"] = True
        return result

    def audit(self) -> dict:
        def count(name: str) -> int:
            path = self._path(name)
            return pl.read_parquet(path).height if path.exists() else 0

        return {
            "registered_sources": len(load_registry(self.settings)),
            "enabled_sources": sum(source.crawl_enabled for source in load_registry(self.settings)),
            "crawl_runs": count("crawl_runs"),
            "crawl_items": count("crawl_items"),
            "document_versions": count("policy_document_versions"),
            "fetch_errors": count("fetch_errors"),
        }
