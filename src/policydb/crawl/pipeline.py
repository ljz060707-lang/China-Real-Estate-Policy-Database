from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

import polars as pl
import yaml

from policydb.crawl.checkpoint import append_unique, ensure_crawl_storage
from policydb.crawl.dedup import content_sha256
from policydb.crawl.discovery import discover_search_items, discover_seed_items
from policydb.crawl.fetcher import RespectfulFetcher
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
    ) -> dict:
        now = datetime.now(UTC)
        run_id = stable_id(run_type, now.isoformat(), prefix="CRAWLRUN")
        sources = [source for source in load_registry(self.settings) if source.crawl_enabled]
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
        for source in sources:
            items.extend(discover_seed_items(source, run_id))
            items.extend(
                discover_search_items(
                    source, run_id, cities, years, keyword_groups
                )
            )
        if items and self._path("crawl_items").exists():
            existing = {
                row["item_id"]: row
                for row in pl.read_parquet(self._path("crawl_items")).iter_rows(named=True)
            }
            for item in items:
                previous = existing.get(item["item_id"])
                if previous:
                    item["first_seen_at"] = previous["first_seen_at"]
                    item["retry_count"] = previous["retry_count"]
        if items:
            append_unique(self._path("crawl_items"), items, "item_id")
        runs = [
            {
                "run_id": run_id,
                "run_type": run_type,
                "scope_id": "large-cities-105",
                "period_start": start_date.isoformat(),
                "period_end": end_date.isoformat(),
                "status": "planned",
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
        return {"run_id": run_id, "source_count": len(sources), "item_count": len(items)}

    def run(self, run_id: str) -> dict:
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
        errors: list[dict] = []
        fetched = 0
        for item in pending.iter_rows(named=True):
            now = datetime.now(UTC)
            try:
                source = source_index[item["source_id"]]
                self.fetcher.rate_limit = source.rate_limit
                result = self.fetcher.fetch(item["url"])
                parsed = parse_document(result.body, result.content_type, result.final_url)
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
                    raw_path.write_bytes(result.body)
                metadata_path = raw_path.with_suffix(raw_path.suffix + ".metadata.json")
                if not metadata_path.exists():
                    metadata_path.write_text(
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
                        ),
                        encoding="utf-8",
                    )
                version_id = stable_id(item["item_id"], result.response_sha256, prefix="DOCVER")
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
                    }
                if version_id in existing_version_ids and existing_versions is not None:
                    existing_versions = existing_versions.with_columns(
                        pl.when(pl.col("document_version_id") == version_id)
                        .then(pl.lit(now.isoformat()))
                        .otherwise(pl.col("last_seen_at"))
                        .alias("last_seen_at")
                    )
                    item_status = "unchanged"
                else:
                    versions.append(version_row)
                    existing_version_ids.add(version_id)
                    item_status = "fetched"
                for attachment in parsed.get("attachments", []):
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
                                    embedded_path.write_bytes(embedded)
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
                            attachment_path.write_bytes(attachment_result.body)
                        attachment_version_id = stable_id(
                            attachment_item_id,
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
                        "retryable": not isinstance(exc, PermissionError),
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
        checkpoint = {
            "checkpoint_id": stable_id(run_id, prefix="CHECKPOINT"),
            "run_id": run_id,
            "last_item_id": pending[-1, "item_id"] if pending.height else None,
            "status": "complete",
            "processed_count": pending.height,
            "created_at": datetime.now(UTC).isoformat(),
            "updated_at": datetime.now(UTC).isoformat(),
        }
        append_unique(self._path("crawl_checkpoints"), [checkpoint], "checkpoint_id")
        return {"run_id": run_id, "fetched": fetched, "failed": len(errors)}

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
