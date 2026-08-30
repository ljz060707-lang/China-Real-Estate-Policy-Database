"""Auditable PDF inventory, archive, discovery, download and parsing.

PDFs are a secondary representation of a policy document, not a replacement
for the existing HTML/curated record.  This module keeps the raw file
append-only, stores content-addressed archive objects, and records every
association and processing decision in the existing Parquet layer.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, fields
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import fitz
import polars as pl
import yaml

from policydb.crawl.dedup import content_sha256, normalized_text_hash
from policydb.crawl.fetcher import RespectfulFetcher
from policydb.crawl.parser import parse_document
from policydb.parquet_store import (
    atomic_write_parquet,
    read_parquet_snapshot,
)
from policydb.settings import Settings
from policydb.transform.normalization import stable_id

PDF_PIPELINE_VERSION = "1.0"
PDF_MAGIC = b"%PDF-"
PDF_ATTACHMENT_ROLES = (
    "PRIMARY_DOCUMENT",
    "SUPPORTING_ATTACHMENT",
    "GAZETTE",
    "FORM",
    "LIST",
    "TABLE",
    "ANNEX",
    "UNKNOWN",
)
_EXCLUDED_DIRS = {
    ".git",
    ".venv",
    ".pytest_cache",
    ".ruff_cache",
    ".mypy_cache",
    "__pycache__",
    "temp",
    "backups",
    "cache",
    "caches",
    "test-fixtures",
    "test_fixtures",
    ".test-tmp",
}


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(dict(payload), ensure_ascii=False, indent=2, default=str) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def _schema_frame(frame: pl.DataFrame, schema: Mapping[str, Any]) -> pl.DataFrame:
    frame = frame.clone()
    for name, dtype in schema.items():
        if name not in frame.columns:
            frame = frame.with_columns(pl.lit(None, dtype=dtype).alias(name))
        else:
            frame = frame.with_columns(pl.col(name).cast(dtype, strict=False).alias(name))
    return frame.select(list(schema))


def _read_table(path: Path, schema: Mapping[str, Any]) -> pl.DataFrame:
    if not path.exists():
        return pl.DataFrame(schema=schema)
    return _schema_frame(read_parquet_snapshot(path), schema)


def _write_table(
    path: Path,
    frame: pl.DataFrame,
    schema: Mapping[str, Any],
    *,
    key_columns: Sequence[str] = (),
    run_id: str = "pdf-pipeline",
) -> None:
    frame = _schema_frame(frame, schema)
    atomic_write_parquet(
        frame,
        path,
        {"module": "pdf_pipeline", "run_id": run_id},
        key_columns=tuple(key_columns),
        expected_schema=schema,
    )


def _upsert_rows(
    path: Path,
    rows: Sequence[Mapping[str, Any]],
    schema: Mapping[str, Any],
    key: str,
    *,
    run_id: str,
) -> pl.DataFrame:
    existing = _read_table(path, schema)
    incoming = _schema_frame(pl.DataFrame(list(rows), infer_schema_length=None), schema) if rows else pl.DataFrame(schema=schema)
    if existing.height and incoming.height:
        merged = pl.concat([existing, incoming], how="vertical_relaxed")
    else:
        merged = existing if existing.height else incoming
    if merged.height:
        merged = merged.unique(subset=[key], keep="last", maintain_order=True)
    _write_table(path, merged, schema, key_columns=(key,), run_id=run_id)
    return merged


def _json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


def _path(value: object) -> Path | None:
    if value in (None, ""):
        return None
    return Path(str(value)).expanduser()


def _resolve_local_path(settings: Settings, value: object) -> Path | None:
    candidate = _path(value)
    if candidate is None:
        return None
    if candidate.is_absolute():
        return candidate
    for root in (settings.root, settings.data_root):
        resolved = root / candidate
        if resolved.exists():
            return resolved
    return settings.root / candidate


def _normalise_text(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


PDF_INVENTORY_SCHEMA = {
    "inventory_id": pl.String,
    "local_path": pl.String,
    "relative_path": pl.String,
    "filename": pl.String,
    "file_size": pl.Int64,
    "modified_at": pl.String,
    "sha256": pl.String,
    "magic_header": pl.String,
    "is_valid_pdf": pl.Boolean,
    "page_count": pl.Int64,
    "metadata_title": pl.String,
    "metadata_author": pl.String,
    "text_char_count": pl.Int64,
    "is_scanned_candidate": pl.Boolean,
    "duplicate_group_id": pl.String,
    "archive_status": pl.String,
    "inventory_method": pl.String,
    "created_at": pl.String,
}

PDF_ASSET_SCHEMA = {
    "pdf_asset_id": pl.String,
    "sha256": pl.String,
    "archive_path": pl.String,
    "original_paths": pl.String,
    "file_size": pl.Int64,
    "page_count": pl.Int64,
    "valid_pdf": pl.Boolean,
    "is_scanned": pl.Boolean,
    "parser_name": pl.String,
    "parser_version": pl.String,
    "text_char_count": pl.Int64,
    "created_at": pl.String,
    "updated_at": pl.String,
}

PDF_ARCHIVE_SCHEMA = {
    "archive_id": pl.String,
    "pdf_asset_id": pl.String,
    "sha256": pl.String,
    "archive_path": pl.String,
    "original_path": pl.String,
    "archive_status": pl.String,
    "source_hash_verified": pl.Boolean,
    "size_bytes": pl.Int64,
    "created_at": pl.String,
    "updated_at": pl.String,
}

PDF_DUPLICATE_SCHEMA = {
    "duplicate_group_id": pl.String,
    "sha256": pl.String,
    "pdf_count": pl.Int64,
    "canonical_path": pl.String,
    "paths_json": pl.String,
    "created_at": pl.String,
}

PDF_ATTACHMENT_SCHEMA = {
    "attachment_id": pl.String,
    "document_id": pl.String,
    "parent_document_id": pl.String,
    "pdf_asset_id": pl.String,
    "city_id": pl.String,
    "source_id": pl.String,
    "source_role": pl.String,
    "discovered_url": pl.String,
    "resolved_url": pl.String,
    "discovery_page_url": pl.String,
    "anchor_text": pl.String,
    "filename": pl.String,
    "attachment_role": pl.String,
    "download_status": pl.String,
    "parse_status": pl.String,
    "ocr_status": pl.String,
    "review_status": pl.String,
    "is_primary_content": pl.Boolean,
    "content_source": pl.String,
    "match_method": pl.String,
    "match_confidence": pl.Float64,
    "content_type": pl.String,
    "content_length": pl.Int64,
    "http_status": pl.Int64,
    "redirect_chain_json": pl.String,
    "discovered_at": pl.String,
    "downloaded_at": pl.String,
    "parsed_at": pl.String,
    "last_attempt_at": pl.String,
    "last_error": pl.String,
    "evidence_json": pl.String,
}

PDF_TEXT_SCHEMA = {
    "text_version_id": pl.String,
    "pdf_asset_id": pl.String,
    "page_number": pl.Int64,
    "raw_page_text": pl.String,
    "normalized_page_text": pl.String,
    "text_hash": pl.String,
    "parser_name": pl.String,
    "parser_version": pl.String,
    "parsed_at": pl.String,
}

PDF_EVIDENCE_SCHEMA = {
    "discovery_id": pl.String,
    "attachment_id": pl.String,
    "document_id": pl.String,
    "source_id": pl.String,
    "city_id": pl.String,
    "discovery_page_url": pl.String,
    "discovered_url": pl.String,
    "anchor_text": pl.String,
    "discovery_method": pl.String,
    "candidate_type": pl.String,
    "evidence_json": pl.String,
    "created_at": pl.String,
}

PDF_DOWNLOAD_SCHEMA = {
    "download_id": pl.String,
    "attachment_id": pl.String,
    "run_id": pl.String,
    "requested_url": pl.String,
    "resolved_url": pl.String,
    "status": pl.String,
    "http_status": pl.Int64,
    "content_type": pl.String,
    "content_length": pl.Int64,
    "sha256": pl.String,
    "network_route": pl.String,
    "redirect_chain_json": pl.String,
    "error_type": pl.String,
    "error_message": pl.String,
    "attempted_at": pl.String,
}

PDF_EVENT_SCHEMA = {
    "event_id": pl.String,
    "run_id": pl.String,
    "attachment_id": pl.String,
    "pdf_asset_id": pl.String,
    "stage": pl.String,
    "status": pl.String,
    "reason_code": pl.String,
    "evidence_json": pl.String,
    "created_at": pl.String,
}


@dataclass(slots=True)
class PDFPipelineConfig:
    inventory_root: Path | None = None
    archive_root: Path | None = None
    enabled: bool = True
    discover_during_html_crawl: bool = True
    download_during_fast_ingest: bool = True
    parse_after_download: bool = True
    ocr_enabled: bool = False
    max_downloads_per_source: int = 20
    max_downloads_per_job: int = 30
    download_workers: int = 4
    parse_workers: int = 2
    connect_timeout_seconds: float = 20.0
    read_timeout_seconds: float = 90.0
    total_timeout_seconds: float = 120.0
    retry_count: int = 2
    max_file_size_mb: int = 100
    scan_text_threshold_per_page: int = 30
    max_discovery_documents: int = 1000

    def validate(self) -> None:
        for name in (
            "max_downloads_per_source",
            "max_downloads_per_job",
            "download_workers",
            "parse_workers",
            "retry_count",
            "max_file_size_mb",
            "scan_text_threshold_per_page",
            "max_discovery_documents",
        ):
            if int(getattr(self, name)) < 1:
                raise ValueError(f"{name} must be positive")
        for name in ("connect_timeout_seconds", "read_timeout_seconds", "total_timeout_seconds"):
            if float(getattr(self, name)) <= 0:
                raise ValueError(f"{name} must be positive")
        if self.ocr_enabled:
            raise ValueError("OCR is intentionally disabled in this release")

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any] | None) -> PDFPipelineConfig:
        values = dict(payload or {})
        allowed = {field.name for field in fields(cls)}
        values = {key: value for key, value in values.items() if key in allowed}
        for name in ("inventory_root", "archive_root"):
            if values.get(name):
                values[name] = Path(str(values[name]))
        config = cls(**values)
        config.validate()
        return config


def load_pdf_config(settings: Settings | None = None, path: Path | None = None) -> PDFPipelineConfig:
    settings = settings or Settings.discover()
    config_path = path or settings.root / "config" / "pdf_pipeline.yaml"
    payload: Mapping[str, Any] = {}
    if config_path.exists():
        loaded = yaml.safe_load(config_path.read_text(encoding="utf-8-sig")) or {}
        if isinstance(loaded, Mapping):
            nested = loaded.get("pdf_pipeline")
            payload = nested if isinstance(nested, Mapping) else loaded
    config = PDFPipelineConfig.from_mapping(payload)
    # An explicit runtime data root is an isolation boundary.  Do not let the
    # repository's static production-oriented YAML redirect a rehearsal or
    # test into another data root.  Production callers without an explicit
    # root retain the configured paths for backward compatibility.
    explicit_data_root = settings.data_root_path is not None or any(
        os.getenv(name) for name in ("CRPD_DATA_ROOT", "POLICYDB_DATA_ROOT")
    )
    if explicit_data_root:
        config.inventory_root = settings.data_root
        configured_archive = os.getenv("CRPD_ARCHIVE_ROOT") or os.getenv("POLICYDB_ARCHIVE_ROOT")
        config.archive_root = (
            Path(configured_archive).expanduser()
            if configured_archive
            else settings.data_root / "raw" / "pdf"
        )
    else:
        if config.inventory_root is None:
            config.inventory_root = settings.data_root
        if config.archive_root is None:
            config.archive_root = settings.data_root / "raw" / "pdf"
    config.validate()
    return config


def _is_excluded(path: Path, inventory_root: Path, archive_root: Path) -> bool:
    try:
        relative = path.relative_to(inventory_root)
    except ValueError:
        relative = path
    parts = {part.lower() for part in relative.parts[:-1]}
    if parts & {item.lower() for item in _EXCLUDED_DIRS}:
        return True
    try:
        path.resolve().relative_to((archive_root / "objects").resolve())
        return True
    except ValueError:
        pass
    for subdir in ("by_city", "objects"):
        try:
            path.resolve().relative_to((archive_root / subdir).resolve())
            return True
        except ValueError:
            continue
    return False


def _inspect_pdf(path: Path, *, threshold_per_page: int = 30) -> dict[str, Any]:
    header = path.open("rb").read(5)
    valid = header == PDF_MAGIC
    page_count = None
    title = None
    author = None
    text_char_count = 0
    parser_error = None
    if valid:
        try:
            with fitz.open(str(path)) as document:
                page_count = len(document)
                metadata = document.metadata or {}
                title = metadata.get("title") or None
                author = metadata.get("author") or None
                text_char_count = sum(len(page.get_text() or "") for page in document)
        except Exception as exc:  # the inventory must retain corrupted evidence
            valid = False
            parser_error = f"{type(exc).__name__}: {str(exc)[:500]}"
    return {
        "magic_header": header.decode("latin1", errors="replace"),
        "is_valid_pdf": bool(valid),
        "page_count": page_count,
        "metadata_title": title,
        "metadata_author": author,
        "text_char_count": int(text_char_count),
        "is_scanned_candidate": bool(valid and page_count and text_char_count < max(100, page_count * threshold_per_page)),
        "parser_error": parser_error,
    }


class PDFPipeline:
    """Single business layer used by CLI, FastBulk and Dashboard jobs."""

    def __init__(
        self,
        settings: Settings | None = None,
        *,
        config: PDFPipelineConfig | None = None,
        fetcher: Any | None = None,
        initialize_storage: bool = True,
    ) -> None:
        self.settings = settings or Settings.discover()
        self.config = config or load_pdf_config(self.settings)
        self.config.validate()
        self.inventory_root = Path(self.config.inventory_root or self.settings.data_root).resolve()
        self.archive_root = Path(self.config.archive_root or self.settings.data_root / "raw" / "pdf").resolve()
        self.objects_root = self.archive_root / "objects"
        self.quarantine_root = self.archive_root / "quarantine"
        self.derived_text_root = self.settings.data_root / "derived" / "pdf_text"
        self.manifest_root = self.settings.data_root / "manifests"
        self.fetcher = fetcher
        if initialize_storage:
            self._ensure_storage()

    def _ensure_storage(self) -> None:
        for path in (self.objects_root, self.archive_root / "by_city", self.quarantine_root, self.derived_text_root, self.manifest_root):
            path.mkdir(parents=True, exist_ok=True)
        for name, schema in {
            "pdf_assets": PDF_ASSET_SCHEMA,
            "document_attachments": PDF_ATTACHMENT_SCHEMA,
            "pdf_text_versions": PDF_TEXT_SCHEMA,
            "pdf_discovery_evidence": PDF_EVIDENCE_SCHEMA,
            "pdf_download_audit": PDF_DOWNLOAD_SCHEMA,
            "pdf_processing_events": PDF_EVENT_SCHEMA,
        }.items():
            path = self.settings.curated / f"{name}.parquet"
            if not path.exists():
                _write_table(path, pl.DataFrame(schema=schema), schema, key_columns=(next(iter(schema)),))

    def _manifest(self, name: str) -> Path:
        return self.manifest_root / f"{name}.parquet"

    def _curated(self, name: str) -> Path:
        return self.settings.curated / f"{name}.parquet"

    def _relative_data_path(self, path: Path) -> str:
        try:
            return path.resolve().relative_to(self.settings.data_root.resolve()).as_posix()
        except ValueError:
            return str(path)

    def _archive_path(self, sha256: str) -> Path:
        return self.objects_root / sha256[:2] / f"{sha256}.pdf"

    def _record_event(self, *, run_id: str, stage: str, status: str, reason_code: str, attachment_id: str | None = None, pdf_asset_id: str | None = None, evidence: Mapping[str, Any] | None = None) -> None:
        event_id = stable_id(run_id, stage, status, attachment_id, pdf_asset_id, reason_code, prefix="PDF_EVENT")
        _upsert_rows(
            self._curated("pdf_processing_events"),
            [{"event_id": event_id, "run_id": run_id, "attachment_id": attachment_id, "pdf_asset_id": pdf_asset_id, "stage": stage, "status": status, "reason_code": reason_code, "evidence_json": _json(evidence or {}), "created_at": _now()}],
            PDF_EVENT_SCHEMA,
            "event_id",
            run_id=run_id,
        )

    def inventory(self, *, limit: int | None = None, run_id: str | None = None) -> dict[str, Any]:
        run_id = run_id or f"PDFINV_{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}"
        files = sorted(
            (path for path in self.inventory_root.rglob("*") if path.is_file() and path.suffix.lower() == ".pdf" and not _is_excluded(path, self.inventory_root, self.archive_root)),
            key=lambda item: str(item).lower(),
        )
        rows: list[dict[str, Any]] = []
        for path in files[:limit] if limit is not None else files:
            stat = path.stat()
            digest = _sha256_file(path)
            inspection = _inspect_pdf(path, threshold_per_page=self.config.scan_text_threshold_per_page)
            rows.append({
                "inventory_id": stable_id(str(path), digest, prefix="PDFINV"),
                "local_path": str(path),
                "relative_path": self._relative_data_path(path),
                "filename": path.name,
                "file_size": stat.st_size,
                "modified_at": datetime.fromtimestamp(stat.st_mtime, UTC).isoformat(),
                "sha256": digest,
                "magic_header": inspection["magic_header"],
                "is_valid_pdf": inspection["is_valid_pdf"],
                "page_count": inspection["page_count"],
                "metadata_title": inspection["metadata_title"],
                "metadata_author": inspection["metadata_author"],
                "text_char_count": inspection["text_char_count"],
                "is_scanned_candidate": inspection["is_scanned_candidate"],
                "duplicate_group_id": stable_id(digest, prefix="PDFDUP"),
                "archive_status": "INVENTORIED" if inspection["is_valid_pdf"] else "QUARANTINE_REVIEW",
                "inventory_method": "recursive_read_only_scan",
                "created_at": _now(),
            })
        frame = _schema_frame(pl.DataFrame(rows, infer_schema_length=None) if rows else pl.DataFrame(schema=PDF_INVENTORY_SCHEMA), PDF_INVENTORY_SCHEMA)
        existing_inventory = _read_table(self._manifest("existing_pdf_inventory"), PDF_INVENTORY_SCHEMA)
        if existing_inventory.height and frame.height:
            frame = _schema_frame(
                pl.concat([existing_inventory, frame], how="vertical_relaxed")
                .unique(subset=["inventory_id"], keep="last", maintain_order=True),
                PDF_INVENTORY_SCHEMA,
            )
        elif existing_inventory.height:
            frame = existing_inventory
        _write_table(self._manifest("existing_pdf_inventory"), frame, PDF_INVENTORY_SCHEMA, key_columns=("inventory_id",), run_id=run_id)
        duplicate_rows = []
        if frame.height:
            for row in frame.group_by("sha256").agg(pl.col("local_path").alias("paths")).to_dicts():
                paths = [str(value) for value in (row.get("paths") or [])]
                duplicate_rows.append({"duplicate_group_id": stable_id(row["sha256"], prefix="PDFDUP"), "sha256": row["sha256"], "pdf_count": len(paths), "canonical_path": sorted(paths)[0], "paths_json": _json(sorted(paths)), "created_at": _now()})
        _write_table(self._manifest("pdf_duplicate_groups"), pl.DataFrame(duplicate_rows, infer_schema_length=None) if duplicate_rows else pl.DataFrame(schema=PDF_DUPLICATE_SCHEMA), PDF_DUPLICATE_SCHEMA, key_columns=("duplicate_group_id",), run_id=run_id)
        invalid = int(frame.filter(~pl.col("is_valid_pdf")).height) if frame.height else 0
        return {"run_id": run_id, "inventory_root": str(self.inventory_root), "scanned": len(files), "recorded": frame.height, "valid_pdf": frame.filter(pl.col("is_valid_pdf")).height if frame.height else 0, "invalid_pdf": invalid, "duplicate_groups": len(duplicate_rows), "limited": limit is not None}

    def _inventory_frame(self) -> pl.DataFrame:
        return _read_table(self._manifest("existing_pdf_inventory"), PDF_INVENTORY_SCHEMA)

    def archive(self, *, limit: int | None = None, run_id: str | None = None) -> dict[str, Any]:
        run_id = run_id or f"PDFARC_{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}"
        existing_inventory = self._inventory_frame()
        if existing_inventory.height:
            inventory_result = {
                "run_id": run_id,
                "inventory_root": str(self.inventory_root),
                "scanned": existing_inventory.height,
                "recorded": existing_inventory.height,
                "valid_pdf": int(existing_inventory.filter(pl.col("is_valid_pdf")).height),
                "invalid_pdf": int(existing_inventory.filter(~pl.col("is_valid_pdf")).height),
                "duplicate_groups": int(existing_inventory.get_column("duplicate_group_id").n_unique()),
                "limited": False,
                "inventory_reused": True,
            }
        else:
            inventory_result = self.inventory(run_id=run_id)
        inventory = self._inventory_frame().filter(pl.col("is_valid_pdf"))
        selected = inventory.head(limit) if limit is not None else inventory
        assets: list[dict[str, Any]] = []
        archives: list[dict[str, Any]] = []
        copied = verified = conflicts = 0
        grouped = selected.group_by("sha256", maintain_order=True).agg(
            pl.col("local_path").alias("local_paths"),
            pl.col("file_size").first().alias("file_size"),
            pl.col("page_count").first().alias("page_count"),
            pl.col("is_scanned_candidate").first().alias("is_scanned_candidate"),
            pl.col("text_char_count").first().alias("text_char_count"),
        )
        for row in grouped.to_dicts():
            local_paths = sorted(str(value) for value in (row.get("local_paths") or []))
            if not local_paths:
                continue
            source = Path(local_paths[0])
            digest = str(row["sha256"])
            target = self._archive_path(digest)
            target.parent.mkdir(parents=True, exist_ok=True)
            status = "ARCHIVED"
            if target.exists():
                if _sha256_file(target) != digest:
                    status = "ARCHIVE_CONFLICT"
                    conflicts += 1
                else:
                    verified += 1
            else:
                part = target.with_suffix(".pdf.part")
                shutil.copyfile(source, part)
                if _sha256_file(part) != digest:
                    part.unlink(missing_ok=True)
                    raise OSError(f"PDF archive hash validation failed: {source}")
                os.replace(part, target)
                copied += 1
                verified += 1
            asset_id = stable_id(digest, prefix="PDFASSET")
            now = _now()
            if status == "ARCHIVED":
                previous_assets = _read_table(self._curated("pdf_assets"), PDF_ASSET_SCHEMA)
                previous = previous_assets.filter(pl.col("pdf_asset_id") == asset_id) if previous_assets.height else previous_assets
                previous_paths: list[str] = []
                if previous.height:
                    try:
                        previous_paths = [str(value) for value in json.loads(previous[0, "original_paths"] or "[]")]
                    except (TypeError, json.JSONDecodeError):
                        previous_paths = []
                assets.append({"pdf_asset_id": asset_id, "sha256": digest, "archive_path": self._relative_data_path(target), "original_paths": _json(sorted(set(previous_paths + local_paths))), "file_size": int(target.stat().st_size), "page_count": row["page_count"], "valid_pdf": True, "is_scanned": bool(row["is_scanned_candidate"]), "parser_name": previous[0, "parser_name"] if previous.height else None, "parser_version": previous[0, "parser_version"] if previous.height else None, "text_char_count": int(row["text_char_count"] or 0), "created_at": previous[0, "created_at"] if previous.height else now, "updated_at": now})
            for original_path in local_paths:
                archives.append({"archive_id": stable_id(asset_id, original_path, prefix="PDFARCH"), "pdf_asset_id": asset_id, "sha256": digest, "archive_path": self._relative_data_path(target), "original_path": original_path, "archive_status": status, "source_hash_verified": status == "ARCHIVED", "size_bytes": int(target.stat().st_size) if target.exists() else int(row["file_size"]), "created_at": now, "updated_at": now})
        if assets:
            _upsert_rows(self._curated("pdf_assets"), assets, PDF_ASSET_SCHEMA, "pdf_asset_id", run_id=run_id)
        archive_frame = _read_table(self._manifest("pdf_archive_manifest"), PDF_ARCHIVE_SCHEMA)
        if archives:
            archive_frame = _schema_frame(pl.concat([archive_frame, pl.DataFrame(archives, infer_schema_length=None)], how="vertical_relaxed").unique(subset=["archive_id"], keep="last", maintain_order=True), PDF_ARCHIVE_SCHEMA)
        _write_table(self._manifest("pdf_archive_manifest"), archive_frame, PDF_ARCHIVE_SCHEMA, key_columns=("archive_id",), run_id=run_id)
        for row in assets:
            self._record_event(run_id=run_id, stage="archive", status="ARCHIVED", reason_code="content_addressed_copy", pdf_asset_id=row["pdf_asset_id"], evidence={"archive_path": row["archive_path"]})
        self._sync_policy_files(run_id=run_id)
        return {**inventory_result, "run_id": run_id, "selected": selected.height, "groups": grouped.height, "copied": copied, "verified": verified, "conflicts": conflicts, "archive_root": str(self.archive_root)}

    def _context_maps(self) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
        versions = _read_table(self._curated("policy_document_versions"), {"document_version_id": pl.String, "record_id": pl.String, "crawl_item_id": pl.String, "source_id": pl.String, "canonical_url": pl.String, "final_url": pl.String, "local_path": pl.String, "content_type": pl.String, "content_sha256": pl.String, "title": pl.String, "parse_status": pl.String})
        items = _read_table(self._curated("crawl_items"), {"item_id": pl.String, "city_id": pl.String, "source_id": pl.String, "canonical_url": pl.String, "final_url": pl.String})
        geos = _read_table(self._curated("record_geographies_normalized"), {"record_id": pl.String, "city_id": pl.String, "city_name": pl.String})
        return ({str(row["document_version_id"]): row for row in versions.to_dicts()}, {str(row["item_id"]): row for row in items.to_dicts()}, {str(row["record_id"]): row for row in geos.to_dicts()})

    @staticmethod
    def _role(anchor: str, *, primary: bool = False) -> str:
        if primary:
            return "PRIMARY_DOCUMENT"
        lowered = anchor.lower()
        if "公报" in anchor or "gazette" in lowered:
            return "GAZETTE"
        if "申请" in anchor or "表格" in anchor or "form" in lowered:
            return "FORM"
        if "名单" in anchor or "list" in lowered:
            return "LIST"
        if "表" in anchor or "table" in lowered:
            return "TABLE"
        if "附件" in anchor or "annex" in lowered:
            return "ANNEX"
        return "UNKNOWN"

    def discover(self, *, limit: int | None = None, city_id: str | None = None, source_id: str | None = None, run_id: str | None = None) -> dict[str, Any]:
        run_id = run_id or f"PDFDISC_{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}"
        version_map, item_map, geo_map = self._context_maps()
        archived_assets = _read_table(self._curated("pdf_assets"), PDF_ASSET_SCHEMA)
        archived_shas = {str(value).lower() for value in archived_assets.get_column("sha256").drop_nulls().to_list()} if archived_assets.height else set()
        rows: list[dict[str, Any]] = []
        evidence: list[dict[str, Any]] = []
        scanned_documents = 0
        truncated = False
        scan_limit = self.config.max_discovery_documents if limit is None else min(self.config.max_discovery_documents, max(200, limit * 20))
        for version in list(version_map.values())[:scan_limit]:
            if limit is not None and len(rows) >= limit:
                truncated = True
                break
            if city_id or source_id:
                item = item_map.get(str(version.get("crawl_item_id")), {})
                geo = geo_map.get(str(version.get("record_id")), {})
                if city_id and str(item.get("city_id") or geo.get("city_id") or "") != str(city_id):
                    continue
                if source_id and str(version.get("source_id") or item.get("source_id") or "") != str(source_id):
                    continue
            document_id = version.get("record_id")
            item = item_map.get(str(version.get("crawl_item_id")), {})
            geo = geo_map.get(str(document_id), {})
            local = _resolve_local_path(self.settings, version.get("local_path"))
            is_pdf = "pdf" in str(version.get("content_type") or "").lower() or str(local or "").lower().endswith(".pdf")
            if is_pdf:
                source_url = version.get("final_url") or version.get("canonical_url")
                observed_sha = None
                if local and local.is_file() and local.suffix.lower() == ".pdf":
                    try:
                        digest = _sha256_file(local)
                    except OSError:
                        digest = None
                    if digest and digest.lower() in archived_shas:
                        observed_sha = digest
                version_sha = str(version.get("content_sha256") or "").lower()
                asset_sha = observed_sha or (version_sha if version_sha in archived_shas else None)
                rows.append(self._attachment_row(document_id=document_id, parent_document_id=document_id, asset_sha=asset_sha, city_id=item.get("city_id") or geo.get("city_id"), source_id=version.get("source_id") or item.get("source_id"), discovered_url=source_url, resolved_url=source_url, discovery_page_url=version.get("canonical_url"), anchor_text=version.get("title") or Path(str(source_url or local or "document.pdf")).name, filename=Path(str(source_url or local or "document.pdf")).name, role=self._role(str(version.get("title") or ""), primary=True), primary=True, method="policy_document_version", local_path=local, evidence={"document_version_id": version.get("document_version_id"), "content_sha256": version.get("content_sha256"), "archived_asset_match": bool(asset_sha)}))
                scanned_documents += 1
                continue
            if local is None or not local.is_file():
                continue
            try:
                parsed = parse_document(local.read_bytes(), str(version.get("content_type") or "text/html"), str(version.get("canonical_url") or ""))
            except OSError:
                continue
            candidates = [
                item
                for item in parsed.get("attachments", [])
                if str(item.get("url") or "").lower().split("?", 1)[0].endswith(".pdf")
                or "pdf" in str(item.get("label") or "").lower()
            ]
            if not candidates:
                continue
            scanned_documents += 1
            for candidate in candidates:
                if limit is not None and len(rows) >= limit:
                    truncated = True
                    break
                url = str(candidate.get("url") or "").strip()
                if not url or urlsplit(url).scheme.lower() not in {"http", "https"}:
                    continue
                label = str(candidate.get("label") or Path(urlsplit(url).path).name or "PDF")
                rows.append(self._attachment_row(document_id=document_id, parent_document_id=document_id, asset_sha=None, city_id=item.get("city_id") or geo.get("city_id"), source_id=version.get("source_id") or item.get("source_id"), discovered_url=url, resolved_url=None, discovery_page_url=version.get("canonical_url"), anchor_text=label, filename=Path(urlsplit(url).path).name or label, role=self._role(label), primary=False, method="html_pdf_link", local_path=None, evidence={"document_version_id": version.get("document_version_id"), "parser": "crawl.parser"}))
        rows = rows[:limit] if limit is not None else rows
        existing_attachments = _read_table(self._curated("document_attachments"), PDF_ATTACHMENT_SCHEMA)
        previous_by_id = {str(row["attachment_id"]): row for row in existing_attachments.to_dicts()} if existing_attachments.height else {}
        archived_asset_ids = {stable_id(value, prefix="PDFASSET") for value in archived_shas}
        operational_fields = ("pdf_asset_id", "download_status", "parse_status", "ocr_status", "review_status", "content_type", "content_length", "http_status", "redirect_chain_json", "downloaded_at", "parsed_at", "last_attempt_at", "last_error")
        for row in rows:
            previous = previous_by_id.get(str(row["attachment_id"]))
            if not previous:
                continue
            current_asset_id = str(row.get("pdf_asset_id") or "")
            previous_asset_id = str(previous.get("pdf_asset_id") or "")
            same_known_asset = bool(current_asset_id and current_asset_id == previous_asset_id and current_asset_id in archived_asset_ids)
            same_remote_candidate = not current_asset_id and not previous_asset_id and row.get("download_status") == "PENDING_DOWNLOAD"
            if same_known_asset or same_remote_candidate:
                for field in operational_fields:
                    if previous.get(field) is not None:
                        row[field] = previous[field]
        if rows:
            for row in rows:
                evidence.append({"discovery_id": stable_id(row["attachment_id"], row["discovery_page_url"], prefix="PDFDISC"), "attachment_id": row["attachment_id"], "document_id": row["document_id"], "source_id": row["source_id"], "city_id": row["city_id"], "discovery_page_url": row["discovery_page_url"], "discovered_url": row["discovered_url"], "anchor_text": row["anchor_text"], "discovery_method": "policy_document_version", "candidate_type": "PDF", "evidence_json": row["evidence_json"], "created_at": row["discovered_at"]})
            _upsert_rows(self._curated("document_attachments"), rows, PDF_ATTACHMENT_SCHEMA, "attachment_id", run_id=run_id)
            _upsert_rows(self._curated("pdf_discovery_evidence"), evidence, PDF_EVIDENCE_SCHEMA, "discovery_id", run_id=run_id)
        self._ensure_unmatched_assets(run_id=run_id, limit=limit)
        self._record_event(run_id=run_id, stage="discover", status="COMPLETED", reason_code="html_and_pdf_evidence_scanned", evidence={"scanned_documents": scanned_documents, "discovered": len(rows)})
        if len(version_map) > scan_limit:
            truncated = True
        return {"run_id": run_id, "scanned_documents": scanned_documents, "discovered": len(rows), "new_candidates": len(rows), "limited": limit is not None, "discovery_truncated": truncated, "scan_limit": scan_limit}

    def _attachment_row(self, *, document_id: object, parent_document_id: object, asset_sha: object, city_id: object, source_id: object, discovered_url: object, resolved_url: object, discovery_page_url: object, anchor_text: str, filename: str, role: str, primary: bool, method: str, local_path: Path | None, evidence: Mapping[str, Any]) -> dict[str, Any]:
        now = _now()
        asset_id = stable_id(str(asset_sha), prefix="PDFASSET") if asset_sha else None
        local_is_pdf = bool(local_path and local_path.is_file() and local_path.suffix.lower() == ".pdf")
        status = "ARCHIVED_EXISTING" if local_is_pdf and asset_id else "PENDING_ARCHIVE" if local_is_pdf else "PENDING_DOWNLOAD"
        return {"attachment_id": stable_id(document_id, discovered_url or local_path, prefix="PDFATTACH"), "document_id": document_id, "parent_document_id": parent_document_id, "pdf_asset_id": asset_id, "city_id": city_id, "source_id": source_id, "source_role": None, "discovered_url": discovered_url, "resolved_url": resolved_url, "discovery_page_url": discovery_page_url, "anchor_text": anchor_text, "filename": filename, "attachment_role": role if role in PDF_ATTACHMENT_ROLES else "UNKNOWN", "download_status": status, "parse_status": "PENDING_PARSE", "ocr_status": "DISABLED", "review_status": "AUTO_LINKED" if primary else "PENDING", "is_primary_content": primary, "content_source": "PDF" if primary else "HTML+PDF", "match_method": method, "match_confidence": 1.0 if primary else 0.95, "content_type": "application/pdf" if local_is_pdf else None, "content_length": local_path.stat().st_size if local_is_pdf else None, "http_status": None, "redirect_chain_json": "[]", "discovered_at": now, "downloaded_at": None, "parsed_at": None, "last_attempt_at": None, "last_error": None, "evidence_json": _json(evidence)}

    def _ensure_unmatched_assets(self, *, run_id: str, limit: int | None = None) -> int:
        assets = _read_table(self._curated("pdf_assets"), PDF_ASSET_SCHEMA)
        attachments = _read_table(self._curated("document_attachments"), PDF_ATTACHMENT_SCHEMA)
        linked = {str(value) for value in attachments.get_column("pdf_asset_id").drop_nulls().to_list()} if attachments.height else set()
        versions, items, geos = self._context_maps()
        files = _read_table(self._curated("policy_files"), {"sha256_actual": pl.String, "sha256_expected": pl.String, "record_id": pl.String, "document_version_id": pl.String, "content_type": pl.String})
        matches: dict[str, list[dict[str, Any]]] = {}
        if files.height:
            for row in files.to_dicts():
                if "pdf" not in str(row.get("content_type") or "").lower():
                    continue
                digest = str(row.get("sha256_actual") or row.get("sha256_expected") or "").lower()
                if digest and row.get("record_id"):
                    matches.setdefault(digest, []).append({**row, "match_method": "sha256_policy_file"})
        for version in versions.values():
            digest = str(version.get("content_sha256") or "").lower()
            if digest and version.get("record_id"):
                matches.setdefault(digest, []).append({**version, "match_method": "sha256_document_version"})
        rows: list[dict[str, Any]] = []
        added = 0
        for asset in assets.to_dicts():
            asset_id = str(asset.get("pdf_asset_id") or "")
            if not asset_id or asset_id in linked:
                continue
            if limit is not None and added >= limit:
                break
            digest_matches = {str(row.get("record_id")): row for row in matches.get(str(asset.get("sha256") or "").lower(), []) if row.get("record_id")}
            if len(digest_matches) == 1:
                match = next(iter(digest_matches.values()))
                document_id = str(match["record_id"])
                version_id = match.get("document_version_id")
                version = versions.get(str(version_id), {}) if version_id else {}
                item = items.get(str(version.get("crawl_item_id")), {})
                geo = geos.get(document_id, {})
                local_path = self.settings.data_root / str(asset.get("archive_path") or "")
                row = self._attachment_row(
                    document_id=document_id,
                    parent_document_id=version_id or document_id,
                    asset_sha=asset.get("sha256"),
                    city_id=item.get("city_id") or geo.get("city_id"),
                    source_id=version.get("source_id") or item.get("source_id"),
                    discovered_url=version.get("final_url") or version.get("canonical_url"),
                    resolved_url=version.get("final_url") or version.get("canonical_url"),
                    discovery_page_url=version.get("canonical_url"),
                    anchor_text=version.get("title") or Path(str(asset.get("archive_path") or "document.pdf")).name,
                    filename=Path(str(asset.get("archive_path") or "document.pdf")).name,
                    role="PRIMARY_DOCUMENT",
                    primary=True,
                    method=str(match.get("match_method") or "sha256_document_version"),
                    local_path=local_path,
                    evidence={"sha256": asset.get("sha256"), "document_id": document_id, "document_version_id": version_id},
                )
                row.update({"download_status": "DOWNLOADED", "parse_status": "OCR_PENDING" if asset.get("is_scanned") else "PARSED" if asset.get("parser_name") else "PENDING_PARSE", "content_source": "PDF", "review_status": "AUTO_LINKED"})
                rows.append(row)
                added += 1
                continue
            paths: list[str] = []
            try:
                paths = [str(value) for value in json.loads(asset.get("original_paths") or "[]")]
            except (TypeError, json.JSONDecodeError):
                pass
            filename = Path(paths[0]).name if paths else Path(str(asset.get("archive_path") or "document.pdf")).name
            asset["original_paths"] = _json(paths) if paths else ""
            rows.append({"attachment_id": stable_id("unmatched", asset_id, prefix="PDFATTACH"), "document_id": None, "parent_document_id": None, "pdf_asset_id": asset_id, "city_id": None, "source_id": None, "source_role": None, "discovered_url": None, "resolved_url": None, "discovery_page_url": None, "anchor_text": Path(json.loads(asset.get("original_paths") or "[]")[0]).name if asset.get("original_paths") else None, "filename": Path(json.loads(asset.get("original_paths") or "[]")[0]).name if asset.get("original_paths") else None, "attachment_role": "UNKNOWN", "download_status": "UNMATCHED_EXISTING_PDF", "parse_status": "PARSED" if asset.get("parser_name") else "PENDING_PARSE", "ocr_status": "DISABLED", "review_status": "HUMAN_REVIEW", "is_primary_content": False, "content_source": "EXISTING_PDF", "match_method": "no_policy_match", "match_confidence": 0.0, "content_type": "application/pdf", "content_length": asset.get("file_size"), "http_status": None, "redirect_chain_json": "[]", "discovered_at": _now(), "downloaded_at": None, "parsed_at": None, "last_attempt_at": None, "last_error": "UNMATCHED_EXISTING_PDF", "evidence_json": _json({"pdf_asset_id": asset_id, "archive_path": asset.get("archive_path")})})
            rows[-1]["anchor_text"] = filename
            rows[-1]["filename"] = filename
            added += 1
        if rows:
            _upsert_rows(self._curated("document_attachments"), rows, PDF_ATTACHMENT_SCHEMA, "attachment_id", run_id=run_id)
        return len(rows)

    def _quarantine_bytes(self, body: bytes, attachment_id: str) -> tuple[str, str]:
        digest = _sha256_bytes(body)
        target = self.quarantine_root / f"{attachment_id}_{digest}.bin"
        if not target.exists():
            part = target.with_suffix(".bin.part")
            part.write_bytes(body)
            if _sha256_file(part) != digest:
                part.unlink(missing_ok=True)
                raise OSError("quarantine hash validation failed")
            os.replace(part, target)
        return self._relative_data_path(target), digest

    def _get_fetcher(self) -> Any:
        if self.fetcher is not None:
            return self.fetcher
        return RespectfulFetcher(user_agent=self.settings.user_agent, timeout=self.config.total_timeout_seconds, connect_timeout=self.config.connect_timeout_seconds, retries=self.config.retry_count, rate_limit=self.settings.default_rate_limit, check_robots=self.settings.respect_robots, max_response_bytes=self.config.max_file_size_mb * 1024 * 1024)

    def _sync_policy_files(self, *, run_id: str) -> int:
        """Expose linked PDF objects through the legacy policy-file view."""
        assets = _read_table(self._curated("pdf_assets"), PDF_ASSET_SCHEMA)
        attachments = _read_table(self._curated("document_attachments"), PDF_ATTACHMENT_SCHEMA)
        if assets.is_empty() or attachments.is_empty():
            return 0
        by_asset = {str(row["pdf_asset_id"]): row for row in assets.to_dicts()}
        rows: list[dict[str, Any]] = []
        for row in attachments.to_dicts():
            asset = by_asset.get(str(row.get("pdf_asset_id") or ""))
            if not asset or not row.get("document_id") or not asset.get("archive_path"):
                continue
            rows.append({
                "policy_file_id": stable_id(str(row["document_id"]), str(asset["sha256"]), prefix="FILEPDF"),
                "document_version_id": row.get("parent_document_id") or row.get("document_id"),
                "record_id": row.get("document_id"),
                "source_local_path": None,
                "archive_relative_path": asset.get("archive_path"),
                "content_type": row.get("content_type") or "application/pdf",
                "sha256_expected": asset.get("sha256"),
                "sha256_actual": asset.get("sha256"),
                "size_bytes": asset.get("file_size"),
                "archive_status": "archived",
                "checked_at": _now(),
            })
        if not rows:
            return 0
        target = self.settings.curated / "policy_files.parquet"
        existing = read_parquet_snapshot(target) if target.exists() else pl.DataFrame()
        incoming = pl.DataFrame(rows, infer_schema_length=None)
        merged = pl.concat([existing, incoming], how="diagonal_relaxed") if existing.height else incoming
        merged = merged.unique(subset=["policy_file_id"], keep="last", maintain_order=True)
        atomic_write_parquet(merged, target, {"module": "pdf_pipeline.policy_files", "run_id": run_id}, key_columns=("policy_file_id",))
        return len(rows)

    def download(self, *, limit: int | None = None, city_id: str | None = None, source_id: str | None = None, run_id: str | None = None) -> dict[str, Any]:
        run_id = run_id or f"PDFGET_{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}"
        path = self._curated("document_attachments")
        frame = _read_table(path, PDF_ATTACHMENT_SCHEMA)
        if frame.is_empty():
            return {"run_id": run_id, "selected": 0, "downloaded": 0, "failed": 0, "status": "NO_PENDING_PDF"}
        pending = frame.filter(pl.col("download_status").is_in(["PENDING_DOWNLOAD", "RETRY_WAIT", "FAILED"]) & pl.col("discovered_url").is_not_null())
        pending = pending.filter(
            pl.col("filename").cast(pl.String).str.to_lowercase().str.ends_with(".pdf")
            | pl.col("discovered_url").cast(pl.String).str.to_lowercase().str.split("?").list.first().str.ends_with(".pdf")
            | pl.col("content_type").cast(pl.String).str.to_lowercase().str.contains("pdf")
        )
        if city_id:
            pending = pending.filter(pl.col("city_id").cast(pl.String) == str(city_id))
        if source_id:
            pending = pending.filter(pl.col("source_id").cast(pl.String) == str(source_id))
        selected_limit = min(limit or self.config.max_downloads_per_job, self.config.max_downloads_per_job)
        source_counts: dict[str, int] = {}
        selected_rows: list[dict[str, Any]] = []
        for row in pending.to_dicts():
            source_key = str(row.get("source_id") or "__unknown__")
            if source_counts.get(source_key, 0) >= self.config.max_downloads_per_source:
                continue
            source_counts[source_key] = source_counts.get(source_key, 0) + 1
            selected_rows.append(row)
            if len(selected_rows) >= selected_limit:
                break
        selected = pl.DataFrame(selected_rows, schema=PDF_ATTACHMENT_SCHEMA) if selected_rows else pl.DataFrame(schema=PDF_ATTACHMENT_SCHEMA)
        fetcher = self._get_fetcher()
        downloaded = failed = quarantined = 0
        for current in selected.to_dicts():
            attachment_id = str(current["attachment_id"])
            url = str(current.get("discovered_url") or "")
            now = _now()
            current["last_attempt_at"] = now
            try:
                try:
                    result = fetcher.fetch(url, referer=current.get("discovery_page_url"))
                except TypeError:
                    result = fetcher.fetch(url)
                body = bytes(result.body or b"")
                content_type = str(result.content_type or "").lower()
                network_route = str(getattr(result, "network_route", "direct") or "direct").lower()
                if network_route not in {"direct", "injected_client", "unknown"}:
                    raise RuntimeError("PROXY_ROUTE_BLOCKED")
                if int(result.status_code or 0) != 200:
                    raise RuntimeError(f"HTTP_{result.status_code}")
                if len(body) > self.config.max_file_size_mb * 1024 * 1024:
                    raise RuntimeError("PDF_FILE_TOO_LARGE")
                if not body.lstrip().startswith(PDF_MAGIC):
                    quarantine_path, digest = self._quarantine_bytes(body, attachment_id)
                    current.update({"download_status": "QUARANTINE_REVIEW", "last_error": "CONTENT_IS_NOT_PDF", "content_type": content_type, "content_length": len(body), "http_status": result.status_code, "resolved_url": result.final_url, "redirect_chain_json": _json(getattr(result, "redirect_chain", [])), "evidence_json": _json({"quarantine_path": quarantine_path, "sha256": digest})})
                    quarantined += 1
                    self._record_event(run_id=run_id, stage="download", status="QUARANTINE_REVIEW", reason_code="content_not_pdf", attachment_id=attachment_id, evidence={"url": url, "quarantine_path": quarantine_path})
                else:
                    digest = content_sha256(body)
                    target = self._archive_path(digest)
                    target.parent.mkdir(parents=True, exist_ok=True)
                    if target.exists() and _sha256_file(target) != digest:
                        raise RuntimeError("ARCHIVE_CONFLICT")
                    if not target.exists():
                        part = target.with_suffix(".pdf.part")
                        part.write_bytes(body)
                        if _sha256_file(part) != digest:
                            part.unlink(missing_ok=True)
                            raise RuntimeError("PDF_HASH_MISMATCH")
                        os.replace(part, target)
                    inspection = _inspect_pdf(target, threshold_per_page=self.config.scan_text_threshold_per_page)
                    if not inspection["is_valid_pdf"]:
                        raise RuntimeError("INVALID_PDF")
                    asset_id = stable_id(digest, prefix="PDFASSET")
                    _upsert_rows(self._curated("pdf_assets"), [{"pdf_asset_id": asset_id, "sha256": digest, "archive_path": self._relative_data_path(target), "original_paths": "[]", "file_size": len(body), "page_count": inspection["page_count"], "valid_pdf": True, "is_scanned": inspection["is_scanned_candidate"], "parser_name": None, "parser_version": None, "text_char_count": inspection["text_char_count"], "created_at": now, "updated_at": now}], PDF_ASSET_SCHEMA, "pdf_asset_id", run_id=run_id)
                    current.update({"pdf_asset_id": asset_id, "download_status": "DOWNLOADED", "parse_status": "OCR_PENDING" if inspection["is_scanned_candidate"] else "PENDING_PARSE", "content_type": content_type or "application/pdf", "content_length": len(body), "http_status": result.status_code, "resolved_url": result.final_url, "redirect_chain_json": _json(getattr(result, "redirect_chain", [])), "downloaded_at": now, "last_error": None})
                    downloaded += 1
                    self._record_event(run_id=run_id, stage="download", status="DOWNLOADED", reason_code="sha256_verified", attachment_id=attachment_id, pdf_asset_id=asset_id, evidence={"archive_path": self._relative_data_path(target), "content_type": content_type})
                _upsert_rows(path, [current], PDF_ATTACHMENT_SCHEMA, "attachment_id", run_id=run_id)
                _upsert_rows(self._curated("pdf_download_audit"), [{"download_id": stable_id(run_id, attachment_id, now, prefix="PDFGET"), "attachment_id": attachment_id, "run_id": run_id, "requested_url": url, "resolved_url": getattr(result, "final_url", None), "status": current["download_status"], "http_status": getattr(result, "status_code", None), "content_type": getattr(result, "content_type", None), "content_length": len(getattr(result, "body", b"") or b""), "sha256": _sha256_bytes(bytes(getattr(result, "body", b"") or b"")) if getattr(result, "body", None) else None, "network_route": getattr(result, "network_route", "direct"), "redirect_chain_json": _json(getattr(result, "redirect_chain", [])), "error_type": None, "error_message": current.get("last_error"), "attempted_at": now}], PDF_DOWNLOAD_SCHEMA, "download_id", run_id=run_id)
            except Exception as exc:
                failed += 1
                current.update({"download_status": "RETRY_WAIT", "last_error": f"{type(exc).__name__}: {str(exc)[:500]}"})
                _upsert_rows(path, [current], PDF_ATTACHMENT_SCHEMA, "attachment_id", run_id=run_id)
                _upsert_rows(self._curated("pdf_download_audit"), [{"download_id": stable_id(run_id, attachment_id, now, prefix="PDFGET"), "attachment_id": attachment_id, "run_id": run_id, "requested_url": url, "resolved_url": None, "status": "RETRY_WAIT", "http_status": None, "content_type": None, "content_length": None, "sha256": None, "network_route": "direct", "redirect_chain_json": "[]", "error_type": type(exc).__name__, "error_message": str(exc)[:500], "attempted_at": now}], PDF_DOWNLOAD_SCHEMA, "download_id", run_id=run_id)
                self._record_event(run_id=run_id, stage="download", status="RETRY_WAIT", reason_code=type(exc).__name__, attachment_id=attachment_id, evidence={"url": url, "error": str(exc)[:500]})
        self._sync_policy_files(run_id=run_id)
        return {"run_id": run_id, "selected": selected.height, "downloaded": downloaded, "failed": failed, "quarantined": quarantined, "status": "COMPLETED"}

    def parse(self, *, limit: int | None = None, city_id: str | None = None, source_id: str | None = None, run_id: str | None = None) -> dict[str, Any]:
        run_id = run_id or f"PDFPARSE_{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}"
        assets = _read_table(self._curated("pdf_assets"), PDF_ASSET_SCHEMA)
        attachments = _read_table(self._curated("document_attachments"), PDF_ATTACHMENT_SCHEMA)
        if city_id:
            ids = attachments.filter(pl.col("city_id").cast(pl.String) == str(city_id)).get_column("pdf_asset_id").drop_nulls().unique().to_list()
            assets = assets.filter(pl.col("pdf_asset_id").is_in(ids))
        if source_id:
            ids = attachments.filter(pl.col("source_id").cast(pl.String) == str(source_id)).get_column("pdf_asset_id").drop_nulls().unique().to_list()
            assets = assets.filter(pl.col("pdf_asset_id").is_in(ids))
        selected = assets.head(limit or assets.height)
        text_rows: list[dict[str, Any]] = []
        parsed = scanned = failed = 0
        for asset in selected.to_dicts():
            asset_id = str(asset["pdf_asset_id"])
            archive = self.settings.data_root / str(asset["archive_path"])
            now = _now()
            try:
                if not archive.resolve().is_relative_to(self.archive_root.resolve()) or not archive.is_file():
                    raise FileNotFoundError(str(archive))
                with fitz.open(str(archive)) as document:
                    pages = []
                    for number, page in enumerate(document, start=1):
                        raw = page.get_text() or ""
                        normal = _normalise_text(raw)
                        pages.append({"page_number": number, "raw_page_text": raw, "normalized_page_text": normal, "text_hash": normalized_text_hash(normal)})
                    full_text = "\n\n".join(item["normalized_page_text"] for item in pages if item["normalized_page_text"])
                    is_scanned = len(full_text) < max(100, len(pages) * self.config.scan_text_threshold_per_page)
                    parser_name = "PyMuPDF"
                    parser_version = str(fitz.VersionBind)
                text_rows.extend({"text_version_id": stable_id(asset_id, item["page_number"], parser_version, prefix="PDFTEXT"), "pdf_asset_id": asset_id, "page_number": item["page_number"], "raw_page_text": item["raw_page_text"], "normalized_page_text": item["normalized_page_text"], "text_hash": item["text_hash"], "parser_name": parser_name, "parser_version": parser_version, "parsed_at": now} for item in pages)
                text_json = {"pdf_asset_id": asset_id, "sha256": asset["sha256"], "parser_name": parser_name, "parser_version": parser_version, "page_count": len(pages), "text_char_count": len(full_text), "is_scanned": is_scanned, "ocr_status": "DISABLED", "pages": pages, "full_text": full_text, "parsed_at": now}
                _atomic_json(self.derived_text_root / f"{asset['sha256']}.json", text_json)
                asset.update({"page_count": len(pages), "is_scanned": is_scanned, "parser_name": parser_name, "parser_version": parser_version, "text_char_count": len(full_text), "updated_at": now})
                _upsert_rows(self._curated("pdf_assets"), [asset], PDF_ASSET_SCHEMA, "pdf_asset_id", run_id=run_id)
                if text_rows:
                    _upsert_rows(self._curated("pdf_text_versions"), [row for row in text_rows if row["pdf_asset_id"] == asset_id], PDF_TEXT_SCHEMA, "text_version_id", run_id=run_id)
                if is_scanned:
                    scanned += 1
                else:
                    parsed += 1
                current_attachments = attachments.filter(pl.col("pdf_asset_id") == asset_id).to_dicts() if attachments.height else []
                for row in current_attachments:
                    row.update({"parse_status": "OCR_PENDING" if is_scanned else "PARSED", "ocr_status": "DISABLED", "parsed_at": now, "last_error": None})
                    _upsert_rows(self._curated("document_attachments"), [row], PDF_ATTACHMENT_SCHEMA, "attachment_id", run_id=run_id)
                self._record_event(run_id=run_id, stage="parse", status="OCR_PENDING" if is_scanned else "PARSED", reason_code="scanned_candidate" if is_scanned else "text_extracted", pdf_asset_id=asset_id, evidence={"page_count": len(pages), "text_char_count": len(full_text)})
            except Exception as exc:
                failed += 1
                current_attachments = attachments.filter(pl.col("pdf_asset_id") == asset_id).to_dicts() if attachments.height else []
                for row in current_attachments:
                    row.update({"parse_status": "PARSE_FAILED", "ocr_status": "DISABLED", "last_error": f"{type(exc).__name__}: {str(exc)[:500]}"})
                    _upsert_rows(self._curated("document_attachments"), [row], PDF_ATTACHMENT_SCHEMA, "attachment_id", run_id=run_id)
                self._record_event(run_id=run_id, stage="parse", status="PARSE_FAILED", reason_code=type(exc).__name__, pdf_asset_id=asset_id, evidence={"error": str(exc)[:500]})
        return {"run_id": run_id, "selected": selected.height, "parsed": parsed, "ocr_pending": scanned, "failed": failed, "status": "COMPLETED"}

    def match(self, *, limit: int | None = None, run_id: str | None = None) -> dict[str, Any]:
        run_id = run_id or f"PDFMATCH_{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}"
        unmatched = self._ensure_unmatched_assets(run_id=run_id, limit=limit)
        attachments = _read_table(self._curated("document_attachments"), PDF_ATTACHMENT_SCHEMA)
        linked = int(attachments.filter(pl.col("document_id").is_not_null() & ~pl.col("download_status").eq("UNMATCHED_EXISTING_PDF")).height) if attachments.height else 0
        human = int(attachments.filter(pl.col("review_status") == "HUMAN_REVIEW").height) if attachments.height else 0
        self._record_event(run_id=run_id, stage="match", status="COMPLETED", reason_code="deterministic_links_and_review_queue", evidence={"linked": linked, "unmatched_new": unmatched, "human_review": human})
        return {"run_id": run_id, "linked": linked, "unmatched_existing_added": unmatched, "human_review": human, "status": "COMPLETED"}

    def summary(self) -> dict[str, Any]:
        assets = _read_table(self._curated("pdf_assets"), PDF_ASSET_SCHEMA)
        attachments = _read_table(self._curated("document_attachments"), PDF_ATTACHMENT_SCHEMA)
        inventory = _read_table(self._manifest("existing_pdf_inventory"), PDF_INVENTORY_SCHEMA)
        pdf_candidate_expr = (
            pl.col("pdf_asset_id").is_not_null()
            | pl.col("content_type").cast(pl.String).str.to_lowercase().str.contains("pdf")
            | pl.col("filename").cast(pl.String).str.to_lowercase().str.ends_with(".pdf")
            | pl.col("discovered_url").cast(pl.String).str.to_lowercase().str.split("?").list.first().str.ends_with(".pdf")
        )
        pdf_attachments = attachments.filter(pdf_candidate_expr) if attachments.height else attachments
        remote_pdf_attachments = attachments.filter(pl.col("discovered_url").is_not_null() & pdf_candidate_expr) if attachments.height else attachments
        valid_assets = assets.filter(pl.col("valid_pdf")) if assets.height else assets
        def count(expr: pl.Expr, frame: pl.DataFrame = pdf_attachments) -> int:
            return int(frame.filter(expr).height) if frame.height else 0
        downloaded = count(pl.col("download_status") == "DOWNLOADED", remote_pdf_attachments)
        archived_existing = count(pl.col("download_status") == "ARCHIVED_EXISTING")
        parsed = int(valid_assets.filter(pl.col("parser_name").is_not_null()).height) if valid_assets.height else 0
        parse_eligible = valid_assets.height
        failures = count(pl.col("download_status").is_in(["RETRY_WAIT", "FAILED", "QUARANTINE_REVIEW"]) | pl.col("parse_status").is_in(["PARSE_FAILED"]))
        linked = count(pl.col("document_id").is_not_null() & ~pl.col("download_status").eq("UNMATCHED_EXISTING_PDF"))
        primary = count(pl.col("is_primary_content") == True)  # noqa: E712
        both = count(pl.col("document_id").is_not_null() & (pl.col("content_source") == "HTML+PDF"))
        return {
            "updated_at": _now(),
            "pdf_assets": assets.height,
            "valid_pdf_assets": valid_assets.height,
            "inventory_files": inventory.height,
            "invalid_pdf": int(inventory.filter(~pl.col("is_valid_pdf")).height) if inventory.height else 0,
            "duplicate_pdf_files": int(inventory.height - inventory.get_column("sha256").n_unique()) if inventory.height else 0,
            "pdf_attachments": pdf_attachments.height,
            "linked_policy_pdf": linked,
            "primary_pdf": primary,
            "html_pdf_both": both,
            "archived_existing": archived_existing,
            "pending_archive": count(pl.col("download_status") == "PENDING_ARCHIVE"),
            "downloaded": downloaded,
            "parsed": parsed,
            "ocr_pending": count(pl.col("parse_status") == "OCR_PENDING"),
            "download_failures": failures,
            "unmatched_existing_pdf": count(pl.col("download_status") == "UNMATCHED_EXISTING_PDF"),
            "pdf_discovery_coverage": {
                "numerator": linked,
                "denominator": pdf_attachments.height,
                "percent": linked / pdf_attachments.height if pdf_attachments.height else None,
                "definition": "PDF attachment records with a deterministic document_id association / identified PDF attachment records",
            },
            "pdf_download_rate": {
                "numerator": downloaded,
                "denominator": remote_pdf_attachments.height,
                "percent": downloaded / remote_pdf_attachments.height if remote_pdf_attachments.height else None,
                "definition": "successful direct PDF downloads / remote PDF attachment records",
            },
            "pdf_parse_rate": {
                "numerator": parsed,
                "denominator": parse_eligible,
                "percent": parsed / parse_eligible if parse_eligible else None,
                "definition": "valid PDF assets with a PyMuPDF text version / valid archived PDF assets",
            },
            "ocr_enabled": False,
            "status": "READY",
        }

    def report(self, *, output: Path | None = None) -> dict[str, Any]:
        result = self.summary()
        target = output or self.settings.outputs / "pdf_pipeline" / "pdf_report.json"
        _atomic_json(target, result)
        return {**result, "report_path": str(target)}

    def run(self, *, limit: int | None = None, city_id: str | None = None, source_id: str | None = None, run_id: str | None = None) -> dict[str, Any]:
        run_id = run_id or f"PDFRUN_{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}"
        inventory = self.inventory(run_id=run_id)
        archive = self.archive(limit=limit, run_id=run_id)
        discover = self.discover(limit=limit, city_id=city_id, source_id=source_id, run_id=run_id)
        match = self.match(limit=limit, run_id=run_id)
        download = self.download(limit=limit, city_id=city_id, source_id=source_id, run_id=run_id)
        parse = self.parse(limit=limit, city_id=city_id, source_id=source_id, run_id=run_id)
        return {"run_id": run_id, "inventory": inventory, "archive": archive, "discover": discover, "match": match, "download": download, "parse": parse, "summary": self.summary(), "status": "COMPLETED"}


def safe_pdf_asset_path(settings: Settings, pdf_asset_id: str) -> Path | None:
    pipeline = PDFPipeline(settings, initialize_storage=False)
    assets = _read_table(pipeline._curated("pdf_assets"), PDF_ASSET_SCHEMA)
    selected = assets.filter(pl.col("pdf_asset_id") == str(pdf_asset_id)) if assets.height else assets
    if selected.is_empty():
        return None
    path = (settings.data_root / str(selected[0, "archive_path"])).resolve()
    try:
        path.relative_to(pipeline.archive_root.resolve())
    except ValueError:
        return None
    return path if path.is_file() else None


def pdf_attachments_for_record(settings: Settings, record_id: str) -> pl.DataFrame:
    pipeline = PDFPipeline(settings, initialize_storage=False)
    attachments = _read_table(pipeline._curated("document_attachments"), PDF_ATTACHMENT_SCHEMA)
    if attachments.is_empty():
        return attachments
    return attachments.filter(pl.col("document_id").cast(pl.String) == str(record_id))


def pdf_text_json(settings: Settings, pdf_asset_id: str) -> dict[str, Any] | None:
    pipeline = PDFPipeline(settings, initialize_storage=False)
    assets = _read_table(pipeline._curated("pdf_assets"), PDF_ASSET_SCHEMA)
    selected = assets.filter(pl.col("pdf_asset_id") == str(pdf_asset_id)) if assets.height else assets
    if selected.is_empty():
        return None
    path = pipeline.derived_text_root / f"{selected[0, 'sha256']}.json"
    if not path.exists():
        return None
    try:
        value = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError):
        return None
    return value if isinstance(value, dict) else None


__all__ = [
    "PDF_ATTACHMENT_ROLES",
    "PDFPipeline",
    "PDFPipelineConfig",
    "load_pdf_config",
    "pdf_attachments_for_record",
    "pdf_text_json",
    "safe_pdf_asset_path",
]
