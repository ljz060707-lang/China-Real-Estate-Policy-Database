"""Create immutable, read-only research snapshots from curated aggregates."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import polars as pl

from policydb.dashboard_metrics import (
    city_role_matrix,
    city_year_coverage,
    document_quality,
    gap_register,
    pdf_summary,
)
from policydb.parquet_store import atomic_write_parquet, read_parquet_snapshot
from policydb.settings import Settings


def _write(frame: pl.DataFrame, path: Path) -> None:
    atomic_write_parquet(frame if frame.width else pl.DataFrame({"_empty": pl.Series([], dtype=pl.String)}), path, {"module": "research_snapshot", "snapshot": path.parent.name})


def create_research_snapshot(settings: Settings | None = None, *, output: Path | None = None) -> dict[str, Any]:
    settings = settings or Settings.discover()
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    target = output or settings.research / f"research_snapshot_{timestamp}"
    if target.exists() and any(target.iterdir()):
        raise FileExistsError(f"research snapshot already exists: {target}")
    target.mkdir(parents=True, exist_ok=True)
    records_path = settings.curated / "records.parquet"
    versions_path = settings.curated / "policy_document_versions.parquet"
    documents = read_parquet_snapshot(records_path) if records_path.exists() else read_parquet_snapshot(versions_path) if versions_path.exists() else pl.DataFrame()
    files: dict[str, str] = {}
    document_path = target / "documents_snapshot.parquet"
    _write(documents, document_path)
    files[document_path.name] = hashlib.sha256(document_path.read_bytes()).hexdigest()
    source_frame = city_role_matrix(settings)
    source_path = target / "source_coverage.parquet"
    _write(source_frame, source_path)
    files[source_path.name] = hashlib.sha256(source_path.read_bytes()).hexdigest()
    city_frame = source_frame.group_by(["city_id", "city_name", "province_name"]).agg(pl.len().alias("required_slots")) if not source_frame.is_empty() else pl.DataFrame()
    city_path = target / "city_coverage.parquet"
    _write(city_frame, city_path)
    files[city_path.name] = hashlib.sha256(city_path.read_bytes()).hexdigest()
    year_path = target / "city_year_coverage.parquet"
    _write(city_year_coverage(settings), year_path)
    files[year_path.name] = hashlib.sha256(year_path.read_bytes()).hexdigest()
    quality = document_quality(settings)
    quality_path = target / "document_completeness.parquet"
    _write(pl.DataFrame([quality], infer_schema_length=None), quality_path)
    files[quality_path.name] = hashlib.sha256(quality_path.read_bytes()).hexdigest()
    gap_path = target / "gap_register.parquet"
    _write(gap_register(settings), gap_path)
    files[gap_path.name] = hashlib.sha256(gap_path.read_bytes()).hexdigest()
    pdf_summary_payload = pdf_summary(settings)
    pdf_files = {
        "pdf_assets": "pdf_assets_snapshot.parquet",
        "document_attachments": "document_attachments_snapshot.parquet",
        "pdf_text_versions": "pdf_text_versions_snapshot.parquet",
    }
    for table_name, filename in pdf_files.items():
        source = settings.curated / f"{table_name}.parquet"
        frame = read_parquet_snapshot(source) if source.exists() else pl.DataFrame()
        destination = target / filename
        _write(frame, destination)
        files[filename] = hashlib.sha256(destination.read_bytes()).hexdigest()
    pdf_completeness_path = target / "pdf_completeness.json"
    pdf_completeness_path.write_text(json.dumps(pdf_summary_payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    files[pdf_completeness_path.name] = hashlib.sha256(pdf_completeness_path.read_bytes()).hexdigest()
    intensity_path = target / "policy_intensity_snapshot.parquet"
    _write(pl.DataFrame(schema={"record_id": pl.String, "intensity": pl.Float64}), intensity_path)
    files[intensity_path.name] = hashlib.sha256(intensity_path.read_bytes()).hexdigest()
    manifest = {"snapshot_id": target.name, "created_at": datetime.now(UTC).isoformat(), "source": "curated", "policy_intensity_enabled": False, "policy_intensity_rows": 0, "pdf_completeness": pdf_summary_payload, "files": files}
    (target / "snapshot_manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return {"snapshot_id": target.name, "output": str(target), "documents": documents.height, "policy_intensity_enabled": False, "policy_intensity_rows": 0, "manifest": str(target / "snapshot_manifest.json")}


__all__ = ["create_research_snapshot"]
