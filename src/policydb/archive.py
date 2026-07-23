from __future__ import annotations

import csv
import hashlib
import json
import os
import re
import shutil
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from policydb.intensity.storage import atomic_write_parquet
from policydb.settings import Settings
from policydb.transform.normalization import stable_id

SHA256 = re.compile(r"^[0-9a-f]{64}$")


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _archive_folder(content_type: str, suffix: str) -> str:
    if suffix == ".pdf" or "pdf" in content_type:
        return "pdf"
    if suffix in {".html", ".htm"} or "html" in content_type:
        return "html"
    if suffix == ".txt" or content_type.startswith("text/plain"):
        return "text"
    return "attachments"


def archive_document_versions(
    settings: Settings | None = None, *, archive_root: Path | None = None
) -> dict:
    settings = settings or Settings.discover()
    archive_root = Path(archive_root or settings.policy_archive_root)
    if not archive_root.exists():
        raise FileNotFoundError(
            f"Policy archive root is unavailable: {archive_root}. "
            "The archive will not fall back to another drive."
        )
    versions_path = settings.curated / "policy_document_versions.parquet"
    versions = pl.read_parquet(versions_path) if versions_path.exists() else pl.DataFrame()
    now = datetime.now(UTC).isoformat()
    rows = []
    for row in versions.iter_rows(named=True):
        local_value = str(row.get("local_path") or "")
        source = Path(local_value)
        if not source.is_absolute():
            source = settings.root / source
        expected = str(row.get("content_sha256") or "").lower()
        status = "missing_source_file"
        actual = None
        relative = None
        size = None
        if source.is_file():
            actual = file_sha256(source)
            if SHA256.fullmatch(expected) and actual != expected:
                status = "hash_mismatch"
            elif not SHA256.fullmatch(expected):
                status = "invalid_expected_hash"
            else:
                suffix = source.suffix.lower() or ".bin"
                folder = _archive_folder(str(row.get("content_type") or ""), suffix)
                relative_path = Path("raw") / folder / actual[:2] / f"{actual}{suffix}"
                target = archive_root / relative_path
                target.parent.mkdir(parents=True, exist_ok=True)
                if not target.exists():
                    temporary = target.with_suffix(target.suffix + ".tmp")
                    shutil.copy2(source, temporary)
                    if file_sha256(temporary) != actual:
                        temporary.unlink(missing_ok=True)
                        raise OSError(f"Archive copy hash validation failed: {source}")
                    os.replace(temporary, target)
                relative = relative_path.as_posix()
                size = target.stat().st_size
                status = "archived"
                metadata = archive_root / "metadata" / actual[:2] / f"{actual}.json"
                metadata.parent.mkdir(parents=True, exist_ok=True)
                if not metadata.exists():
                    temporary = metadata.with_suffix(".json.tmp")
                    temporary.write_text(
                        json.dumps(
                            {
                                "sha256": actual,
                                "archive_path": relative,
                                "source_local_path": local_value,
                                "content_type": row.get("content_type"),
                                "first_archived_at": now,
                            },
                            ensure_ascii=False,
                            indent=2,
                        ),
                        encoding="utf-8",
                    )
                    os.replace(temporary, metadata)
        rows.append(
            {
                "policy_file_id": stable_id(
                    str(row.get("document_version_id")), expected, prefix="FILE"
                ),
                "document_version_id": row.get("document_version_id"),
                "record_id": row.get("record_id"),
                "source_local_path": local_value,
                "archive_relative_path": relative,
                "content_type": row.get("content_type"),
                "sha256_expected": expected or None,
                "sha256_actual": actual,
                "size_bytes": size,
                "archive_status": status,
                "checked_at": now,
            }
        )
    frame = pl.DataFrame(rows, infer_schema_length=None) if rows else pl.DataFrame(
        schema={
            "policy_file_id": pl.String,
            "document_version_id": pl.String,
            "record_id": pl.String,
            "source_local_path": pl.String,
            "archive_relative_path": pl.String,
            "content_type": pl.String,
            "sha256_expected": pl.String,
            "sha256_actual": pl.String,
            "size_bytes": pl.Int64,
            "archive_status": pl.String,
            "checked_at": pl.String,
        }
    )
    atomic_write_parquet(frame, settings.curated / "policy_files.parquet")
    atomic_write_parquet(frame, settings.curated / "archive_integrity_checks.parquet")
    report_dir = settings.root / "outputs/archive"
    report_dir.mkdir(parents=True, exist_ok=True)
    with (report_dir / "archive_coverage_report.csv").open(
        "w", encoding="utf-8-sig", newline=""
    ) as handle:
        writer = csv.DictWriter(handle, fieldnames=frame.columns)
        writer.writeheader()
        writer.writerows(frame.iter_rows(named=True))
    counts = (
        {
            row["archive_status"]: row["len"]
            for row in frame.group_by("archive_status").len().iter_rows(named=True)
        }
        if frame.height
        else {}
    )
    manifest = {
        "created_at": now,
        "archive_root": str(archive_root),
        "document_versions": frame.height,
        "status_counts": counts,
        "hash_verified": counts.get("archived", 0),
    }
    manifest_dir = archive_root / "manifests"
    manifest_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = manifest_dir / "latest.json"
    temporary = manifest_path.with_suffix(".json.tmp")
    temporary.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    os.replace(temporary, manifest_path)
    return manifest
