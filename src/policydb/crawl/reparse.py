"""Versioned, offline extraction repair. Never fetch a URL or mutate a raw asset."""
from __future__ import annotations

import argparse
import json
import os
import shutil
from datetime import UTC, datetime
from pathlib import Path

import polars as pl
import psutil

from policydb.archive import file_sha256
from policydb.crawl.dedup import content_sha256, normalized_text_hash, simhash64
from policydb.crawl.parser import PARSER_VERSION, parse_document
from policydb.parquet_store import atomic_write_parquet, read_parquet_snapshot
from policydb.settings import Settings


def resolve_raw_path(settings: Settings, row: dict) -> Path | None:
    """Resolve migrated assets without altering their original recorded provenance."""
    original = Path(str(row.get("local_path") or ""))
    candidates = [original if original.is_absolute() else settings.root / original]
    text = str(original).replace("\\", "/")
    for marker, base in (("/CRPD/archive/", settings.archive_root), ("/CRPD/raw/", settings.raw)):
        if marker in text:
            candidates.append(base / text.split(marker, 1)[1])
    digest = str(row.get("content_sha256") or "")
    if len(digest) == 64 and all(c in "0123456789abcdef" for c in digest):
        for folder in ("attachments", "html", "pdf"):
            directory = settings.archive_root / folder / digest[:2]
            if directory.exists():
                candidates.extend(directory.glob(f"{digest}.*"))
    return next((path for path in candidates if path.is_file()), None)


def binary_text_suspect(text: str) -> bool:
    return text.startswith(("Rar!", "PK\x03\x04", "\ufffd\ufffd\x11", "MZ\x00")) or "\0" in text[:4096]


def reparse_archived_versions(settings: Settings, output: Path, *, apply: bool = False, limit: int | None = None) -> dict:
    """Repair proven binary-text corruption only. The previous snapshot is retained.

    The maintenance launcher owns the production single-writer lock when applying.
    Audit-only runs are safe while production writes new snapshots.
    """
    output.mkdir(parents=True, exist_ok=False)
    path = settings.curated / "policy_document_versions.parquet"
    original_hash = file_sha256(path)
    frame = read_parquet_snapshot(path)
    # Materialize a consistent snapshot; detect any competing mutation before apply.
    if file_sha256(path) != original_hash:
        raise RuntimeError("SOURCE_SNAPSHOT_CHANGED_RETRY_AT_SAFE_BOUNDARY")
    candidates = frame.filter(pl.col("extracted_text").fill_null("").str.slice(0, 4096).map_elements(binary_text_suspect, return_dtype=pl.Boolean))
    if limit is not None:
        candidates = candidates.head(limit)
    patches, audit = [], []
    for row in candidates.iter_rows(named=True):
        raw = resolve_raw_path(settings, row)
        evidence = {"document_version_id": row["document_version_id"], "old_parser_version": row.get("parser_version"), "new_parser_version": PARSER_VERSION, "old_text_chars": len(row.get("extracted_text") or ""), "content_sha256": row.get("content_sha256"), "resolved_raw_path": str(raw) if raw else None, "status": "RAW_MISSING"}
        if raw:
            body = raw.read_bytes()
            if content_sha256(body) != row.get("content_sha256"):
                evidence["status"] = "RAW_HASH_MISMATCH"
            else:
                parsed = parse_document(body, row.get("content_type"), row.get("final_url") or row.get("canonical_url"))
                text = parsed["full_text"]
                evidence.update(status="REPARSED", parse_status=parsed["parse_status"], reason=parsed.get("parser_error"), new_text_chars=len(text), new_text_sha256=content_sha256(text))
                if parsed["document_type"] == "html":
                    evidence["status"] = "NEEDS_REVIEW_NOT_PROVEN_BINARY"
                else:
                    patches.append({"document_version_id": row["document_version_id"], "extracted_text": text, "parse_status": parsed["parse_status"], "parser_version": PARSER_VERSION, "normalized_text_hash": normalized_text_hash(text) if text else None, "simhash64": simhash64(text) if text else None})
        audit.append(evidence)
    audit_frame = pl.DataFrame(audit, infer_schema_length=None) if audit else pl.DataFrame(schema={"document_version_id": pl.String, "status": pl.String})
    atomic_write_parquet(audit_frame, output / "extraction_audit.parquet", key_columns=("document_version_id",))
    patch_frame = pl.DataFrame(patches, infer_schema_length=None) if patches else pl.DataFrame(schema={"document_version_id": pl.String})
    atomic_write_parquet(patch_frame, output / "extraction_patches.parquet", key_columns=("document_version_id",))
    summary = {"created_at": datetime.now(UTC).isoformat(), "mode": "APPLY" if apply else "PREVIEW", "parser_version": PARSER_VERSION, "source_snapshot_sha256": original_hash, "documents_before": frame.height, "suspects": candidates.height, "repairable": len(patches), "unresolved": len(audit) - len(patches), "raw_mutated": False, "network_calls": 0, "old_text_chars": sum(r["old_text_chars"] for r in audit), "replacement_text_chars": sum(r.get("new_text_chars", 0) for r in audit), "applied": False}
    if apply and patches:
        if file_sha256(path) != original_hash:
            raise RuntimeError("WRITER_CONFLICT_SOURCE_CHANGED")
        backup = output / "policy_document_versions.before.parquet"
        shutil.copy2(path, backup)
        if file_sha256(backup) != original_hash:
            raise RuntimeError("BACKUP_HASH_MISMATCH")
        changed = frame.filter(pl.col("document_version_id").is_in([r["document_version_id"] for r in patches]))
        atomic_write_parquet(changed, output / "prior_extractions.parquet", key_columns=("document_version_id",))
        fields = [c for c in patch_frame.columns if c != "document_version_id"]
        joined = frame.join(patch_frame.rename({c: f"__new_{c}" for c in fields}).with_columns(pl.lit(True).alias("__repaired")), on="document_version_id", how="left")
        repaired = joined.with_columns([pl.when(pl.col("__repaired").fill_null(False)).then(pl.col(f"__new_{c}")).otherwise(pl.col(c)).alias(c) for c in fields]).select(frame.columns)
        if repaired.height != frame.height or repaired["document_version_id"].n_unique() != frame.height:
            raise RuntimeError("REPAIR_IDENTITY_GATE_FAILED")
        for column in set(frame.columns) - set(fields):
            if not frame[column].equals(repaired[column]):
                raise RuntimeError(f"REPAIR_CHANGED_IDENTITY_OR_HISTORY:{column}")
        atomic_write_parquet(repaired, path, {"module": "crawl.reparse", "audit_directory": str(output)}, key_columns=("document_version_id",))
        summary.update(applied=True, documents_after=repaired.height, resulting_snapshot_sha256=file_sha256(path), backup=str(backup), bytes_before=backup.stat().st_size, bytes_after=path.stat().st_size)
    (output / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return summary


def assert_maintenance_owner() -> None:
    owner = int(os.environ.get("CRPD_MAINTENANCE_OWNER_PID", "0"))
    ancestors = psutil.Process().parents()
    if not owner or owner not in {p.pid for p in ancestors}:
        raise RuntimeError("APPLY_REQUIRES_SINGLE_WRITER_MAINTENANCE_LAUNCHER")
    parent = psutil.Process(owner)
    if "apply_crawl_quality_v3_when_idle.ps1" not in " ".join(parent.cmdline()):
        raise RuntimeError("UNRECOGNIZED_MAINTENANCE_OWNER")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--limit", type=int)
    args = parser.parse_args()
    if args.apply:
        assert_maintenance_owner()
    print(json.dumps(reparse_archived_versions(Settings.discover(args.root), args.output, apply=args.apply, limit=args.limit), ensure_ascii=True))


if __name__ == "__main__":
    main()
