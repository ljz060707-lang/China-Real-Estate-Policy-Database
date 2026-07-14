from __future__ import annotations

from pathlib import Path

import polars as pl

CRAWL_SCHEMAS = {
    "crawl_runs": {
        "run_id": pl.String,
        "run_type": pl.String,
        "scope_id": pl.String,
        "period_start": pl.String,
        "period_end": pl.String,
        "status": pl.String,
        "source_count": pl.Int64,
        "item_count": pl.Int64,
        "fetched_count": pl.Int64,
        "failed_count": pl.Int64,
        "started_at": pl.String,
        "finished_at": pl.String,
        "created_at": pl.String,
        "updated_at": pl.String,
    },
    "crawl_items": {
        "item_id": pl.String,
        "run_id": pl.String,
        "source_id": pl.String,
        "url": pl.String,
        "canonical_url": pl.String,
        "status": pl.String,
        "city_id": pl.String,
        "query_year": pl.Int64,
        "keyword_group": pl.String,
        "retry_count": pl.Int64,
        "first_seen_at": pl.String,
        "last_seen_at": pl.String,
        "created_at": pl.String,
        "updated_at": pl.String,
    },
    "crawl_checkpoints": {
        "checkpoint_id": pl.String,
        "run_id": pl.String,
        "last_item_id": pl.String,
        "status": pl.String,
        "processed_count": pl.Int64,
        "created_at": pl.String,
        "updated_at": pl.String,
    },
    "fetch_errors": {
        "error_id": pl.String,
        "run_id": pl.String,
        "item_id": pl.String,
        "source_id": pl.String,
        "url": pl.String,
        "error_type": pl.String,
        "error_message": pl.String,
        "retryable": pl.Boolean,
        "created_at": pl.String,
        "updated_at": pl.String,
    },
    "policy_document_versions": {
        "document_version_id": pl.String,
        "record_id": pl.String,
        "crawl_item_id": pl.String,
        "source_id": pl.String,
        "canonical_url": pl.String,
        "final_url": pl.String,
        "content_sha256": pl.String,
        "local_path": pl.String,
        "content_type": pl.String,
        "http_status": pl.Int64,
        "title": pl.String,
        "extracted_text": pl.String,
        "parse_status": pl.String,
        "is_material_change": pl.Boolean,
        "first_seen_at": pl.String,
        "last_seen_at": pl.String,
        "created_at": pl.String,
        "updated_at": pl.String,
    },
    "llm_extractions": {
        "extraction_id": pl.String,
        "content_sha256": pl.String,
        "model_name": pl.String,
        "prompt_version": pl.String,
        "schema_version": pl.String,
        "status": pl.String,
        "output_json": pl.String,
        "confidence": pl.Float64,
        "needs_review": pl.Boolean,
        "error_type": pl.String,
        "called_at": pl.String,
        "created_at": pl.String,
        "updated_at": pl.String,
    },
}


def ensure_crawl_storage(curated: Path) -> None:
    curated.mkdir(parents=True, exist_ok=True)
    for name, schema in CRAWL_SCHEMAS.items():
        path = curated / f"{name}.parquet"
        if not path.exists():
            pl.DataFrame(schema=schema).write_parquet(path, compression="zstd")


def append_unique(path: Path, rows: list[dict], key: str) -> pl.DataFrame:
    incoming = pl.DataFrame(rows, infer_schema_length=None)
    if path.exists():
        current = pl.read_parquet(path)
        frame = pl.concat([current, incoming], how="diagonal_relaxed")
    else:
        frame = incoming
    frame = frame.unique(subset=[key], keep="last", maintain_order=True)
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.write_parquet(path, compression="zstd")
    return frame
