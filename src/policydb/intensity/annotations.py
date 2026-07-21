from __future__ import annotations

from datetime import UTC, datetime

import polars as pl

from policydb.intensity.rules import split_clauses
from policydb.intensity.storage import atomic_write_parquet
from policydb.settings import Settings


def prepare_annotations(
    settings: Settings | None = None,
    *,
    document_count: int = 500,
    clause_count: int = 3000,
) -> dict:
    settings = settings or Settings.discover()
    root = settings.root / "data" / "annotations" / "policy_intensity"
    root.mkdir(parents=True, exist_ok=True)
    records = pl.read_parquet(settings.curated / "records.parquet").filter(
        pl.col("full_text").is_not_null() & pl.col("official_status").is_in(["official", "official_reprint"])
    ).sort(["record_date", "record_id"])
    sample = records.head(document_count).select(
        "record_id", "title", "record_date", "official_status", "source_sheet", "source_row", "full_text"
    ).with_columns(
        pl.lit(None, dtype=pl.String).alias("annotator_id"),
        pl.lit(None, dtype=pl.String).alias("annotation_status"),
        pl.lit(None, dtype=pl.String).alias("document_family_id"),
        pl.lit(None, dtype=pl.String).alias("notes"),
    )
    atomic_write_parquet(sample, root / "document_sample.parquet")
    clauses = []
    for row in sample.iter_rows(named=True):
        for clause in split_clauses(row["full_text"] or "", record_id=row["record_id"]):
            clauses.append(
                {
                    "clause_id": clause.clause_id,
                    "record_id": row["record_id"],
                    "clause_text": clause.text,
                    "evidence_start": clause.start,
                    "evidence_end": clause.end,
                    "is_policy_action": None,
                    "instrument_labels": None,
                    "direction": None,
                    "D1": None,
                    "D2": None,
                    "D3": None,
                    "D4": None,
                    "D5": None,
                    "D6": None,
                    "D7": None,
                    "annotator_id": None,
                    "annotation_status": "unlabeled",
                }
            )
            if len(clauses) >= clause_count:
                break
        if len(clauses) >= clause_count:
            break
    clause_frame = pl.DataFrame(clauses, infer_schema_length=None) if clauses else pl.DataFrame()
    if not clause_frame.is_empty():
        atomic_write_parquet(clause_frame, root / "clause_sample.parquet")
        empty = clause_frame.head(0)
        for name in ("double_coded", "adjudicated_gold", "train", "validation", "test", "city_holdout", "time_holdout"):
            atomic_write_parquet(empty, root / f"{name}.parquet")
    metrics = {
        "generated_at": datetime.now(UTC).isoformat(),
        "document_sample": sample.height,
        "clause_sample": len(clauses),
        "double_coded": 0,
        "adjudicated_gold": 0,
        "formal_requirement_documents": 500,
        "formal_requirement_clauses": 3000,
        "formal_requirement_double_coded_share": 0.20,
        "status": "awaiting_human_annotation",
        "research_ready": False,
    }
    return metrics

