from __future__ import annotations

import json

import polars as pl

from policydb.source_slots import list_candidates


def unique_count(frame: pl.DataFrame, column: str) -> int:
    if frame.is_empty() or column not in frame.columns:
        return 0

    return (
        frame.select(pl.col(column).drop_nulls().n_unique())
        .item()
    )


def true_rows(frame: pl.DataFrame, column: str) -> pl.DataFrame:
    if frame.is_empty() or column not in frame.columns:
        return frame.head(0)

    return frame.filter(
        pl.col(column)
        .fill_null(False)
        .cast(pl.Boolean, strict=False)
    )


frame = list_candidates()

if frame.is_empty():
    result = {
        "total_slots": 525,
        "candidate_records": 0,
        "candidate_slots": 0,
        "probed_candidates": 0,
        "probed_slots": 0,
        "verified_slots": 0,
        "enabled_slots": 0,
        "candidate_cities": 0,
        "verified_cities": 0,
        "enabled_cities": 0,
    }
else:
    probe_expression = pl.lit(False)

    if "health_probe_count" in frame.columns:
        probe_expression = (
            probe_expression
            | (
                pl.col("health_probe_count")
                .fill_null(0)
                .cast(pl.Int64, strict=False)
                > 0
            )
        )

    if "last_checked_at" in frame.columns:
        probe_expression = (
            probe_expression
            | pl.col("last_checked_at").is_not_null()
        )

    probed = frame.filter(probe_expression)
    verified = true_rows(frame, "is_verified")
    enabled = true_rows(frame, "is_enabled")

    result = {
        "total_slots": 525,
        "candidate_records": frame.height,
        "candidate_slots": unique_count(frame, "slot_id"),
        "probed_candidates": probed.height,
        "probed_slots": unique_count(probed, "slot_id"),
        "verified_slots": unique_count(verified, "slot_id"),
        "enabled_slots": unique_count(enabled, "slot_id"),
        "candidate_cities": unique_count(frame, "city_id"),
        "verified_cities": unique_count(verified, "city_id"),
        "enabled_cities": unique_count(enabled, "city_id"),
    }

# ASCII-safe JSON prevents PowerShell decoding problems.
print(json.dumps(result, ensure_ascii=True))
