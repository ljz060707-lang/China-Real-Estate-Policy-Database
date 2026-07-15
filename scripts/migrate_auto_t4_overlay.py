from __future__ import annotations

from datetime import UTC, datetime

import duckdb
import polars as pl

from policydb.query.database import build_database
from policydb.settings import Settings
from policydb.transform.t4_matching import build_t4_match_candidates


def main() -> None:
    settings = Settings.discover()
    with duckdb.connect(str(settings.database), read_only=True) as con:
        task_cells = {
            row[0]
            for row in con.execute(
                """SELECT DISTINCT source_cell FROM manual_review_tasks
                   WHERE review_type='unmatched_t4'
                     AND field_name LIKE 'policy_features.%'"""
            ).fetchall()
            if row[0]
        }
    path = settings.curated / "policy_features.parquet"
    features = pl.read_parquet(path)
    automated = features.filter(
        pl.col("source_cell").is_in(task_cells) & pl.col("record_id").is_not_null()
    )
    now = datetime.now(UTC).isoformat()
    if automated.height:
        overlay = automated.select("source_cell", "record_id").with_columns(
            pl.col("source_cell").str.extract(r"(\d+)", 1).cast(pl.Int64).alias("source_row"),
            pl.lit("deterministic_auto_match").alias("classification_source"),
            pl.lit(0.9).alias("confidence"),
            pl.lit(now).alias("created_at"),
            pl.lit(now).alias("updated_at"),
        )
        overlay_path = settings.curated / "auto_t4_links.parquet"
        if overlay_path.exists():
            overlay = pl.concat(
                [pl.read_parquet(overlay_path), overlay], how="diagonal_relaxed"
            )
        overlay.unique("source_cell", keep="last").write_parquet(
            overlay_path, compression="zstd"
        )
    features.with_columns(
        pl.when(pl.col("source_cell").is_in(task_cells))
        .then(None)
        .otherwise(pl.col("record_id"))
        .alias("record_id")
    ).write_parquet(path, compression="zstd")
    result = build_t4_match_candidates(settings)
    build_database(settings)
    print({"overlay_cells": automated.height, **result})


if __name__ == "__main__":
    main()
