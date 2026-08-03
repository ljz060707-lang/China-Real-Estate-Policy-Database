from __future__ import annotations

from pathlib import Path

import polars as pl

from policydb.parquet_store import atomic_write_parquet as safe_atomic_write_parquet
from policydb.parquet_store import read_parquet_snapshot


def atomic_write_parquet(frame: pl.DataFrame, path: Path) -> None:
    safe_atomic_write_parquet(frame, path, {"module": "intensity.storage"})


def upsert_parquet(frame: pl.DataFrame, path: Path, key: str) -> None:
    if path.exists():
        existing = read_parquet_snapshot(path)
        frame = pl.concat([existing, frame], how="diagonal_relaxed")
    atomic_write_parquet(frame.unique(key, keep="last", maintain_order=True), path)
