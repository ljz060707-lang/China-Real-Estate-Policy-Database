from __future__ import annotations

import os
from pathlib import Path

import polars as pl


def atomic_write_parquet(frame: pl.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    frame.write_parquet(temporary)
    os.replace(temporary, path)


def upsert_parquet(frame: pl.DataFrame, path: Path, key: str) -> None:
    if path.exists():
        existing = pl.read_parquet(path)
        frame = pl.concat([existing, frame], how="diagonal_relaxed")
    atomic_write_parquet(frame.unique(key, keep="last", maintain_order=True), path)

