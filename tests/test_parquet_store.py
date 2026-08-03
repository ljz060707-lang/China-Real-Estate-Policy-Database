from __future__ import annotations

import hashlib
import json
from pathlib import Path

import polars as pl
import pytest

from policydb import parquet_store
from policydb.parquet_store import (
    ParquetLockError,
    ParquetWriteError,
    atomic_write_parquet,
    merge_and_replace_parquet,
    parquet_write_lock,
    read_parquet_snapshot,
)


def _hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_snapshot_read_disables_memory_mapping(monkeypatch, tmp_path: Path) -> None:
    path = tmp_path / "snapshot.parquet"
    pl.DataFrame({"id": ["A"]}).write_parquet(path)
    observed: dict[str, object] = {}
    original = parquet_store.pl.read_parquet

    def wrapped(*args, **kwargs):
        observed.update(kwargs)
        return original(*args, **kwargs)

    monkeypatch.setattr(parquet_store.pl, "read_parquet", wrapped)
    assert read_parquet_snapshot(path)["id"].to_list() == ["A"]
    assert observed["memory_map"] is False


def test_atomic_write_validates_and_releases_lock(tmp_path: Path) -> None:
    path = tmp_path / "nested" / "table.parquet"
    atomic_write_parquet(
        pl.DataFrame({"id": ["A"], "value": [1]}),
        path,
        {"run_id": "R1", "job_id": "J1", "worker_id": "W1"},
        key_columns=("id",),
    )
    assert read_parquet_snapshot(path).height == 1
    assert not Path(f"{path}.lock").exists()
    assert not list(path.parent.glob(".*.tmp.parquet"))


def test_merge_is_serialized_and_idempotent(tmp_path: Path) -> None:
    path = tmp_path / "table.parquet"
    first = merge_and_replace_parquet(path, pl.DataFrame({"id": ["A"], "value": [1]}), ("id",))
    second = merge_and_replace_parquet(path, pl.DataFrame({"id": ["A", "B"], "value": [2, 3]}), ("id",))
    assert first.height == 1
    assert second.sort("id")["value"].to_list() == [2, 3]


def test_live_lock_blocks_second_writer(tmp_path: Path) -> None:
    path = tmp_path / "table.parquet"
    with parquet_write_lock(path, {"run_id": "R1"}):
        with pytest.raises(ParquetLockError):
            with parquet_write_lock(path, {"run_id": "R2"}):
                pass


def test_1224_is_retried_without_losing_previous_file(monkeypatch, tmp_path: Path) -> None:
    path = tmp_path / "table.parquet"
    atomic_write_parquet(pl.DataFrame({"id": ["old"]}), path, {"run_id": "OLD"})
    original_hash = _hash(path)
    original_replace = parquet_store.os.replace
    calls = {"count": 0}

    def flaky_replace(source, destination):
        if str(destination) == str(path) and calls["count"] < 2:
            calls["count"] += 1
            raise OSError(1224, "mapped section")
        return original_replace(source, destination)

    monkeypatch.setattr(parquet_store.ParquetWriteLease, "heartbeat", lambda self: self.payload)
    monkeypatch.setattr(parquet_store.os, "replace", flaky_replace)
    atomic_write_parquet(pl.DataFrame({"id": ["new"]}), path, {"run_id": "R2"})
    assert calls["count"] == 2
    assert _hash(path) != original_hash


def test_replace_failure_preserves_original_and_writes_diagnostic(monkeypatch, tmp_path: Path) -> None:
    path = tmp_path / "table.parquet"
    atomic_write_parquet(pl.DataFrame({"id": ["old"]}), path, {"run_id": "OLD"})
    original_hash = _hash(path)
    original_replace = parquet_store.os.replace

    def always_fail(source, destination):
        if str(destination) == str(path):
            raise OSError(1224, "mapped section")
        return original_replace(source, destination)

    # Avoid replacing the lock file in heartbeat; the target replacement is
    # the only operation being fault-injected.
    monkeypatch.setattr(parquet_store.ParquetWriteLease, "heartbeat", lambda self: self.payload)
    monkeypatch.setattr(parquet_store.os, "replace", always_fail)
    with pytest.raises(ParquetWriteError):
        atomic_write_parquet(pl.DataFrame({"id": ["new"]}), path, {"run_id": "R3"})
    assert _hash(path) == original_hash
    diagnostics = list((tmp_path / "_parquet_diagnostics").rglob("*.error.json"))
    assert diagnostics
    payload = json.loads(diagnostics[0].read_text(encoding="utf-8"))
    assert payload["original_file_hash"] == original_hash
