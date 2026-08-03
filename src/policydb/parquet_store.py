"""Windows-safe, detached Parquet snapshots and serialized writes.

The continuous crawler has several stages that may run in different
processes.  A Parquet file is therefore treated as an immutable snapshot:
read it fully with memory mapping disabled, build the next snapshot in a
unique sibling temporary file, validate it, and replace the destination only
while holding a cross-process lease.

This module deliberately has no business semantics.  It is the single
storage primitive used by crawl/checkpoint and full-sync code so a failed
write cannot silently advance a checkpoint or destroy the previous file.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import socket
import time
import traceback
import uuid
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import polars as pl

try:  # psutil is a declared project dependency; keep import errors explicit.
    import psutil
except ImportError as exc:  # pragma: no cover - packaging failure, not a data path
    raise RuntimeError("policydb.parquet_store requires psutil") from exc


class ParquetStoreError(RuntimeError):
    """Base error for safe Parquet reads and writes."""


class ParquetLockError(ParquetStoreError):
    """Raised when a live writer owns a destination."""


class ParquetWriteError(ParquetStoreError):
    """Raised when a validated temporary snapshot cannot replace its target."""


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _sha256(path: Path) -> str | None:
    if not path.exists():
        return None
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _safe_part(value: object, default: str) -> str:
    text = str(value or default)
    return "".join(char if char.isalnum() or char in "-_." else "_" for char in text)[:80]


def _process_start_time(pid: int) -> str | None:
    try:
        return datetime.fromtimestamp(psutil.Process(pid).create_time(), tz=UTC).isoformat()
    except (OSError, psutil.Error, ValueError):
        return None


def _process_is_alive(pid: object, expected_start: object = None) -> bool:
    try:
        process = psutil.Process(int(pid))
        if not process.is_running():
            return False
        if expected_start:
            actual = datetime.fromtimestamp(process.create_time(), tz=UTC)
            expected = datetime.fromisoformat(str(expected_start).replace("Z", "+00:00"))
            if expected.tzinfo is None:
                expected = expected.replace(tzinfo=UTC)
            if abs((actual - expected).total_seconds()) > 2:
                return False
        return True
    except (OSError, ValueError, TypeError, psutil.Error):
        return False


def _lock_path(destination: Path) -> Path:
    return Path(f"{destination}.lock")


def _read_json(path: Path) -> dict[str, Any] | None:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return value if isinstance(value, dict) else None


class ParquetWriteLease:
    """Atomic lock-file lease for one formal Parquet destination."""

    def __init__(
        self,
        destination: Path,
        context: Mapping[str, Any] | None = None,
        *,
        lease_seconds: int = 900,
    ) -> None:
        self.destination = Path(destination)
        self.lock_path = _lock_path(self.destination)
        self.context = dict(context or {})
        self.lease_seconds = max(1, int(lease_seconds))
        self.payload: dict[str, Any] | None = None

    def _new_payload(self) -> dict[str, Any]:
        now = datetime.now(UTC)
        pid = os.getpid()
        return {
            "destination": str(self.destination),
            "pid": pid,
            "process_start_time": _process_start_time(pid),
            "run_id": self.context.get("run_id"),
            "job_id": self.context.get("job_id"),
            "worker_id": self.context.get("worker_id") or str(pid),
            "acquired_at": now.isoformat(),
            "heartbeat_at": now.isoformat(),
            "expires_at": (now.timestamp() + self.lease_seconds),
            "host": socket.gethostname(),
        }

    def acquire(self) -> ParquetWriteLease:
        self.destination.parent.mkdir(parents=True, exist_ok=True)
        payload = self._new_payload()
        for _ in range(2):
            try:
                descriptor = os.open(
                    str(self.lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY
                )
                try:
                    encoded = (json.dumps(payload, ensure_ascii=False, indent=2) + "\n").encode(
                        "utf-8"
                    )
                    os.write(descriptor, encoded)
                    os.fsync(descriptor)
                finally:
                    os.close(descriptor)
                self.payload = payload
                return self
            except FileExistsError as exc:
                current = _read_json(self.lock_path)
                now = time.time()
                if current is None:
                    raise ParquetLockError(
                        f"Parquet lock exists but is unreadable: {self.lock_path}"
                    ) from exc
                expired = float(current.get("expires_at") or 0) <= now
                live = _process_is_alive(current.get("pid"), current.get("process_start_time"))
                if not expired or live:
                    raise ParquetLockError(
                        f"live Parquet writer owns {self.destination}: "
                        f"pid={current.get('pid')} run_id={current.get('run_id')}"
                    ) from exc
                # Recovery is allowed only after both lease expiry and a dead
                # process.  The unlink is narrowly scoped to this exact lock.
                try:
                    self.lock_path.unlink()
                except FileNotFoundError:
                    continue
        raise ParquetLockError(f"could not acquire Parquet lock: {self.lock_path}")

    def heartbeat(self) -> dict[str, Any]:
        if self.payload is None:
            raise ParquetLockError("cannot heartbeat an unacquired Parquet lease")
        current = _read_json(self.lock_path)
        if not current or current.get("pid") != self.payload.get("pid"):
            raise ParquetLockError(f"Parquet lock ownership lost: {self.lock_path}")
        now = datetime.now(UTC)
        current["heartbeat_at"] = now.isoformat()
        current["expires_at"] = now.timestamp() + self.lease_seconds
        temporary = self.lock_path.with_name(f".{self.lock_path.name}.{os.getpid()}.tmp")
        try:
            temporary.write_text(json.dumps(current, ensure_ascii=False, indent=2), encoding="utf-8")
            os.replace(temporary, self.lock_path)
        finally:
            temporary.unlink(missing_ok=True)
        self.payload = current
        return current

    def release(self) -> None:
        if self.payload is None:
            return
        current = _read_json(self.lock_path)
        if current and current.get("pid") == self.payload.get("pid"):
            self.lock_path.unlink(missing_ok=True)
        self.payload = None

    def __enter__(self) -> ParquetWriteLease:
        return self.acquire()

    def __exit__(self, _exc_type, _exc, _traceback) -> None:
        self.release()


@contextmanager
def parquet_write_lock(
    destination: Path,
    context: Mapping[str, Any] | None = None,
    *,
    lease_seconds: int = 900,
) -> Iterator[ParquetWriteLease]:
    """Acquire the exact ``<destination>.lock`` lease for one writer."""

    lease = ParquetWriteLease(destination, context, lease_seconds=lease_seconds)
    with lease:
        yield lease


def _detached_frame(table: Any) -> pl.DataFrame:
    if isinstance(table, pl.DataFrame):
        return table.clone()
    if isinstance(table, pl.LazyFrame):
        raise TypeError("lazy Parquet frames must be collected before storage")
    try:
        import pyarrow as pa

        if isinstance(table, pa.Table):
            return pl.from_arrow(table).clone()
    except ImportError:  # pragma: no cover - pyarrow is a declared dependency
        pass
    raise TypeError(f"unsupported Parquet table type: {type(table).__name__}")


def read_parquet_snapshot(
    path: Path,
    *,
    columns: Sequence[str] | None = None,
    n_rows: int | None = None,
) -> pl.DataFrame:
    """Materialize an independent Parquet snapshot with memory mapping off."""

    source = Path(path)
    if not source.exists():
        raise FileNotFoundError(source)
    frame = pl.read_parquet(
        source,
        columns=list(columns) if columns is not None else None,
        n_rows=n_rows,
        memory_map=False,
    )
    return frame.clone()


def _validate_frame(
    frame: pl.DataFrame,
    *,
    key_columns: Sequence[str] = (),
    expected_schema: Mapping[str, Any] | None = None,
) -> None:
    missing = [column for column in key_columns if column not in frame.columns]
    if missing:
        raise ParquetWriteError(f"Parquet key columns missing: {missing}")
    if key_columns and frame.select(list(key_columns)).is_duplicated().any():
        raise ParquetWriteError(f"Parquet key uniqueness failed: {list(key_columns)}")
    if expected_schema:
        for column, dtype in expected_schema.items():
            if column not in frame.columns:
                raise ParquetWriteError(f"Parquet schema column missing: {column}")
            if frame.schema[column] != dtype:
                raise ParquetWriteError(
                    f"Parquet schema mismatch for {column}: "
                    f"{frame.schema[column]} != {dtype}"
                )


def _is_transient_replace_error(exc: OSError) -> bool:
    return int(getattr(exc, "winerror", 0) or 0) == 1224 or int(getattr(exc, "errno", 0) or 0) == 1224 or any(
        phrase in str(exc).lower()
        for phrase in ("1224", "mapped section", "sharing violation")
    )


def _diagnose_write_failure(
    destination: Path,
    temporary: Path,
    context: Mapping[str, Any],
    exc: BaseException,
    *,
    attempt: int,
) -> Path:
    run_id = _safe_part(context.get("run_id"), "unknown-run")
    diagnostics = destination.parent / "_parquet_diagnostics" / run_id
    diagnostics.mkdir(parents=True, exist_ok=True)
    suffix = uuid.uuid4().hex[:12]
    retained = diagnostics / f"{destination.name}.{suffix}.failed.tmp.parquet"
    if temporary.exists():
        shutil.move(str(temporary), str(retained))
    payload = {
        "created_at": _now(),
        "destination": str(destination),
        "temporary_path": str(retained),
        "attempt": attempt,
        "original_file_hash": _sha256(destination),
        "temporary_file_hash": _sha256(retained),
        "error_type": type(exc).__name__,
        "error_message": str(exc),
        "errno": getattr(exc, "errno", None),
        "winerror": getattr(exc, "winerror", None),
        "operation": "os.replace",
        "process_id": os.getpid(),
        "process_start_time": _process_start_time(os.getpid()),
        "worker_id": context.get("worker_id") or str(os.getpid()),
        "run_id": context.get("run_id"),
        "job_id": context.get("job_id"),
        "traceback": traceback.format_exc(),
    }
    error_path = diagnostics / f"{destination.name}.{suffix}.error.json"
    error_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return error_path


def _atomic_write_parquet_locked(
    frame: pl.DataFrame,
    destination: Path,
    context: Mapping[str, Any],
    *,
    key_columns: Sequence[str] = (),
    expected_schema: Mapping[str, Any] | None = None,
    replace_retries: int = 2,
) -> dict[str, Any]:
    destination.parent.mkdir(parents=True, exist_ok=True)
    _validate_frame(frame, key_columns=key_columns, expected_schema=expected_schema)
    temporary = destination.with_name(
        ".{name}.{run}.{job}.{worker}.{uuid}.tmp.parquet".format(
            name=destination.name,
            run=_safe_part(context.get("run_id"), "run"),
            job=_safe_part(context.get("job_id"), "job"),
            worker=_safe_part(context.get("worker_id"), str(os.getpid())),
            uuid=uuid.uuid4().hex,
        )
    )
    original_hash = _sha256(destination)
    try:
        frame.write_parquet(temporary, compression="zstd")
        descriptor = os.open(str(temporary), os.O_RDWR)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        snapshot = read_parquet_snapshot(temporary)
        _validate_frame(snapshot, key_columns=key_columns, expected_schema=expected_schema)
        if snapshot.height != frame.height or snapshot.columns != frame.columns:
            raise ParquetWriteError("temporary Parquet validation changed row count or schema")
        last_error: OSError | None = None
        for attempt in range(int(replace_retries) + 1):
            try:
                os.replace(temporary, destination)
                last_error = None
                break
            except OSError as exc:
                last_error = exc
                if not _is_transient_replace_error(exc) or attempt >= int(replace_retries):
                    raise
                time.sleep(0.25 * (attempt + 1))
        if last_error is not None:
            raise last_error
        return {
            "destination": str(destination),
            "temporary_path": str(temporary),
            "row_count": frame.height,
            "columns": frame.columns,
            "original_file_hash": original_hash,
            "new_file_hash": _sha256(destination),
            "run_id": context.get("run_id"),
            "job_id": context.get("job_id"),
            "worker_id": context.get("worker_id") or str(os.getpid()),
        }
    except Exception as exc:
        error_path = _diagnose_write_failure(
            destination,
            temporary,
            context,
            exc,
            attempt=int(replace_retries) + 1,
        )
        if isinstance(exc, ParquetStoreError):
            raise
        raise ParquetWriteError(
            f"Parquet write failed for {destination}; diagnostics={error_path}"
        ) from exc
    finally:
        temporary.unlink(missing_ok=True)


def atomic_write_parquet(
    table: Any,
    destination: Path,
    context: Mapping[str, Any] | None = None,
    *,
    key_columns: Sequence[str] = (),
    expected_schema: Mapping[str, Any] | None = None,
    replace_retries: int = 2,
) -> dict[str, Any]:
    """Write a validated snapshot and atomically replace ``destination``."""

    frame = _detached_frame(table)
    write_context = dict(context or {})
    with parquet_write_lock(destination, write_context) as lease:
        lease.heartbeat()
        return _atomic_write_parquet_locked(
            frame,
            Path(destination),
            write_context,
            key_columns=key_columns,
            expected_schema=expected_schema,
            replace_retries=replace_retries,
        )


def merge_and_replace_parquet(
    destination: Path,
    incoming: Any,
    key_columns: Sequence[str],
    context: Mapping[str, Any] | None = None,
    *,
    expected_schema: Mapping[str, Any] | None = None,
) -> pl.DataFrame:
    """Serialize read/merge/validate/replace for one keyed Parquet table."""

    target = Path(destination)
    additions = _detached_frame(incoming)
    write_context = dict(context or {})
    with parquet_write_lock(target, write_context) as lease:
        current = read_parquet_snapshot(target) if target.exists() else pl.DataFrame()
        if current.is_empty() and not current.columns:
            merged = additions
        elif additions.is_empty() and not additions.columns:
            merged = current
        else:
            merged = pl.concat([current, additions], how="diagonal_relaxed")
        if key_columns:
            merged = merged.unique(subset=list(key_columns), keep="last", maintain_order=True)
        lease.heartbeat()
        _atomic_write_parquet_locked(
            merged,
            target,
            write_context,
            key_columns=key_columns,
            expected_schema=expected_schema,
        )
        return merged.clone()


def append_unique_parquet(
    destination: Path,
    rows: Sequence[Mapping[str, Any]],
    key_column: str,
    context: Mapping[str, Any] | None = None,
) -> pl.DataFrame:
    """Append keyed rows through the same serialized merge path."""

    incoming = pl.DataFrame(list(rows), infer_schema_length=None) if rows else pl.DataFrame()
    return merge_and_replace_parquet(destination, incoming, (key_column,), context)


__all__ = [
    "ParquetLockError",
    "ParquetStoreError",
    "ParquetWriteError",
    "append_unique_parquet",
    "atomic_write_parquet",
    "merge_and_replace_parquet",
    "parquet_write_lock",
    "read_parquet_snapshot",
]
