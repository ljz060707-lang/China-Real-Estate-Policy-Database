"""Explicit runtime isolation contracts for bounded CRPD workflows."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from policydb.settings import Settings


class RuntimeContextError(RuntimeError):
    """Raised before a job can write when its declared runtime is unsafe."""


@dataclass(frozen=True)
class RuntimeContext:
    run_mode: str
    data_root: Path
    database_path: Path
    release_root: Path
    run_id: str
    production_write_allowed: bool

    def validate(self, *, expected_production_database: Path | None = None) -> RuntimeContext:
        data_root = self.data_root.resolve()
        database = self.database_path.resolve()
        release_root = self.release_root.resolve()
        mode = self.run_mode.upper().strip()
        if mode == "REHEARSAL":
            if self.production_write_allowed:
                raise RuntimeContextError("REHEARSAL cannot set production_write_allowed=true")
            if "promotion_rehearsal" not in {part.lower() for part in data_root.parts}:
                raise RuntimeContextError(
                    "REHEARSAL data_root must be under a promotion_rehearsal directory"
                )
            if expected_production_database and database == expected_production_database.resolve():
                raise RuntimeContextError("REHEARSAL database_path equals production database")
        elif mode == "PRODUCTION":
            if not self.production_write_allowed:
                raise RuntimeContextError(
                    "PRODUCTION requires explicit production_write_allowed=true"
                )
            if expected_production_database and database != expected_production_database.resolve():
                raise RuntimeContextError("PRODUCTION database_path does not match expected production database")
        elif mode not in {"", "UNSPECIFIED", "TEST", "DEMO"}:
            raise RuntimeContextError(f"unsupported runtime mode: {self.run_mode}")
        if mode == "REHEARSAL" and release_root == database:
            raise RuntimeContextError("release_root cannot be the database file")
        return self


def build_runtime_context(
    settings: Settings,
    *,
    run_mode: Literal["REHEARSAL", "PRODUCTION", "TEST", "DEMO", "UNSPECIFIED"] | str,
    run_id: str,
    production_write_allowed: bool,
    expected_production_database: Path | None = None,
    release_root: Path | None = None,
) -> RuntimeContext:
    """Build and validate a context without creating directories or writing data."""

    context = RuntimeContext(
        run_mode=str(run_mode),
        data_root=settings.data_root,
        database_path=settings.database,
        release_root=release_root or settings.root / "data" / "releases",
        run_id=str(run_id),
        production_write_allowed=bool(production_write_allowed),
    )
    return context.validate(expected_production_database=expected_production_database)


__all__ = ["RuntimeContext", "RuntimeContextError", "build_runtime_context"]
