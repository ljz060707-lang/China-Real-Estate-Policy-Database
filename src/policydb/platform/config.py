"""Unified CRPD path/DB configuration (additive).

Reads CRPD_* environment variables with the verified production defaults.
All platform modules must read paths from here; no hardcoded drive paths in
new code.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[3]

_DEFAULTS = {
    "CRPD_HOME": str(_REPO_ROOT),
    "CRPD_DATA_ROOT": r"E:\Data Set\CRPD",
    "CRPD_DB": r"E:\Data Set\CRPD\database\policydb.duckdb",
    "CRPD_RAW_ROOT": r"E:\Data Set\CRPD\raw",
    "CRPD_CURATED_ROOT": r"E:\Data Set\CRPD\curated",
    "CRPD_OUTPUT_ROOT": r"E:\Data Set\CRPD\outputs",
    "CRPD_ARCHIVE_ROOT": r"E:\Data Set\CRPD\archive",
    "CRPD_LOG_ROOT": r"E:\Data Set\CRPD\logs",
    "CRPD_CACHE_ROOT": r"E:\Data Set\CRPD\cache",
    "CRPD_MANIFEST_ROOT": r"E:\Data Set\CRPD\manifests",
}


def env(name: str) -> str:
    return os.environ.get(name, _DEFAULTS[name])


@dataclass(frozen=True)
class CRPDConfig:
    home: Path
    data_root: Path
    database: Path
    raw_root: Path
    curated_root: Path
    output_root: Path
    archive_root: Path
    log_root: Path
    cache_root: Path
    manifest_root: Path

    @classmethod
    def discover(cls) -> CRPDConfig:
        return cls(
            home=Path(env("CRPD_HOME")),
            data_root=Path(env("CRPD_DATA_ROOT")),
            database=Path(env("CRPD_DB")),
            raw_root=Path(env("CRPD_RAW_ROOT")),
            curated_root=Path(env("CRPD_CURATED_ROOT")),
            output_root=Path(env("CRPD_OUTPUT_ROOT")),
            archive_root=Path(env("CRPD_ARCHIVE_ROOT")),
            log_root=Path(env("CRPD_LOG_ROOT")),
            cache_root=Path(env("CRPD_CACHE_ROOT")),
            manifest_root=Path(env("CRPD_MANIFEST_ROOT")),
        )

    def ensure_dirs(self) -> None:
        for p in (self.raw_root, self.curated_root, self.output_root,
                  self.archive_root, self.log_root, self.cache_root,
                  self.manifest_root):
            p.mkdir(parents=True, exist_ok=True)


def load_settings() -> object:
    """Return the existing policydb Settings for the production root."""
    from policydb.settings import Settings  # existing module, unchanged
    return Settings.discover(env("CRPD_HOME"))
