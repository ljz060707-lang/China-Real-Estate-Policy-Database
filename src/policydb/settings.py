from __future__ import annotations

import os
from pathlib import Path

from pydantic import BaseModel


def _load_local_env(root: Path) -> None:
    """Load the project's simple KEY=VALUE file without an extra runtime dependency."""
    path = root / ".env"
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8-sig").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("'\""))


class Settings(BaseModel):
    root: Path
    data_version: str = "0.1.0"
    database_path: Path | None = None

    @classmethod
    def discover(cls, root: str | Path | None = None) -> Settings:
        value = Path(root or os.getenv("POLICYDB_ROOT", Path.cwd())).resolve()
        if not (value / "pyproject.toml").exists() and (value / "policy-database").exists():
            value = value / "policy-database"
        _load_local_env(value)
        return cls(root=value, data_version=os.getenv("POLICYDB_DATA_VERSION", "0.1.0"))

    @property
    def database(self) -> Path:
        return self.database_path or self.root / "database" / "policydb.duckdb"

    @property
    def curated(self) -> Path:
        return self.root / "data" / "curated"

    @property
    def research(self) -> Path:
        return self.root / "data" / "research"

    @property
    def manual_corrections(self) -> Path:
        return self.root / "data" / "reference" / "manual_corrections.csv"

    @property
    def review_history(self) -> Path:
        return self.root / "data" / "logs" / "review_history.csv"
