from __future__ import annotations

import os
from pathlib import Path

from pydantic import BaseModel

from policydb.config.preferences import PreferencesStore
from policydb.config.secret_store import default_secret_store


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
    curated_path: Path | None = None

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
        return self.curated_path or self.root / "data" / "curated"

    @property
    def research(self) -> Path:
        return self.root / "data" / "research"

    @property
    def manual_corrections(self) -> Path:
        return self.root / "data" / "reference" / "manual_corrections.csv"

    @property
    def review_history(self) -> Path:
        return self.root / "data" / "logs" / "review_history.csv"

    @property
    def preferences_path(self) -> Path:
        return self.root / "data" / "reference" / "user_preferences.json"

    @property
    def preferences(self) -> dict:
        return PreferencesStore(self.preferences_path).load()

    def _preference(self, name: str, env_name: str, default):
        if env_name in os.environ:
            value = os.environ[env_name]
            if isinstance(default, bool):
                return value.strip().lower() in {"1", "true", "yes", "on"}
            if isinstance(default, int):
                return int(value)
            if isinstance(default, float):
                return float(value)
            return value
        return self.preferences.get(name, default)

    @property
    def read_only(self) -> bool:
        return bool(self._preference("read_only", "POLICYDB_READ_ONLY", False))

    @property
    def glm_api_key(self) -> str | None:
        return default_secret_store().get_secret("glm_api_key")

    @property
    def glm_model(self) -> str:
        return str(self._preference("glm_model", "GLM_MODEL", "glm-4-flash"))

    @property
    def glm_base_url(self) -> str:
        return str(
            self._preference(
                "glm_base_url",
                "GLM_BASE_URL",
                "https://open.bigmodel.cn/api/paas/v4/chat/completions",
            )
        )

    @property
    def tianditu_token(self) -> str | None:
        return default_secret_store().get_secret("tianditu_token")

    @property
    def tianditu_map_approval(self) -> str:
        return str(
            self._preference(
                "tianditu_map_approval", "TIANDITU_MAP_APPROVAL", "GS（2024）0568号"
            )
        )

    @property
    def tianditu_qualification(self) -> str:
        return str(
            self._preference(
                "tianditu_qualification", "TIANDITU_QUALIFICATION", "甲测资字1100471"
            )
        )

    @property
    def search_provider(self) -> str:
        return str(self._preference("search_provider", "SEARCH_PROVIDER", "None"))

    @property
    def search_api_key(self) -> str | None:
        return default_secret_store().get_secret("search_api_key")

    @property
    def search_base_url(self) -> str | None:
        value = self._preference("search_base_url", "SEARCH_BASE_URL", "")
        return str(value) or None

    @property
    def request_timeout(self) -> float:
        return float(self._preference("request_timeout", "POLICYDB_REQUEST_TIMEOUT", 30.0))

    @property
    def connect_timeout(self) -> float:
        return float(self._preference("connect_timeout", "POLICYDB_CONNECT_TIMEOUT", 10.0))

    @property
    def max_retries(self) -> int:
        return int(self._preference("max_retries", "POLICYDB_MAX_RETRIES", 3))

    @property
    def default_rate_limit(self) -> float:
        return float(
            self._preference("default_rate_limit", "POLICYDB_DEFAULT_RATE_LIMIT", 0.5)
        )

    @property
    def user_agent(self) -> str:
        return str(
            self._preference(
                "user_agent",
                "POLICYDB_USER_AGENT",
                "Mozilla/5.0 (compatible; PolicyDBResearchBot/0.1; +local-research)",
            )
        )

    @property
    def respect_robots(self) -> bool:
        return bool(self._preference("respect_robots", "POLICYDB_RESPECT_ROBOTS", True))

    @property
    def http_proxy(self) -> str | None:
        return default_secret_store().get_secret("http_proxy")

    @property
    def project_python_path(self) -> Path | None:
        value = str(self._preference("project_python_path", "POLICYDB_PYTHON", "")).strip()
        return Path(value).expanduser() if value else None

    @property
    def max_concurrency(self) -> int:
        return min(
            max(int(self._preference("max_concurrency", "POLICYDB_MAX_CONCURRENCY", 4)), 1),
            16,
        )
