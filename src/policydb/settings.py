from __future__ import annotations

import os
from pathlib import Path

import yaml
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
    data_root_path: Path | None = None
    database_path: Path | None = None
    curated_path: Path | None = None
    raw_path: Path | None = None
    archive_path: Path | None = None
    research_path: Path | None = None
    outputs_path: Path | None = None
    logs_path: Path | None = None
    automation_path: Path | None = None
    control_path: Path | None = None
    runtime_path: Path | None = None
    cache_path: Path | None = None
    temp_path: Path | None = None
    test_artifacts_path: Path | None = None
    dashboard_path: Path | None = None
    backups_path: Path | None = None
    dashboard_runtime_path: Path | None = None

    def _normalise_path(self, value: str | Path) -> Path:
        path = Path(value).expanduser()
        return path if path.is_absolute() else (self.root / path)

    def _load_storage_config(self) -> dict:
        """Read only the project storage config; never create directories here."""

        if os.getenv("POLICYDB_SKIP_STORAGE_CONFIG", "").lower() in {
            "1",
            "true",
            "yes",
            "on",
        }:
            return {}
        path = self.root / "config" / "storage.yaml"
        if not path.exists():
            return {}
        payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        if not isinstance(payload, dict):
            raise ValueError(f"Storage config must be a mapping: {path}")
        nested = payload.get("storage")
        if isinstance(nested, dict):
            payload = nested
        return {str(key): value for key, value in payload.items()}

    def _config_value(self, *names: str):
        config = self._load_storage_config()
        for name in names:
            value = config.get(name)
            if value not in (None, ""):
                return value
        return None

    def _resolved_storage_path(
        self,
        field: str,
        environments: tuple[str, ...],
        config_names: tuple[str, ...],
    ) -> Path | None:
        explicit = getattr(self, field)
        if explicit is not None:
            return self._normalise_path(explicit)
        for environment in environments:
            value = os.getenv(environment)
            if value:
                return self._normalise_path(value)
        configured = self._config_value(*config_names)
        return self._normalise_path(configured) if configured not in (None, "") else None

    def _path_setting(self, preference: str, environment: str) -> Path | None:
        value = os.getenv(environment) or str(self.preferences.get(preference, "")).strip()
        return Path(value).expanduser() if value else None

    @property
    def data_root(self) -> Path:
        """Resolve the authoritative data root without creating or touching it."""

        explicit = self._resolved_storage_path(
            "data_root_path", ("CRPD_DATA_ROOT", "POLICYDB_DATA_ROOT"), ("data_root",)
        )
        return explicit or self.root / "data"

    @classmethod
    def discover(cls, root: str | Path | None = None) -> Settings:
        value = Path(root or os.getenv("POLICYDB_ROOT", Path.cwd())).resolve()
        if not (value / "pyproject.toml").exists() and (value / "policy-database").exists():
            value = value / "policy-database"
        _load_local_env(value)
        return cls(root=value, data_version=os.getenv("POLICYDB_DATA_VERSION", "0.1.0"))

    @property
    def database_root(self) -> Path:
        return self.data_root / "database"

    @property
    def database(self) -> Path:
        resolved = self._resolved_storage_path(
            "database_path", ("POLICYDB_DATABASE",), ("database", "database_path")
        )
        if resolved:
            return resolved
        # Preserve the existing repository-local database for portable callers while
        # making every external/production root derive from data_root/database.
        legacy = self.root / "database" / "policydb.duckdb"
        if self.data_root == self.root / "data" and legacy.exists():
            return legacy
        return self.database_root / "policydb.duckdb"

    def _derived_path(
        self,
        field: str,
        environments: tuple[str, ...],
        config_names: tuple[str, ...],
        directory: str,
    ) -> Path:
        return (
            self._resolved_storage_path(field, environments, config_names)
            or self.data_root / directory
        )

    @property
    def curated_root(self) -> Path:
        return self._derived_path(
            "curated_path", ("POLICYDB_CURATED_ROOT",), ("curated", "curated_root"), "curated"
        )

    @property
    def curated(self) -> Path:
        return self.curated_root

    @property
    def raw_root(self) -> Path:
        return self._derived_path(
            "raw_path", ("POLICYDB_RAW_ROOT",), ("raw", "raw_root"), "raw"
        )

    @property
    def raw(self) -> Path:
        return self.raw_root

    @property
    def research_root(self) -> Path:
        return self._derived_path(
            "research_path",
            ("POLICYDB_RESEARCH_ROOT",),
            ("research", "research_root"),
            "research",
        )

    @property
    def research(self) -> Path:
        return self.research_root

    @property
    def archive_root(self) -> Path:
        if self.archive_path is not None:
            return self._normalise_path(self.archive_path)
        explicit = os.getenv("CRPD_ARCHIVE_ROOT") or os.getenv("POLICYDB_ARCHIVE_ROOT")
        if explicit:
            return self._normalise_path(explicit)
        configured = self._config_value("archive", "archive_root")
        if configured not in (None, ""):
            return self._normalise_path(configured)
        return self.data_root / "archive"

    @property
    def logs(self) -> Path:
        return self.logs_root

    @property
    def logs_root(self) -> Path:
        return self._derived_path(
            "logs_path", ("POLICYDB_LOG_ROOT",), ("logs", "logs_root"), "logs"
        )

    @property
    def automation_root(self) -> Path:
        return self._derived_path(
            "automation_path",
            ("POLICYDB_AUTOMATION_ROOT",),
            ("automation", "automation_root"),
            "automation",
        )

    @property
    def automation(self) -> Path:
        return self.automation_root

    @property
    def control_root(self) -> Path:
        return self._derived_path(
            "control_path",
            ("POLICYDB_CONTROL_ROOT",),
            ("control", "control_root"),
            "control",
        )

    @property
    def control(self) -> Path:
        return self.control_root

    @property
    def runtime_root(self) -> Path:
        return self._derived_path(
            "runtime_path",
            ("POLICYDB_RUNTIME_ROOT",),
            ("runtime", "runtime_root"),
            "runtime",
        )

    @property
    def runtime(self) -> Path:
        return self.runtime_root

    @property
    def cache_root(self) -> Path:
        return self._derived_path(
            "cache_path", ("POLICYDB_CACHE_ROOT",), ("cache", "cache_root"), "cache"
        )

    @property
    def cache(self) -> Path:
        return self.cache_root

    @property
    def temp_root(self) -> Path:
        return self._derived_path(
            "temp_path", ("POLICYDB_TEMP_ROOT",), ("temp", "temp_root"), "temp"
        )

    @property
    def temp(self) -> Path:
        return self.temp_root

    @property
    def test_artifacts_root(self) -> Path:
        return self._derived_path(
            "test_artifacts_path",
            ("POLICYDB_TEST_ARTIFACTS_ROOT",),
            ("test_artifacts", "test_artifacts_root"),
            "test_artifacts",
        )

    @property
    def test_artifacts(self) -> Path:
        return self.test_artifacts_root

    @property
    def dashboard_root(self) -> Path:
        return self._derived_path(
            "dashboard_path",
            ("POLICYDB_DASHBOARD_ROOT",),
            ("dashboard", "dashboard_root"),
            "dashboard",
        )

    @property
    def dashboard(self) -> Path:
        return self.dashboard_root

    @property
    def backups_root(self) -> Path:
        return self._derived_path(
            "backups_path",
            ("POLICYDB_BACKUPS_ROOT",),
            ("backups", "backups_root"),
            "backups",
        )

    @property
    def backups(self) -> Path:
        return self.backups_root

    @property
    def dashboard_runtime_root(self) -> Path:
        resolved = self._resolved_storage_path(
            "dashboard_runtime_path",
            ("POLICYDB_DASHBOARD_RUNTIME_ROOT",),
            ("dashboard_runtime", "dashboard_runtime_root"),
        )
        return resolved or self.runtime_root / "dashboard"

    @property
    def dashboard_runtime(self) -> Path:
        return self.dashboard_runtime_root

    @property
    def outputs(self) -> Path:
        resolved = self.outputs_root
        legacy = self.root / "outputs"
        if (
            self.data_root == self.root / "data"
            and not self._resolved_storage_path(
                "outputs_path",
                ("POLICYDB_OUTPUT_ROOT",),
                ("outputs", "outputs_root"),
            )
            and legacy.exists()
        ):
            return legacy
        return resolved

    @property
    def outputs_root(self) -> Path:
        return self._derived_path(
            "outputs_path",
            ("POLICYDB_OUTPUT_ROOT",),
            ("outputs", "outputs_root"),
            "outputs",
        )

    @property
    def jobs(self) -> Path:
        return self.data_root / "jobs"

    @property
    def manifests(self) -> Path:
        return self.data_root / "manifests"

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
    def ai_provider(self) -> str:
        return str(self._preference("ai_provider", "AI_PROVIDER", "siliconflow")).lower()

    @property
    def siliconflow_api_key(self) -> str | None:
        return default_secret_store().get_secret(
            "siliconflow_api_key"
        ) or default_secret_store().get_secret("glm_api_key")

    @property
    def siliconflow_base_url(self) -> str:
        return str(
            self._preference(
                "siliconflow_base_url",
                "SILICONFLOW_BASE_URL",
                "https://api.siliconflow.cn/v1",
            )
        ).rstrip("/")

    @property
    def siliconflow_chat_model(self) -> str:
        return str(
            self._preference("siliconflow_chat_model", "SILICONFLOW_CHAT_MODEL", "")
        )

    @property
    def siliconflow_verify_model(self) -> str:
        return str(
            self._preference(
                "siliconflow_verify_model",
                "SILICONFLOW_VERIFY_MODEL",
                self.siliconflow_chat_model,
            )
        )

    @property
    def siliconflow_embedding_model(self) -> str:
        return str(
            self._preference(
                "siliconflow_embedding_model",
                "SILICONFLOW_EMBEDDING_MODEL",
                "BAAI/bge-m3",
            )
        )

    @property
    def siliconflow_rerank_model(self) -> str:
        return str(
            self._preference(
                "siliconflow_rerank_model",
                "SILICONFLOW_RERANK_MODEL",
                "BAAI/bge-reranker-v2-m3",
            )
        )

    @property
    def policy_archive_root(self) -> Path:
        """Legacy property name retained for archive callers."""
        # Explicit run-scoped environment paths must win over the legacy
        # preference.  Otherwise an isolated rehearsal (or a migrated data
        # root) can still write content-addressed evidence to the old archive.
        if os.getenv("CRPD_ARCHIVE_ROOT") or os.getenv("POLICYDB_ARCHIVE_ROOT"):
            return self.archive_root
        configured = str(self.preferences.get("policy_archive_root", "")).strip()
        return self._normalise_path(configured) if configured else self.archive_root

    @property
    def glm_api_key(self) -> str | None:
        return self.siliconflow_api_key if self.ai_provider == "siliconflow" else (
            default_secret_store().get_secret("glm_api_key")
        )

    @property
    def glm_model(self) -> str:
        if self.ai_provider == "siliconflow" and self.siliconflow_chat_model:
            return self.siliconflow_chat_model
        return str(self._preference("glm_model", "GLM_MODEL", "glm-4-flash"))

    @property
    def glm_base_url(self) -> str:
        if self.ai_provider == "siliconflow":
            return self.siliconflow_base_url + "/chat/completions"
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
    def search_providers(self) -> list[str]:
        value = self._preference("search_providers", "SEARCH_PROVIDERS", self.search_provider)
        parts = value if isinstance(value, list) else str(value).split(",")
        return [str(part).strip() for part in parts if str(part).strip()]

    @property
    def search_api_key(self) -> str | None:
        return default_secret_store().get_secret("search_api_key")

    def search_api_key_for(self, provider: str) -> str | None:
        normalized = provider.strip().lower()
        return (
            default_secret_store().get_secret(f"search_api_key_{normalized}")
            or self.search_api_key
        )

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
    def crpd_proxy_url(self) -> str | None:
        return os.getenv("CRPD_PROXY_URL") or self.http_proxy

    @property
    def ai_proxy_url(self) -> str | None:
        return os.getenv("CRPD_AI_PROXY_URL") or self.crpd_proxy_url

    @property
    def search_proxy_url(self) -> str | None:
        return os.getenv("CRPD_SEARCH_PROXY_URL") or self.crpd_proxy_url

    @property
    def government_route(self) -> str:
        value = os.getenv("CRPD_GOVERNMENT_ROUTE", "direct").strip().lower()
        if value != "direct":
            raise ValueError("CRPD_GOVERNMENT_ROUTE must be 'direct'")
        return value

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
