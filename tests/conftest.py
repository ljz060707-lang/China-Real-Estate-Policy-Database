import os
import tempfile
from pathlib import Path

import pytest

from policydb import PolicyDB
from policydb import settings as settings_module

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _pytest_storage_settings(config) -> settings_module.Settings:
    """Resolve test artifacts without leaking production config into portable runs."""

    root = Path(str(config.rootpath)).resolve()
    portable = bool(os.getenv("CI")) or os.getenv("POLICYDB_TEST_PORTABLE") == "1"
    portable = portable or root != PROJECT_ROOT
    if portable:
        portable_root = Path(tempfile.gettempdir()) / "policydb-pytest" / root.name / "data"
        return settings_module.Settings(root=root, data_root_path=portable_root)
    return settings_module.Settings.discover(PROJECT_ROOT)


def pytest_configure(config) -> None:
    """Keep implicit pytest temp/cache artifacts on the resolved storage root."""

    settings = _pytest_storage_settings(config)
    if getattr(config.option, "basetemp", None) in (None, ""):
        run_id = os.getenv("CRPD_TEST_RUN_ID") or f"pytest-{os.getpid()}"
        pytest_temp_parent = settings.temp_root / "pytest"
        pytest_temp_parent.mkdir(parents=True, exist_ok=True)
        config.option.basetemp = str(pytest_temp_parent / run_id)
    cache_path = settings.cache_root / "pytest"
    cache_path.mkdir(parents=True, exist_ok=True)
    cache_root = str(cache_path)
    if getattr(config.option, "cache_dir", None) in (None, ""):
        config.option.cache_dir = cache_root
    # pytest's cache plugin reads this ini value during pytest_configure.
    config._inicache["cache_dir"] = cache_root


@pytest.fixture(scope="session")
def root(isolate_project_environment):
    return Path(__file__).resolve().parents[1]


@pytest.fixture(scope="session", autouse=True)
def isolate_project_environment():
    """Keep repository and temporary-root tests independent of the developer .env."""
    original_loader = settings_module._load_local_env
    original_storage_loader = settings_module.Settings._load_storage_config
    original_preferences = settings_module.Settings.preferences
    settings_module._load_local_env = lambda _root: None
    settings_module.Settings._load_storage_config = lambda self: {}
    path_preference_names = {
        "crpd_data_root",
        "policy_archive_root",
        "policydb_database",
        "policydb_curated_root",
        "policydb_log_root",
        "policydb_output_root",
        "policydb_research_root",
    }
    settings_module.Settings.preferences = property(
        lambda self: {
            key: value
            for key, value in original_preferences.__get__(self, type(self)).items()
            if key not in path_preference_names
        }
    )
    environment_names = (
        "CRPD_DATA_ROOT",
        "POLICYDB_DATA_ROOT",
        "POLICYDB_CURATED_ROOT",
        "POLICYDB_DATABASE",
        "POLICYDB_RAW_ROOT",
        "POLICYDB_LOG_ROOT",
        "POLICYDB_OUTPUT_ROOT",
        "POLICYDB_RESEARCH_ROOT",
        "POLICYDB_AUTOMATION_ROOT",
        "POLICYDB_CONTROL_ROOT",
        "POLICYDB_RUNTIME_ROOT",
        "POLICYDB_CACHE_ROOT",
        "POLICYDB_TEMP_ROOT",
        "POLICYDB_TEST_ARTIFACTS_ROOT",
        "POLICYDB_DASHBOARD_ROOT",
        "POLICYDB_BACKUPS_ROOT",
        "POLICYDB_DASHBOARD_RUNTIME_ROOT",
    )
    previous = {name: os.environ.pop(name, None) for name in environment_names}
    try:
        yield
    finally:
        settings_module._load_local_env = original_loader
        settings_module.Settings._load_storage_config = original_storage_loader
        settings_module.Settings.preferences = original_preferences
        for name, value in previous.items():
            if value is not None:
                os.environ[name] = value


@pytest.fixture(scope="session")
def db(root):
    return PolicyDB.open(root)
