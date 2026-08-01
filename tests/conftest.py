import os
from pathlib import Path

import pytest

from policydb import PolicyDB
from policydb import settings as settings_module


@pytest.fixture(scope="session")
def root(isolate_project_environment):
    return Path(__file__).resolve().parents[1]


@pytest.fixture(scope="session", autouse=True)
def isolate_project_environment():
    """Keep repository and temporary-root tests independent of the developer .env."""
    original_loader = settings_module._load_local_env
    original_preferences = settings_module.Settings.preferences
    settings_module._load_local_env = lambda _root: None
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
        "POLICYDB_CURATED_ROOT",
        "POLICYDB_DATABASE",
        "POLICYDB_LOG_ROOT",
        "POLICYDB_OUTPUT_ROOT",
        "POLICYDB_RESEARCH_ROOT",
    )
    previous = {name: os.environ.pop(name, None) for name in environment_names}
    try:
        yield
    finally:
        settings_module._load_local_env = original_loader
        settings_module.Settings.preferences = original_preferences
        for name, value in previous.items():
            if value is not None:
                os.environ[name] = value


@pytest.fixture(scope="session")
def db(root):
    return PolicyDB.open(root)
