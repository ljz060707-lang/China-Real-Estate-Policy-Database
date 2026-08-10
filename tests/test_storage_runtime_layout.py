from __future__ import annotations

from pathlib import Path

from policydb.settings import Settings
from policydb.storage import RUNTIME_DIRECTORIES, storage_directories, storage_plan, verify_storage


def test_storage_directories_are_derived_from_settings_data_root(tmp_path: Path) -> None:
    data_root = tmp_path / "external-data"
    settings = Settings(root=tmp_path, data_root_path=data_root)

    directories = storage_directories(settings)

    assert directories["database"] == data_root / "database"
    assert directories["curated"] == data_root / "curated"
    assert directories["raw"] == data_root / "raw"
    assert directories["archive"] == data_root / "archive"
    assert directories["runtime/dashboard"] == data_root / "runtime" / "dashboard"
    assert directories["test_artifacts"] == data_root / "test_artifacts"
    assert directories["backups"] == data_root / "backups"
    assert not data_root.exists()


def test_storage_plan_and_verify_use_the_same_resolved_layout(tmp_path: Path) -> None:
    target = tmp_path / "CRPD"
    settings = Settings(root=tmp_path)

    plan = storage_plan(settings, target=target)
    assert set(RUNTIME_DIRECTORIES).issubset(plan["directories"].keys())
    assert plan["directories"]["database"] == str(target / "database")
    assert plan["directories"]["runtime/dashboard"] == str(target / "runtime" / "dashboard")

    target.mkdir()
    result = verify_storage(settings, target=target)
    assert result["passed"] is False
    assert "database" in result["missing_directories"]
    assert result["directory_status"]["runtime/dashboard"]["path"] == str(
        target / "runtime" / "dashboard"
    )
