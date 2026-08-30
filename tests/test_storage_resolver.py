from __future__ import annotations

from pathlib import Path

from policydb.settings import Settings


def test_storage_resolution_priority_is_explicit_then_env_then_config_then_fallback(
    tmp_path: Path, monkeypatch
) -> None:
    explicit = tmp_path / "explicit"
    environment = tmp_path / "environment"
    configured = tmp_path / "configured"

    monkeypatch.setattr(Settings, "_load_storage_config", lambda self: {"data_root": str(configured)})
    monkeypatch.setenv("CRPD_DATA_ROOT", str(environment))
    assert Settings(root=tmp_path, data_root_path=explicit).data_root == explicit
    assert Settings(root=tmp_path).data_root == environment

    monkeypatch.delenv("CRPD_DATA_ROOT")
    assert Settings(root=tmp_path).data_root == configured

    monkeypatch.setattr(Settings, "_load_storage_config", lambda self: {})
    assert Settings(root=tmp_path).data_root == tmp_path / "data"


def test_derived_paths_are_under_data_root_and_property_access_does_not_create_directories(
    tmp_path: Path,
) -> None:
    data_root = tmp_path / "external-data"
    settings = Settings(root=tmp_path, data_root_path=data_root)

    expected = {
        "database_root": data_root / "database",
        "curated": data_root / "curated",
        "raw": data_root / "raw",
        "outputs": data_root / "outputs",
        "logs": data_root / "logs",
        "automation": data_root / "automation",
        "control": data_root / "control",
        "runtime": data_root / "runtime",
        "cache": data_root / "cache",
        "temp": data_root / "temp",
        "test_artifacts": data_root / "test_artifacts",
        "dashboard": data_root / "dashboard",
        "backups": data_root / "backups",
        "dashboard_runtime": data_root / "runtime" / "dashboard",
        "jobs": data_root / "jobs",
    }
    actual = {name: getattr(settings, name) for name in expected}
    assert actual == expected
    assert settings.database == data_root / "database" / "policydb.duckdb"
    assert not data_root.exists()


def test_tmp_project_does_not_read_real_project_storage_config(tmp_path: Path) -> None:
    config = tmp_path / "config" / "storage.yaml"
    config.parent.mkdir()
    config.write_text("data_root: 'E:/Data Set/CRPD'\n", encoding="utf-8")

    settings = Settings(root=tmp_path)

    assert settings.data_root == tmp_path / "data"
    assert "e:/data set/crpd" not in str(settings.data_root).lower()


def test_legacy_archive_preference_remains_supported(tmp_path: Path, monkeypatch) -> None:
    archive = tmp_path / "archive-from-preferences"
    monkeypatch.setattr(Settings, "preferences", property(lambda self: {"policy_archive_root": str(archive)}))

    assert Settings(root=tmp_path).policy_archive_root == archive


def test_archive_environment_overrides_legacy_preference(tmp_path: Path, monkeypatch) -> None:
    configured = tmp_path / "archive-from-preferences"
    isolated = tmp_path / "archive-from-environment"
    monkeypatch.setattr(
        Settings,
        "preferences",
        property(lambda self: {"policy_archive_root": str(configured)}),
    )
    monkeypatch.setenv("CRPD_ARCHIVE_ROOT", str(isolated))

    assert Settings(root=tmp_path).policy_archive_root == isolated


def test_explicit_archive_path_isolates_smoke_from_production_env(
    tmp_path: Path,
    monkeypatch,
) -> None:
    production = tmp_path / "production-archive"
    isolated = tmp_path / "smoke-archive"
    monkeypatch.setenv("CRPD_ARCHIVE_ROOT", str(production))

    settings = Settings(root=tmp_path, archive_path=isolated)

    assert settings.archive_root == isolated
