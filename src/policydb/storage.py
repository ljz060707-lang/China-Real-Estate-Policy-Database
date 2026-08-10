from __future__ import annotations

import hashlib
import json
import os
import shutil
from datetime import UTC, datetime
from pathlib import Path

from policydb.config.preferences import PreferencesStore
from policydb.settings import Settings

# These are logical names, not a second path configuration.  Their concrete
# locations are always resolved from Settings (or an explicit migration target)
# by ``storage_directories`` below.  Keep the tuple for callers that imported
# the old public constant.
RUNTIME_DIRECTORIES = (
    "database",
    "curated",
    "raw",
    "archive",
    "archive/pdf",
    "archive/html",
    "archive/text",
    "archive/attachments",
    "research",
    "outputs",
    "logs",
    "automation",
    "control",
    "runtime",
    "runtime/dashboard",
    "cache",
    "temp",
    "test_artifacts",
    "dashboard",
    "backups",
    "manifests",
    "jobs",
    "quarantine",
)


def storage_directories(
    settings: Settings | None = None, *, target: Path | None = None
) -> dict[str, Path]:
    """Return the concrete storage layout without creating directories.

    Settings remains the only source of path truth.  ``target`` is intentionally
    an explicit migration destination and therefore derives the same relative
    layout below that data root.  The function is read-only and safe to call
    from checks, tests, and CLI planning commands.
    """

    settings = settings or Settings.discover()
    data_root = Path(target or settings.data_root)
    return {
        "database": data_root / "database",
        "curated": data_root / "curated",
        "raw": data_root / "raw",
        "archive": data_root / "archive",
        "research": data_root / "research",
        "outputs": data_root / "outputs",
        "logs": data_root / "logs",
        "automation": data_root / "automation",
        "control": data_root / "control",
        "runtime": data_root / "runtime",
        "runtime/dashboard": data_root / "runtime" / "dashboard",
        "cache": data_root / "cache",
        "temp": data_root / "temp",
        "test_artifacts": data_root / "test_artifacts",
        "dashboard": data_root / "dashboard",
        "backups": data_root / "backups",
        "manifests": data_root / "manifests",
        "jobs": data_root / "jobs",
        "quarantine": data_root / "quarantine",
        "archive/pdf": data_root / "archive" / "pdf",
        "archive/html": data_root / "archive" / "html",
        "archive/text": data_root / "archive" / "text",
        "archive/attachments": data_root / "archive" / "attachments",
    }


def _migration_sources(settings: Settings, target: Path) -> list[tuple[Path, Path]]:
    """List known repository-local sources that can be copied into the target.

    This is deliberately conservative: it only describes the legacy locations
    already handled by this migration module and never treats the target as a
    source.  The migration remains copy+hash+atomic and never deletes sources.
    """

    directories = storage_directories(settings, target=target)
    candidates = (
        (settings.root / "data" / "curated", directories["curated"]),
        (settings.root / "data" / "research", directories["research"]),
        (settings.root / "data" / "raw", directories["raw"]),
        (settings.root / "data" / "archive", directories["archive"]),
        (settings.root / "database", directories["database"]),
        (settings.root / "outputs", directories["outputs"]),
        (settings.root / "logs", directories["logs"]),
    )
    seen: set[tuple[str, str]] = set()
    result: list[tuple[Path, Path]] = []
    for source, destination in candidates:
        key = (str(source.resolve()), str(destination.resolve()))
        if key not in seen:
            result.append((source, destination))
            seen.add(key)
    return result


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def storage_plan(settings: Settings | None = None, *, target: Path | None = None) -> dict:
    settings = settings or Settings.discover()
    target = Path(target or settings.data_root)
    directories = storage_directories(settings, target=target)
    mappings = _migration_sources(settings, target)
    return {
        "target": str(target),
        "target_exists": target.exists(),
        "directories": {name: str(path) for name, path in directories.items()},
        "runtime_directories": [str(directories[name]) for name in RUNTIME_DIRECTORIES],
        "source_mappings": [
            {
                "source": str(source),
                "target": str(destination),
                "source_exists": source.exists(),
                "file_count": sum(1 for item in source.rglob("*") if item.is_file())
                if source.exists()
                else 0,
            }
            for source, destination in mappings
        ],
        "operation": "copy_verify_then_switch",
        "deletes_source": False,
    }


def migrate_storage(
    settings: Settings | None = None,
    *,
    target: Path | None = None,
    confirm: bool = False,
) -> dict:
    settings = settings or Settings.discover()
    target = Path(target or settings.data_root)
    plan = storage_plan(settings, target=target)
    if not confirm:
        return {**plan, "confirmation_required": True}
    if target.drive and not Path(target.drive + "\\").exists():
        raise FileNotFoundError(f"CRPD data drive is unavailable: {target.drive}")
    target.mkdir(parents=True, exist_ok=True)
    directories = storage_directories(settings, target=target)
    for directory in directories.values():
        directory.mkdir(parents=True, exist_ok=True)
    copied = verified = skipped = 0
    for source, destination in _migration_sources(settings, target):
        if not source.exists() or source.resolve() == destination.resolve():
            continue
        for item in source.rglob("*"):
            if not item.is_file():
                continue
            relative = item.relative_to(source)
            output = destination / relative
            output.parent.mkdir(parents=True, exist_ok=True)
            source_hash = _sha256(item)
            if output.exists():
                if _sha256(output) != source_hash:
                    raise FileExistsError(f"Target conflict with different content: {output}")
                skipped += 1
                verified += 1
                continue
            temporary = output.with_suffix(output.suffix + ".tmp")
            shutil.copy2(item, temporary)
            if _sha256(temporary) != source_hash:
                temporary.unlink(missing_ok=True)
                raise OSError(f"SHA-256 verification failed: {item}")
            os.replace(temporary, output)
            copied += 1
            verified += 1
    preferences = settings.preferences.copy()
    preferences.update(
        {
            "crpd_data_root": str(target),
            "policy_archive_root": str(target / "archive"),
            "policydb_database": str(target / "database" / "policydb.duckdb"),
            "policydb_curated_root": str(target / "curated"),
            "policydb_research_root": str(target / "research"),
            "policydb_log_root": str(target / "logs"),
            "policydb_output_root": str(target / "outputs"),
        }
    )
    PreferencesStore(settings.preferences_path).save(preferences)
    manifest = {
        "created_at": datetime.now(UTC).isoformat(),
        "target": str(target),
        "copied": copied,
        "verified": verified,
        "skipped_existing": skipped,
        "source_deleted": False,
    }
    manifest_path = target / "manifests" / "storage_migration.json"
    temporary = manifest_path.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(temporary, manifest_path)
    return {**manifest, "confirmation_required": False, "manifest": str(manifest_path)}


def verify_storage(settings: Settings | None = None, *, target: Path | None = None) -> dict:
    settings = settings or Settings.discover()
    target = Path(target or settings.data_root)
    directories = storage_directories(settings, target=target)
    missing = [name for name in RUNTIME_DIRECTORIES if not directories[name].is_dir()]
    database = target / "database" / "policydb.duckdb"
    result = {
        "target": str(target),
        "available": target.exists(),
        "missing_directories": missing,
        "directories": {name: str(path) for name, path in directories.items()},
        "directory_status": {
            name: {"path": str(path), "exists": path.is_dir()}
            for name, path in directories.items()
        },
        "database_exists": database.exists(),
        "configured_data_root": str(settings.data_root),
        "configured_database": str(settings.database),
        "passed": target.exists() and not missing,
    }
    return result
