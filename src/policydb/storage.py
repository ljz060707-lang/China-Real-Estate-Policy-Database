from __future__ import annotations

import hashlib
import json
import os
import shutil
from datetime import UTC, datetime
from pathlib import Path

from policydb.config.preferences import PreferencesStore
from policydb.settings import Settings

RUNTIME_DIRECTORIES = (
    "archive/pdf",
    "archive/html",
    "archive/text",
    "archive/attachments",
    "curated",
    "research",
    "database",
    "manifests",
    "logs",
    "outputs",
    "jobs",
    "quarantine",
    "backups",
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def storage_plan(settings: Settings | None = None, *, target: Path | None = None) -> dict:
    settings = settings or Settings.discover()
    target = Path(target or settings.data_root)
    mappings = [
        (settings.root / "data" / "curated", target / "curated"),
        (settings.root / "data" / "research", target / "research"),
        (settings.root / "database", target / "database"),
        (settings.root / "outputs", target / "outputs"),
        (target / "raw", target / "archive"),
    ]
    return {
        "target": str(target),
        "target_exists": target.exists(),
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
    for relative in RUNTIME_DIRECTORIES:
        (target / relative).mkdir(parents=True, exist_ok=True)
    copied = verified = skipped = 0
    for mapping in plan["source_mappings"]:
        source = Path(mapping["source"])
        destination = Path(mapping["target"])
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
    missing = [relative for relative in RUNTIME_DIRECTORIES if not (target / relative).is_dir()]
    database = target / "database" / "policydb.duckdb"
    result = {
        "target": str(target),
        "available": target.exists(),
        "missing_directories": missing,
        "database_exists": database.exists(),
        "configured_data_root": str(settings.data_root),
        "configured_database": str(settings.database),
        "passed": target.exists() and not missing,
    }
    return result

