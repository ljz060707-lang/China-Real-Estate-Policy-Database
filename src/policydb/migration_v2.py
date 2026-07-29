from __future__ import annotations

import hashlib
import json
import os
import shutil
from datetime import UTC, datetime
from pathlib import Path

import polars as pl
import yaml

from policydb.crawl.checkpoint import CRAWL_SCHEMAS
from policydb.crawl.models import RegisteredSource
from policydb.crawl.registry import (
    load_registry,
    materialize_registry_parquet,
    save_registry_atomic,
)
from policydb.settings import Settings

SCHEMA_VERSION = 2
NATIONAL_DOMAINS = {
    "gov.cn", "mohurd.gov.cn", "pbc.gov.cn", "nfra.gov.cn", "cbirc.gov.cn",
    "csrc.gov.cn", "ndrc.gov.cn", "chinatax.gov.cn",
}
CORE_FACTS = ("crawl_source_windows", "dedup_decisions", "field_confidence")
EXTENDED_FACTS = ("crawl_items", "policy_document_versions", "llm_extractions", "llm_verifications")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _backup(settings: Settings) -> Path:
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    target = settings.root / "outputs" / "v2_migration_backup" / stamp
    target.mkdir(parents=True, exist_ok=False)
    candidates = [
        settings.database,
        settings.root / "data" / "reference" / "source_registry.yaml",
        *(settings.curated / f"{name}.parquet" for name in EXTENDED_FACTS),
    ]
    manifest: list[dict[str, object]] = []
    for source in candidates:
        if not source.exists():
            continue
        relative = source.relative_to(settings.root)
        destination = target / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)
        manifest.append(
            {"path": str(relative), "sha256": _sha256(source), "size": source.stat().st_size}
        )
    (target / "backup_manifest.json").write_text(
        json.dumps({"schema_version": SCHEMA_VERSION, "files": manifest}, indent=2),
        encoding="utf-8",
    )
    return target


def _align_parquet(path: Path, schema: dict[str, pl.DataType]) -> bool:
    if path.exists():
        frame = pl.read_parquet(path)
    else:
        frame = pl.DataFrame(schema=schema)
    changed = False
    for name, dtype in schema.items():
        if name not in frame.columns:
            frame = frame.with_columns(pl.lit(None, dtype=dtype).alias(name))
            changed = True
    if not path.exists():
        changed = True
    if changed:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(".parquet.v2.tmp")
        frame.write_parquet(temporary, compression="zstd")
        os.replace(temporary, path)
    return changed


def _migrate_source(source: RegisteredSource) -> RegisteredSource:
    data = source.model_dump(mode="python")
    if not data.get("city_ids") and data.get("city_id"):
        data["city_ids"] = [str(data["city_id"])]
    jurisdiction_level = str(data.get("jurisdiction_level") or "unknown")
    if data.get("scope_type") == "unknown":
        data["scope_type"] = {
            "national": "national",
            "province": "provincial",
            "provincial": "provincial",
            "city": "municipal",
            "municipal": "municipal",
            "county": "county",
        }.get(jurisdiction_level, "unknown")
        if data.get("domain") in NATIONAL_DOMAINS:
            data["scope_type"] = "national"
    if data.get("official_status") in {"official", "official_reprint"}:
        data["required_level"] = (
            "required"
            if data.get("domain")
            in {"gov.cn", "mohurd.gov.cn", "pbc.gov.cn", "nfra.gov.cn", "csrc.gov.cn"}
            else "recommended"
        )
    data["homepage_url"] = data.get("homepage_url") or (
        f"https://{data['domain']}" if data.get("domain") else None
    )
    data["parser_version"] = str(data.get("parser_version") or "1")
    return RegisteredSource.model_validate(data)


def migration_plan(settings: Settings | None = None) -> dict:
    settings = settings or Settings.discover()
    registry_path = settings.root / "data" / "reference" / "source_registry.yaml"
    registry_version = 0
    if registry_path.exists():
        registry_version = int((yaml.safe_load(registry_path.read_text(encoding="utf-8")) or {}).get("version", 0))
    return {
        "schema_version_from": registry_version,
        "schema_version_to": SCHEMA_VERSION,
        "source_count": len(load_registry(settings)),
        "create_facts": [name for name in CORE_FACTS if not (settings.curated / f"{name}.parquet").exists()],
        "extend_facts": [name for name in EXTENDED_FACTS if (settings.curated / f"{name}.parquet").exists()],
        "raw_writes": 0,
        "database_exists": settings.database.exists(),
    }


def apply_migration(settings: Settings | None = None) -> dict:
    settings = settings or Settings.discover()
    if settings.read_only:
        raise PermissionError("V2 migration is disabled in read-only mode")
    plan = migration_plan(settings)
    backup = _backup(settings)
    changed: list[str] = []
    for name in (*CORE_FACTS, *EXTENDED_FACTS):
        if _align_parquet(settings.curated / f"{name}.parquet", CRAWL_SCHEMAS[name]):
            changed.append(name)
    sources = [_migrate_source(source) for source in load_registry(settings)]
    save_registry_atomic(sources, settings, action="migrate_v2", schema_version=SCHEMA_VERSION)
    materialize_registry_parquet(sources, settings)
    result = {**plan, "backup": str(backup), "changed": changed, "applied": True}
    result.update(verify_migration(settings))
    output = settings.root / "outputs" / "v2_migration_report.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    return result


def verify_migration(settings: Settings | None = None) -> dict:
    settings = settings or Settings.discover()
    sources = load_registry(settings)
    missing_columns: dict[str, list[str]] = {}
    for name in (*CORE_FACTS, *EXTENDED_FACTS):
        path = settings.curated / f"{name}.parquet"
        columns = set(pl.read_parquet_schema(path)) if path.exists() else set()
        missing = sorted(set(CRAWL_SCHEMAS[name]) - columns)
        if missing:
            missing_columns[name] = missing
    records = pl.scan_parquet(settings.curated / "records.parquet")
    t1 = records.filter(pl.col("source_sheet") == "T1 房地产政策目录").select(
        pl.len().alias("count"), pl.col("record_date").min().alias("min_date"), pl.col("record_date").max().alias("max_date")
    ).collect().row(0, named=True)
    registry_path = settings.root / "data" / "reference" / "source_registry.yaml"
    registry_version = int((yaml.safe_load(registry_path.read_text(encoding="utf-8")) or {}).get("version", 0))
    passed = not missing_columns and registry_version == SCHEMA_VERSION and t1["count"] == 3011
    return {
        "verified": passed,
        "registry_version": registry_version,
        "source_count_after": len(sources),
        "missing_columns": missing_columns,
        "t1_count": t1["count"],
        "t1_min_date": str(t1["min_date"]),
        "t1_max_date": str(t1["max_date"]),
    }
