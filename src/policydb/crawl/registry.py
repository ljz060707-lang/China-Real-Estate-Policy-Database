from __future__ import annotations

import json
import os
import shutil
from datetime import UTC, datetime
from pathlib import Path

import polars as pl
import yaml

from policydb.crawl.dedup import canonicalize_url
from policydb.crawl.models import RegisteredSource
from policydb.settings import Settings
from policydb.transform.normalization import stable_id


def load_registry(settings: Settings | None = None) -> list[RegisteredSource]:
    settings = settings or Settings.discover()
    path = settings.root / "data" / "reference" / "source_registry.yaml"
    if not path.exists():
        return []
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return [RegisteredSource.model_validate(item) for item in data.get("sources", [])]


def save_registry_atomic(
    sources: list[RegisteredSource], settings: Settings | None = None, *, action: str,
    schema_version: int = 2,
) -> Path:
    settings = settings or Settings.discover()
    if settings.read_only:
        raise PermissionError("read-only deployment cannot modify source registry")
    path = settings.root / "data" / "reference" / "source_registry.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    now = datetime.now(UTC)
    if path.exists():
        backup_dir = path.parent / "backups"
        backup_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, backup_dir / f"source_registry_{now:%Y%m%dT%H%M%SZ}.yaml")
    payload = {
        "version": schema_version,
        "generated_at": now.isoformat(),
        "source_count": len(sources),
        "sources": [source.model_dump(mode="json", exclude_none=True) for source in sources],
    }
    temp = path.with_suffix(".yaml.tmp")
    temp.write_text(yaml.safe_dump(payload, allow_unicode=True, sort_keys=False), encoding="utf-8")
    os.replace(temp, path)
    log_path = settings.root / "data" / "logs" / "source_registry_changes.jsonl"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps({"at": now.isoformat(), "action": action, "source_count": len(sources)}, ensure_ascii=False) + "\n")
    return path


def materialize_registry_parquet(
    sources: list[RegisteredSource], settings: Settings | None = None
) -> Path:
    settings = settings or Settings.discover()
    path = settings.curated / "source_registry.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = [source.model_dump(mode="json", exclude_none=False) for source in sources]
    frame = pl.DataFrame(
        rows,
        infer_schema_length=None,
        schema_overrides={
            "seed_urls": pl.List(pl.String),
            "list_page_urls": pl.List(pl.String),
            "city_ids": pl.List(pl.String),
            "province_codes": pl.List(pl.String),
            "coverage_start_date": pl.String,
            "coverage_end_date": pl.String,
        },
    )
    temporary = path.with_suffix(".parquet.registry.tmp")
    frame.write_parquet(temporary, compression="zstd")
    os.replace(temporary, path)
    return path


def set_sources_enabled(
    source_ids: list[str], enabled: bool, settings: Settings | None = None
) -> dict:
    settings = settings or Settings.discover()
    selected = set(source_ids)
    now = datetime.now(UTC)
    sources = load_registry(settings)
    changed = 0
    updated = []
    for source in sources:
        if source.source_id in selected and source.crawl_enabled != enabled:
            source = source.model_copy(
                update={"crawl_enabled": enabled, "updated_at": now.isoformat()}
            )
            changed += 1
        updated.append(source)
    save_registry_atomic(updated, settings, action=f"set_enabled={enabled};changed={changed}")
    return {"changed": changed, "enabled": enabled}


def materialize_seed_record_links(settings: Settings | None = None) -> dict:
    settings = settings or Settings.discover()
    records_path = settings.curated / "records.parquet"
    if not records_path.exists():
        return {"rows": 0}
    records = pl.read_parquet(records_path)
    if "primary_source_url" not in records.columns:
        return {"rows": 0}
    source_by_url = {
        canonicalize_url(url): source.source_id
        for source in load_registry(settings)
        for url in source.seed_urls
        if url.startswith(("http://", "https://"))
    }
    now = datetime.now(UTC).isoformat()
    rows = []
    for row in records.iter_rows(named=True):
        url = str(row.get("primary_source_url") or "").strip()
        canonical = canonicalize_url(url) if url.startswith(("http://", "https://")) else ""
        source_id = source_by_url.get(canonical)
        if not source_id:
            continue
        rows.append(
            {
                "source_seed_record_id": stable_id(source_id, canonical, row["record_id"], prefix="SEEDREC"),
                "source_id": source_id,
                "seed_url": url,
                "record_id": row["record_id"],
                "source_sheet": row.get("source_sheet"),
                "source_cell": f"row:{row.get('source_row')}" if row.get("source_row") else None,
                "source_role": "primary_source",
                "created_at": now,
                "updated_at": now,
            }
        )
    path = settings.curated / "source_seed_records.parquet"
    if rows:
        pl.DataFrame(rows).write_parquet(path, compression="zstd")
    return {"rows": len(rows), "path": str(path)}
