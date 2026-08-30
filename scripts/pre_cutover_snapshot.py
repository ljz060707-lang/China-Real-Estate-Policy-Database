"""CRPD pre-cutover snapshot — freeze the OLD production state (read-only).

Writes CRPD_PRE_CUTOVER_MANIFEST.json capturing: production DB SHA-256 +
table row counts + schema version, source registry state, release pointers,
CRPD config, EP930 frozen scope/hash. The old production is never modified.
"""
from __future__ import annotations

import hashlib
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import polars as pl

DATA_ROOT = Path(r"E:\Data Set\CRPD")
DB = DATA_ROOT / "database" / "policydb.duckdb"
OUT = DATA_ROOT / "reports" / "runs" / "CRPD_CUTOVER_20260821" / "CRPD_PRE_CUTOVER_MANIFEST.json"

KEY_TABLES = [
    "records", "policy_document_versions", "policy_actions", "policy_classifications",
    "documents", "attachments", "dedup_decisions", "crawl_items", "fetch_errors",
    "record_terms", "source_registry", "source_requirement_slots", "source_candidates",
    "coverage_gaps", "crawl_source_windows", "manual_review_tasks",
]


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def main() -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, ValueError):
        pass
    OUT.parent.mkdir(parents=True, exist_ok=True)

    row_counts: dict[str, int] = {}
    with duckdb.connect(str(DB), read_only=True) as con:
        for table in KEY_TABLES:
            try:
                row_counts[table] = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            except Exception:  # noqa: BLE001
                row_counts[table] = -1
        tables = con.execute("SELECT count(*) FROM information_schema.tables").fetchone()[0]
        views = con.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_type='VIEW'"
        ).fetchone()[0]

    registry_path = DATA_ROOT / "curated" / "source_registry.parquet"
    registry = pl.read_parquet(registry_path) if registry_path.exists() else None

    from policydb.platform.episode_adapter import verify_frozen_scope

    ep930 = verify_frozen_scope()

    manifest = {
        "schema": "CRPD_PRE_CUTOVER_MANIFEST",
        "created_at": datetime.now(UTC).isoformat(),
        "old_production": {
            "data_root": str(DATA_ROOT),
            "database": str(DB),
            "database_sha256": sha256(DB),
            "database_size_bytes": DB.stat().st_size,
            "tables": tables,
            "views": views,
            "row_counts": row_counts,
        },
        "source_registry": {
            "parquet": str(registry_path),
            "rows": registry.height if registry is not None else -1,
            "enabled": int(registry.filter(pl.col("crawl_enabled")).height) if registry is not None else -1,
            "sha256": sha256(registry_path) if registry_path.exists() else "",
        },
        "release_pointers": {
            "repo_historical": [p.name for p in Path(r"E:\policy-database\data\releases").glob("*")] if Path(r"E:\policy-database\data\releases").exists() else [],
            "note": "historical releases moved to E:\\Data Set\\CRPD\\releases\\historical\\repo_legacy during storage cleanup",
        },
        "config": {
            "env_CRPD_DATA_ROOT": __import__("os").environ.get("CRPD_DATA_ROOT", ""),
            "env_POLICYDB_DATABASE": __import__("os").environ.get("POLICYDB_DATABASE", ""),
        },
        "ep930": {
            "frozen_scope_intact": ep930["frozen_scope_intact"],
            "recorded_frozen_scope_hash": ep930["recorded_frozen_scope_hash"],
            "frozen_queue_items": ep930.get("frozen_queue_items"),
            "city_count": ep930.get("city_count"),
            "frozen_output_dir": str(Path(r"E:\Data Set\CRPD\outputs\special_projects\2016_930")),
        },
    }
    OUT.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(manifest, ensure_ascii=False, indent=2))
    print(f"manifest -> {OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
