from __future__ import annotations

import hashlib

import duckdb
import polars as pl
import yaml

from policydb.settings import Settings
from policydb.transform.collections import build_collection_layer


def _sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def test_taxonomy_has_requested_seven_collections_and_34_subcollections(root):
    config = yaml.safe_load((root / "config" / "taxonomy.yml").read_text("utf-8"))
    collections = config["research_collections"]
    assert len(collections) == 7
    assert sum(len(value["subcollections"]) for value in collections.values()) == 34


def test_every_staging_sheet_has_one_primary_mapping(root):
    mappings = yaml.safe_load(
        (root / "config" / "collection_mapping.yml").read_text("utf-8")
    )["sheet_mappings"]
    staging_sheets = {
        pl.read_parquet(path, columns=["source_sheet_name"])["source_sheet_name"][0]
        for path in (root / "data" / "staging" / "excel").glob("*.parquet")
    }
    assert set(mappings) == staging_sheets
    assert all(mapping.get("primary") for mapping in mappings.values())


def test_all_staging_cells_are_queryable_in_duckdb(root):
    with duckdb.connect(str(root / "database" / "policydb.duckdb"), read_only=True) as con:
        count, sheets = con.execute(
            "SELECT count(*),count(DISTINCT source_sheet_name) FROM staging_excel_cells"
        ).fetchone()
    assert count == 91793
    assert sheets == 28


def test_all_records_have_collection_assignments(root):
    with duckdb.connect(str(root / "database" / "policydb.duckdb"), read_only=True) as con:
        row = con.execute("SELECT * FROM v_information_completeness").fetchone()
        missing_evidence = con.execute(
            "SELECT count(*) FROM record_collections "
            "WHERE confidence IS NULL OR evidence_excerpt IS NULL"
        ).fetchone()[0]
    assert row[:5] == (28, 91793, 28, 3568, 3568)
    assert missing_evidence == 0


def test_collection_build_is_idempotent_and_does_not_modify_raw(root):
    settings = Settings.discover(root)
    seed = next((root / "data" / "raw" / "seed").glob("*.xlsx"))
    before_hash = _sha256(seed)
    first = build_collection_layer(settings)
    second = build_collection_layer(settings)
    assert first["record_collection_relation_count"] == second[
        "record_collection_relation_count"
    ]
    assert first["staging_cell_count"] == second["staging_cell_count"]
    assert _sha256(seed) == before_hash
