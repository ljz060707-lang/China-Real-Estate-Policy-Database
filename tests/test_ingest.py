import json

import polars as pl

from policydb.ingest.excel import file_hash, inventory_excel, parse_date, slug_sheet


def seed(root):
    return root / "data" / "raw" / "seed" / "【中金不动产与空间服务】政策数据库 20260705.xlsx"


def test_seed_exists(root):
    assert seed(root).exists()


def test_seed_hash(root):
    assert (
        file_hash(seed(root)) == "829951c7e88eebdffd729b96c1aac7ccf4c37037a11e6f8df8da9e36605039bf"
    )


def test_inventory_28(root):
    assert inventory_excel(seed(root))["sheet_count"] == 28


def test_staging_28(root):
    assert len(list((root / "data" / "staging" / "excel").glob("*.parquet"))) == 28


def test_staging_lineage(root):
    f = next((root / "data" / "staging" / "excel").glob("*T1*"))
    cols = set(pl.read_parquet(f).columns)
    assert {"source_row", "source_cell", "source_file_sha256", "is_formula"} <= cols


def test_parse_date():
    assert str(parse_date("2024年5月17日")) == "2024-05-17"


def test_slug_sheet():
    assert "/" not in slug_sheet("a/b")


def test_manifest_main_count(root):
    assert (
        json.loads(
            (root / "data" / "staging" / "import_manifest.json").read_text(encoding="utf-8")
        )["main_policy_count"]
        == 3011
    )
