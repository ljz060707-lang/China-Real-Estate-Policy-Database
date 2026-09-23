from __future__ import annotations

import hashlib
import json
from datetime import date
from pathlib import Path

import polars as pl
from openpyxl import Workbook

from policydb.ingest.external_reference import _year, import_reference_folder
from policydb.settings import Settings


def _write_city_registry(root: Path) -> None:
    reference = root / "data" / "reference"
    reference.mkdir(parents=True)
    (reference / "cities_105.csv").write_text(
        "city_id,city_name,city_name_short,province_name,aliases\n"
        "CITY_BJ,北京市,北京,北京市,北京|北京市\n"
        "CITY_SH,上海市,上海,上海市,上海|上海市\n",
        encoding="utf-8",
    )


def _write_reference_workbook(path: Path) -> None:
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "城市政策"
    sheet.append(["区域", "日期", "分类", "文件", "主要内容", "链接", "备注"])
    sheet.append(
        [
            "北京市",
            date(2024, 1, 2),
            "住房政策",
            "北京政策一",
            "北京市政策正文摘要",
            "https://example.gov.cn/beijing/1",
            None,
        ]
    )
    sheet.append(
        [
            "上海市",
            "2023-05-03",
            "住房政策",
            "上海政策一",
            "上海市政策正文摘要",
            "https://example.gov.cn/shanghai/1",
            None,
        ]
    )
    workbook.save(path)
    workbook.close()


def test_year_inference_does_not_treat_url_ids_as_dates() -> None:
    assert _year("2022年政策") == 2022
    assert _year("https://www.thepaper.cn/newsDetail_forward_20282923") is None


def test_external_reference_is_archived_and_reconciled_without_classification(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    _write_city_registry(root)
    curated = root / "data" / "curated"
    curated.mkdir(parents=True)
    pl.DataFrame(
        [{"item_id": "ITEM_BJ", "city_id": "CITY_BJ", "query_year": 2024}]
    ).write_parquet(curated / "crawl_items.parquet")
    pl.DataFrame(
        [
            {
                "document_version_id": "DOC_BJ",
                "crawl_item_id": "ITEM_BJ",
                "publication_date": date(2024, 1, 2),
            }
        ]
    ).write_parquet(curated / "policy_document_versions.parquet")

    source_dir = tmp_path / "external"
    source_dir.mkdir()
    source_file = source_dir / "city_policies.xlsx"
    _write_reference_workbook(source_file)

    result = import_reference_folder(source_dir, Settings(root=root))

    assert result["records"] == 2
    assert result["observed_city_years"] == 2
    assert result["classification_status"] == "NOT_RUN"
    assert result["api_calls"] == 0
    assert result["queue_mutation"] is False

    manifest_path = Path(result["manifest"])
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["formal_promotion"] is False
    assert manifest["queue_mutation"] is False
    summary = json.loads(
        Path(manifest["outputs"]["comparison_summary"]).read_text(encoding="utf-8")
    )
    assert summary["reconciliation_status_counts"]["REFERENCE_ONLY"] == 1
    assert summary["not_a_formal_coverage_claim"] is True
    assert manifest["source_files"][0]["sha256"] == hashlib.sha256(
        source_file.read_bytes()
    ).hexdigest()
    assert Path(manifest["source_files"][0]["raw_path"]).read_bytes() == source_file.read_bytes()

    reconciliation = pl.read_parquet(
        Path(manifest["outputs"]["reference_city_year_reconciliation"])
    )
    status_by_city_year = {
        (row["city_id"], row["year"]): row["reconciliation_status"]
        for row in reconciliation.iter_rows(named=True)
    }
    assert status_by_city_year[("CITY_BJ", 2024)] == "REFERENCE_AND_EXISTING"
    assert status_by_city_year[("CITY_SH", 2023)] == "REFERENCE_ONLY"

    records = pl.read_parquet(Path(manifest["outputs"]["reference_records"]))
    assert records["classification_status"].unique().to_list() == ["NOT_RUN"]
    assert records["formal_promotion_status"].unique().to_list() == ["NOT_PROMOTED"]

    second = import_reference_folder(source_dir, Settings(root=root))
    assert second["dataset_id"] == result["dataset_id"]
    assert second["records"] == result["records"]
    assert second["api_calls"] == 0
