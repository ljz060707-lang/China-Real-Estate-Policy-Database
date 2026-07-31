from __future__ import annotations

import shutil
from datetime import date
from pathlib import Path

import polars as pl

from policydb.api import PolicyDB
from policydb.crawl.registry import load_registry
from policydb.scope import load_cities_105
from policydb.seed_source_candidates import (
    audit_download_bytes,
    classify_seed_page,
    export_source_candidate_audit,
    generate_candidates_from_seed_records,
    is_official_gov_url,
)
from policydb.settings import Settings
from policydb.source_slots import build_requirement_slots, upsert_candidates


def _seed_root(tmp_path: Path) -> Settings:
    root = tmp_path / "repo"
    reference = root / "data" / "reference"
    curated = root / "data" / "curated"
    reference.mkdir(parents=True)
    curated.mkdir(parents=True)
    source_root = Path(__file__).resolve().parents[1]
    for name in ("cities_105.csv", "city_source_requirements.yaml"):
        shutil.copy2(source_root / "data" / "reference" / name, reference / name)
    (reference / "source_registry.yaml").write_text(
        "version: 2\nsources: []\n", encoding="utf-8"
    )
    settings = Settings(root=root)
    load_cities_105(settings).write_parquet(curated / "cities_105.parquet")
    pl.DataFrame(
        [
            {
                "record_id": "R_CONTENT",
                "title": "南京市住房保障政策通知",
                "record_date": date(2024, 1, 2),
                "geography_original": "南京市",
            },
            {
                "record_id": "R_PORTAL",
                "title": "南京市人民政府",
                "record_date": date(2024, 1, 3),
                "geography_original": "南京市",
            },
            {
                "record_id": "R_CONFLICT",
                "title": "跨市协同政策",
                "record_date": date(2024, 1, 4),
                "geography_original": "南京市、苏州市",
            },
            {
                "record_id": "R_NON_GOV",
                "title": "媒体转载",
                "record_date": date(2024, 1, 5),
                "geography_original": "南京市",
            },
        ],
        infer_schema_length=None,
    ).write_parquet(curated / "records.parquet")
    pl.DataFrame(
        [
            {
                "record_id": "R_CONTENT",
                "seed_url": "https://fcj.nanjing.gov.cn/2024/01/notice.html",
                "source_id": "S_HOUSING",
                "source_sheet": "T1",
                "source_cell": "G2",
            },
            {
                "record_id": "R_PORTAL",
                "seed_url": "https://www.nanjing.gov.cn/",
                "source_id": "S_PORTAL",
                "source_sheet": "T1",
                "source_cell": "G3",
            },
            {
                "record_id": "R_CONFLICT",
                "seed_url": "https://www.nanjing.gov.cn/",
                "source_id": "S_PORTAL",
                "source_sheet": "T1",
                "source_cell": "G4",
            },
            {
                "record_id": "R_NON_GOV",
                "seed_url": "https://example.com/reprint.html",
                "source_id": "S_MEDIA",
                "source_sheet": "T1",
                "source_cell": "G5",
            },
        ],
        infer_schema_length=None,
    ).write_parquet(curated / "source_seed_records.parquet")
    pl.DataFrame(
        [
            {
                "jurisdiction_id": "J_NJ",
                "name": "南京市",
                "name_full": "江苏省南京市",
                "name_normalized": "南京市",
                "administrative_code": "320100",
                "level": "city",
            },
            {
                "jurisdiction_id": "J_SZ",
                "name": "苏州市",
                "name_full": "江苏省苏州市",
                "name_normalized": "苏州市",
                "administrative_code": "320500",
                "level": "city",
            },
        ],
        infer_schema_length=None,
    ).write_parquet(curated / "jurisdictions.parquet")
    relation_rows = [
        {
            "record_id": "R_CONTENT",
            "jurisdiction_id": "J_NJ",
            "geography_original": "南京市",
            "jurisdiction_name": "南京市",
            "relation_type": "issuing_jurisdiction",
            "match_method": "exact",
            "match_confidence": 1.0,
        },
        {
            "record_id": "R_PORTAL",
            "jurisdiction_id": "J_NJ",
            "geography_original": "南京市",
            "jurisdiction_name": "南京市",
            "relation_type": "issuing_jurisdiction",
            "match_method": "exact",
            "match_confidence": 1.0,
        },
        {
            "record_id": "R_CONFLICT",
            "jurisdiction_id": "J_NJ",
            "geography_original": "南京市、苏州市",
            "jurisdiction_name": "南京市",
            "relation_type": "applicable_jurisdiction",
            "match_method": "alias",
            "match_confidence": 0.9,
        },
        {
            "record_id": "R_CONFLICT",
            "jurisdiction_id": "J_SZ",
            "geography_original": "南京市、苏州市",
            "jurisdiction_name": "苏州市",
            "relation_type": "applicable_jurisdiction",
            "match_method": "alias",
            "match_confidence": 0.9,
        },
        {
            "record_id": "R_NON_GOV",
            "jurisdiction_id": "J_NJ",
            "geography_original": "南京市",
            "jurisdiction_name": "南京市",
            "relation_type": "issuing_jurisdiction",
            "match_method": "exact",
            "match_confidence": 1.0,
        },
    ]
    pl.DataFrame(relation_rows, infer_schema_length=None).write_parquet(
        curated / "record_jurisdictions.parquet"
    )
    return settings


def test_official_domain_and_page_type_are_conservative():
    assert is_official_gov_url("https://fcj.nanjing.gov.cn/a")
    assert is_official_gov_url("https://gov.cn/")
    assert not is_official_gov_url("https://gov.cn.example.com/")
    assert not is_official_gov_url("https://example-gov.cn/")
    assert (
        classify_seed_page("https://fcj.nanjing.gov.cn/2024/01/notice.html")
        == "policy_content_page"
    )
    assert (
        classify_seed_page("https://www.nanjing.gov.cn/")
        == "site_or_column_entry"
    )
    sample = pl.DataFrame({"candidate_id": ["C1"]})
    assert audit_download_bytes(sample, ".csv")
    assert audit_download_bytes(sample, ".parquet")
    assert audit_download_bytes(sample, ".xlsx")


def test_seed_candidates_keep_provenance_and_never_enable(tmp_path):
    settings = _seed_root(tmp_path)
    first = generate_candidates_from_seed_records(settings)
    candidates = pl.read_parquet(settings.curated / "source_candidates.parquet")
    evidence = pl.read_parquet(
        settings.curated / "source_candidate_evidence.parquet"
    )
    slots = pl.read_parquet(settings.curated / "source_requirement_slots.parquet")

    assert first["rejected_non_gov_url_count"] == 1
    assert not candidates["is_verified"].any()
    assert not candidates["is_enabled"].any()
    assert not evidence["is_verified"].any()
    assert not evidence["is_enabled"].any()
    assert load_registry(settings) == []
    assert candidates.filter(
        (pl.col("city_id") == "CITY_320100")
        & (pl.col("source_role") == "housing_department")
        & (pl.col("candidate_kind") == "policy_content_evidence")
    ).height == 1
    assert candidates.filter(
        (pl.col("city_id") == "CITY_320100")
        & (pl.col("source_role") == "provident_fund_center")
        & (
            pl.col("candidate_kind")
            == "municipal_portal_substitute_candidate"
        )
    ).height >= 1
    assert slots.filter(
        (pl.col("city_id") == "CITY_320100")
        & (pl.col("source_role") == "provident_fund_center")
    )[0, "coverage_status"] == "municipal_portal_substitute_candidate"
    assert evidence.filter(pl.col("record_id") == "R_CONFLICT")[
        "needs_manual_review"
    ].all()
    assert {
        "record_id",
        "original_url",
        "jurisdiction_id",
        "relation_type",
        "generation_batch_id",
    }.issubset(evidence.columns)

    candidate_ids = set(candidates["candidate_id"].to_list())
    evidence_ids = set(evidence["evidence_id"].to_list())
    second = generate_candidates_from_seed_records(settings)
    assert second["candidate_count"] == first["candidate_count"]
    assert set(
        pl.read_parquet(settings.curated / "source_candidates.parquet")[
            "candidate_id"
        ].to_list()
    ) == candidate_ids
    assert set(
        pl.read_parquet(settings.curated / "source_candidate_evidence.parquet")[
            "evidence_id"
        ].to_list()
    ) == evidence_ids
    api_rows = PolicyDB(settings).source_candidate_audit(city="南京市")
    assert api_rows["slot_id"].n_unique() == 5


def test_audit_exports_csv_parquet_and_excel(tmp_path):
    settings = _seed_root(tmp_path)
    generate_candidates_from_seed_records(settings)
    result = export_source_candidate_audit(settings=settings)
    assert result["slot_count"] == 525
    assert result["candidate_count"] > 0
    assert {Path(path).suffix for path in result["outputs"]} == {
        ".csv",
        ".parquet",
        ".xlsx",
    }
    assert all(Path(path).exists() for path in result["outputs"])


def test_existing_enabled_candidate_keeps_status_but_seed_evidence_does_not(
    tmp_path,
):
    settings = _seed_root(tmp_path)
    build_requirement_slots(settings)
    upsert_candidates(
        [
            {
                "city_id": "CITY_320100",
                "source_role": "housing_department",
                "candidate_url": "https://fcj.nanjing.gov.cn/2024/01/notice.html",
                "discovery_method": "existing_registry",
                "is_official": True,
                "is_verified": True,
                "is_enabled": True,
                "manual_review_status": "approved",
            }
        ],
        settings,
    )
    generate_candidates_from_seed_records(settings)
    candidates = pl.read_parquet(settings.curated / "source_candidates.parquet")
    row = candidates.filter(
        (pl.col("city_id") == "CITY_320100")
        & (pl.col("source_role") == "housing_department")
        & (
            pl.col("canonical_url")
            == "https://fcj.nanjing.gov.cn/2024/01/notice.html"
        )
    ).row(0, named=True)
    evidence = pl.read_parquet(
        settings.curated / "source_candidate_evidence.parquet"
    ).filter(pl.col("candidate_id") == row["candidate_id"])
    assert row["is_enabled"] is True
    assert row["is_verified"] is True
    assert row["is_seed_derived"] is False
    assert row["has_seed_evidence"] is True
    assert row["discovery_method"] == "existing_registry"
    assert not evidence["is_enabled"].any()
    assert not evidence["is_verified"].any()
