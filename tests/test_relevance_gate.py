from datetime import date

import polars as pl

from policydb.ingest.relevance import (
    assess_document_relevance,
    audit_recent_relevance,
    backfill_publication_dates,
)
from policydb.settings import Settings


def _settings(tmp_path):
    curated = tmp_path / "curated"
    curated.mkdir()
    return Settings(root=tmp_path, curated_path=curated, outputs_path=tmp_path / "outputs")


def test_source_role_alone_does_not_make_a_document_relevant():
    decision = assess_document_relevance("市建设局", "机构简介和联系方式", source_role="housing_department")
    assert decision.status == "OUT_OF_SCOPE"
    assert not decision.accepted


def test_non_policy_recent_documents_are_rejected_and_mixed_evidence_reviewed():
    rejected = assess_document_relevance("关于干部任免的通知", "干部任免公告", source_role="housing_department")
    mixed = assess_document_relevance("住房和城乡建设局人事任免", "住房政策与人事任免说明", source_role="housing_department")
    assert rejected.status == "REJECT_NON_POLICY"
    assert mixed.status == "RELEVANCE_REVIEW"


def test_recent_audit_demotes_without_deleting_and_backfills_only_null_publication_date(tmp_path):
    settings = _settings(tmp_path)
    pl.DataFrame(
        [
            {
                "document_version_id": "V_BAD",
                "record_id": "R_BAD",
                "crawl_item_id": "I1",
                "source_id": "SRC_TEST",
                "canonical_url": "https://example.gov.cn/personnel/1",
                "title": "关于干部任免的通知",
                "extracted_text": "干部任免公告",
            },
            {
                "document_version_id": "V_GOOD",
                "record_id": "R_GOOD",
                "crawl_item_id": "I2",
                "source_id": "SRC_TEST",
                "canonical_url": "https://example.gov.cn/housing/1",
                "title": "住房政策通知",
                "extracted_text": "住房政策实施办法",
            },
        ],
        infer_schema_length=None,
    ).write_parquet(settings.curated / "policy_document_versions.parquet")
    pl.DataFrame(
        [
            {"item_id": "I1", "run_id": "RUN_RECENT", "city_id": "CITY_1"},
            {"item_id": "I2", "run_id": "RUN_RECENT", "city_id": "CITY_1"},
        ],
        infer_schema_length=None,
    ).write_parquet(settings.curated / "crawl_items.parquet")
    pl.DataFrame(
        [
            {"record_id": "R_BAD", "record_type": "policy_document", "status": "issued", "notes": "raw"},
            {"record_id": "R_GOOD", "record_type": "policy_document", "status": "issued", "notes": "raw"},
        ],
        infer_schema_length=None,
    ).write_parquet(settings.curated / "records.parquet")

    result = audit_recent_relevance(settings, run_ids=["RUN_RECENT"], apply=True)
    assert result["rejected_versions"] == 1
    records = pl.read_parquet(settings.curated / "records.parquet")
    bad = records.filter(pl.col("record_id") == "R_BAD").row(0, named=True)
    assert bad["status"] == "excluded_non_policy"
    assert bad["record_type"] == "non_policy_evidence"
    assert records.height == 2
    rejects = pl.read_parquet(settings.outputs / "recent_30d" / "RECENT_30D_RELEVANCE_REJECTS.parquet")
    assert rejects[0, "document_version_id"] == "V_BAD"

    pl.DataFrame(
        [
            {"record_id": "R_BAD", "publication_date": None, "record_date": date(2026, 8, 1), "notes": "raw"},
            {"record_id": "R_GOOD", "publication_date": date(2026, 8, 2), "record_date": date(2026, 8, 1), "notes": "raw"},
        ],
        schema={
            "record_id": pl.String,
            "publication_date": pl.Date,
            "record_date": pl.Date,
            "notes": pl.String,
        },
    ).write_parquet(settings.curated / "records.parquet")
    backfill = backfill_publication_dates(settings, apply=True)
    assert backfill["backfilled"] == 1
    updated = pl.read_parquet(settings.curated / "records.parquet")
    assert updated.filter(pl.col("record_id") == "R_BAD")[0, "publication_date"] == date(2026, 8, 1)
    assert updated.filter(pl.col("record_id") == "R_GOOD")[0, "publication_date"] == date(2026, 8, 2)
