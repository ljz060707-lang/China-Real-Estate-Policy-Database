from datetime import date

import polars as pl

from policydb.ingest.promote_versions import promote_document_versions
from policydb.settings import Settings


def _settings(tmp_path):
    curated = tmp_path / "curated"
    curated.mkdir()
    return Settings(root=tmp_path, curated_path=curated)


def _write_fixture(settings, *, version_id="V1", item_id="I1", run_id="R1", title="房地产政策通知", body="住房政策实施办法正文"):
    now = "2026-08-11T00:00:00+00:00"
    pl.DataFrame(
        [
            {
                "document_version_id": version_id,
                "record_id": None,
                "crawl_item_id": item_id,
                "source_id": "SRC_TEST",
                "canonical_url": f"https://example.gov.cn/detail/{version_id}",
                "final_url": f"https://example.gov.cn/detail/{version_id}",
                "content_sha256": f"hash-{version_id}",
                "local_path": f"archive/html/{version_id}.html",
                "content_type": "text/html",
                "http_status": 200,
                "title": title,
                "extracted_text": body,
                "parse_status": "parsed",
                "is_material_change": True,
                "first_seen_at": now,
                "last_seen_at": now,
                "created_at": now,
                "updated_at": now,
                "normalized_text_hash": f"text-{version_id}",
                "simhash64": "1",
                "policy_identity_key": f"identity-{version_id}",
                "parser_version": "2",
                "network_route": "direct_ok",
                "redirect_chain_json": "[]",
                "protocol": "HTTP/1.1",
            }
        ],
        infer_schema_length=None,
    ).write_parquet(settings.curated / "policy_document_versions.parquet")
    pl.DataFrame(
        [
            {
                "item_id": item_id,
                "run_id": run_id,
                "city_id": None,
                "candidate_date": "2026-08-01" if version_id == "V1" else None,
                "candidate_date_source": "structured_date" if version_id == "V1" else None,
            }
        ],
        infer_schema_length=None,
    ).write_parquet(settings.curated / "crawl_items.parquet")


def test_promotion_propagates_date_and_is_idempotent(tmp_path):
    settings = _settings(tmp_path)
    _write_fixture(settings)

    first = promote_document_versions(settings, run_id="R1", apply=True)
    assert first["selected_versions"] == 1
    assert first["eligible_versions"] == 1
    assert first["new_records"] == 1

    records = pl.read_parquet(settings.curated / "records.parquet")
    assert records.height == 1
    assert records[0, "record_date"] == date(2026, 8, 1)
    assert records[0, "publication_date"] == date(2026, 8, 1)
    assert records[0, "full_text"] == "住房政策实施办法正文"
    versions = pl.read_parquet(settings.curated / "policy_document_versions.parquet")
    assert versions[0, "record_id"] == records[0, "record_id"]
    assert versions[0, "publication_date"] == date(2026, 8, 1)
    assert versions[0, "publication_date_source"] == "structured_date"

    second = promote_document_versions(settings, run_id="R1", apply=True)
    assert second["new_records"] == 0
    assert second["updated_records"] == 1
    assert pl.read_parquet(settings.curated / "records.parquet").height == 1


def test_missing_date_stays_null_and_invalid_version_is_rejected(tmp_path):
    settings = _settings(tmp_path)
    _write_fixture(settings, version_id="V2", item_id="I2", run_id="R2")

    result = promote_document_versions(settings, run_id="R2", apply=True)
    assert result["eligible_versions"] == 1
    records = pl.read_parquet(settings.curated / "records.parquet")
    assert records[0, "record_date"] is None
    assert records[0, "publication_date"] is None

    _write_fixture(settings, version_id="V3", item_id="I3", run_id="R3", title="", body="")
    rejected = promote_document_versions(settings, run_id="R3", apply=False)
    assert rejected["selected_versions"] == 1
    assert rejected["eligible_versions"] == 0
    assert rejected["rejected_versions"] == 1
