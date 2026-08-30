from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import fitz
import polars as pl
import pytest

from policydb.dashboard_metrics import pdf_city_coverage, pdf_processing_funnel, pdf_summary
from policydb.pdf_pipeline import (
    PDF_ATTACHMENT_SCHEMA,
    PDFPipeline,
    PDFPipelineConfig,
    load_pdf_config,
    safe_pdf_asset_path,
)
from policydb.settings import Settings


def _settings(tmp_path: Path) -> Settings:
    curated = tmp_path / "data" / "curated"
    curated.mkdir(parents=True)
    return Settings(root=tmp_path, curated_path=curated)


def _pdf_bytes(text: str | None = None) -> bytes:
    document = fitz.open()
    page = document.new_page()
    if text:
        page.insert_text((72, 72), text)
    body = document.tobytes()
    document.close()
    return body


def _write_pdf(path: Path, text: str | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(_pdf_bytes(text))


def test_inventory_archive_is_append_only_and_duplicate_aware(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    incoming = tmp_path / "incoming"
    first = incoming / "one.pdf"
    second = incoming / "nested" / "copy.pdf"
    _write_pdf(first, "policy text")
    second.parent.mkdir(parents=True)
    second.write_bytes(first.read_bytes())
    (incoming / "invalid.pdf").write_bytes(b"not a pdf")
    pipeline = PDFPipeline(settings, config=PDFPipelineConfig(inventory_root=incoming, archive_root=settings.data_root / "raw" / "pdf"))

    inventory = pipeline.inventory()
    assert inventory["recorded"] == 3
    assert inventory["valid_pdf"] == 2
    assert inventory["invalid_pdf"] == 1
    archived = pipeline.archive()
    assert archived["groups"] == 1
    assert archived["copied"] == 1
    assets = pl.read_parquet(settings.curated / "pdf_assets.parquet")
    assert assets.height == 1
    assert len(assets[0, "original_paths"]) > 10
    rerun = pipeline.archive()
    assert rerun["copied"] == 0
    assert pl.read_parquet(settings.curated / "pdf_assets.parquet").height == 1


def test_parse_text_and_scanned_candidate_without_ocr(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    incoming = tmp_path / "incoming"
    _write_pdf(incoming / "text.pdf", "a policy page with enough extracted characters to demonstrate deterministic text parsing without treating the page as a scanned candidate")
    _write_pdf(incoming / "scan.pdf")
    pipeline = PDFPipeline(settings, config=PDFPipelineConfig(inventory_root=incoming, archive_root=settings.data_root / "raw" / "pdf"))
    pipeline.archive()
    pipeline.match()
    result = pipeline.parse()
    assert result["parsed"] == 1
    assert result["ocr_pending"] == 1
    assert result["failed"] == 0
    text_rows = pl.read_parquet(settings.curated / "pdf_text_versions.parquet")
    assert text_rows.height == 2
    attachments = pl.read_parquet(settings.curated / "document_attachments.parquet")
    assert set(attachments["parse_status"].to_list()) == {"PARSED", "OCR_PENDING"}
    assert pipeline.summary()["ocr_enabled"] is False


class _FakeFetcher:
    def __init__(self, body: bytes) -> None:
        self.body = body
        self.calls = 0

    def fetch(self, url: str, *, referer: str | None = None):
        del url, referer
        self.calls += 1
        return SimpleNamespace(
            body=self.body,
            content_type="application/pdf",
            status_code=200,
            final_url="https://city.gov.cn/file.pdf",
            redirect_chain=[],
            network_route="direct",
        )


def _add_pending_attachment(pipeline: PDFPipeline) -> None:
    row = pipeline._attachment_row(
        document_id="R1",
        parent_document_id="R1",
        asset_sha=None,
        city_id="CITY_A",
        source_id="SRC_A",
        discovered_url="https://city.gov.cn/file.pdf",
        resolved_url=None,
        discovery_page_url="https://city.gov.cn/list",
        anchor_text="政策附件",
        filename="file.pdf",
        role="SUPPORTING_ATTACHMENT",
        primary=False,
        method="test",
        local_path=None,
        evidence={"test": True},
    )
    pipeline._ensure_storage()
    from policydb.pdf_pipeline import _upsert_rows

    _upsert_rows(pipeline._curated("document_attachments"), [row], PDF_ATTACHMENT_SCHEMA, "attachment_id", run_id="TEST")


def test_download_is_direct_bounded_and_idempotent(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    config = PDFPipelineConfig(inventory_root=tmp_path / "incoming", archive_root=settings.data_root / "raw" / "pdf", max_downloads_per_source=1, max_downloads_per_job=1)
    fetcher = _FakeFetcher(_pdf_bytes("downloaded policy"))
    pipeline = PDFPipeline(settings, config=config, fetcher=fetcher)
    _add_pending_attachment(pipeline)
    first = pipeline.download(limit=10, run_id="GET_TEST")
    assert first["selected"] == 1
    assert first["downloaded"] == 1
    assert fetcher.calls == 1
    second = pipeline.download(limit=10, run_id="GET_TEST_2")
    assert second["selected"] == 0
    assert fetcher.calls == 1
    assert pdf_summary(settings)["downloaded"] == 1
    assert pdf_city_coverage(settings)[0, "linked"] == 1
    assert "downloaded" in pdf_processing_funnel(settings)["stage"].to_list()


def test_invalid_pdf_body_is_quarantined(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    pipeline = PDFPipeline(settings, config=PDFPipelineConfig(inventory_root=tmp_path / "incoming", archive_root=settings.data_root / "raw" / "pdf"), fetcher=_FakeFetcher(b"<html>not a PDF</html>"))
    _add_pending_attachment(pipeline)
    result = pipeline.download(limit=1)
    assert result["quarantined"] == 1
    attachments = pl.read_parquet(settings.curated / "document_attachments.parquet")
    assert attachments[0, "download_status"] == "QUARANTINE_REVIEW"
    assert list((settings.data_root / "raw" / "pdf" / "quarantine").glob("*.bin"))


def test_summary_scopes_rates_to_pdf_records(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    pipeline = PDFPipeline(settings, config=PDFPipelineConfig(inventory_root=tmp_path / "incoming", archive_root=settings.data_root / "raw" / "pdf"))
    pdf_row = pipeline._attachment_row(
        document_id="PDF_RECORD",
        parent_document_id="PDF_RECORD",
        asset_sha=None,
        city_id="CITY_A",
        source_id="SRC_A",
        discovered_url="https://city.gov.cn/file.pdf",
        resolved_url=None,
        discovery_page_url="https://city.gov.cn/list",
        anchor_text="PDF",
        filename="file.pdf",
        role="ANNEX",
        primary=False,
        method="test",
        local_path=None,
        evidence={"test": True},
    )
    non_pdf_row = {**pdf_row, "attachment_id": "DOC_ATTACHMENT", "discovered_url": "https://city.gov.cn/file.doc", "filename": "file.doc", "content_type": "application/msword"}
    pdf_row["download_status"] = "RETRY_WAIT"
    non_pdf_row["download_status"] = "RETRY_WAIT"
    pipeline._ensure_storage()
    from policydb.pdf_pipeline import _upsert_rows

    _upsert_rows(pipeline._curated("document_attachments"), [pdf_row, non_pdf_row], PDF_ATTACHMENT_SCHEMA, "attachment_id", run_id="TEST")
    summary = pipeline.summary()
    assert summary["pdf_attachments"] == 1
    assert summary["download_failures"] == 1
    assert summary["pdf_download_rate"]["denominator"] == 1
    assert summary["pdf_parse_rate"]["denominator"] == 0


def test_safe_pdf_viewer_only_resolves_content_addressed_asset(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    pipeline = PDFPipeline(settings, config=PDFPipelineConfig(inventory_root=tmp_path / "incoming", archive_root=settings.data_root / "raw" / "pdf"))
    pipeline._ensure_storage()
    from policydb.pdf_pipeline import PDF_ASSET_SCHEMA, _write_table

    _write_table(pipeline._curated("pdf_assets"), pl.DataFrame([{"pdf_asset_id": "BAD", "archive_path": "../outside.pdf"}], schema=PDF_ASSET_SCHEMA), PDF_ASSET_SCHEMA, key_columns=("pdf_asset_id",), run_id="TEST")
    assert safe_pdf_asset_path(settings, "BAD") is None
    assert safe_pdf_asset_path(settings, "../../outside") is None


def test_ocr_configuration_cannot_be_enabled(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="OCR"):
        PDFPipelineConfig(ocr_enabled=True).validate()


def test_explicit_isolated_settings_override_static_pdf_paths(tmp_path: Path) -> None:
    root = tmp_path / "project"
    (root / "config").mkdir(parents=True)
    (root / "config" / "pdf_pipeline.yaml").write_text(
        "pdf_pipeline:\n"
        "  inventory_root: 'E:/Data Set/CRPD'\n"
        "  archive_root: 'E:/Data Set/CRPD/raw/pdf'\n",
        encoding="utf-8",
    )
    isolated = tmp_path / "isolated"
    settings = Settings(root=root, data_root_path=isolated)
    config = load_pdf_config(settings)
    assert config.inventory_root == isolated.resolve()
    assert config.archive_root == (isolated / "raw" / "pdf").resolve()
