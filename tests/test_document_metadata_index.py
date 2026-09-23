import polars as pl

from policydb.crawl.pipeline import _is_reusable_document_version, read_document_metadata
from policydb.parquet_store import merge_and_replace_parquet


def test_metadata_update_preserves_large_text_and_extra_columns(tmp_path):
    path = tmp_path / "versions.parquet"
    before = pl.DataFrame({"id": ["old"], "extracted_text": ["原文" * 1000], "last_seen_at": ["first"], "user_note": ["keep"]})
    before.write_parquet(path)
    result = merge_and_replace_parquet(
        path, pl.DataFrame({"id": ["new"], "extracted_text": ["新文"]}), ("id",),
        column_updates=pl.DataFrame({"id": ["old"], "last_seen_at": ["later"]}),
    )
    old = result.filter(pl.col("id") == "old").row(0, named=True)
    assert old["extracted_text"] == "原文" * 1000
    assert old["user_note"] == "keep"
    assert old["last_seen_at"] == "later"
    assert result.height == 2


def test_metadata_index_excludes_text_and_preserves_text_only_cache(tmp_path):
    path = tmp_path / "versions.parquet"
    pl.DataFrame({"document_version_id": ["v1"], "extracted_text": ["正文"], "title": [None], "local_path": [None], "http_status": [200], "content_sha256": ["hash"], "parse_status": ["parsed"]}).write_parquet(path)
    index = read_document_metadata(path)
    assert "extracted_text" not in index.columns
    assert _is_reusable_document_version(index.row(0, named=True))
