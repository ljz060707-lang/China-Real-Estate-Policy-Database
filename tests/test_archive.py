from __future__ import annotations

import hashlib

import polars as pl
import pytest

from policydb.archive import archive_document_versions
from policydb.settings import Settings


def test_archive_is_content_addressed_and_does_not_modify_raw(tmp_path):
    root = tmp_path / "repo"
    curated = root / "data/curated"
    raw = root / "data/raw/webpages"
    archive = tmp_path / "archive"
    curated.mkdir(parents=True)
    raw.mkdir(parents=True)
    archive.mkdir()
    source = raw / "policy.html"
    source.write_bytes(b"<html>policy</html>")
    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    pl.DataFrame(
        [
            {
                "document_version_id": "V1",
                "record_id": "R1",
                "local_path": "data/raw/webpages/policy.html",
                "content_sha256": digest,
                "content_type": "text/html",
            }
        ]
    ).write_parquet(curated / "policy_document_versions.parquet")
    before = source.read_bytes()
    result = archive_document_versions(Settings(root=root), archive_root=archive)
    assert result["hash_verified"] == 1
    assert (archive / f"raw/html/{digest[:2]}/{digest}.html").exists()
    assert source.read_bytes() == before


def test_archive_missing_drive_does_not_fallback(tmp_path):
    root = tmp_path / "repo"
    (root / "data/curated").mkdir(parents=True)
    with pytest.raises(FileNotFoundError):
        archive_document_versions(
            Settings(root=root), archive_root=tmp_path / "missing"
        )
    assert not (root / "data/archive").exists()
