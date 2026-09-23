import hashlib
import json

import polars as pl

from policydb.crawl.reparse import reparse_archived_versions
from policydb.settings import Settings


def test_offline_repair_retains_raw_snapshot_identity_and_history(tmp_path):
    settings = Settings(root=tmp_path)
    settings.curated.mkdir(parents=True)
    raw = tmp_path / "raw.rar"
    body = b"Rar!\x1a\x07\0" + b"binary" * 50
    raw.write_bytes(body)
    row = {"document_version_id": "v1", "local_path": str(raw), "content_sha256": hashlib.sha256(body).hexdigest(), "extracted_text": body.decode(errors="replace"), "parse_status": "parsed", "parser_version": "2", "normalized_text_hash": "old", "simhash64": "old", "first_seen_at": "2024-01-01", "last_seen_at": "2024-02-01", "title": "原始标题"}
    path = settings.curated / "policy_document_versions.parquet"
    pl.DataFrame([row]).write_parquet(path)
    before = path.read_bytes()
    preview = reparse_archived_versions(settings, tmp_path / "preview")
    assert not preview["applied"] and preview["repairable"] == 1
    assert path.read_bytes() == before
    summary = reparse_archived_versions(settings, tmp_path / "applied", apply=True)
    assert summary["documents_after"] == 1
    assert (tmp_path / "applied" / "policy_document_versions.before.parquet").read_bytes() == before
    new = pl.read_parquet(path).row(0, named=True)
    assert new["extracted_text"] == ""
    assert new["parse_status"] == "deferred_unsupported"
    for key in ("title", "content_sha256", "local_path", "first_seen_at", "last_seen_at"):
        assert new[key] == row[key]
    assert raw.read_bytes() == body
    assert json.loads((tmp_path / "applied" / "summary.json").read_text())["network_calls"] == 0
    repeat = reparse_archived_versions(settings, tmp_path / "repeat", apply=True)
    assert repeat["suspects"] == 0


def test_missing_or_changed_raw_is_not_silently_cleared(tmp_path):
    settings = Settings(root=tmp_path)
    settings.curated.mkdir(parents=True)
    path = settings.curated / "policy_document_versions.parquet"
    pl.DataFrame({"document_version_id": ["v1"], "extracted_text": ["Rar!fake"], "content_sha256": ["a" * 64], "local_path": [str(tmp_path / "missing.rar")]}).write_parquet(path)
    before = path.read_bytes()
    summary = reparse_archived_versions(settings, tmp_path / "check", apply=True)
    assert summary["unresolved"] == 1 and not summary["applied"]
    assert path.read_bytes() == before
