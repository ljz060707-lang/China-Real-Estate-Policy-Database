from __future__ import annotations

import polars as pl

from policydb.dedup_audit import materialize_policy_identity
from policydb.settings import Settings


def test_identity_materialization_keeps_versions_and_finds_exact_cluster(tmp_path):
    root = tmp_path / "repo"
    curated = root / "data/curated"
    curated.mkdir(parents=True)
    pl.DataFrame([{"record_id": "R1"}, {"record_id": "R2"}]).write_parquet(
        curated / "records.parquet"
    )
    pl.DataFrame(
        [
            {
                "document_version_id": "V1",
                "record_id": "R1",
                "source_id": "S1",
                "canonical_url": "https://a.example/1",
                "content_sha256": "a" * 64,
                "normalized_text_hash": "b" * 64,
                "policy_identity_key": "c" * 64,
            },
            {
                "document_version_id": "V2",
                "record_id": "R2",
                "source_id": "S2",
                "canonical_url": "https://b.example/2",
                "content_sha256": "a" * 64,
                "normalized_text_hash": "b" * 64,
                "policy_identity_key": "c" * 64,
            },
        ]
    ).write_parquet(curated / "policy_document_versions.parquet")
    result = materialize_policy_identity(Settings(root=root))
    assert result["entities"] == 2
    assert result["duplicate_clusters"] >= 1
    assert pl.read_parquet(curated / "policy_publications.parquet").height == 2
