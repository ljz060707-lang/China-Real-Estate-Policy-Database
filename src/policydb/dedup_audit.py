from __future__ import annotations

import json
from datetime import UTC, datetime

import polars as pl

from policydb.intensity.storage import atomic_write_parquet
from policydb.settings import Settings
from policydb.transform.normalization import stable_id


def materialize_policy_identity(settings: Settings | None = None) -> dict:
    settings = settings or Settings.discover()
    records = pl.read_parquet(settings.curated / "records.parquet")
    versions_path = settings.curated / "policy_document_versions.parquet"
    versions = pl.read_parquet(versions_path) if versions_path.exists() else pl.DataFrame()
    now = datetime.now(UTC).isoformat()
    entities = records.select("record_id").with_columns(
        pl.col("record_id").map_elements(
            lambda value: stable_id(value, prefix="ENTITY"), return_dtype=pl.String
        ).alias("policy_entity_id"),
        pl.lit("provisional_one_record").alias("entity_status"),
        pl.lit(now).alias("created_at"),
        pl.lit(now).alias("updated_at"),
    ).select("policy_entity_id", "record_id", "entity_status", "created_at", "updated_at")
    atomic_write_parquet(entities, settings.curated / "policy_entities.parquet")
    entity_lookup = dict(zip(entities["record_id"], entities["policy_entity_id"], strict=True))
    publication_rows = []
    for row in versions.iter_rows(named=True):
        publication_rows.append(
            {
                "publication_id": stable_id(
                    str(row.get("document_version_id")), prefix="PUBLICATION"
                ),
                "policy_entity_id": entity_lookup.get(row.get("record_id")),
                "record_id": row.get("record_id"),
                "document_version_id": row.get("document_version_id"),
                "source_id": row.get("source_id"),
                "canonical_url": row.get("canonical_url"),
                "publication_role": "unresolved",
                "created_at": now,
            }
        )
    publications = pl.DataFrame(publication_rows) if publication_rows else pl.DataFrame(
        schema={
            "publication_id": pl.String,
            "policy_entity_id": pl.String,
            "record_id": pl.String,
            "document_version_id": pl.String,
            "source_id": pl.String,
            "canonical_url": pl.String,
            "publication_role": pl.String,
            "created_at": pl.String,
        }
    )
    atomic_write_parquet(publications, settings.curated / "policy_publications.parquet")
    groups = []
    if versions.height:
        for key_name in ("content_sha256", "normalized_text_hash", "policy_identity_key"):
            valid = versions.filter(
                pl.col(key_name).is_not_null() & (pl.col(key_name).str.len_chars() > 0)
            )
            for group in valid.group_by(key_name).agg(
                pl.col("document_version_id"), pl.len().alias("member_count")
            ).filter(pl.col("member_count") > 1).iter_rows(named=True):
                members = sorted(str(value) for value in group["document_version_id"])
                groups.append(
                    {
                        "cluster_id": stable_id(key_name, str(group[key_name]), prefix="DUP"),
                        "cluster_type": key_name,
                        "member_document_version_ids": json.dumps(members),
                        "member_count": len(members),
                        "decision": (
                            "same_policy_same_version"
                            if key_name in {"content_sha256", "normalized_text_hash"}
                            else "uncertain"
                        ),
                        "decision_source": "deterministic_hash",
                        "review_required": key_name == "policy_identity_key",
                        "created_at": now,
                    }
                )
    clusters = pl.DataFrame(groups).unique("cluster_id") if groups else pl.DataFrame(
        schema={
            "cluster_id": pl.String,
            "cluster_type": pl.String,
            "member_document_version_ids": pl.String,
            "member_count": pl.Int64,
            "decision": pl.String,
            "decision_source": pl.String,
            "review_required": pl.Boolean,
            "created_at": pl.String,
        }
    )
    atomic_write_parquet(clusters, settings.curated / "policy_duplicate_clusters.parquet")
    output = settings.root / "outputs/dedup"
    output.mkdir(parents=True, exist_ok=True)
    clusters.write_csv(output / "dedup_audit_report.csv")
    return {
        "entities": entities.height,
        "publications": publications.height,
        "duplicate_clusters": clusters.height,
        "automatic_exact_clusters": clusters.filter(~pl.col("review_required")).height,
        "review_required": clusters.filter(pl.col("review_required")).height,
    }
