from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime

import polars as pl

from policydb.crawl.checkpoint import CRAWL_SCHEMAS, append_unique
from policydb.settings import Settings
from policydb.transform.normalization import stable_id

SCORING_VERSION = "v2.0.0"


@dataclass(frozen=True)
class ConfidenceComponents:
    source_authority: float
    evidence_coverage: float
    cross_source_agreement: float
    extraction_certainty: float
    entity_match: float

    @property
    def score(self) -> float:
        return round(
            0.30 * self.source_authority
            + 0.25 * self.evidence_coverage
            + 0.20 * self.cross_source_agreement
            + 0.15 * self.extraction_certainty
            + 0.10 * self.entity_match,
            6,
        )


def review_required(score: float, *, conflict: bool = False, official: bool = True) -> bool:
    return conflict or not official or score < 0.85


def record_field_confidence(
    *, record_id: str, field_name: str, field_value: object,
    components: ConfidenceComponents, document_version_id: str | None = None,
    evidence_excerpt: str | None = None, evidence_location: str | None = None,
    evidence_source_url: str | None = None, extraction_method: str = "rule",
    conflict_status: str = "none", conflicting_values: list[object] | None = None,
    model_version: str | None = None, prompt_version: str | None = None,
    settings: Settings | None = None,
) -> dict:
    settings = settings or Settings.discover()
    now = datetime.now(UTC).isoformat()
    conflict = conflict_status != "none"
    row = {
        "field_confidence_id": stable_id(record_id, document_version_id or "", field_name, str(field_value), SCORING_VERSION, prefix="FCONF"),
        "record_id": record_id, "document_version_id": document_version_id,
        "field_name": field_name, "field_value": None if field_value is None else str(field_value),
        "confidence_score": components.score,
        "source_authority_score": components.source_authority,
        "evidence_coverage_score": components.evidence_coverage,
        "cross_source_agreement_score": components.cross_source_agreement,
        "extraction_certainty_score": components.extraction_certainty,
        "entity_match_score": components.entity_match,
        "evidence_excerpt": evidence_excerpt, "evidence_location": evidence_location,
        "evidence_source_url": evidence_source_url, "extraction_method": extraction_method,
        "conflict_status": conflict_status,
        "conflicting_values": json.dumps(conflicting_values or [], ensure_ascii=False),
        "review_required": review_required(components.score, conflict=conflict),
        "scoring_version": SCORING_VERSION, "model_version": model_version,
        "prompt_version": prompt_version, "created_at": now, "updated_at": now,
    }
    append_unique(settings.curated / "field_confidence.parquet", [row], "field_confidence_id")
    return row


def record_confidence(scores: list[float], critical_scores: list[float]) -> float | None:
    values = critical_scores or scores
    if not values:
        return None
    return round(0.70 * (sum(values) / len(values)) + 0.30 * min(values), 6)


def materialize_field_confidence(settings: Settings | None = None) -> dict:
    settings = settings or Settings.discover()
    records_path = settings.curated / "records.parquet"
    if not records_path.exists():
        return {"rows": 0, "records": 0}
    records = pl.read_parquet(records_path)
    now = datetime.now(UTC).isoformat()
    rows: list[dict] = []
    authority = {
        "official": 1.0,
        "official_reprint": 0.85,
        "authoritative_media": 0.7,
        "general_media": 0.55,
        "self_media": 0.35,
        "rumour": 0.1,
    }
    for record in records.iter_rows(named=True):
        source_score = authority.get(str(record.get("official_status") or "unknown"), 0.35)
        evidence_text = "\n".join(
            str(record.get(name) or "") for name in ("title", "summary", "full_text")
        )
        for field_name in ("title", "record_date", "direction"):
            value = record.get(field_name)
            if value in (None, ""):
                continue
            value_text = str(value)
            evidence_score = 1.0 if value_text and value_text in evidence_text else 0.6
            components = ConfidenceComponents(
                source_score, evidence_score, 0.7, 0.75, 0.8
            )
            rows.append(
                {
                    "field_confidence_id": stable_id(
                        record["record_id"], field_name, value_text, SCORING_VERSION, prefix="FCONF"
                    ),
                    "record_id": record["record_id"], "document_version_id": None,
                    "field_name": field_name, "field_value": value_text,
                    "confidence_score": components.score,
                    "source_authority_score": components.source_authority,
                    "evidence_coverage_score": components.evidence_coverage,
                    "cross_source_agreement_score": components.cross_source_agreement,
                    "extraction_certainty_score": components.extraction_certainty,
                    "entity_match_score": components.entity_match,
                    "evidence_excerpt": evidence_text[:500] or None,
                    "evidence_location": f"{record.get('source_sheet')}!row:{record.get('source_row')}",
                    "evidence_source_url": record.get("primary_source_url"),
                    "extraction_method": "legacy_excel",
                    "conflict_status": "none", "conflicting_values": "[]",
                    "review_required": review_required(
                        components.score,
                        official=record.get("official_status") in {"official", "official_reprint"},
                    ),
                    "scoring_version": SCORING_VERSION, "model_version": None,
                    "prompt_version": None, "created_at": now, "updated_at": now,
                }
            )
    path = settings.curated / "field_confidence.parquet"
    incoming = pl.DataFrame(rows, schema=CRAWL_SCHEMAS["field_confidence"], infer_schema_length=None)
    current = pl.read_parquet(path) if path.exists() else pl.DataFrame(schema=CRAWL_SCHEMAS["field_confidence"])
    combined = pl.concat([current, incoming], how="diagonal_relaxed").unique(
        subset=["field_confidence_id"], keep="last", maintain_order=True
    )
    temporary = path.with_suffix(".parquet.confidence.tmp")
    combined.write_parquet(temporary, compression="zstd")
    temporary.replace(path)
    return {"rows": incoming.height, "total_rows": combined.height, "records": records.height}
