from __future__ import annotations

from datetime import UTC, datetime

import polars as pl

from policydb.intensity.storage import atomic_write_parquet
from policydb.parquet_store import read_parquet_snapshot
from policydb.settings import Settings

VALID_PRIMARY = {"D", "S", "F", "H", "G"}


def materialize_policy_pools(settings: Settings | None = None) -> dict:
    settings = settings or Settings.discover()
    records = read_parquet_snapshot(settings.curated / "records.parquet")
    entities_path = settings.curated / "policy_entities.parquet"
    entities = read_parquet_snapshot(entities_path) if entities_path.exists() else pl.DataFrame()
    actions_path = settings.curated / "policy_actions.parquet"
    classes_path = settings.curated / "policy_classifications.parquet"
    actions = read_parquet_snapshot(actions_path) if actions_path.exists() else pl.DataFrame()
    classes = read_parquet_snapshot(classes_path) if classes_path.exists() else pl.DataFrame()
    entity_lookup = (
        dict(zip(entities["record_id"], entities["policy_entity_id"], strict=False))
        if entities.height
        else {}
    )
    class_by_action = {
        row["action_id"]: row for row in classes.iter_rows(named=True)
    } if classes.height else {}
    record_lookup = {row["record_id"]: row for row in records.iter_rows(named=True)}
    assessments: dict[str, list[dict]] = {}
    for action in actions.iter_rows(named=True):
        record = record_lookup.get(action.get("record_id"))
        classification = class_by_action.get(action.get("action_id"))
        if not record or not classification:
            continue
        full_text = str(record.get("full_text") or "")
        evidence = str(classification.get("evidence_text") or action.get("evidence_text") or "").strip()
        evidence_located = bool(evidence and evidence in full_text)
        official = record.get("official_status") in {"official", "official_reprint"}
        text_complete = len(full_text.strip()) >= 200 and action.get("text_completeness") in {
            "full_official_text", "partial_official_text"
        }
        category_valid = classification.get("primary_category") in VALID_PRIMARY
        no_conflict = classification.get("review_status") not in {
            "conflict", "manual_review_required", "rejected"
        }
        confidence = float(classification.get("confidence") or 0.0)
        score = min(
            confidence,
            1.0 if official else 0.6,
            1.0 if text_complete else 0.6,
            1.0 if evidence_located else 0.0,
            1.0 if category_valid and no_conflict else 0.0,
        )
        reasons = []
        if not official:
            reasons.append("official_source_unverified")
        if not text_complete:
            reasons.append("text_incomplete")
        if not evidence_located:
            reasons.append("evidence_not_located")
        if not category_valid:
            reasons.append("invalid_category")
        if not no_conflict:
            reasons.append("classification_conflict")
        assessments.setdefault(action["record_id"], []).append(
            {"score": score, "reasons": reasons, "action_id": action["action_id"]}
        )
    now = datetime.now(UTC).isoformat()
    stock_rows = []
    review_rows = []
    for record_id, record in record_lookup.items():
        items = assessments.get(record_id, [])
        score = min((item["score"] for item in items), default=0.0)
        reasons = sorted({reason for item in items for reason in item["reasons"]})
        if not items:
            reasons.append("no_policy_action")
        base = {
            "policy_entity_id": entity_lookup.get(record_id),
            "record_id": record_id,
            "title": record.get("title"),
            "record_date": record.get("record_date"),
            "official_status": record.get("official_status"),
            "action_count": len(items),
            "composite_confidence": score,
            "updated_at": now,
        }
        if items and score >= 0.90 and not reasons:
            stock_rows.append({**base, "pool_status": "formal_stock"})
        else:
            if not items:
                route = "pending_automatic_extraction"
            elif "classification_conflict" in reasons:
                route = "manual_review_required"
            elif reasons:
                route = "automatic_source_recovery"
            else:
                route = "second_automatic_review"
            review_rows.append(
                {
                    **base,
                    "pool_status": route,
                    "review_reasons": ";".join(reasons) or "confidence_below_stock_threshold",
                }
            )
    stock = pl.DataFrame(stock_rows, infer_schema_length=None) if stock_rows else pl.DataFrame(
        schema={
            "policy_entity_id": pl.String, "record_id": pl.String, "title": pl.String,
            "record_date": pl.Date, "official_status": pl.String, "action_count": pl.Int64,
            "composite_confidence": pl.Float64, "updated_at": pl.String, "pool_status": pl.String,
        }
    )
    review = pl.DataFrame(review_rows, infer_schema_length=None) if review_rows else pl.DataFrame(
        schema={
            "policy_entity_id": pl.String, "record_id": pl.String, "title": pl.String,
            "record_date": pl.Date, "official_status": pl.String, "action_count": pl.Int64,
            "composite_confidence": pl.Float64, "updated_at": pl.String, "pool_status": pl.String,
            "review_reasons": pl.String,
        }
    )
    atomic_write_parquet(stock, settings.curated / "policy_stock_pool.parquet")
    atomic_write_parquet(review, settings.curated / "policy_increment_review_pool.parquet")
    return {
        "records": records.height,
        "formal_stock": stock.height,
        "pending_automatic_extraction": review.filter(pl.col("pool_status") == "pending_automatic_extraction").height,
        "automatic_source_recovery": review.filter(pl.col("pool_status") == "automatic_source_recovery").height,
        "second_automatic_review": review.filter(pl.col("pool_status") == "second_automatic_review").height,
        "manual_review_required": review.filter(pl.col("pool_status") == "manual_review_required").height,
    }
