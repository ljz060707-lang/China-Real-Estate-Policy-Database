from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime

import duckdb
import polars as pl

from policydb.intensity.models import DecisionCandidate, ModelDecision
from policydb.intensity.router import HybridDecisionRouter
from policydb.intensity.storage import atomic_write_parquet, upsert_parquet
from policydb.settings import Settings


def route_predictions(settings: Settings | None = None) -> dict:
    settings = settings or Settings.discover()
    dimensions_path = settings.curated / "policy_intensity_dimensions.parquet"
    if not dimensions_path.exists():
        return {"status": "blocked_missing_dimension_scores", "decisions": 0}
    dimensions = pl.read_parquet(dimensions_path).filter(pl.col("applicable"))
    predictions_path = settings.curated / "policy_model_predictions.parquet"
    predictions = pl.read_parquet(predictions_path) if predictions_path.exists() else pl.DataFrame()
    router = HybridDecisionRouter()
    decisions: list[ModelDecision] = []
    for row in dimensions.iter_rows(named=True):
        candidates = []
        if row.get("mapped_score") is not None:
            candidates.append(
                DecisionCandidate(
                    method="rule_dimension",
                    value=row.get("rubric_value") if row["dimension_code"] != "D8" else row["mapped_score"],
                    confidence=float(row.get("decision_confidence") or 0),
                    evidence_text=row.get("evidence_text"),
                    evidence_start=row.get("evidence_start"),
                    evidence_end=row.get("evidence_end"),
                )
            )
        if not predictions.is_empty():
            task = f"rubric_{row['dimension_code']}"
            for model_row in predictions.filter(
                (pl.col("action_id") == row["action_id"]) & (pl.col("task_name") == task)
            ).iter_rows(named=True):
                value = model_row.get("predicted_value")
                try:
                    value = json.loads(value) if value is not None else None
                except json.JSONDecodeError:
                    pass
                candidates.append(
                    DecisionCandidate(
                        method=f"model:{model_row['model_name']}",
                        value=value,
                        confidence=float(model_row.get("confidence") or 0),
                        evidence_text=model_row.get("evidence_text"),
                        evidence_start=model_row.get("evidence_start"),
                        evidence_end=model_row.get("evidence_end"),
                    )
                )
        decisions.append(
            router.route(
                record_id=row["record_id"],
                action_id=row["action_id"],
                task_name="calibration" if row["dimension_code"] == "D8" else f"rubric_{row['dimension_code']}",
                candidates=candidates,
            )
        )
    if decisions:
        frame = pl.DataFrame([item.model_dump(mode="json") for item in decisions], infer_schema_length=None).with_columns(
            pl.col("accepted_value").cast(pl.String, strict=False)
        )
        decision_path = settings.curated / "policy_model_decisions.parquet"
        if decision_path.exists():
            existing = pl.read_parquet(decision_path).filter(
                ~(
                    (pl.col("router_version") == router.version)
                    & pl.col("accepted_value").is_null()
                    & (pl.col("decision_reason") == "no_evidenced_candidate")
                )
            )
            atomic_write_parquet(existing, decision_path)
        upsert_parquet(frame, decision_path, "decision_id")
    return {
        "status": "completed",
        "decisions": len(decisions),
        "review_required": sum(item.review_required for item in decisions),
        "router_version": router.version,
    }


def create_model_review_tasks(settings: Settings | None = None) -> dict:
    settings = settings or Settings.discover()
    path = settings.curated / "policy_model_decisions.parquet"
    if not path.exists():
        return {"status": "blocked_missing_decisions", "created": 0}
    decisions = pl.read_parquet(path).filter(
        pl.col("review_required") & pl.col("accepted_value").is_not_null()
    )
    now = datetime.now(UTC).isoformat()
    created = 0
    with duckdb.connect(str(settings.database)) as connection:
        existing = {row[0] for row in connection.execute("SELECT task_id FROM manual_review_tasks").fetchall()}
        for row in decisions.iter_rows(named=True):
            reason = row.get("decision_reason") or ""
            review_type = (
                "glm_no_evidence"
                if reason == "no_evidenced_candidate"
                else "rule_glm_numeric_conflict"
                if "numeric" in reason and "conflict" in reason
                else "model_disagreement"
            )
            digest = hashlib.sha256((row["decision_id"] + review_type).encode()).hexdigest()[:24]
            task_id = f"REVIEW_INTENSITY_{digest}"
            if task_id in existing:
                continue
            connection.execute(
                """INSERT INTO manual_review_tasks
                   (task_id,record_id,review_type,field_name,old_value,suggested_value,
                    confidence,status,review_note,created_at,updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                [
                    task_id,
                    row["record_id"],
                    review_type,
                    row["task_name"],
                    None,
                    row.get("accepted_value"),
                    row.get("decision_confidence"),
                    "pending",
                    row.get("decision_reason"),
                    now,
                    now,
                ],
            )
            created += 1
    return {"status": "completed", "eligible": decisions.height, "created": created}


def validate_intensity(settings: Settings | None = None) -> dict:
    settings = settings or Settings.discover()
    required = [
        "policy_actions.parquet",
        "policy_action_calibrations.parquet",
        "policy_intensity_dimensions.parquet",
        "policy_intensity_scores.parquet",
    ]
    files = {name: (settings.curated / name).exists() for name in required}
    formal = provisional = 0
    if files["policy_intensity_scores.parquet"]:
        scores = pl.read_parquet(settings.curated / "policy_intensity_scores.parquet")
        formal = scores.filter(pl.col("formal_status") == "formal").height
        provisional = scores.filter(pl.col("formal_status") == "provisional").height
    return {
        "passed_structural": all(files.values()),
        "files": files,
        "formal_scores": formal,
        "provisional_scores": provisional,
        "formal_benchmark_passed": False,
        "research_ready": False,
        "blocking_reasons": ["adjudicated gold is empty", "held-out model thresholds not evaluated"],
    }
