from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from policydb.intensity.rules import DeterministicPolicyRules
from policydb.intensity.scoring import aggregate_action_score, score_dimensions
from policydb.intensity.storage import atomic_write_parquet, upsert_parquet
from policydb.query.database import build_database
from policydb.settings import Settings


class PolicyIntensityService:
    """Runs deterministic action extraction and experimental scoring without touching Raw."""

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or Settings.discover()
        self.reference = self.settings.root / "data" / "reference"
        self.output = self.settings.root / "outputs" / "policy_intensity"
        self.rules = DeterministicPolicyRules(self.reference)

    @staticmethod
    def _rows(models: list) -> pl.DataFrame:
        if not models:
            return pl.DataFrame()
        return pl.DataFrame([item.model_dump(mode="json") for item in models], infer_schema_length=None)

    def extract(self, *, limit: int | None = None, formal_only: bool = False) -> dict:
        self._ensure_model_fact_tables()
        records = pl.read_parquet(self.settings.curated / "records.parquet")
        records = records.filter(pl.col("full_text").is_not_null())
        if formal_only:
            records = records.filter(pl.col("official_status").is_in(["official", "official_reprint"]))
        if limit is not None:
            records = records.head(limit)
        version_lookup: dict[str, str] = {}
        versions_path = self.settings.curated / "policy_document_versions.parquet"
        if versions_path.exists():
            versions = pl.read_parquet(versions_path).filter(pl.col("record_id").is_not_null())
            if not versions.is_empty():
                version_lookup = dict(
                    versions.group_by("record_id").agg(pl.col("document_version_id").last()).iter_rows()
                )
        actions = []
        calibrations = []
        for row in records.iter_rows(named=True):
            extracted = self.rules.extract_actions(
                record_id=row["record_id"],
                text=row.get("full_text") or "",
                title=row.get("title"),
                official_status=row.get("official_status") or "unknown",
                document_version_id=version_lookup.get(row["record_id"]),
            )
            actions.extend(extracted)
            for action in extracted:
                calibrations.extend(self.rules.extract_calibrations(action))
        action_frame = self._rows(actions)
        calibration_frame = self._rows(calibrations)
        if not action_frame.is_empty():
            upsert_parquet(action_frame, self.settings.curated / "policy_actions.parquet", "action_id")
        if not calibration_frame.is_empty():
            upsert_parquet(
                calibration_frame,
                self.settings.curated / "policy_action_calibrations.parquet",
                "calibration_id",
            )
        report = {
            "generated_at": datetime.now(UTC).isoformat(),
            "records_scanned": records.height,
            "actions": len(actions),
            "formal_eligible_actions": sum(action.formal_eligible for action in actions),
            "provisional_actions": sum(not action.formal_eligible for action in actions),
            "calibrations": len(calibrations),
            "paired_calibrations": sum(item.pairing_status == "paired" for item in calibrations),
            "raw_modified": False,
            "rules_version": self.rules.version,
        }
        self._write_json("extraction_report.json", report)
        return report

    def _ensure_model_fact_tables(self) -> None:
        prediction_path = self.settings.curated / "policy_model_predictions.parquet"
        if not prediction_path.exists():
            atomic_write_parquet(
                pl.DataFrame(
                    schema={
                        "prediction_id": pl.String,
                        "action_id": pl.String,
                        "record_id": pl.String,
                        "task_name": pl.String,
                        "model_name": pl.String,
                        "model_version": pl.String,
                        "prompt_version": pl.String,
                        "schema_version": pl.String,
                        "predicted_value": pl.String,
                        "confidence": pl.Float64,
                        "evidence_text": pl.String,
                        "evidence_start": pl.Int64,
                        "evidence_end": pl.Int64,
                        "cache_key": pl.String,
                        "created_at": pl.String,
                    }
                ),
                prediction_path,
            )

    def score(self) -> dict:
        action_path = self.settings.curated / "policy_actions.parquet"
        if not action_path.exists():
            return {"status": "blocked_missing_actions", "scores": 0}
        actions = pl.read_parquet(action_path)
        calibrations_path = self.settings.curated / "policy_action_calibrations.parquet"
        calibrations = pl.read_parquet(calibrations_path) if calibrations_path.exists() else pl.DataFrame()
        from policydb.intensity.models import ActionCalibration, PolicyAction

        dimension_models = []
        score_models = []
        for row in actions.iter_rows(named=True):
            action = PolicyAction.model_validate(row)
            action_calibrations = []
            if not calibrations.is_empty():
                action_calibrations = [
                    ActionCalibration.model_validate(item)
                    for item in calibrations.filter(pl.col("action_id") == action.action_id).iter_rows(named=True)
                ]
            dimensions = score_dimensions(action, action_calibrations)
            dimension_models.extend(dimensions)
            authority = 1.0 if action.text_completeness == "full_official_text" else 0.7
            quality = min(1.0, 0.5 + 0.5 * (action.formal_eligible))
            score_models.append(
                aggregate_action_score(
                    action,
                    dimensions,
                    source_authority=authority,
                    data_quality=quality,
                )
            )
        dimension_frame = self._rows(dimension_models)
        score_frame = self._rows(score_models)
        if not dimension_frame.is_empty():
            upsert_parquet(
                dimension_frame,
                self.settings.curated / "policy_intensity_dimensions.parquet",
                "score_id",
            )
        if not score_frame.is_empty():
            upsert_parquet(
                score_frame,
                self.settings.curated / "policy_intensity_scores.parquet",
                "score_id",
            )
        report = {
            "generated_at": datetime.now(UTC).isoformat(),
            "actions_scored": len(score_models),
            "dimension_scores": len(dimension_models),
            "formal_scores": sum(item.formal_status == "formal" for item in score_models),
            "provisional_scores": sum(item.formal_status == "provisional" for item in score_models),
            "research_ready": False,
            "score_version": "0.1.0-experimental",
        }
        self._write_json("scoring_report.json", report)
        return report

    def aggregate(self) -> dict:
        score_path = self.settings.curated / "policy_intensity_scores.parquet"
        links_path = self.settings.curated / "policy_applicable_cities.parquet"
        records_path = self.settings.curated / "records.parquet"
        if not score_path.exists() or not links_path.exists():
            return {"status": "blocked_missing_scores_or_city_links", "rows": 0}
        scores = pl.read_parquet(score_path)
        records = pl.read_parquet(records_path).select("record_id", "record_date", "official_level")
        links = pl.read_parquet(links_path).filter(~pl.col("needs_review"))
        actions = pl.read_parquet(self.settings.curated / "policy_actions.parquet").select(
            "action_id", "direction", "instrument"
        )
        base = scores.join(actions, on="action_id", how="left").join(records, on="record_id", how="left").join(
            links.select("record_id", "city_id").unique(), on="record_id", how="inner"
        ).filter(pl.col("record_date").is_not_null())
        if base.is_empty():
            panel = pl.DataFrame()
        else:
            base = base.with_columns(
                pl.col("record_date").dt.year().alias("year"),
                pl.col("record_date").dt.month().alias("month"),
            )
            panel = base.group_by("city_id", "year", "month").agg(
                pl.col("record_id").n_unique().alias("policy_count"),
                pl.col("action_id").n_unique().alias("action_count"),
                pl.col("textual_policy_design_intensity").mean().alias("mean_design_intensity"),
                pl.col("textual_policy_design_intensity").sum().alias("gross_design_intensity"),
                pl.col("textual_policy_design_intensity").filter(pl.col("direction") == "loosening").sum().alias("loosening_intensity"),
                pl.col("textual_policy_design_intensity").filter(pl.col("direction") == "tightening").sum().alias("tightening_intensity"),
                pl.col("instrument").n_unique().alias("instrument_diversity"),
                pl.col("record_id").filter(pl.col("formal_status") == "formal").n_unique().alias("formal_policy_count"),
            ).with_columns(
                (pl.col("loosening_intensity") - pl.col("tightening_intensity")).alias("net_intensity"),
                pl.lit("partial_coverage").alias("coverage_status"),
            ).sort("city_id", "year", "month")
        if not panel.is_empty():
            atomic_write_parquet(panel, self.settings.research / "city_month_policy_intensity.parquet")
        report = {
            "generated_at": datetime.now(UTC).isoformat(),
            "rows": panel.height,
            "cities": panel["city_id"].n_unique() if not panel.is_empty() else 0,
            "coverage_status": "partial_coverage",
            "confirmed_zero_supported": False,
            "research_ready": False,
        }
        self._write_json("aggregation_report.json", report)
        return report

    def rebuild_database(self) -> Path:
        return build_database(self.settings)

    def _write_json(self, name: str, payload: dict) -> None:
        self.output.mkdir(parents=True, exist_ok=True)
        path = self.output / name
        temporary = path.with_suffix(path.suffix + ".tmp")
        temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        temporary.replace(path)
