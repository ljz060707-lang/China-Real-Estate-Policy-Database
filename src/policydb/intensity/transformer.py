from __future__ import annotations

import importlib.util

import polars as pl

from policydb.settings import Settings


def train_transformer(settings: Settings | None = None, *, model_name: str = "hfl/chinese-macbert-base") -> dict:
    settings = settings or Settings.discover()
    gold = settings.root / "data" / "annotations" / "policy_intensity" / "adjudicated_gold.parquet"
    if not gold.exists() or pl.read_parquet(gold).is_empty():
        return {
            "status": "blocked_missing_gold",
            "model_name": model_name,
            "training_rows": 0,
            "metrics": {},
            "research_ready": False,
        }
    missing = [name for name in ("torch", "transformers") if importlib.util.find_spec(name) is None]
    if missing:
        return {
            "status": "blocked_missing_optional_dependency",
            "missing": missing,
            "install": "uv sync --extra intensity-transformer",
            "training_rows": 0,
            "metrics": {},
            "research_ready": False,
        }
    return {
        "status": "ready_for_explicit_training",
        "model_name": model_name,
        "message": "Use an explicit training run with reviewed gold; model download is never triggered by Dashboard import.",
        "research_ready": False,
    }

