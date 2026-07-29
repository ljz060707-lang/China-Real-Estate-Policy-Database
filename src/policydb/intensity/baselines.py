from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from policydb.settings import Settings


def _hash_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def train_baselines(settings: Settings | None = None, *, seed: int = 20260722) -> dict:
    settings = settings or Settings.discover()
    gold = settings.root / "data" / "annotations" / "policy_intensity" / "adjudicated_gold.parquet"
    if not gold.exists() or pl.read_parquet(gold).is_empty():
        return {
            "status": "blocked_missing_gold",
            "training_rows": 0,
            "models": ["tfidf_logistic_regression", "tfidf_linear_svm"],
            "metrics": {},
            "research_ready": False,
        }
    try:
        import joblib
        from sklearn.calibration import CalibratedClassifierCV
        from sklearn.feature_extraction.text import TfidfVectorizer
        from sklearn.linear_model import LogisticRegression
        from sklearn.metrics import classification_report
        from sklearn.model_selection import GroupShuffleSplit
        from sklearn.pipeline import FeatureUnion, Pipeline
        from sklearn.svm import LinearSVC
    except ImportError:
        return {
            "status": "blocked_missing_optional_dependency",
            "install": "uv sync --extra intensity-ml",
            "training_rows": 0,
            "metrics": {},
            "research_ready": False,
        }
    frame = pl.read_parquet(gold).filter(pl.col("is_policy_action").is_not_null())
    if frame.height < 20:
        return {"status": "blocked_insufficient_gold", "training_rows": frame.height, "metrics": {}, "research_ready": False}
    texts = frame["clause_text"].to_list()
    labels = frame["is_policy_action"].cast(pl.Int8).to_list()
    groups = frame.get_column("document_family_id").fill_null(frame["record_id"]).to_list() if "document_family_id" in frame.columns else frame["record_id"].to_list()
    train_index, test_index = next(GroupShuffleSplit(n_splits=1, test_size=0.25, random_state=seed).split(texts, labels, groups))
    features = FeatureUnion([
        ("char", TfidfVectorizer(analyzer="char", ngram_range=(2, 5), min_df=2, max_features=50_000)),
        ("word", TfidfVectorizer(analyzer="word", ngram_range=(1, 2), min_df=2, max_features=30_000)),
    ])
    models = {
        "tfidf_logistic_regression": LogisticRegression(max_iter=2000, class_weight="balanced", random_state=seed),
        "tfidf_linear_svm": CalibratedClassifierCV(LinearSVC(class_weight="balanced", random_state=seed)),
    }
    output = settings.root / "outputs" / "policy_intensity" / "models"
    output.mkdir(parents=True, exist_ok=True)
    metrics = {}
    for name, classifier in models.items():
        pipeline = Pipeline([("features", features), ("classifier", classifier)])
        pipeline.fit([texts[index] for index in train_index], [labels[index] for index in train_index])
        predicted = pipeline.predict([texts[index] for index in test_index])
        metrics[name] = classification_report([labels[index] for index in test_index], predicted, output_dict=True, zero_division=0)
        joblib.dump(pipeline, output / f"{name}.joblib")
    metadata = {
        "status": "trained_experimental",
        "training_rows": len(train_index),
        "test_rows": len(test_index),
        "split": "grouped_by_document_family",
        "seed": seed,
        "gold_sha256": _hash_file(gold),
        "trained_at": datetime.now(UTC).isoformat(),
        "metrics": metrics,
        "research_ready": False,
    }
    (output / "metadata.json").write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")
    return metadata

