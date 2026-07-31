from __future__ import annotations

from types import SimpleNamespace

from policydb.cli import _execute_exhaustive_postprocess


class _FakeEnricher:
    def __init__(self, settings):
        self.settings = settings

    def enrich_pending(self, run_id=None):
        return {
            "pending": 2,
            "completed": 1,
            "awaiting_api_key": 1,
            "failed": 0,
            "irrelevant": 0,
        }

    def verify_pending(self, run_id=None):
        return {"pending": 1, "completed": 1, "awaiting_api_key": 0, "failed": 0}


class _FakeCrawler:
    settings = SimpleNamespace()

    def __init__(self):
        self.persisted = None

    def apply_postprocess_metrics(self, metrics):
        self.persisted = metrics
        return {"updated_shards": len(metrics)}

    def city_status(self, city):
        return {"rows": 1, "city": city}


def test_run_ai_executes_real_pipeline_and_persists_residuals(monkeypatch):
    calls = []
    monkeypatch.setattr("policydb.cli.GLMEnricher", _FakeEnricher)
    for name in (
        "materialize_action_classifications",
        "materialize_policy_identity",
        "materialize_policy_pools",
        "materialize_field_confidence",
        "build_city_source_month_coverage",
        "build_database",
    ):
        monkeypatch.setattr(
            f"policydb.cli.{name}",
            lambda settings, name=name: calls.append(name) or {"ok": True},
        )
    crawler = _FakeCrawler()
    result = _execute_exhaustive_postprocess(
        crawler,
        {"city_id": "CITY_320100", "run_ids": ["RUN_1"]},
        run_ai=True,
        archive=False,
    )
    assert result["run_ai_executed"] is True
    assert result["archive_executed"] is False
    assert crawler.persisted["RUN_1"]["ai_pending_count"] == 2
    assert len(calls) == 6


def test_no_run_ai_keeps_explicit_pending_state():
    crawler = _FakeCrawler()
    result = _execute_exhaustive_postprocess(
        crawler,
        {
            "city_id": "CITY_320100",
            "run_ids": ["RUN_1"],
            "run_metrics": {
                "RUN_1": {
                    "ai_pending_count": 7,
                    "dedup_pending_count": 7,
                    "archive_missing_count": 0,
                }
            },
        },
        run_ai=False,
        archive=False,
    )
    assert result["run_ai_executed"] is False
    assert crawler.persisted["RUN_1"]["ai_pending_count"] == 7
    assert crawler.persisted["RUN_1"]["dedup_pending_count"] == 7
