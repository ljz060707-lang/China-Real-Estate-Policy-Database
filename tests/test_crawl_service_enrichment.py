from __future__ import annotations

from datetime import date

from policydb.crawl.service import CrawlService
from policydb.jobs.models import CrawlJobRequest
from policydb.settings import Settings


class _Pipeline:
    def plan(self, **_kwargs):
        return {
            "status": "planned",
            "run_id": "RUN_ARCHIVE_BEFORE_AI",
            "item_count": 1,
            "source_count": 1,
        }

    def run(self, *_args, **_kwargs):
        return {"fetched": 1, "failed": 0}


def test_run_glm_archives_versions_before_enrichment(tmp_path, monkeypatch):
    settings = Settings(root=tmp_path, data_root_path=tmp_path / "data")
    settings.curated.mkdir(parents=True)
    events: list[str] = []

    monkeypatch.setattr(Settings, "glm_api_key", property(lambda _self: "configured"))
    monkeypatch.setattr(
        "policydb.crawl.service.archive_document_versions",
        lambda *_args, **_kwargs: events.append("archive") or {"hash_verified": 1},
    )

    class _Enricher:
        def __init__(self, _settings):
            pass

        def enrich_pending(self, *, run_id):
            events.append(f"enrich:{run_id}")
            assert events[0] == "archive"
            return {"completed": 1, "failed": 0}

        def verify_pending(self, *, run_id):
            events.append(f"verify:{run_id}")
            return {"completed": 1, "failed": 0}

    monkeypatch.setattr("policydb.crawl.service.GLMEnricher", _Enricher)
    request = CrawlJobRequest(
        mode="historical_105",
        start_date=date(2016, 1, 1),
        end_date=date(2016, 12, 31),
        max_candidates=1,
        max_fetches=1,
        run_glm=True,
        run_verification=True,
        rebuild_database=False,
        run_validation=False,
    )

    result = CrawlService(settings, pipeline=_Pipeline()).execute(request)

    assert result["archive"]["hash_verified"] == 1
    assert events == [
        "archive",
        "enrich:RUN_ARCHIVE_BEFORE_AI",
        "verify:RUN_ARCHIVE_BEFORE_AI",
    ]
