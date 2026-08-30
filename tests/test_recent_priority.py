import shutil
from datetime import date
from pathlib import Path

import polars as pl

import policydb.recent_priority as recent_priority
from policydb.autopilot_cli import _resume_recent_window
from policydb.recent_priority import Recent30DConfig, build_recent_queue, run_recent_30d
from policydb.settings import Settings


def test_recent_queue_is_enabled_official_source_scoped_and_city_round_robin(root, tmp_path):
    test_root = tmp_path / "repo"
    reference = test_root / "data" / "reference"
    reference.mkdir(parents=True)
    for name in ("cities_105.csv", "source_registry.yaml"):
        shutil.copy2(root / "data" / "reference" / name, reference / name)
    settings = Settings(
        root=test_root,
        data_root_path=tmp_path / "data",
        outputs_path=tmp_path / "outputs",
        automation_path=tmp_path / "automation",
    )
    frame = build_recent_queue(
        settings,
        config=Recent30DConfig(
            start_date=date(2026, 7, 12),
            end_date=date(2026, 8, 11),
            max_items=3,
        ),
    )
    assert frame.height > 0
    assert frame["item_id"].n_unique() == frame.height
    assert set(frame["status"].unique().to_list()) == {"PENDING", "SOURCE_INCOMPLETE"}
    assert frame.filter(pl.col("status") == "SOURCE_INCOMPLETE").height == 2
    assert frame["city_id"].n_unique() >= 2
    assert frame.filter(pl.col("source_role").is_null()).height == 0


def test_resume_reuses_persisted_queue_window(tmp_path):
    settings = Settings(root=tmp_path, outputs_path=tmp_path / "outputs")
    queue_root = settings.outputs / "recent_30d"
    queue_root.mkdir(parents=True)
    pl.DataFrame(
        [{"item_id": "I1", "start_date": "2026-07-12", "end_date": "2026-08-11"}],
    ).write_parquet(queue_root / "RECENT_30D_QUEUE.parquet")
    start, end = _resume_recent_window(
        settings,
        start_date=None,
        end_date=None,
        resume=True,
    )
    assert start == date(2026, 7, 12)
    assert end == date(2026, 8, 11)


def test_recent_run_promotes_formal_records_before_next_queue_item(root, tmp_path, monkeypatch):
    test_root = tmp_path / "repo"
    reference = test_root / "data" / "reference"
    reference.mkdir(parents=True)
    for name in ("cities_105.csv", "source_registry.yaml"):
        shutil.copy2(root / "data" / "reference" / name, reference / name)
    settings = Settings(
        root=test_root,
        data_root_path=tmp_path / "data",
        outputs_path=tmp_path / "outputs",
        automation_path=tmp_path / "automation",
    )

    def fake_run_city(self, city, *, source_ids, **_kwargs):
        source_id = source_ids[0]
        item_id = "FAKE_ITEM_1"
        now = "2026-08-11T00:00:00+00:00"
        self.settings.curated.mkdir(parents=True, exist_ok=True)
        pl.DataFrame(
            [
                {
                    "item_id": item_id,
                    "run_id": "RECENT_RUN_1",
                    "city_id": city,
                    "candidate_date": "2026-08-01",
                    "candidate_date_source": "structured_date",
                }
            ],
            infer_schema_length=None,
        ).write_parquet(self.settings.curated / "crawl_items.parquet")
        pl.DataFrame(
            [
                {
                    "document_version_id": "FAKE_VERSION_1",
                    "record_id": None,
                    "crawl_item_id": item_id,
                    "source_id": source_id,
                    "canonical_url": "https://example.gov.cn/policy/1",
                    "final_url": "https://example.gov.cn/policy/1",
                    "content_sha256": "fake-content-1",
                    "local_path": "archive/html/fake-1.html",
                    "content_type": "text/html",
                    "http_status": 200,
                    "title": "住房政策通知",
                    "extracted_text": "住房政策实施办法正文",
                    "parse_status": "parsed",
                    "created_at": now,
                    "updated_at": now,
                }
            ],
            infer_schema_length=None,
        ).write_parquet(self.settings.curated / "policy_document_versions.parquet")
        return {
            "batch_id": "RECENT_BATCH_1",
            "run_ids": ["RECENT_RUN_1"],
            "run_metrics": {"RECENT_RUN_1": {"fetched": 1, "failed": 0}},
            "processed_shards": 1,
        }

    monkeypatch.setattr(recent_priority.ExhaustiveCrawler, "run_city", fake_run_city)
    monkeypatch.setattr(recent_priority, "archive_document_versions", lambda *args, **kwargs: {})
    monkeypatch.setattr(recent_priority, "materialize_policy_identity", lambda *args, **kwargs: {})
    monkeypatch.setattr(recent_priority, "build_database", lambda *args, **kwargs: Path("ignored"))

    result = run_recent_30d(
        settings,
        config=Recent30DConfig(
            start_date=date(2026, 7, 12),
            end_date=date(2026, 8, 11),
            max_items=1,
            apply=True,
        ),
    )
    assert result["processed_items"] == 1
    assert result["status"] == "PARTIAL"
    assert result["records_promoted"] == 1
    records = pl.read_parquet(settings.curated / "records.parquet")
    assert records.height == 1
    policy_list = pl.read_parquet(settings.outputs / "recent_30d" / "RECENT_30D_POLICY_LIST.parquet")
    assert policy_list.height == 1
    assert policy_list[0, "city_id"]
