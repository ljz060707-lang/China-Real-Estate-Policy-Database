from __future__ import annotations

import json
import shutil
from pathlib import Path

import polars as pl

from policydb.settings import Settings
from policydb.source_slots import build_requirement_slots
from policydb.supervisor import repair_recipe, supervisor_status


def _settings(tmp_path: Path) -> Settings:
    root = tmp_path / "repo"
    reference = root / "data" / "reference"
    reference.mkdir(parents=True)
    source = Path(__file__).resolve().parents[1] / "data" / "reference"
    for name in ("cities_105.csv", "city_source_requirements.yaml"):
        shutil.copy2(source / name, reference / name)
    (reference / "source_registry.yaml").write_text(
        "version: 2\nsources: []\n", encoding="utf-8"
    )
    return Settings(root=root)


def test_supervisor_reports_tun_and_actionable_shards(tmp_path):
    settings = _settings(tmp_path)
    build_requirement_slots(settings)
    pl.DataFrame(
        [
            {
                "shard_id": "SHARD_1",
                "city_name": "南京市",
                "status": "partial_network",
                "checkpoint": "RUN_1",
                "started_at": "2026-07-31T00:00:00+00:00",
                "updated_at": "2026-07-31T00:01:00+00:00",
                "ai_pending_count": 2,
            }
        ]
    ).write_parquet(settings.curated / "crawl_shards.parquet")
    network = settings.outputs / "acceptance" / "network_source_audit.json"
    network.parent.mkdir(parents=True, exist_ok=True)
    network.write_text(
        json.dumps({"status_counts": {"tun_intercepted": 4}}), encoding="utf-8"
    )
    result = supervisor_status(settings, stale_minutes=30)
    assert result["healthy"] is False
    assert result["issue_counts"] == {
        "ai_pending": 1,
        "partial_network": 1,
        "tun_intercepted": 1,
    }
    assert Path(result["output"]).exists()


def test_repair_recipe_never_claims_tun_can_be_auto_repaired():
    recipe = repair_recipe("tun_intercepted")
    assert recipe["automatic"] is False
    assert "Fake-IP" in recipe["action"]
