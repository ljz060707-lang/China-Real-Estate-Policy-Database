from __future__ import annotations

import shutil
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.geography_panel import GeographyPanelUnavailable, panel_health, query_region_panel
from policydb import PolicyDB
from policydb.geography import load_cities_105, normalize_geography


def test_city_suffix_and_county_level_are_normalized(db):
    cities = load_cities_105(db.settings)
    plain = normalize_geography("广州", cities)[0]
    full = normalize_geography("广东省广州市越秀区", cities)[0]
    county_city = normalize_geography("江苏省昆山市", cities)[0]
    assert plain["city_name"] == "广州市" and plain["province_name"] == "广东省"
    assert full["city_name"] == "广州市" and full["county_name"] == "越秀区"
    assert county_city["jurisdiction_level"] == "county_level_city"
    assert county_city["parent_city_name"] == "苏州市"


def test_region_panel_empty_filter_is_safe(db):
    result = query_region_panel(db, "省级", year=1900)
    assert result["ranking"].is_empty()
    assert result["trend"].is_empty()
    assert result["total"] == 0


def test_missing_view_has_actionable_error():
    class BrokenDB:
        def _query(self, *_args, **_kwargs):
            raise RuntimeError("missing view")

    with pytest.raises(GeographyPanelUnavailable, match="build-database"):
        panel_health(BrokenDB())


def test_read_only_cloud_rebuilds_database_from_parquet(tmp_path, root, monkeypatch):
    target = tmp_path / "cloud"
    shutil.copytree(root / "data" / "curated", target / "data" / "curated")
    monkeypatch.setenv("POLICYDB_READ_ONLY", "1")
    cloud_db = PolicyDB.open(target)
    assert cloud_db.settings.database.exists()
    assert cloud_db._query("SELECT count(*) FROM v_city_month_policy_panel").item() >= 0
