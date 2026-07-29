from pathlib import Path

import yaml


def test_historical_scan_is_sharded_and_starts_in_2018():
    data = yaml.safe_load(
        Path("data/reference/search_query_templates.yaml").read_text(encoding="utf-8")
    )
    assert data["default_start_date"].isoformat() == "2018-01-01"
    assert "city_id" in data["shard_key"] and "source_role" in data["shard_key"]


def test_seed_discovery_filters_url_year_and_preserves_city():
    from datetime import date

    from policydb.crawl.discovery import discover_seed_items
    from policydb.crawl.models import RegisteredSource

    source = RegisteredSource(
        source_id="SRC_TEST",
        source_name="Test government",
        domain="example.gov.cn",
        source_type="government",
        source_role="municipal_government",
        official_status="official",
        seed_urls=[
            "https://example.gov.cn/policy/202007/t20200723_1.html",
            "https://example.gov.cn/policy/202607/t20260702_2.html",
        ],
    )
    rows = discover_seed_items(
        source,
        "RUN_TEST",
        city_id="CITY_320100",
        start_date=date(2026, 6, 29),
        end_date=date(2026, 7, 29),
    )
    assert [row["url"] for row in rows] == [source.seed_urls[1]]
    assert rows[0]["city_id"] == "CITY_320100"