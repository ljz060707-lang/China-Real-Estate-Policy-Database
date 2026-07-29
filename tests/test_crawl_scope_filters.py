from datetime import date

import polars as pl
import yaml

from policydb.coverage import record_source_window
from policydb.crawl.pipeline import CrawlPipeline
from policydb.settings import Settings


def test_selected_city_topic_and_year_reach_execution_plan(tmp_path, monkeypatch):
    root = tmp_path / "repo"
    reference = root / "data" / "reference"
    reference.mkdir(parents=True)
    (reference / "source_registry.yaml").write_text(
        yaml.safe_dump(
            {
                "sources": [
                    {
                        "source_id": "SRC_SEARCH",
                        "source_name": "站内搜索",
                        "domain": "example.gov.cn",
                        "source_type": "government",
                        "source_role": "canonical_candidate",
                        "official_status": "official",
                        "search_url_template": (
                            "https://example.gov.cn/search?city={city_id}"
                            "&year={year}&topic={keyword_group}"
                        ),
                        "city_ids": ["CITY_NJ"],
                        "scope_type": "municipal",
                        "crawl_enabled": True,
                    },
                    {
                        "source_id": "SRC_WH",
                        "source_name": "武汉来源",
                        "domain": "wuhan.gov.cn",
                        "source_type": "government",
                        "source_role": "canonical_candidate",
                        "official_status": "official",
                        "seed_urls": ["https://wuhan.gov.cn/policy/1"],
                        "city_ids": ["CITY_WH"],
                        "scope_type": "municipal",
                        "crawl_enabled": True,
                    }
                ]
            },
            allow_unicode=True,
        ),
        encoding="utf-8",
    )
    (reference / "crawl_keywords.yaml").write_text(
        yaml.safe_dump(
            {
                "groups": {
                    "purchase": {"terms": ["限购"]},
                    "fund": {"terms": ["公积金"]},
                }
            },
            allow_unicode=True,
        ),
        encoding="utf-8",
    )
    cities = pl.DataFrame(
        {
            "city_id": ["CITY_NJ", "CITY_WH"],
            "city_name": ["南京市", "武汉市"],
            "city_name_short": ["南京", "武汉"],
            "province_name": ["江苏省", "湖北省"],
            "province_code": ["32", "42"],
            "aliases": ["南京|南京市", "武汉|武汉市"],
        }
    )
    monkeypatch.setattr(
        "policydb.crawl.pipeline.load_cities_105", lambda _settings: cities
    )

    pipeline = CrawlPipeline(Settings(root=root))
    plan = pipeline.plan(
        run_type="historical_105",
        start_date=date(2021, 1, 1),
        end_date=date(2021, 12, 31),
        cities=["南京"],
        topics=["限购"],
        max_candidates_total=50,
    )

    rows = pl.read_parquet(
        root / "data" / "curated" / "crawl_items.parquet"
    ).filter(pl.col("run_id") == plan["run_id"])
    assert rows.height == 1
    assert rows["city_id"].to_list() == ["CITY_NJ"]
    assert rows["query_year"].to_list() == [2021]
    assert rows["keyword_group"].to_list() == ["purchase"]


def test_unknown_selected_city_does_not_fall_back_to_all_cities(
    tmp_path, monkeypatch
):
    root = tmp_path / "repo"
    reference = root / "data" / "reference"
    reference.mkdir(parents=True)
    (reference / "source_registry.yaml").write_text(
        yaml.safe_dump(
            {
                "sources": [
                    {
                        "source_id": "SRC_SEARCH",
                        "source_name": "站内搜索",
                        "domain": "example.gov.cn",
                        "source_type": "government",
                        "source_role": "canonical_candidate",
                        "official_status": "official",
                        "search_url_template": "https://example.gov.cn/?city={city_id}",
                        "city_ids": ["CITY_NJ"],
                        "scope_type": "municipal",
                        "crawl_enabled": True,
                    }
                ]
            },
            allow_unicode=True,
        ),
        encoding="utf-8",
    )
    (reference / "crawl_keywords.yaml").write_text(
        yaml.safe_dump({"groups": {"all": {"terms": ["房地产"]}}}),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "policydb.crawl.pipeline.load_cities_105",
        lambda _settings: pl.DataFrame(
            {
                "city_id": ["CITY_NJ"],
                "city_name": ["南京市"],
                "city_name_short": ["南京"],
                "province_name": ["江苏省"],
                "province_code": ["32"],
                "aliases": ["南京|南京市"],
            }
        ),
    )
    plan = CrawlPipeline(Settings(root=root)).plan(
        run_type="historical_105",
        start_date=date(2021, 1, 1),
        end_date=date(2021, 12, 31),
        cities=["不存在的城市"],
    )
    assert plan["item_count"] == 0


def test_resume_skips_matching_complete_window(tmp_path):
    root = tmp_path / "repo"
    reference = root / "data" / "reference"
    reference.mkdir(parents=True)
    (reference / "source_registry.yaml").write_text(
        yaml.safe_dump(
            {
                "sources": [
                    {
                        "source_id": "SRC_DONE",
                        "source_name": "已完成来源",
                        "domain": "example.gov.cn",
                        "source_type": "government",
                        "source_role": "canonical_candidate",
                        "official_status": "official",
                        "seed_urls": ["https://example.gov.cn/policy"],
                        "crawl_enabled": True,
                    }
                ]
            },
            allow_unicode=True,
        ),
        encoding="utf-8",
    )
    settings = Settings(root=root)
    pipeline = CrawlPipeline(settings)
    record_source_window(
        run_id="OLD",
        source_id="SRC_DONE",
        period_start=date(2021, 1, 1),
        period_end=date(2021, 12, 31),
        scan_method="historical_105",
        candidate_count=0,
        fetched_count=0,
        policy_count=0,
        error_count=0,
        page_count=2,
        completion_evidence={"exhaustive": True},
        settings=settings,
    )
    plan = pipeline.plan(
        run_type="historical_105",
        start_date=date(2021, 1, 1),
        end_date=date(2021, 12, 31),
        resume=True,
    )
    assert plan["item_count"] == 0
