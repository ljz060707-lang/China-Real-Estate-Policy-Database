from datetime import date

import polars as pl
import yaml

from policydb.crawl.pipeline import CrawlPipeline
from policydb.jurisdiction import discover_jurisdiction_sources, jurisdiction_source_id
from policydb.settings import Settings


def _write_registry(root, sources):
    reference = root / "data" / "reference"
    reference.mkdir(parents=True)
    (root / "data" / "curated").mkdir(parents=True)
    (reference / "source_registry.yaml").write_text(
        yaml.safe_dump({"sources": sources}, allow_unicode=True), encoding="utf-8"
    )


def _source(source_id, *, scope_type, seed_url, province_codes=None, city_ids=None):
    return {
        "source_id": source_id,
        "source_name": source_id,
        "domain": seed_url.split("/")[2],
        "source_type": "government",
        "source_role": "national_policy" if scope_type == "national" else "housing_department",
        "official_status": "official",
        "seed_urls": [seed_url],
        "list_page_urls": [],
        "crawl_enabled": True,
        "scope_type": scope_type,
        "province_codes": province_codes or [],
        "city_ids": city_ids or [],
        "agency_type": "state_council" if scope_type == "national" else "provincial_government",
    }


def test_jurisdiction_plan_selects_scope_once_without_city_fanout(tmp_path):
    root = tmp_path / "repo"
    _write_registry(
        root,
        [
            _source("NATIONAL", scope_type="national", seed_url="https://www.gov.cn/policy"),
            _source(
                "PROVINCIAL",
                scope_type="provincial",
                seed_url="https://zjt.example.gov.cn/policy",
                province_codes=["32"],
            ),
            _source(
                "MUNICIPAL",
                scope_type="municipal",
                seed_url="https://nj.example.gov.cn/policy",
                city_ids=["CITY_NJ"],
            ),
        ],
    )
    pipeline = CrawlPipeline(Settings(root=root))
    plan = pipeline.plan(
        run_type="jurisdictional_recent_5y",
        start_date=date(2021, 1, 1),
        end_date=date(2021, 12, 31),
        scope_types=["national"],
        max_candidates_total=50,
    )
    rows = pl.read_parquet(root / "data" / "curated" / "crawl_items.parquet")
    assert plan["source_count"] == 1
    assert rows.height == 1
    assert rows[0, "source_id"] == "NATIONAL"
    assert rows[0, "city_id"] is None
    assert rows[0, "jurisdiction_level"] == "national"
    assert rows[0, "jurisdiction_scope_type"] == "national"


def test_provincial_scope_preserves_explicit_province_metadata(tmp_path, monkeypatch):
    root = tmp_path / "repo"
    _write_registry(
        root,
        [
            _source(
                "PROVINCIAL",
                scope_type="provincial",
                seed_url="https://zjt.example.gov.cn/policy",
                province_codes=["32"],
            )
        ],
    )
    cities = pl.DataFrame(
        {
            "city_id": ["CITY_NJ"],
            "city_name": ["南京市"],
            "city_name_short": ["南京"],
            "province_name": ["江苏省"],
            "province_code": ["32"],
            "aliases": ["南京"],
        }
    )
    monkeypatch.setattr("policydb.crawl.pipeline.load_cities_105", lambda _settings: cities)
    pipeline = CrawlPipeline(Settings(root=root))
    plan = pipeline.plan(
        run_type="jurisdictional_recent_5y",
        start_date=date(2021, 1, 1),
        end_date=date(2021, 12, 31),
        scope_types=["provincial"],
        provinces=["江苏"],
        max_candidates_total=50,
    )
    rows = pl.read_parquet(root / "data" / "curated" / "crawl_items.parquet")
    assert plan["source_count"] == 1
    assert rows[0, "city_id"] is None
    assert rows[0, "jurisdiction_level"] == "provincial"
    assert rows[0, "jurisdiction_scope_type"] == "provincial"
    assert rows[0, "jurisdiction_province_codes_json"] == '["32"]'
    assert rows[0, "jurisdiction_province_name"] == "江苏省"


def test_jurisdiction_discovery_is_official_only_and_idempotent(tmp_path, monkeypatch):
    root = tmp_path / "repo"
    _write_registry(root, [])
    (root / "config").mkdir(parents=True)
    (root / "config" / "jurisdiction_scope.yaml").write_text(
        yaml.safe_dump(
            {
                "scope_version": "test-v1",
                "national_sources": [
                    {
                        "source_name": "官方",
                        "source_role": "national_policy",
                        "agency_type": "state_council",
                        "list_page_urls": ["https://www.gov.cn/policy/"],
                        "seed_urls": [],
                    },
                    {
                        "source_name": "非官方",
                        "source_role": "national_policy",
                        "agency_type": "state_council",
                        "list_page_urls": ["https://example.com/policy/"],
                        "seed_urls": [],
                    },
                ],
                "provincial_source_ids": [],
                "provincial_search_queries": [],
            },
            allow_unicode=True,
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "policydb.jurisdiction.load_cities_105",
        lambda _settings: pl.DataFrame(
            {"province_code": ["32"], "province_name": ["江苏省"]}
        ),
    )
    settings = Settings(root=root)
    first = discover_jurisdiction_sources(settings, use_search=False, apply=True)
    second = discover_jurisdiction_sources(settings, use_search=False, apply=True)
    assert first["applied_count"] == 1
    assert second["applied_count"] == 1
    assert jurisdiction_source_id("national", "gov.cn", "national_policy") == jurisdiction_source_id(
        "national", "gov.cn", "national_policy"
    )
    registry = yaml.safe_load((root / "data" / "reference" / "source_registry.yaml").read_text(encoding="utf-8"))
    assert len(registry["sources"]) == 1
    assert registry["sources"][0]["scope_type"] == "national"
    audit = pl.read_parquet(root / "data" / "curated" / "jurisdiction_source_candidates.parquet")
    assert audit.height == 2
    assert audit.filter(~pl.col("official_domain_verified")).height == 1


def test_provincial_seed_mapping_clones_without_mutating_municipal_source(tmp_path, monkeypatch):
    root = tmp_path / "repo"
    _write_registry(
        root,
        [
            {
                **_source(
                    "P1",
                    scope_type="municipal",
                    seed_url="https://province.example.gov.cn/policy",
                    city_ids=["CITY_1"],
                ),
                "agency_type": "housing_department",
            }
        ],
    )
    (root / "config").mkdir(parents=True)
    (root / "config" / "jurisdiction_scope.yaml").write_text(
        yaml.safe_dump(
            {
                "scope_version": "test-v1",
                "national_sources": [],
                "provincial_source_seeds": [{"source_id": "P1", "province_codes": ["32"]}],
                "provincial_search_queries": [],
            },
            allow_unicode=True,
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "policydb.jurisdiction.load_cities_105",
        lambda _settings: pl.DataFrame(
            {"province_code": ["32"], "province_name": ["江苏省"]}
        ),
    )
    result = discover_jurisdiction_sources(
        Settings(root=root), levels=["provincial"], use_search=False, apply=True
    )
    assert result["applied_count"] == 1
    registry = yaml.safe_load(
        (root / "data" / "reference" / "source_registry.yaml").read_text(encoding="utf-8")
    )["sources"]
    original = next(row for row in registry if row["source_id"] == "P1")
    clone = next(row for row in registry if row["source_id"] != "P1")
    assert original["scope_type"] == "municipal"
    assert original["city_ids"] == ["CITY_1"]
    assert clone["scope_type"] == "provincial"
    assert clone["province_codes"] == ["32"]
    assert clone["city_ids"] == []
