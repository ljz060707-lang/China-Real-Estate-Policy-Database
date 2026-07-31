from policydb.config.providers import SearchResult
from policydb.settings import Settings
from policydb.source_discovery import discover_city_sources, is_reusable_source_entry


def test_content_pages_are_never_reusable_source_entries():
    assert not is_reusable_source_entry(
        "https://www.beijing.gov.cn/zhengce/zhengcefagui/202511/t20251128_4310605.html"
    )
    assert not is_reusable_source_entry("https://example.gov.cn/article?id=42")
    assert is_reusable_source_entry("https://www.beijing.gov.cn/zhengce/zhengcefagui/")


class FakeProvider:
    name = "fixture"

    def search(self, query, **kwargs):
        return [SearchResult(url="https://www.nanjing.gov.cn/policy/1", title="南京市人民政府")]


def test_discovery_keeps_official_candidate_disabled(tmp_path, monkeypatch):
    settings = Settings(root=tmp_path)
    monkeypatch.setattr(
        "policydb.source_discovery.load_cities_105",
        lambda _: __import__("polars").DataFrame(
            {
                "city_id": ["CITY_320100"],
                "city_name": ["南京市"],
                "city_name_short": ["南京"],
                "province_name": ["江苏省"],
            }
        ),
    )
    result = discover_city_sources(
        "南京", settings, provider=FakeProvider(), roles=["municipal_government"]
    )
    assert result["official_candidate_count"] == 1
    assert result["added_disabled_sources"] == 0
