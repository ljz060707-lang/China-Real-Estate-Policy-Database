import httpx

from policydb.config.providers import (
    DuckDuckGoHtmlSearchProvider,
    SearchResult,
    build_search_provider,
)
from policydb.settings import Settings
from policydb.source_discovery import discover_city_sources, is_reusable_source_entry


def test_content_pages_are_never_reusable_source_entries():
    assert not is_reusable_source_entry(
        "https://www.beijing.gov.cn/zhengce/zhengcefagui/202511/t20251128_4310605.html"
    )
    assert not is_reusable_source_entry("https://example.gov.cn/article?id=42")
    assert is_reusable_source_entry("https://www.beijing.gov.cn/zhengce/zhengcefagui/")
    assert is_reusable_source_entry("https://city.gov.cn/zwgk/index.html")
    assert is_reusable_source_entry("https://city.gov.cn/zwgk/index_18071.html")


def test_keyless_search_provider_extracts_real_target_url():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            text=(
                '<div class="result"><a class="result__a" '
                'href="//duckduckgo.com/l/?uddg=https%3A%2F%2Fcity.gov.cn%2Fzwgk%2F">'
                '城市政务公开</a><div class="result__snippet">官网入口</div></div>'
            ),
            request=request,
        )

    provider = DuckDuckGoHtmlSearchProvider(client=httpx.Client(transport=httpx.MockTransport(handler)))
    result = provider.search("城市 政府 官网")
    assert result[0].url == "https://city.gov.cn/zwgk/"
    assert result[0].title == "城市政务公开"
    assert build_search_provider("ddg", None).name == "DuckDuckGoHTML"


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
