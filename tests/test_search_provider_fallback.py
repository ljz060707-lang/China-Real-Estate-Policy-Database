from policydb.config.providers import FallbackSearchProvider, SearchResult


class Broken:
    name = "broken"

    def search(self, query, **kwargs):
        raise TimeoutError


class Working:
    name = "working"

    def search(self, query, **kwargs):
        return [SearchResult(url="https://example.gov.cn/policy")]


def test_search_failure_uses_backup_provider():
    provider = FallbackSearchProvider([Broken(), Working()])
    assert provider.search("policy")[0].url.endswith("/policy")
    assert [item["status"] for item in provider.last_attempts] == ["failed", "ok"]
