from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Protocol

import httpx


@dataclass(frozen=True)
class SearchResult:
    url: str
    title: str = ""
    snippet: str = ""
    published_at: str | None = None


class SearchProvider(Protocol):
    name: str

    def search(
        self,
        query: str,
        *,
        start_date: date | None = None,
        end_date: date | None = None,
        max_results: int = 10,
    ) -> list[SearchResult]: ...


class NoneSearchProvider:
    name = "None"

    def search(self, query: str, **_: object) -> list[SearchResult]:
        return []


class _HttpSearchProvider:
    name = ""

    def __init__(self, api_key: str, *, base_url: str, client: httpx.Client | None = None) -> None:
        self.api_key = api_key
        self.base_url = base_url
        self.client = client or httpx.Client(timeout=30)


class BingSearchProvider(_HttpSearchProvider):
    name = "Bing"

    def __init__(self, api_key: str, *, base_url: str = "https://api.bing.microsoft.com/v7.0/search", client: httpx.Client | None = None) -> None:
        super().__init__(api_key, base_url=base_url, client=client)

    def search(self, query: str, **kwargs: object) -> list[SearchResult]:
        maximum = int(kwargs.get("max_results", 10))
        response = self.client.get(
            self.base_url,
            params={"q": query, "count": maximum, "textDecorations": False},
            headers={"Ocp-Apim-Subscription-Key": self.api_key},
        )
        response.raise_for_status()
        return [SearchResult(url=item["url"], title=item.get("name", ""), snippet=item.get("snippet", "")) for item in response.json().get("webPages", {}).get("value", [])[:maximum]]


class SerperSearchProvider(_HttpSearchProvider):
    name = "Serper"

    def __init__(self, api_key: str, *, base_url: str = "https://google.serper.dev/search", client: httpx.Client | None = None) -> None:
        super().__init__(api_key, base_url=base_url, client=client)

    def search(self, query: str, **kwargs: object) -> list[SearchResult]:
        maximum = int(kwargs.get("max_results", 10))
        response = self.client.post(self.base_url, headers={"X-API-KEY": self.api_key}, json={"q": query, "num": maximum})
        response.raise_for_status()
        return [SearchResult(url=item["link"], title=item.get("title", ""), snippet=item.get("snippet", ""), published_at=item.get("date")) for item in response.json().get("organic", [])[:maximum]]


class TavilySearchProvider(_HttpSearchProvider):
    name = "Tavily"

    def __init__(self, api_key: str, *, base_url: str = "https://api.tavily.com/search", client: httpx.Client | None = None) -> None:
        super().__init__(api_key, base_url=base_url, client=client)

    def search(self, query: str, **kwargs: object) -> list[SearchResult]:
        maximum = int(kwargs.get("max_results", 10))
        response = self.client.post(self.base_url, json={"api_key": self.api_key, "query": query, "max_results": maximum, "search_depth": "advanced"})
        response.raise_for_status()
        return [SearchResult(url=item["url"], title=item.get("title", ""), snippet=item.get("content", ""), published_at=item.get("published_date")) for item in response.json().get("results", [])[:maximum]]


def build_search_provider(name: str, api_key: str | None, *, base_url: str | None = None, client: httpx.Client | None = None) -> SearchProvider:
    normalized = (name or "None").strip().lower()
    if normalized == "none" or not api_key:
        return NoneSearchProvider()
    classes = {"bing": BingSearchProvider, "serper": SerperSearchProvider, "tavily": TavilySearchProvider}
    if normalized not in classes:
        raise ValueError(f"Unsupported search provider: {name}")
    kwargs = {"client": client}
    if base_url:
        kwargs["base_url"] = base_url
    return classes[normalized](api_key, **kwargs)
