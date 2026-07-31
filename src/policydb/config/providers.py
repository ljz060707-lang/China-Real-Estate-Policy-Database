from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date
from typing import Protocol
from urllib.parse import parse_qs, urlsplit

import httpx
from bs4 import BeautifulSoup


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
        self.client = client or httpx.Client(
            timeout=30,
            trust_env=not bool(os.getenv("CRPD_SEARCH_PROXY_URL")),
            verify=True,
            **(
                {"proxy": os.environ["CRPD_SEARCH_PROXY_URL"]}
                if os.getenv("CRPD_SEARCH_PROXY_URL")
                else {}
            ),
        )


class DuckDuckGoHtmlSearchProvider:
    """Keyless discovery fallback; results remain unverified candidates."""

    name = "DuckDuckGoHTML"

    def __init__(self, *, client: httpx.Client | None = None) -> None:
        proxy = os.getenv("CRPD_SEARCH_PROXY_URL")
        self.client = client or httpx.Client(
            timeout=30,
            trust_env=not bool(proxy),
            verify=True,
            follow_redirects=True,
            headers={"User-Agent": "Mozilla/5.0 (compatible; CRPDSourceAudit/1.0)"},
            **({"proxy": proxy} if proxy else {}),
        )

    def search(self, query: str, **kwargs: object) -> list[SearchResult]:
        maximum = int(kwargs.get("max_results", 10))
        response = self.client.post("https://html.duckduckgo.com/html/", data={"q": query})
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        results: list[SearchResult] = []
        for anchor in soup.select("a.result__a[href]"):
            href = str(anchor.get("href") or "")
            query_string = parse_qs(urlsplit(href).query)
            target = query_string.get("uddg", [href])[0]
            if not target.startswith(("http://", "https://")):
                continue
            container = anchor.find_parent(class_="result")
            snippet = container.select_one(".result__snippet") if container else None
            results.append(
                SearchResult(
                    url=target,
                    title=anchor.get_text(" ", strip=True),
                    snippet=snippet.get_text(" ", strip=True) if snippet else "",
                )
            )
            if len(results) >= maximum:
                break
        return results


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
    if normalized in {"duckduckgohtml", "duckduckgo", "ddg"}:
        return DuckDuckGoHtmlSearchProvider(client=client)
    if normalized == "none" or not api_key:
        return NoneSearchProvider()
    classes = {"bing": BingSearchProvider, "serper": SerperSearchProvider, "tavily": TavilySearchProvider}
    if normalized not in classes:
        raise ValueError(f"Unsupported search provider: {name}")
    kwargs = {"client": client}
    if base_url:
        kwargs["base_url"] = base_url
    return classes[normalized](api_key, **kwargs)

class FallbackSearchProvider:
    """Try configured providers in order and preserve provider-level diagnostics."""

    name = "Fallback"

    def __init__(self, providers: list[SearchProvider]) -> None:
        self.providers = providers
        self.last_attempts: list[dict] = []

    def search(self, query: str, **kwargs: object) -> list[SearchResult]:
        maximum = int(kwargs.get("max_results", 10))
        collected: list[SearchResult] = []
        seen: set[str] = set()
        self.last_attempts = []
        for provider in self.providers:
            try:
                results = provider.search(query, **kwargs)
                self.last_attempts.append(
                    {"provider": provider.name, "status": "ok", "result_count": len(results)}
                )
            except Exception as exc:
                self.last_attempts.append(
                    {"provider": provider.name, "status": "failed", "error_type": type(exc).__name__}
                )
                continue
            for result in results:
                if result.url in seen:
                    continue
                seen.add(result.url)
                collected.append(result)
                if len(collected) >= maximum:
                    return collected
            if collected:
                return collected
        return collected


def build_search_fallback(settings=None, *, clients: dict[str, httpx.Client] | None = None) -> SearchProvider:
    from policydb.settings import Settings

    settings = settings or Settings.discover()
    providers: list[SearchProvider] = []
    for name in settings.search_providers:
        provider = build_search_provider(
            name,
            settings.search_api_key_for(name),
            base_url=settings.search_base_url if len(settings.search_providers) == 1 else None,
            client=(clients or {}).get(name.lower()),
        )
        if not isinstance(provider, NoneSearchProvider):
            providers.append(provider)
    return FallbackSearchProvider(providers) if providers else NoneSearchProvider()
