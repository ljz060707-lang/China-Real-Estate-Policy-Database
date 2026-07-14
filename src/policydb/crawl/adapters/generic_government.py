from __future__ import annotations

from bs4 import BeautifulSoup

from policydb.crawl.dedup import canonicalize_url


class GenericGovernmentAdapter:
    def discover_links(self, html: str, base_url: str) -> list[str]:
        from urllib.parse import urljoin

        soup = BeautifulSoup(html, "html.parser")
        return list(
            dict.fromkeys(
                canonicalize_url(urljoin(base_url, anchor["href"]))
                for anchor in soup.select("a[href]")
                if not anchor["href"].lower().startswith(("javascript:", "mailto:"))
            )
        )
