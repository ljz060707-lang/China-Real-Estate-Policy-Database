from __future__ import annotations

import time
from datetime import UTC, datetime
from urllib.parse import urlsplit
from urllib.robotparser import RobotFileParser

import httpx

from policydb.crawl.dedup import content_sha256
from policydb.crawl.models import FetchResult


class RespectfulFetcher:
    def __init__(
        self,
        *,
        client: httpx.Client | None = None,
        user_agent: str = "PolicyDBResearchBot/0.1",
        timeout: float = 30,
        retries: int = 3,
        rate_limit: float = 0.5,
        check_robots: bool = True,
    ) -> None:
        self.user_agent = user_agent
        self.client = client or httpx.Client(
            headers={"User-Agent": user_agent}, timeout=timeout, follow_redirects=True
        )
        self.retries = retries
        self.rate_limit = rate_limit
        self.check_robots = check_robots
        self._robots: dict[str, RobotFileParser] = {}
        self._last_request: dict[str, float] = {}

    def _allowed(self, url: str) -> bool:
        if not self.check_robots:
            return True
        parts = urlsplit(url)
        origin = f"{parts.scheme}://{parts.netloc}"
        if origin not in self._robots:
            parser = RobotFileParser(origin + "/robots.txt")
            try:
                response = self.client.get(parser.url)
                parser.parse(response.text.splitlines() if response.status_code == 200 else [])
            except httpx.HTTPError:
                parser.parse([])
            self._robots[origin] = parser
        return self._robots[origin].can_fetch(self.user_agent, url)

    def fetch(self, url: str) -> FetchResult:
        if not self._allowed(url):
            raise PermissionError(f"robots.txt disallows {url}")
        origin = urlsplit(url).netloc
        elapsed = time.monotonic() - self._last_request.get(origin, 0)
        if elapsed < self.rate_limit:
            time.sleep(self.rate_limit - elapsed)
        error: Exception | None = None
        for attempt in range(self.retries):
            try:
                response = self.client.get(url)
                response.raise_for_status()
                self._last_request[origin] = time.monotonic()
                return FetchResult(
                    requested_url=url,
                    final_url=str(response.url),
                    status_code=response.status_code,
                    content_type=response.headers.get("content-type"),
                    body=response.content,
                    response_sha256=content_sha256(response.content),
                    retrieved_at=datetime.now(UTC),
                    etag=response.headers.get("etag"),
                    last_modified=response.headers.get("last-modified"),
                )
            except httpx.HTTPError as exc:
                error = exc
                if attempt + 1 < self.retries:
                    time.sleep(min(2**attempt, 8))
        assert error is not None
        raise error

