from __future__ import annotations

import time
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from urllib.parse import urlsplit
from urllib.robotparser import RobotFileParser

import httpx

from policydb.crawl.dedup import content_sha256
from policydb.crawl.models import FetchResult


class CrawlFetchError(RuntimeError):
    retryable = True


class DnsError(CrawlFetchError): ...
class ConnectError(CrawlFetchError): ...
class ConnectTimeout(CrawlFetchError): ...
class ReadTimeout(CrawlFetchError): ...
class TlsError(CrawlFetchError): ...
class Http403(CrawlFetchError):
    retryable = False
class Http404(CrawlFetchError):
    retryable = False
class Http429(CrawlFetchError): ...
class Http5xx(CrawlFetchError): ...
class RobotsBlocked(CrawlFetchError):
    retryable = False
class CaptchaDetected(CrawlFetchError):
    retryable = False
class PermissionErrorLocal(CrawlFetchError):
    retryable = False
class ParseError(CrawlFetchError):
    retryable = False
class EmptyContent(CrawlFetchError): ...
class UnsupportedContentType(CrawlFetchError):
    retryable = False


def classify_fetch_error(error: Exception, url: str = "") -> CrawlFetchError:
    message = f"{type(error).__name__}: {error}" + (f" [{url}]" if url else "")
    if isinstance(error, CrawlFetchError):
        return error
    if isinstance(error, httpx.ConnectTimeout):
        return ConnectTimeout(message)
    if isinstance(error, httpx.ReadTimeout):
        return ReadTimeout(message)
    if isinstance(error, httpx.ConnectError):
        lowered = str(error).lower()
        return DnsError(message) if "name" in lowered or "dns" in lowered else ConnectError(message)
    if isinstance(error, httpx.HTTPStatusError):
        status = error.response.status_code
        if status == 403:
            return Http403(message)
        if status == 404:
            return Http404(message)
        if status == 429:
            return Http429(message)
        if status >= 500:
            return Http5xx(message)
    return ConnectError(message)


class RespectfulFetcher:
    def __init__(
        self,
        *,
        client: httpx.Client | None = None,
        user_agent: str = "Mozilla/5.0 (compatible; PolicyDBResearchBot/0.1; +local-research)",
        timeout: float = 30,
        connect_timeout: float = 10,
        retries: int = 3,
        rate_limit: float = 0.5,
        check_robots: bool = True,
        max_response_bytes: int = 50 * 1024 * 1024,
    ) -> None:
        self.user_agent = user_agent
        self.client = client or httpx.Client(
            headers={"User-Agent": user_agent},
            timeout=httpx.Timeout(timeout, connect=connect_timeout),
            follow_redirects=True,
            max_redirects=10,
        )
        self.retries = retries
        self.rate_limit = rate_limit
        self.check_robots = check_robots
        self.max_response_bytes = max_response_bytes
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

    def fetch(
        self,
        url: str,
        *,
        etag: str | None = None,
        last_modified: str | None = None,
    ) -> FetchResult:
        if not self._allowed(url):
            raise RobotsBlocked(f"robots.txt disallows {url}")
        origin = urlsplit(url).netloc
        elapsed = time.monotonic() - self._last_request.get(origin, 0)
        if elapsed < self.rate_limit:
            time.sleep(self.rate_limit - elapsed)
        error: Exception | None = None
        for attempt in range(self.retries):
            try:
                headers = {}
                if etag:
                    headers["If-None-Match"] = etag
                if last_modified:
                    headers["If-Modified-Since"] = last_modified
                response = self.client.get(url, headers=headers)
                if response.status_code == 304:
                    return FetchResult(
                        requested_url=url,
                        final_url=str(response.url),
                        status_code=304,
                        content_type=response.headers.get("content-type"),
                        body=b"",
                        response_sha256="",
                        retrieved_at=datetime.now(UTC),
                        etag=response.headers.get("etag") or etag,
                        last_modified=response.headers.get("last-modified") or last_modified,
                        not_modified=True,
                    )
                if response.status_code == 429:
                    error = Http429(f"HTTP 429 for {url}")
                    if attempt + 1 < self.retries:
                        time.sleep(self._retry_delay(response, attempt))
                        continue
                response.raise_for_status()
                if len(response.content) > self.max_response_bytes:
                    raise EmptyContent(f"response exceeds {self.max_response_bytes} bytes")
                content_type = response.headers.get("content-type", "").lower()
                sample = response.text[:5000].lower() if "text" in content_type else ""
                if any(marker in sample for marker in ("请输入验证码", "captcha", "访问验证")):
                    raise CaptchaDetected(f"captcha detected for {url}")
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
            except (httpx.HTTPError, CrawlFetchError) as exc:
                error = classify_fetch_error(exc, url)
                if attempt + 1 < self.retries:
                    time.sleep(min(2**attempt, 8))
        assert error is not None
        raise error

    @staticmethod
    def _retry_delay(response: httpx.Response, attempt: int) -> float:
        value = response.headers.get("retry-after")
        if value:
            try:
                return min(float(value), 60.0)
            except ValueError:
                try:
                    return max(0.0, min((parsedate_to_datetime(value) - datetime.now(UTC)).total_seconds(), 60.0))
                except (TypeError, ValueError):
                    pass
        return min(2**attempt, 8)
