from __future__ import annotations

import time
from collections.abc import Callable
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any
from urllib.parse import urlsplit
from urllib.robotparser import RobotFileParser

import httpx

from policydb.crawl.dedup import content_sha256
from policydb.crawl.models import FetchResult
from policydb.network import GovernmentDirectClient, government_browser_headers


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
        result = ConnectTimeout(message)
        _copy_network_context(error, result)
        return result
    if isinstance(error, httpx.ReadTimeout):
        result = ReadTimeout(message)
        _copy_network_context(error, result)
        return result
    if isinstance(error, httpx.ConnectError):
        lowered = str(error).lower()
        if "ssl" in lowered or "tls" in lowered or "unexpected_eof" in lowered or "eof while reading" in lowered:
            result = TlsError(message)
        else:
            result = (
                DnsError(message)
                if "name" in lowered or "dns" in lowered
                else ConnectError(message)
            )
        _copy_network_context(error, result)
        return result
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


def _copy_network_context(source: Exception, target: Exception) -> None:
    for name in (
        "redirect_chain",
        "network_route",
        "requested_protocol",
        "failed_url",
    ):
        if hasattr(source, name):
            setattr(target, name, getattr(source, name))


class RespectfulFetcher:
    def __init__(
        self,
        *,
        client: httpx.Client | None = None,
        user_agent: str | None = None,
        timeout: float = 30,
        connect_timeout: float = 10,
        retries: int = 3,
        rate_limit: float = 0.5,
        check_robots: bool = True,
        max_response_bytes: int = 50 * 1024 * 1024,
        attempt_callback: Callable[[dict[str, Any]], Any] | None = None,
    ) -> None:
        self.user_agent = government_browser_headers(user_agent)["User-Agent"]
        self.direct_client = (
            None
            if client is not None
            else GovernmentDirectClient(
                user_agent=self.user_agent,
                timeout=timeout,
                connect_timeout=connect_timeout,
                attempt_callback=attempt_callback,
            )
        )
        self.client = client or self.direct_client.client
        self.retries = retries
        self.rate_limit = rate_limit
        self.check_robots = check_robots
        self.max_response_bytes = max_response_bytes
        self.attempt_callback = attempt_callback
        self._robots: dict[str, RobotFileParser] = {}
        self._last_request: dict[str, float] = {}

    def _allowed(self, url: str) -> bool:
        if not self.check_robots:
            return True
        parts = urlsplit(url)
        origin = f"{parts.scheme}://{parts.netloc}"
        if origin not in self._robots:
            parser = RobotFileParser(origin + "/robots.txt")
            attempt_id = None
            try:
                if self.direct_client is not None:
                    # The direct client owns the request and invokes the
                    # attempt callback itself.  Calling its underlying
                    # httpx client here would bypass the HTTP budget ledger.
                    direct_response = self.direct_client.get(parser.url)
                    parser.parse(
                        direct_response.content.decode("utf-8", errors="ignore").splitlines()
                        if direct_response.status_code == 200
                        else []
                    )
                else:
                    if self.attempt_callback is not None:
                        attempt_id = self.attempt_callback(
                            {"phase": "before", "stage": "robots", "url": parser.url, "attempt": 1}
                        )
                    response = self.client.get(parser.url)
                    if self.attempt_callback is not None:
                        self.attempt_callback(
                            {"phase": "after", "attempt_id": attempt_id, "url": parser.url, "status_code": response.status_code}
                        )
                    parser.parse(response.text.splitlines() if response.status_code == 200 else [])
            except httpx.HTTPError as exc:
                if self.attempt_callback is not None and self.direct_client is None:
                    self.attempt_callback(
                        {"phase": "after", "attempt_id": attempt_id, "url": parser.url, "error_type": type(exc).__name__, "error_message": str(exc)}
                    )
                parser.parse([])
            self._robots[origin] = parser
        return self._robots[origin].can_fetch(self.user_agent, url)

    def fetch(
        self,
        url: str,
        *,
        etag: str | None = None,
        last_modified: str | None = None,
        referer: str | None = None,
    ) -> FetchResult:
        if not self._allowed(url):
            raise RobotsBlocked(f"robots.txt disallows {url}")
        origin = urlsplit(url).netloc
        elapsed = time.monotonic() - self._last_request.get(origin, 0)
        if elapsed < self.rate_limit:
            time.sleep(self.rate_limit - elapsed)
        error: Exception | None = None
        for attempt in range(self.retries):
            response = None
            injected_attempt_id = None
            try:
                headers = {}
                if etag:
                    headers["If-None-Match"] = etag
                if last_modified:
                    headers["If-Modified-Since"] = last_modified
                if referer:
                    headers["Referer"] = referer
                direct_response = (
                    self.direct_client.get(url, headers=headers)
                    if self.direct_client is not None
                    else None
                )
                if direct_response is None and self.attempt_callback is not None:
                    injected_attempt_id = self.attempt_callback(
                        {"phase": "before", "stage": "government_fetch", "url": url, "attempt": attempt + 1}
                    )
                response = self.client.get(url, headers=headers) if direct_response is None else None
                if direct_response is None and self.attempt_callback is not None:
                    self.attempt_callback(
                        {"phase": "after", "attempt_id": injected_attempt_id, "url": url, "status_code": response.status_code}
                    )
                status_code = (
                    direct_response.status_code
                    if direct_response is not None
                    else response.status_code
                )
                response_headers = (
                    direct_response.headers
                    if direct_response is not None
                    else response.headers
                )
                response_content = (
                    direct_response.content
                    if direct_response is not None
                    else response.content
                )
                response_url = (
                    direct_response.final_url
                    if direct_response is not None
                    else str(response.url)
                )
                if status_code == 304:
                    return FetchResult(
                        requested_url=url,
                        final_url=response_url,
                        status_code=304,
                        content_type=response_headers.get("content-type"),
                        body=b"",
                        response_sha256="",
                        retrieved_at=datetime.now(UTC),
                        etag=response_headers.get("etag") or etag,
                        last_modified=response_headers.get("last-modified") or last_modified,
                        not_modified=True,
                        redirect_chain=(
                            direct_response.redirect_chain
                            if direct_response is not None
                            else []
                        ),
                        network_route=(
                            direct_response.network_route
                            if direct_response is not None
                            else "injected_client"
                        ),
                        protocol=urlsplit(response_url).scheme,
                    )
                if status_code == 429:
                    error = Http429(f"HTTP 429 for {url}")
                    if attempt + 1 < self.retries:
                        if response is not None:
                            time.sleep(self._retry_delay(response, attempt))
                        else:
                            time.sleep(min(2**attempt, 8))
                        continue
                if response is not None:
                    response.raise_for_status()
                elif status_code >= 400:
                    raise httpx.HTTPStatusError(
                        f"HTTP {status_code}",
                        request=httpx.Request("GET", url),
                        response=httpx.Response(status_code, request=httpx.Request("GET", url)),
                    )
                if len(response_content) > self.max_response_bytes:
                    raise EmptyContent(f"response exceeds {self.max_response_bytes} bytes")
                content_type = response_headers.get("content-type", "").lower()
                sample = (
                    response_content[:5000].decode("utf-8", errors="ignore").lower()
                    if "text" in content_type
                    else ""
                )
                if any(marker in sample for marker in ("请输入验证码", "captcha", "访问验证")):
                    raise CaptchaDetected(f"captcha detected for {url}")
                self._last_request[origin] = time.monotonic()
                return FetchResult(
                    requested_url=url,
                    final_url=response_url,
                    status_code=status_code,
                    content_type=response_headers.get("content-type"),
                    body=response_content,
                    response_sha256=content_sha256(response_content),
                    retrieved_at=datetime.now(UTC),
                    etag=response_headers.get("etag"),
                    last_modified=response_headers.get("last-modified"),
                    redirect_chain=(
                        direct_response.redirect_chain
                        if direct_response is not None
                        else []
                    ),
                    network_route=(
                        direct_response.network_route
                        if direct_response is not None
                        else "injected_client"
                    ),
                    protocol=urlsplit(response_url).scheme,
                    resolved_addresses=(
                        direct_response.resolved_addresses
                        if direct_response is not None
                        else []
                    ),
                    fallback_used=(
                        direct_response.fallback_used
                        if direct_response is not None
                        else None
                    ),
                )
            except (httpx.HTTPError, CrawlFetchError) as exc:
                if (
                    self.attempt_callback is not None
                    and self.direct_client is None
                    and injected_attempt_id is not None
                    and response is None
                ):
                    self.attempt_callback(
                        {
                            "phase": "after",
                            "attempt_id": injected_attempt_id,
                            "url": url,
                            "error_type": type(exc).__name__,
                            "error_message": str(exc),
                        }
                    )
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
