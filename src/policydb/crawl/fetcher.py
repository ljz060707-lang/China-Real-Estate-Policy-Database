from __future__ import annotations

import threading
import time
from collections import Counter
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
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


@dataclass(frozen=True, slots=True)
class FetchTask:
    """One immutable network request in the bounded fetch lane."""

    key: str
    url: str
    etag: str | None = None
    last_modified: str | None = None
    rate_limit: float | None = None


class HostAwareFetchPool:
    """Bounded cross-host prefetch with deterministic, caller-owned commits.

    The pool performs network reads only.  Callers still consume results in
    their stable task order and remain solely responsible for parsing,
    checkpointing and database writes.  At most ``max_workers`` futures are
    queued, preventing an unbounded crawl plan from becoming an unbounded
    in-memory work queue.
    """

    def __init__(
        self,
        fetcher: object,
        tasks: list[FetchTask],
        *,
        max_workers: int,
        per_host_concurrency: int = 1,
    ) -> None:
        if max_workers < 1:
            raise ValueError("max_workers must be positive")
        if per_host_concurrency < 1:
            raise ValueError("per_host_concurrency must be positive")
        keys = [task.key for task in tasks]
        if len(keys) != len(set(keys)):
            raise ValueError("fetch task keys must be unique")
        self.fetcher = fetcher
        self.tasks = tasks
        self.max_workers = min(max_workers, max(len(tasks), 1))
        self.per_host_concurrency = per_host_concurrency
        self._executor: ThreadPoolExecutor | None = None
        self._futures: dict[str, Future[FetchResult]] = {}
        self._next_task = 0
        self._host_semaphores: dict[str, threading.BoundedSemaphore] = {}
        self._host_lock = threading.Lock()
        self._metrics_lock = threading.Lock()
        self._attempted = 0
        self._succeeded = 0
        self._failed = 0
        self._latencies: list[float] = []
        self._response_bytes = 0
        self._cache_hits = 0
        self._status_codes: Counter[int] = Counter()
        self._error_types: Counter[str] = Counter()
        self._started_at: float | None = None
        self._closed = False
        self._submission_stopped = False

    def __enter__(self) -> HostAwareFetchPool:
        self._started_at = time.perf_counter()
        self._executor = ThreadPoolExecutor(
            max_workers=self.max_workers,
            thread_name_prefix="crpd-fetch",
        )
        self._fill_window()
        return self

    def __exit__(self, *_exc_info: object) -> None:
        self.close()

    def _host_semaphore(self, url: str) -> threading.BoundedSemaphore:
        host = urlsplit(url).netloc.lower()
        with self._host_lock:
            return self._host_semaphores.setdefault(
                host,
                threading.BoundedSemaphore(self.per_host_concurrency),
            )

    def _run(self, task: FetchTask) -> FetchResult:
        started = time.perf_counter()
        with self._metrics_lock:
            self._attempted += 1
        try:
            with self._host_semaphore(task.url):
                kwargs: dict[str, object] = {
                    "etag": task.etag,
                    "last_modified": task.last_modified,
                }
                if isinstance(self.fetcher, RespectfulFetcher):
                    kwargs["rate_limit"] = task.rate_limit
                result = self.fetcher.fetch(task.url, **kwargs)
            with self._metrics_lock:
                self._succeeded += 1
                self._response_bytes += int(result.response_bytes or 0)
                self._cache_hits += int(bool(result.cache_hit))
                self._status_codes[int(result.status_code)] += 1
            return result
        except Exception as exc:
            with self._metrics_lock:
                self._failed += 1
                self._error_types[type(exc).__name__] += 1
            raise
        finally:
            with self._metrics_lock:
                self._latencies.append(time.perf_counter() - started)

    def _fill_window(self) -> None:
        if self._executor is None:
            raise RuntimeError("fetch pool must be entered before use")
        window = max(self.max_workers, 1)
        while (
            not self._submission_stopped
            and self._next_task < len(self.tasks)
            and len(self._futures) < window
        ):
            task = self.tasks[self._next_task]
            self._next_task += 1
            self._futures[task.key] = self._executor.submit(self._run, task)

    def get(self, key: str) -> FetchResult:
        """Return one result while preserving the caller's stable order."""

        if self._closed:
            raise RuntimeError("fetch pool is closed")
        future = self._futures.get(key)
        if future is None:
            raise KeyError(f"fetch task is not in the active bounded window: {key}")
        try:
            return future.result()
        finally:
            self._futures.pop(key, None)
            self._fill_window()

    def stop_submitting(self) -> None:
        """Stop extending the window while allowing in-flight work to settle."""

        self._submission_stopped = True

    def has_task(self, key: str) -> bool:
        return key in self._futures

    def metrics(self) -> dict[str, int | float | None]:
        with self._metrics_lock:
            latencies = sorted(self._latencies)
            attempted = self._attempted
            succeeded = self._succeeded
            failed = self._failed
            response_bytes = self._response_bytes
            cache_hits = self._cache_hits
            status_codes = self._status_codes.copy()
            error_types = self._error_types.copy()
        elapsed = (
            time.perf_counter() - self._started_at
            if self._started_at is not None
            else 0.0
        )

        def percentile(fraction: float) -> float | None:
            if not latencies:
                return None
            index = min(round((len(latencies) - 1) * fraction), len(latencies) - 1)
            return round(latencies[index], 3)

        timeout_count = sum(
            count
            for name, count in error_types.items()
            if "timeout" in name.lower()
        )
        http_429_count = status_codes.get(429, 0) + error_types.get("Http429", 0)
        http_5xx_count = sum(
            count for status, count in status_codes.items() if 500 <= status <= 599
        ) + error_types.get("Http5xx", 0)

        def rate(count: int) -> float:
            return round(count / attempted, 6) if attempted else 0.0

        return {
            "attempted": attempted,
            "succeeded": succeeded,
            "failed": failed,
            "global_concurrency": self.max_workers,
            "per_host_concurrency": self.per_host_concurrency,
            "wall_seconds": round(elapsed, 3),
            "requests_per_minute": (
                round(attempted * 60 / elapsed, 3) if elapsed > 0 else None
            ),
            "bytes_per_minute": (
                round(response_bytes * 60 / elapsed, 3) if elapsed > 0 else None
            ),
            "latency_p50_seconds": percentile(0.50),
            "latency_p95_seconds": percentile(0.95),
            "timeout_rate": rate(timeout_count),
            "http_429_rate": rate(http_429_count),
            "http_5xx_rate": rate(http_5xx_count),
            "cache_reuse_rate": rate(cache_hits),
        }

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for future in self._futures.values():
            future.cancel()
        if self._executor is not None:
            self._executor.shutdown(wait=True, cancel_futures=True)


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
        self._state_lock = threading.Lock()
        self._callback_lock = threading.Lock()
        self._origin_locks: dict[str, threading.Lock] = {}
        self.attempt_callback = attempt_callback
        self.user_agent = government_browser_headers(user_agent)["User-Agent"]
        self.direct_client = (
            None
            if client is not None
            else GovernmentDirectClient(
                user_agent=self.user_agent,
                timeout=timeout,
                connect_timeout=connect_timeout,
                attempt_callback=self._attempt_event if attempt_callback else None,
            )
        )
        self.client = client or self.direct_client.client
        self.retries = retries
        self.rate_limit = rate_limit
        self.check_robots = check_robots
        self.max_response_bytes = max_response_bytes
        self._robots: dict[str, RobotFileParser] = {}
        self._last_request: dict[str, float] = {}

    def _attempt_event(self, event: dict[str, Any]) -> Any:
        if self.attempt_callback is None:
            return None
        with self._callback_lock:
            return self.attempt_callback(event)

    def _origin_lock(self, origin: str) -> threading.Lock:
        with self._state_lock:
            return self._origin_locks.setdefault(origin, threading.Lock())

    def _allowed(self, url: str) -> bool:
        if not self.check_robots:
            return True
        parts = urlsplit(url)
        origin = f"{parts.scheme}://{parts.netloc}"
        with self._origin_lock(origin):
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
                            attempt_id = self._attempt_event(
                                {"phase": "before", "stage": "robots", "url": parser.url, "attempt": 1}
                            )
                        response = self.client.get(parser.url)
                        if self.attempt_callback is not None:
                            self._attempt_event(
                                {"phase": "after", "attempt_id": attempt_id, "url": parser.url, "status_code": response.status_code}
                            )
                        parser.parse(response.text.splitlines() if response.status_code == 200 else [])
                except httpx.HTTPError as exc:
                    if self.attempt_callback is not None and self.direct_client is None:
                        self._attempt_event(
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
        rate_limit: float | None = None,
    ) -> FetchResult:
        if not self._allowed(url):
            raise RobotsBlocked(f"robots.txt disallows {url}")
        origin = urlsplit(url).netloc
        effective_rate_limit = self.rate_limit if rate_limit is None else max(rate_limit, 0.0)
        with self._origin_lock(origin):
            elapsed = time.monotonic() - self._last_request.get(origin, 0)
            if elapsed < effective_rate_limit:
                time.sleep(effective_rate_limit - elapsed)
            self._last_request[origin] = time.monotonic()
        error: Exception | None = None
        request_started_at = datetime.now(UTC)
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
                    injected_attempt_id = self._attempt_event(
                        {"phase": "before", "stage": "government_fetch", "url": url, "attempt": attempt + 1}
                    )
                response = self.client.get(url, headers=headers) if direct_response is None else None
                if direct_response is None and self.attempt_callback is not None:
                    self._attempt_event(
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
                        request_started_at=request_started_at,
                        request_finished_at=datetime.now(UTC),
                        response_bytes=0,
                        cache_hit=True,
                        network_source="LIVE_HTTP",
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
                    request_started_at=request_started_at,
                    request_finished_at=datetime.now(UTC),
                    response_bytes=len(response_content),
                    cache_hit=False,
                    network_source="LIVE_HTTP",
                )
            except (httpx.HTTPError, CrawlFetchError) as exc:
                if (
                    self.attempt_callback is not None
                    and self.direct_client is None
                    and injected_attempt_id is not None
                    and response is None
                ):
                    self._attempt_event(
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
