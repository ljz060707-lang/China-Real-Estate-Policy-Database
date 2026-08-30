"""CRPD platform fetcher regressions — typed error classification + redirects.

Error classification is deterministic; redirect behavior is tested with an
injected httpx client (MockTransport), no network.
"""
from __future__ import annotations

import httpx
import pytest

from policydb.crawl.fetcher import (
    CaptchaDetected,
    ConnectError,
    ConnectTimeout,
    CrawlFetchError,
    DnsError,
    Http5xx,
    Http403,
    Http404,
    Http429,
    ReadTimeout,
    RespectfulFetcher,
    RobotsBlocked,
    TlsError,
    classify_fetch_error,
)

URL = "https://zjj.example.gov.cn/zcfg/t20220101.html"


def _http_status_error(status: int) -> httpx.HTTPStatusError:
    request = httpx.Request("GET", URL)
    return httpx.HTTPStatusError(
        f"HTTP {status}", request=request, response=httpx.Response(status, request=request)
    )


def test_typed_error_passthrough_is_identity():
    error = Http404("not found")
    assert classify_fetch_error(error, URL) is error


@pytest.mark.parametrize(
    ("exception", "expected"),
    [
        (httpx.ConnectTimeout("timeout"), ConnectTimeout),
        (httpx.ReadTimeout("timeout"), ReadTimeout),
        (httpx.ConnectError("SSL: CERTIFICATE_VERIFY_FAILED"), TlsError),
        (httpx.ConnectError("unexpected_eof while reading"), TlsError),
        (httpx.ConnectError("[Errno -2] Name or service not known"), DnsError),
        (httpx.ConnectError("Connection refused"), ConnectError),
        (_http_status_error(403), Http403),
        (_http_status_error(404), Http404),
        (_http_status_error(429), Http429),
        (_http_status_error(500), Http5xx),
        (_http_status_error(503), Http5xx),
        (_http_status_error(401), ConnectError),
        (RuntimeError("boom"), ConnectError),
    ],
)
def test_error_classification(exception, expected):
    classified = classify_fetch_error(exception, URL)
    assert isinstance(classified, expected)
    assert isinstance(classified, CrawlFetchError)
    assert URL in str(classified)


@pytest.mark.parametrize(
    ("cls", "retryable"),
    [
        (Http403, False),
        (Http404, False),
        (Http429, True),
        (Http5xx, True),
        (RobotsBlocked, False),
        (CaptchaDetected, False),
        (ConnectTimeout, True),
    ],
)
def test_retryable_flags(cls, retryable):
    assert cls("x").retryable is retryable


def test_fetch_follows_redirect_and_reports_final_url():
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/start":
            return httpx.Response(302, headers={"Location": "/zcfg/2022/t20220101.html"})
        return httpx.Response(
            200,
            content="<html><head><title>通知</title></head><body><p>正文</p></body></html>".encode(),
            headers={"content-type": "text/html; charset=utf-8"},
        )

    client = httpx.Client(transport=httpx.MockTransport(handler), follow_redirects=True)
    fetcher = RespectfulFetcher(client=client, check_robots=False, retries=1)
    result = fetcher.fetch("https://zjj.example.gov.cn/start")
    assert result.status_code == 200
    assert result.final_url == "https://zjj.example.gov.cn/zcfg/2022/t20220101.html"
    assert result.requested_url == "https://zjj.example.gov.cn/start"
    assert result.response_sha256
    assert result.network_route == "injected_client"


def test_fetch_403_raises_typed_non_retryable():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(403)

    client = httpx.Client(transport=httpx.MockTransport(handler))
    fetcher = RespectfulFetcher(client=client, check_robots=False, retries=1)
    with pytest.raises(Http403):
        fetcher.fetch(URL)
