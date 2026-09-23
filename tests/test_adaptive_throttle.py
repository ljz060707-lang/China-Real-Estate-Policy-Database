from __future__ import annotations

import httpx
import pytest

from policydb.crawl.fetcher import RespectfulFetcher


def test_adaptive_throttle_backs_off_after_failure_and_recovers(monkeypatch):
    responses = iter([503, 200])
    sleeps: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        status = next(responses)
        return httpx.Response(status, text="ok", request=request)

    monkeypatch.setattr("policydb.crawl.fetcher.time.sleep", sleeps.append)
    fetcher = RespectfulFetcher(
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        check_robots=False,
        retries=1,
        rate_limit=0.5,
        adaptive_throttle=True,
        adaptive_min_delay=0.2,
    )

    with pytest.raises(Exception, match="503 Service Unavailable"):
        fetcher.fetch("https://example.gov.cn/a")
    after_failure = fetcher.throttle_state()["example.gov.cn"]["delay_seconds"]
    assert after_failure >= 1.0

    fetcher.fetch("https://example.gov.cn/b")
    after_success = fetcher.throttle_state()["example.gov.cn"]["delay_seconds"]
    assert 0.2 <= after_success < after_failure
    assert sleeps[-1] >= 1.0
