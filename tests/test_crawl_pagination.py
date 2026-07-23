from datetime import date

from policydb.crawl.discovery import ListPageDiscovery
from policydb.crawl.models import DiscoveryRequest, FetchResult, RegisteredSource


class _PagedFetcher:
    def fetch(self, url):
        page = int(url.rsplit("/", 1)[-1])
        body = (
            f'<a href="/detail/{page}">政策 {page} 2021-01-01</a>'
            f'<a rel="next" href="/list/{page + 1}">下一页</a>'
        ).encode()
        from datetime import UTC, datetime

        return FetchResult(
            requested_url=url,
            final_url=url,
            status_code=200,
            content_type="text/html",
            body=body,
            response_sha256=str(page),
            retrieved_at=datetime.now(UTC),
        )


def _source():
    return RegisteredSource(
        source_id="S1",
        source_name="来源",
        domain="example.gov.cn",
        source_type="government",
        source_role="canonical_candidate",
        official_status="official",
        list_page_urls=["https://example.gov.cn/list/1"],
    )


def test_page_limit_is_exposed_as_partial_evidence():
    discovery = ListPageDiscovery(_PagedFetcher())
    candidates = discovery.discover(
        DiscoveryRequest(
            run_id="R1",
            mode="historical_105",
            start_date=date(2021, 1, 1),
            end_date=date(2021, 12, 31),
            max_pages=2,
            max_candidates=20,
        ),
        _source(),
    )
    assert len(candidates) == 2
    assert discovery.last_scan["pages_scanned"] == 2
    assert discovery.last_scan["stop_reason"] == "page_limit"
    assert discovery.last_scan["pagination_exhausted"] is False
