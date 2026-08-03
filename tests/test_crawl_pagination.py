import json
from datetime import UTC, date, datetime

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


def test_gazette_issue_index_does_not_promote_breadcrumbs_to_documents():
    class _GazetteFetcher:
        def __init__(self):
            self.calls = []

        def fetch(self, url, *, referer=None):
            self.calls.append((url, referer))
            if "findUrl?" in url:
                issue = "1" if "gbqs=1" in url else "2"
                issue_url = (
                    "https://www.beijing.gov.cn/zhengce/zfgb/lsgb/"
                    f"20260{issue}/t20260{issue}20_{issue}.html"
                )
                body = json.dumps({"url": issue_url}).encode()
                return FetchResult(
                    requested_url=url,
                    final_url=url,
                    status_code=200,
                    content_type="application/json",
                    body=body,
                    response_sha256="endpoint-" + issue,
                    retrieved_at=datetime.now(UTC),
                    network_route="direct",
                )
            body = (
                b'<a href="/gongkai/">breadcrumb</a>'
                b'<a href="/zhengce/">breadcrumb</a>'
                b'<script>showUrl(2026,1); showUrl(2026,2)</script>'
            )
            return FetchResult(
                requested_url=url,
                final_url=url,
                status_code=200,
                content_type="text/html",
                body=body,
                response_sha256="index",
                retrieved_at=datetime.now(UTC),
                network_route="direct",
            )

    source = RegisteredSource(
        source_id="BJ-GAZETTE",
        source_name="Beijing Gazette",
        domain="beijing.gov.cn",
        source_type="government",
        source_role="government_gazette",
        agency_type="government_gazette",
        official_status="official",
        list_page_urls=["https://www.beijing.gov.cn/so/zcdh/zfgbHistory"],
    )
    fetcher = _GazetteFetcher()
    discovery = ListPageDiscovery(fetcher)
    candidates = discovery.discover(
        DiscoveryRequest(
            run_id="R-GAZETTE",
            mode="historical_105",
            start_date=date(2018, 1, 1),
            end_date=date(2026, 12, 31),
            max_pages=20,
            max_candidates=20,
        ),
        source,
    )

    assert len(candidates) == 2
    assert all("/zhengce/zfgb/lsgb/" in item.url for item in candidates)
    assert all("/gongkai/" not in item.url for item in candidates)
    assert discovery.last_scan["special_strategy"] == "gazette_issue_index"
    assert discovery.last_scan["pagination_exhausted"] is True
    assert discovery.last_scan["issue_resolution_error_count"] == 0
    assert len(fetcher.calls) == 3
    assert all(call[1] for call in fetcher.calls[1:])
