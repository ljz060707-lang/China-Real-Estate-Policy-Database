from __future__ import annotations

import shutil
from datetime import date
from pathlib import Path

import httpx
import polars as pl

from policydb.crawl.fetcher import TlsError, classify_fetch_error
from policydb.exhaustive import (
    CandidateDate,
    ExhaustiveCrawler,
    can_certify_city_year,
    candidate_period_decision,
    completion_status,
    extract_candidate_date,
    split_window,
)
from policydb.network import AIProxyClient, GovernmentDirectClient
from policydb.settings import Settings
from policydb.source_slots import (
    audit_525,
    build_requirement_slots,
    upsert_candidates,
    verify_candidates,
)


def _source_root(tmp_path: Path) -> Settings:
    root = tmp_path / "repo"
    reference = root / "data" / "reference"
    reference.mkdir(parents=True)
    source_reference = Path(__file__).resolve().parents[1] / "data" / "reference"
    for name in (
        "cities_105.csv",
        "city_source_requirements.yaml",
    ):
        shutil.copy2(source_reference / name, reference / name)
    (reference / "source_registry.yaml").write_text(
        "version: 2\nsources: []\n", encoding="utf-8"
    )
    return Settings(root=root)


def test_candidate_date_patterns_and_confidence():
    cases = {
        "http://a.gov.cn/xx/t20230208_1.html": date(2023, 2, 8),
        "https://a.gov.cn/202302/notice.html": date(2023, 2, 1),
        "https://a.gov.cn/2023/02/28/notice.html": date(2023, 2, 28),
        "https://a.gov.cn/policy?publishdate=2023-02-07": date(2023, 2, 7),
    }
    for url, expected in cases.items():
        result = extract_candidate_date(url)
        assert result.value == expected
        assert result.confidence >= 0.85


def test_cross_month_and_unknown_are_separate():
    assert (
        candidate_period_decision(
            CandidateDate(date(2023, 1, 31), "url", 0.99),
            date(2023, 2, 1),
            date(2023, 2, 28),
        )
        == "cross_period_rejected"
    )
    assert (
        candidate_period_decision(
            CandidateDate(None, "unknown", 0.0),
            date(2023, 2, 1),
            date(2023, 2, 28),
        )
        == "date_unknown"
    )


def test_government_client_preserves_original_protocol_and_redirect_chain():
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/start":
            return httpx.Response(
                301,
                headers={"location": "https://www.example.gov.cn/final"},
                request=request,
            )
        return httpx.Response(200, text="policy", request=request)

    client = GovernmentDirectClient(
        transport=httpx.MockTransport(handler),
        allowed_aliases={"www.example.gov.cn"},
    )
    result = client.get("http://www.example.gov.cn/start")
    assert result.requested_url.startswith("http://")
    assert result.final_url.startswith("https://")
    assert [item["status_code"] for item in result.redirect_chain] == [301, 200]


def test_government_client_rejects_cross_domain_redirect():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            302,
            headers={"location": "https://example.com/not-official"},
            request=request,
        )

    client = GovernmentDirectClient(transport=httpx.MockTransport(handler))
    try:
        client.get("http://city.gov.cn/start")
    except httpx.HTTPError as exc:
        assert "outside verified government domain" in str(exc)
    else:
        raise AssertionError("cross-domain redirect was not rejected")


def test_tls_eof_has_specific_error_class():
    error = httpx.ConnectError("[SSL: UNEXPECTED_EOF_WHILE_READING]")
    assert isinstance(classify_fetch_error(error), TlsError)


def test_clients_use_separate_proxy_policies(monkeypatch):
    calls = []

    class FakeClient:
        def __init__(self, **kwargs):
            calls.append(kwargs)

        def close(self):
            return None

    monkeypatch.setattr("policydb.network.httpx.Client", FakeClient)
    GovernmentDirectClient()
    AIProxyClient()
    assert calls[0]["trust_env"] is False
    assert calls[1]["trust_env"] is True
    assert calls[0]["verify"] is True
    assert calls[1]["verify"] is True


def test_requirement_grid_is_exactly_525_and_auditable(tmp_path):
    settings = _source_root(tmp_path)
    result = build_requirement_slots(settings)
    assert result["cities"] == 105
    assert result["required_slots"] == 525
    assert result["slots_unresolved"] == 525
    assert Path(result["output"]).exists()


def test_candidate_does_not_verify_without_city_and_role_evidence(tmp_path):
    settings = _source_root(tmp_path)
    build_requirement_slots(settings)
    upsert_candidates(
        [
            {
                "city_id": "CITY_320100",
                "source_role": "housing_department",
                "candidate_url": "https://unrelated.gov.cn/index.html",
                "site_name": "某政府网站",
            }
        ],
        settings,
    )
    result = verify_candidates(city="南京市", settings=settings)
    assert result["checked"] == 1
    assert result["verified"] == 0
    assert audit_525(settings)["slots_with_enabled_source"] == 0


def test_cap_or_network_failure_cannot_complete():
    assert (
        completion_status(
            {
                "pagination_exhausted": False,
                "candidate_cap_hit": True,
                "unique_candidate_count": 10000,
            }
        )
        == "partial_cap"
    )
    assert (
        completion_status(
            {
                "pagination_exhausted": True,
                "network_error_count": 1,
                "unique_candidate_count": 0,
            }
        )
        == "partial_network"
    )


def test_confirmed_zero_requires_natural_exhaustion():
    assert (
        completion_status(
            {
                "pagination_exhausted": True,
                "source_verified": True,
                "unique_candidate_count": 0,
                "retryable_errors": 0,
                "pending_fetch": 0,
                "date_unknown_count": 0,
                "archive_missing_count": 0,
            }
        )
        == "confirmed_zero"
    )
    assert (
        completion_status(
            {
                "pagination_exhausted": False,
                "source_verified": True,
                "unique_candidate_count": 0,
            }
        )
        != "confirmed_zero"
    )


def test_adaptive_split_reaches_days():
    halves = split_window(date(2023, 2, 1), date(2023, 2, 28))
    assert halves == [
        (date(2023, 2, 1), date(2023, 2, 14)),
        (date(2023, 2, 15), date(2023, 2, 28)),
    ]
    assert split_window(date(2023, 2, 1), date(2023, 2, 2)) == [
        (date(2023, 2, 1), date(2023, 2, 1)),
        (date(2023, 2, 2), date(2023, 2, 2)),
    ]


def test_city_year_certification_is_strict():
    complete = {
        "source_slot_coverage_pct": 100,
        "verified_source_coverage_pct": 100,
        "temporal_shard_coverage_pct": 100,
        "pagination_exhaustion_pct": 100,
        "error_closure_pct": 100,
        "archive_completion_pct": 100,
        "text_extraction_pct": 100,
        "ai_processing_pct": 100,
        "dedup_routing_pct": 100,
        "cap_hit_count": 0,
        "date_unknown_count": 0,
        "conflict_count": 0,
    }
    assert can_certify_city_year(complete)
    assert not can_certify_city_year({**complete, "date_unknown_count": 1})


def test_progress_materializes_all_city_year_cells(tmp_path):
    settings = _source_root(tmp_path)
    build_requirement_slots(settings)
    result = ExhaustiveCrawler(settings).rebuild_progress()
    years = date.today().year - 2018 + 1
    assert result["city_year_rows"] == 105 * years
    frame = pl.read_parquet(settings.curated / "city_year_progress.parquet")
    assert frame["city_id"].n_unique() == 105
    assert set(frame["status"].unique()) == {"source_incomplete"}
