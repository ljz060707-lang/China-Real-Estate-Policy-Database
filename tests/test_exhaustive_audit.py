from __future__ import annotations

import shutil
from datetime import date
from pathlib import Path
from types import SimpleNamespace

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
from policydb.network import (
    AIProxyClient,
    GovernmentDirectClient,
    compare_routes,
    probe_direct,
    probe_proxy,
)
from policydb.settings import Settings
from policydb.source_slots import (
    audit_525,
    build_requirement_slots,
    list_candidates,
    probe_candidates,
    reconcile_registry_roles,
    seed_candidates_from_registry,
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


def test_proxy_probe_detects_mixed_protocol_without_credentials(monkeypatch):
    monkeypatch.setattr(
        "policydb.network._curl_proxy",
        lambda url, proxy_url: {"ok": True, "status_code": 200},
    )
    result = probe_proxy(
        url="https://example.com", proxy_url="http://user:secret@127.0.0.1:7897"
    )
    assert result["protocol"] == "mixed"
    assert result["proxy"]["credentials_present"] is True
    assert "secret" not in str(result)


def test_direct_probe_marks_tun_fake_ip(monkeypatch):
    monkeypatch.setattr(
        GovernmentDirectClient,
        "resolve_host",
        staticmethod(lambda url: ["198.18.0.75"]),
    )
    monkeypatch.setattr(
        "policydb.network._attempt_httpx",
        lambda url, trust_env: {"ok": False, "network_status": "tls_incompatible"},
    )
    monkeypatch.setattr("policydb.network._curl_direct", lambda url: {"ok": False})
    result = probe_direct(url="https://city.gov.cn/")
    assert result["tun_fake_ip_detected"] is True
    assert result["network_status"] == "tun_intercepted"


def test_route_compare_prefers_proxy_when_direct_is_blocked(monkeypatch):
    monkeypatch.setattr(
        "policydb.network.probe_direct",
        lambda url: {"network_status": "tun_intercepted", "tun_fake_ip_detected": True},
    )
    monkeypatch.setattr(
        "policydb.network.probe_proxy",
        lambda **kwargs: {"attempts": {"http": {"ok": True}}},
    )
    result = compare_routes(url="https://city.gov.cn/")
    assert result["selected_route"] == "proxy"


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


def test_candidate_requires_two_direct_parser_probes(tmp_path):
    settings = _source_root(tmp_path)
    build_requirement_slots(settings)
    upsert_candidates(
        [
            {
                "city_id": "CITY_320100",
                "source_role": "housing_department",
                "candidate_url": "https://fcj.nanjing.gov.cn/zwgk/",
                "site_name": "南京市住房和城乡建设局",
                "department_name": "南京市住房和城乡建设局",
            }
        ],
        settings,
    )

    class FakeFetcher:
        def fetch(self, url):
            return SimpleNamespace(
                status_code=200,
                body=(
                    b"<html><title>Policies</title><nav><a href='/'>Home</a></nav>"
                        b"<a href='/zwgk/2026/01/01/policy-1'>2026-01-01 Policy item one</a>"
                    b"<a rel='next' href='/zwgk/page-2'>Next</a></html>"
                ),
                content_type="text/html",
                final_url=url,
                redirect_chain=[],
                network_route="direct_ok",
                response_sha256="abc",
            )

    result = probe_candidates(settings=settings, fetcher=FakeFetcher())
    row = list_candidates(settings=settings).row(0, named=True)
    assert result["checked"] == 1
    assert result["parser_verified"] == 1
    assert result["verification"]["verified"] == 1
    assert row["health_probe_success_count"] == 2
    assert row["pagination_strategy"] == "next_link"
    assert row["network_route"] == "direct_ok"


def test_authoritative_verification_revokes_detail_page_and_enablement(tmp_path):
    settings = _source_root(tmp_path)
    build_requirement_slots(settings)
    upsert_candidates(
        [
            {
                "city_id": "CITY_320100",
                "source_role": "municipal_government",
                "candidate_url": (
                    "https://www.nanjing.gov.cn/zdgk/202302/"
                    "t20230203_3818202.html"
                ),
                "site_name": "南京市人民政府",
                "city_match_evidence": "registry city match",
                "role_match_evidence": "registry role match",
                "health_status": "healthy",
                "entry_eligible": True,
                "is_verified": True,
                "is_enabled": True,
                "manual_review_status": "approved",
            }
        ],
        settings,
    )
    result = verify_candidates(city="南京市", settings=settings)
    candidate = list_candidates(city="南京市", settings=settings).row(0, named=True)
    assert result["checked"] == 1
    assert result["verified"] == 0
    assert result["enabled"] == 0
    assert result["failed_candidates"] == 1
    assert result["candidate_results"][0]["failed_gates"]
    assert candidate["is_verified"] is False
    assert candidate["is_enabled"] is False
    assert candidate["entry_eligible"] is False
    assert candidate["manual_review_status"] == "rejected_by_gate"


def test_registry_role_reconciliation_and_gazette_projection(tmp_path):
    settings = _source_root(tmp_path)
    registry = {
        "version": 2,
        "sources": [
            {
                "source_id": "SRC_GJJ",
                "source_name": "南京住房公积金管理中心",
                "domain": "gjj.nanjing.gov.cn",
                "source_type": "government",
                "source_role": "canonical_candidate",
                "agency_type": "housing_department",
                "official_status": "official",
                "homepage_url": "https://gjj.nanjing.gov.cn/",
                "list_page_urls": ["https://gjj.nanjing.gov.cn/zcfg/"],
                "city_ids": ["CITY_320100"],
                "scope_type": "municipal",
                "crawl_enabled": False,
            },
            {
                "source_id": "SRC_GOV",
                "source_name": "南京市人民政府",
                "domain": "nanjing.gov.cn",
                "source_type": "government",
                "source_role": "canonical_candidate",
                "agency_type": "municipal_government",
                "official_status": "official",
                "homepage_url": "https://www.nanjing.gov.cn/",
                "gazette_url": "https://www.nanjing.gov.cn/xxgkn/zfgb/",
                "city_ids": ["CITY_320100"],
                "scope_type": "municipal",
                "crawl_enabled": False,
            },
        ],
    }
    (settings.root / "data/reference/source_registry.yaml").write_text(
        __import__("yaml").safe_dump(registry, allow_unicode=True), encoding="utf-8"
    )
    build_requirement_slots(settings)
    preview = reconcile_registry_roles(settings=settings)
    assert preview["change_count"] == 1
    reconcile_registry_roles(settings=settings, apply=True)
    seed_candidates_from_registry(settings)
    candidates = list_candidates(city="南京市", settings=settings)
    assert candidates.filter(
        (pl.col("source_role") == "provident_fund_center")
        & pl.col("canonical_url").str.contains("gjj.nanjing")
    ).height >= 1
    assert candidates.filter(
        (pl.col("source_role") == "government_gazette")
        & pl.col("canonical_url").str.contains("/xxgkn/zfgb")
    ).height == 1


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
