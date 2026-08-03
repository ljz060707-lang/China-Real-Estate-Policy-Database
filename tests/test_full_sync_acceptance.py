from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import polars as pl

import policydb.source_slots as source_slots
from policydb.full_sync import (
    BudgetLedger,
    FullSyncConfig,
    HttpAttemptRecorder,
    _attempt_log_count,
    build_sync_plan,
)
from policydb.settings import Settings
from policydb.source_completion_ai_workflow import build_ai_plan
from policydb.source_slots import _parse_entry_probe, probe_candidates


def _settings(tmp_path: Path) -> Settings:
    return Settings(root=tmp_path, curated_path=tmp_path / "curated", database_path=tmp_path / "db.duckdb")


def _slot(**updates: object) -> dict[str, object]:
    row: dict[str, object] = {
        "slot_id": "SLOT1",
        "city_id": "CITY1",
        "city_name": "Test City",
        "province_name": "Test Province",
        "source_role": "housing_department",
        "work_status": "candidate_failed_fixable",
        "candidate_count": 1,
        "verified_candidate_count": 1,
        "enabled_source_count": 0,
        "best_candidate_id": "CAND1",
        "best_candidate_url": "https://housing.test.gov.cn/list/",
        "health_probe_success_count": 0,
        "role_confidence": None,
        "city_confidence": None,
        "slots_verified": False,
        "is_verified": False,
        "crawl_enabled": False,
        "enabled": False,
    }
    row.update(updates)
    return row


def _source() -> dict[str, object]:
    return {
        "source_id": "SRC1",
        "source_role": "housing_department",
        "agency_type": "housing_department",
        "domain": "housing.test.gov.cn",
        "official_status": "official",
        "official_domain_verified": True,
        "health_status": "healthy",
        "crawl_enabled": False,
        "homepage_url": "https://housing.test.gov.cn/",
        "list_page_urls": ["https://housing.test.gov.cn/list/"],
        "city_ids": ["CITY1"],
    }


def test_http_budget_counts_real_attempt_once_even_when_completion_is_appended(tmp_path: Path) -> None:
    ledger = BudgetLedger(tmp_path / "budget.json", limits={"http_calls": 3})
    attempts = tmp_path / "outbound_http_attempts.jsonl"
    recorder = HttpAttemptRecorder(ledger, attempts, run_id="RUN1", source_id="SRC1", slot_id="SLOT1", stage="probe")

    attempt_id = recorder({"phase": "before", "url": "https://test.gov.cn/list/", "attempt": 1})
    recorder({"phase": "after", "attempt_id": attempt_id, "url": "https://test.gov.cn/list/", "status_code": 200})
    recorder({"phase": "after", "attempt_id": attempt_id, "url": "https://test.gov.cn/list/", "status_code": 200})

    assert ledger.used["http_calls"] == 1
    assert _attempt_log_count(attempts) == 1
    assert len(attempts.read_text(encoding="utf-8").splitlines()) == 2


def test_normal_discovery_excludes_verified_candidate_but_scoped_audit_is_explicit(monkeypatch, tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    monkeypatch.setattr("policydb.full_sync.build_slot_work_queue", lambda _settings: pl.DataFrame([_slot()]))
    monkeypatch.setattr("policydb.full_sync.load_registry", lambda _settings: [_source()])

    normal = build_sync_plan(
        settings,
        FullSyncConfig(
            scope="city",
            city_id="CITY1",
            discover_missing=True,
            discovery_mode="SEARCH_AND_AI",
            max_slots=1,
            max_sources=1,
        ),
    )
    audit = build_sync_plan(
        settings,
        FullSyncConfig(
            scope="city",
            city_id="CITY1",
            discover_missing=True,
            discovery_mode="SEARCH_AND_AI",
            all_five_source_roles=True,
            max_slots=1,
            max_sources=1,
        ),
    )

    assert normal["source_discovery_queue"] == []
    assert audit["source_audit_queue"][0]["audit_only"] is True
    assert audit["source_verification_queue"][0]["slot_id"] == "SLOT1"


def test_scope_file_keeps_strict_replacement_sources_for_the_same_city_and_role(monkeypatch, tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    scope_file = tmp_path / "scope.json"
    scope_file.write_text(
        json.dumps(
            {
                "city_id": "CITY1",
                "city_ids": ["CITY1"],
                "source_ids": ["SRC_OLD"],
                "source_roles": ["housing_department"],
            }
        ),
        encoding="utf-8",
    )
    old_source = {**_source(), "source_id": "SRC_OLD", "source_role": "canonical_candidate", "crawl_enabled": False}
    replacement = {**_source(), "source_id": "SRC_NEW", "source_role": "housing_department", "crawl_enabled": True}
    monkeypatch.setattr("policydb.full_sync.build_slot_work_queue", lambda _settings: pl.DataFrame([_slot()]))
    monkeypatch.setattr("policydb.full_sync.load_registry", lambda _settings: [old_source, replacement])

    plan = build_sync_plan(
        settings,
        FullSyncConfig(
            scope="city",
            city_id="CITY1",
            scope_file=scope_file,
            all_five_source_roles=True,
            max_slots=1,
            max_sources=2,
        ),
    )

    assert {row["source_id"] for row in plan["sources"]} == {"SRC_OLD", "SRC_NEW"}


def test_ai_planner_excludes_verified_candidates_unless_audit_mode(monkeypatch, tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    frame = pl.DataFrame([_slot()])
    monkeypatch.setattr("policydb.source_completion_ai_workflow.build_slot_work_queue", lambda _settings: frame)

    assert build_ai_plan(settings, city_id="CITY1", max_slots=1).height == 0
    audit = build_ai_plan(settings, city_id="CITY1", max_slots=1, audit_existing=True)
    assert audit.height == 1
    assert audit[0, "slot_id"] == "SLOT1"


def test_budget_file_does_not_reserve_planned_http_calls(tmp_path: Path) -> None:
    ledger = BudgetLedger(tmp_path / "budget.json", limits={"http_calls": 2})
    ledger.persist()
    payload = json.loads((tmp_path / "budget.json").read_text(encoding="utf-8"))
    assert payload["used"]["http_calls"] == 0


def test_candidate_probe_accepts_a_bounded_candidate_id_list(monkeypatch, tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    seen: dict[str, object] = {}

    def fake_probe(**kwargs: object) -> dict[str, int]:
        seen.update(kwargs)
        return {"checked": 0, "healthy": 0, "parser_verified": 0, "failed": 0}

    monkeypatch.setattr("policydb.source_slots._probe_candidates_v2", fake_probe)
    result = probe_candidates(
        candidate_ids=["CAND1", "CAND2", "CAND3"],
        rounds=2,
        settings=settings,
    )

    assert result["checked"] == 0
    assert seen["candidate_ids"] == ["CAND1", "CAND2", "CAND3"]


def test_probe_verifies_only_candidates_actually_probed(monkeypatch, tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    frame = pl.DataFrame(
        [
            {
                "candidate_id": "CAND1",
                "slot_id": "SLOT1",
                "city_id": "CITY1",
                "source_role": "housing_department",
                "candidate_url": "https://housing.test.gov.cn/list/",
                "canonical_url": "https://housing.test.gov.cn/list/",
                "site_name": None,
                "department_name": None,
            },
            {
                "candidate_id": "CAND2",
                "slot_id": "SLOT1",
                "city_id": "CITY1",
                "source_role": "housing_department",
                "candidate_url": "https://housing.test.gov.cn/other/",
                "canonical_url": "https://housing.test.gov.cn/other/",
                "site_name": None,
                "department_name": None,
            },
        ]
    )
    seen: dict[str, object] = {}

    monkeypatch.setattr(source_slots, "list_candidates", lambda **_kwargs: frame)
    monkeypatch.setattr(source_slots, "upsert_candidates", lambda *_args, **_kwargs: {"upserted": 1})
    monkeypatch.setattr(
        source_slots,
        "_parse_entry_probe",
        lambda _result: {
            "status_ok": True,
            "next_urls": ["https://housing.test.gov.cn/list/page2"],
            "page_title": "Housing policies",
            "breadcrumb_detected": True,
            "navigation_count": 1,
            "detail_link_count": 2,
            "js_shell": False,
            "publication_date_available": True,
            "page_text_excerpt": "Housing policy list",
        },
    )
    monkeypatch.setattr(
        source_slots,
        "verify_candidates",
        lambda **kwargs: seen.update({"candidate_ids": kwargs.get("candidate_ids")}) or {"checked": 1, "verified": 0},
    )

    result = source_slots._probe_candidates_v2(
        city=None,
        source_id=None,
        candidate_id=None,
        candidate_ids=["CAND1"],
        slot_id="SLOT1",
        limit=None,
        rounds=2,
        settings=settings,
        fetcher=SimpleNamespace(
            fetch=lambda _url: SimpleNamespace(
                status_code=200,
                final_url="https://housing.test.gov.cn/list/",
                redirect_chain=[],
                network_route="direct_ok",
                response_sha256="fixture-sha256",
            )
        ),
    )

    assert result["checked"] == 1
    assert seen["candidate_ids"] == ["CAND1"]


def test_entry_probe_detects_date_rendered_outside_the_anchor() -> None:
    result = SimpleNamespace(
        body=b"<html><title>Policy list</title><a href='/detail'>Policy</a><span>2026-07-31</span></html>",
        content_type="text/html",
        status_code=200,
        final_url="https://city.gov.cn/policies/",
        network_route="direct_ok",
    )

    parsed = _parse_entry_probe(result)

    assert parsed["status_ok"] is True
    assert parsed["publication_date_available"] is True


def test_gazette_history_resolves_official_issue_date_evidence() -> None:
    index = SimpleNamespace(
        body=b"<html><title>Government Gazette</title><script>showUrl(2026,1);showUrl(2026,2)</script></html>",
        content_type="text/html",
        status_code=200,
        final_url="https://www.beijing.gov.cn/so/zcdh/zfgbHistory",
        network_route="direct_ok",
        response_sha256="index-sha",
    )
    issue_urls = {
        "https://www.beijing.gov.cn/zhengce/zfgb/lsgb/202601/t20260120_1.html": b"<html><title>Gazette issue</title><span>2026-01-20</span></html>",
        "https://www.beijing.gov.cn/zhengce/zfgb/lsgb/202602/t20260220_2.html": b"<html><title>Gazette issue</title><span>2026-02-20</span></html>",
    }

    class FakeFetcher:
        def fetch(self, url: str, *, referer: str | None = None):
            if "findUrl?" in url:
                issue = "1" if "gbqs=1" in url else "2"
                payload_url = next(
                    item for item in issue_urls if item.endswith(f"_{issue}.html")
                )
                return SimpleNamespace(
                    body=json.dumps({"url": payload_url}).encode("utf-8"),
                    content_type="application/json",
                    status_code=200,
                    final_url=url,
                    network_route="direct_ok",
                    response_sha256=f"endpoint-{issue}",
                )
            return SimpleNamespace(
                body=issue_urls[url],
                content_type="text/html",
                status_code=200,
                final_url=url,
                network_route="direct_ok",
                response_sha256=f"issue-{url[-6:]}",
            )

    parsed = _parse_entry_probe(index)
    evidence = source_slots._probe_gazette_issue_index(FakeFetcher(), index, parsed)

    assert evidence["strategy"] == "gazette_issue_index"
    assert len(evidence["detail_links"]) == 2
    assert evidence["publication_date_available"] is True
    assert len(evidence["evidence"]) == 2
