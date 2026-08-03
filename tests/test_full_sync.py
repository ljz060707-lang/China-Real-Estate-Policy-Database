from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import polars as pl
import pytest

from policydb.coverage import record_source_window
from policydb.full_sync import (
    SLOT_STATES,
    SOURCE_STATES,
    BudgetExceeded,
    BudgetLedger,
    FullSyncConfig,
    FullSyncController,
    InvalidTransition,
    JobLeaseStore,
    LeaseConflict,
    SyncStateStore,
    _candidate_role_identity_score,
    _has_backfill_completion_evidence,
    _looks_like_detail_page,
    _retry_wait_active,
    build_sync_plan,
    build_watermark,
    canonical_document_key,
    classify_document_change,
    classify_slot_state,
    classify_source_state,
    database_sync_status,
    derive_global_status,
    detect_coverage_gaps,
    document_version_key,
    load_test_evidence,
    source_freshness_status,
    source_is_crawl_ready,
    transition_allowed,
    transition_state,
    upsert_coverage_gaps,
    upsert_document_versions,
    watermark_equal,
)
from policydb.parquet_store import atomic_write_parquet
from policydb.settings import Settings


def _settings(tmp_path: Path) -> Settings:
    return Settings(root=tmp_path, curated_path=tmp_path / "curated", database_path=tmp_path / "db.duckdb")


def _source(**updates: object) -> dict[str, object]:
    result: dict[str, object] = {
        "source_id": "SRC1",
        "source_name": "Test source",
        "domain": "test.gov.cn",
        "source_role": "housing_department",
        "agency_type": "housing_department",
        "official_status": "official",
        "official_domain_verified": True,
        "crawl_enabled": True,
        "health_status": "healthy",
        "homepage_url": "https://test.gov.cn/",
        "list_page_urls": ["https://test.gov.cn/list/"],
        "city_ids": ["CITY1"],
        "expected_frequency": "monthly",
    }
    result.update(updates)
    return result


def _slot(**updates: object) -> dict[str, object]:
    result: dict[str, object] = {
        "slot_id": "SLOT1",
        "city_id": "CITY1",
        "source_role": "housing_department",
        "work_status": "no_candidate",
        "candidate_count": 0,
        "verified_candidate_count": 0,
        "enabled_source_count": 0,
    }
    result.update(updates)
    return result


def _document(**updates: object) -> dict[str, object]:
    result: dict[str, object] = {
        "city_id": "CITY1",
        "issuing_agency": "Housing bureau",
        "document_number": "2024-1",
        "title": "Policy title",
        "published_at": "2024-01-02T00:00:00+00:00",
        "canonical_url": "https://test.gov.cn/a",
        "content_hash": "hash-a",
        "extracted_text": "body",
        "parse_status": "ok",
    }
    result.update(updates)
    return result


def test_state_sets_include_required_progress_states() -> None:
    assert {"UNRESOLVED", "CRAWL_READY", "CURRENT", "BLOCKED"} <= set(SLOT_STATES)
    assert {"VERIFIED", "BACKFILL_COMPLETE", "STALE", "PARSER_BROKEN"} <= set(SOURCE_STATES)


def test_monotonic_slot_transition_is_allowed() -> None:
    assert transition_allowed("VERIFIED", "ENABLED")
    assert transition_state("ENABLED", "CRAWL_READY", reason_code="gate_passed") == "CRAWL_READY"


def test_backward_slot_transition_requires_explicit_branch() -> None:
    assert not transition_allowed("CURRENT", "UNRESOLVED")
    with pytest.raises(InvalidTransition):
        transition_state("CURRENT", "UNRESOLVED", reason_code="bad")


def test_retry_branch_is_explicitly_allowed() -> None:
    assert transition_allowed("CURRENT", "RETRY_WAIT")
    assert transition_state("RETRY_WAIT", "CURRENT", reason_code="retry_succeeded") == "CURRENT"


def test_slot_classification_is_deterministic() -> None:
    assert classify_slot_state(_slot()) == "UNRESOLVED"
    assert classify_slot_state(_slot(candidate_count=2)) == "CANDIDATES_FOUND"
    assert classify_slot_state(_slot(verified_candidate_count=1)) == "VERIFIED"
    assert classify_slot_state(_slot(enabled_source_count=1), "CRAWL_READY") == "CRAWL_READY"


def test_source_gate_requires_all_deterministic_evidence() -> None:
    assert source_is_crawl_ready(_source())
    assert not source_is_crawl_ready(_source(official_domain_verified=False))
    assert not source_is_crawl_ready(_source(health_status="degraded"))
    assert not source_is_crawl_ready(_source(list_page_urls=["https://test.gov.cn/detail/1"]))


def test_candidate_role_identity_prefers_the_institution_hostname() -> None:
    assert _candidate_role_identity_score(
        {"source_role": "housing_department", "candidate_url": "https://zjw.beijing.gov.cn/"}
    ) == 0
    assert _candidate_role_identity_score(
        {"source_role": "housing_department", "candidate_url": "https://gjj.beijing.gov.cn/"}
    ) == 2
    assert _candidate_role_identity_score(
        {"source_role": "provident_fund_center", "candidate_url": "https://gjj.beijing.gov.cn/"}
    ) == 0
    assert _candidate_role_identity_score(
        {"source_role": "natural_resources_department", "candidate_url": "https://ghzrzyw.beijing.gov.cn/"}
    ) == 0
    assert _candidate_role_identity_score(
        {
            "source_role": "housing_department",
            "list_page_urls": ["https://zjw.beijing.gov.cn/"],
            "source_name": "北京市住房和城乡建设委员会",
        }
    ) == 0


def test_deep_numeric_index_pages_are_detail_pages() -> None:
    assert _looks_like_detail_page(
        "https://zjw.beijing.gov.cn/bjjs/xxgk/zcwj2024/qtzcwj/xxyx13/743574454/index.shtml"
    )
    assert not _looks_like_detail_page("https://zjw.beijing.gov.cn/bjjs/xxgk/fgwj3/")


def test_source_state_disabled_and_ready() -> None:
    assert classify_source_state(_source(crawl_enabled=False)) == "DISABLED"
    assert classify_source_state(_source()) == "CRAWL_READY"
    assert classify_source_state(_source(backfill_status="complete"), {"backfill_status": "complete"}) == "INCREMENTAL_HEALTHY"
    assert classify_source_state(
        _source(backfill_status="complete_with_gaps"),
        {"backfill_status": "complete_with_gaps"},
    ) == "INCREMENTAL_HEALTHY"


def test_complete_with_gaps_is_a_backfilled_slot_state() -> None:
    assert classify_slot_state(
        _slot(enabled_source_count=1, backfill_status="complete_with_gaps"),
        "INCREMENTAL_HEALTHY",
    ) == "BACKFILLED"


def test_source_freshness_current_and_stale() -> None:
    now = datetime(2026, 8, 2, tzinfo=UTC)
    assert source_freshness_status(now.isoformat(), "housing_department", now=now) == "current"
    assert source_freshness_status((now - timedelta(days=2)).isoformat(), "housing_department", now=now) == "stale"


def test_retry_wait_blocks_resume_until_next_retry_at() -> None:
    now = datetime(2026, 8, 2, 9, 0, tzinfo=UTC)
    assert _retry_wait_active({"next_retry_at": "2026-08-02T09:30:00+00:00"}, now=now)
    assert not _retry_wait_active({"next_retry_at": "2026-08-02T08:30:00+00:00"}, now=now)
    assert not _retry_wait_active({"next_retry_at": None}, now=now)


def test_sync_state_retry_fields_are_carried_into_source_execution_rows(monkeypatch, tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    queue = pl.DataFrame([_slot(slot_id="SLOT1", candidate_count=1)], infer_schema_length=None)
    monkeypatch.setattr("policydb.full_sync.build_slot_work_queue", lambda _settings: queue)
    monkeypatch.setattr("policydb.full_sync.load_registry", lambda _settings: [_source()])
    atomic_write_parquet(
        pl.DataFrame(
            [
                {
                    "source_id": "SRC1",
                    "source_state": "CRAWL_READY",
                    "backfill_status": "partial",
                    "next_retry_at": "2026-08-02T09:30:00+00:00",
                }
            ]
        ),
        settings.curated / "source_sync_state.parquet",
    )
    plan = build_sync_plan(settings, FullSyncConfig(max_slots=1, max_sources=1))
    assert plan["sources"][0]["next_retry_at"] == "2026-08-02T09:30:00+00:00"


def test_slot_summary_uses_same_preferred_source_as_execution(monkeypatch, tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    queue = pl.DataFrame(
        [_slot(enabled_source_count=1, verified_candidate_count=1)],
        infer_schema_length=None,
    )
    old_source = _source(
        source_id="SRC_OLD",
        source_status="RETRY_WAIT",
        backfill_status="not_started",
    )
    completed_source = _source(
        source_id="SRC_COMPLETED",
        source_status="COMPLETED",
        backfill_status="complete",
    )
    monkeypatch.setattr("policydb.full_sync.build_slot_work_queue", lambda _settings: queue)
    monkeypatch.setattr(
        "policydb.full_sync.load_registry",
        lambda _settings: [old_source, completed_source],
    )

    plan = build_sync_plan(settings, FullSyncConfig(max_slots=1, max_sources=1))

    assert plan["slot_rows"][0]["source_id"] == "SRC_COMPLETED"
    assert plan["slot_rows"][0]["slot_state"] == "BACKFILLED"


def test_watermark_tracks_more_than_published_date() -> None:
    watermark = build_watermark({}, documents=[_document()], list_url="https://test.gov.cn/list/?page=2", list_content_hash="list-h", etag="E1", source_response_hash="R1")
    assert watermark["max_published_at"].startswith("2024-01-02")
    assert watermark["last_list_content_hash"] == "list-h"
    assert watermark["etag"] == "E1"
    assert watermark["last_article_url_hash"]


def test_watermark_equality_ignores_updated_at() -> None:
    left = build_watermark({}, documents=[_document()])
    right = dict(left, updated_at="later")
    assert watermark_equal(left, right)


def test_canonical_document_key_converges_url_variants_with_number() -> None:
    left = canonical_document_key(_document(canonical_url="https://test.gov.cn/a"))
    right = canonical_document_key(_document(canonical_url="https://mirror.gov.cn/reprint", title="different title"))
    assert left == right


def test_document_version_key_changes_when_content_changes() -> None:
    assert document_version_key(_document(content_hash="a")) != document_version_key(_document(content_hash="b"))


def test_document_change_classification() -> None:
    previous = _document()
    assert classify_document_change(None, previous) == "INSERTED"
    assert classify_document_change(previous, dict(previous)) == "UNCHANGED"
    assert classify_document_change(previous, dict(previous, content_hash="new")) == "REVISED"
    assert classify_document_change(previous, dict(previous, canonical_url="https://other.gov.cn/a")) == "REPRINT"


def test_year_gap_is_detected() -> None:
    gaps = detect_coverage_gaps([_document(published_at="2024-01-01T00:00:00+00:00")], source=_source(source_id="SRC1"), expected_start=date(2023, 1, 1), expected_end=date(2024, 12, 31))
    assert any(row["gap_type"] == "year_missing" for row in gaps)


def test_month_gap_is_detected_for_monthly_source() -> None:
    gaps = detect_coverage_gaps([_document(published_at="2024-01-01T00:00:00+00:00")], source=_source(), expected_start=date(2024, 1, 1), expected_end=date(2024, 3, 31))
    assert {row["gap_type"] for row in gaps} >= {"month_missing"}


def test_page_discontinuity_is_detected() -> None:
    gaps = detect_coverage_gaps([], source=_source(), page_numbers=[1, 3])
    assert any(row["gap_type"] == "page_discontinuity" for row in gaps)


def test_document_quality_gaps_are_explicit() -> None:
    gaps = detect_coverage_gaps([_document(extracted_text="", published_at=None, document_number=None, issuing_agency=None, parse_status="failed")], source=_source())
    kinds = {row["gap_type"] for row in gaps}
    assert {"parse_failed", "article_body_empty", "publication_date_missing", "document_number_missing", "issuing_agency_missing"} <= kinds


def test_gap_ids_are_idempotent() -> None:
    rows = detect_coverage_gaps([_document(extracted_text="")], source=_source())
    assert len({row["gap_id"] for row in rows}) == len(rows)


def test_upsert_coverage_gaps_is_idempotent(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    rows = detect_coverage_gaps([_document(extracted_text="")], source=_source())
    assert upsert_coverage_gaps(settings, rows)["incoming"] == len(rows)
    assert upsert_coverage_gaps(settings, rows)["open"] == len(rows)
    assert pl.read_parquet(settings.curated / "coverage_gaps.parquet").height == len(rows)


def test_document_upsert_is_idempotent(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    first = upsert_document_versions(settings, [_document()])
    second = upsert_document_versions(settings, [_document()])
    assert first["inserted"] == 1
    assert second["unchanged"] == 1
    assert pl.read_parquet(settings.curated / "document_versions.parquet").height == 1


def test_document_revision_preserves_prior_version(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    upsert_document_versions(settings, [_document()])
    result = upsert_document_versions(settings, [_document(content_hash="new")])
    frame = pl.read_parquet(settings.curated / "document_versions.parquet")
    assert result["inserted"] == 1
    assert frame.height == 2
    assert "SUPERSEDED" in frame["version_status"].to_list()


def test_budget_reservation_persists(tmp_path: Path) -> None:
    ledger = BudgetLedger(tmp_path / "budget.json", limits={"ai_calls": 2})
    ledger.reserve("ai", 1)
    assert json.loads((tmp_path / "budget.json").read_text(encoding="utf-8"))["used"]["ai_calls"] == 1


def test_budget_cap_stops_before_second_call(tmp_path: Path) -> None:
    ledger = BudgetLedger(tmp_path / "budget.json", limits={"ai_calls": 1})
    ledger.reserve("ai", 1)
    with pytest.raises(BudgetExceeded):
        ledger.reserve("ai", 1)


def test_lease_conflict_and_release(tmp_path: Path) -> None:
    path = tmp_path / "leases.json"
    first = JobLeaseStore(path, run_id="R1")
    second = JobLeaseStore(path, run_id="R2")
    first.claim("source", "SRC1")
    with pytest.raises(LeaseConflict):
        second.claim("source", "SRC1")
    first.release("source", "SRC1")
    second.claim("source", "SRC1")


def test_stale_lease_recovery(tmp_path: Path) -> None:
    path = tmp_path / "leases.json"
    path.write_text(json.dumps({"source:SRC1": {"status": "CLAIMED", "expires_at": "2000-01-01T00:00:00+00:00"}}), encoding="utf-8")
    recovered = JobLeaseStore(path, run_id="R1").recover_stale()
    assert recovered[0]["status"] == "STALE_RECOVERED"


def test_transition_events_are_append_only_and_idempotent(tmp_path: Path) -> None:
    store = SyncStateStore(tmp_path / "run", run_id="R1")
    store.transition("batch_claimed", reason_code="test")
    store.transition("batch_claimed", reason_code="test")
    assert len((tmp_path / "run" / "state_transitions.jsonl").read_text(encoding="utf-8").splitlines()) == 1


def test_checkpoint_resume_set(tmp_path: Path) -> None:
    store = SyncStateStore(tmp_path / "run", run_id="R1")
    store.checkpoint("SOURCE_COMPLETED", resource_id="SRC1")
    store.checkpoint("SOURCE_COMPLETED", resource_id="SRC1")
    assert store.completed_resources("SOURCE_COMPLETED") == {"SRC1"}


def test_global_status_does_not_wait_for_all_slots() -> None:
    status = derive_global_status(
        [{"slot_state": "UNRESOLVED"}, {"slot_state": "CURRENT"}],
        [{"source_state": "INCREMENTAL_HEALTHY"}],
    )
    assert status == "SOURCE_COMPLETION"


def test_global_status_current_with_gaps() -> None:
    assert derive_global_status([{"slot_state": "CURRENT"}], [{"source_state": "INCREMENTAL_HEALTHY"}], open_gaps=1) == "CURRENT_WITH_GAPS"


def test_database_sync_status_has_explicit_dimensions(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    status = database_sync_status(settings, [{"city_id": "C1", "slot_state": "CURRENT"}], [{"source_id": "S1", "source_state": "INCREMENTAL_HEALTHY", "freshness_status": "current"}])
    assert status["coverage_ratio"] == 1.0
    assert "backfill_ratio" in status and "data_quality_ratio" in status


def test_config_requires_confirmation_for_full_apply() -> None:
    with pytest.raises(ValueError):
        FullSyncConfig(apply=True, all_remaining=True).validate()
    FullSyncConfig(apply=True, all_remaining=True, confirm_full_sync=True).validate()


def test_test_evidence_is_tri_state(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    assert load_test_evidence(settings)["test_result"] == "unknown"
    path = settings.outputs / "evidence.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"overall_status": "passed", "commit_sha": "abc"}), encoding="utf-8-sig")
    assert load_test_evidence(settings, path)["test_result"] == "passed"


def test_plan_is_read_only_with_zero_paid_calls(monkeypatch, tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    queue = pl.DataFrame([_slot()], infer_schema_length=None)
    monkeypatch.setattr("policydb.full_sync.build_slot_work_queue", lambda _settings: queue)
    monkeypatch.setattr("policydb.full_sync.load_registry", lambda _settings: [])
    result = FullSyncController(settings, config=FullSyncConfig(max_slots=1, max_sources=1), output=tmp_path / "run", run_id="R1").plan()
    assert result["paid_api_calls_started"] == 0
    assert result["estimates"]["ai_calls"] == 0


def test_build_sync_plan_keeps_ready_source_independent_of_unresolved_slot(monkeypatch, tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    queue = pl.DataFrame([_slot(), _slot(slot_id="SLOT2", work_status="verified_enabled", verified_candidate_count=1, enabled_source_count=1)], infer_schema_length=None)
    monkeypatch.setattr("policydb.full_sync.build_slot_work_queue", lambda _settings: queue)
    monkeypatch.setattr("policydb.full_sync.load_registry", lambda _settings: [_source(source_id="SRC1")])
    plan = build_sync_plan(settings, FullSyncConfig(max_slots=2, max_sources=1))
    assert plan["estimates"]["sources"] == 1
    assert any(row["slot_state"] == "UNRESOLVED" for row in plan["slot_rows"])


def test_controller_status_reads_current_status(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    controller = FullSyncController(settings, output=tmp_path / "run", run_id="R1")
    controller.store.write_status({"global_status": "CURRENT_WITH_GAPS"})
    assert controller.status()["global_status"] == "CURRENT_WITH_GAPS"


def test_plan_status_has_live_sync_counters(monkeypatch, tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    monkeypatch.setattr("policydb.full_sync.build_slot_work_queue", lambda _settings: pl.DataFrame([_slot()], infer_schema_length=None))
    monkeypatch.setattr("policydb.full_sync.load_registry", lambda _settings: [])
    controller = FullSyncController(settings, config=FullSyncConfig(max_slots=1, max_sources=1), output=tmp_path / "run", run_id="R1")
    controller.plan()
    status = controller.status()
    assert {"current_batch", "current_slot", "current_step", "ai_calls", "ai_attempts", "candidates", "probes", "human_review", "retries", "verified", "enabled", "unresolved", "latest_error", "last_progress_at", "last_heartbeat_at"} <= set(status)


def test_backfill_requires_strict_completion_evidence(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    incomplete = {
        "strict_completion": True,
        "pagination_complete": True,
        "termination_reason": "next_page_absent",
        "termination_evidence_ids": ["SCAN1"],
        "transaction_committed": True,
        "checkpoint_persisted": True,
        "completion_invariants_passed": False,
        "exhaustive": True,
    }
    record_source_window(
        run_id="RUN1",
        source_id="SRC1",
        period_start=date(2025, 1, 1),
        period_end=date(2025, 12, 31),
        scan_method="historical_105",
        candidate_count=1,
        fetched_count=1,
        policy_count=1,
        error_count=0,
        page_count=1,
        completion_evidence=incomplete,
        settings=settings,
    )
    assert not _has_backfill_completion_evidence(settings, "SRC1", run_id="RUN1")
    complete = dict(incomplete, completion_invariants_passed=True)
    record_source_window(
        run_id="RUN1",
        source_id="SRC1",
        period_start=date(2025, 1, 1),
        period_end=date(2025, 12, 31),
        scan_method="historical_105",
        candidate_count=1,
        fetched_count=1,
        policy_count=1,
        error_count=0,
        page_count=1,
        completion_evidence=complete,
        settings=settings,
    )
    assert _has_backfill_completion_evidence(settings, "SRC1", run_id="RUN1")


def test_backfill_resume_reuses_strict_window_when_plan_has_no_items(monkeypatch, tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    evidence = {
        "strict_completion": True,
        "pagination_complete": True,
        "termination_reason": "END_OF_PAGINATION",
        "termination_evidence_ids": ["SCAN_OLD"],
        "transaction_committed": True,
        "checkpoint_persisted": True,
        "completion_invariants_passed": True,
        "exhaustive": True,
    }
    record_source_window(
        run_id="RUN_OLD",
        source_id="SRC1",
        period_start=date(2018, 1, 1),
        period_end=date(2026, 8, 2),
        scan_method="historical_105",
        candidate_count=4,
        fetched_count=4,
        policy_count=4,
        error_count=0,
        page_count=1,
        completion_evidence=evidence,
        settings=settings,
    )

    class _NoopResumePipeline:
        def __init__(self, _settings, *, fetcher=None):
            self.settings = _settings
            self.fetcher = fetcher

        def plan(self, **_kwargs):
            return {
                "run_id": "RUN_NEW",
                "source_count": 1,
                "item_count": 0,
                "status": "planned",
                "diagnostic": None,
                "discovery_errors": [],
            }

        def run(self, run_id, **_kwargs):
            return {
                "run_id": run_id,
                "status": "complete",
                "fetched": 0,
                "failed": 0,
                "persisted_fetched": 0,
                "persisted_failed": 0,
                "budget_paused": False,
            }

    monkeypatch.setattr("policydb.full_sync.CrawlPipeline", _NoopResumePipeline)
    controller = FullSyncController(
        settings,
        config=FullSyncConfig(
            apply=True,
            resume=True,
            backfill_from=date(2018, 1, 1),
            backfill_to=date(2026, 8, 2),
        ),
        output=tmp_path / "run",
        run_id="RUN_CONTROLLER",
    )
    result = controller._run_source_v2(_source(source_id="SRC1"), mode="backfill", max_fetches=10)

    assert result["status"] == "completed"
    assert result["backfill_completion_evidence"] is True
    assert result["backfill_completion_reused"] is True
    transitions = [json.loads(line) for line in (tmp_path / "run" / "state_transitions.jsonl").read_text().splitlines()]
    assert any(item["event_type"] == "backfill_reused_completion" for item in transitions)


def test_incremental_sync_preserves_completed_backfill_state(monkeypatch, tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    completed_at = "2026-08-02T12:00:00+00:00"
    atomic_write_parquet(
        pl.DataFrame(
            [
                {
                    "source_id": "SRC1",
                    "source_status": "COMPLETED",
                    "backfill_status": "complete",
                    "backfill_completed_at": completed_at,
                    "historical_watermark": json.dumps({"max_published_at": "2026-08-01T00:00:00+00:00"}),
                    "incremental_watermark": json.dumps({"max_published_at": "2026-08-01T00:00:00+00:00"}),
                    "current_watermark": json.dumps({"max_published_at": "2026-08-01T00:00:00+00:00"}),
                }
            ],
            infer_schema_length=None,
        ),
        settings.curated / "source_sync_state.parquet",
    )

    class _NoopIncrementalPipeline:
        def __init__(self, _settings, *, fetcher=None):
            self.settings = _settings
            self.fetcher = fetcher

        def plan(self, **_kwargs):
            return {
                "run_id": "RUN_INCREMENTAL",
                "source_count": 1,
                "item_count": 0,
                "status": "planned",
                "diagnostic": None,
                "discovery_errors": [],
            }

        def run(self, run_id, **_kwargs):
            return {
                "run_id": run_id,
                "status": "complete",
                "fetched": 0,
                "failed": 0,
                "persisted_fetched": 0,
                "persisted_failed": 0,
                "budget_paused": False,
            }

    monkeypatch.setattr("policydb.full_sync.CrawlPipeline", _NoopIncrementalPipeline)
    controller = FullSyncController(
        settings,
        config=FullSyncConfig(apply=True, resume=True, lookback_days=7),
        output=tmp_path / "run",
        run_id="RUN_CONTROLLER_INCREMENTAL",
    )
    result = controller._run_source_v2(_source(source_id="SRC1"), mode="incremental", max_fetches=10)

    assert result["status"] == "completed"
    row = pl.read_parquet(settings.curated / "source_sync_state.parquet").filter(pl.col("source_id") == "SRC1").to_dicts()[0]
    assert row["backfill_status"] == "complete"
    assert row["backfill_completed_at"] == completed_at
    assert json.loads(row["historical_watermark"])["max_published_at"] == "2026-08-01T00:00:00+00:00"


def test_full_sync_report_writes_consistent_machine_and_human_outputs(monkeypatch, tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    queue = pl.DataFrame([_slot()], infer_schema_length=None)
    monkeypatch.setattr("policydb.full_sync.build_slot_work_queue", lambda _settings: queue)
    monkeypatch.setattr("policydb.full_sync.load_registry", lambda _settings: [])
    controller = FullSyncController(
        settings,
        config=FullSyncConfig(scope="all", max_slots=1, max_sources=1, report_formats="json,xlsx,parquet"),
        output=tmp_path / "report",
        run_id="REPORT1",
    )
    report = controller.report()
    assert Path(report["output_dir"]).joinpath("full_sync_report.json").exists()
    assert Path(report["output_dir"]).joinpath("full_sync_report.parquet").exists()
    assert Path(report["output_dir"]).joinpath("full_sync_report.xlsx").exists()
    assert report["report"]["completeness"]["overall_is_display_only"] is True
