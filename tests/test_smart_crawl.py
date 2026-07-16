from __future__ import annotations

import json
from datetime import date

import httpx
import polars as pl
import pytest
import yaml
from pydantic import ValidationError

from policydb.config.preferences import PreferencesStore
from policydb.config.providers import BingSearchProvider, NoneSearchProvider
from policydb.config.secret_store import KeyringSecretStore, redact_secrets
from policydb.crawl.dedup import content_sha256
from policydb.crawl.discovery import ListPageDiscovery
from policydb.crawl.fetcher import Http429, RespectfulFetcher, RobotsBlocked
from policydb.crawl.models import DiscoveryRequest, RegisteredSource
from policydb.crawl.pipeline import CrawlPipeline
from policydb.crawl.registry import load_registry, save_registry_atomic, set_sources_enabled
from policydb.enrich.glm import GLMEnricher
from policydb.jobs import CrawlJobRequest, JobManager
from policydb.jobs.manager import PolicyWriteLock
from policydb.jobs.reporting import generate_crawl_report
from policydb.jobs.worker import run_job
from policydb.recovery import (
    RecoveryRecord,
    SourceCandidate,
    SourceRecoveryEngine,
    score_source_candidate,
)
from policydb.settings import Settings


def _repo(tmp_path):
    root = tmp_path / "repo"
    (root / "data" / "reference").mkdir(parents=True)
    (root / "data" / "curated").mkdir(parents=True)
    (root / "data" / "logs").mkdir(parents=True)
    (root / "data" / "reference" / "source_registry.yaml").write_text(
        yaml.safe_dump({"sources": []}), encoding="utf-8"
    )
    return root


def _source(**updates):
    values = {
        "source_id": "SRC",
        "source_name": "测试政府",
        "domain": "example.gov.cn",
        "source_type": "government",
        "source_role": "canonical_candidate",
        "official_status": "official",
        "seed_urls": [],
        "list_page_urls": ["https://example.gov.cn/list"],
        "crawl_enabled": True,
        "rate_limit": 0,
    }
    values.update(updates)
    return RegisteredSource.model_validate(values)


def _fetcher(handler, **kwargs):
    return RespectfulFetcher(
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        check_robots=False,
        rate_limit=0,
        **kwargs,
    )


def test_enabled_sources_zero_has_actionable_diagnosis(tmp_path):
    root = _repo(tmp_path)
    plan = CrawlPipeline(Settings(root=root), fetcher=_fetcher(lambda request: httpx.Response(200, request=request))).plan(
        run_type="official_update", start_date=date(2026, 1, 1), end_date=date(2026, 1, 2)
    )
    assert plan["status"] == "blocked_no_enabled_sources"
    assert "来源体检" in plan["diagnostic"]


def test_list_page_discovers_detail_link():
    html = '<a href="/policy/1">住房政策 2026-07-15</a>'
    candidates = ListPageDiscovery(_fetcher(lambda request: httpx.Response(200, text=html, request=request))).discover(
        DiscoveryRequest(run_id="R", mode="official_update"), _source()
    )
    assert candidates[0].url == "https://example.gov.cn/policy/1"


def test_list_page_resolves_relative_url():
    html = '<a href="../policy/2">城市更新通知</a>'
    source = _source(list_page_urls=["https://example.gov.cn/news/list/index.html"])
    result = ListPageDiscovery(_fetcher(lambda request: httpx.Response(200, text=html, request=request))).discover(
        DiscoveryRequest(run_id="R", mode="official_update"), source
    )
    assert result[0].url == "https://example.gov.cn/news/policy/2"


def test_list_page_filters_date_window():
    html = '<a href="/old">旧政策 2020-01-01</a><a href="/new">新政策 2026-07-15</a>'
    result = ListPageDiscovery(_fetcher(lambda request: httpx.Response(200, text=html, request=request))).discover(
        DiscoveryRequest(run_id="R", mode="official_update", start_date=date(2026, 7, 1), end_date=date(2026, 7, 16)), _source()
    )
    assert [item.title_hint for item in result] == ["新政策 2026-07-15"]


def test_pagination_cycle_is_bounded():
    calls = 0

    def handler(request):
        nonlocal calls
        calls += 1
        other = "/list2" if request.url.path == "/list" else "/list"
        return httpx.Response(200, text=f'<a href="{other}">下一页</a><a href="/p{calls}">政策</a>', request=request)

    result = ListPageDiscovery(_fetcher(handler)).discover(
        DiscoveryRequest(run_id="R", mode="official_update", max_pages=10), _source()
    )
    assert calls == 2 and len(result) == 2


def test_seed_backtrack_runs_with_disabled_source(tmp_path):
    root = _repo(tmp_path)
    source = _source(crawl_enabled=False, list_page_urls=[], seed_urls=["https://example.gov.cn/policy/1"])
    (root / "data" / "reference" / "source_registry.yaml").write_text(yaml.safe_dump({"sources": [source.model_dump(mode="json")]}, allow_unicode=True), encoding="utf-8")
    plan = CrawlPipeline(Settings(root=root), fetcher=_fetcher(lambda request: httpx.Response(200, text="住房政策", request=request))).plan(
        run_type="seed_backtrack", start_date=date(2020, 1, 1), end_date=date(2026, 1, 1), max_items=5
    )
    assert plan["source_count"] == 1 and plan["item_count"] == 1


def test_official_candidate_score_uses_explainable_weights():
    record = RecoveryRecord(record_id="R", title="住房政策", document_number="京建〔2026〕1号", issuing_agency="北京市住建委", record_date=date(2026, 7, 1), region="北京市", full_text="完整正文")
    candidate = SourceCandidate(url="https://beijing.gov.cn/p", title=record.title, document_number=record.document_number, issuing_agency=record.issuing_agency, publication_date=record.record_date, region=record.region, text=record.full_text, official_status="official")
    score = score_source_candidate(record, candidate)
    assert score.score == 1 and score.components["document_number"] == 1


def test_candidate_conflict_caps_score():
    record = RecoveryRecord(record_id="R", title="住房政策", document_number="A1")
    candidate = SourceCandidate(url="https://gov.cn/p", title="住房政策", document_number="B2", official_status="official")
    assert score_source_candidate(record, candidate).score <= 0.69


def test_media_candidate_cannot_be_canonical(tmp_path):
    engine = SourceRecoveryEngine(Settings(root=_repo(tmp_path)))
    result = engine.recover(
        RecoveryRecord(record_id="R", title="住房政策"),
        [SourceCandidate(url="https://media.example/p", title="住房政策", official_status="general_media")],
    )
    assert result["status"] == "low_confidence" and result["source_role"] == "discovery_lead"


def test_robots_blocked_is_distinct_error():
    def handler(request):
        text = "User-agent: *\nDisallow: /private" if request.url.path == "/robots.txt" else "secret"
        return httpx.Response(200, text=text, request=request)

    fetcher = RespectfulFetcher(client=httpx.Client(transport=httpx.MockTransport(handler)), rate_limit=0)
    with pytest.raises(RobotsBlocked):
        fetcher.fetch("https://example.gov.cn/private")


def test_http_429_uses_retry_after(monkeypatch):
    calls = 0
    sleeps = []

    def handler(request):
        nonlocal calls
        calls += 1
        return httpx.Response(429, headers={"retry-after": "2"}, request=request)

    monkeypatch.setattr("policydb.crawl.fetcher.time.sleep", sleeps.append)
    with pytest.raises(Http429):
        _fetcher(handler, retries=2).fetch("https://example.gov.cn/p")
    assert calls == 2 and 2.0 in sleeps


def test_etag_and_304_are_supported():
    def handler(request):
        assert request.headers["if-none-match"] == '"abc"'
        return httpx.Response(304, request=request)

    result = _fetcher(handler).fetch("https://example.gov.cn/p", etag='"abc"')
    assert result.not_modified and result.status_code == 304


def test_content_hash_deduplicates_equal_bytes():
    assert content_sha256(b"same") == content_sha256("same")


def test_job_state_is_persistent(tmp_path):
    manager = JobManager(Settings(root=_repo(tmp_path)))
    state = manager.create(CrawlJobRequest(mode="seed_backtrack"))
    assert JobManager(manager.settings).load_state(state.job_id).status == "queued"


def test_page_refresh_can_restore_job_state(tmp_path):
    manager = JobManager(Settings(root=_repo(tmp_path)))
    state = manager.create(CrawlJobRequest(mode="seed_backtrack"))
    manager.update(state.job_id, status="discovering", stage="discovering", message="工作中")
    assert JobManager(manager.settings).load_state(state.job_id).message == "工作中"


def test_job_cancel_is_cooperative(tmp_path):
    manager = JobManager(Settings(root=_repo(tmp_path)))
    state = manager.create(CrawlJobRequest(mode="seed_backtrack"))
    assert manager.cancel(state.job_id).cancel_requested


def test_pipeline_checks_cancel_between_fetch_items(tmp_path):
    root = _repo(tmp_path)
    source = _source(list_page_urls=[])
    save_registry_atomic([source], Settings(root=root), action="test_setup")
    pl.DataFrame(
        [
            {
                "item_id": "ITEM1",
                "run_id": "RUN1",
                "source_id": source.source_id,
                "url": "https://example.gov.cn/policy/1",
                "canonical_url": "https://example.gov.cn/policy/1",
                "status": "pending",
            }
        ]
    ).write_parquet(root / "data" / "curated" / "crawl_items.parquet")

    def unexpected_fetch(request):
        raise AssertionError(f"cancelled job fetched {request.url}")

    result = CrawlPipeline(
        Settings(root=root), fetcher=_fetcher(unexpected_fetch)
    ).run("RUN1", cancel_check=lambda: True)
    assert result["cancelled"] is True
    assert result["fetched"] == 0


def test_stale_worker_state_is_recovered(tmp_path, monkeypatch):
    root = _repo(tmp_path)
    manager = JobManager(Settings(root=root))
    state = manager.create(CrawlJobRequest(mode="smart"))
    manager.save_state(
        state.model_copy(
            update={"status": "fetching", "stage": "fetching", "pid": 99999999}
        )
    )
    monkeypatch.setattr(manager, "_pid_alive", lambda _pid: False)

    recovered = manager.list_states()[0]

    assert recovered.status == "failed"
    assert recovered.error_type == "StaleWorker"
    assert manager.load_state(state.job_id).status == "failed"


def test_write_lock_prevents_concurrent_writer(tmp_path):
    settings = Settings(root=_repo(tmp_path))
    with PolicyWriteLock(settings, "A"):
        with pytest.raises(RuntimeError):
            with PolicyWriteLock(settings, "B"):
                pass


def test_keyring_adapter_saves_and_reads(monkeypatch):
    values = {}

    class FakeKeyring:
        @staticmethod
        def get_password(service, name):
            return values.get((service, name))

        @staticmethod
        def set_password(service, name, value):
            values[(service, name)] = value

        @staticmethod
        def delete_password(service, name):
            values.pop((service, name), None)

    monkeypatch.setattr(KeyringSecretStore, "_keyring", staticmethod(lambda: FakeKeyring))
    store = KeyringSecretStore()
    store.set_secret("glm_api_key", "test-secret")
    assert store.get_secret("glm_api_key") == "test-secret"
    store.delete_secret("glm_api_key")
    assert not store.has_secret("glm_api_key")


def test_api_key_never_enters_preferences(tmp_path):
    store = PreferencesStore(tmp_path / "preferences.json")
    with pytest.raises(ValueError):
        store.save({"glm_api_key": "secret"})
    assert not store.path.exists()


def test_logs_redact_common_secret_shapes():
    safe = redact_secrets("Authorization: Bearer sk-secret GLM_API_KEY=abc")
    assert "sk-secret" not in safe and "=abc" not in safe


def test_read_only_mode_blocks_job_creation(tmp_path, monkeypatch):
    monkeypatch.setenv("POLICYDB_READ_ONLY", "1")
    with pytest.raises(PermissionError):
        JobManager(Settings(root=_repo(tmp_path))).create(CrawlJobRequest(mode="seed_backtrack"))


def test_glm_filters_to_current_run(tmp_path):
    root = _repo(tmp_path)
    pl.DataFrame({"item_id": ["I1", "I2"], "run_id": ["RUN1", "RUN2"]}).write_parquet(root / "data" / "curated" / "crawl_items.parquet")
    pl.DataFrame({"document_version_id": ["D1", "D2"], "crawl_item_id": ["I1", "I2"], "content_sha256": ["H1", "H2"], "extracted_text": ["住房政策", "住房政策"]}).write_parquet(root / "data" / "curated" / "policy_document_versions.parquet")
    assert GLMEnricher(Settings(root=root), api_key=None)._pending_versions("RUN1").height == 1


def test_report_files_and_metrics_are_consistent(tmp_path):
    settings = Settings(root=_repo(tmp_path))
    manager = JobManager(settings)
    state = manager.create(CrawlJobRequest(mode="seed_backtrack"))
    output = generate_crawl_report(settings, state, {"metrics": {"fetched": 3, "candidate_count": 5}, "recommendations": []})
    summary = json.loads((output / "summary.json").read_text(encoding="utf-8"))
    assert summary["fetched"] == 3 and (output / "errors.csv").exists()


def test_mock_background_job_generates_complete_report(tmp_path):
    settings = Settings(root=_repo(tmp_path))
    manager = JobManager(settings)
    state = manager.create(CrawlJobRequest(mode="seed_backtrack", max_fetches=5, demo_mode=True, rebuild_database=False, run_validation=False))
    result = run_job(state.job_id, settings)
    final = manager.load_state(state.job_id)
    assert result["metrics"]["candidate_count"] == 5
    assert final.status == "completed_with_warnings"
    assert (manager.job_dir(state.job_id) / "report.md").exists()


def test_registry_write_is_atomic_and_backed_up(tmp_path):
    settings = Settings(root=_repo(tmp_path))
    source = _source(crawl_enabled=False)
    save_registry_atomic([source], settings, action="initial")
    set_sources_enabled([source.source_id], True, settings)
    assert load_registry(settings)[0].crawl_enabled
    assert list((settings.root / "data" / "reference" / "backups").glob("*.yaml"))


def test_none_search_provider_is_explicitly_empty():
    assert NoneSearchProvider().search("anything") == []


def test_bing_search_provider_uses_mocked_api():
    def handler(request):
        assert request.headers["ocp-apim-subscription-key"] == "test"
        return httpx.Response(200, json={"webPages": {"value": [{"url": "https://gov.cn/p", "name": "政策"}]}}, request=request)

    provider = BingSearchProvider("test", client=httpx.Client(transport=httpx.MockTransport(handler)))
    assert provider.search("政策", max_results=1)[0].title == "政策"


def test_job_request_rejects_secret_fields():
    with pytest.raises(ValidationError):
        CrawlJobRequest.model_validate({"mode": "smart", "glm_api_key": "secret"})
