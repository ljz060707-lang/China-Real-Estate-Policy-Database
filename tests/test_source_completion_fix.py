from __future__ import annotations

import json
import shutil
from pathlib import Path

import polars as pl

from policydb.autopilot_checkpoints import GlobalSlotCheckpointStore, is_slot_claimable
from policydb.crawl.models import RegisteredSource
from policydb.crawl.registry import load_registry, materialize_registry_parquet
from policydb.settings import Settings
from policydb.source_candidate_triage import (
    prefilter_candidate_frame,
    rank_candidate_proposals,
)
from policydb.source_evidence_enrichment import (
    enrich_candidate_evidence,
    select_evidence_enrichment_candidates,
)
from policydb.source_jurisdiction import JurisdictionMapping, is_clear_detail_url
from policydb.source_slots import (
    build_requirement_slots,
    evaluate_candidate_gates,
    list_candidates,
    reclassify_candidate_after_probe,
    upsert_candidates,
)


def _settings(tmp_path: Path) -> Settings:
    root = tmp_path / "repo"
    reference = root / "data" / "reference"
    reference.mkdir(parents=True)
    source_reference = Path(__file__).resolve().parents[1] / "data" / "reference"
    for name in ("cities_105.csv", "city_source_requirements.yaml"):
        shutil.copy2(source_reference / name, reference / name)
    (reference / "source_registry.yaml").write_text(
        "version: 2\nsources: []\n", encoding="utf-8"
    )
    return Settings(root=root)


def _mapping(*, city_ids=("CITY_460100",), role="provident_fund_center", approval="approved") -> JurisdictionMapping:
    return JurisdictionMapping(
        mapping_id="JURIS_TEST_HAINAN_GJJ",
        authority_level="provincial_centralized",
        authority_name="Hainan Housing Provident Fund Management Bureau",
        source_role=role,
        authority_domain="gjj.hainan.gov.cn",
        homepage_url="http://gjj.hainan.gov.cn/",
        list_page_urls=("http://gjj.hainan.gov.cn/hngjj/zccc/newxxgk_index.shtml",),
        covered_city_ids=tuple(city_ids),
        approval_status=approval,
        source_bundle_id="BUNDLE_TEST_HAINAN_GJJ",
    )


def _probe_row(url: str = "http://gjj.hainan.gov.cn/hngjj/zccc/newxxgk_index.shtml") -> dict:
    probe = [
        {
            "round": 1,
            "status_code": 200,
            "network_route": "direct_ok",
            "parser_ok": True,
            "js_shell": False,
            "detail_link_count": 90,
            "response_sha256": "a" * 64,
            "page_title": "policy list",
        },
        {
            "round": 2,
            "status_code": 200,
            "network_route": "direct_ok",
            "parser_ok": True,
            "js_shell": False,
            "detail_link_count": 90,
            "response_sha256": "b" * 64,
            "page_title": "policy list",
        },
    ]
    return {
        "candidate_id": "CAND_HAIKOU",
        "slot_id": "SLOT_HAIKOU_GJJ",
        "city_id": "CITY_460100",
        "city_name": "Haikou",
        "source_role": "provident_fund_center",
        "candidate_url": url,
        "canonical_url": url,
        "final_url": url,
        "candidate_kind": "official_list_entry_candidate",
        "page_type": "verified_list_entry",
        "entry_eligible": True,
        "health_status": "healthy",
        "network_route": "direct_ok",
        "http_status": 200,
        "health_probe_count": 2,
        "health_probe_success_count": 2,
        "parser_status": "list_detected",
        "pagination_strategy": "natural_single_page",
        "publication_date_available": True,
        "article_link_extraction_ready": True,
        "probe_evidence_json": json.dumps(probe),
        "city_match_evidence": None,
        "role_match_evidence": None,
        "is_verified": False,
        "is_enabled": False,
    }


def test_approved_central_mapping_controls_city_gate() -> None:
    row = _probe_row()
    slot = {"city_id": "CITY_460100", "city_name": "Haikou", "source_role": "provident_fund_center"}
    result, gates = evaluate_candidate_gates(row, slot=slot, jurisdiction_mappings=[_mapping()])
    city_gate = next(item for item in gates if item["gate_name"] == "city_match")
    assert city_gate["gate_status"] == "PASS"
    assert city_gate["reason_code"] == "centralized_authority_city_coverage"
    assert "jurisdiction_mapping:JURIS_TEST_HAINAN_GJJ" in city_gate["evidence_ids"]
    assert result["jurisdiction_mapping_id"] == "JURIS_TEST_HAINAN_GJJ"

    absent, absent_gates = evaluate_candidate_gates(row, slot=slot, jurisdiction_mappings=[])
    assert next(item for item in absent_gates if item["gate_name"] == "city_match")["gate_status"] == "UNKNOWN"
    assert "city_match" in absent["failed_gates"]

    pending, pending_gates = evaluate_candidate_gates(
        row,
        slot=slot,
        jurisdiction_mappings=[_mapping(approval="pending_human")],
    )
    assert next(item for item in pending_gates if item["gate_name"] == "city_match")["gate_status"] == "UNKNOWN"

    wrong_role, _ = evaluate_candidate_gates(
        row,
        slot={**slot, "source_role": "housing_department"},
        jurisdiction_mappings=[_mapping()],
    )
    assert wrong_role["status"] == "REJECTED"
    assert "city_match" in wrong_role["failed_gates"]

    wrong_city, _ = evaluate_candidate_gates(
        row,
        slot={**slot, "city_id": "CITY_460200"},
        jurisdiction_mappings=[_mapping(city_ids=("CITY_460100",))],
    )
    assert wrong_city["status"] == "REJECTED"


def test_probe_evidence_reclassifies_policy_column_and_preserves_initial_label(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    build_requirement_slots(settings)
    row = _probe_row()
    row.update(
        {
            "candidate_id": "CAND_RECLASS",
            "city_name": None,
            "initial_page_type": "policy_content_page",
            "initial_candidate_kind": "policy_content_evidence",
            "initial_entry_eligible": False,
            "page_type": "policy_content_page",
            "candidate_kind": "policy_content_evidence",
            "entry_eligible": False,
        }
    )
    upsert_candidates([row], settings)
    result = reclassify_candidate_after_probe("CAND_RECLASS", settings=settings, run_id="RUN_RECLASS")
    assert result["reclassified"] is True
    current = list_candidates(candidate_id="CAND_RECLASS", settings=settings).row(0, named=True)
    assert current["initial_page_type"] == "policy_content_page"
    assert current["initial_candidate_kind"] == "policy_content_evidence"
    assert current["initial_entry_eligible"] is False
    assert current["page_type"] == "verified_list_entry"
    assert current["candidate_kind"] == "official_list_entry_candidate"
    assert current["entry_eligible"] is True
    assert current["reclassification_method"] == "deterministic_probe_reclassification"


def test_detail_page_is_rejected_before_top3_and_fixed_order_keeps_bundle_entries() -> None:
    settings = Settings(root=Path(__file__).resolve().parents[1])
    mapping = _mapping()
    frame = pl.from_dicts(
        [
            {
                "proposal_id": "HOME",
                "slot_id": "SLOT_HAIKOU_GJJ",
                "city_id": "CITY_460100",
                "source_role": "provident_fund_center",
                "candidate_url": "http://gjj.hainan.gov.cn/",
                "candidate_title": "Hainan Housing Provident Fund",
                "candidate_snippet": "official authority",
            },
            {
                "proposal_id": "LIST",
                "slot_id": "SLOT_HAIKOU_GJJ",
                "city_id": "CITY_460100",
                "source_role": "provident_fund_center",
                "candidate_url": "http://gjj.hainan.gov.cn/hngjj/zccc/newxxgk_index.shtml",
                "candidate_title": "Policy information",
                "candidate_snippet": "official policy list",
            },
            {
                "proposal_id": "LAW",
                "slot_id": "SLOT_HAIKOU_GJJ",
                "city_id": "CITY_460100",
                "source_role": "provident_fund_center",
                "candidate_url": "https://ccdi.gov.cn/fgk/law_display/3572",
                "candidate_title": "Housing Provident Fund Regulation",
                "candidate_snippet": "central law detail",
            },
            {
                "proposal_id": "OTHER_CITY",
                "slot_id": "SLOT_HAIKOU_GJJ",
                "city_id": "CITY_460100",
                "source_role": "provident_fund_center",
                "candidate_url": "https://sanya.gov.cn/",
                "candidate_title": "Sanya government",
                "candidate_snippet": "other city",
            },
        ]
    )
    prefiltered = prefilter_candidate_frame(settings, frame, mappings=[mapping])
    selected, evidence = rank_candidate_proposals(
        prefiltered,
        settings=settings,
        mappings=[mapping],
    )
    assert set(selected["proposal_id"].to_list()) == {"HOME", "LIST"}
    law = evidence.filter(pl.col("proposal_id") == "LAW").row(0, named=True)
    assert law["selection_status"] == "rejected_by_deterministic_prefilter"
    assert "detail_or_legal_page" in law["prefilter_reason_codes"]
    assert "central_authority_wrongly_assigned" in law["prefilter_reason_codes"]
    assert evidence.filter(pl.col("selection_status") == "selected_top3").height <= 3


def test_numeric_official_public_page_is_detail_evidence_not_entry() -> None:
    assert is_clear_detail_url("https://hefei.gov.cn/zwgk/public/17501/39253271.html") is True
    assert is_clear_detail_url("https://hefei.gov.cn/zwgk/public/17501/index.html") is False


def test_housing_page_evidence_accepts_provincial_department_suffix(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    frame = pl.from_dicts(
        [
            {
                "proposal_id": "AH_HOUSING",
                "slot_id": "SLOT_HEFEI_HOUSING",
                "city_id": "CITY_340100",
                "source_role": "housing_department",
                "candidate_url": "https://dohurd.ah.gov.cn/ztzl/gdzt/zfgjj30ncsp/hfs/index.html",
                "candidate_title": "official housing entry",
                "candidate_snippet": "official page",
                "page_title": "合肥市_安徽省住房和城乡建设厅",
                "page_text_excerpt": "合肥市 住房和城乡建设厅 政策信息",
                "evidence_enrichment_status": "completed",
            }
        ],
        infer_schema_length=None,
    )
    result = prefilter_candidate_frame(settings, frame)
    assert result[0, "prefilter_status"] == "shortlist"
    assert result[0, "city_match_evidence"] == "page_evidence:city_match"
    assert result[0, "role_match_evidence"] == "page_evidence:role_match"


def test_detail_parent_enrichment_does_not_starve_original_candidates() -> None:
    frame = pl.from_dicts(
        [
            {
                "slot_id": "SLOT_STARVATION",
                "candidate_url": "https://a.gov.cn/list-a/index.html",
                "canonical_url": "https://a.gov.cn/list-a/index.html",
                "prefilter_status": "evidence_enrichment_probe",
                "discovery_method": "ai_assisted_search",
                "deterministic_score": 120,
            },
            {
                "slot_id": "SLOT_STARVATION",
                "candidate_url": "https://a.gov.cn/list-b/index.html",
                "canonical_url": "https://a.gov.cn/list-b/index.html",
                "prefilter_status": "evidence_enrichment_probe",
                "discovery_method": "ai_assisted_search",
                "deterministic_score": 110,
            },
            {
                "slot_id": "SLOT_STARVATION",
                "candidate_url": "https://a.gov.cn/list-c/index.html",
                "canonical_url": "https://a.gov.cn/list-c/index.html",
                "prefilter_status": "evidence_enrichment_probe",
                "discovery_method": "ai_assisted_search",
                "deterministic_score": 100,
            },
            {
                "slot_id": "SLOT_STARVATION",
                "candidate_url": "https://a.gov.cn/list-parent/index.html",
                "canonical_url": "https://a.gov.cn/list-parent/index.html",
                "prefilter_status": "evidence_enrichment_probe",
                "discovery_method": "detail_parent_path_hypothesis",
                "deterministic_score": 999,
            },
        ],
        infer_schema_length=None,
    )
    selected = select_evidence_enrichment_candidates(frame, max_per_slot=3)
    assert selected.height == 3
    assert selected.filter(pl.col("discovery_method") == "ai_assisted_search").height == 2
    assert selected.filter(pl.col("discovery_method") == "detail_parent_path_hypothesis").height == 1


def test_ranked_evidence_controls_enrichment_order_without_bypassing_top3_gate(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    frame = pl.from_dicts(
        [
            {
                "proposal_id": "LOW_SCORE",
                "slot_id": "SLOT_RANKED_ENRICHMENT",
                "city_id": "CITY_320100",
                "source_role": "housing_department",
                "candidate_url": "https://nanjing.gov.cn/other/home",
                "canonical_url": "https://nanjing.gov.cn/other/home",
                "prefilter_status": "evidence_enrichment_probe",
                "prefilter_reason_codes": ["city_evidence_missing", "role_evidence_missing"],
                "discovery_method": "ai_assisted_search",
            },
            {
                "proposal_id": "HIGH_SCORE",
                "slot_id": "SLOT_RANKED_ENRICHMENT",
                "city_id": "CITY_320100",
                "source_role": "housing_department",
                "candidate_url": "https://nanjing.gov.cn/zwgk/index.html",
                "canonical_url": "https://nanjing.gov.cn/zwgk/index.html",
                "prefilter_status": "evidence_enrichment_probe",
                "prefilter_reason_codes": ["city_evidence_missing", "role_evidence_missing"],
                "discovery_method": "ai_assisted_search",
                "source_bundle_id": "BUNDLE_HIGH_SCORE",
            },
        ],
        infer_schema_length=None,
    )
    formal, ranked = rank_candidate_proposals(frame, settings=settings, max_candidates=3)
    assert formal.is_empty()
    assert ranked[0, "proposal_id"] == "HIGH_SCORE"
    assert ranked[0, "deterministic_score"] > ranked[1, "deterministic_score"]
    enrichment = select_evidence_enrichment_candidates(ranked, max_per_slot=1)
    assert enrichment[0, "proposal_id"] == "HIGH_SCORE"


def test_same_bundle_duplicate_is_mergeable_but_unmapped_cross_slot_url_is_not() -> None:
    row = _probe_row()
    row["source_bundle_id"] = "BUNDLE_TEST_HAINAN_GJJ"
    row["duplicate_scope"] = "same_source_bundle"
    slot = {"city_id": "CITY_460100", "city_name": "Haikou", "source_role": "provident_fund_center"}
    _, same_bundle_gates = evaluate_candidate_gates(
        row,
        slot=slot,
        duplicate_count=2,
        jurisdiction_mappings=[_mapping()],
    )
    assert next(item for item in same_bundle_gates if item["gate_name"] == "duplicate_or_existing_source")["gate_status"] == "PASS"

    _, unmapped_gates = evaluate_candidate_gates(
        {**row, "source_bundle_id": None, "duplicate_scope": None},
        slot=slot,
        duplicate_count=2,
        jurisdiction_mappings=[],
    )
    assert next(item for item in unmapped_gates if item["gate_name"] == "duplicate_or_existing_source")["gate_status"] == "FAIL"


def test_strict_gate_rejects_missing_probe_evidence() -> None:
    row = _probe_row("https://zjw.nanjing.gov.cn/")
    row.update(
        {
            "city_id": "CITY_320100",
            "city_name": "Nanjing",
            "source_role": "housing_department",
            "health_probe_success_count": 1,
            "city_match_evidence": "CITY_320100",
            "role_match_evidence": "housing_department",
        }
    )
    result, gates = evaluate_candidate_gates(row, slot=row, jurisdiction_mappings=[])
    assert result["verified"] is False
    assert next(item for item in gates if item["gate_name"] == "direct_healthy")["gate_status"] == "FAIL"
    assert next(item for item in gates if item["gate_name"] == "strict_admission_ready")["gate_status"] == "FAIL"


def test_probe_reclassified_list_url_can_pass_entry_gate_after_full_evidence() -> None:
    row = _probe_row()
    slot = {"city_id": "CITY_460100", "city_name": "Haikou", "source_role": "provident_fund_center"}
    mapping = _mapping()

    before, before_gates = evaluate_candidate_gates(
        row,
        slot=slot,
        jurisdiction_mappings=[mapping],
    )
    assert next(item for item in before_gates if item["gate_name"] == "reusable_list_entry")["gate_status"] == "FAIL"
    assert before["verified"] is False

    row["reclassification_method"] = "deterministic_probe_reclassification"
    after, after_gates = evaluate_candidate_gates(
        row,
        slot=slot,
        jurisdiction_mappings=[mapping],
    )
    entry_gate = next(item for item in after_gates if item["gate_name"] == "reusable_list_entry")
    assert entry_gate["gate_status"] == "PASS"
    assert entry_gate["reason_code"] == "reusable_entry_after_probe_reclassification"
    assert after["verified"] is True


def test_empty_yaml_registry_falls_back_to_materialized_registry(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    source = RegisteredSource(
        source_id="SRC_TEST_FALLBACK",
        source_name="Test Housing Bureau",
        domain="test.gov.cn",
        source_type="official_government",
        source_role="housing_department",
        agency_type="housing_department",
        official_status="official",
        list_page_urls=["https://test.gov.cn/policies/"],
        city_ids=["CITY_320100"],
        crawl_enabled=True,
        official_domain_verified=True,
        health_status="healthy",
    )
    materialize_registry_parquet([source], settings)
    loaded = load_registry(settings)
    assert [item.source_id for item in loaded] == ["SRC_TEST_FALLBACK"]
    assert loaded[0].crawl_enabled is True


def test_manual_research_and_human_review_slots_are_not_claimable() -> None:
    base = {
        "slot_id": "SLOT_MANUAL_REVIEW",
        "city_id": "CITY_460100",
        "source_role": "housing_department",
    }
    assert is_slot_claimable({**base, "work_status": "no_candidate_manual_research"}) is False
    assert is_slot_claimable({**base, "work_status": "no_candidate_manual_research"}, research_mode=True) is True
    assert is_slot_claimable({**base, "status": "HUMAN_REVIEW"}) is False
    assert is_slot_claimable({**base, "manual_review_status": "pending_human_review"}) is False


def test_official_weak_candidate_enters_bounded_evidence_enrichment_and_can_be_reconsidered(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    frame = pl.from_dicts(
        [
            {
                "proposal_id": "WEAK_OFFICIAL",
                "slot_id": "SLOT_NANJING_NATURAL",
                "city_id": "CITY_320100",
                "source_role": "natural_resources_department",
                "candidate_url": "https://zrzyt.hlj.gov.cn/",
                "candidate_title": "自然资源和规划局",
                "candidate_snippet": "官方栏目入口",
            }
        ],
        infer_schema_length=None,
    )
    before = prefilter_candidate_frame(settings, frame)
    assert before[0, "prefilter_status"] == "evidence_enrichment_probe"

    class FakeResult:
        requested_url = "https://zrzyt.hlj.gov.cn/"
        final_url = "https://zrzyt.hlj.gov.cn/"
        status_code = 200
        content_type = "text/html"
        body = (
            "<html><head><title>南京市自然资源和规划局</title></head>"
            "<body><div class='breadcrumb'>南京市 / 自然资源和规划局 / 政务公开</div>"
            "<h1>南京市自然资源和规划局</h1>"
            "<a href='/zwgk/index.html'>政策文件</a></body></html>"
        ).encode()
        response_sha256 = "sha-page-1"
        network_route = "direct"

    class FakeFetcher:
        def __init__(self) -> None:
            self.urls: list[str] = []

        def fetch(self, url: str):
            self.urls.append(url)
            return FakeResult()

    fetcher = FakeFetcher()
    enriched, evidence = enrich_candidate_evidence(
        before,
        fetcher=fetcher,
        max_per_slot=3,
        run_id="ENRICH_TEST",
    )
    after = prefilter_candidate_frame(settings, enriched)
    assert fetcher.urls == ["https://zrzyt.hlj.gov.cn/"]
    assert evidence[0]["status"] == "completed"
    primary = after.filter(pl.col("candidate_url") == "https://zrzyt.hlj.gov.cn/")
    assert primary.height == 1
    assert primary[0, "prefilter_status"] == "shortlist"
    assert "page_evidence" in str(primary[0, "city_match_evidence"])
    assert "page_evidence" in str(primary[0, "role_match_evidence"])
    assert primary[0, "evidence_enrichment_status"] == "completed"
    assert primary[0, "page_role_evidence"]
    assert primary[0, "page_agency_evidence"]
    assert primary[0, "page_entry_type_evidence"]
    assert primary[0, "page_redirect_chain_json"] == "[]"
    assert not bool(primary[0, "is_verified"] if "is_verified" in primary.columns else False)
    assert any(
        row.get("discovery_method") == "page_enrichment_same_domain"
        for row in enriched.iter_rows(named=True)
    )
    derived = next(
        row
        for row in enriched.iter_rows(named=True)
        if row.get("discovery_method") == "page_enrichment_same_domain"
    )
    assert derived.get("page_title") is None
    assert derived.get("page_role_evidence") is None
    assert derived.get("parent_page_title")
    assert derived.get("parent_page_role_evidence")
    assert derived.get("evidence_enrichment_status") is None


def test_page_evidence_aliases_survive_candidate_upsert(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    upsert_candidates(
        [
            {
                "candidate_id": "CAND_PAGE_EVIDENCE_ALIASES",
                "city_id": "CITY_320100",
                "source_role": "housing_department",
                "candidate_url": "https://zjj.nanjing.gov.cn/policy/index.html",
                "candidate_title": "official housing list",
                "page_city_evidence": "page text names target city",
                "page_role_evidence": "page heading names housing bureau",
                "page_agency_evidence": "page title and breadcrumb",
                "page_entry_type_evidence": "list navigation and article links",
                "page_pagination_evidence": "natural_single_page",
                "page_redirect_chain_json": "[]",
                "page_response_sha256": "page-evidence-sha",
            }
        ],
        settings,
    )
    current = list_candidates(candidate_id="CAND_PAGE_EVIDENCE_ALIASES", settings=settings).row(0, named=True)
    assert current["page_city_evidence"] == "page text names target city"
    assert current["page_role_evidence"] == "page heading names housing bureau"
    assert current["page_agency_evidence"] == "page title and breadcrumb"
    assert current["page_entry_type_evidence"] == "list navigation and article links"
    assert current["page_pagination_evidence"] == "natural_single_page"
    assert current["page_redirect_chain_json"] == "[]"


def test_detail_page_parent_hypothesis_is_fetched_before_formal_prefilter(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    detail_url = "https://huaian.gov.cn/col/16657_173466/art/17460288/1747119876527aQf9lhsR.html"
    frame = pl.from_dicts(
        [
            {
                "proposal_id": "DETAIL",
                "slot_id": "SLOT_HUAIAN_GJJ",
                "city_id": "CITY_320800",
                "source_role": "provident_fund_center",
                "candidate_url": detail_url,
                "candidate_title": "淮安市人民政府 房贷公积金政策",
                "candidate_snippet": "official policy detail",
            }
        ],
        infer_schema_length=None,
    )
    before = prefilter_candidate_frame(settings, frame)
    assert before[0, "prefilter_status"] == "rejected_by_deterministic_prefilter"

    class FakeResult:
        status_code = 200
        content_type = "text/html"
        response_sha256 = "sha-parent"
        network_route = "direct_ok"
        body = (
            "<html><head><title>淮安市住房公积金管理中心政策信息</title></head>"
            "<body><h1>住房公积金政策文件</h1>"
            "<div class='breadcrumb'>淮安市 / 住房公积金</div>"
            "<a href='/col/16657_173466/index.html'>政策文件</a></body></html>"
        ).encode()

        def __init__(self, url: str) -> None:
            self.requested_url = url
            self.final_url = url

    class FakeFetcher:
        def __init__(self) -> None:
            self.urls: list[str] = []

        def fetch(self, url: str) -> FakeResult:
            self.urls.append(url)
            return FakeResult(url)

    fetcher = FakeFetcher()
    enriched, _ = enrich_candidate_evidence(
        before,
        fetcher=fetcher,
        max_per_slot=3,
        run_id="PARENT_TEST",
    )
    after = prefilter_candidate_frame(settings, enriched)
    parent = after.filter(pl.col("discovery_method") == "detail_parent_path_hypothesis")
    assert parent.height >= 1
    assert parent.filter(pl.col("prefilter_status") == "shortlist").height >= 1
    assert any("/col/16657_173466" in url for url in fetcher.urls)
    if "is_verified" in parent.columns:
        assert not any(bool(value) for value in parent["is_verified"].to_list())


def test_zero_yield_completed_checkpoint_can_be_requeued_without_deleting_history(tmp_path: Path) -> None:
    root = tmp_path / "autopilot"
    store = GlobalSlotCheckpointStore(root)
    slot_id = "SLOT_ZERO_YIELD"
    store.append(
        {
            "event": "CHECKPOINT_COMPLETED",
            "slot_id": slot_id,
            "status": "COMPLETED",
            "run_id": "OLD_RUN",
            "terminal_outcome": "deterministic_probe_completed_without_verified_candidate",
            "work_fingerprint": "fp",
            "ai_call_persisted": True,
        }
    )
    run_dir = tmp_path / "old_run"
    run_dir.mkdir()
    (run_dir / "run_summary.json").write_text(
        json.dumps(
            {
                "run_dir": str(run_dir),
                "slot_results": [
                    {
                        "slot_id": slot_id,
                        "applied_candidates": 0,
                        "probed_candidates": 0,
                        "human_review": 0,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    result = store.requeue_zero_yield_run(run_dir, repair_run_id="REPAIR_RUN")
    assert result["requeued"] == 1
    assert store.snapshot()[slot_id]["status"] == "FAILED_RECOVERABLE"
    assert any(item["event"] == "CHECKPOINT_COMPLETED" for item in store.history())
    assert is_slot_claimable(
        {
            "slot_id": slot_id,
            "city_id": "CITY_TEST",
            "source_role": "housing_department",
            "status": "unresolved",
        },
        store.snapshot()[slot_id],
    ) is True
