from __future__ import annotations

import hashlib
from datetime import date

import duckdb
import httpx
import polars as pl
import yaml

from policydb.confidence import (
    ConfidenceComponents,
    record_confidence,
    review_required,
)
from policydb.coverage import build_source_matrix, record_source_window
from policydb.crawl.dedup import (
    canonicalize_url,
    classify_text_pair,
    glm_cache_key,
    normalize_policy_text,
    normalized_text_hash,
    policy_identity_key,
    simhash64,
    simhash_similarity,
)
from policydb.crawl.fetcher import RespectfulFetcher
from policydb.crawl.models import RegisteredSource
from policydb.crawl.pipeline import CrawlPipeline
from policydb.migration_v2 import apply_migration, migration_plan
from policydb.settings import Settings
from policydb.source_quality import unresolved_sources, validate_registry


def _source(**updates) -> RegisteredSource:
    values = {
        "source_id": "SRC1",
        "source_name": "测试来源",
        "domain": "example.gov.cn",
        "source_type": "government",
        "source_role": "canonical_candidate",
        "official_status": "official",
    }
    values.update(updates)
    return RegisteredSource.model_validate(values)


def _write_repo(root, sources: list[dict], *, with_records: bool = False) -> Settings:
    (root / "data" / "reference").mkdir(parents=True)
    (root / "data" / "curated").mkdir(parents=True)
    (root / "pyproject.toml").write_text("[project]\nname='test'\nversion='0'\n", encoding="utf-8")
    (root / "data" / "reference" / "source_registry.yaml").write_text(
        yaml.safe_dump({"version": 1, "sources": sources}, allow_unicode=True), encoding="utf-8"
    )
    if with_records:
        pl.DataFrame(
            {
                "record_id": [f"R{i}" for i in range(3011)],
                "source_sheet": ["T1 房地产政策目录"] * 3011,
                "record_date": [date(2003, 6, 5)] + [date(2026, 7, 2)] * 3010,
            }
        ).write_parquet(root / "data" / "curated" / "records.parquet")
    return Settings(root=root)


def test_url_canonicalization_removes_tracking_and_fragment():
    assert canonicalize_url("HTTPS://www.Example.gov.cn/a/?utm_source=x&b=2#top") == "https://example.gov.cn/a?b=2"


def test_url_canonicalization_unifies_mobile_host():
    assert canonicalize_url("https://m.example.gov.cn/a") == canonicalize_url("https://example.gov.cn/a/")


def test_url_canonicalization_sorts_preserved_query():
    assert canonicalize_url("https://x.gov.cn/a?z=2&a=1") == "https://x.gov.cn/a?a=1&z=2"


def test_text_normalization_is_whitespace_stable():
    assert normalize_policy_text("住房 政策\n通知") == normalize_policy_text("住房政策通知")


def test_normalized_text_hash_is_stable():
    assert normalized_text_hash("住房 政策") == normalized_text_hash("住房政策")


def test_simhash_identical_text_similarity_is_one():
    value = simhash64("关于调整住房公积金贷款政策的通知")
    assert simhash_similarity(value, value) == 1.0


def test_dedup_exact_normalized_text():
    decision = classify_text_pair("住房 政策", "住房政策")
    assert (decision.level, decision.decision) == ("L4", "duplicate_content")


def test_dedup_numeric_conflict_is_material_change():
    decision = classify_text_pair(
        "首付比例调整为20%", "首付比例调整为30%", left_numbers=["20"], right_numbers=["30"]
    )
    assert decision.decision == "material_change"


def test_dedup_dissimilar_text_is_new_document():
    assert classify_text_pair("住房公积金贷款", "城市道路绿化养护").decision == "new_document"


def test_policy_identity_key_is_deterministic():
    values = dict(title="通知", document_number="住建〔2026〕1号", agency="住建局")
    assert policy_identity_key(**values) == policy_identity_key(**values)


def test_glm_cache_key_changes_with_prompt_version():
    assert glm_cache_key("h", "m", "p1", "s") != glm_cache_key("h", "m", "p2", "s")


def test_confidence_weighted_formula():
    score = ConfidenceComponents(1.0, 0.8, 0.6, 0.4, 0.2).score
    assert score == 0.70


def test_confidence_conflict_always_requires_review():
    assert review_required(0.99, conflict=True)


def test_confidence_official_high_score_can_pass():
    assert not review_required(0.90, conflict=False, official=True)


def test_record_confidence_penalizes_critical_minimum():
    assert record_confidence([0.9, 0.9], [0.9, 0.5]) == 0.64


def test_registered_source_maps_legacy_agency_type():
    assert _source(agency_type="local_government").agency_type == "municipal_government"


def test_registered_source_normalizes_comma_lists():
    assert _source(city_ids="CITY1,CITY2").city_ids == ["CITY1", "CITY2"]


def test_source_matrix_expands_national_source(tmp_path):
    source = _source(scope_type="national", crawl_enabled=True).model_dump(mode="json")
    settings = _write_repo(tmp_path, [source])
    pl.DataFrame(
        {
            "city_id": ["C1", "C2"], "city_name": ["甲市", "乙市"],
            "province_name": ["甲省", "乙省"], "province_code": [1, 2],
        }
    ).write_parquet(settings.curated / "cities_105.parquet")
    assert build_source_matrix(settings).height == 2


def test_unresolved_source_is_reported_without_guessing(tmp_path):
    settings = _write_repo(tmp_path, [_source(scope_type="unknown").model_dump(mode="json")])
    result = unresolved_sources(settings)
    assert result.height == 1
    assert "scope_type_unknown" in result[0, "reasons"]


def test_registry_rejects_unofficial_required_source(tmp_path):
    settings = _write_repo(
        tmp_path,
        [_source(official_status="unknown", required_level="required").model_dump(mode="json")],
    )
    assert not validate_registry(settings)["passed"]


def test_complete_zero_requires_complete_scan(tmp_path):
    settings = _write_repo(tmp_path, [])
    row = record_source_window(
        run_id="RUN", source_id="SRC", period_start=date(2026, 1, 1),
        period_end=date(2026, 1, 31), scan_method="list", candidate_count=0,
        fetched_count=0, policy_count=0, error_count=0, page_count=2,
        completion_evidence={"exhaustive": True}, settings=settings,
    )
    assert row["coverage_status"] == "complete_confirmed_zero"


def test_detail_only_scan_remains_partial(tmp_path):
    settings = _write_repo(tmp_path, [])
    row = record_source_window(
        run_id="RUN", source_id="SRC", period_start=date(2026, 1, 1),
        period_end=date(2026, 1, 31), scan_method="seed", candidate_count=1,
        fetched_count=1, policy_count=0, error_count=0, page_count=0, settings=settings,
    )
    assert row["coverage_status"] == "partial"


def test_migration_dry_run_never_writes_raw(tmp_path):
    settings = _write_repo(tmp_path, [_source().model_dump(mode="json")])
    assert migration_plan(settings)["raw_writes"] == 0


def test_migration_preserves_raw_and_t1_anchor(tmp_path):
    settings = _write_repo(tmp_path, [_source().model_dump(mode="json")], with_records=True)
    raw = settings.root / "data" / "raw" / "seed" / "seed.xlsx"
    raw.parent.mkdir(parents=True)
    raw.write_bytes(b"immutable")
    before = hashlib.sha256(raw.read_bytes()).hexdigest()
    result = apply_migration(settings)
    assert result["verified"]
    assert hashlib.sha256(raw.read_bytes()).hexdigest() == before
    assert result["t1_count"] == 3011


def test_v2_database_views_are_queryable(root):
    with duckdb.connect(str(root / "database" / "policydb.duckdb"), read_only=True) as con:
        assert con.execute("SELECT count(DISTINCT city_id) FROM v_city_month_coverage").fetchone()[0] == 105
        assert con.execute(
            "SELECT count(*) FROM v_city_month_policy_panel_research_ready "
            "WHERE policy_count=0 AND coverage_status<>'complete_confirmed_zero'"
        ).fetchone()[0] == 0


def test_five_url_v2_pipeline_writes_auditable_decisions(tmp_path):
    source = _source(
        seed_urls=[f"https://example.gov.cn/policy/{index}" for index in range(5)],
        crawl_enabled=True,
        rate_limit=0,
    ).model_dump(mode="json")
    settings = _write_repo(tmp_path, [source])

    def handler(request: httpx.Request) -> httpx.Response:
        number = request.url.path.rsplit("/", 1)[-1]
        return httpx.Response(
            200,
            headers={"content-type": "text/html; charset=utf-8", "etag": f'"{number}"'},
            text=f"<html><title>住房政策{number}</title><body>住房政策正文{number}</body></html>",
            request=request,
        )

    pipeline = CrawlPipeline(
        settings,
        fetcher=RespectfulFetcher(
            client=httpx.Client(transport=httpx.MockTransport(handler)),
            check_robots=False,
            rate_limit=0,
        ),
    )
    plan = pipeline.plan(
        run_type="seed_backtrack",
        start_date=date(2026, 1, 1),
        end_date=date(2026, 1, 31),
        include_disabled_seed=True,
    )
    result = pipeline.run(plan["run_id"])
    assert result == {"run_id": plan["run_id"], "fetched": 5, "failed": 0}
    assert pl.read_parquet(settings.curated / "policy_document_versions.parquet").height == 5
    assert pl.read_parquet(settings.curated / "dedup_decisions.parquet").height == 5
    windows = pl.read_parquet(settings.curated / "crawl_source_windows.parquet")
    assert windows.height == 1
    assert windows[0, "coverage_status"] == "partial"

    repeat_plan = pipeline.plan(
        run_type="seed_backtrack",
        start_date=date(2026, 1, 1),
        end_date=date(2026, 1, 31),
        include_disabled_seed=True,
    )
    pipeline.run(repeat_plan["run_id"])
    versions = pl.read_parquet(settings.curated / "policy_document_versions.parquet")
    decisions = pl.read_parquet(settings.curated / "dedup_decisions.parquet")
    assert versions.height == 5
    assert decisions.height == 10
    assert decisions.filter(pl.col("run_id") == repeat_plan["run_id"])[
        "decision"
    ].to_list() == ["duplicate_content"] * 5
