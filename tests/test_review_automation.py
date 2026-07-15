from __future__ import annotations

import httpx

from policydb.crawl.fetcher import RespectfulFetcher
from policydb.recovery import (
    RecoveryRecord,
    SourceCandidate,
    SourceRecoveryEngine,
)
from policydb.review_automation import (
    IndependentVerification,
    deterministic_verdict,
    diagnose_document_problem,
)
from policydb.settings import Settings


def test_diagnosis_distinguishes_segmentation_from_true_missing():
    segmented = diagnose_document_problem(
        text="标题正文",
        parsed={"repair_actions": [{"reason": "short_block"}]},
    )
    missing = diagnose_document_problem(text="", source_url=None, parsed={})
    assert segmented.diagnosis == "segmentation_error"
    assert missing.diagnosis == "true_information_missing"


def test_low_confidence_or_conflict_falls_back_to_human():
    status, confidence, evidence = deterministic_verdict(
        official_status="unknown",
        title_conflict=True,
        city_conflict=False,
        date_conflict=False,
        completeness_score=0.5,
        rule_model_agreement=False,
        first_confidence=0.6,
    )
    assert status == "manual_review_required"
    assert confidence < 0.7
    assert "title_conflict" in evidence


def test_second_review_never_decides_without_deterministic_rule():
    verification = IndependentVerification(
        field_evidence_valid=True,
        segmentation_complete=True,
        city_scope_supported=True,
        classification_supported=True,
        direction_supported=True,
        strength_supported=True,
        confidence=0.98,
    )
    status, confidence, _ = deterministic_verdict(
        official_status="official",
        title_conflict=False,
        city_conflict=False,
        date_conflict=False,
        completeness_score=0.85,
        rule_model_agreement=True,
        first_confidence=0.72,
        second_review=verification,
    )
    assert status == "auto_verified"
    assert confidence >= 0.9


def test_official_source_is_recovered_to_append_only_raw(tmp_path):
    body = (
        "<html><title>关于城市更新的通知</title><article>"
        "<p>关于城市更新的通知</p><p>城市更新政策正文，支持老旧小区改造。</p>"
        "</article></html>"
    ).encode()

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            content=body,
            headers={"content-type": "text/html"},
            request=request,
        )

    root = tmp_path / "repo"
    (root / "data" / "curated").mkdir(parents=True)
    engine = SourceRecoveryEngine(
        Settings(root=root),
        fetcher=RespectfulFetcher(
            client=httpx.Client(transport=httpx.MockTransport(handler)),
            check_robots=False,
            rate_limit=0,
        ),
    )
    result = engine.recover(
        RecoveryRecord(record_id="POL_1", title="关于城市更新的通知"),
        [
            SourceCandidate(
                url="https://example.gov.cn/policy/1",
                title="关于城市更新的通知",
                official_status="official",
            )
        ],
    )
    assert result["status"] == "auto_recovered_official"
    assert (root / result["local_path"]).exists()


def test_source_candidate_conflict_is_not_auto_applied(tmp_path):
    engine = SourceRecoveryEngine(Settings(root=tmp_path))
    record = RecoveryRecord(record_id="P", title="住房政策")
    candidates = [
        SourceCandidate(url=f"https://a{i}.gov.cn/p", title="住房政策", official_status="official")
        for i in range(2)
    ]
    result = engine.recover(record, candidates)
    assert result["status"] == "candidate_conflict"
