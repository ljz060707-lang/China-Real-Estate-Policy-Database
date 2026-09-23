from __future__ import annotations

import json
import threading
import time

import httpx
import polars as pl
import pytest
from pydantic import ValidationError

from policydb.enrich.glm import AIActionClassification, GLMEnricher, GLMExtraction
from policydb.settings import Settings


def _valid_output() -> dict:
    return {
        "is_relevant": True,
        "policy_title": "测试政策",
        "policy_direction": "easing",
        "confidence": 0.9,
        "needs_review": False,
    }


def test_glm_schema_accepts_valid_json():
    result = GLMExtraction.model_validate(_valid_output())
    assert result.demand_flags.purchase_limit is False


def test_glm_schema_rejects_missing_confidence():
    value = _valid_output()
    value.pop("confidence")
    with pytest.raises(ValidationError):
        GLMExtraction.model_validate(value)


def test_glm_invalid_json_retries_then_succeeds(tmp_path):
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        content = "not-json" if calls == 1 else json.dumps(_valid_output(), ensure_ascii=False)
        return httpx.Response(
            200,
            json={"choices": [{"message": {"content": content}}]},
            request=request,
        )

    root = tmp_path / "repo"
    (root / "data" / "curated").mkdir(parents=True)
    enricher = GLMEnricher(
        Settings(root=root),
        api_key="test-key",
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        retries=1,
    )
    result = enricher.extract("abc", "政策正文")
    assert result and result.confidence == 0.9
    assert calls == 2


def test_glm_without_key_creates_pending_cache(tmp_path):
    root = tmp_path / "repo"
    (root / "data" / "curated").mkdir(parents=True)
    enricher = GLMEnricher(Settings(root=root), api_key=None)
    assert enricher.extract("hash", "正文") is None
    cache = pl.read_parquet(root / "data" / "curated" / "llm_extractions.parquet")
    assert cache[0, "status"] == "awaiting_api_key"
    assert cache[0, "needs_review"]


def test_glm_chunks_long_text():
    assert len(GLMEnricher.chunks("x" * 25001, size=10000)) == 3


def test_glm_concurrency_has_bounded_eight_request_ceiling(tmp_path):
    root = tmp_path / "repo"
    (root / "data" / "curated").mkdir(parents=True)
    enricher = GLMEnricher(Settings(root=root), api_key=None, concurrency=99)
    assert enricher.concurrency == 8


def test_glm_second_pass_is_independent_and_evidence_only(tmp_path):
    verification = {
        "field_evidence_valid": True,
        "segmentation_complete": True,
        "city_scope_supported": True,
        "classification_supported": True,
        "direction_supported": True,
        "strength_supported": True,
        "confidence": 0.94,
    }
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        output = _valid_output() if calls == 1 else verification
        return httpx.Response(
            200,
            json={"choices": [{"message": {"content": json.dumps(output)}}]},
            request=request,
        )

    root = tmp_path / "repo"
    (root / "data" / "curated").mkdir(parents=True)
    enricher = GLMEnricher(
        Settings(root=root),
        api_key="test",
        client=httpx.Client(transport=httpx.MockTransport(handler)),
    )
    extraction = enricher.extract("hash", "原文证据")
    checked = enricher.verify("hash", "原文证据", extraction)
    assert checked and checked.field_evidence_valid and checked.confidence == 0.94
    assert calls == 2


def test_glm_enrich_pending_uses_bounded_api_concurrency(tmp_path, monkeypatch):
    root = tmp_path / "repo"
    (root / "data" / "curated").mkdir(parents=True)
    enricher = GLMEnricher(Settings(root=root), api_key="test", concurrency=2)
    versions = pl.DataFrame(
        {
            "content_sha256": [f"hash-{index}" for index in range(4)],
            "extracted_text": [f"房地产政策正文{index}" for index in range(4)],
        }
    )
    monkeypatch.setattr(
        enricher,
        "_pending_versions",
        lambda *args, **kwargs: versions,
    )
    active = 0
    maximum = 0
    calls = 0
    lock = threading.Lock()

    def fake_extract(content_sha256, text):
        nonlocal active, maximum, calls
        with lock:
            active += 1
            calls += 1
            maximum = max(maximum, active)
        time.sleep(0.03)
        with lock:
            active -= 1
        return GLMExtraction.model_validate(_valid_output())

    monkeypatch.setattr(enricher, "extract", fake_extract)
    result = enricher.enrich_pending()

    assert result["completed"] == 4
    assert calls == 4
    assert maximum == 2


def test_glm_verify_pending_uses_bounded_api_concurrency(tmp_path, monkeypatch):
    root = tmp_path / "repo"
    curated = root / "data" / "curated"
    curated.mkdir(parents=True)
    settings = Settings(root=root)
    enricher = GLMEnricher(settings, api_key="test", concurrency=2)
    extraction = GLMExtraction.model_validate(_valid_output()).model_dump_json()
    pl.DataFrame(
        {
            "content_sha256": [f"hash-{index}" for index in range(4)],
            "status": ["complete"] * 4,
            "output_json": [extraction] * 4,
        }
    ).write_parquet(enricher.cache_path)
    (curated / "policy_document_versions.parquet").write_bytes(b"placeholder")
    versions = pl.DataFrame(
        {
            "content_sha256": [f"hash-{index}" for index in range(4)],
            "extracted_text": ["房地产政策正文"] * 4,
        }
    )
    monkeypatch.setattr(
        enricher,
        "_pending_versions",
        lambda *args, **kwargs: versions,
    )
    active = 0
    maximum = 0
    calls = 0
    lock = threading.Lock()

    def fake_verify(content_sha256, text, first_pass):
        nonlocal active, maximum, calls
        with lock:
            active += 1
            calls += 1
            maximum = max(maximum, active)
        time.sleep(0.03)
        with lock:
            active -= 1
        return object()

    monkeypatch.setattr(enricher, "verify", fake_verify)
    result = enricher.verify_pending()

    assert result["completed"] == 4
    assert calls == 4
    assert maximum == 2


def test_ai_action_requires_matching_primary_secondary():
    with pytest.raises(ValidationError):
        AIActionClassification.model_validate(
            {
                "action_text": "提高公积金贷款额度",
                "primary_category": "D",
                "secondary_category": "F03",
                "instrument_type": "credit_finance",
                "direction": "loosening",
                "evidence_excerpt": "提高公积金贷款额度",
                "evidence_start": 0,
                "evidence_end": 9,
                "confidence": 0.95,
            }
        )


def test_ai_action_evidence_must_uniquely_match_source():
    extraction = GLMExtraction.model_validate(
        {
            **_valid_output(),
            "policy_actions": [
                {
                    "action_text": "提高公积金贷款额度",
                    "primary_category": "D",
                    "secondary_category": "D06",
                    "instrument_type": "credit_finance",
                    "direction": "loosening",
                    "evidence_excerpt": "提高公积金贷款额度",
                    "evidence_start": 0,
                    "evidence_end": 9,
                    "confidence": 0.95,
                }
            ],
        }
    )
    with pytest.raises(ValueError):
        GLMEnricher._validate_action_evidence("提高公积金贷款额度；提高公积金贷款额度", extraction)
