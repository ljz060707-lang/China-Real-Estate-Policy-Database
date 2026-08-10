from types import SimpleNamespace

from policydb.ai_audit import AIAuditStore
from policydb.source_completion_ai_workflow import (
    SourceAIAssessment,
    _ai_status,
    _call_ai,
    _queries,
    _sha,
    interface_audit,
)


def test_structured_ai_schema_is_conservative():
    value = SourceAIAssessment(search_queries=["南京市 住房公积金 官网"], confidence=0.4)
    assert value.recommended_action == "proposed"
    assert value.entry_type_hint == "unknown"
    assert "is_verified" not in interface_audit()


def test_generated_query_count_matches_structured_output_limit():
    row = {
        "city_name": "南京市",
        "city_id": "CITY_320100",
        "source_role": "housing_department",
    }
    queries = _queries(row)
    assert len(queries) <= 8
    SourceAIAssessment(search_queries=queries)


def test_ai_status_never_marks_proposal_verified():
    assert _ai_status({"work_status": "verified_enabled"}) == "A_verified_enabled"
    assert _ai_status({"work_status": "no_candidate_manual_research", "best_candidate_id": None}) == "D_no_candidate_ai_discoverable"


def test_call_ai_retries_and_returns_structured_value():
    class Provider:
        def __init__(self):
            self.calls = 0

        def structured(self, **kwargs):
            self.calls += 1
            if self.calls < 3:
                raise RuntimeError("temporary")
            return SourceAIAssessment(search_queries=["query"], confidence=0.5), SimpleNamespace(prompt_tokens=2, completion_tokens=1)

    provider = Provider()
    value, trace, error = _call_ai(provider, "model", "system", "user")
    assert provider.calls == 3
    assert value is not None and value.search_queries == ["query"]
    assert trace is not None
    assert error is None


def test_request_hash_is_stable_and_does_not_need_secret():
    assert _sha({"slot_id": "S1", "model": "m"}) == _sha({"model": "m", "slot_id": "S1"})
    assert "Bearer" not in interface_audit()


def _audit_payload():
    return {
        "request_id": "REQ_TEST_1",
        "slot_id": "SLOT_TEST_1",
        "provider": "test",
        "model": "model",
        "prompt_version": "test-v1",
        "prompt_hash": "prompt-hash",
        "request_hash": "request-hash",
        "cache_key": "cache-key",
    }


def test_ai_audit_persists_started_and_completed_without_secrets(tmp_path):
    store = AIAuditStore(tmp_path)
    store.start({**_audit_payload(), "api_key": "must-not-persist"})
    store.complete("REQ_TEST_1", response_hash="response-hash", prompt_tokens=10, completion_tokens=3, total_tokens=13, estimated_cost_usd=None, cache_hit=False)
    record = store.records()[0]
    assert record["status"] == "response_completed"
    assert record["total_tokens"] == 13
    assert "api_key" not in record
    assert "authorization" not in record


def test_ai_audit_marks_interrupted_request_on_resume(tmp_path):
    AIAuditStore(tmp_path).start(_audit_payload())
    recovered = AIAuditStore(tmp_path).recover_interrupted()
    assert recovered[0]["status"] == "interrupted"
    assert recovered[0]["error_type"] == "process_interrupted"


def test_ai_audit_failure_keeps_started_request(tmp_path):
    store = AIAuditStore(tmp_path)
    store.start(_audit_payload())
    store.fail("REQ_TEST_1", error_type="TimeoutError", error_message="safe error")
    record = store.records()[0]
    assert record["status"] == "response_failed"
    assert record["error_type"] == "TimeoutError"


def test_ai_call_audit_retries_once_and_persists_one_logical_request(tmp_path):
    class FlakyProvider:
        attempts = 0

        def structured(self, **_kwargs):
            self.attempts += 1
            if self.attempts == 1:
                raise TimeoutError("temporary")
            return SourceAIAssessment(search_queries=["query"], confidence=0.5), SimpleNamespace(prompt_tokens=2, completion_tokens=1)

    provider = FlakyProvider()
    value, trace, error = _call_ai(provider, "model", "system", "user", audit=AIAuditStore(tmp_path), audit_payload=_audit_payload())
    assert value is not None and trace is not None and error is None
    assert provider.attempts == 2
    records = AIAuditStore(tmp_path).records()
    assert len(records) == 1
    assert records[0]["status"] == "response_completed"
    assert records[0]["attempt"] == 2


def test_ai_audit_cache_hit_has_zero_incremental_cost(tmp_path):
    store = AIAuditStore(tmp_path)
    store.start(_audit_payload())
    store.complete(
        "REQ_TEST_1",
        response_hash="cached-response",
        prompt_tokens=0,
        completion_tokens=0,
        total_tokens=0,
        estimated_cost_usd=0,
        cache_hit=True,
    )
    record = store.records()[0]
    assert record["cache_hit"] is True
    assert record["estimated_cost_usd"] == 0
