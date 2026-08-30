from __future__ import annotations

import importlib.util
from pathlib import Path


def _module():
    path = Path(__file__).parents[1] / "scripts" / "audit_ep930_timeout_chain.py"
    spec = importlib.util.spec_from_file_location("audit_ep930_timeout_chain", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_duration_does_not_claim_nested_retry_without_attempt_trace() -> None:
    module = _module()
    record = {
        "failure_class": "READ_TIMEOUT",
        "configured_read_timeout": 30,
        "max_retries": 3,
    }
    assert module._nested_retry_evidence(record, 126_000) == "CONFIGURED_RETRY_CHAIN_PLAUSIBLE_NO_PER_ATTEMPT_TRACE"
    assert module._retry_source(record, module._nested_retry_evidence(record, 126_000)) == "UNKNOWN"


def test_explicit_transport_attempts_are_distinguished() -> None:
    module = _module()
    record = {"failure_class": "READ_TIMEOUT", "sdk_attempts": 4, "retry_source": "SDK"}
    nested = module._nested_retry_evidence(record, 126_000)
    assert nested == "CONFIRMED_EXPLICIT_TRANSPORT_ATTEMPTS"
    assert module._retry_source(record, nested) == "SDK"


def test_http_200_schema_failure_is_included_in_latency_stats() -> None:
    module = _module()
    stats = module._response_latency_stats(
        [
            {"http_status": "200", "response_received": "true", "schema_valid": "false", "latency_ms": 20_000},
            {"http_status": "200", "response_received": "true", "schema_valid": "true", "latency_ms": 10_000},
            {"http_status": "500", "response_received": "true", "schema_valid": "", "latency_ms": 1_000},
        ]
    )
    assert stats["sample_count"] == 2
    assert stats["schema_invalid_count"] == 1
    assert stats["median_seconds"] == 15.0
