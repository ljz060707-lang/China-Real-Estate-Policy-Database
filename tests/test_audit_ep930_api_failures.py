from __future__ import annotations

from scripts.audit_ep930_api_failures import _classify, _forensics_summary


def test_schema_type_mismatch_is_distinguished_from_missing_field() -> None:
    category, diagnostics = _classify(
        {
            "status": "response_failed",
            "failure_class": "SCHEMA_VALIDATION_FAILURE",
            "http_status": 200,
            "response_received": True,
            "json_parse_ok": True,
            "schema_valid": False,
            "schema_errors": [
                "actions.0.policy_type: Input should be a valid string",
            ],
        }
    )

    assert category == "SCHEMA_FIELD_TYPE_MISMATCH"
    assert diagnostics["failed_field"] == "actions.0.policy_type"
    assert diagnostics["expected_type"] == "string"


def test_missing_required_field_and_provider_http_are_distinct() -> None:
    missing, _ = _classify(
        {
            "status": "response_failed",
            "json_parse_ok": True,
            "schema_valid": False,
            "schema_errors": ["actions: Field required"],
        }
    )
    http_error, _ = _classify(
        {
            "status": "response_failed",
            "http_status": 402,
            "response_received": True,
        }
    )

    assert missing == "SCHEMA_MISSING_REQUIRED_FIELD"
    assert http_error == "PROVIDER_HTTP_ERROR"


def test_legacy_timeout_without_transport_metadata_stays_unknown() -> None:
    category, diagnostics = _classify(
        {
            "status": "response_failed",
            "error_type": "APITimeoutError",
            "error_message": "request timed out",
        }
    )

    assert category == "UNKNOWN_PROVIDER_FAILURE"
    assert diagnostics["failed_field"] == ""


def test_forensics_summary_does_not_require_raw_response_body() -> None:
    summary = _forensics_summary(
        [
            {
                "timestamp": "2026-08-15T00:00:00Z",
                "probe_type": "RECOVERY_NETWORK_PROBE",
                "failure_category": "READ_TIMEOUT",
                "latency_ms": "127000",
                "configured_read_timeout": "30.0",
                "timeout_class": "read",
                "request_id": "REQ1",
            }
        ]
    )

    assert summary["read_timeout_evidence"]["sample_count"] == 1
    assert summary["read_timeout_evidence"]["configured_read_timeout_seconds"] == [30.0]
    assert "raw_response_payload" not in summary
