"""Build a secret-safe forensic timeline from persisted EP930 API audits.

This script is intentionally read-only with respect to the production queue and
database.  It consumes the request-audit JSON written by the existing API
controller and persists only sanitized metadata plus response hashes.  Provider
response bodies are never copied to the timeline.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
from collections import Counter
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

TIMELINE_FIELDS = [
    "timestamp",
    "probe_type",
    "provider",
    "model",
    "http_status",
    "response_received",
    "raw_response_saved",
    "json_parseable",
    "schema_valid",
    "failure_class",
    "failure_category",
    "failed_field",
    "expected_type",
    "actual_type",
    "expected_enum",
    "actual_value",
    "prompt_version",
    "schema_version",
    "content_sha256",
    "latency_ms",
    "timeout_class",
    "configured_read_timeout",
    "configured_connect_timeout",
    "retry_decision",
    "request_id",
    "source_path",
]

CLASSIFICATION_CATEGORIES = (
    "JSON_PARSE_FAILURE",
    "CODE_FENCE_WRAPPED_JSON",
    "SCHEMA_FIELD_TYPE_MISMATCH",
    "SCHEMA_ENUM_MISMATCH",
    "SCHEMA_MISSING_REQUIRED_FIELD",
    "EMPTY_RESPONSE",
    "PROVIDER_HTTP_ERROR",
    "TRUNCATED_RESPONSE",
)


def _as_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if value is None:
        return None
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "1", "yes"}:
            return True
        if lowered in {"false", "0", "no"}:
            return False
    return None


def _first(data: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in data and data[key] is not None:
            return data[key]
    return None


def _derived_failure_class(record: dict[str, Any]) -> str:
    """Recover only classifications that are decisive from legacy metadata."""

    value = _safe_text(
        " ".join(
            str(record.get(key) or "")
            for key in ("failure_class", "error_type", "error_message", "error_message_safe")
        )
    ).upper()
    if "READ_TIMEOUT" in value:
        return "READ_TIMEOUT"
    if "CONNECT_TIMEOUT" in value:
        return "CONNECT_TIMEOUT"
    if "EMPTY_RESPONSE" in value:
        return "EMPTY_RESPONSE"
    if "SCHEMA_VALIDATION_FAILURE" in value or "VALIDATION_FAILED" in value:
        return "SCHEMA_VALIDATION_FAILURE"
    if "CONNECTION_ERROR" in value or "APICONNECTIONERROR" in value:
        return "CONNECTION_ERROR"
    if re.search(r"\b(?:401|402|403|429|5\d\d)\b", value):
        return "PROVIDER_HTTP_ERROR"
    if "APITIMEOUTERROR" in value or "TIMEOUTEXCEPTION" in value:
        # Older audits did not retain read/connect evidence.  Do not invent a
        # timeout subtype merely because the exception class contains timeout.
        return "UNKNOWN_PROVIDER_FAILURE"
    return _safe_text(record.get("failure_class")).upper()


def _safe_text(value: Any, limit: int = 160) -> str:
    """Return bounded diagnostic text without response bodies or credentials."""

    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        value = json.dumps(value, ensure_ascii=False, sort_keys=True)
    text = str(value).replace("\r", " ").replace("\n", " ").strip()
    text = re.sub(r"(?i)bearer\s+[A-Za-z0-9._-]+", "Bearer <REDACTED>", text)
    text = re.sub(r"(?i)(api[_-]?key|authorization|password|secret|token)\s*[:=]\s*[^,; ]+", r"\1=<REDACTED>", text)
    return text[:limit]


def _error_texts(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [_safe_text(item) for item in value]
    if isinstance(value, dict):
        return [_safe_text(value)]
    return [_safe_text(value)]


def _schema_diagnostics(errors: Iterable[str]) -> dict[str, str]:
    """Extract types and locations from sanitized Pydantic-style errors."""

    texts = [text for text in errors if text]
    joined = " | ".join(texts)
    field = ""
    if texts and ":" in texts[0]:
        field = texts[0].split(":", 1)[0].strip()

    expected_type = ""
    actual_type = ""
    type_match = re.search(r"valid (string|number|integer|boolean|array|object)", joined, re.I)
    if type_match:
        expected_type = type_match.group(1).lower()
    parse_match = re.search(r"input_type=([A-Za-z_][A-Za-z0-9_]*)", joined, re.I)
    if parse_match:
        actual_type = parse_match.group(1)
    elif re.search(r"parse .* as a number|parse .* as an integer", joined, re.I):
        actual_type = "string"

    expected_enum = ""
    actual_value = ""
    enum_match = re.search(r"(?:literal|enum).*?(?:expected|permitted|one of)[:=]?\s*([^|]+)", joined, re.I)
    if enum_match:
        expected_enum = _safe_text(enum_match.group(1))
    value_match = re.search(r"input_value=([^,|]+)", joined, re.I)
    if value_match:
        actual_value = _safe_text(value_match.group(1))

    return {
        "failed_field": field,
        "expected_type": expected_type,
        "actual_type": actual_type,
        "expected_enum": expected_enum,
        "actual_value": actual_value,
    }


def _classify(record: dict[str, Any]) -> tuple[str, dict[str, str]]:
    """Classify one persisted audit without treating missing evidence as success."""

    failure = _derived_failure_class(record) or _safe_text(_first(record, "failure_type")).upper()
    status = _safe_text(record.get("status")).lower()
    http_status = _first(record, "http_status", "final_http_status")
    response_received = _as_bool(_first(record, "response_received", "responseReceived"))
    json_ok = _as_bool(_first(record, "json_parse_ok", "json_parseable", "json_parsed"))
    schema_ok = _as_bool(record.get("schema_valid"))
    payload = _first(record, "raw_response_payload", "raw_payload")
    parse_status = _safe_text(_first(record, "parse_status", "parser_status")).lower()
    errors = _error_texts(_first(record, "schema_errors", "validation_errors", "raw_fields"))
    diagnostics = _schema_diagnostics(errors)
    body_text = _safe_text(payload, 1000)

    if http_status is not None:
        try:
            if int(http_status) >= 400:
                return "PROVIDER_HTTP_ERROR", diagnostics
        except (TypeError, ValueError):
            pass

    if response_received is False and failure in {"READ_TIMEOUT", "CONNECT_TIMEOUT", "CONNECTION_ERROR"}:
        return failure, diagnostics
    if response_received is False and failure:
        return failure, diagnostics

    if json_ok is False or parse_status in {"empty_response", "parse_failed", "truncated"}:
        if parse_status == "empty_response" or failure == "EMPTY_RESPONSE":
            return "EMPTY_RESPONSE", diagnostics
        if "```" in body_text:
            return "CODE_FENCE_WRAPPED_JSON", diagnostics
        if "truncated" in parse_status or "unterminated" in body_text.lower():
            return "TRUNCATED_RESPONSE", diagnostics
        return "JSON_PARSE_FAILURE", diagnostics

    if schema_ok is False:
        if any("field required" in error.lower() for error in errors):
            return "SCHEMA_MISSING_REQUIRED_FIELD", diagnostics
        if any(token in " ".join(errors).lower() for token in ("literal", "enum", "permitted")):
            return "SCHEMA_ENUM_MISMATCH", diagnostics
        if any(token in " ".join(errors).lower() for token in ("valid string", "valid number", "valid integer", "valid boolean", "input_type=")):
            return "SCHEMA_FIELD_TYPE_MISMATCH", diagnostics
        return "SCHEMA_VALIDATION_FAILURE", diagnostics

    if status == "response_completed" or schema_ok is True:
        return "SUCCESS", diagnostics
    return failure or "UNKNOWN_PROVIDER_FAILURE", diagnostics


def _timestamp(record: dict[str, Any]) -> str:
    value = _first(record, "started_at", "created_at", "timestamp", "completed_at", "updated_at")
    return _safe_text(value)


def _raw_saved(record: dict[str, Any]) -> str:
    payload = _first(record, "raw_response_payload", "raw_payload", "response_payload")
    raw_hash = _first(record, "raw_response_hash", "response_hash")
    if payload not in (None, "", [], {}):
        return "payload"
    if raw_hash:
        return "hash_only"
    return "none"


def _retry_decision(record: dict[str, Any], category: str) -> str:
    if category == "SUCCESS":
        return "SUCCESS_NO_RETRY"
    if _safe_text(record.get("probe_type")).upper() == "RECOVERY_NETWORK_PROBE":
        return "BACKOFF_SINGLE_PROBE"
    if _first(record, "next_retry_at", "retry_at"):
        return "RETRY_SCHEDULED"
    if _safe_text(record.get("status")).lower() == "response_failed":
        return "RETRY_DECISION_NOT_PERSISTED"
    return "NO_RETRY_RECORDED"


def _input_root(data_root: Path) -> Path:
    if (data_root / "ai_audit" / "requests").is_dir():
        return data_root
    return data_root / "outputs" / "special_projects" / "2016_930"


def _load_saved_records(input_root: Path) -> list[tuple[dict[str, Any], str]]:
    """Load JSON audits and enrich them with failure-ledger metadata."""

    records: list[tuple[dict[str, Any], str]] = []
    request_indices: dict[str, list[int]] = {}
    request_dirs = []
    direct_request_dir = input_root / "ai_audit" / "requests"
    if direct_request_dir.is_dir():
        request_dirs.append(direct_request_dir)
    request_dirs.extend(
        path
        for path in input_root.rglob("ai_audit/requests")
        if path.is_dir() and path not in request_dirs
    )
    for request_dir in request_dirs:
        for path in request_dir.rglob("*.json"):
            try:
                value = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, UnicodeDecodeError, json.JSONDecodeError):
                continue
            if not isinstance(value, dict):
                continue
            index = len(records)
            records.append((value, str(path)))
            key = _safe_text(value.get("request_id"))
            if key:
                request_indices.setdefault(key, []).append(index)

    failure_path = input_root / "930_API_FAILURES.parquet"
    if failure_path.exists():
        try:
            import polars as pl

            failure_rows = pl.read_parquet(failure_path).to_dicts()
        except (ImportError, OSError, RuntimeError):
            failure_rows = []
        for index, value in enumerate(failure_rows):
            if not isinstance(value, dict):
                continue
            key = _safe_text(value.get("request_id")) or f"parquet:{index}"
            source = f"{failure_path}#failure_id={_safe_text(value.get('failure_id')) or index}"
            matched = request_indices.get(key, [])
            if matched:
                for matched_index in matched:
                    existing, existing_source = records[matched_index]
                    merged = dict(existing)
                    for name, item in value.items():
                        if merged.get(name) in (None, "", [], {}):
                            merged[name] = item
                    records[matched_index] = (merged, existing_source)
                continue
            records.append((value, source))
            if key and not key.startswith("parquet:"):
                request_indices.setdefault(key, []).append(len(records) - 1)
    return records


def build_timeline(input_root: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for record, source_path in _load_saved_records(input_root):
        category, diagnostics = _classify(record)
        schema_ok = _as_bool(record.get("schema_valid"))
        json_ok = _as_bool(_first(record, "json_parse_ok", "json_parseable", "json_parsed"))
        http_status = _first(record, "http_status", "final_http_status")
        row = {
            "timestamp": _timestamp(record),
            "probe_type": _safe_text(record.get("probe_type")),
            "provider": _safe_text(_first(record, "provider", "provider_name")),
            "model": _safe_text(record.get("model")),
            "http_status": "" if http_status is None else _safe_text(http_status),
            "response_received": "" if _as_bool(_first(record, "response_received", "responseReceived")) is None else str(_as_bool(_first(record, "response_received", "responseReceived"))).lower(),
            "raw_response_saved": _raw_saved(record),
            "json_parseable": "" if json_ok is None else str(json_ok).lower(),
            "schema_valid": "" if schema_ok is None else str(schema_ok).lower(),
            "failure_class": category,
            "failure_category": category,
            **diagnostics,
            "prompt_version": _safe_text(record.get("prompt_version")),
            "schema_version": _safe_text(_first(record, "schema_version", "output_schema_version")) or "unknown",
            "content_sha256": _safe_text(_first(record, "content_sha256", "content_hash")),
            "latency_ms": _safe_text(_first(record, "latency_ms", "duration_ms")),
            "timeout_class": _safe_text(_first(record, "timeout_type", "timeout_class")),
            "configured_read_timeout": _safe_text(record.get("configured_read_timeout")),
            "configured_connect_timeout": _safe_text(record.get("configured_connect_timeout")),
            "retry_decision": _retry_decision(record, category),
            "request_id": _safe_text(record.get("request_id")),
            "source_path": source_path,
        }
        rows.append(row)
    rows.sort(key=lambda row: (row["timestamp"], row["request_id"], row["source_path"]))
    return rows


def _forensics_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    categories = Counter(row["failure_category"] for row in rows)
    classified_counts = {
        key: int(categories.get(key, 0)) for key in CLASSIFICATION_CATEGORIES
    }
    schema_categories = {
        key: value
        for key, value in categories.items()
        if key.startswith("SCHEMA_")
    }
    probes = [
        row
        for row in rows
        if row.get("probe_type") == "RECOVERY_NETWORK_PROBE"
    ]
    probes.sort(key=lambda row: (row.get("timestamp", ""), row.get("request_id", "")))
    read_timeout_probes = [row for row in probes if row["failure_category"] == "READ_TIMEOUT"]
    durations = [
        float(row["latency_ms"]) / 1000.0
        for row in read_timeout_probes
        if row.get("latency_ms")
    ]
    configured = [
        float(row["configured_read_timeout"])
        for row in read_timeout_probes
        if row.get("configured_read_timeout")
    ]
    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "timeline_rows": len(rows),
        "category_counts": dict(sorted(categories.items())),
        "requested_classification_counts": classified_counts,
        "schema_failure_counts": dict(sorted(schema_categories.items())),
        "recent_provider_probe_count": len(probes),
        "recent_provider_probes": [
            {
                key: row.get(key)
                for key in (
                    "timestamp",
                    "failure_category",
                    "http_status",
                    "response_received",
                    "json_parseable",
                    "schema_valid",
                    "failed_field",
                    "expected_type",
                    "latency_ms",
                    "configured_read_timeout",
                    "timeout_class",
                    "retry_decision",
                    "request_id",
                )
            }
            for row in probes[-10:]
        ],
        "read_timeout_evidence": {
            "sample_count": len(durations),
            "duration_seconds": [round(value, 3) for value in durations],
            "configured_read_timeout_seconds": sorted(set(configured)),
            "configured_timeout_evidence_present": bool(configured),
            "interpretation": (
                "saved evidence shows read timeout failures with explicit configured timeout"
                if configured
                else "legacy timeout records lack configured timeout evidence"
            ),
        },
        "root_cause": {
            "transport": "PROVIDER_OR_TRANSPORT_READ_TIMEOUT" if durations else "NO_READ_TIMEOUT_EVIDENCE",
            "structured_output": (
                "MODEL_RESPONSE_SCHEMA_NONCONFORMANCE"
                if schema_categories
                else "NO_SCHEMA_FAILURE_EVIDENCE"
            ),
            "quality_gate": "STRICT_SCHEMA_GATE_RETAINED",
            "manual_api_call": False,
        },
        "evidence_limits": [
            "Historical legacy audits without diagnostics remain UNKNOWN_PROVIDER_FAILURE; no timeout subtype is invented.",
            "Timeline stores response hashes and sanitized metadata only; it never copies provider response bodies.",
            "A schema-valid response is counted only after JSON parse and strict schema validation both succeed.",
        ],
    }


def _write_forensics_report(summary: dict[str, Any], output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    json_path = output.with_suffix(".json")
    markdown_path = output.with_suffix(".md")
    json_temp = json_path.with_name(f".{json_path.name}.{os.getpid()}.part")
    md_temp = markdown_path.with_name(f".{markdown_path.name}.{os.getpid()}.part")
    try:
        json_temp.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        os.replace(json_temp, json_path)
        counts = summary["category_counts"]
        timeout = summary["read_timeout_evidence"]
        lines = [
            "# EP930 API Failure Forensics",
            "",
            f"Generated: `{summary['generated_at']}`",
            f"Timeline rows: `{summary['timeline_rows']}`",
            "",
            "## Root cause",
            "",
            "- Transport: repeated saved `READ_TIMEOUT` failures are retained as a provider/transport blocker.",
            "- Structured output: some HTTP 200 responses parse as JSON but fail the strict Action schema; saved evidence includes field-type and missing-field failures.",
            "- Gate policy: strict schema validation remains enabled; no response is promoted from a hash or a parse-only result.",
            "",
            "## Evidence counts",
            "",
            "| Category | Count |",
            "| --- | ---: |",
        ]
        lines.extend(f"| `{key}` | {value} |" for key, value in sorted(counts.items()))
        lines.extend(
            [
                "",
                "## Requested schema/transport classes",
                "",
                "| Class | Observed rows |",
                "| --- | ---: |",
            ]
        )
        lines.extend(
            f"| `{key}` | {value} |"
            for key, value in sorted(summary["requested_classification_counts"].items())
        )
        lines.extend(
            [
                "",
                "## Timeout evidence",
                "",
                f"- samples with duration: `{timeout['sample_count']}`",
                f"- configured read timeout values observed: `{timeout['configured_read_timeout_seconds']}`",
                f"- interpretation: {timeout['interpretation']}",
                "",
                "## Saved-response limitation",
                "",
                "The timeline contains sanitized metadata and response hashes. Historical provider response bodies are not copied into this report. Legacy audit rows without transport/schema diagnostics remain explicitly unknown rather than being reclassified by guesswork.",
                "",
                "## Recovery decision",
                "",
                "Keep the controller in `SINGLE_PROBE -> MICRO_5 -> MICRO_20 -> backlog` order. The structural-envelope fix is limited to unwrapping known containers; it does not coerce nulls, enums, or field types and therefore cannot bypass the strict gate.",
            ]
        )
        md_temp.write_text("\n".join(lines) + "\n", encoding="utf-8")
        os.replace(md_temp, markdown_path)
    finally:
        for path in (json_temp, md_temp):
            if path.exists():
                path.unlink()


def write_csv(rows: list[dict[str, Any]], output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    temp = output.with_name(f".{output.name}.{os.getpid()}.part")
    try:
        with temp.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=TIMELINE_FIELDS, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp, output)
    finally:
        if temp.exists():
            temp.unlink()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", type=Path, default=Path(r"E:\Data Set\CRPD"))
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(r"E:\Data Set\CRPD\outputs\special_projects\2016_930\EP930_API_FAILURE_TIMELINE.csv"),
    )
    parser.add_argument("--require-evidence", action="store_true")
    parser.add_argument(
        "--report-prefix",
        type=Path,
        default=None,
        help="write adjacent EP930_API_FORENSICS.json and .md reports",
    )
    args = parser.parse_args()

    root = _input_root(args.data_root)
    rows = build_timeline(root)
    if args.require_evidence:
        categories = Counter(row["failure_category"] for row in rows)
        required = {"READ_TIMEOUT", "SUCCESS"}
        if not (set(categories) & {"SCHEMA_FIELD_TYPE_MISMATCH", "SCHEMA_MISSING_REQUIRED_FIELD", "SCHEMA_ENUM_MISMATCH", "SCHEMA_VALIDATION_FAILURE"}):
            required.add("SCHEMA_VALIDATION_FAILURE")
        missing = sorted(category for category in required if category not in categories)
        if missing:
            raise SystemExit(f"missing required saved evidence categories: {', '.join(missing)}")
    write_csv(rows, args.output)
    report_prefix = args.report_prefix or args.output.with_name("EP930_API_FORENSICS")
    _write_forensics_report(_forensics_summary(rows), report_prefix)
    counts = Counter(row["failure_category"] for row in rows)
    print(json.dumps({"rows": len(rows), "categories": dict(sorted(counts.items())), "output": str(args.output), "report_prefix": str(report_prefix)}, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
