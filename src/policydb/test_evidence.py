"""Reliable, secret-safe interpretation of pytest process evidence.

The source-completion runner may receive a valid JUnit report, a malformed
JUnit report, or no JUnit report at all.  The process exit code and pytest's
terminal summary are authoritative when they are available; JUnit is a
secondary structured source and its parse failure must remain visible without
turning an otherwise successful run into ``unknown``.
"""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

_COUNT_RE = {
    name: re.compile(rf"(?<!\d)(?P<value>\d+)\s+{name}\b", re.IGNORECASE)
    for name in ("passed", "failed", "error", "errors", "skipped", "xfailed", "xpassed", "warnings")
}
_COLLECTED_RE = re.compile(r"(?<!\d)(?P<value>\d+)\s+(?:items?|tests?)\s+collected\b|\bcollected\s+(?P<value2>\d+)\s+(?:items?|tests?)?", re.IGNORECASE)
_DURATION_RE = re.compile(r"\bin\s+(?P<seconds>\d+(?:\.\d+)?)s\b", re.IGNORECASE)


def _read_text(path: Path | str | None) -> tuple[str, str | None]:
    if not path:
        return "", None
    candidate = Path(path)
    if not candidate.exists():
        return "", "missing"
    try:
        return candidate.read_text(encoding="utf-8-sig", errors="replace"), None
    except OSError as exc:
        return "", f"read_error:{type(exc).__name__}"


def _count_from_text(text: str, name: str) -> int | None:
    match = _COUNT_RE[name].search(text or "")
    if not match:
        return None
    return int(match.group("value"))


def _pytest_stdout_counts(stdout: str) -> dict[str, int | float | None]:
    counts: dict[str, int | float | None] = {
        name: _count_from_text(stdout, name)
        for name in ("passed", "failed", "error", "errors", "skipped", "xfailed", "xpassed", "warnings")
    }
    counts["errors"] = counts["errors"] if counts["errors"] is not None else counts["error"]
    counts.pop("error", None)
    collected = _COLLECTED_RE.search(stdout or "")
    counts["collected"] = (
        int(collected.group("value") or collected.group("value2"))
        if collected
        else None
    )
    duration = _DURATION_RE.search(stdout or "")
    counts["duration_seconds"] = float(duration.group("seconds")) if duration else None
    return counts


def _junit_counts(path: Path | str | None) -> tuple[dict[str, int | float | None], str, str | None]:
    if not path:
        return {}, "not_provided", None
    candidate = Path(path)
    if not candidate.exists():
        return {}, "missing", None
    try:
        root = ET.parse(candidate).getroot()
    except (ET.ParseError, OSError) as exc:
        # Keep the parser error as evidence, but do not include XML content or
        # request headers in any artifact.
        return {}, "malformed_xml", f"{type(exc).__name__}: {str(exc)[:500]}"

    suites = [root, *root.findall(".//testsuite")]
    testcases = root.findall(".//testcase")
    def attr_int(name: str) -> int | None:
        values: list[int] = []
        for suite in suites:
            value = suite.attrib.get(name)
            if value is None:
                continue
            try:
                values.append(int(float(value)))
            except (TypeError, ValueError):
                continue
        return sum(values) if values else None

    tests = attr_int("tests")
    failures = attr_int("failures")
    errors = attr_int("errors")
    skipped = attr_int("skipped")
    if testcases:
        failures = sum(1 for item in testcases if item.find("failure") is not None)
        errors = sum(1 for item in testcases if item.find("error") is not None)
        skipped = sum(1 for item in testcases if item.find("skipped") is not None)
        tests = len(testcases)
    failures = int(failures or 0)
    errors = int(errors or 0)
    skipped = int(skipped or 0)
    passed = max(0, int(tests or 0) - failures - errors - skipped) if tests is not None else None
    duration_values: list[float] = []
    for suite in suites:
        value = suite.attrib.get("time")
        if value is not None:
            try:
                duration_values.append(float(value))
            except (TypeError, ValueError):
                pass
    return (
        {
            "collected": tests,
            "passed": passed,
            "failed": failures,
            "errors": errors,
            "skipped": skipped,
            "duration_seconds": sum(duration_values) if duration_values else None,
        },
        "valid",
        None,
    )


def parse_pytest_status(
    *,
    stdout_text: str = "",
    stderr_text: str = "",
    exit_code: int | None = None,
    junit_path: Path | str | None = None,
    commit_sha: str | None = None,
    test_timestamp: str | None = None,
    test_suite: str | None = None,
    stdout_path: Path | str | None = None,
    stderr_path: Path | str | None = None,
) -> dict[str, Any]:
    """Return a tri-state pytest result with all available count evidence.

    ``passed`` requires a zero process exit code and no parsed failures or
    errors.  A non-zero exit code is ``failed`` even when a JUnit file is
    missing.  ``unknown`` is reserved for genuinely incomplete evidence.
    """

    stdout_counts = _pytest_stdout_counts(stdout_text)
    junit_counts, junit_status, junit_error = _junit_counts(junit_path)
    counts: dict[str, int | float | None] = dict(stdout_counts)
    if junit_status == "valid":
        # A valid structured report is more precise than the compact terminal
        # summary (which can count reruns or report an aggregate differently).
        for key in ("collected", "passed", "failed", "errors", "skipped", "duration_seconds"):
            if junit_counts.get(key) is not None:
                counts[key] = junit_counts[key]
    else:
        for key in ("collected", "passed", "failed", "errors", "skipped", "duration_seconds"):
            if counts.get(key) is None and junit_counts.get(key) is not None:
                counts[key] = junit_counts[key]
    if counts.get("collected") is None:
        observed = [counts.get(key) for key in ("passed", "failed", "errors", "skipped")]
        if any(value is not None for value in observed):
            counts["collected"] = sum(int(value or 0) for value in observed)

    failed_count = int(counts.get("failed") or 0)
    error_count = int(counts.get("errors") or 0)
    if exit_code is not None:
        result = "passed" if int(exit_code) == 0 and failed_count == 0 and error_count == 0 else "failed"
    elif failed_count or error_count:
        result = "failed"
    elif counts.get("passed") is not None and int(counts.get("passed") or 0) > 0:
        result = "passed"
    else:
        result = "unknown"

    mismatch_fields: list[str] = []
    for key in ("collected", "passed", "failed", "errors", "skipped"):
        if stdout_counts.get(key) is not None and junit_counts.get(key) is not None and stdout_counts[key] != junit_counts[key]:
            mismatch_fields.append(key)
    return {
        "test_result": result,
        "overall_status": result,
        "exit_code": exit_code,
        "collected": counts.get("collected"),
        "passed": counts.get("passed"),
        "failed": counts.get("failed"),
        "errors": counts.get("errors"),
        "skipped": counts.get("skipped"),
        "warnings": counts.get("warnings"),
        "duration_seconds": counts.get("duration_seconds"),
        "test_commit_sha": commit_sha,
        "test_timestamp": test_timestamp,
        "test_suite": test_suite,
        "stdout_path": str(stdout_path) if stdout_path else None,
        "stderr_path": str(stderr_path) if stderr_path else None,
        "junit_path": str(junit_path) if junit_path else None,
        "junit_parse_status": junit_status,
        "junit_parse_error": junit_error,
        "junit_counts": junit_counts or None,
        "stdout_counts": stdout_counts,
        "junit_consistency": "mismatch" if mismatch_fields else "consistent_or_unavailable",
        "junit_mismatch_fields": mismatch_fields,
        "stderr_nonempty": bool(stderr_text.strip()),
    }


def parse_pytest_report_file(path: Path | str) -> dict[str, Any]:
    """Parse a JSON test report that references stdout/stderr/JUnit files."""

    report_path = Path(path)
    try:
        import json

        payload = json.loads(report_path.read_text(encoding="utf-8-sig"))
    except (OSError, ValueError):
        return {"test_result": "unknown", "overall_status": "unknown", "report_path": str(report_path)}
    if not isinstance(payload, dict):
        return {"test_result": "unknown", "overall_status": "unknown", "report_path": str(report_path)}

    def resolve(value: Any) -> Path | None:
        if not value:
            return None
        candidate = Path(str(value))
        return candidate if candidate.is_absolute() else report_path.parent / candidate

    stdout_path = resolve(payload.get("stdout"))
    stderr_path = resolve(payload.get("stderr"))
    junit_path = resolve(payload.get("junit") or payload.get("junit_path"))
    stdout, _ = _read_text(stdout_path)
    stderr, _ = _read_text(stderr_path)
    parsed = parse_pytest_status(
        stdout_text=stdout,
        stderr_text=stderr,
        exit_code=payload.get("exit_code"),
        junit_path=junit_path,
        commit_sha=payload.get("test_commit_sha") or payload.get("commit_sha"),
        test_timestamp=payload.get("test_timestamp") or payload.get("timestamp"),
        test_suite=payload.get("test_suite"),
        stdout_path=stdout_path,
        stderr_path=stderr_path,
    )
    parsed["report_path"] = str(report_path)
    return parsed
