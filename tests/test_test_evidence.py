from pathlib import Path

from policydb.test_evidence import parse_pytest_report_file, parse_pytest_status


def test_valid_junit_and_zero_exit_are_passed(tmp_path: Path) -> None:
    junit = tmp_path / "junit.xml"
    junit.write_text(
        '<testsuite tests="3" failures="0" errors="0" skipped="1">'
        '<testcase classname="a" name="one" />'
        '<testcase classname="a" name="two" />'
        '<testcase classname="a" name="three"><skipped /></testcase>'
        "</testsuite>",
        encoding="utf-8",
    )
    result = parse_pytest_status(
        stdout_text="3 passed, 1 skipped in 0.42s",
        exit_code=0,
        junit_path=junit,
        commit_sha="abc123",
        test_timestamp="2026-08-06T00:00:00Z",
        test_suite="pytest -q",
    )
    assert result["test_result"] == "passed"
    assert result["collected"] == 3
    assert result["passed"] == 2
    assert result["skipped"] == 1
    assert result["junit_parse_status"] == "valid"
    assert result["test_commit_sha"] == "abc123"


def test_malformed_hostname_junit_does_not_override_valid_pytest_exit(tmp_path: Path) -> None:
    junit = tmp_path / "junit.xml"
    junit.write_text('<testsuite hostname="bad<hostname" tests="4" />', encoding="utf-8")
    result = parse_pytest_status(
        stdout_text="4 passed, 2 warnings in 1.00s",
        stderr_text="",
        exit_code=0,
        junit_path=junit,
    )
    assert result["test_result"] == "passed"
    assert result["passed"] == 4
    assert result["junit_parse_status"] == "malformed_xml"
    assert result["junit_parse_error"]


def test_missing_junit_with_valid_stdout_and_exit_is_passed(tmp_path: Path) -> None:
    result = parse_pytest_status(
        stdout_text="428 passed, 2 warnings in 176.87s (0:02:56)",
        exit_code=0,
        junit_path=tmp_path / "missing.xml",
    )
    assert result["test_result"] == "passed"
    assert result["collected"] == 428
    assert result["junit_parse_status"] == "missing"


def test_nonzero_exit_is_failed_even_without_junit(tmp_path: Path) -> None:
    result = parse_pytest_status(
        stdout_text="1 failed, 3 passed in 0.10s",
        exit_code=1,
        junit_path=tmp_path / "missing.xml",
    )
    assert result["test_result"] == "failed"
    assert result["failed"] == 1


def test_report_file_resolves_relative_evidence_paths(tmp_path: Path) -> None:
    (tmp_path / "stdout.txt").write_text("2 passed in 0.2s", encoding="utf-8")
    (tmp_path / "report.json").write_text(
        '{"stdout":"stdout.txt","exit_code":0,"test_commit_sha":"sha",'
        '"test_timestamp":"2026-08-06T00:00:00Z","test_suite":"pytest"}',
        encoding="utf-8",
    )
    result = parse_pytest_report_file(tmp_path / "report.json")
    assert result["test_result"] == "passed"
    assert result["passed"] == 2
    assert result["test_commit_sha"] == "sha"
