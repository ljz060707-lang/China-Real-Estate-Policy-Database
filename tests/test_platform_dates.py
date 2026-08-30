"""CRPD platform date-parser regressions — deterministic date extraction."""
from __future__ import annotations

from datetime import date, datetime

from policydb.ingest.excel import parse_date


def test_parse_date_variants():
    assert parse_date("2022-01-05") == date(2022, 1, 5)
    assert parse_date("2022/1/5") == date(2022, 1, 5)
    assert parse_date("2022.01.05") == date(2022, 1, 5)
    assert parse_date("2022年1月5日") == date(2022, 1, 5)
    assert parse_date("发布于 2022-01-05 10:30:00") == date(2022, 1, 5)


def test_parse_date_typed_values():
    assert parse_date(datetime(2022, 1, 5, 8, 30)) == date(2022, 1, 5)
    assert parse_date(date(2022, 1, 5)) == date(2022, 1, 5)


def test_parse_date_invalid_is_none():
    assert parse_date(None) is None
    assert parse_date("") is None
    assert parse_date("无日期") is None
    assert parse_date("2022-13-40") is None
    assert parse_date("2022") is None
