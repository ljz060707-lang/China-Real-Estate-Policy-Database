from pathlib import Path


def test_province_time_coverage_views_are_declared():
    sql = Path("migrations/023_unified_coverage_pools.sql").read_text(encoding="utf-8")
    assert "v_province_month_coverage" in sql
    assert "v_province_year_coverage" in sql
    assert "v_source_role_coverage" in sql
