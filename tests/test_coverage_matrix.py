from __future__ import annotations

from datetime import date

import polars as pl

from policydb.coverage import build_city_source_month_coverage
from policydb.settings import Settings


def test_coverage_grid_never_turns_not_scanned_into_zero(tmp_path, monkeypatch):
    root = tmp_path / "repo"
    curated = root / "data/curated"
    curated.mkdir(parents=True)
    pl.DataFrame(
        [
            {
                "city_id": "C1",
                "city_name": "测试市",
                "province_name": "测试省",
            }
        ]
    ).write_parquet(curated / "cities_105.parquet")
    monkeypatch.setattr(
        "policydb.coverage.build_source_matrix",
        lambda settings: pl.DataFrame(
            schema={
                "source_id": pl.String,
                "city_id": pl.String,
                "agency_type": pl.String,
            }
        ),
    )
    result = build_city_source_month_coverage(
        Settings(root=root),
        start=date(2026, 1, 1),
        end=date(2026, 2, 1),
    )
    frame = pl.read_csv(root / "outputs/coverage/city_source_month_coverage.csv")
    assert result["coverage_cells"] == 8
    assert frame["coverage_status"].unique().to_list() == ["not_scanned"]
    assert frame["policy_count"].sum() == 0
    assert result["complete_cells"] == 0
