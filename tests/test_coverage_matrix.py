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
    monkeypatch.setattr(
        "policydb.coverage.load_cities_105",
        lambda _settings: pl.DataFrame(
            [
                {
                    "city_id": "C1",
                    "city_name": "测试市",
                    "province_name": "测试省",
                }
            ]
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


def test_coverage_ignores_placeholder_city_parquet(tmp_path, monkeypatch):
    root = tmp_path / "repo"
    curated = root / "data/curated"
    curated.mkdir(parents=True)
    pl.DataFrame(
        [
            {"city_id": "PLACEHOLDER_1", "city_name": "占位一", "province_name": "占位省"},
            {"city_id": "PLACEHOLDER_2", "city_name": "占位二", "province_name": "占位省"},
        ]
    ).write_parquet(curated / "cities_105.parquet")
    canonical = pl.DataFrame(
        [
            {
                "city_id": f"CITY_{index:03d}",
                "city_name": f"城市{index}",
                "province_name": "测试省",
            }
            for index in range(105)
        ]
    )
    reference = root / "data" / "reference"
    reference.mkdir(parents=True)
    # The existence of the reviewed reference file selects the production
    # denominator; the loader is patched so this test remains deterministic.
    canonical.write_csv(reference / "cities_105.csv")
    monkeypatch.setattr("policydb.coverage.load_cities_105", lambda _settings: canonical)
    monkeypatch.setattr(
        "policydb.coverage.build_source_matrix",
        lambda _settings: pl.DataFrame(
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

    assert result["coverage_cells"] == 105 * 4 * 2
