from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[1]
MONITOR = REPO_ROOT / "scripts" / "monitor_recent_5y_city_year.py"


def _run_monitor(data_root: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(MONITOR), "--data-root", str(data_root), "--once"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=False,
    )


def test_monitor_once_renders_dynamic_city_year_bars_from_real_parquet(tmp_path: Path) -> None:
    curated = tmp_path / "curated"
    automation = tmp_path / "automation"
    curated.mkdir()
    automation.mkdir()
    pl.DataFrame(
        [
            {
                "city_id": "CITY_A",
                "city_name": "北京市",
                "province_name": "北京市",
                "year": 2021,
                "overall_completion_pct": 100.0,
                "status": "certified_complete",
            },
            {
                "city_id": "CITY_A",
                "city_name": "北京市",
                "province_name": "北京市",
                "year": 2022,
                "overall_completion_pct": 50.0,
                "status": "running",
            },
            {
                "city_id": "CITY_B",
                "city_name": "上海市",
                "province_name": "上海市",
                "year": 2021,
                "overall_completion_pct": 0.0,
                "status": "not_started",
            },
        ]
    ).write_parquet(curated / "city_year_progress.parquet")
    pl.DataFrame(
        [
            {
                "city_id": "CITY_A",
                "city_name": "北京市",
                "province_name": "北京市",
                "start_date": "2021-01-01",
                "status": "complete_unverified",
                "pagination_exhausted": True,
                "fetched": 3,
                "document_versions": 3,
                "unique_candidate_count": 4,
                "fetch_attempted": 4,
                "failed": 0,
            },
            {
                "city_id": "CITY_A",
                "city_name": "北京市",
                "province_name": "北京市",
                "start_date": "2021-02-01",
                "status": "pending",
                "pagination_exhausted": False,
                "fetched": 1,
                "document_versions": 1,
                "unique_candidate_count": 2,
                "fetch_attempted": 1,
                "failed": 0,
            },
        ]
    ).write_parquet(curated / "crawl_shards.parquet")
    (automation / "RECENT_5Y_FULL_CRAWL_STATE.json").write_text(
        json.dumps(
            {
                "run_id": "RECENT5Y_TEST",
                "status": "RUNNING",
                "current_city": "北京市",
                "current_city_index": 1,
                "city_total": 105,
                "fetch_concurrency": 6,
                "per_host_concurrency": 1,
                "api_concurrency": 4,
            }
        ),
        encoding="utf-8",
    )

    result = _run_monitor(tmp_path)

    assert result.returncode == 0, result.stderr
    assert "CITY × YEAR PROGRESS" in result.stdout
    assert "2021" in result.stdout and "2022" in result.stdout
    assert "北京市" in result.stdout and "上海市" in result.stdout
    assert "100.0%" in result.stdout
    assert "50.0%" in result.stdout
    assert "NOT_STARTED" in result.stdout
    assert "RECENT5Y_TEST" in result.stdout
    assert "fetch_concurrency=6" in result.stdout
    assert "per_host_concurrency=1" in result.stdout
    assert "api_concurrency=4" in result.stdout
    assert "CRAWL CHECKPOINT" in result.stdout
    assert "docs=4" in result.stdout
    assert "strict=100.0%" in result.stdout


def test_monitor_once_reports_missing_snapshot_without_fabricating_progress(tmp_path: Path) -> None:
    result = _run_monitor(tmp_path)

    assert result.returncode == 0, result.stderr
    assert "CITY × YEAR PROGRESS" in result.stdout
    assert "NOT_AVAILABLE" in result.stdout
    assert "0.0%" not in result.stdout


def test_monitor_counts_reused_document_versions_as_documents(tmp_path: Path) -> None:
    curated = tmp_path / "curated"
    automation = tmp_path / "automation"
    curated.mkdir()
    automation.mkdir()
    pl.DataFrame(
        [
            {
                "city_id": "CITY_A",
                "city_name": "北京市",
                "province_name": "北京市",
                "year": 2021,
                "overall_completion_pct": None,
                "status": "running",
            }
        ]
    ).write_parquet(curated / "city_year_progress.parquet")
    pl.DataFrame(
        [
            {
                "city_id": "CITY_A",
                "city_name": "北京市",
                "province_name": "北京市",
                "start_date": "2021-01-01",
                "status": "unchanged",
                "pagination_exhausted": True,
                "fetched": 0,
                "document_versions": 7,
                "unique_candidate_count": 7,
                "failed": 0,
            }
        ]
    ).write_parquet(curated / "crawl_shards.parquet")
    (automation / "RECENT_5Y_FULL_CRAWL_STATE.json").write_text(
        json.dumps({"run_id": "REUSE_TEST", "status": "RUNNING"}),
        encoding="utf-8",
    )

    result = _run_monitor(tmp_path)

    assert result.returncode == 0, result.stderr
    year_line = next(line for line in result.stdout.splitlines() if line.startswith("2021:"))
    assert "docs=7" in year_line
