import importlib.util
from pathlib import Path

import polars as pl

from policydb.exhaustive import pending_fetch_count, text_extraction_percentage


def test_reuse_counts_as_available_not_pending_fetch():
    assert pending_fetch_count({"unique_candidate_count": 7, "fetched": 0, "document_versions": 7, "failed": 0}) == 0


def test_pagination_is_not_text_extraction_evidence():
    assert text_extraction_percentage([{"pagination_exhausted": True}]) is None
    assert text_extraction_percentage([{"completion_evidence_json": '{"text_extraction":{"main_parsed":3,"main_pending":1}}'}]) == 75


def test_monitor_excludes_split_parents_and_out_of_range(tmp_path):
    spec = importlib.util.spec_from_file_location("monitor", Path(__file__).resolve().parents[1] / "scripts/monitor_recent_5y_city_year.py")
    monitor = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(monitor)
    rows = []
    for status, start, end in [("split_parent", "2021-09-01", "2021-09-30"), ("pending", "2021-01-01", "2021-01-31"), ("complete", "2021-09-01", "2021-09-15")]:
        rows.append({"city_id": "C", "city_name": "城市", "province_name": "省", "start_date": start, "end_date": end, "status": status, "pagination_exhausted": status == "complete", "fetched": 1, "document_versions": 1})
    path = tmp_path / "shards.parquet"
    pl.DataFrame(rows).write_parquet(path)
    result, error = monitor._load_shard_progress(path, start_date="2021-09-01", end_date="2026-09-12")
    assert error is None
    assert result[("C", 2021)]["shard_total"] == 1
    assert result[("C", 2021)]["checkpoint_pct"] == 100
