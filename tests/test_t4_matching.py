from __future__ import annotations

import polars as pl
import yaml


def test_excel_contract_distinguishes_two_t1_range_columns(root):
    config = yaml.safe_load(
        (root / "config" / "excel_sheet_map.yaml").read_text(encoding="utf-8")
    )
    fields = config["sheets"]["T1 房地产政策目录"]["fields"]
    assert fields["B"] == "jurisdiction_raw"
    assert fields["D"] == "policy_category_raw"


def test_t4_candidates_keep_score_evidence_and_review_status(root):
    candidates = pl.read_parquet(
        root / "data" / "curated" / "t4_match_candidates.parquet"
    )
    assert candidates.height == 2156
    assert candidates["t4_match_id"].n_unique() == candidates.height
    assert candidates["match_score"].is_between(0, 100).all()
    assert candidates["evidence"].is_not_null().all()
    assert set(candidates["review_status"].unique()) == {"approved", "pending"}


def test_t4_fuzzy_candidates_are_not_automatically_applied(root):
    candidates = pl.read_parquet(
        root / "data" / "curated" / "t4_match_candidates.parquet"
    )
    assert candidates.filter(pl.col("review_status") == "pending").height == 532

