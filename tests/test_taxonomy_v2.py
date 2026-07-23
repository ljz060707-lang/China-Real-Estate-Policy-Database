from __future__ import annotations

import polars as pl

from policydb.settings import Settings
from policydb.taxonomy_v2 import (
    build_cicc_mapping,
    classify_action,
    load_taxonomy,
    materialize_action_classifications,
)


def test_taxonomy_has_five_primary_categories(root):
    taxonomy = load_taxonomy(Settings.discover(root))
    assert set(taxonomy["primary_categories"]) == {"D", "S", "F", "H", "G"}
    assert taxonomy["primary_categories"]["D"]["secondary"]["D06"] == "住房公积金"


def test_action_classification_uses_specific_keyword_before_instrument():
    result = classify_action("financing", "建立房地产项目白名单并给予融资支持")
    assert result[:3] == ("F", "F02", "credit_finance")


def test_materialize_classification_preserves_evidence(tmp_path):
    settings = Settings(root=tmp_path, curated_path=tmp_path / "curated")
    settings.curated.mkdir()
    pl.DataFrame(
        [
            {
                "action_id": "A1",
                "record_id": "R1",
                "instrument": "provident_fund",
                "direction": "loosening",
                "clause_text": "提高住房公积金贷款额度",
                "evidence_text": "提高住房公积金贷款额度",
                "evidence_start": 0,
                "evidence_end": 12,
            }
        ]
    ).write_parquet(settings.curated / "policy_actions.parquet")
    result = materialize_action_classifications(settings)
    frame = pl.read_parquet(settings.curated / "policy_classifications.parquet")
    assert result["coverage"] == 1.0
    assert frame[0, "secondary_category"] == "D06"
    assert frame[0, "evidence_text"] == "提高住房公积金贷款额度"


def test_cicc_mapping_keeps_ambiguous_topic_for_review(tmp_path):
    settings = Settings(root=tmp_path, curated_path=tmp_path / "curated")
    settings.curated.mkdir()
    pl.DataFrame(
        [
            {
                "source_sheet": "T1 房地产政策目录",
                "legacy_category": "公积金政策放松、购房补贴",
            }
        ]
    ).write_parquet(settings.curated / "records.parquet")
    report = build_cicc_mapping(settings)
    assert report["unmapped_topics"] == 1
    assert (tmp_path / "outputs/taxonomy/unmapped_topics.csv").exists()
