from datetime import UTC, datetime

import polars as pl

from policydb.policy_pools import materialize_policy_pools
from policydb.settings import Settings


def test_pool_routes_no_action_to_automatic_extraction(tmp_path):
    curated = tmp_path / "data/curated"
    curated.mkdir(parents=True)
    now = datetime.now(UTC)
    pl.DataFrame(
        {
            "record_id": ["R1"],
            "title": ["政策"],
            "record_date": [now.date()],
            "official_status": ["official"],
            "full_text": ["正文" * 100],
        }
    ).write_parquet(curated / "records.parquet")
    result = materialize_policy_pools(Settings(root=tmp_path))
    assert result["pending_automatic_extraction"] == 1
    assert result["manual_review_required"] == 0
