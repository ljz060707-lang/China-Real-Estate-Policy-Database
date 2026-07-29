from datetime import date

from policydb.coverage import record_source_window
from policydb.settings import Settings


def test_complete_window_writes_city_policy_count_and_exhaustive(tmp_path):
    settings = Settings(root=tmp_path)
    settings.curated.mkdir(parents=True)
    row = record_source_window(
        run_id="R",
        source_id="S",
        city_id="CITY",
        period_start=date(2026, 1, 1),
        period_end=date(2026, 1, 31),
        scan_method="list",
        candidate_count=2,
        fetched_count=2,
        policy_count=1,
        error_count=0,
        page_count=2,
        completion_evidence={"exhaustive": True},
        settings=settings,
    )
    assert row["city_id"] == "CITY" and row["policy_count"] == 1 and row["is_complete"]
