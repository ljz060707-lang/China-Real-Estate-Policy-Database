from datetime import date

from policydb.coverage import record_source_window
from policydb.settings import Settings


def test_only_exhaustive_error_free_scan_can_confirm_zero(tmp_path):
    settings = Settings(root=tmp_path)
    settings.curated.mkdir(parents=True)
    row = record_source_window(
        run_id="R1",
        source_id="S1",
        period_start=date(2021, 1, 1),
        period_end=date(2021, 1, 31),
        scan_method="historical_105",
        candidate_count=0,
        fetched_count=0,
        policy_count=0,
        error_count=0,
        page_count=3,
        completion_evidence={"pagination_exhausted": True, "exhaustive": True},
        settings=settings,
    )
    assert row["coverage_status"] == "complete_confirmed_zero"
