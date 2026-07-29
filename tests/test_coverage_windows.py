from datetime import date

from policydb.coverage import record_source_window
from policydb.settings import Settings


def test_page_cap_keeps_coverage_window_partial(tmp_path):
    settings = Settings(root=tmp_path)
    settings.curated.mkdir(parents=True)
    row = record_source_window(
        run_id="R1",
        source_id="S1",
        period_start=date(2021, 1, 1),
        period_end=date(2021, 1, 31),
        scan_method="historical_105",
        candidate_count=20,
        fetched_count=20,
        policy_count=0,
        error_count=0,
        page_count=20,
        completion_evidence={
            "pagination_exhausted": False,
            "stop_reason": "page_limit",
            "exhaustive": False,
        },
        settings=settings,
    )
    assert row["coverage_status"] == "partial"
    assert row["is_complete"] is False
