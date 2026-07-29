from __future__ import annotations

import json
from datetime import UTC, datetime

import duckdb

from policydb.settings import Settings


def run_coverage_audit(
    settings: Settings | None = None, *, sample_size: int = 30
) -> dict:
    settings = settings or Settings.discover()
    with duckdb.connect(str(settings.database), read_only=True) as con:
        source_completeness = con.execute(
            "SELECT count(*) FILTER (WHERE required_level='required') required_sources,"
            "count(*) FILTER (WHERE required_level='required' AND scope_type<>'unknown') mapped_required_sources,"
            "count(*) FILTER (WHERE required_level='required' AND crawl_enabled) enabled_required_sources "
            "FROM source_registry"
        ).fetchone()
        complete_windows = con.execute(
            "SELECT count(*) FROM crawl_source_windows WHERE is_complete"
        ).fetchone()[0]
        recall_rows = con.execute(
            "SELECT source_id,city_id,period_start,candidate_count,fetched_count,policy_count,"
            "CASE WHEN candidate_count=0 THEN 1.0 ELSE fetched_count::DOUBLE/candidate_count END recall_proxy "
            "FROM crawl_source_windows WHERE is_complete ORDER BY finished_at DESC LIMIT ?",
            [sample_size],
        ).fetchall()
        zero_rows = con.execute(
            "SELECT source_id,city_id,period_start,completion_evidence "
            "FROM crawl_source_windows WHERE coverage_status='complete_confirmed_zero' "
            "ORDER BY finished_at DESC LIMIT ?",
            [sample_size],
        ).fetchall()
        conflicts = con.execute(
            "SELECT count(*) FROM field_confidence WHERE conflict_status<>'none'"
        ).fetchone()[0]
    report = {
        "created_at": datetime.now(UTC).isoformat(),
        "sample_size": sample_size,
        "source_completeness": {
            "required_sources": source_completeness[0],
            "mapped_required_sources": source_completeness[1],
            "enabled_required_sources": source_completeness[2],
        },
        "monthly_sample_recall": {
            "status": "evaluated" if recall_rows else "not_evaluated_no_complete_windows",
            "sample_count": len(recall_rows),
            "rows": recall_rows,
        },
        "zero_policy_sample_audit": {
            "status": "evaluated" if zero_rows else "not_evaluated_no_confirmed_zero_windows",
            "sample_count": len(zero_rows),
            "rows": zero_rows,
        },
        "cross_source_conflicts": conflicts,
        "complete_window_count": complete_windows,
    }
    output = settings.root / "outputs" / "validation" / "v2_coverage_audit.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return report

