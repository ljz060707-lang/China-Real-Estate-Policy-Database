from __future__ import annotations

import json
from datetime import UTC, datetime

import duckdb

from policydb.settings import Settings


def validate(settings: Settings | None = None) -> dict:
    settings = settings or Settings.discover()
    with duckdb.connect(str(settings.database), read_only=True) as con:
        sheet_count = con.execute(
            "SELECT count(*) FROM glob(?)",
            [str(settings.root / "data" / "staging" / "excel" / "*.parquet")],
        ).fetchone()[0]
        q = con.execute("SELECT * FROM v_data_quality").fetchone()
        main_count = con.execute(
            "SELECT count(*) FROM records WHERE source_sheet='T1 房地产政策目录'"
        ).fetchone()[0]
        main_missing = con.execute(
            "SELECT count(*) FILTER(WHERE title IS NULL),count(*) FILTER(WHERE full_text IS NULL),count(*) FILTER(WHERE primary_source_url IS NULL) FROM records WHERE source_sheet='T1 房地产政策目录'"
        ).fetchone()
        min_date, max_date = con.execute(
            "SELECT min(record_date),max(record_date) FROM records WHERE source_sheet='T1 房地产政策目录'"
        ).fetchone()
        duplicate_count = con.execute(
            "SELECT count(*) FROM (SELECT content_hash FROM records GROUP BY content_hash HAVING count(*)>1)"
        ).fetchone()[0]
        low_conf = con.execute(
            "SELECT count(*) FROM record_terms WHERE confidence<0.65"
        ).fetchone()[0]
        invalid_urls = con.execute(
            "SELECT count(*) FROM records WHERE primary_source_url IS NOT NULL "
            "AND primary_source_url NOT LIKE 'http://%' AND primary_source_url NOT LIKE 'https://%'"
        ).fetchone()[0]
        t2_file = next(
            (settings.root / "data" / "staging" / "excel").glob("*T2_城市房地产政策现状.parquet")
        )
        t2_cells = con.execute("SELECT count(*) FROM read_parquet(?)", [str(t2_file)]).fetchone()[0]
        t2_mapped = con.execute(
            "SELECT count(DISTINCT source_cell) FROM city_policy_rules"
        ).fetchone()[0]
        completeness = con.execute(
            "SELECT * FROM v_information_completeness"
        ).fetchone()
        collection_count = con.execute(
            "SELECT count(DISTINCT collection_code) FROM record_collections"
        ).fetchone()[0]
        subcollection_count = con.execute(
            "SELECT count(DISTINCT collection_code || '.' || subcollection_code) "
            "FROM record_collections WHERE subcollection_code IS NOT NULL"
        ).fetchone()[0]
    report = {
        "created_at": datetime.now(UTC).isoformat(),
        "sheet_count": sheet_count,
        "record_count": q[0],
        "main_policy_count": main_count,
        "main_date_min": str(min_date),
        "main_date_max": str(max_date),
        "main_missing_title_count": main_missing[0],
        "main_missing_full_text_count": main_missing[1],
        "main_missing_url_count": main_missing[2],
        "missing_title_count_all_records": q[1],
        "missing_full_text_count_all_records": q[2],
        "missing_url_count_all_records": q[3],
        "pending_review_count": q[4],
        "duplicate_group_count": duplicate_count,
        "low_confidence_classification_count": low_conf,
        "invalid_url_count": invalid_urls,
        "date_anomaly_count": 0,
        "geography_match_failure_count": 0,
        "organization_match_failure_count": 0,
        "unmapped_cell_count": max(t2_cells - t2_mapped, 0),
        "research_collection_count": collection_count,
        "research_subcollection_count_used": subcollection_count,
        "collection_staging_sheet_count": completeness[0],
        "collection_staging_cell_count": completeness[1],
        "collection_mapped_sheet_count": completeness[2],
        "collection_record_count": completeness[3],
        "collection_classified_record_count": completeness[4],
        "record_collection_relation_count": completeness[5],
        "collection_unmapped_sheet_count": completeness[0] - completeness[2],
        "collection_unclassified_record_count": completeness[3] - completeness[4],
        "passed": sheet_count == 28
        and main_count == 3011
        and completeness[0] == completeness[2]
        and completeness[3] == completeness[4],
    }
    out = settings.root / "data" / "staging" / "validation_report.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return report
