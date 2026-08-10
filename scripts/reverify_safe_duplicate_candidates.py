from __future__ import annotations

import json
from pathlib import Path

import polars as pl

from policydb.source_slots import (
    list_candidates,
    verify_candidates,
)

ROOT = Path(r"D:\Data Set\CRPD")

INPUT = (
    ROOT
    / "outputs"
    / "acceptance"
    / "department_entry_slot_shortlist.csv"
)

OUTPUT_DIR = (
    ROOT
    / "outputs"
    / "acceptance"
    / "duplicate_cleanup"
)

OUTPUT_DIR.mkdir(
    parents=True,
    exist_ok=True,
)

EXCLUDED = {
    ("哈尔滨市", "natural_resources_department"),
    ("成都市", "housing_department"),
    ("杭州市", "municipal_government"),
    ("杭州市", "natural_resources_department"),
    ("石家庄市", "natural_resources_department"),
}


def main() -> None:
    shortlist = pl.read_csv(
        INPUT,
        infer_schema_length=None,
    )

    rows = [
        row
        for row in shortlist.to_dicts()
        if (
            str(row.get("recommended_action") or "")
            == "合并重复候选并保留唯一入口"
        )
        and (
            str(row.get("city_name") or ""),
            str(row.get("source_role") or ""),
        )
        not in EXCLUDED
    ]

    candidate_ids = [
        str(row["candidate_id"])
        for row in rows
        if row.get("candidate_id")
    ]

    print(
        f"Selected for deterministic recheck: "
        f"{len(candidate_ids)}"
    )

    result = verify_candidates(
        candidate_ids=candidate_ids,
        run_id="same_slot_duplicate_cleanup_20260804",
    )

    full_log = (
        OUTPUT_DIR
        / "safe_duplicate_reverification.json"
    )

    full_log.write_text(
        json.dumps(
            result,
            ensure_ascii=False,
            indent=2,
            default=str,
        ),
        encoding="utf-8",
    )

    candidates = list_candidates().filter(
        pl.col("candidate_id").is_in(
            candidate_ids
        )
    )

    report_columns = [
        column
        for column in [
            "candidate_id",
            "slot_id",
            "city_id",
            "source_role",
            "candidate_url",
            "is_verified",
            "manual_review_status",
            "verification_reason_codes",
            "verification_failed_gates",
            "verification_checked_at",
        ]
        if column in candidates.columns
    ]

    report = (
        candidates
        .select(report_columns)
        .sort(
            [
                column
                for column in [
                    "is_verified",
                    "city_id",
                    "source_role",
                ]
                if column in report_columns
            ],
            descending=[
                True,
                False,
                False,
            ][:len([
                column
                for column in [
                    "is_verified",
                    "city_id",
                    "source_role",
                ]
                if column in report_columns
            ])],
        )
    )

    report_csv = (
        OUTPUT_DIR
        / "safe_duplicate_reverification.csv"
    )

    report_xlsx = (
        OUTPUT_DIR
        / "safe_duplicate_reverification.xlsx"
    )

    report.write_csv(
        report_csv,
        include_bom=True,
    )

    report.write_excel(
        report_xlsx,
        autofit=True,
    )

    verified = (
        report.filter(
            pl.col("is_verified").fill_null(False)
        ).height
        if "is_verified" in report.columns
        else 0
    )

    failed = report.height - verified

    print("=" * 68)
    print(f"Checked  : {report.height}")
    print(f"Verified : {verified}")
    print(f"Rejected : {failed}")
    print(f"JSON     : {full_log}")
    print(f"CSV      : {report_csv}")
    print(f"Excel    : {report_xlsx}")
    print("=" * 68)

    if (
        failed
        and "verification_reason_codes"
        in report.columns
    ):
        print(
            report
            .filter(
                ~pl.col(
                    "is_verified"
                ).fill_null(False)
            )
            .group_by(
                "verification_reason_codes"
            )
            .len()
            .sort(
                "len",
                descending=True,
            )
        )


if __name__ == "__main__":
    main()
