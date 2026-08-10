from __future__ import annotations

import json
import shutil
from datetime import UTC, datetime

import polars as pl

from policydb.parquet_store import (
    atomic_write_parquet,
    read_parquet_snapshot,
)
from policydb.settings import Settings
from policydb.source_slots import (
    build_requirement_slots,
    enable_source_strict,
    list_candidates,
    promote_candidate,
    slot_paths,
    verify_candidates,
)

EXCLUDE_ID = "SRCCAND_DEA059C55B0D04EDE33F"

SAFE_IDS = [
    # 南京市政府公报
    "SRCCAND_E379A454765AAC7F4846",

    # 南京市规划和自然资源局
    "SRCCAND_0825A887C392E589C9AF",

    # 哈尔滨市自然资源和规划局集中公开列表
    "SRCCAND_E8E8AA48C1AA7EF652E1",

    # 成都市住房和城乡建设局
    "SRCCAND_A54D98FABAA73E6D99C1",
]


def main() -> None:
    settings = Settings.discover()
    candidate_path = slot_paths(settings)[1]

    output_dir = (
        settings.outputs
        / "acceptance"
        / "safe_duplicate_resolution"
    )
    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    stamp = datetime.now().strftime(
        "%Y%m%d_%H%M%S"
    )
    now = datetime.now(UTC).isoformat()

    frame = read_parquet_snapshot(
        candidate_path
    )

    matched = frame.filter(
        pl.col("candidate_id") == EXCLUDE_ID
    )

    if matched.height != 1:
        raise RuntimeError(
            f"常州候选数量异常：{matched.height}"
        )

    backup = (
        output_dir
        / f"source_candidates_before_{stamp}.parquet"
    )
    shutil.copy2(candidate_path, backup)

    rows = frame.to_dicts()
    updated_rows = []

    for row in rows:
        if str(row.get("candidate_id")) == EXCLUDE_ID:
            reason = (
                "excluded_detail_page_"
                "gi_news_not_reusable"
            )

            row["is_verified"] = False
            row["is_enabled"] = False
            row["entry_eligible"] = False
            row["page_type"] = "policy_content_page"
            row["candidate_kind"] = "policy_content_evidence"
            row["manual_review_status"] = reason

            row["verification_failed_gates"] = json.dumps(
                [
                    "reusable_list_entry",
                    "curated_candidate_exclusion",
                ],
                ensure_ascii=False,
            )

            row["verification_reason_codes"] = json.dumps(
                [reason],
                ensure_ascii=False,
            )

            row["verification_summary_json"] = json.dumps(
                {
                    "candidate_id": EXCLUDE_ID,
                    "status": "CURATED_EXCLUDED",
                    "verified": False,
                    "reason_codes": [reason],
                    "last_checked_at": now,
                },
                ensure_ascii=False,
            )

            row["verification_checked_at"] = now
            row["updated_at"] = now

        updated_rows.append(row)

    updated = (
        pl.DataFrame(
            updated_rows,
            infer_schema_length=None,
        )
        .sort(
            [
                "city_id",
                "source_role",
                "candidate_id",
            ]
        )
    )

    atomic_write_parquet(
        updated,
        candidate_path,
        {
            "module":
                "safe_duplicate_resolution",
            "created_at": now,
        },
    )

    audit_after_exclusion = (
        build_requirement_slots(settings)
    )

    verification = verify_candidates(
        candidate_ids=SAFE_IDS,
        run_id=(
            "safe_duplicate_resolution_"
            "20260804"
        ),
        settings=settings,
    )

    refreshed = list_candidates(
        settings=settings
    ).filter(
        pl.col("candidate_id").is_in(
            SAFE_IDS
        )
    )

    verified = refreshed.filter(
        pl.col("is_verified")
        .fill_null(False)
    )

    promoted = []
    enabled = []
    errors = []

    for row in verified.iter_rows(
        named=True
    ):
        candidate_id = str(
            row["candidate_id"]
        )

        try:
            promotion = promote_candidate(
                candidate_id,
                settings=settings,
            )
            promoted.append(promotion)

            enablement = enable_source_strict(
                str(promotion["source_id"]),
                settings=settings,
            )
            enabled.append(enablement)

        except Exception as exc:
            errors.append(
                {
                    "candidate_id":
                        candidate_id,
                    "error_type":
                        type(exc).__name__,
                    "message":
                        str(exc),
                }
            )

    final_audit = build_requirement_slots(
        settings
    )

    report = {
        "applied": True,
        "excluded_candidate_id": EXCLUDE_ID,
        "recheck_candidate_ids": SAFE_IDS,
        "verification": verification,
        "verified_count": verified.height,
        "promoted": promoted,
        "enabled": enabled,
        "errors": errors,
        "backup": str(backup),
        "audit_after_exclusion":
            audit_after_exclusion,
        "final_audit": final_audit,
    }

    report_path = (
        output_dir
        / f"safe_duplicate_resolution_{stamp}.json"
    )

    report_path.write_text(
        json.dumps(
            report,
            ensure_ascii=False,
            indent=2,
            default=str,
        ),
        encoding="utf-8",
    )

    print("=" * 76)
    print(f"Verified : {verified.height}")
    print(f"Promoted : {len(promoted)}")
    print(f"Enabled  : {len(enabled)}")
    print(f"Errors   : {len(errors)}")
    print(f"Backup   : {backup}")
    print(f"Report   : {report_path}")
    print("=" * 76)
    print(
        json.dumps(
            final_audit,
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
