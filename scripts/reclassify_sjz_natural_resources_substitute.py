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
    slot_paths,
)

CANDIDATE_ID = "SRCCAND_254A350D4A01A9F043F2"


def main() -> None:
    settings = Settings.discover()
    candidate_path = slot_paths(settings)[1]

    frame = read_parquet_snapshot(
        candidate_path
    )

    matched = frame.filter(
        pl.col("candidate_id")
        == CANDIDATE_ID
    )

    if matched.height != 1:
        raise RuntimeError(
            f"候选数量异常：{matched.height}"
        )

    output_dir = (
        settings.outputs
        / "acceptance"
        / "municipal_substitute_reclassification"
    )

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    stamp = datetime.now().strftime(
        "%Y%m%d_%H%M%S"
    )

    backup = (
        output_dir
        / f"source_candidates_before_{stamp}.parquet"
    )

    shutil.copy2(
        candidate_path,
        backup,
    )

    now = datetime.now(UTC).isoformat()
    rows = frame.to_dicts()

    for row in rows:
        if (
            str(row.get("candidate_id"))
            != CANDIDATE_ID
        ):
            continue

        reasons = [
            "department_page_evidence_missing",
            "municipal_portal_substitute_only",
            "probe_integrity_invalid_sha256",
        ]

        row["is_verified"] = False
        row["is_enabled"] = False

        row["candidate_kind"] = (
            "municipal_portal_substitute_candidate"
        )

        row["role_assignment_method"] = (
            "municipal_portal_substitute"
        )

        row["substitute_for_role"] = (
            "natural_resources_department"
        )

        row["substitute_reason"] = (
            "石家庄市政府信息公开入口；"
            "页面证据不足以证明其为自然资源部门专属栏目"
        )

        row["role_match_evidence"] = None
        row["role_confidence"] = 0.0
        row["overall_confidence"] = 0.5

        row["manual_review_status"] = (
            "substitute_pending_department_entry"
        )

        row["verification_failed_gates"] = (
            json.dumps(
                [
                    "source_role_match",
                    "probe_evidence_integrity",
                    "strict_admission_ready",
                ],
                ensure_ascii=False,
            )
        )

        row["verification_reason_codes"] = (
            json.dumps(
                reasons,
                ensure_ascii=False,
            )
        )

        row["verification_summary_json"] = (
            json.dumps(
                {
                    "candidate_id":
                        CANDIDATE_ID,
                    "status":
                        "MUNICIPAL_SUBSTITUTE_ONLY",
                    "verified": False,
                    "reason_codes": reasons,
                    "checked_at": now,
                },
                ensure_ascii=False,
            )
        )

        row["verification_checked_at"] = now
        row["updated_at"] = now

    updated = (
        pl.DataFrame(
            rows,
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
                "municipal_substitute_reclassification",
            "created_at": now,
        },
    )

    audit = build_requirement_slots(
        settings
    )

    report_path = (
        output_dir
        / f"reclassification_{stamp}.json"
    )

    report_path.write_text(
        json.dumps(
            {
                "candidate_id":
                    CANDIDATE_ID,
                "backup": str(backup),
                "audit": audit,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    print("=" * 72)
    print("石家庄候选已降为市政府替代入口")
    print(f"Backup : {backup}")
    print(f"Report : {report_path}")
    print("=" * 72)
    print(
        json.dumps(
            audit,
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
