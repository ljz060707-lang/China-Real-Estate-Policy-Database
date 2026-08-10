from __future__ import annotations

import argparse
import json
import shutil
from datetime import UTC, datetime
from typing import Any

import polars as pl

from policydb.crawl.registry import (
    load_registry,
    materialize_registry_parquet,
    save_registry_atomic,
)
from policydb.parquet_store import (
    atomic_write_parquet,
    read_parquet_snapshot,
)
from policydb.settings import Settings
from policydb.source_slots import (
    build_requirement_slots,
    slot_paths,
)

IMPACT_CLASS = (
    "ENABLED_SOURCE_HAS_"
    "INVALID_EVIDENCE_ONLY"
)


def split_pipe(value: Any) -> list[str]:
    return [
        item.strip()
        for item in str(value or "").split("|")
        if item.strip()
    ]


def main() -> None:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--apply",
        action="store_true",
        help="实际隔离异常候选和来源",
    )

    args = parser.parse_args()

    settings = Settings.discover()

    impact_path = (
        settings.outputs
        / "acceptance"
        / "probe_integrity_impact"
        / "invalid_verified_slot_impact.csv"
    )

    if not impact_path.exists():
        raise FileNotFoundError(
            f"缺少影响报告：{impact_path}"
        )

    impact = pl.read_csv(
        impact_path,
        infer_schema_length=None,
    )

    invalid_classes = (
        impact
        .filter(
            pl.col("impact_classification")
            != IMPACT_CLASS
        )
    )

    if invalid_classes.height:
        raise RuntimeError(
            "影响报告中存在其他分类，"
            "不能使用本脚本自动处理。"
        )

    candidate_ids: set[str] = set()
    source_ids: set[str] = set()
    slot_ids: set[str] = set()

    for row in impact.to_dicts():
        slot_id = str(
            row.get("slot_id") or ""
        )

        if slot_id:
            slot_ids.add(slot_id)

        candidate_ids.update(
            split_pipe(
                row.get(
                    "invalid_candidate_ids"
                )
            )
        )

        source_ids.update(
            split_pipe(
                row.get(
                    "enabled_source_ids"
                )
            )
        )

    print("=" * 78)
    print(
        "MODE:",
        "APPLY" if args.apply
        else "DRY RUN",
    )
    print(f"Affected slots       : {len(slot_ids)}")
    print(f"Candidates to revoke : {len(candidate_ids)}")
    print(f"Sources to disable   : {len(source_ids)}")
    print("=" * 78)

    if len(slot_ids) != 26:
        raise RuntimeError(
            f"预期26个槽位，实际{len(slot_ids)}个。"
        )

    if len(candidate_ids) != 28:
        raise RuntimeError(
            f"预期28条候选，实际{len(candidate_ids)}条。"
        )

    candidate_path = slot_paths(
        settings
    )[1]

    candidates = read_parquet_snapshot(
        candidate_path
    )

    registry = load_registry(settings)

    existing_candidate_ids = {
        str(value)
        for value in candidates[
            "candidate_id"
        ].to_list()
    }

    existing_source_ids = {
        source.source_id
        for source in registry
    }

    missing_candidates = sorted(
        candidate_ids
        - existing_candidate_ids
    )

    missing_sources = sorted(
        source_ids
        - existing_source_ids
    )

    if missing_candidates:
        print("Missing candidates:")
        print("\n".join(missing_candidates))

    if missing_sources:
        print("Missing sources:")
        print("\n".join(missing_sources))

    if missing_candidates or missing_sources:
        raise RuntimeError(
            "候选表或注册表与影响报告不一致，"
            "未执行修改。"
        )

    display_columns = [
        column
        for column in [
            "city_name",
            "source_role",
            "enabled_source_ids",
            "invalid_candidate_ids",
        ]
        if column in impact.columns
    ]

    print(
        impact
        .select(display_columns)
        .sort(
            [
                column
                for column in [
                    "city_name",
                    "source_role",
                ]
                if column in display_columns
            ]
        )
    )

    if not args.apply:
        print()
        print(
            "当前为预览模式，未修改数据。"
        )
        return

    output_dir = (
        settings.outputs
        / "acceptance"
        / "probe_integrity_quarantine"
    )

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    stamp = datetime.now().strftime(
        "%Y%m%d_%H%M%S"
    )

    now = datetime.now(
        UTC
    ).isoformat()

    candidate_backup = (
        output_dir
        / (
            "source_candidates_before_"
            f"quarantine_{stamp}.parquet"
        )
    )

    registry_backup = (
        output_dir
        / (
            "source_registry_before_"
            f"quarantine_{stamp}.json"
        )
    )

    shutil.copy2(
        candidate_path,
        candidate_backup,
    )

    registry_backup.write_text(
        json.dumps(
            [
                source.model_dump(
                    mode="json"
                )
                for source in registry
            ],
            ensure_ascii=False,
            indent=2,
            default=str,
        ),
        encoding="utf-8",
    )

    # 第一阶段：先停用来源。
    # 即便后续候选写入失败，也不会继续抓取可疑来源。
    updated_registry = []
    disabled_count = 0

    for source in registry:
        if source.source_id in source_ids:
            changes = {
                "crawl_enabled": False,
                "recommended_enabled": False,
                "verified_at": None,
                "updated_at": now,
            }

            if hasattr(
                source,
                "source_health_score",
            ):
                changes[
                    "source_health_score"
                ] = None

            source = source.model_copy(
                update=changes
            )

            disabled_count += 1

        updated_registry.append(source)

    save_registry_atomic(
        updated_registry,
        settings,
        action=(
            "quarantine_invalid_probe_"
            f"evidence_sources={disabled_count}"
        ),
    )

    materialize_registry_parquet(
        updated_registry,
        settings,
    )

    # 第二阶段：撤销候选验证并清空不可信探测结果。
    rows = candidates.to_dicts()
    updated_rows = []
    revoked_count = 0

    numeric_probe_fields = {
        "health_probe_success_count",
        "parser_probe_success_count",
        "direct_probe_success_count",
        "probe_success_count",
    }

    timestamp_probe_fields = {
        "health_checked_at",
        "parser_checked_at",
        "last_probe_at",
    }

    for row in rows:
        candidate_id = str(
            row.get("candidate_id") or ""
        )

        if candidate_id in candidate_ids:
            revoked_count += int(
                bool(
                    row.get("is_verified")
                )
            )

            reasons = [
                "probe_integrity_no_valid_sha256",
                "probe_evidence_quarantined",
                "requires_real_network_reprobe",
            ]

            row["is_verified"] = False
            row["is_enabled"] = False

            row["manual_review_status"] = (
                "quarantined_invalid_"
                "probe_evidence"
            )

            # 清空不可信探测证据，防止旧abc证据再次通过验证。
            row["probe_evidence_json"] = "[]"

            if "health_status" in row:
                row["health_status"] = (
                    "pending_evaluation"
                )

            if "parser_status" in row:
                row["parser_status"] = (
                    "pending_evaluation"
                )

            for field in numeric_probe_fields:
                if field in row:
                    row[field] = 0

            for field in timestamp_probe_fields:
                if field in row:
                    row[field] = None

            row[
                "verification_failed_gates"
            ] = json.dumps(
                [
                    "probe_evidence_integrity",
                    "two_round_real_probe",
                    "strict_admission_ready",
                ],
                ensure_ascii=False,
            )

            row[
                "verification_reason_codes"
            ] = json.dumps(
                reasons,
                ensure_ascii=False,
            )

            row[
                "verification_summary_json"
            ] = json.dumps(
                {
                    "candidate_id":
                        candidate_id,
                    "status":
                        "QUARANTINED",
                    "verified": False,
                    "reason_codes":
                        reasons,
                    "checked_at":
                        now,
                },
                ensure_ascii=False,
            )

            row[
                "verification_checked_at"
            ] = now

            row["updated_at"] = now

        updated_rows.append(row)

    updated_candidates = (
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
        updated_candidates,
        candidate_path,
        {
            "module":
                "probe_integrity_quarantine",
            "created_at":
                now,
        },
    )

    final_audit = build_requirement_slots(
        settings
    )

    report = {
        "applied": True,
        "affected_slot_count":
            len(slot_ids),
        "candidate_count":
            len(candidate_ids),
        "revoked_verified_count":
            revoked_count,
        "source_count":
            len(source_ids),
        "disabled_source_count":
            disabled_count,
        "candidate_ids":
            sorted(candidate_ids),
        "source_ids":
            sorted(source_ids),
        "candidate_backup":
            str(candidate_backup),
        "registry_backup":
            str(registry_backup),
        "final_audit":
            final_audit,
    }

    report_path = (
        output_dir
        / (
            "probe_integrity_quarantine_"
            f"{stamp}.json"
        )
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

    print()
    print("=" * 78)
    print("QUARANTINE COMPLETE")
    print(f"Candidates revoked : {revoked_count}")
    print(f"Sources disabled   : {disabled_count}")
    print(f"Candidate backup   : {candidate_backup}")
    print(f"Registry backup    : {registry_backup}")
    print(f"Report             : {report_path}")
    print("=" * 78)

    print(
        json.dumps(
            final_audit,
            ensure_ascii=False,
            indent=2,
        )
    )

    if (
        final_audit.get(
            "enabled_unverified_slots"
        )
        != 0
    ):
        raise SystemExit(
            "隔离已执行，但仍存在启用未验证槽位。"
        )


if __name__ == "__main__":
    main()
