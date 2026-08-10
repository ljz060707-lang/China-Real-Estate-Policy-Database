from __future__ import annotations

import argparse
import json
from datetime import datetime
from typing import Any

import polars as pl

from policydb.crawl.registry import (
    load_registry,
)
from policydb.parquet_store import (
    read_parquet_snapshot,
)
from policydb.settings import Settings
from policydb.source_slots import (
    build_requirement_slots,
    enable_source_strict,
    list_candidates,
    promote_candidate,
    reconcile_registry,
    slot_paths,
)


def active_candidates(
    frame: pl.DataFrame,
) -> pl.DataFrame:
    if (
        frame.is_empty()
        or "manual_review_status"
        not in frame.columns
    ):
        return frame

    return frame.filter(
        ~pl.col("manual_review_status")
        .fill_null("")
        .cast(pl.String)
        .str.starts_with("excluded_")
    )


def source_role(source) -> str:
    return str(
        source.agency_type
        or source.source_role
        or ""
    )


def main() -> None:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--apply",
        action="store_true",
        help="实际停用无效来源，并启用正确的已验证来源",
    )

    args = parser.parse_args()

    settings = Settings.discover()

    slot_path, _ = slot_paths(settings)

    slots = read_parquet_snapshot(
        slot_path
    )

    candidates = active_candidates(
        list_candidates(settings=settings)
    )

    registry = load_registry(settings)

    # 包含两类：
    # 1. 有验证候选但没有启用来源；
    # 2. 有启用来源，但启用来源未对应验证候选。
    targets = slots.filter(
        (pl.col("verified_candidate_count") > 0)
        & (
            pl.col(
                "verified_enabled_source_count"
            )
            == 0
        )
    )

    invalid_enabled = reconcile_registry(
        apply=False,
        settings=settings,
    )

    plans: list[dict[str, Any]] = []

    for slot in targets.iter_rows(
        named=True
    ):
        slot_id = str(slot["slot_id"])
        city_id = str(slot["city_id"])
        role = str(slot["source_role"])

        verified = candidates.filter(
            (pl.col("slot_id") == slot_id)
            & pl.col("is_verified")
            .fill_null(False)
        )

        if verified.is_empty():
            plans.append(
                {
                    "slot_id": slot_id,
                    "city_id": city_id,
                    "city_name":
                        slot.get("city_name"),
                    "source_role": role,
                    "status":
                        "ERROR_NO_VERIFIED_CANDIDATE",
                }
            )
            continue

        rows = verified.to_dicts()

        rows.sort(
            key=lambda row: (
                float(
                    row.get(
                        "overall_confidence"
                    )
                    or 0
                ),
                int(
                    row.get(
                        "health_probe_success_count"
                    )
                    or 0
                ),
                str(
                    row.get(
                        "last_checked_at"
                    )
                    or ""
                ),
            ),
            reverse=True,
        )

        selected = rows[0]

        currently_enabled = [
            source.source_id
            for source in registry
            if source.crawl_enabled
            and city_id in source.city_ids
            and source_role(source) == role
        ]

        plans.append(
            {
                "slot_id": slot_id,
                "city_id": city_id,
                "city_name":
                    slot.get("city_name"),
                "source_role": role,
                "coverage_status":
                    slot.get("coverage_status"),
                "currently_enabled_source_ids":
                    currently_enabled,
                "selected_candidate_id":
                    selected.get("candidate_id"),
                "selected_candidate_url":
                    selected.get(
                        "candidate_url"
                    )
                    or selected.get(
                        "canonical_url"
                    ),
                "candidate_domain":
                    selected.get("domain"),
                "health_status":
                    selected.get(
                        "health_status"
                    ),
                "parser_status":
                    selected.get(
                        "parser_status"
                    ),
                "overall_confidence":
                    selected.get(
                        "overall_confidence"
                    ),
                "status":
                    "READY_FOR_ALIGNMENT",
            }
        )

    print("=" * 78)
    print(
        "MODE:",
        "APPLY" if args.apply
        else "DRY RUN",
    )
    print(
        f"Invalid enabled sources : "
        f"{invalid_enabled['invalid_enabled_count']}"
    )
    print(
        f"Slots needing alignment : "
        f"{len(plans)}"
    )
    print("=" * 78)

    print(
        json.dumps(
            {
                "invalid_enabled_sources":
                    invalid_enabled,
                "alignment_plans":
                    plans,
            },
            ensure_ascii=False,
            indent=2,
            default=str,
        )
    )

    errors_in_plan = [
        row
        for row in plans
        if row["status"]
        != "READY_FOR_ALIGNMENT"
    ]

    if errors_in_plan:
        raise SystemExit(
            "存在无法自动对齐的槽位，"
            "当前未修改任何数据。"
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
        / "final_source_alignment"
    )

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    stamp = datetime.now().strftime(
        "%Y%m%d_%H%M%S"
    )

    registry_snapshot = (
        output_dir
        / (
            "registry_before_alignment_"
            f"{stamp}.json"
        )
    )

    registry_snapshot.write_text(
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

    # 第一步：停用没有有效验证候选支撑的旧来源。
    reconciliation = reconcile_registry(
        apply=True,
        settings=settings,
    )

    promoted = []
    enabled = []
    errors = []

    # 第二步：只对目标槽位的首选验证候选进行提升和启用。
    for plan in plans:
        candidate_id = str(
            plan["selected_candidate_id"]
        )

        try:
            promotion = promote_candidate(
                candidate_id,
                settings=settings,
            )

            promoted.append(
                promotion
            )

            source_id = str(
                promotion["source_id"]
            )

            enablement = (
                enable_source_strict(
                    source_id,
                    settings=settings,
                )
            )

            enabled.append(
                enablement
            )

        except Exception as exc:
            errors.append(
                {
                    "candidate_id":
                        candidate_id,
                    "city_name":
                        plan["city_name"],
                    "source_role":
                        plan["source_role"],
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
        "registry_snapshot":
            str(registry_snapshot),
        "reconciliation":
            reconciliation,
        "alignment_plans":
            plans,
        "promoted":
            promoted,
        "enabled":
            enabled,
        "errors":
            errors,
        "final_audit":
            final_audit,
    }

    report_path = (
        output_dir
        / (
            "final_source_alignment_"
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
    print("FINAL ALIGNMENT RESULT")
    print(
        f"Invalid disabled : "
        f"{reconciliation['invalid_enabled_count']}"
    )
    print(
        f"Promoted         : "
        f"{len(promoted)}"
    )
    print(
        f"Enabled          : "
        f"{len(enabled)}"
    )
    print(
        f"Errors           : "
        f"{len(errors)}"
    )
    print(f"Snapshot         : {registry_snapshot}")
    print(f"Report           : {report_path}")
    print("=" * 78)

    print(
        json.dumps(
            final_audit,
            ensure_ascii=False,
            indent=2,
        )
    )

    aligned = (
        final_audit.get(
            "enabled_unverified_slots"
        )
        == 0
        and final_audit.get(
            "slots_enabled"
        )
        == final_audit.get(
            "slots_verified"
        )
    )

    if not aligned:
        raise SystemExit(
            "操作已执行，但验证与启用数量仍未完全对齐；"
            "请检查报告中的errors。"
        )


if __name__ == "__main__":
    main()
