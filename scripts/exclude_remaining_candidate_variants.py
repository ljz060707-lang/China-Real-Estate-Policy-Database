from __future__ import annotations

import argparse
import json
import shutil
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlsplit

import polars as pl

from policydb.crawl.dedup import canonicalize_url
from policydb.parquet_store import (
    atomic_write_parquet,
    read_parquet_snapshot,
)
from policydb.settings import Settings
from policydb.source_slots import (
    build_requirement_slots,
    slot_paths,
)


def canonical(value: Any) -> str:
    text = str(value or "").strip()

    if not text:
        return ""

    try:
        return canonicalize_url(text)
    except Exception:
        return text.lower()


def exclusion_reason(
    row: dict[str, Any],
) -> str | None:
    url = canonical(
        row.get("canonical_url")
        or row.get("candidate_url")
    )

    parsed = urlsplit(url)
    host = (
        parsed.hostname
        or ""
    ).lower().removeprefix("www.")

    path = parsed.path.rstrip("/").lower()

    role = str(
        row.get("source_role")
        or ""
    )

    # 常州户籍政策是具体正文页，不能作为任何持续来源入口。
    if (
        host == "changzhou.gov.cn"
        and path
        == "/gi_news/129140203215777"
    ):
        return (
            "excluded_detail_page_"
            "not_reusable"
        )

    # 杭州政府公报独立域只允许归属于政府公报槽位。
    if (
        host == "zfgb.hangzhou.gov.cn"
        and role != "government_gazette"
    ):
        return (
            "excluded_cross_slot_"
            "gazette_domain_role_mismatch"
        )

    return None


def main() -> None:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--apply",
        action="store_true",
    )

    args = parser.parse_args()

    settings = Settings.discover()
    candidate_path = slot_paths(settings)[1]

    frame = read_parquet_snapshot(
        candidate_path
    )

    now = datetime.now(
        UTC
    ).isoformat()

    rows = frame.to_dicts()

    affected: list[dict[str, Any]] = []
    updated_rows = []

    for row in rows:
        reason = exclusion_reason(row)

        if reason:
            previous_status = str(
                row.get(
                    "manual_review_status"
                )
                or ""
            )

            # 已排除记录保持现状，避免反复计数。
            if not previous_status.startswith(
                "excluded_"
            ):
                affected.append(
                    {
                        "candidate_id":
                            row.get("candidate_id"),
                        "city_id":
                            row.get("city_id"),
                        "source_role":
                            row.get("source_role"),
                        "candidate_url":
                            row.get("candidate_url"),
                        "previous_status":
                            previous_status,
                        "previous_verified":
                            bool(
                                row.get(
                                    "is_verified"
                                )
                            ),
                        "reason":
                            reason,
                    }
                )

            row["is_verified"] = False
            row["is_enabled"] = False

            row[
                "manual_review_status"
            ] = reason

            row[
                "verification_failed_gates"
            ] = json.dumps(
                [
                    "curated_candidate_"
                    "exclusion"
                ],
                ensure_ascii=False,
            )

            row[
                "verification_reason_codes"
            ] = json.dumps(
                [reason],
                ensure_ascii=False,
            )

            row[
                "verification_summary_json"
            ] = json.dumps(
                {
                    "candidate_id":
                        row.get(
                            "candidate_id"
                        ),
                    "status":
                        "CURATED_EXCLUDED",
                    "verified": False,
                    "reason_codes":
                        [reason],
                    "last_checked_at":
                        now,
                },
                ensure_ascii=False,
            )

            row[
                "verification_checked_at"
            ] = now

            row["updated_at"] = now

        updated_rows.append(row)

    print("=" * 76)
    print(
        "MODE:",
        "APPLY"
        if args.apply
        else "DRY RUN",
    )
    print(
        f"Newly affected candidates: "
        f"{len(affected)}"
    )
    print("=" * 76)

    for item in affected:
        print(
            f"{item['candidate_id']} | "
            f"{item['city_id']} | "
            f"{item['source_role']} | "
            f"{item['candidate_url']} | "
            f"{item['reason']}"
        )

    if not args.apply:
        print()
        print(
            "预览完成，尚未修改数据。"
        )
        return

    output_dir = (
        settings.outputs
        / "acceptance"
        / "candidate_variant_cleanup"
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
        / (
            "source_candidates_before_"
            f"variant_cleanup_{stamp}.parquet"
        )
    )

    shutil.copy2(
        candidate_path,
        backup,
    )

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
                "candidate_variant_cleanup",
            "created_at":
                now,
        },
    )

    audit = build_requirement_slots(
        settings
    )

    report = {
        "applied": True,
        "affected_count":
            len(affected),
        "affected_candidates":
            affected,
        "backup":
            str(backup),
        "audit":
            audit,
    }

    report_path = (
        output_dir
        / (
            "candidate_variant_cleanup_"
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
    print("=" * 76)
    print("定向排除完成")
    print(f"Backup : {backup}")
    print(f"Report : {report_path}")
    print("=" * 76)


if __name__ == "__main__":
    main()
