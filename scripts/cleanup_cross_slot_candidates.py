from __future__ import annotations

import argparse
import json
import shutil
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import polars as pl

from policydb.crawl.dedup import (
    canonicalize_url,
)
from policydb.crawl.registry import (
    load_registry,
)
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

ROOT = Path(r"D:\Data Set\CRPD")

CONFLICT_FILE = (
    ROOT
    / "outputs"
    / "acceptance"
    / "post_dedupe_audit"
    / "remaining_cross_slot_conflicts.csv"
)

OUTPUT_DIR = (
    ROOT
    / "outputs"
    / "acceptance"
    / "cross_slot_cleanup"
)

# 3个旧版重复注册表来源。
RETIRED_DUPLICATE_SOURCE_IDS = {
    "SRC_F0A3C4B6D0C61FD8",
    "SRC_9EF3857B07B0EF7B",
    "SRC_F2CA191CAA3DC577",
}

# 13个已确认错误或不适合作为持续入口的来源。
INVALID_SOURCE_IDS = {
    "SRC_FCF60932C77D54C67C9B",
    "SRC_E2B21562DE256E52B5AD",
    "SRC_8A9E9CF9AA9642FCA440",
    "SRC_3641DDC0EE388F1B2BD0",
    "SRC_861BA5A34E9069575526",
    "SRC_0696722E8684908886A0",
    "SRC_28B792472794EF3244A0",
    "SRC_73FAE12654BCBD237F35",
    "SRC_5DDF2A1B99C13C4AA05F",
    "SRC_10DD46753C475938B7FA",
    "SRC_FC2E1948CCB5C2C66726",
    "SRC_89F3C6785DF972F8ADBF",
    "SRC_02F6A2907A9C214C4D4B",
}


def canonical(value: Any) -> str:
    text = str(value or "").strip()

    if not text:
        return ""

    try:
        return canonicalize_url(text)
    except Exception:
        return text.lower()


# URL → 唯一允许的角色。
ALLOWED_ROLES_RAW = {
    # 政策正文页，不允许进入任何来源槽位。
    "http://changzhou.gov.cn/gi_news/129140203215777":
        set(),

    # 成都公积金。
    "https://cdzfgjj.chengdu.gov.cn/cdgjj/c1156367/common_list.shtml":
        {"provident_fund_center"},

    "https://cdzfgjj.chengdu.gov.cn/cdgjj/c1156368/common_list.shtml":
        {"provident_fund_center"},

    # 各地住房公积金中心。
    "https://gjj.hangzhou.gov.cn/col/col1229287760/index.html":
        {"provident_fund_center"},

    "https://gjj.nanjing.gov.cn/zcfg":
        {"provident_fund_center"},

    "https://gjj.nanning.gov.cn/xxgk/zdxzjchgfxwj":
        {"provident_fund_center"},

    "https://gjj.wuhan.gov.cn/":
        {"provident_fund_center"},

    "https://sygjj.shenyang.gov.cn/zwgknew/fdzdgknr/zxwj_1":
        {"provident_fund_center"},

    # 市政府门户。
    "https://harbin.gov.cn/":
        {"municipal_government"},

    "https://sjz.gov.cn/":
        {"municipal_government"},

    # 杭州市政府公报。
    "https://zfgb.hangzhou.gov.cn/bulletin.shtml":
        {"government_gazette"},
}

ALLOWED_ROLES = {
    canonical(url): roles
    for url, roles in
    ALLOWED_ROLES_RAW.items()
}


def truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value

    return str(value or "").strip().lower() in {
        "1",
        "true",
        "yes",
        "y",
    }


def source_role(source) -> str:
    for value in (
        source.agency_type,
        source.source_role,
    ):
        if value:
            return str(value)

    return ""


def source_urls(source) -> set[str]:
    values = [
        source.homepage_url,
        *list(source.list_page_urls or []),
    ]

    return {
        canonical(value)
        for value in values
        if value
    }


def derive_invalid_url_slot_keys(
    registry,
) -> set[tuple[str, str, str]]:
    keys: set[
        tuple[str, str, str]
    ] = set()

    for source in registry:
        if (
            source.source_id
            not in INVALID_SOURCE_IDS
        ):
            continue

        role = source_role(source)

        for city_id in source.city_ids:
            for url in source_urls(source):
                keys.add(
                    (
                        str(city_id),
                        role,
                        url,
                    )
                )

    return keys


def exclusion_status(
    row: dict[str, Any],
    invalid_url_slot_keys:
        set[tuple[str, str, str]],
) -> str | None:
    candidate_url = canonical(
        row.get("canonical_url")
        or row.get("candidate_url")
    )

    role = str(
        row.get("source_role") or ""
    )

    city_id = str(
        row.get("city_id") or ""
    )

    source_id = str(
        row.get("source_id") or ""
    )

    if candidate_url in ALLOWED_ROLES:
        allowed = ALLOWED_ROLES[
            candidate_url
        ]

        if not allowed:
            return (
                "excluded_detail_page_"
                "not_reusable"
            )

        if role not in allowed:
            return (
                "excluded_cross_slot_"
                "role_mismatch"
            )

    if source_id in RETIRED_DUPLICATE_SOURCE_IDS:
        return (
            "excluded_registry_"
            "duplicate_retired"
        )

    if source_id in INVALID_SOURCE_IDS:
        return (
            "excluded_registry_"
            "source_invalid"
        )

    if (
        city_id,
        role,
        candidate_url,
    ) in invalid_url_slot_keys:
        return (
            "excluded_registry_"
            "source_invalid"
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
    registry = load_registry(settings)

    candidate_path = slot_paths(
        settings
    )[1]

    frame = read_parquet_snapshot(
        candidate_path
    )

    invalid_url_slot_keys = (
        derive_invalid_url_slot_keys(
            registry
        )
    )

    rows = frame.to_dicts()

    excluded_ids: list[str] = []
    excluded_slots: set[str] = set()
    revoked_verified = 0
    status_counts: dict[str, int] = {}

    now = datetime.now(
        UTC
    ).isoformat()

    updated_rows = []

    for row in rows:
        status = exclusion_status(
            row,
            invalid_url_slot_keys,
        )

        if status:
            candidate_id = str(
                row.get("candidate_id")
                or ""
            )

            excluded_ids.append(
                candidate_id
            )

            excluded_slots.add(
                str(
                    row.get("slot_id")
                    or ""
                )
            )

            revoked_verified += int(
                bool(
                    row.get("is_verified")
                )
            )

            status_counts[status] = (
                status_counts.get(
                    status,
                    0,
                )
                + 1
            )

            row["is_verified"] = False
            row["is_enabled"] = False
            row[
                "manual_review_status"
            ] = status

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
                [status],
                ensure_ascii=False,
            )

            row[
                "verification_summary_json"
            ] = json.dumps(
                {
                    "candidate_id":
                        candidate_id,
                    "status":
                        "CURATED_EXCLUDED",
                    "verified": False,
                    "reason_codes":
                        [status],
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

    conflicts = pl.read_csv(
        CONFLICT_FILE,
        infer_schema_length=None,
    )

    target_ids = []

    for row in conflicts.to_dicts():
        if not truthy(
            row.get(
                "is_target_rejected_candidate"
            )
        ):
            continue

        url = canonical(
            row.get("canonical_url")
        )

        allowed = ALLOWED_ROLES.get(
            url
        )

        role = str(
            row.get("source_role")
            or ""
        )

        if (
            allowed
            and role in allowed
        ):
            target_ids.append(
                str(row["candidate_id"])
            )

    target_ids = list(
        dict.fromkeys(target_ids)
    )

    print("=" * 76)
    print(
        "MODE:",
        "APPLY"
        if args.apply
        else "DRY RUN",
    )
    print(
        f"Candidates excluded : "
        f"{len(excluded_ids)}"
    )
    print(
        f"Affected slots      : "
        f"{len(excluded_slots)}"
    )
    print(
        f"Verified revoked    : "
        f"{revoked_verified}"
    )
    print(
        f"Recheck targets     : "
        f"{len(target_ids)}"
    )
    print(
        "Exclusion statuses  : "
        f"{status_counts}"
    )
    print("=" * 76)

    if not args.apply:
        print(
            "未修改数据。确认后增加 --apply。"
        )
        return

    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    stamp = datetime.now().strftime(
        "%Y%m%d_%H%M%S"
    )

    backup_path = (
        OUTPUT_DIR
        / (
            "source_candidates_before_"
            f"{stamp}.parquet"
        )
    )

    shutil.copy2(
        candidate_path,
        backup_path,
    )

    updated = pl.DataFrame(
        updated_rows,
        infer_schema_length=None,
    ).sort(
        [
            "city_id",
            "source_role",
            "candidate_id",
        ]
    )

    atomic_write_parquet(
        updated,
        candidate_path,
        {
            "module":
                "curated_cross_slot_cleanup",
            "created_at": now,
        },
    )

    audit_after_exclusion = (
        build_requirement_slots(
            settings
        )
    )

    verification = verify_candidates(
        candidate_ids=target_ids,
        run_id=(
            "curated_cross_slot_"
            "reverification_20260804"
        ),
        settings=settings,
    )

    refreshed = list_candidates(
        settings=settings
    ).filter(
        pl.col("candidate_id").is_in(
            target_ids
        )
    )

    verified_targets = (
        refreshed
        .filter(
            pl.col("is_verified")
            .fill_null(False)
        )
    )

    promoted = []
    enabled = []
    errors = []

    for row in verified_targets.iter_rows(
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
        "backup": str(backup_path),
        "excluded_candidate_count":
            len(excluded_ids),
        "excluded_slot_count":
            len(excluded_slots),
        "revoked_verified_candidates":
            revoked_verified,
        "exclusion_status_counts":
            status_counts,
        "target_candidate_ids":
            target_ids,
        "verification": verification,
        "verified_target_count":
            verified_targets.height,
        "promoted": promoted,
        "enabled": enabled,
        "errors": errors,
        "audit_after_exclusion":
            audit_after_exclusion,
        "final_audit": final_audit,
    }

    report_path = (
        OUTPUT_DIR
        / (
            "cross_slot_cleanup_"
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
    print(
        f"Verified targets : "
        f"{verified_targets.height}"
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
    print(f"Backup           : {backup_path}")
    print(f"Report           : {report_path}")
    print()
    print(
        json.dumps(
            final_audit,
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
