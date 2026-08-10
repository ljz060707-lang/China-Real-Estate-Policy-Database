from __future__ import annotations

import argparse
import json
from datetime import datetime
from typing import Any

from policydb.crawl.dedup import canonicalize_url
from policydb.crawl.registry import (
    load_registry,
    materialize_registry_parquet,
    save_registry_atomic,
)
from policydb.settings import Settings
from policydb.source_slots import build_requirement_slots

DISABLE_REASONS = {
    "SRC_F0A3C4B6D0C61FD8":
        "上海政府公报旧16位同域重复记录",
    "SRC_9EF3857B07B0EF7B":
        "北京政府公报旧16位同域重复记录",
    "SRC_F2CA191CAA3DC577":
        "南京自然资源旧16位同域重复记录",

    "SRC_FCF60932C77D54C67C9B":
        "佛山测绘监管子站；保留自然资源局主站",
    "SRC_E2B21562DE256E52B5AD":
        "北京住建旧8080入口；保留当前HTTPS主站",
    "SRC_8A9E9CF9AA9642FCA440":
        "实际为北京市生态环境局，不属于自然资源角色",

    "SRC_3641DDC0EE388F1B2BD0":
        "住建委政策详情页，不是北京公积金持续入口",
    "SRC_861BA5A34E9069575526":
        "个人网上业务平台，不是政策列表入口",

    "SRC_0696722E8684908886A0":
        "实际为南京市城乡建设委员会，不属于政府公报",
    "SRC_28B792472794EF3244A0":
        "南京市政府专题总入口，不是政府公报入口",

    "SRC_73FAE12654BCBD237F35":
        "南京政务服务网；保留南京市政府门户",

    "SRC_5DDF2A1B99C13C4AA05F":
        "实际为天津市财政局，不属于自然资源角色",

    "SRC_10DD46753C475938B7FA":
        "广州市政府住房专题替代入口；保留市住建局",

    "SRC_FC2E1948CCB5C2C66726":
        "国家政务服务平台，不是晋江本地公积金来源",
    "SRC_89F3C6785DF972F8ADBF":
        "晋江市政府首页，不是住房公积金中心入口",

    "SRC_02F6A2907A9C214C4D4B":
        "山东省住建厅入口；保留青岛市住建局",
}


# 旧同域来源 → 新来源
MERGE_HISTORY = {
    "SRC_F0A3C4B6D0C61FD8":
        "SRC_F0A3C4B6D0C61FD82C84",

    "SRC_9EF3857B07B0EF7B":
        "SRC_9EF3857B07B0EF7B53B8",

    "SRC_F2CA191CAA3DC577":
        "SRC_394D8230A8C53AFBA5DF",
}


def canonical(value: Any) -> str:
    text = str(value or "").strip()

    if not text:
        return ""

    try:
        return canonicalize_url(text)
    except Exception:
        return text.lower()


def source_urls(source) -> list[str]:
    values = [
        source.homepage_url,
        *list(source.list_page_urls or []),
        *list(
            getattr(
                source,
                "historical_entry_urls",
                [],
            )
            or []
        ),
    ]

    return [
        str(value)
        for value in values
        if value
    ]


def main() -> None:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--apply",
        action="store_true",
        help="真正写入注册表；省略时仅预览",
    )

    args = parser.parse_args()

    settings = Settings.discover()
    sources = load_registry(settings)

    by_id = {
        source.source_id: source
        for source in sources
    }

    missing = sorted(
        set(DISABLE_REASONS)
        - set(by_id)
    )

    plan = []

    for source_id, reason in DISABLE_REASONS.items():
        source = by_id.get(source_id)

        if source is None:
            continue

        plan.append(
            {
                "source_id": source_id,
                "source_name": source.source_name,
                "domain": source.domain,
                "city_ids": list(source.city_ids),
                "source_role": source.source_role,
                "agency_type": source.agency_type,
                "currently_enabled": bool(
                    source.crawl_enabled
                ),
                "reason": reason,
                "merge_history_into": (
                    MERGE_HISTORY.get(source_id)
                ),
            }
        )

    print("=" * 78)
    print(
        "MODE:",
        "APPLY" if args.apply else "DRY RUN",
    )
    print(
        f"Planned source disables: "
        f"{len(plan)}"
    )
    print(
        f"Missing source IDs: "
        f"{len(missing)}"
    )
    print("=" * 78)

    for row in plan:
        print(
            f"{row['source_id']} | "
            f"{row['source_name']} | "
            f"{row['reason']}"
        )

    if missing:
        print()
        print("Missing IDs:")
        print("\n".join(missing))

    if not args.apply:
        print()
        print(
            "未修改数据。确认后增加 --apply。"
        )
        return

    output_dir = (
        settings.outputs
        / "acceptance"
        / "registry_cleanup"
    )

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    stamp = datetime.now().strftime(
        "%Y%m%d_%H%M%S"
    )

    snapshot_path = (
        output_dir
        / f"registry_before_cleanup_{stamp}.json"
    )

    snapshot_path.write_text(
        json.dumps(
            [
                source.model_dump(mode="json")
                for source in sources
            ],
            ensure_ascii=False,
            indent=2,
            default=str,
        ),
        encoding="utf-8",
    )

    updated_by_id = dict(by_id)

    # 将三个旧同域来源的URL保存到新来源历史字段。
    for old_id, keep_id in MERGE_HISTORY.items():
        old = updated_by_id.get(old_id)
        keep = updated_by_id.get(keep_id)

        if old is None or keep is None:
            continue

        active_urls = {
            canonical(value)
            for value in [
                keep.homepage_url,
                *list(keep.list_page_urls or []),
            ]
            if value
        }

        history = list(
            getattr(
                keep,
                "historical_entry_urls",
                [],
            )
            or []
        )

        for value in source_urls(old):
            if (
                canonical(value)
                and canonical(value)
                not in active_urls
            ):
                history.append(value)

        unique_history = []
        observed = set()

        for value in history:
            key = canonical(value)

            if not key or key in observed:
                continue

            observed.add(key)
            unique_history.append(value)

        updated_by_id[keep_id] = (
            keep.model_copy(
                update={
                    "historical_entry_urls":
                        unique_history
                }
            )
        )

    # 停用错误、重复或不适合持续抓取的来源。
    for source_id in DISABLE_REASONS:
        source = updated_by_id.get(source_id)

        if source is None:
            continue

        updated_by_id[source_id] = (
            source.model_copy(
                update={
                    "crawl_enabled": False,
                    "recommended_enabled": False,
                }
            )
        )

    updated = [
        updated_by_id[source.source_id]
        for source in sources
    ]

    save_registry_atomic(
        updated,
        settings,
        action=(
            "cleanup_multi_enabled_sources_"
            f"{len(DISABLE_REASONS)}"
        ),
    )

    materialize_registry_parquet(
        updated,
        settings,
    )

    audit = build_requirement_slots(
        settings
    )

    report_path = (
        output_dir
        / f"registry_cleanup_result_{stamp}.json"
    )

    report = {
        "applied": True,
        "disabled_source_count":
            len(DISABLE_REASONS),
        "disabled_sources": plan,
        "missing_source_ids": missing,
        "snapshot": str(snapshot_path),
        "audit": audit,
    }

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
    print("清理完成")
    print(f"Snapshot: {snapshot_path}")
    print(f"Report:   {report_path}")
    print()
    print(
        json.dumps(
            audit,
            ensure_ascii=False,
            indent=2,
            default=str,
        )
    )


if __name__ == "__main__":
    main()
