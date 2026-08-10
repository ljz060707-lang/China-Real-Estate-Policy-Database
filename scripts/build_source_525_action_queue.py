from pathlib import Path

import polars as pl

SOURCE = Path(
    r"D:\Data Set\CRPD\outputs\acceptance\source_525_audit.csv"
)

OUTPUT_CSV = Path(
    r"D:\Data Set\CRPD\outputs\acceptance"
    r"\source_525_action_queue.csv"
)

OUTPUT_XLSX = Path(
    r"D:\Data Set\CRPD\outputs\acceptance"
    r"\source_525_action_queue.xlsx"
)


def main() -> None:
    frame = pl.read_csv(SOURCE, infer_schema_length=None)

    frame = frame.with_columns(
        pl.when(
            pl.col("coverage_status")
            == "enabled_source_pending_verification"
        )
        .then(pl.lit(1))
        .when(
            pl.col("coverage_status")
            == "department_entry_candidate"
        )
        .then(pl.lit(2))
        .when(
            pl.col("coverage_status")
            == "municipal_portal_substitute_candidate"
        )
        .then(pl.lit(3))
        .when(
            pl.col("coverage_status")
            == "content_evidence_only"
        )
        .then(pl.lit(4))
        .when(
            pl.col("coverage_status")
            == "no_candidate"
        )
        .then(pl.lit(5))
        .otherwise(pl.lit(9))
        .alias("priority"),

        pl.when(
            pl.col("coverage_status")
            == "enabled_source_pending_verification"
        )
        .then(pl.lit("复核并停用未验证来源"))
        .when(
            pl.col("coverage_status")
            == "department_entry_candidate"
        )
        .then(pl.lit("优先探测部门入口并补足城市和角色证据"))
        .when(
            pl.col("coverage_status")
            == "municipal_portal_substitute_candidate"
        )
        .then(pl.lit("核验市政府门户能否作为部门角色替代入口"))
        .when(
            pl.col("coverage_status")
            == "content_evidence_only"
        )
        .then(pl.lit("从政策正文反向定位官方栏目或列表入口"))
        .when(
            pl.col("coverage_status")
            == "no_candidate"
        )
        .then(pl.lit("重新搜索并登记官方部门入口"))
        .otherwise(pl.lit("已完成"))
        .alias("next_action"),
    )

    queue = (
        frame
        .filter(
            pl.col("coverage_status")
            != "verified_enabled_source"
        )
        .sort(
            [
                "priority",
                "province_name",
                "city_name",
                "source_role",
            ]
        )
    )

    preferred = [
        "priority",
        "city_id",
        "city_name",
        "province_name",
        "source_role",
        "coverage_status",
        "next_action",
        "candidate_count",
        "verified_candidate_count",
        "registered_source_count",
        "enabled_source_count",
        "direct_healthy_candidate_count",
        "parser_ready_candidate_count",
    ]

    columns = [
        column
        for column in preferred
        if column in queue.columns
    ]

    result = queue.select(columns)

    # UTF-8 BOM，确保Windows Excel正确识别中文。
    result.write_csv(
        OUTPUT_CSV,
        include_bom=True,
    )

    result.write_excel(
        OUTPUT_XLSX,
        autofit=True,
    )

    summary = (
        result
        .group_by(
            [
                "priority",
                "coverage_status",
                "next_action",
            ]
        )
        .len()
        .sort("priority")
    )

    print("=" * 70)
    print(f"Unresolved slots : {result.height}")
    print(f"CSV               : {OUTPUT_CSV}")
    print(f"Excel             : {OUTPUT_XLSX}")
    print("=" * 70)
    print(summary)


if __name__ == "__main__":
    main()
