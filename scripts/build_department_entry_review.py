from pathlib import Path

import polars as pl

ROOT = Path(r"D:\Data Set\CRPD")

SLOT_AUDIT = (
    ROOT
    / "outputs"
    / "acceptance"
    / "source_525_audit.csv"
)

CANDIDATE_AUDIT = (
    ROOT
    / "outputs"
    / "source_candidates"
    / "source_candidate_audit.parquet"
)

OUTPUT_CSV = (
    ROOT
    / "outputs"
    / "acceptance"
    / "department_entry_candidate_review.csv"
)

OUTPUT_XLSX = (
    ROOT
    / "outputs"
    / "acceptance"
    / "department_entry_candidate_review.xlsx"
)


def existing_columns(
    frame: pl.DataFrame,
    names: list[str],
) -> list[str]:
    return [
        name
        for name in names
        if name in frame.columns
    ]


def main() -> None:
    slots = pl.read_csv(
        SLOT_AUDIT,
        infer_schema_length=None,
    )

    candidates = pl.read_parquet(
        CANDIDATE_AUDIT,
    )

    # Curated-invalid candidates remain in historical audit files,
    # but must not participate in operational review or ranking.
    if (
        candidates.height
        and "manual_review_status"
        in candidates.columns
    ):
        candidates = candidates.filter(
            ~pl.col("manual_review_status")
            .fill_null("")
            .cast(pl.String)
            .str.starts_with("excluded_")
        )

    target_slots = (
        slots
        .filter(
            pl.col("coverage_status")
            == "department_entry_candidate"
        )
    )

    if "slot_id" not in target_slots.columns:
        raise RuntimeError(
            "source_525_audit.csv中没有slot_id字段。"
        )

    if "slot_id" not in candidates.columns:
        raise RuntimeError(
            "source_candidate_audit.parquet中没有slot_id字段。"
        )

    target_ids = (
        target_slots
        .select("slot_id")
        .unique()
    )

    review = candidates.join(
        target_ids,
        on="slot_id",
        how="inner",
    )

    # 构建便于排序的修复优先级：
    # 1 = 健康且解析成功，只缺确定性证据；
    # 2 = 健康但解析未通过；
    # 3 = 网络健康未通过。
    health_expr = (
        pl.col("health_status")
        .fill_null("")
        .is_in([
            "healthy",
            "ok",
            "direct_ok",
        ])
        if "health_status" in review.columns
        else pl.lit(False)
    )

    parser_expr = (
        pl.col("parser_status")
        .fill_null("")
        .is_in([
            "ok",
            "verified",
            "list_detected",
            "pagination_detected",
        ])
        if "parser_status" in review.columns
        else pl.lit(False)
    )

    verified_expr = (
        pl.col("is_verified")
        .fill_null(False)
        if "is_verified" in review.columns
        else pl.lit(False)
    )

    review = review.with_columns(
        health_expr.alias("health_pass"),
        parser_expr.alias("parser_pass"),
        verified_expr.alias("verified_pass"),
    ).with_columns(
        pl.when(
            pl.col("health_pass")
            & pl.col("parser_pass")
            & ~pl.col("verified_pass")
        )
        .then(pl.lit(1))
        .when(
            pl.col("health_pass")
            & ~pl.col("parser_pass")
        )
        .then(pl.lit(2))
        .otherwise(pl.lit(3))
        .alias("repair_priority")
    )

    preferred = [
        "repair_priority",
        "slot_id",
        "city_id",
        "city_name",
        "province_name",
        "source_role",
        "candidate_id",
        "candidate_url",
        "canonical_url",
        "site_name",
        "department_name",
        "candidate_kind",
        "page_type",
        "entry_eligible",
        "is_official",
        "health_pass",
        "health_status",
        "http_status",
        "network_route",
        "health_probe_count",
        "health_probe_success_count",
        "parser_pass",
        "parser_status",
        "pagination_strategy",
        "publication_date_available",
        "article_link_extraction_ready",
        "is_verified",
        "manual_review_status",
        "verification_reason_codes",
        "verification_failed_gates",
        "verification_summary_json",
        "city_match_evidence",
        "role_match_evidence",
        "official_domain_evidence",
        "discovery_method",
        "discovery_evidence_url",
        "overall_confidence",
        "last_checked_at",
    ]

    columns = existing_columns(
        review,
        preferred,
    )

    sort_columns = existing_columns(
        review,
        [
            "repair_priority",
            "city_name",
            "source_role",
            "overall_confidence",
        ],
    )

    descending = [
        False,
        False,
        False,
        True,
    ][:len(sort_columns)]

    result = review.select(columns)

    if sort_columns:
        result = result.sort(
            sort_columns,
            descending=descending,
        )

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
        .group_by("repair_priority")
        .agg(
            pl.len().alias("candidate_count"),
            pl.col("slot_id")
            .n_unique()
            .alias("slot_count"),
        )
        .sort("repair_priority")
    )

    print("=" * 72)
    print(
        f"Target slots      : "
        f"{target_slots.height}"
    )
    print(
        f"Candidate records : "
        f"{result.height}"
    )
    print(f"CSV               : {OUTPUT_CSV}")
    print(f"Excel             : {OUTPUT_XLSX}")
    print("=" * 72)
    print(summary)

    if "verification_reason_codes" in result.columns:
        reasons = (
            result
            .filter(
                pl.col(
                    "verification_reason_codes"
                ).is_not_null()
            )
            .group_by(
                "verification_reason_codes"
            )
            .agg(
                pl.len().alias(
                    "candidate_count"
                ),
                pl.col("slot_id")
                .n_unique()
                .alias("slot_count"),
            )
            .sort(
                "slot_count",
                descending=True,
            )
            .head(20)
        )

        print()
        print("Top failure patterns:")
        print(reasons)


if __name__ == "__main__":
    main()
