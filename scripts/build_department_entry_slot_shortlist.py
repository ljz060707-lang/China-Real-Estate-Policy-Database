from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import polars as pl

ROOT = Path(r"D:\Data Set\CRPD")

INPUT = (
    ROOT
    / "outputs"
    / "acceptance"
    / "department_entry_candidate_review.csv"
)

OUTPUT_CSV = (
    ROOT
    / "outputs"
    / "acceptance"
    / "department_entry_slot_shortlist.csv"
)

OUTPUT_XLSX = (
    ROOT
    / "outputs"
    / "acceptance"
    / "department_entry_slot_shortlist.xlsx"
)


def truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {
        "true", "1", "yes", "y", "pass"
    }


def parse_reasons(value: Any) -> list[str]:
    if value is None:
        return []

    if isinstance(value, list):
        return [str(item) for item in value]

    text = str(value).strip()

    if not text:
        return []

    try:
        parsed = json.loads(text)

        if isinstance(parsed, list):
            return [str(item) for item in parsed]

        if isinstance(parsed, str):
            return [parsed]

    except (json.JSONDecodeError, TypeError):
        pass

    return [
        part.strip().strip('"[]')
        for part in text.split(",")
        if part.strip()
    ]


def contains_reason(
    reasons: list[str],
    keywords: tuple[str, ...],
) -> bool:
    joined = " ".join(reasons).lower()
    return any(keyword.lower() in joined for keyword in keywords)


def numeric(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def candidate_score(row: dict[str, Any]) -> float:
    reasons = parse_reasons(
        row.get("verification_reason_codes")
    )

    score = 0.0

    entry_eligible = truthy(
        row.get("entry_eligible")
    )

    candidate_kind = str(
        row.get("candidate_kind") or ""
    ).lower()

    page_type = str(
        row.get("page_type") or ""
    ).lower()

    health_pass = truthy(
        row.get("health_pass")
    )

    parser_pass = truthy(
        row.get("parser_pass")
    )

    if entry_eligible:
        score += 120

    if candidate_kind == "site_or_column_entry":
        score += 100

    if page_type not in {
        "policy_detail",
        "content_page",
        "policy_content_page",
        "pdf",
    }:
        score += 40

    if truthy(row.get("is_official")):
        score += 30

    if health_pass:
        score += 25

    if parser_pass:
        score += 25

    if truthy(
        row.get("publication_date_available")
    ):
        score += 10

    if truthy(
        row.get("article_link_extraction_ready")
    ):
        score += 10

    if str(
        row.get("city_match_evidence") or ""
    ).strip():
        score += 10

    if str(
        row.get("role_match_evidence") or ""
    ).strip():
        score += 10

    score += min(
        10.0,
        numeric(
            row.get("overall_confidence")
        ) * 10,
    )

    if contains_reason(
        reasons,
        ("detail_or_pdf_not_reusable",),
    ):
        score -= 250

    if contains_reason(
        reasons,
        ("direct_health_failed",),
    ):
        score -= 100

    if contains_reason(
        reasons,
        (
            "parser_failed",
            "parser_not_ready",
            "article_link_extraction",
            "pagination_not_ready",
        ),
    ):
        score -= 70

    if contains_reason(
        reasons,
        (
            "source_role_mismatch",
            "source_role_evidence_missing",
        ),
    ):
        score -= 50

    if contains_reason(
        reasons,
        ("city_evidence_missing",),
    ):
        score -= 40

    # 重复候选通常可以通过保留唯一入口修复，
    # 因此只给予较小惩罚。
    if contains_reason(
        reasons,
        ("duplicate_or_existing_source",),
    ):
        score -= 10

    return round(score, 2)


def recommended_action(
    row: dict[str, Any],
) -> str:
    reasons = parse_reasons(
        row.get("verification_reason_codes")
    )

    detail = contains_reason(
        reasons,
        ("detail_or_pdf_not_reusable",),
    )

    health = contains_reason(
        reasons,
        ("direct_health_failed",),
    )

    parser = contains_reason(
        reasons,
        (
            "parser_failed",
            "parser_not_ready",
            "pagination_not_ready",
            "article_link_extraction",
        ),
    )

    city = contains_reason(
        reasons,
        ("city_evidence_missing",),
    )

    role = contains_reason(
        reasons,
        (
            "source_role_mismatch",
            "source_role_evidence_missing",
        ),
    )

    duplicate = contains_reason(
        reasons,
        ("duplicate_or_existing_source",),
    )

    actions: list[str] = []

    if detail:
        actions.append("替换为同一官网的栏目或列表入口")

    if health:
        actions.append("更换入口或重新进行网络探测")

    if parser:
        actions.append("修复列表链接或分页识别")

    if city or role:
        actions.append("补充城市及部门角色确定性证据")

    if duplicate:
        actions.append("合并重复候选并保留唯一入口")

    if not actions:
        actions.append("重新执行确定性验证")

    return "；".join(actions)


def main() -> None:
    frame = pl.read_csv(
        INPUT,
        infer_schema_length=None,
    )

    if (
        frame.height
        and "manual_review_status"
        in frame.columns
    ):
        frame = frame.filter(
            ~pl.col("manual_review_status")
            .fill_null("")
            .cast(pl.String)
            .str.starts_with("excluded_")
        )

    rows = frame.to_dicts()

    for row in rows:
        row["candidate_score"] = (
            candidate_score(row)
        )

        row["recommended_action"] = (
            recommended_action(row)
        )

        row["_canonical_key"] = str(
            row.get("canonical_url")
            or row.get("candidate_url")
            or row.get("candidate_id")
            or ""
        ).strip().lower()

    grouped: dict[str, list[dict[str, Any]]] = {}

    for row in rows:
        slot_id = str(
            row.get("slot_id") or ""
        )

        if not slot_id:
            continue

        grouped.setdefault(
            slot_id,
            [],
        ).append(row)

    output_rows: list[dict[str, Any]] = []

    for _slot_id, candidates in grouped.items():
        # 同一槽位相同标准化网址只保留最高分候选。
        unique_by_url: dict[
            str,
            dict[str, Any],
        ] = {}

        for candidate in candidates:
            key = str(
                candidate.get("_canonical_key")
                or candidate.get("candidate_id")
                or ""
            )

            previous = unique_by_url.get(key)

            if (
                previous is None
                or numeric(
                    candidate.get(
                        "candidate_score"
                    )
                )
                > numeric(
                    previous.get(
                        "candidate_score"
                    )
                )
            ):
                unique_by_url[key] = candidate

        ranked = sorted(
            unique_by_url.values(),
            key=lambda item: (
                numeric(
                    item.get("candidate_score")
                ),
                numeric(
                    item.get("overall_confidence")
                ),
                str(
                    item.get("last_checked_at")
                    or ""
                ),
            ),
            reverse=True,
        )

        best = dict(ranked[0])

        alternate_urls = [
            str(
                item.get("candidate_url")
                or item.get("canonical_url")
                or ""
            )
            for item in ranked[1:4]
        ]

        best["raw_candidate_count"] = len(
            candidates
        )

        best["unique_url_count"] = len(
            ranked
        )

        best["alternate_urls_top3"] = (
            " | ".join(alternate_urls)
        )

        output_rows.append(best)

    result = pl.DataFrame(
        output_rows,
        infer_schema_length=None,
    )

    preferred = [
        "candidate_score",
        "slot_id",
        "city_id",
        "city_name",
        "province_name",
        "source_role",
        "recommended_action",
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
        "parser_pass",
        "parser_status",
        "pagination_strategy",
        "publication_date_available",
        "article_link_extraction_ready",
        "city_match_evidence",
        "role_match_evidence",
        "verification_reason_codes",
        "verification_failed_gates",
        "overall_confidence",
        "raw_candidate_count",
        "unique_url_count",
        "alternate_urls_top3",
        "last_checked_at",
    ]

    columns = [
        column
        for column in preferred
        if column in result.columns
    ]

    result = (
        result
        .select(columns)
        .sort(
            [
                "candidate_score",
                "city_name",
                "source_role",
            ],
            descending=[
                True,
                False,
                False,
            ],
        )
    )

    result.write_csv(
        OUTPUT_CSV,
        include_bom=True,
    )

    result.write_excel(
        OUTPUT_XLSX,
        autofit=True,
    )

    print("=" * 74)
    print(
        f"Slots shortlisted : {result.height}"
    )
    print(f"CSV               : {OUTPUT_CSV}")
    print(f"Excel             : {OUTPUT_XLSX}")
    print("=" * 74)

    summary = (
        result
        .group_by("recommended_action")
        .len()
        .sort(
            "len",
            descending=True,
        )
    )

    print(summary)

    print()
    print("Top 15 candidates:")
    print(
        result.select(
            [
                column
                for column in [
                    "candidate_score",
                    "city_name",
                    "source_role",
                    "candidate_url",
                    "recommended_action",
                ]
                if column in result.columns
            ]
        ).head(15)
    )


if __name__ == "__main__":
    main()
