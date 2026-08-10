from __future__ import annotations

from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

import polars as pl

from policydb.crawl.dedup import canonicalize_url
from policydb.crawl.registry import load_registry
from policydb.scope import load_cities_105
from policydb.source_discovery import REQUIRED_ROLES
from policydb.source_slots import list_candidates

ROOT = Path(r"D:\Data Set\CRPD")

SHORTLIST = (
    ROOT
    / "outputs"
    / "acceptance"
    / "department_entry_slot_shortlist.csv"
)

OUTPUT_DIR = (
    ROOT
    / "outputs"
    / "acceptance"
    / "post_dedupe_audit"
)

DUPLICATE_ACTION = "合并重复候选并保留唯一入口"


def canonical(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""

    try:
        return canonicalize_url(text)
    except Exception:
        return text.lower()


def source_id_length(source_id: str) -> int:
    value = str(source_id or "")
    if value.startswith("SRC_"):
        value = value[4:]
    return len(value)


def build_multi_enabled_report(
    candidates: pl.DataFrame,
) -> pl.DataFrame:
    cities = load_cities_105()
    sources = load_registry()

    candidate_rows = candidates.to_dicts()
    rows: list[dict[str, Any]] = []

    for city in cities.iter_rows(named=True):
        city_id = str(city["city_id"])
        city_name = str(city["city_name"])
        province_name = str(city["province_name"])

        for role in REQUIRED_ROLES:
            enabled = [
                source
                for source in sources
                if bool(source.crawl_enabled)
                and city_id in source.city_ids
                and (
                    source.agency_type == role
                    or source.source_role == role
                )
            ]

            if len(enabled) <= 1:
                continue

            domain_counts = Counter(
                str(source.domain or "").lower()
                for source in enabled
            )

            for source in enabled:
                domain = str(source.domain or "").lower()

                verified_rows = [
                    row
                    for row in candidate_rows
                    if bool(row.get("is_verified"))
                    and str(row.get("city_id") or "") == city_id
                    and str(row.get("source_role") or "") == role
                    and str(row.get("domain") or "").lower() == domain
                ]

                active_urls = [
                    source.homepage_url,
                    *list(source.list_page_urls or []),
                ]

                active_canonical = {
                    canonical(url)
                    for url in active_urls
                    if url
                }

                exact_verified = sum(
                    canonical(
                        row.get("candidate_url")
                        or row.get("canonical_url")
                    )
                    in active_canonical
                    for row in verified_rows
                )

                rows.append(
                    {
                        "city_id": city_id,
                        "city_name": city_name,
                        "province_name": province_name,
                        "source_role": role,
                        "enabled_source_count": len(enabled),
                        "unique_domain_count": len(domain_counts),
                        "source_id": source.source_id,
                        "source_id_hex_length": source_id_length(
                            source.source_id
                        ),
                        "source_name": source.source_name,
                        "domain": domain,
                        "same_domain_enabled_count": domain_counts[domain],
                        "likely_registry_duplicate": (
                            domain_counts[domain] > 1
                        ),
                        "official_domain_verified": bool(
                            source.official_domain_verified
                        ),
                        "health_status": source.health_status,
                        "verified_candidate_count_for_domain": len(
                            verified_rows
                        ),
                        "exact_active_entry_verified_count": exact_verified,
                        "homepage_url": source.homepage_url,
                        "list_page_urls": " | ".join(
                            str(value)
                            for value in source.list_page_urls
                            if value
                        ),
                        "historical_entry_urls": " | ".join(
                            str(value)
                            for value in getattr(
                                source,
                                "historical_entry_urls",
                                [],
                            )
                            if value
                        ),
                        "verified_at": str(
                            source.verified_at or ""
                        ),
                    }
                )

    if not rows:
        return pl.DataFrame()

    return (
        pl.DataFrame(
            rows,
            infer_schema_length=None,
        )
        .sort(
            [
                "province_name",
                "city_name",
                "source_role",
                "domain",
                "source_id",
            ]
        )
    )


def build_cross_slot_conflicts(
    candidates: pl.DataFrame,
) -> tuple[pl.DataFrame, int]:
    shortlist = pl.read_csv(
        SHORTLIST,
        infer_schema_length=None,
    )

    target_ids = set(
        shortlist
        .filter(
            pl.col("recommended_action")
            == DUPLICATE_ACTION
        )
        ["candidate_id"]
        .drop_nulls()
        .cast(pl.String)
        .to_list()
    )

    candidate_rows = candidates.to_dicts()

    remaining_targets = [
        row
        for row in candidate_rows
        if str(row.get("candidate_id") or "") in target_ids
        and not bool(row.get("is_verified"))
        and "duplicate_or_existing_source"
        in str(
            row.get("verification_reason_codes")
            or ""
        )
    ]

    conflict_urls = {
        canonical(
            row.get("canonical_url")
            or row.get("candidate_url")
        )
        for row in remaining_targets
    }

    conflict_urls.discard("")

    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)

    for row in candidate_rows:
        url = canonical(
            row.get("canonical_url")
            or row.get("candidate_url")
        )

        if url in conflict_urls:
            grouped[url].append(row)

    rows: list[dict[str, Any]] = []

    remaining_target_ids = {
        str(row.get("candidate_id") or "")
        for row in remaining_targets
    }

    for url, group in grouped.items():
        slot_count = len(
            {
                str(row.get("slot_id") or "")
                for row in group
                if row.get("slot_id")
            }
        )

        role_count = len(
            {
                str(row.get("source_role") or "")
                for row in group
                if row.get("source_role")
            }
        )

        city_count = len(
            {
                str(row.get("city_id") or "")
                for row in group
                if row.get("city_id")
            }
        )

        for row in group:
            candidate_id = str(
                row.get("candidate_id") or ""
            )

            rows.append(
                {
                    "canonical_url": url,
                    "conflict_candidate_count": len(group),
                    "conflict_slot_count": slot_count,
                    "conflict_role_count": role_count,
                    "conflict_city_count": city_count,
                    "is_target_rejected_candidate": (
                        candidate_id in remaining_target_ids
                    ),
                    "candidate_id": candidate_id,
                    "slot_id": row.get("slot_id"),
                    "city_id": row.get("city_id"),
                    "source_role": row.get("source_role"),
                    "source_id": row.get("source_id"),
                    "candidate_url": row.get("candidate_url"),
                    "site_name": row.get("site_name"),
                    "department_name": row.get("department_name"),
                    "candidate_kind": row.get("candidate_kind"),
                    "page_type": row.get("page_type"),
                    "is_verified": bool(
                        row.get("is_verified")
                    ),
                    "manual_review_status": row.get(
                        "manual_review_status"
                    ),
                    "city_match_evidence": row.get(
                        "city_match_evidence"
                    ),
                    "role_match_evidence": row.get(
                        "role_match_evidence"
                    ),
                    "verification_reason_codes": row.get(
                        "verification_reason_codes"
                    ),
                }
            )

    if not rows:
        return pl.DataFrame(), len(remaining_targets)

    result = (
        pl.DataFrame(
            rows,
            infer_schema_length=None,
        )
        .sort(
            [
                "canonical_url",
                "is_target_rejected_candidate",
                "city_id",
                "source_role",
            ],
            descending=[
                False,
                True,
                False,
                False,
            ],
        )
    )

    return result, len(remaining_targets)


def write_outputs(
    frame: pl.DataFrame,
    stem: str,
) -> None:
    if frame.is_empty():
        print(f"{stem}: no rows")
        return

    csv_path = OUTPUT_DIR / f"{stem}.csv"
    xlsx_path = OUTPUT_DIR / f"{stem}.xlsx"

    frame.write_csv(
        csv_path,
        include_bom=True,
    )

    frame.write_excel(
        xlsx_path,
        autofit=True,
    )

    print(f"{stem}:")
    print(f"  rows  = {frame.height}")
    print(f"  csv   = {csv_path}")
    print(f"  xlsx  = {xlsx_path}")


def main() -> None:
    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    candidates = list_candidates()

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

    multi = build_multi_enabled_report(
        candidates
    )

    conflicts, rejected_target_count = (
        build_cross_slot_conflicts(
            candidates
        )
    )

    write_outputs(
        multi,
        "multi_enabled_sources",
    )

    write_outputs(
        conflicts,
        "remaining_cross_slot_conflicts",
    )

    print("=" * 76)

    if not multi.is_empty():
        multi_slots = (
            multi
            .select(
                [
                    "city_id",
                    "source_role",
                ]
            )
            .unique()
            .height
        )

        same_domain_slots = (
            multi
            .filter(
                pl.col(
                    "likely_registry_duplicate"
                )
            )
            .select(
                [
                    "city_id",
                    "source_role",
                ]
            )
            .unique()
            .height
        )

        different_domain_slots = (
            multi_slots
            - same_domain_slots
        )

        print(
            f"Multi-enabled slots          : "
            f"{multi_slots}"
        )

        print(
            f"Same-domain duplicate slots  : "
            f"{same_domain_slots}"
        )

        print(
            f"Different-domain slots       : "
            f"{different_domain_slots}"
        )

    print(
        f"Rejected duplicate targets   : "
        f"{rejected_target_count}"
    )

    if not conflicts.is_empty():
        print(
            f"Cross-slot canonical URLs     : "
            f"{conflicts['canonical_url'].n_unique()}"
        )

    print("=" * 76)


if __name__ == "__main__":
    main()
