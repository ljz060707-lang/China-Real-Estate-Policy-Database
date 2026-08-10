from __future__ import annotations

import json
import re
from collections import defaultdict
from typing import Any
from urllib.parse import urlsplit

import polars as pl

from policydb.crawl.dedup import canonicalize_url
from policydb.crawl.registry import load_registry
from policydb.scope import load_cities_105
from policydb.settings import Settings
from policydb.source_discovery import REQUIRED_ROLES
from policydb.source_slots import list_candidates

SHA256 = re.compile(r"^[0-9a-fA-F]{64}$")


def parse_json(value: Any) -> Any:
    if isinstance(value, (list, dict)):
        return value

    try:
        return json.loads(str(value))
    except Exception:
        return None


def canonical(value: Any) -> str:
    text = str(value or "").strip()

    if not text:
        return ""

    try:
        return canonicalize_url(text)
    except Exception:
        return text.lower()


def domain(value: Any) -> str:
    return (
        urlsplit(str(value or ""))
        .hostname
        or ""
    ).lower().removeprefix("www.")


def candidate_integrity(
    row: dict[str, Any],
) -> dict[str, Any]:
    probes = parse_json(
        row.get("probe_evidence_json")
    )

    if not isinstance(probes, list):
        probes = []

    hashes = [
        str(
            probe.get("response_sha256")
            or ""
        ).strip()
        for probe in probes
    ]

    valid_hashes = [
        value
        for value in hashes
        if SHA256.fullmatch(value)
    ]

    invalid_hashes = [
        value
        for value in hashes
        if value
        and not SHA256.fullmatch(value)
    ]

    titles = [
        str(
            probe.get("page_title")
            or ""
        ).strip()
        for probe in probes
    ]

    return {
        "probe_round_count": len(probes),
        "valid_sha256_count":
            len(valid_hashes),
        "invalid_hash_values":
            " | ".join(invalid_hashes),
        "page_titles":
            " | ".join(titles),
        "integrity_valid":
            bool(valid_hashes),
    }


def registered_role(source) -> str:
    if source.agency_type in REQUIRED_ROLES:
        return str(source.agency_type)

    if source.source_role in REQUIRED_ROLES:
        return str(source.source_role)

    return ""


def source_urls(source) -> set[str]:
    return {
        canonical(value)
        for value in [
            source.homepage_url,
            *list(source.list_page_urls or []),
        ]
        if value
    }


def main() -> None:
    settings = Settings.discover()

    candidates = list_candidates(
        settings=settings
    )

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

    verified = candidates.filter(
        pl.col("is_verified")
        .fill_null(False)
    )

    candidate_rows = []

    for row in verified.to_dicts():
        candidate_rows.append(
            {
                **row,
                **candidate_integrity(row),
            }
        )

    assessed = pl.DataFrame(
        candidate_rows,
        infer_schema_length=None,
    )

    invalid = assessed.filter(
        ~pl.col("integrity_valid")
    )

    valid = assessed.filter(
        pl.col("integrity_valid")
    )

    cities = load_cities_105(settings)

    city_lookup = {
        str(row["city_id"]):
            str(row["city_name"])
        for row in cities.iter_rows(
            named=True
        )
    }

    registry = load_registry(settings)

    enabled_by_slot = defaultdict(list)

    for source in registry:
        if not source.crawl_enabled:
            continue

        role = registered_role(source)

        if not role:
            continue

        for city_id in source.city_ids:
            enabled_by_slot[
                (str(city_id), role)
            ].append(source)

    impact_rows = []

    affected_slot_ids = (
        invalid["slot_id"]
        .drop_nulls()
        .unique()
        .to_list()
    )

    for slot_id in affected_slot_ids:
        invalid_slot = invalid.filter(
            pl.col("slot_id") == slot_id
        )

        valid_slot = valid.filter(
            pl.col("slot_id") == slot_id
        )

        first = invalid_slot.to_dicts()[0]

        city_id = str(
            first.get("city_id") or ""
        )

        role = str(
            first.get("source_role") or ""
        )

        enabled_sources = enabled_by_slot.get(
            (city_id, role),
            [],
        )

        valid_urls = {
            canonical(
                row.get("canonical_url")
                or row.get("candidate_url")
            )
            for row in valid_slot.to_dicts()
        }

        valid_urls.discard("")

        valid_domains = {
            domain(value)
            for value in valid_urls
            if value
        }

        exact_supported_ids = []
        domain_supported_ids = []

        for source in enabled_sources:
            active_urls = source_urls(source)

            if active_urls & valid_urls:
                exact_supported_ids.append(
                    source.source_id
                )

            if (
                str(source.domain or "")
                .lower()
                .removeprefix("www.")
                in valid_domains
            ):
                domain_supported_ids.append(
                    source.source_id
                )

        if exact_supported_ids:
            classification = (
                "SAFE_EXACT_VALID_SUPPORT"
            )
        elif domain_supported_ids:
            classification = (
                "SAFE_DOMAIN_VALID_SUPPORT"
            )
        elif valid_slot.height:
            classification = (
                "VALID_ALTERNATE_NOT_BOUND_"
                "TO_ENABLED_SOURCE"
            )
        elif enabled_sources:
            classification = (
                "ENABLED_SOURCE_HAS_"
                "INVALID_EVIDENCE_ONLY"
            )
        else:
            classification = (
                "INVALID_VERIFIED_NOT_ENABLED"
            )

        impact_rows.append(
            {
                "impact_classification":
                    classification,
                "slot_id": slot_id,
                "city_id": city_id,
                "city_name":
                    city_lookup.get(
                        city_id,
                        city_id,
                    ),
                "source_role": role,
                "invalid_verified_count":
                    invalid_slot.height,
                "valid_verified_count":
                    valid_slot.height,
                "enabled_source_count":
                    len(enabled_sources),
                "enabled_source_ids":
                    " | ".join(
                        source.source_id
                        for source
                        in enabled_sources
                    ),
                "enabled_domains":
                    " | ".join(
                        str(source.domain)
                        for source
                        in enabled_sources
                    ),
                "exact_supported_source_ids":
                    " | ".join(
                        exact_supported_ids
                    ),
                "domain_supported_source_ids":
                    " | ".join(
                        domain_supported_ids
                    ),
                "invalid_candidate_ids":
                    " | ".join(
                        invalid_slot[
                            "candidate_id"
                        ].to_list()
                    ),
                "valid_candidate_ids":
                    " | ".join(
                        valid_slot[
                            "candidate_id"
                        ].to_list()
                    )
                    if valid_slot.height
                    else "",
                "invalid_candidate_urls":
                    " | ".join(
                        invalid_slot[
                            "candidate_url"
                        ].to_list()
                    ),
                "valid_candidate_urls":
                    " | ".join(
                        valid_slot[
                            "candidate_url"
                        ].to_list()
                    )
                    if valid_slot.height
                    else "",
            }
        )

    impact = (
        pl.DataFrame(
            impact_rows,
            infer_schema_length=None,
        )
        .sort(
            [
                "impact_classification",
                "city_name",
                "source_role",
            ]
        )
    )

    invalid_report = (
        invalid
        .select(
            [
                column
                for column in [
                    "candidate_id",
                    "slot_id",
                    "city_id",
                    "source_role",
                    "candidate_url",
                    "source_id",
                    "manual_review_status",
                    "probe_round_count",
                    "valid_sha256_count",
                    "invalid_hash_values",
                    "page_titles",
                    "health_status",
                    "parser_status",
                    "verification_checked_at",
                ]
                if column in invalid.columns
            ]
        )
        .sort(
            [
                "city_id",
                "source_role",
                "candidate_id",
            ]
        )
    )

    output_dir = (
        settings.outputs
        / "acceptance"
        / "probe_integrity_impact"
    )

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    invalid_csv = (
        output_dir
        / "invalid_verified_candidates.csv"
    )

    invalid_xlsx = (
        output_dir
        / "invalid_verified_candidates.xlsx"
    )

    impact_csv = (
        output_dir
        / "invalid_verified_slot_impact.csv"
    )

    impact_xlsx = (
        output_dir
        / "invalid_verified_slot_impact.xlsx"
    )

    invalid_report.write_csv(
        invalid_csv,
        include_bom=True,
    )

    invalid_report.write_excel(
        invalid_xlsx,
        autofit=True,
    )

    impact.write_csv(
        impact_csv,
        include_bom=True,
    )

    impact.write_excel(
        impact_xlsx,
        autofit=True,
    )

    summary = (
        impact
        .group_by(
            "impact_classification"
        )
        .agg(
            pl.len().alias(
                "slot_count"
            ),
            pl.col(
                "invalid_verified_count"
            ).sum().alias(
                "invalid_candidate_count"
            ),
        )
        .sort(
            "slot_count",
            descending=True,
        )
    )

    print("=" * 76)
    print(
        f"Verified candidates        : "
        f"{verified.height}"
    )
    print(
        f"Invalid verified candidates: "
        f"{invalid.height}"
    )
    print(
        f"Affected slots             : "
        f"{impact.height}"
    )
    print(f"Invalid CSV               : {invalid_csv}")
    print(f"Impact CSV                : {impact_csv}")
    print("=" * 76)
    print(summary)

    dangerous = impact.filter(
        pl.col(
            "impact_classification"
        ).is_in(
            [
                "ENABLED_SOURCE_HAS_"
                "INVALID_EVIDENCE_ONLY",

                "VALID_ALTERNATE_NOT_BOUND_"
                "TO_ENABLED_SOURCE",
            ]
        )
    )

    if dangerous.height:
        print()
        print("Slots requiring intervention:")
        print(
            dangerous.select(
                [
                    "impact_classification",
                    "city_name",
                    "source_role",
                    "enabled_source_ids",
                    "invalid_candidate_ids",
                    "valid_candidate_ids",
                ]
            )
        )


if __name__ == "__main__":
    main()
