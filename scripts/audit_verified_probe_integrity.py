from __future__ import annotations

import json
import re
from typing import Any

import polars as pl

from policydb.settings import Settings
from policydb.source_slots import list_candidates

SHA256 = re.compile(
    r"^[0-9a-fA-F]{64}$"
)

PLACEHOLDERS = {
    "abc",
    "test",
    "dummy",
    "placeholder",
    "sha256",
}

GENERIC_TITLES = {
    "policies",
    "policy",
    "home",
    "index",
    "government",
}


def parse_json(value: Any) -> Any:
    if value is None:
        return None

    if isinstance(value, (list, dict)):
        return value

    try:
        return json.loads(str(value))
    except Exception:
        return None


def main() -> None:
    settings = Settings.discover()

    frame = list_candidates(
        settings=settings
    )

    if "manual_review_status" in frame.columns:
        frame = frame.filter(
            ~pl.col("manual_review_status")
            .fill_null("")
            .cast(pl.String)
            .str.starts_with("excluded_")
        )

    verified = frame.filter(
        pl.col("is_verified")
        .fill_null(False)
    )

    rows = []

    for candidate in verified.to_dicts():
        probes = parse_json(
            candidate.get(
                "probe_evidence_json"
            )
        )

        if not isinstance(probes, list):
            probes = []

        hashes = [
            str(item.get("response_sha256") or "")
            .strip()
            for item in probes
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
            str(item.get("page_title") or "")
            .strip()
            for item in probes
        ]

        generic_titles = [
            value
            for value in titles
            if value.lower()
            in GENERIC_TITLES
        ]

        placeholder_hash = any(
            value.lower() in PLACEHOLDERS
            or len(value) < 16
            for value in invalid_hashes
        )

        if not probes:
            status = "FATAL_NO_PROBE_EVIDENCE"
        elif not valid_hashes:
            status = "FATAL_NO_VALID_SHA256"
        elif placeholder_hash:
            status = "FATAL_PLACEHOLDER_HASH"
        elif generic_titles:
            status = "WARN_GENERIC_TITLE"
        else:
            status = "OK"

        rows.append(
            {
                "integrity_status": status,
                "candidate_id":
                    candidate.get("candidate_id"),
                "city_id":
                    candidate.get("city_id"),
                "source_role":
                    candidate.get("source_role"),
                "candidate_url":
                    candidate.get("candidate_url"),
                "is_verified":
                    candidate.get("is_verified"),
                "probe_round_count":
                    len(probes),
                "valid_sha256_count":
                    len(valid_hashes),
                "invalid_hash_values":
                    " | ".join(invalid_hashes),
                "generic_page_titles":
                    " | ".join(generic_titles),
                "health_status":
                    candidate.get("health_status"),
                "parser_status":
                    candidate.get("parser_status"),
            }
        )

    result = (
        pl.DataFrame(
            rows,
            infer_schema_length=None,
        )
        .sort(
            [
                "integrity_status",
                "city_id",
                "source_role",
            ]
        )
    )

    output_dir = (
        settings.outputs
        / "acceptance"
        / "probe_integrity_audit"
    )

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    csv_path = (
        output_dir
        / "verified_candidate_probe_integrity.csv"
    )

    xlsx_path = (
        output_dir
        / "verified_candidate_probe_integrity.xlsx"
    )

    result.write_csv(
        csv_path,
        include_bom=True,
    )

    result.write_excel(
        xlsx_path,
        autofit=True,
    )

    summary = (
        result
        .group_by("integrity_status")
        .len()
        .sort(
            "len",
            descending=True,
        )
    )

    suspicious = result.filter(
        pl.col("integrity_status")
        != "OK"
    )

    print("=" * 72)
    print(f"Verified candidates : {verified.height}")
    print(f"Suspicious          : {suspicious.height}")
    print(f"CSV                 : {csv_path}")
    print(f"Excel               : {xlsx_path}")
    print("=" * 72)
    print(summary)

    if suspicious.height:
        print()
        print(
            suspicious.select(
                [
                    "integrity_status",
                    "candidate_id",
                    "city_id",
                    "source_role",
                    "candidate_url",
                    "invalid_hash_values",
                    "generic_page_titles",
                ]
            )
        )


if __name__ == "__main__":
    main()
