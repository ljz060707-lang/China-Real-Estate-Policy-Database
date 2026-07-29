from __future__ import annotations

import json
from pathlib import Path

import polars as pl

from policydb.coverage import build_source_matrix
from policydb.crawl.registry import load_registry
from policydb.settings import Settings


def validate_registry(settings: Settings | None = None) -> dict:
    settings = settings or Settings.discover()
    sources = load_registry(settings)
    ids = [source.source_id for source in sources]
    unresolved = [
        source.source_id
        for source in sources
        if source.scope_type == "unknown"
        or (source.scope_type in {"municipal", "county", "multi_region"} and not source.city_ids)
        or (source.scope_type == "provincial" and not source.province_codes)
    ]
    unofficial_required = [
        source.source_id
        for source in sources
        if source.required_level == "required"
        and source.official_status not in {"official", "official_reprint"}
    ]
    duplicate_ids = sorted({source_id for source_id in ids if ids.count(source_id) > 1})
    return {
        "source_count": len(sources),
        "duplicate_source_ids": duplicate_ids,
        "unresolved_scope_count": len(unresolved),
        "unresolved_source_ids": unresolved,
        "unofficial_required_source_ids": unofficial_required,
        "passed": not duplicate_ids and not unofficial_required,
    }


def unresolved_sources(settings: Settings | None = None) -> pl.DataFrame:
    settings = settings or Settings.discover()
    rows = []
    for source in load_registry(settings):
        reasons = []
        if source.scope_type == "unknown":
            reasons.append("scope_type_unknown")
        if source.scope_type in {"municipal", "county", "multi_region"} and not source.city_ids:
            reasons.append("city_ids_missing")
        if source.scope_type == "provincial" and not source.province_codes:
            reasons.append("province_codes_missing")
        if reasons:
            rows.append(
                {
                    "source_id": source.source_id,
                    "source_name": source.source_name,
                    "domain": source.domain,
                    "scope_type": source.scope_type,
                    "agency_type": source.agency_type,
                    "reasons": ";".join(reasons),
                }
            )
    return pl.DataFrame(rows, infer_schema_length=None) if rows else pl.DataFrame(
        schema={name: pl.String for name in ("source_id", "source_name", "domain", "scope_type", "agency_type", "reasons")}
    )


def export_source_audit(output: Path, settings: Settings | None = None) -> dict:
    settings = settings or Settings.discover()
    output.parent.mkdir(parents=True, exist_ok=True)
    matrix = build_source_matrix(settings)
    unresolved = unresolved_sources(settings)
    if output.suffix.lower() == ".parquet":
        matrix.write_parquet(output, compression="zstd")
        unresolved.write_parquet(output.with_name(output.stem + "_unresolved.parquet"), compression="zstd")
    else:
        matrix.write_csv(output)
        unresolved.write_csv(output.with_name(output.stem + "_unresolved.csv"))
    summary = validate_registry(settings)
    output.with_name(output.stem + "_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return {**summary, "matrix_rows": matrix.height, "output": str(output)}

