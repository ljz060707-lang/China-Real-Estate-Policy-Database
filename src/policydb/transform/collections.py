from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

import polars as pl
import yaml

from policydb.settings import Settings


def _load_yaml(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def _cell_id(source_hash: str | None, sheet: str, cell: str) -> str:
    value = f"{source_hash or ''}|{sheet}|{cell}"
    return f"CELL_{hashlib.sha256(value.encode('utf-8')).hexdigest()[:24].upper()}"


def _taxonomy(settings: Settings) -> dict:
    return _load_yaml(settings.root / "config" / "taxonomy.yml")["research_collections"]


def _collection_names(
    taxonomy: dict, collection_code: str, subcollection_code: str | None
) -> tuple[str, str | None]:
    collection = taxonomy[collection_code]
    subcollections = collection["subcollections"]
    if subcollection_code is not None and subcollection_code not in subcollections:
        raise ValueError(f"Unknown subcollection: {collection_code}.{subcollection_code}")
    return collection["name"], subcollections.get(subcollection_code)


def _source_mapping_rows(settings: Settings) -> tuple[list[dict], dict]:
    taxonomy = _taxonomy(settings)
    config = _load_yaml(settings.root / "config" / "collection_mapping.yml")
    rows: list[dict] = []
    for sheet, mapping in config["sheet_mappings"].items():
        assignments = [("primary", mapping["primary"])] + [
            ("secondary", item) for item in mapping.get("secondary", [])
        ]
        for role, assignment in assignments:
            collection_code = assignment["collection"]
            subcollection_code = assignment.get("subcollection")
            collection_name, subcollection_name = _collection_names(
                taxonomy, collection_code, subcollection_code
            )
            rows.append(
                {
                    "source_sheet": sheet,
                    "collection_code": collection_code,
                    "collection_name": collection_name,
                    "subcollection_code": subcollection_code,
                    "subcollection_name": subcollection_name,
                    "mapping_role": role,
                    "source_kind": mapping["source_kind"],
                    "rationale": mapping["rationale"],
                    "review_status": "approved",
                }
            )
    return rows, config


def _read_staging_cells(settings: Settings) -> pl.DataFrame:
    files = sorted((settings.root / "data" / "staging" / "excel").glob("*.parquet"))
    if not files:
        raise FileNotFoundError("No Excel staging Parquet files found")
    return pl.concat([pl.read_parquet(path) for path in files], how="vertical_relaxed")


def _build_cell_catalog(
    staging: pl.DataFrame, source_mappings: pl.DataFrame
) -> pl.DataFrame:
    primary = source_mappings.filter(pl.col("mapping_role") == "primary").select(
        "source_sheet",
        "collection_code",
        "collection_name",
        "subcollection_code",
        "subcollection_name",
        "source_kind",
    )
    catalog = (
        staging.select(
            pl.struct("source_file_sha256", "source_sheet_name", "source_cell")
            .map_elements(
                lambda row: _cell_id(
                    row["source_file_sha256"], row["source_sheet_name"], row["source_cell"]
                ),
                return_dtype=pl.String,
            )
            .alias("cell_id"),
            pl.col("source_sheet_name").alias("source_sheet"),
            "source_cell",
            "source_row",
            "source_column",
            "original_field_name",
            "is_formula",
            "is_merged",
            "import_batch_id",
            "source_file",
            "source_file_sha256",
        )
        .join(primary, on="source_sheet", how="left", validate="m:1")
        .sort("source_sheet", "source_row", "source_column")
    )
    return catalog


def _record_collection_rows(
    records: pl.DataFrame, source_rows: list[dict], config: dict, taxonomy: dict
) -> list[dict]:
    source_index: dict[str, list[dict]] = {}
    for row in source_rows:
        source_index.setdefault(row["source_sheet"], []).append(row)
    assignments: dict[tuple[str, str, str | None], dict] = {}

    def add(
        record: dict,
        collection_code: str,
        subcollection_code: str | None,
        *,
        source: str,
        confidence: float,
        evidence: str,
        review_status: str,
        is_primary: bool = False,
    ) -> None:
        collection_name, subcollection_name = _collection_names(
            taxonomy, collection_code, subcollection_code
        )
        key = (record["record_id"], collection_code, subcollection_code)
        candidate = {
            "record_id": record["record_id"],
            "collection_code": collection_code,
            "collection_name": collection_name,
            "subcollection_code": subcollection_code,
            "subcollection_name": subcollection_name,
            "classification_source": source,
            "confidence": confidence,
            "evidence_excerpt": evidence[:500],
            "review_status": review_status,
            "is_primary": is_primary,
            "source_sheet": record.get("source_sheet"),
        }
        previous = assignments.get(key)
        if previous is None or confidence > previous["confidence"]:
            assignments[key] = candidate

    fields = ("title", "summary", "full_text", "legacy_category", "notes")
    for record in records.iter_rows(named=True):
        sheet = record.get("source_sheet")
        for mapping in source_index.get(sheet, []):
            add(
                record,
                mapping["collection_code"],
                mapping["subcollection_code"],
                source="source_sheet",
                confidence=1.0 if mapping["mapping_role"] == "primary" else 0.95,
                evidence=f"来源工作表：{sheet}；{mapping['rationale']}",
                review_status="approved",
                is_primary=mapping["mapping_role"] == "primary",
            )

        geography = str(record.get("geography_original") or "").strip()
        official_level = str(record.get("official_level") or "").strip()
        is_central = official_level == "central" or geography in {"全国", "中央", "国家"}
        is_local = bool(geography and geography not in {"全国", "中央", "国家"}) or (
            official_level == "local"
        )
        if is_central:
            add(
                record,
                "central_party_state",
                None,
                source="jurisdiction_rule",
                confidence=0.75,
                evidence=f"official_level={official_level}; geography={geography}",
                review_status="unreviewed",
            )
        if is_local:
            add(
                record,
                "local_government",
                None,
                source="jurisdiction_rule",
                confidence=0.75,
                evidence=f"official_level={official_level}; geography={geography}",
                review_status="unreviewed",
            )

        values = [(field, str(record.get(field) or "")) for field in fields]
        for rule in config.get("record_rules", []):
            if rule["collection"] == "central_party_state" and not is_central:
                continue
            if rule["collection"] == "local_government" and not is_local:
                continue
            match = next(
                (
                    (field, value, keyword)
                    for field, value in values
                    for keyword in rule["keywords"]
                    if keyword.lower() in value.lower()
                ),
                None,
            )
            if match:
                field, value, keyword = match
                position = value.lower().find(keyword.lower())
                excerpt = value[max(0, position - 80) : position + len(keyword) + 120]
                add(
                    record,
                    rule["collection"],
                    rule["subcollection"],
                    source=f"rule:{rule['rule_id']}",
                    confidence=float(rule["confidence"]),
                    evidence=f"{field}命中“{keyword}”：{excerpt}",
                    review_status="unreviewed",
                )

        if not any(key[0] == record["record_id"] for key in assignments):
            add(
                record,
                "tracking_statistics",
                "policy_statistics",
                source="fallback",
                confidence=0.5,
                evidence=f"来源工作表：{sheet}；未获得更具体的确定性归属",
                review_status="pending",
                is_primary=True,
            )
    return list(assignments.values())


def build_collection_layer(settings: Settings | None = None) -> dict:
    settings = settings or Settings.discover()
    source_rows, config = _source_mapping_rows(settings)
    taxonomy = _taxonomy(settings)
    staging = _read_staging_cells(settings)
    source_mappings = pl.DataFrame(source_rows, infer_schema_length=None)
    staging_sheets = set(staging["source_sheet_name"].unique().to_list())
    configured_sheets = set(config["sheet_mappings"])
    missing_mappings = sorted(staging_sheets - configured_sheets)
    unknown_mappings = sorted(configured_sheets - staging_sheets)
    if missing_mappings or unknown_mappings:
        raise ValueError(
            f"Sheet mapping mismatch: missing={missing_mappings}, unknown={unknown_mappings}"
        )

    cell_catalog = _build_cell_catalog(staging, source_mappings)
    records = pl.read_parquet(settings.curated / "records.parquet")
    record_rows = _record_collection_rows(records, source_rows, config, taxonomy)
    record_collections = pl.DataFrame(record_rows, infer_schema_length=None).sort(
        "record_id", "collection_code", "subcollection_code"
    )
    settings.curated.mkdir(parents=True, exist_ok=True)
    source_mappings.write_parquet(
        settings.curated / "source_sheet_collections.parquet", compression="zstd"
    )
    cell_catalog.write_parquet(
        settings.curated / "staging_cell_catalog.parquet", compression="zstd"
    )
    record_collections.write_parquet(
        settings.curated / "record_collections.parquet", compression="zstd"
    )

    classified_records = record_collections["record_id"].n_unique()
    subclassified_records = record_collections.filter(
        pl.col("subcollection_code").is_not_null()
    )["record_id"].n_unique()
    report = {
        "created_at": datetime.now(UTC).isoformat(),
        "collection_count": len(taxonomy),
        "subcollection_count": sum(
            len(item["subcollections"]) for item in taxonomy.values()
        ),
        "staging_sheet_count": len(staging_sheets),
        "mapped_sheet_count": source_mappings["source_sheet"].n_unique(),
        "staging_cell_count": staging.height,
        "catalog_cell_count": cell_catalog.height,
        "unmapped_sheet_count": len(missing_mappings),
        "unmapped_cell_count": cell_catalog.filter(
            pl.col("collection_code").is_null()
        ).height,
        "record_count": records.height,
        "classified_record_count": classified_records,
        "subclassified_record_count": subclassified_records,
        "unclassified_record_count": records.height - classified_records,
        "record_collection_relation_count": record_collections.height,
        "collection_record_counts": {
            row["collection_code"]: row["record_count"]
            for row in record_collections.group_by("collection_code")
            .agg(pl.col("record_id").n_unique().alias("record_count"))
            .sort("collection_code")
            .iter_rows(named=True)
        },
        "passed": not missing_mappings
        and not unknown_mappings
        and cell_catalog.height == staging.height
        and classified_records == records.height,
    }
    report_path = settings.root / "data" / "staging" / "collection_coverage_report.json"
    report_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return report
