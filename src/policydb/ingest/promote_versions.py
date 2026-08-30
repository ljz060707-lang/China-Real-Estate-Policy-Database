"""Promote crawled document versions into the formal records layer.

The crawler intentionally writes immutable-ish document versions first.  This
module is the deterministic, replayable hand-off into ``records.parquet``.
It never invents publication dates or titles: dates come from the crawler's
candidate-date evidence and missing values remain null.  Every write is an
atomic snapshot through :mod:`policydb.parquet_store` and can be repeated for
the same run without adding duplicate records.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from datetime import UTC, date, datetime

import polars as pl

from policydb.crawl.registry import load_registry
from policydb.ingest.excel import RECORD_COLUMNS
from policydb.ingest.relevance import assess_document_relevance
from policydb.parquet_store import atomic_write_parquet, read_parquet_snapshot
from policydb.scope import load_cities_105
from policydb.settings import Settings
from policydb.transform.normalization import (
    clean_text,
    content_hash,
    normalize_title,
    normalize_url,
    stable_id,
)

RECORD_SCHEMA = {
    "record_id": pl.String,
    "record_type": pl.String,
    "title": pl.String,
    "title_normalized": pl.String,
    "record_date": pl.Date,
    "publication_date": pl.Date,
    "issuance_date": pl.Date,
    "effective_date": pl.Date,
    "expiry_date": pl.Date,
    "record_date_original": pl.String,
    "status": pl.String,
    "direction": pl.String,
    "summary": pl.String,
    "full_text": pl.String,
    "language": pl.String,
    "official_level": pl.String,
    "official_status": pl.String,
    "source_quality": pl.Int64,
    "primary_source_url": pl.String,
    "landing_page_url": pl.String,
    "document_url": pl.String,
    "retrieved_at": pl.Datetime(time_unit="us", time_zone="UTC"),
    "content_hash": pl.String,
    "source_file": pl.String,
    "source_sheet": pl.String,
    "source_row": pl.Int64,
    "import_batch_id": pl.String,
    "created_at": pl.Datetime(time_unit="us", time_zone="UTC"),
    "updated_at": pl.Datetime(time_unit="us", time_zone="UTC"),
    "manual_review_status": pl.String,
    "notes": pl.String,
    "geography_original": pl.String,
    "legacy_category": pl.String,
}

DATE_FIELDS = (
    "publication_date",
    "issuance_date",
    "record_date",
    "effective_date",
    "expiry_date",
)


def _parse_date(value: object) -> date | None:
    """Parse an explicitly supplied date; return null rather than guessing."""

    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = clean_text(value)
    if not text:
        return None
    match = re.search(r"(19\d{2}|20\d{2})[-/.年](\d{1,2})[-/.月](\d{1,2})", text)
    if not match:
        return None
    try:
        return date(*(int(part) for part in match.groups()))
    except ValueError:
        return None


def _nonempty(value: object) -> bool:
    return value is not None and (not isinstance(value, str) or bool(value.strip()))


def _now() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


def _read_joined_versions(
    settings: Settings,
    *,
    run_ids: Iterable[str] | None = None,
    run_id: str | None = None,
    document_version_ids: Iterable[str] | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
) -> pl.DataFrame:
    versions_path = settings.curated / "policy_document_versions.parquet"
    if not versions_path.exists():
        return pl.DataFrame()
    versions = read_parquet_snapshot(versions_path)
    if versions.is_empty():
        return versions
    item_path = settings.curated / "crawl_items.parquet"
    if item_path.exists() and "crawl_item_id" in versions.columns:
        items = read_parquet_snapshot(item_path)
        item_columns = [
            column
            for column in ("item_id", "run_id", "city_id", "candidate_date", "candidate_date_source")
            if column in items.columns
        ]
        if "item_id" in item_columns:
            versions = versions.join(
                items.select(item_columns),
                left_on="crawl_item_id",
                right_on="item_id",
                how="left",
            )
    requested_runs = {str(value) for value in (run_ids or ()) if value}
    if run_id:
        requested_runs.add(str(run_id))
    if requested_runs and "run_id" in versions.columns:
        versions = versions.filter(pl.col("run_id").cast(pl.String).is_in(requested_runs))
    requested_versions = {str(value) for value in (document_version_ids or ()) if value}
    if requested_versions:
        versions = versions.filter(
            pl.col("document_version_id").cast(pl.String).is_in(requested_versions)
        )
    if (start_date or end_date) and "candidate_date" in versions.columns:
        parsed = pl.col("candidate_date").cast(pl.String, strict=False).str.to_date(
            "%Y-%m-%d", strict=False
        )
        if start_date:
            versions = versions.filter(parsed >= pl.lit(start_date))
        if end_date:
            versions = versions.filter(parsed <= pl.lit(end_date))
    return versions


def _city_lookup(settings: Settings) -> dict[str, dict]:
    try:
        return {
            str(row["city_id"]): row
            for row in load_cities_105(settings).iter_rows(named=True)
        }
    except (FileNotFoundError, ValueError):
        # Isolated unit tests and read-only recovery environments may not have
        # the 105-city reference file.  Missing geography remains explicit.
        return {}


def _source_lookup(settings: Settings) -> dict[str, object]:
    try:
        return {str(source.source_id): source for source in load_registry(settings)}
    except (FileNotFoundError, ValueError):
        return {}


def _coerce_records(frame: pl.DataFrame) -> pl.DataFrame:
    rows = frame
    if rows.height == 0 and not rows.columns:
        return pl.DataFrame(
            {column: pl.Series([], dtype=dtype) for column, dtype in RECORD_SCHEMA.items()}
        ).select(RECORD_COLUMNS)
    for column, dtype in RECORD_SCHEMA.items():
        if column not in rows.columns:
            rows = rows.with_columns(
                pl.Series(column, [None] * rows.height, dtype=dtype)
            )
        else:
            rows = rows.with_columns(pl.col(column).cast(dtype, strict=False).alias(column))
    return rows.select(RECORD_COLUMNS)


def _record_id(row: dict) -> str:
    existing = clean_text(row.get("record_id"))
    if existing:
        return existing
    url = normalize_url(row.get("final_url") or row.get("canonical_url"))
    title = clean_text(row.get("title"))
    source_id = clean_text(row.get("source_id"))
    if url or title:
        return stable_id(source_id, url, title, prefix="POL")
    return stable_id(row.get("document_version_id"), prefix="POL")


def _incoming_row(
    row: dict,
    *,
    city: dict | None,
    source: object | None,
    batch_id: str,
    now: datetime,
) -> dict | None:
    http_status = row.get("http_status")
    if int(http_status or 0) != 200:
        return None
    title = clean_text(row.get("title"))
    body = clean_text(row.get("extracted_text"))
    if not title or not body:
        return None
    url = normalize_url(row.get("final_url") or row.get("canonical_url"))
    if not url or not url.lower().startswith(("http://", "https://")):
        return None
    candidate_date = _parse_date(row.get("candidate_date"))
    content = clean_text(row.get("content_sha256")) or content_hash(title, body)
    official_status = clean_text(getattr(source, "official_status", None)) or "unknown"
    quality = 5 if official_status in {"official", "official_reprint"} else 0
    content_type = str(row.get("content_type") or "").lower()
    document_url = url if ("pdf" in content_type or "attachment" in content_type) else None
    city_name = clean_text((city or {}).get("city_name"))
    record_id = _record_id(row)
    return {
        "record_id": record_id,
        "record_type": "policy_document",
        "title": title,
        "title_normalized": normalize_title(title),
        "record_date": candidate_date,
        "publication_date": candidate_date,
        "issuance_date": candidate_date,
        "effective_date": None,
        "expiry_date": None,
        "record_date_original": clean_text(row.get("candidate_date")),
        "status": "issued",
        "direction": None,
        "summary": None,
        "full_text": body,
        "language": "zh-CN",
        "official_level": "local" if city_name else None,
        "official_status": official_status,
        "source_quality": quality,
        "primary_source_url": url,
        "landing_page_url": normalize_url(row.get("canonical_url")),
        "document_url": document_url,
        "retrieved_at": now,
        "content_hash": content,
        "source_file": clean_text(row.get("local_path")),
        "source_sheet": None,
        "source_row": None,
        "import_batch_id": batch_id,
        "created_at": now,
        "updated_at": now,
        "manual_review_status": "unreviewed",
        "notes": (
            f"promoted_from_document_version={row.get('document_version_id')};"
            f"parse_status={row.get('parse_status')};"
            f"candidate_date_source={row.get('candidate_date_source')}"
        ),
        "geography_original": city_name,
        "legacy_category": None,
    }


def _merge_record_rows(existing: pl.DataFrame, incoming: list[dict]) -> pl.DataFrame:
    current = _coerce_records(existing) if existing.height or existing.columns else _coerce_records(pl.DataFrame())
    by_id = {str(row["record_id"]): row for row in current.iter_rows(named=True)}
    for row in incoming:
        key = str(row["record_id"])
        old = dict(by_id.get(key) or {})
        for column in RECORD_COLUMNS:
            value = row.get(column)
            if column == "created_at" and _nonempty(old.get(column)):
                continue
            if _nonempty(value):
                old[column] = value
            elif column not in old:
                old[column] = None
        old["record_id"] = key
        by_id[key] = old
    return _coerce_records(pl.DataFrame(list(by_id.values()), infer_schema_length=None))


def _merge_relation(
    settings: Settings,
    name: str,
    rows: list[dict],
    key: str | tuple[str, ...],
    schema: dict[str, pl.DataType] | None = None,
) -> int:
    if not rows:
        return 0
    path = settings.curated / f"{name}.parquet"
    old = read_parquet_snapshot(path) if path.exists() else pl.DataFrame()
    new = pl.DataFrame(rows, schema=schema) if schema else pl.DataFrame(rows, infer_schema_length=None)
    combined = (
        pl.concat([old, new], how="diagonal_relaxed")
        if old.height or old.columns
        else new
    )
    key_columns = (key,) if isinstance(key, str) else tuple(key)
    merged = combined.unique(subset=list(key_columns), keep="last", maintain_order=True)
    atomic_write_parquet(
        merged,
        path,
        {"module": "ingest.promote_versions"},
        key_columns=key_columns,
    )
    return len(rows)


def promote_document_versions(
    settings: Settings | None = None,
    *,
    run_ids: Iterable[str] | None = None,
    run_id: str | None = None,
    document_version_ids: Iterable[str] | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    apply: bool = True,
) -> dict:
    """Promote a bounded version scope and return a durable audit summary."""

    settings = settings or Settings.discover()
    run_id_values = [str(value) for value in (run_ids or ()) if value]
    versions = _read_joined_versions(
        settings,
        run_ids=run_id_values,
        run_id=run_id,
        document_version_ids=document_version_ids,
        start_date=start_date,
        end_date=end_date,
    )
    batch_id = str(run_id or (run_id_values[0] if run_id_values else "VERSION_PROMOTION"))
    if versions.is_empty():
        return {
            "selected_versions": 0,
            "eligible_versions": 0,
            "rejected_versions": 0,
            "promoted_records": 0,
            "new_records": 0,
            "updated_records": 0,
            "record_ids": [],
            "document_version_ids": [],
            "apply": apply,
        }
    cities = _city_lookup(settings)
    sources = _source_lookup(settings)
    incoming: list[dict] = []
    rejected: list[dict] = []
    relation_rows: list[dict] = []
    source_rows: list[dict] = []
    version_updates: dict[str, str] = {}
    version_date_updates: dict[str, tuple[date | None, str | None]] = {}
    now = _now()
    for row in versions.iter_rows(named=True):
        city = cities.get(str(row.get("city_id")))
        source = sources.get(str(row.get("source_id")))
        record = _incoming_row(row, city=city, source=source, batch_id=batch_id, now=now)
        version_id = str(row.get("document_version_id") or "")
        if record is None:
            rejected.append(
                {
                    "document_version_id": version_id,
                    "reason": "requires_http_200_title_body_and_http_url",
                }
            )
            continue
        relevance = assess_document_relevance(
            row.get("title"),
            row.get("extracted_text"),
            source_role=getattr(source, "agency_type", None) or getattr(source, "source_role", None),
        )
        if not relevance.accepted:
            rejected.append(
                {
                    "document_version_id": version_id,
                    "record_id": record["record_id"],
                    "reason": "relevance_gate",
                    "relevance_status": relevance.status,
                    "reason_codes": ";".join(relevance.reason_codes),
                    "positive_terms": ";".join(relevance.positive_terms),
                    "negative_terms": ";".join(relevance.negative_terms),
                    "action_terms": ";".join(relevance.action_terms),
                    "evidence_excerpt": relevance.evidence_excerpt,
                }
            )
            continue
        incoming.append(record)
        version_updates[version_id] = str(record["record_id"])
        version_date_updates[version_id] = (
            record["publication_date"],
            clean_text(row.get("candidate_date_source")),
        )
        city_id = clean_text(row.get("city_id"))
        if city_id:
            jurisdiction_id = stable_id(city_id, prefix="JUR")
            relation_rows.append(
                {
                    "record_id": record["record_id"],
                    "jurisdiction_id": jurisdiction_id,
                    "geography_original": city.get("city_name") if city else None,
                    "jurisdiction_name": city.get("city_name") if city else None,
                    "relation_type": "applicable",
                    "match_method": "crawler_city_scope",
                    "match_confidence": 1.0,
                }
            )
        source_id = clean_text(row.get("source_id"))
        if source_id:
            official_status = clean_text(getattr(source, "official_status", None)) or "unknown"
            source_rows.append(
                {
                    "policy_source_id": stable_id(record["record_id"], source_id, prefix="POLSRC"),
                    "record_id": record["record_id"],
                    "source_id": source_id,
                    "source_url": record["primary_source_url"],
                    "normalized_url": record["primary_source_url"],
                    "source_role": "canonical" if official_status == "official" else "supporting",
                    "is_canonical": official_status == "official",
                    "official_status": official_status,
                    "needs_review": official_status != "official",
                    "created_at": now.isoformat(),
                    "updated_at": now.isoformat(),
                }
            )
    existing_path = settings.curated / "records.parquet"
    existing = read_parquet_snapshot(existing_path) if existing_path.exists() else pl.DataFrame()
    existing_ids = set(existing.get_column("record_id").cast(pl.String).to_list()) if "record_id" in existing.columns else set()
    incoming_ids = {str(row["record_id"]) for row in incoming}
    if apply and incoming:
        _merge_relation(
            settings,
            "record_jurisdictions",
            relation_rows,
            ("record_id", "jurisdiction_id"),
            schema={
                "record_id": pl.String,
                "jurisdiction_id": pl.String,
                "geography_original": pl.String,
                "jurisdiction_name": pl.String,
                "relation_type": pl.String,
                "match_method": pl.String,
                "match_confidence": pl.Float64,
            },
        )
        _merge_relation(settings, "policy_sources", source_rows, "policy_source_id")
        merged = _merge_record_rows(existing, incoming)
        atomic_write_parquet(
            merged,
            existing_path,
            {"module": "ingest.promote_versions", "batch_id": batch_id},
            key_columns=("record_id",),
            expected_schema=RECORD_SCHEMA,
        )
        # Link the immutable version to its formal record only after the
        # destination records snapshot has been durably replaced.  A rerun is
        # safe and repairs an interrupted linkage without deleting versions.
        all_versions = read_parquet_snapshot(settings.curated / "policy_document_versions.parquet")
        updated_versions = all_versions
        if "publication_date" not in updated_versions.columns:
            updated_versions = updated_versions.with_columns(
                pl.Series("publication_date", [None] * updated_versions.height, dtype=pl.Date),
                pl.Series(
                    "publication_date_source",
                    [None] * updated_versions.height,
                    dtype=pl.String,
                ),
            )
        else:
            updated_versions = updated_versions.with_columns(
                pl.col("publication_date").cast(pl.Date, strict=False).alias("publication_date")
            )
        if version_updates:
            update_frame = pl.DataFrame(
                {
                    "document_version_id": list(version_updates),
                    "promoted_record_id": list(version_updates.values()),
                    "publication_date_update": [
                        version_date_updates[version_id][0]
                        for version_id in version_updates
                    ],
                    "publication_date_source_update": [
                        version_date_updates[version_id][1]
                        for version_id in version_updates
                    ],
                }
            )
            updated_versions = (
                updated_versions.join(update_frame, on="document_version_id", how="left")
                .with_columns(
                    pl.coalesce([pl.col("promoted_record_id"), pl.col("record_id")]).alias("record_id"),
                    pl.coalesce(
                        [pl.col("publication_date_update"), pl.col("publication_date")]
                    ).alias("publication_date"),
                    pl.coalesce(
                        [
                            pl.col("publication_date_source_update"),
                            pl.col("publication_date_source"),
                        ]
                    ).alias("publication_date_source"),
                )
                .drop(
                    "promoted_record_id",
                    "publication_date_update",
                    "publication_date_source_update",
                )
            )
            atomic_write_parquet(
                updated_versions,
                settings.curated / "policy_document_versions.parquet",
                {"module": "ingest.promote_versions", "batch_id": batch_id},
                key_columns=("document_version_id",),
            )
    return {
        "selected_versions": versions.height,
        "eligible_versions": len(incoming),
        "rejected_versions": len(rejected),
        "rejected": rejected,
        "promoted_records": len(incoming),
        "new_records": len(incoming_ids - existing_ids),
        "updated_records": len(incoming_ids & existing_ids),
        "record_ids": sorted(incoming_ids),
        "document_version_ids": sorted(version_updates),
        "apply": apply,
        "batch_id": batch_id,
    }


__all__ = ["promote_document_versions"]
