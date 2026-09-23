"""Import heterogeneous external policy references without promoting them as facts.

The external files used by the project are useful for breadth and gap finding, but
many rows are summaries or media links rather than verified primary documents.  This
module therefore creates a separate, append-only reference layer.  It preserves the
raw files and cell-level lineage, derives a city-year reconciliation, and emits a
fetch-priority guide.  It deliberately does not classify, promote, or mutate crawl
queues.
"""

from __future__ import annotations

import csv
import hashlib
import json
import os
import re
import shutil
from collections import Counter, defaultdict
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import polars as pl
from openpyxl import load_workbook
from openpyxl.utils.datetime import from_excel

from policydb.parquet_store import atomic_write_parquet, read_parquet_snapshot
from policydb.settings import Settings
from policydb.transform.normalization import (
    clean_text,
    content_hash,
    normalize_title,
    normalize_url,
    stable_id,
)

DATASET_SLUG = "china_city_real_estate_policy_2024_07"
REFERENCE_ROOT = "external_reference"
HEADER_MARKERS = (
    "城市",
    "区域",
    "地区",
    "日期",
    "时间",
    "标题",
    "文件",
    "内容",
    "链接",
    "网址",
    "分类",
    "政策类型",
)


def _text(value: Any) -> str | None:
    if value is None:
        return None
    cleaned = clean_text(value)
    return cleaned or None


def _file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _normalise_city_text(value: Any) -> str:
    text = _text(value) or ""
    return re.sub(r"[\s·,，。；;、()（）/\\_-]", "", text).lower()


def _load_city_registry(settings: Settings) -> tuple[dict[str, dict[str, str]], dict[str, dict[str, str]]]:
    path = settings.root / "data" / "reference" / "cities_105.csv"
    by_id: dict[str, dict[str, str]] = {}
    aliases: dict[str, dict[str, str]] = {}
    if not path.exists():
        return by_id, aliases
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            city_id = _text(row.get("city_id"))
            city_name = _text(row.get("city_name"))
            if not city_id or not city_name:
                continue
            info = {
                "city_id": city_id,
                "city_name": city_name,
                "province_name": _text(row.get("province_name")) or "",
            }
            by_id[city_id] = info
            candidates = [
                city_name,
                _text(row.get("city_name_short")) or "",
                *(_text(row.get("aliases")) or "").split("|"),
            ]
            for candidate in candidates:
                normalised = _normalise_city_text(candidate)
                if len(normalised) >= 2:
                    aliases.setdefault(normalised, info)
    return by_id, aliases


def _match_city(value: Any, aliases: dict[str, dict[str, str]]) -> tuple[dict[str, str] | None, str]:
    raw = _text(value) or ""
    normalised = _normalise_city_text(raw)
    if not normalised:
        return None, "none"
    for alias in sorted(aliases, key=len, reverse=True):
        if alias == normalised or alias in normalised:
            return aliases[alias], "registry_alias"
    return None, "unmapped"


def _parse_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, (int, float)) and 20000 <= float(value) <= 100000:
        try:
            converted = from_excel(value)
            return converted.date() if isinstance(converted, datetime) else converted
        except (TypeError, ValueError, OverflowError):
            return None
    text = _text(value)
    if not text:
        return None
    match = re.search(
        r"((?:19|20)\d{2})\s*[年./-]\s*(\d{1,2})\s*[月./-]\s*(\d{1,2})",
        text,
    )
    if not match:
        return None
    try:
        return date(*(int(part) for part in match.groups()))
    except ValueError:
        return None


def _year(value: Any) -> int | None:
    parsed = _parse_date(value)
    if parsed:
        return parsed.year
    text = _text(value) or ""
    # IDs in article URLs often contain an eight-digit number that looks like
    # a year (for example ``...forward_20282923``).  They are not date evidence.
    if re.search(r"(?:https?://|www\.)", text, flags=re.IGNORECASE):
        return None
    match = re.search(r"((?:19|20)\d{2})", text)
    return int(match.group(1)) if match else None


def _first_date(values: list[Any]) -> tuple[Any, date | None, int | None]:
    for value in values:
        parsed = _parse_date(value)
        if parsed:
            return value, parsed, parsed.year
    for value in values:
        parsed_year = _year(value)
        if parsed_year:
            return value, None, parsed_year
    return None, None, None


def _header_index(rows: list[list[Any]]) -> int | None:
    for index, row in enumerate(rows[:20]):
        labels = [_text(value) or "" for value in row]
        score = sum(any(marker in label for marker in HEADER_MARKERS) for label in labels)
        if score >= 2:
            return index
    return None


def _column_index(header: list[Any], terms: tuple[str, ...], excludes: tuple[str, ...] = ()) -> int | None:
    for index, value in enumerate(header):
        label = _text(value) or ""
        if any(term in label for term in terms) and not any(term in label for term in excludes):
            return index
    return None


def _join_values(values: list[Any]) -> str | None:
    parts = [_text(value) for value in values]
    nonempty = [part for part in parts if part]
    return "；".join(nonempty) if nonempty else None


def _make_record(
    *,
    dataset_id: str,
    batch_id: str,
    source_file: dict[str, Any],
    sheet_index: int,
    sheet_name: str,
    source_row: int,
    source_column: int,
    city_raw: Any,
    date_raw: Any,
    title_raw: Any,
    content_raw: Any,
    category_raw: Any,
    source_url_raw: Any,
    city_info: dict[str, str] | None,
    city_match_method: str,
) -> dict[str, Any] | None:
    title = _text(title_raw)
    content = _text(content_raw)
    category = _text(category_raw)
    url = _text(source_url_raw)
    raw_city = _text(city_raw)
    parsed_date = _parse_date(date_raw)
    parsed_year = parsed_date.year if parsed_date else _year(date_raw)
    if not any((title, content, category, url, raw_city, _text(date_raw))):
        return None
    reference_text = " ".join(part for part in (title, content, category) if part)
    canonical_city = city_info["city_name"] if city_info else raw_city
    scope_level = "city" if city_info else ("national" if "全国" in (raw_city or "") else "unknown")
    return {
        "reference_record_id": stable_id(
            dataset_id,
            source_file["sha256"],
            sheet_name,
            source_row,
            source_column,
            city_raw,
            date_raw,
            title,
            content,
            prefix="EXTREF",
        ),
        "dataset_id": dataset_id,
        "import_batch_id": batch_id,
        "source_file": source_file["name"],
        "source_file_sha256": source_file["sha256"],
        "source_sheet_name": sheet_name,
        "source_sheet_index": sheet_index,
        "source_row": source_row,
        "source_column": source_column,
        "city_name_raw": raw_city,
        "city_name": canonical_city,
        "city_id": city_info["city_id"] if city_info else None,
        "province_name": city_info["province_name"] if city_info else None,
        "city_match_method": city_match_method,
        "scope_level": scope_level,
        "year": parsed_year,
        "date_raw": _text(date_raw),
        "date_normalized": parsed_date.isoformat() if parsed_date else None,
        "title_raw": title,
        "title_normalized": normalize_title(title) if title else None,
        "content_raw": content,
        "category_raw": category,
        "source_url_raw": url,
        "source_url_normalized": normalize_url(url) if url else None,
        "reference_content_sha256": content_hash(reference_text) if reference_text else None,
        "reference_quality": "UNVERIFIED_EXTERNAL_REFERENCE",
        "official_evidence_status": "UNVERIFIED",
        "classification_status": "NOT_RUN",
        "api_classification_status": "NOT_RUN",
        "formal_promotion_status": "NOT_PROMOTED",
        "review_status": "REFERENCE_ONLY",
        "created_at": datetime.now(UTC).isoformat(),
        "updated_at": datetime.now(UTC).isoformat(),
    }


def _matrix_groups(
    header: list[Any], aliases: dict[str, dict[str, str]]
) -> tuple[dict[str, list[int]], dict[str, str]]:
    groups: dict[str, list[int]] = defaultdict(list)
    raw_names: dict[str, str] = {}
    current: dict[str, str] | None = None
    current_raw = ""
    for index, value in enumerate(header, 1):
        city_info, _ = _match_city(value, aliases)
        if city_info:
            current = city_info
            current_raw = _text(value) or city_info["city_name"]
        if current:
            groups[current["city_id"]].append(index)
            raw_names[current["city_id"]] = current_raw
    return dict(groups), raw_names


def _stage_cells(
    workbook: Any,
    source_file: dict[str, Any],
    batch_id: str,
    dataset_id: str,
) -> list[dict[str, Any]]:
    cells: list[dict[str, Any]] = []
    for sheet_index, worksheet in enumerate(workbook.worksheets, 1):
        merged_lookup: dict[str, tuple[str, str]] = {}
        for merged in worksheet.merged_cells.ranges:
            anchor = worksheet.cell(merged.min_row, merged.min_col).coordinate
            for row in range(merged.min_row, merged.max_row + 1):
                for column in range(merged.min_col, merged.max_col + 1):
                    coordinate = worksheet.cell(row, column).coordinate
                    merged_lookup[coordinate] = (str(merged), anchor)
        for row in worksheet.iter_rows():
            for cell in row:
                merged_range, merged_anchor = merged_lookup.get(cell.coordinate, (None, None))
                if cell.value is None and merged_range is None:
                    continue
                cells.append(
                    {
                        "cell_id": stable_id(
                            dataset_id,
                            source_file["sha256"],
                            worksheet.title,
                            cell.coordinate,
                            prefix="EXTCELL",
                        ),
                        "dataset_id": dataset_id,
                        "import_batch_id": batch_id,
                        "source_file": source_file["name"],
                        "source_file_sha256": source_file["sha256"],
                        "source_sheet_name": worksheet.title,
                        "sheet_index": sheet_index,
                        "sheet_state": worksheet.sheet_state,
                        "source_row": cell.row,
                        "source_column": cell.column,
                        "source_cell": cell.coordinate,
                        "original_field_name": _text(worksheet.cell(1, cell.column).value),
                        "cell_value": _text(cell.value),
                        "cell_data_type": cell.data_type,
                        "is_formula": cell.data_type == "f",
                        "formula_text": str(cell.value) if cell.data_type == "f" else None,
                        "is_merged": merged_range is not None,
                        "merged_range": merged_range,
                        "merged_anchor": merged_anchor,
                    }
                )
    return cells


def _extract_workbook(
    workbook: Any,
    source_file: dict[str, Any],
    batch_id: str,
    dataset_id: str,
    aliases: dict[str, dict[str, str]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    records: list[dict[str, Any]] = []
    for sheet_index, worksheet in enumerate(workbook.worksheets, 1):
        rows = [[cell.value for cell in row] for row in worksheet.iter_rows()]
        if not rows:
            continue
        header_index = _header_index(rows)
        if header_index is not None:
            header = rows[header_index]
            city_column = _column_index(
                header,
                ("城市名称", "省/市/县", "区域", "地区", "政策所属地区"),
                ("等级",),
            )
            date_column = _column_index(header, ("日期", "时间", "颁布", "成文"))
            title_column = _column_index(header, ("标题", "文件"))
            content_column = _column_index(header, ("政策内容", "主要内容", "核心内容", "内容"))
            category_column = _column_index(header, ("分类", "类型", "关键词", "政策类型"))
            url_column = _column_index(header, ("链接", "网址", "url"))
            sheet_city, sheet_city_method = _match_city(worksheet.title, aliases)
            for source_row, row in enumerate(rows[header_index + 1 :], header_index + 2):
                if not any(value is not None for value in row):
                    continue
                city_raw = row[city_column] if city_column is not None and city_column < len(row) else None
                city_info, city_method = _match_city(city_raw, aliases)
                if city_info is None and city_raw is None and sheet_city:
                    city_info, city_method, city_raw = sheet_city, sheet_city_method, worksheet.title
                date_raw = row[date_column] if date_column is not None and date_column < len(row) else None
                if date_raw is None:
                    date_raw, _, _ = _first_date(row)
                title_raw = row[title_column] if title_column is not None and title_column < len(row) else None
                content_raw = row[content_column] if content_column is not None and content_column < len(row) else None
                category_raw = row[category_column] if category_column is not None and category_column < len(row) else None
                url_raw = row[url_column] if url_column is not None and url_column < len(row) else None
                if content_raw is None and title_raw is None:
                    content_raw = _join_values(row)
                record = _make_record(
                    dataset_id=dataset_id,
                    batch_id=batch_id,
                    source_file=source_file,
                    sheet_index=sheet_index,
                    sheet_name=worksheet.title,
                    source_row=source_row,
                    source_column=(city_column or 0) + 1,
                    city_raw=city_raw,
                    date_raw=date_raw,
                    title_raw=title_raw,
                    content_raw=content_raw,
                    category_raw=category_raw,
                    source_url_raw=url_raw,
                    city_info=city_info,
                    city_match_method=city_method,
                )
                if record:
                    records.append(record)
            continue

        header = rows[0]
        groups, raw_names = _matrix_groups(header, aliases)
        if groups:
            for source_row, row in enumerate(rows[1:], 2):
                for city_id, columns in groups.items():
                    values = [row[column - 1] for column in columns if column - 1 < len(row)]
                    if not any(value is not None for value in values):
                        continue
                    date_raw, _, _ = _first_date(values)
                    labels = [header[column - 1] for column in columns if column - 1 < len(header)]
                    title = _join_values(labels)
                    content = _join_values(values)
                    city_info = aliases.get(_normalise_city_text(raw_names[city_id]))
                    record = _make_record(
                        dataset_id=dataset_id,
                        batch_id=batch_id,
                        source_file=source_file,
                        sheet_index=sheet_index,
                        sheet_name=worksheet.title,
                        source_row=source_row,
                        source_column=columns[0],
                        city_raw=raw_names[city_id],
                        date_raw=date_raw,
                        title_raw=title,
                        content_raw=content,
                        category_raw=None,
                        source_url_raw=None,
                        city_info=city_info,
                        city_match_method="registry_alias",
                    )
                    if record:
                        records.append(record)
            continue

        sheet_city, sheet_city_method = _match_city(worksheet.title, aliases)
        for source_row, row in enumerate(rows[1:], 2):
            if not any(value is not None for value in row):
                continue
            date_raw, _, _ = _first_date(row)
            row_city, row_city_method = _match_city(_join_values(row[:3]), aliases)
            city_info = row_city or sheet_city
            city_method = row_city_method if row_city else sheet_city_method
            record = _make_record(
                dataset_id=dataset_id,
                batch_id=batch_id,
                source_file=source_file,
                sheet_index=sheet_index,
                sheet_name=worksheet.title,
                source_row=source_row,
                source_column=1,
                city_raw=_join_values(row[:3]) if row_city else (worksheet.title if sheet_city else None),
                date_raw=date_raw,
                title_raw=_text(row[0]) if row else None,
                content_raw=_join_values(row[1:]),
                category_raw=None,
                source_url_raw=None,
                city_info=city_info,
                city_match_method=city_method,
            )
            if record:
                records.append(record)
    return records, _stage_cells(workbook, source_file, batch_id, dataset_id)


def _file_metadata(path: Path, raw_path: Path, sha256: str, dataset_id: str) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "dataset_id": dataset_id,
        "name": path.name,
        "suffix": path.suffix.lower(),
        "bytes": path.stat().st_size,
        "sha256": sha256,
        "raw_path": str(raw_path),
        "kind": "spreadsheet" if path.suffix.lower() in {".xlsx", ".xlsm"} else path.suffix.lower().lstrip(".") or "file",
        "pdf_page_count": None,
        "pdf_text_char_count": None,
    }
    if path.suffix.lower() == ".pdf":
        try:
            import fitz

            with fitz.open(path) as document:
                metadata["pdf_page_count"] = len(document)
                metadata["pdf_text_char_count"] = sum(len(page.get_text()) for page in document)
        except Exception:  # noqa: BLE001 - archive metadata must not block the import
            metadata["pdf_metadata_status"] = "UNAVAILABLE"
    return metadata


def _empty_frame(columns: list[str]) -> pl.DataFrame:
    return pl.DataFrame({column: pl.Series([], dtype=pl.String) for column in columns})


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
    temporary.replace(path)


def _configured_years(settings: Settings) -> list[int]:
    path = settings.root / "config" / "crpd_autonomous.json"
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        crawl = payload.get("crawl") if isinstance(payload.get("crawl"), dict) else {}
        start = _year(crawl.get("start_date") or payload.get("start_date"))
        end_value = crawl.get("end_date") or payload.get("end_date")
        end = date.today().year if str(end_value).lower() == "today" else _year(end_value)
        return list(range(start, end + 1)) if start and end and end >= start else []
    except (OSError, json.JSONDecodeError, TypeError):
        return []


def _existing_city_year_counts(settings: Settings) -> tuple[dict[tuple[str, int], int], dict[tuple[str, int], int], dict[tuple[str, int], dict[str, Any]]]:
    item_path = settings.curated / "crawl_items.parquet"
    version_path = settings.curated / "policy_document_versions.parquet"
    shard_path = settings.curated / "crawl_shards.parquet"
    progress_path = settings.curated / "city_year_progress.parquet"
    item_lookup: dict[str, tuple[str | None, int | None]] = {}
    if item_path.exists():
        try:
            items = read_parquet_snapshot(item_path, columns=["item_id", "city_id", "query_year"])
            item_lookup = {
                str(row["item_id"]): (row.get("city_id"), _year(row.get("query_year")))
                for row in items.iter_rows(named=True)
            }
        except Exception:  # noqa: BLE001 - comparison is additive and read-only
            item_lookup = {}
    version_ids: dict[tuple[str, int], set[str]] = defaultdict(set)
    if version_path.exists():
        try:
            versions = read_parquet_snapshot(
                version_path,
                columns=["document_version_id", "crawl_item_id", "publication_date"],
            )
            for row in versions.iter_rows(named=True):
                city_id, item_year = item_lookup.get(str(row.get("crawl_item_id") or ""), (None, None))
                year_value = _year(row.get("publication_date")) or item_year
                if city_id and year_value:
                    version_ids[(str(city_id), year_value)].add(str(row.get("document_version_id") or ""))
        except Exception:  # noqa: BLE001
            pass
    existing_versions = {key: len(values) for key, values in version_ids.items()}
    shard_documents: dict[tuple[str, int], int] = defaultdict(int)
    if shard_path.exists():
        try:
            shards = read_parquet_snapshot(
                shard_path,
                columns=["city_id", "start_date", "document_versions"],
            )
            for row in shards.iter_rows(named=True):
                city_id = _text(row.get("city_id"))
                year_value = _year(row.get("start_date"))
                if city_id and year_value:
                    shard_documents[(city_id, year_value)] += int(row.get("document_versions") or 0)
        except Exception:  # noqa: BLE001
            pass
    progress: dict[tuple[str, int], dict[str, Any]] = {}
    if progress_path.exists():
        try:
            progress_frame = read_parquet_snapshot(progress_path)
            for row in progress_frame.iter_rows(named=True):
                city_id = _text(row.get("city_id"))
                year_value = _year(row.get("year"))
                if city_id and year_value:
                    previous = progress.get((city_id, year_value))
                    if previous is None or str(row.get("updated_at") or "") >= str(previous.get("updated_at") or ""):
                        progress[(city_id, year_value)] = {
                            "status": row.get("status"),
                            "overall_completion_pct": row.get("overall_completion_pct"),
                            "updated_at": row.get("updated_at"),
                        }
        except Exception:  # noqa: BLE001
            pass
    return existing_versions, dict(shard_documents), progress


def _build_reconciliation(
    settings: Settings,
    records: list[dict[str, Any]],
    registry: dict[str, dict[str, str]],
    dataset_id: str = DATASET_SLUG,
) -> list[dict[str, Any]]:
    reference_counts: dict[tuple[str, int], int] = defaultdict(int)
    reference_sources: dict[tuple[str, int], set[str]] = defaultdict(set)
    years = set(_configured_years(settings))
    for row in records:
        city_id = _text(row.get("city_id"))
        year_value = row.get("year")
        if city_id and isinstance(year_value, int):
            reference_counts[(city_id, year_value)] += 1
            reference_sources[(city_id, year_value)].add(str(row.get("source_file") or ""))
            years.add(year_value)
    existing_versions, shard_documents, progress = _existing_city_year_counts(settings)
    years.update(year for _, year in existing_versions)
    years.update(year for _, year in shard_documents)
    years.update(year for _, year in progress)
    if not years:
        years.add(date.today().year)
    rows: list[dict[str, Any]] = []
    for city_id, city in registry.items():
        for year_value in sorted(years):
            key = (city_id, year_value)
            reference_count = reference_counts.get(key, 0)
            existing_count = existing_versions.get(key, 0)
            shard_count = shard_documents.get(key, 0)
            progress_row = progress.get(key, {})
            if reference_count and not existing_count and not shard_count:
                status = "REFERENCE_ONLY"
                priority = 0
                reason = "external reference observed; no current crawl document evidence"
            elif reference_count and (existing_count or shard_count):
                status = "REFERENCE_AND_EXISTING"
                priority = 1
                reason = "external reference observed; reconcile with existing crawl evidence"
            elif existing_count or shard_count:
                status = "EXISTING_ONLY"
                priority = 2
                reason = "existing crawl evidence without external reference row"
            else:
                status = "NO_REFERENCE_OR_EXISTING"
                priority = 3
                reason = "no evidence observed in either source"
            rows.append(
                {
                    "reconciliation_id": stable_id(city_id, year_value, prefix="EXTRECON"),
                    "dataset_id": dataset_id,
                    "city_id": city_id,
                    "city_name": city["city_name"],
                    "province_name": city["province_name"],
                    "year": year_value,
                    "reference_row_count": reference_count,
                    "reference_source_file_count": len(reference_sources.get(key, set())),
                    "existing_document_version_count": existing_count,
                    "existing_shard_document_count": shard_count,
                    "crawl_status": progress_row.get("status"),
                    "crawl_overall_completion_pct": progress_row.get("overall_completion_pct"),
                    "reconciliation_status": status,
                    "crawl_priority": priority,
                    "priority_reason": reason,
                    "reference_gap_for_crawl": bool(reference_count and not existing_count),
                    "queue_mutation": False,
                }
            )
    return rows


def import_reference_folder(folder: str | Path, settings: Settings | None = None) -> dict[str, Any]:
    """Archive and reconcile an external reference folder without classification."""

    settings = settings or Settings.discover()
    source_dir = Path(folder).expanduser().resolve()
    if not source_dir.is_dir():
        raise NotADirectoryError(source_dir)
    source_paths = sorted((path for path in source_dir.iterdir() if path.is_file()), key=lambda path: path.name)
    if not source_paths:
        raise FileNotFoundError(f"No files found in {source_dir}")
    file_hashes = [(path.name, _file_hash(path)) for path in source_paths]
    dataset_fingerprint = hashlib.sha256(
        "\n".join(f"{name}\t{digest}" for name, digest in file_hashes).encode("utf-8")
    ).hexdigest()
    dataset_id = f"{DATASET_SLUG}_{dataset_fingerprint[:16]}"
    batch_id = f"external_reference_{dataset_fingerprint[:16]}"
    raw_dir = settings.raw / REFERENCE_ROOT / dataset_id
    staging_dir = settings.root / "data" / "staging" / REFERENCE_ROOT / dataset_id
    curated_dir = settings.curated / REFERENCE_ROOT / dataset_id
    output_dir = settings.outputs / REFERENCE_ROOT / dataset_id
    for path in (raw_dir, staging_dir, curated_dir, output_dir):
        path.mkdir(parents=True, exist_ok=True)

    source_files: list[dict[str, Any]] = []
    for source_path, (_, digest) in zip(source_paths, file_hashes, strict=True):
        destination = raw_dir / source_path.name
        if destination.exists() and _file_hash(destination) != digest:
            destination = raw_dir / f"{source_path.stem}_{digest[:8]}{source_path.suffix}"
        if not destination.exists():
            shutil.copy2(source_path, destination)
        if _file_hash(destination) != digest:
            raise RuntimeError(f"Raw copy hash mismatch: {source_path}")
        source_files.append(_file_metadata(source_path, destination, digest, dataset_id))

    registry, aliases = _load_city_registry(settings)
    records: list[dict[str, Any]] = []
    cells: list[dict[str, Any]] = []
    for source_file in source_files:
        if source_file["suffix"] not in {".xlsx", ".xlsm"}:
            continue
        # ``raw_path`` may differ from the source basename when a previous raw
        # import retained a same-name file with a different hash.  Always parse
        # the exact hash-checked copy that was archived above.
        workbook = load_workbook(Path(source_file["raw_path"]), data_only=False, read_only=False)
        workbook_records, workbook_cells = _extract_workbook(
            workbook,
            source_file,
            batch_id,
            dataset_id,
            aliases,
        )
        records.extend(workbook_records)
        cells.extend(workbook_cells)
        workbook.close()
    records = list({row["reference_record_id"]: row for row in records}.values())
    unmapped: dict[str, dict[str, Any]] = {}
    for row in records:
        raw_city = _text(row.get("city_name_raw"))
        if raw_city and not row.get("city_id") and row.get("scope_level") != "national":
            entry = unmapped.setdefault(raw_city, {"city_name_raw": raw_city, "row_count": 0, "source_files": set()})
            entry["row_count"] += 1
            entry["source_files"].add(str(row.get("source_file") or ""))
    unmapped_rows = [
        {
            "unmapped_location_id": stable_id(dataset_id, key, prefix="EXTUNMAPPED"),
            "dataset_id": dataset_id,
            "city_name_raw": key,
            "row_count": value["row_count"],
            "source_files": sorted(value["source_files"]),
            "status": "NEEDS_CITY_MAPPING",
        }
        for key, value in sorted(unmapped.items())
    ]
    reconciliation = _build_reconciliation(settings, records, registry, dataset_id)
    observed_city_years = [row for row in reconciliation if row["reference_row_count"] > 0]
    priority_rows = sorted(
        observed_city_years,
        key=lambda row: (int(row["crawl_priority"]), -int(row["reference_row_count"]), row["province_name"], row["city_name"], int(row["year"])),
    )
    for index, row in enumerate(priority_rows, 1):
        row["priority_rank"] = index
    reconciliation_counts = Counter(str(row["reconciliation_status"]) for row in reconciliation)
    curated_outputs = {
        "reference_files": curated_dir / "reference_files.parquet",
        "reference_records": curated_dir / "reference_records.parquet",
        "reference_city_year_coverage": curated_dir / "reference_city_year_coverage.parquet",
        "reference_city_year_reconciliation": curated_dir / "reference_city_year_reconciliation.parquet",
        "reference_crawl_priority": curated_dir / "reference_crawl_priority.parquet",
        "reference_unmapped_locations": curated_dir / "reference_unmapped_locations.parquet",
        "reference_cells": staging_dir / "reference_cells.parquet",
    }
    atomic_write_parquet(pl.DataFrame(source_files, infer_schema_length=None), curated_outputs["reference_files"], {"import_batch_id": batch_id}, key_columns=("sha256",))
    atomic_write_parquet(pl.DataFrame(records, infer_schema_length=None), curated_outputs["reference_records"], {"import_batch_id": batch_id}, key_columns=("reference_record_id",))
    coverage_rows = [
        {
            "coverage_id": stable_id(row["city_id"], row["year"], prefix="EXTCOVERAGE"),
            "dataset_id": dataset_id,
            "city_id": row["city_id"],
            "city_name": row["city_name"],
            "province_name": row["province_name"],
            "year": row["year"],
            "reference_row_count": row["reference_row_count"],
            "source_file_count": row["reference_source_file_count"],
            "status": "REFERENCE_OBSERVED_UNVERIFIED",
            "classification_status": "NOT_RUN",
        }
        for row in observed_city_years
    ]
    atomic_write_parquet(
        pl.DataFrame(coverage_rows, infer_schema_length=None) if coverage_rows else _empty_frame(["coverage_id"]),
        curated_outputs["reference_city_year_coverage"],
        {"import_batch_id": batch_id},
        key_columns=("coverage_id",),
    )
    atomic_write_parquet(pl.DataFrame(reconciliation, infer_schema_length=None), curated_outputs["reference_city_year_reconciliation"], {"import_batch_id": batch_id}, key_columns=("reconciliation_id",))
    atomic_write_parquet(
        pl.DataFrame(priority_rows, infer_schema_length=None) if priority_rows else _empty_frame(["reconciliation_id"]),
        curated_outputs["reference_crawl_priority"],
        {"import_batch_id": batch_id, "queue_mutation": False},
        key_columns=("reconciliation_id",),
    )
    atomic_write_parquet(
        pl.DataFrame(unmapped_rows, infer_schema_length=None) if unmapped_rows else _empty_frame(["unmapped_location_id"]),
        curated_outputs["reference_unmapped_locations"],
        {"import_batch_id": batch_id},
        key_columns=("unmapped_location_id",),
    )
    atomic_write_parquet(
        pl.DataFrame(cells, infer_schema_length=None) if cells else _empty_frame(["cell_id"]),
        curated_outputs["reference_cells"],
        {"import_batch_id": batch_id},
        key_columns=("cell_id",),
    )
    summary_path = output_dir / "reference_reconciliation_summary.json"
    summary = {
        "dataset_id": dataset_id,
        "import_batch_id": batch_id,
        "generated_at": datetime.now(UTC).isoformat(),
        "reference_records": len(records),
        "mapped_city_records": sum(bool(row.get("city_id")) for row in records),
        "unmapped_location_count": len(unmapped_rows),
        "observed_city_count": len({row["city_id"] for row in observed_city_years if row.get("city_id")}),
        "observed_city_years": len(observed_city_years),
        "reconciliation_status_counts": dict(sorted(reconciliation_counts.items())),
        "priority_city_years": len(priority_rows),
        "priority_order": [
            "REFERENCE_ONLY",
            "REFERENCE_AND_EXISTING",
            "EXISTING_ONLY",
            "NO_REFERENCE_OR_EXISTING",
        ],
        "classification_status": "NOT_RUN",
        "api_calls": 0,
        "formal_promotion": False,
        "queue_mutation": False,
        "recommended_next_step": (
            "Use reference_only city-years as bounded crawl-gap candidates; verify official "
            "evidence and deduplicate against current DocumentVersions before formal promotion."
        ),
        "not_a_formal_coverage_claim": True,
    }
    _write_json(summary_path, summary)
    manifest = {
        "dataset_id": dataset_id,
        "import_batch_id": batch_id,
        "source_directory": str(source_dir),
        "source_files": source_files,
        "raw_directory": str(raw_dir),
        "staging_directory": str(staging_dir),
        "curated_directory": str(curated_dir),
        "output_directory": str(output_dir),
        "records": len(records),
        "staged_cells": len(cells),
        "mapped_city_records": sum(bool(row.get("city_id")) for row in records),
        "unmapped_location_count": len(unmapped_rows),
        "observed_city_years": len(observed_city_years),
        "reference_only_city_years": sum(row["reconciliation_status"] == "REFERENCE_ONLY" for row in reconciliation),
        "years": sorted({int(row["year"]) for row in observed_city_years}),
        "classification_status": "NOT_RUN",
        "api_calls": 0,
        "formal_promotion": False,
        "queue_mutation": False,
        "comparison_basis": [
            "policy_document_versions.parquet",
            "crawl_shards.parquet",
            "city_year_progress.parquet",
        ],
        "created_at": datetime.now(UTC).isoformat(),
        "outputs": {
            **{key: str(value) for key, value in curated_outputs.items()},
            "comparison_summary": str(summary_path),
        },
    }
    manifest_path = output_dir / "reference_import_manifest.json"
    _write_json(manifest_path, manifest)
    return {
        "dataset_id": dataset_id,
        "import_batch_id": batch_id,
        "records": len(records),
        "staged_cells": len(cells),
        "observed_city_years": len(observed_city_years),
        "reference_only_city_years": manifest["reference_only_city_years"],
        "unmapped_location_count": len(unmapped_rows),
        "classification_status": "NOT_RUN",
        "api_calls": 0,
        "queue_mutation": False,
        "manifest": str(manifest_path),
        "priority_table": str(curated_outputs["reference_crawl_priority"]),
        "comparison_summary": str(summary_path),
    }


__all__ = ["import_reference_folder"]
