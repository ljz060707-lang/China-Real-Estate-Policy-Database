"""Small, read-only aggregates for the Streamlit dashboard.

The dashboard never scans raw HTML or archive files.  It reads curated
Parquet columns, run status JSON, and the source/slot registries only.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import polars as pl

from policydb.parquet_store import read_parquet_snapshot
from policydb.settings import Settings
from policydb.source_discovery import REQUIRED_ROLES


def _read(settings: Settings, name: str, columns: list[str] | None = None) -> pl.DataFrame:
    path = settings.curated / f"{name}.parquet"
    if not path.exists():
        return pl.DataFrame()
    try:
        return read_parquet_snapshot(path, columns=columns)
    except pl.exceptions.ColumnNotFoundError:
        try:
            frame = read_parquet_snapshot(path)
            return frame.select([column for column in (columns or frame.columns) if column in frame.columns])
        except (OSError, pl.exceptions.PolarsError):
            return pl.DataFrame()
    except (OSError, pl.exceptions.PolarsError):
        return pl.DataFrame()


def _json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        value = json.loads(path.read_text(encoding="utf-8-sig"))
        return value if isinstance(value, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def _utc(value: object) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _pct(numerator: int | float | None, denominator: int | float | None) -> float | None:
    if numerator is None or denominator in (None, 0):
        return None
    return float(numerator) / float(denominator)


def _kpi(label: str, numerator: int | float | None, denominator: int | float | None, *, definition: str) -> dict[str, Any]:
    return {"label": label, "numerator": numerator, "denominator": denominator, "percent": _pct(numerator, denominator), "definition": definition, "updated_at": datetime.now(UTC).isoformat()}


def _slot_flags(row: Mapping[str, Any]) -> dict[str, bool]:
    status = str(row.get("status") or row.get("coverage_status") or "").lower()
    candidate = int(row.get("candidate_count") or 0) > 0
    verified = int(row.get("verified_candidate_count") or 0) > 0 or status in {"verified", "enabled", "backfilled", "current"}
    enabled = int(row.get("enabled_source_count") or 0) > 0 or status in {"enabled", "backfilled", "current"}
    resolved = candidate or status not in {"", "unresolved", "no_candidate"}
    return {"resolved": resolved, "verified": verified, "enabled": enabled, "candidate": candidate}


def overview_metrics(settings: Settings | None = None) -> dict[str, Any]:
    settings = settings or Settings.discover()
    slots = _read(settings, "source_requirement_slots")
    required_slots = slots.filter(pl.col("required")) if not slots.is_empty() and "required" in slots.columns else slots
    slot_rows = required_slots.to_dicts() if not required_slots.is_empty() else []
    flags = [_slot_flags(row) for row in slot_rows]
    cities = {str(row.get("city_id")) for row in slot_rows if row.get("city_id")}
    docs = _read(settings, "records", ["record_id", "record_date", "publication_date", "full_text", "title", "primary_source_url", "content_hash"])
    versions = _read(settings, "policy_document_versions", ["document_version_id", "source_id", "created_at", "extracted_text", "title", "canonical_url", "content_sha256"])
    sync = _read(settings, "source_sync_state")
    if docs.is_empty() and not versions.is_empty():
        document_count = versions.height
    else:
        document_count = docs.height
    city_year = city_year_coverage(settings)
    gaps = _read(settings, "coverage_gaps")
    open_gaps = gaps.filter(~pl.col("status").cast(pl.String).str.to_lowercase().is_in(["resolved", "closed"])) if not gaps.is_empty() and "status" in gaps.columns else gaps
    critical = open_gaps.filter(pl.col("severity").cast(pl.String).str.to_lowercase().is_in(["critical", "high"])) if not open_gaps.is_empty() and "severity" in open_gaps.columns else pl.DataFrame()
    partial_sources = sync.filter(pl.col("source_status").cast(pl.String).is_in(["PARTIAL_BUT_USABLE", "COMPLETE_WITH_GAPS"])) if not sync.is_empty() and "source_status" in sync.columns else pl.DataFrame()
    fast_state = _json(settings.outputs / "fast_bulk_ingest" / "current_status.json")
    full_state = _json(settings.outputs / "all_cities_since_2018" / "current_status.json")
    runtime = fast_state or full_state or {}
    last_update = None
    for row in (sync.to_dicts() if not sync.is_empty() else []):
        candidate = _utc(row.get("updated_at") or row.get("last_successful_crawl_at"))
        if candidate and (last_update is None or candidate > last_update):
            last_update = candidate
    return {
        "kpis": {
            "documents": _kpi("政策文档", document_count, None, definition="Curated records 行数；无 records 时使用 policy_document_versions 行数。"),
            "cities_with_documents": _kpi("有政策文档的城市", None if city_year.is_empty() else city_year.filter(pl.col("document_count") > 0).get_column("city_id").n_unique(), len(cities), definition="按城市注册表和已关联的记录/来源统计。"),
            "source_slots": _kpi("必需来源槽位", len(cities) * len(REQUIRED_ROLES), len(cities) * len(REQUIRED_ROLES), definition="105 城 × 5 个必需来源角色；分母来自 required slot 注册表。"),
            "resolved_slots": _kpi("resolved 槽位", sum(item["resolved"] for item in flags), len(slot_rows), definition="slot 有候选或已进入非 unresolved 状态。"),
            "verified_slots": _kpi("verified 槽位", sum(item["verified"] for item in flags), len(slot_rows), definition="verified_candidate_count>0 或持久化 verified 状态。"),
            "enabled_slots": _kpi("enabled 槽位", sum(item["enabled"] for item in flags), len(slot_rows), definition="enabled_source_count>0 或持久化 enabled 状态。"),
            "backfilled_sources": _kpi("已回溯来源", sync.filter(pl.col("backfill_status").cast(pl.String).str.to_lowercase().is_in(["complete", "complete_with_gaps", "backfilled", "partial"])).height if not sync.is_empty() and "backfill_status" in sync.columns else None, sync.height if not sync.is_empty() else None, definition="source_sync_state 中已有回溯状态的来源。partial 仅计为已获得可用文本，不表示穷尽。"),
            "partial_but_usable": _kpi("PARTIAL_BUT_USABLE 来源", partial_sources.height, sync.height if not sync.is_empty() else None, definition="已有可用文本但仍有回溯或完整性缺口。"),
            "city_year_coverage": _kpi("2018年以来 city-year", None if city_year.is_empty() else city_year.filter(pl.col("document_count") > 0).height, None if city_year.is_empty() else city_year.height, definition="动态日期窗口内存在至少一篇文档的 city-year。"),
        },
        "document_count": document_count,
        "open_gaps": open_gaps.height,
        "critical_gaps": critical.height,
        "partial_sources": partial_sources.height,
        "latest_document_date": _latest_document_date(docs),
        "last_progress_at": runtime.get("last_progress_at") or runtime.get("last_heartbeat_at") or (last_update.isoformat() if last_update else None),
        "runtime": runtime,
        "gold": gold_placeholder(settings),
        "city_year": city_year,
    }


def _latest_document_date(docs: pl.DataFrame) -> str | None:
    if docs.is_empty():
        return None
    for column in ("publication_date", "record_date"):
        if column in docs.columns:
            values = docs.get_column(column).drop_nulls()
            if len(values):
                return str(values.max())
    return None


def city_role_matrix(settings: Settings | None = None) -> pl.DataFrame:
    settings = settings or Settings.discover()
    slots = _read(settings, "source_requirement_slots")
    if slots.is_empty():
        return pl.DataFrame()
    sync = _read(settings, "source_sync_state")
    sync_by = {str(row.get("source_id")): row for row in (sync.to_dicts() if not sync.is_empty() else [])}
    output = []
    for row in slots.to_dicts():
        status = str(row.get("status") or "unresolved").lower()
        flags = _slot_flags(row)
        source_id = row.get("preferred_source_id")
        sync_row = sync_by.get(str(source_id)) if source_id else None
        crawl_status = str((sync_row or {}).get("source_status") or "")
        if crawl_status in {"COMPLETE_WITH_GAPS", "PARTIAL_BUT_USABLE"}:
            status = "PARTIAL_BUT_USABLE"
        elif flags["enabled"]:
            status = "enabled"
        elif flags["verified"]:
            status = "verified"
        elif flags["candidate"]:
            status = "candidate"
        output.append({**row, "display_status": status, "crawl_status": crawl_status or None, "backfill_status": (sync_row or {}).get("backfill_status"), "last_attempt": (sync_row or {}).get("updated_at")})
    return pl.DataFrame(output, infer_schema_length=None)


def source_health(settings: Settings | None = None) -> pl.DataFrame:
    settings = settings or Settings.discover()
    frame = _read(settings, "source_sync_state")
    if frame.is_empty():
        frame = _read(settings, "source_registry")
    return frame


def city_year_coverage(settings: Settings | None = None, *, start_year: int = 2018, end_year: int | None = None) -> pl.DataFrame:
    settings = settings or Settings.discover()
    end_year = end_year or date.today().year
    geo = _read(settings, "record_geographies_normalized", ["record_id", "city_id", "city_name", "province_name"])
    docs = _read(settings, "records", ["record_id", "record_date", "publication_date"])
    if geo.is_empty() or docs.is_empty():
        return pl.DataFrame(schema={"city_id": pl.String, "city_name": pl.String, "year": pl.Int64, "document_count": pl.Int64})
    date_column = "record_date" if "record_date" in docs.columns else "publication_date"
    joined = docs.join(geo.unique(subset=["record_id"]), on="record_id", how="inner")
    joined = joined.with_columns(pl.col(date_column).cast(pl.Date, strict=False).dt.year().alias("year"))
    joined = joined.filter(pl.col("year").is_between(start_year, end_year))
    return joined.group_by(["city_id", "city_name", "year"]).agg(pl.col("record_id").n_unique().alias("document_count")).sort(["city_id", "year"])


def gap_register(settings: Settings | None = None) -> pl.DataFrame:
    return _read(settings or Settings.discover(), "coverage_gaps")


def cycle_history(settings: Settings | None = None, limit: int = 100) -> pl.DataFrame:
    frame = _read(settings or Settings.discover(), "crawl_runs")
    if frame.is_empty():
        return frame
    return frame.sort("created_at", descending=True).head(limit)


def document_quality(settings: Settings | None = None) -> dict[str, Any]:
    settings = settings or Settings.discover()
    docs = _read(settings, "records", ["record_id", "title", "record_date", "full_text", "primary_source_url", "content_hash"])
    if docs.is_empty():
        docs = _read(settings, "policy_document_versions", ["document_version_id", "title", "extracted_text", "canonical_url", "content_sha256"])
        if docs.is_empty():
            return {"total": 0, "missing_title": None, "missing_date": None, "missing_text": None, "missing_url": None, "duplicate_hash": None}
        return {"total": docs.height, "missing_title": int(docs.get_column("title").is_null().sum()) if "title" in docs.columns else None, "missing_date": None, "missing_text": int(docs.get_column("extracted_text").fill_null("").str.len_chars().lt(20).sum()) if "extracted_text" in docs.columns else None, "missing_url": int(docs.get_column("canonical_url").is_null().sum()) if "canonical_url" in docs.columns else None, "duplicate_hash": None}
    return {"total": docs.height, "missing_title": int(docs.get_column("title").fill_null("").str.strip_chars().eq("").sum()), "missing_date": int(docs.get_column("record_date").is_null().sum()) if "record_date" in docs.columns else None, "missing_text": int(docs.get_column("full_text").fill_null("").str.len_chars().lt(20).sum()) if "full_text" in docs.columns else None, "missing_url": int(docs.get_column("primary_source_url").is_null().sum()) if "primary_source_url" in docs.columns else None, "duplicate_hash": int(docs.get_column("content_hash").drop_nulls().len() - docs.get_column("content_hash").drop_nulls().n_unique()) if "content_hash" in docs.columns else None}


def gold_placeholder(settings: Settings | None = None) -> dict[str, Any]:
    return {"enabled": False, "status": "DISABLED_PLACEHOLDER", "reason": "政策强度指标体系仍在设计", "measurable_documents": 0, "policy_intensity_calls": 0, "next_step": "配置指标体系、提示词版本和测度模型后启用", "updated_at": datetime.now(UTC).isoformat()}


__all__ = ["city_role_matrix", "city_year_coverage", "cycle_history", "document_quality", "gap_register", "gold_placeholder", "overview_metrics", "source_health"]
