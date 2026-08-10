from __future__ import annotations

import json
import re
from calendar import monthrange
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import polars as pl

from policydb.crawl.pipeline import CrawlPipeline
from policydb.crawl.registry import load_registry
from policydb.parquet_store import atomic_write_parquet, read_parquet_snapshot
from policydb.scope import load_cities_105
from policydb.settings import Settings
from policydb.source_discovery import REQUIRED_ROLES
from policydb.source_slots import build_requirement_slots, slot_paths
from policydb.transform.normalization import stable_id

DATE_PATTERNS: tuple[tuple[str, re.Pattern[str], float], ...] = (
    ("url_t_yyyymmdd", re.compile(r"(?:^|[/_-])t(20\d{2})(\d{2})(\d{2})(?:[/_.-]|$)", re.I), 0.98),
    ("url_yyyymmdd", re.compile(r"(?:^|[/_-])(20\d{2})(\d{2})(\d{2})(?:[/_.-]|$)"), 0.95),
    ("url_path_ymd", re.compile(r"(?:^|/)(20\d{2})[-/](\d{1,2})[-/](\d{1,2})(?:/|$)"), 0.96),
    ("url_path_ym", re.compile(r"(?:^|/)(20\d{2})(\d{2})(?:/|$)"), 0.88),
)
QUERY_DATE_KEYS = ("date", "publishdate", "pubdate", "time", "created", "release")
TERMINAL_SHARD_STATUSES = {
    "complete_policy_found",
    "confirmed_zero",
    "source_incomplete",
    "partial_network",
    "partial_parser",
    "partial_cap",
    "partial_archive",
    "cancelled",
    "failed",
    "split_parent",
}


@dataclass(slots=True)
class CandidateDate:
    value: date | None
    source: str
    confidence: float


def _safe_date(year: str, month: str, day: str = "1") -> date | None:
    try:
        return date(int(year), int(month), int(day))
    except ValueError:
        return None


def extract_candidate_date(
    url: str,
    *,
    list_date: date | None = None,
    nearby_text: str | None = None,
    meta_date: date | None = None,
    last_modified: date | None = None,
) -> CandidateDate:
    """Return the strongest observed date; unknown remains explicit."""
    if list_date:
        return CandidateDate(list_date, "list_item", 0.99)
    if meta_date:
        return CandidateDate(meta_date, "page_meta", 0.97)
    parsed = urlsplit(url)
    query = parse_qs(parsed.query)
    for key in QUERY_DATE_KEYS:
        for value in query.get(key, []):
            digits = re.sub(r"\D", "", value)
            if len(digits) >= 8:
                found = _safe_date(digits[:4], digits[4:6], digits[6:8])
                if found:
                    return CandidateDate(found, f"query:{key}", 0.94)
    search_target = parsed.path
    for source, pattern, confidence in DATE_PATTERNS:
        match = pattern.search(search_target)
        if match:
            groups = match.groups()
            found = _safe_date(*groups)
            if found:
                return CandidateDate(found, source, confidence)
    if nearby_text:
        match = re.search(
            r"(20\d{2})\s*[年./-]\s*(\d{1,2})\s*[月./-]\s*(\d{1,2})\s*日?",
            nearby_text,
        )
        if match:
            found = _safe_date(*match.groups())
            if found:
                return CandidateDate(found, "nearby_text", 0.9)
    if last_modified:
        return CandidateDate(last_modified, "last_modified", 0.35)
    return CandidateDate(None, "unknown", 0.0)


def candidate_period_decision(
    candidate: CandidateDate, start_date: date, end_date: date
) -> str:
    if candidate.value is None:
        return "date_unknown"
    if candidate.confidence >= 0.85 and not (
        start_date <= candidate.value <= end_date
    ):
        return "cross_period_rejected"
    return "accepted"


def month_windows(start_date: date, end_date: date) -> list[tuple[date, date]]:
    cursor = start_date.replace(day=1)
    windows: list[tuple[date, date]] = []
    while cursor <= end_date:
        month_end = date(
            cursor.year, cursor.month, monthrange(cursor.year, cursor.month)[1]
        )
        windows.append((max(cursor, start_date), min(month_end, end_date)))
        cursor = month_end + timedelta(days=1)
    return windows


def split_window(start_date: date, end_date: date) -> list[tuple[date, date]]:
    """Adaptive split: month/range → halves → weeks → days."""
    days = (end_date - start_date).days + 1
    if days <= 1:
        return [(start_date, end_date)]
    if days > 14:
        midpoint = start_date + timedelta(days=(days // 2) - 1)
    elif days > 7:
        midpoint = start_date + timedelta(days=6)
    else:
        midpoint = start_date
    return [(start_date, midpoint), (midpoint + timedelta(days=1), end_date)]


def _atomic_parquet(frame: pl.DataFrame, path: Path) -> None:
    atomic_write_parquet(frame, path, {"module": "exhaustive"})


def _upsert(frame: pl.DataFrame, path: Path, key: str) -> None:
    if path.exists():
        existing = read_parquet_snapshot(path)
        existing = existing.filter(~pl.col(key).is_in(frame[key].to_list()))
        frame = pl.concat([existing, frame], how="diagonal_relaxed")
    _atomic_parquet(frame, path)


def _json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


def completion_status(row: dict) -> str:
    if row.get("source_missing"):
        return "source_incomplete"
    if row.get("network_error_count", 0):
        return "partial_network"
    if row.get("parser_error_count", 0):
        return "partial_parser"
    if (
        row.get("candidate_cap_hit")
        or row.get("fetch_cap_hit")
        or row.get("page_cap_hit")
        or row.get("global_safety_limit_hit")
    ):
        return "partial_cap"
    if not row.get("pagination_exhausted", False):
        return "partial_temporal"
    if not row.get("source_verified", False):
        return "source_incomplete"
    if row.get("retryable_errors", 0) or row.get("pending_fetch", 0):
        return "failed"
    if row.get("date_unknown_count", 0):
        return "complete_unverified"
    if row.get("archive_missing_count", 0):
        return "partial_archive"
    return (
        "confirmed_zero"
        if int(row.get("unique_candidate_count", 0) or 0) == 0
        else "complete_policy_found"
    )


def can_certify_city_year(metrics: dict) -> bool:
    return all(
        (
            metrics.get("source_slot_coverage_pct") == 100,
            metrics.get("verified_source_coverage_pct") == 100,
            metrics.get("temporal_shard_coverage_pct") == 100,
            metrics.get("pagination_exhaustion_pct") == 100,
            metrics.get("error_closure_pct") == 100,
            metrics.get("archive_completion_pct") == 100,
            metrics.get("text_extraction_pct") == 100,
            metrics.get("ai_processing_pct") == 100,
            metrics.get("dedup_routing_pct") == 100,
            metrics.get("cap_hit_count", 0) == 0,
            metrics.get("date_unknown_count", 0) == 0,
            metrics.get("conflict_count", 0) == 0,
        )
    )


class ExhaustiveCrawler:
    def __init__(
        self,
        settings: Settings | None = None,
        pipeline: CrawlPipeline | None = None,
    ) -> None:
        self.settings = settings or Settings.discover()
        self.pipeline = pipeline or CrawlPipeline(self.settings)
        self.shards_path = self.settings.curated / "crawl_shards.parquet"
        self.evidence_path = self.settings.curated / "crawl_shard_evidence.parquet"
        self.events_path = self.settings.curated / "pipeline_progress_events.parquet"

    def resolve_city(self, city: str) -> dict:
        frame = load_cities_105(self.settings).filter(
            (pl.col("city_name") == city)
            | (pl.col("city_name_short") == city)
            | (pl.col("city_id") == city)
        )
        if frame.height != 1:
            raise ValueError(f"city must uniquely match the 105-city list: {city}")
        return frame.row(0, named=True)

    def plan_city(
        self,
        city: str,
        *,
        start_date: date,
        end_date: date,
        source_roles: list[str] | None = None,
        source_ids: list[str] | None = None,
    ) -> dict:
        city_row = self.resolve_city(city)
        if not slot_paths(self.settings)[0].exists():
            build_requirement_slots(self.settings)
        roles = source_roles or list(REQUIRED_ROLES)
        registry = load_registry(self.settings)
        registered: dict[str, list] = {}
        for role in roles:
            registered[role] = [
                source
                for source in registry
                if city_row["city_id"] in source.city_ids
                and (source.agency_type == role or source.source_role == role)
                and source.crawl_enabled
                and (not source_ids or source.source_id in source_ids)
            ]
        now = datetime.now(UTC).isoformat()
        batch_id = stable_id(
            city_row["city_id"],
            start_date.isoformat(),
            end_date.isoformat(),
            ",".join(sorted(roles)),
            prefix="EXHAUST",
        )
        rows: list[dict] = []
        for role in roles:
            sources = registered[role]
            role_slot = read_parquet_snapshot(slot_paths(self.settings)[0]).filter(
                (pl.col("city_id") == city_row["city_id"])
                & (pl.col("source_role") == role)
            )
            source_verified = bool(
                role_slot.height
                and role_slot[0, "verified_candidate_count"] > 0
            )
            if not sources:
                sources = [None]
            for source in sources:
                source_id = source.source_id if source else f"UNRESOLVED:{role}"
                for window_start, window_end in month_windows(start_date, end_date):
                    shard_id = stable_id(
                        city_row["city_id"],
                        role,
                        source_id,
                        window_start.isoformat(),
                        window_end.isoformat(),
                        prefix="SHARD",
                    )
                    rows.append(
                        {
                            "shard_id": shard_id,
                            "batch_id": batch_id,
                            "city_id": city_row["city_id"],
                            "city_name": city_row["city_name"],
                            "province_name": city_row["province_name"],
                            "source_id": source_id,
                            "source_role": role,
                            "start_date": window_start.isoformat(),
                            "end_date": window_end.isoformat(),
                            "status": "source_incomplete" if source is None else "pending",
                            "source_verified": source_verified if source else False,
                            "pages_scanned": 0,
                            "last_page_url": None,
                            "pagination_exhausted": False,
                            "candidate_count": 0,
                            "unique_candidate_count": 0,
                            "candidate_cap_hit": False,
                            "fetch_cap_hit": False,
                            "page_cap_hit": False,
                            "global_safety_limit_hit": False,
                            "fetch_attempted": 0,
                            "fetched": 0,
                            "failed": 0,
                            "retryable_errors": 0,
                            "permanent_errors": 0,
                            "document_versions": 0,
                            "attachment_count": 0,
                            "date_unknown_count": 0,
                            "cross_period_rejected_count": 0,
                            "archive_missing_count": 0,
                            "ai_pending_count": 0,
                            "dedup_pending_count": 0,
                            "checkpoint": None,
                            "completion_evidence_json": _json(
                                {
                                    "source_missing": source is None,
                                    "reason": (
                                        "required source slot is unresolved or not enabled"
                                        if source is None
                                        else "not yet scanned"
                                    ),
                                }
                            ),
                            "started_at": None,
                            "finished_at": now if source is None else None,
                            "created_at": now,
                            "updated_at": now,
                        }
                    )
        if self.shards_path.exists():
            existing = {
                str(item["shard_id"]): item
                for item in read_parquet_snapshot(self.shards_path).iter_rows(named=True)
            }
            rows = [
                existing.get(str(item["shard_id"]), item)
                for item in rows
            ]
        frame = pl.DataFrame(rows, infer_schema_length=None)
        _upsert(frame, self.shards_path, "shard_id")
        self.rebuild_progress()
        return {
            "batch_id": batch_id,
            "city_id": city_row["city_id"],
            "city_name": city_row["city_name"],
            "shards": frame.height,
            "runnable": frame.filter(pl.col("status") == "pending").height,
            "source_incomplete": frame.filter(
                pl.col("status") == "source_incomplete"
            ).height,
        }

    def _filter_run_candidates(
        self, run_id: str, start_date: date, end_date: date
    ) -> tuple[int, int]:
        path = self.settings.curated / "crawl_items.parquet"
        if not path.exists():
            return 0, 0
        frame = read_parquet_snapshot(path)
        unknown = 0
        rejected = 0
        updated: list[dict] = []
        for row in frame.iter_rows(named=True):
            if row.get("run_id") != run_id:
                updated.append(row)
                continue
            stored_date = row.get("candidate_date")
            candidate = (
                CandidateDate(
                    date.fromisoformat(str(stored_date)),
                    str(row.get("candidate_date_source") or "list_item"),
                    float(row.get("candidate_date_confidence") or 0.99),
                )
                if stored_date
                else extract_candidate_date(
                    str(row.get("url") or row.get("canonical_url") or "")
                )
            )
            decision = candidate_period_decision(candidate, start_date, end_date)
            row["candidate_date"] = (
                candidate.value.isoformat() if candidate.value else None
            )
            row["candidate_date_source"] = candidate.source
            row["candidate_date_confidence"] = candidate.confidence
            row["period_decision"] = decision
            if decision == "cross_period_rejected":
                row["status"] = "blocked"
                rejected += 1
            elif decision == "date_unknown":
                unknown += 1
            updated.append(row)
        _atomic_parquet(pl.DataFrame(updated, infer_schema_length=None), path)
        return unknown, rejected

    def _event(
        self, batch_id: str, shard_id: str, stage: str, message: str, **counts
    ) -> None:
        row = pl.DataFrame(
            [
                {
                    "event_id": stable_id(
                        shard_id, stage, datetime.now(UTC).isoformat(), prefix="PROGEVT"
                    ),
                    "batch_id": batch_id,
                    "shard_id": shard_id,
                    "stage": stage,
                    "message": message,
                    "counts_json": _json(counts),
                    "created_at": datetime.now(UTC).isoformat(),
                }
            ]
        )
        _upsert(row, self.events_path, "event_id")

    def run_city(
        self,
        city: str,
        *,
        start_date: date,
        end_date: date,
        source_roles: list[str] | None = None,
        source_ids: list[str] | None = None,
        max_pages_per_source: int = 50,
        max_candidates_per_shard: int = 500,
        max_fetches_per_shard: int = 500,
        resume: bool = True,
        retry_errors: bool = False,
        progress=None,
    ) -> dict:
        plan = self.plan_city(
            city,
            start_date=start_date,
            end_date=end_date,
            source_roles=source_roles,
            source_ids=source_ids,
        )
        frame = read_parquet_snapshot(self.shards_path).filter(
            pl.col("batch_id") == plan["batch_id"]
        )
        runnable_status = ["pending"]
        if retry_errors:
            runnable_status.extend(
                ["failed", "partial_network", "partial_parser", "partial_cap"]
            )
        pending = frame.filter(pl.col("status").is_in(runnable_status))
        total = pending.height
        completed = 0
        run_ids: list[str] = []
        run_metrics: dict[str, dict[str, int]] = {}
        for index, shard in enumerate(pending.iter_rows(named=True), 1):
            if resume and shard["status"] in {
                "complete_policy_found",
                "confirmed_zero",
            }:
                continue
            shard_start = date.fromisoformat(str(shard["start_date"]))
            shard_end = date.fromisoformat(str(shard["end_date"]))
            self._event(
                plan["batch_id"],
                shard["shard_id"],
                "discovering",
                f"{shard['city_name']} {shard_start}—{shard_end}",
            )
            run_plan = self.pipeline.plan(
                run_type="exhaustive_city",
                start_date=shard_start,
                end_date=shard_end,
                official_first=True,
                cities=[str(shard["city_id"])],
                source_ids=[str(shard["source_id"])],
                source_roles=[str(shard["source_role"])],
                max_candidates_total=max_candidates_per_shard,
                max_candidates_per_source=max_candidates_per_shard,
                max_pages_per_source=max_pages_per_source,
                global_safety_limit=max_candidates_per_shard,
                resume=False,
            )
            unknown, rejected = self._filter_run_candidates(
                run_plan["run_id"], shard_start, shard_end
            )
            run_result = self.pipeline.run(
                run_plan["run_id"], max_fetches=max_fetches_per_shard
            )
            run_ids.append(str(run_plan["run_id"]))
            run_metrics[str(run_plan["run_id"])] = {
                "fetched": int(run_result.get("fetched", 0) or 0),
                "ai_pending_count": int(run_result.get("fetched", 0) or 0),
                "dedup_pending_count": int(run_result.get("fetched", 0) or 0),
                "archive_missing_count": 0,
            }
            scans_path = self.settings.curated / "crawl_discovery_scans.parquet"
            scans = (
                read_parquet_snapshot(scans_path).filter(
                    pl.col("run_id") == run_plan["run_id"]
                )
                if scans_path.exists()
                else pl.DataFrame()
            )
            pagination_exhausted = bool(
                scans.height
                and scans["pagination_exhausted"].fill_null(False).all()
            )
            pages_scanned = (
                int(scans["pages_scanned"].fill_null(0).sum())
                if scans.height
                else 0
            )
            stop_reasons = (
                scans["stop_reason"].drop_nulls().cast(pl.String).to_list()
                if scans.height
                else ["no_discovery_scan"]
            )
            cap_hit = (
                run_plan["item_count"] >= max_candidates_per_shard
                or any(
                    reason in {"max_pages", "max_candidates", "safety_limit"}
                    for reason in stop_reasons
                )
            )
            metrics = {
                "source_missing": False,
                "source_verified": bool(shard.get("source_verified", False)),
                "pagination_exhausted": pagination_exhausted,
                "candidate_cap_hit": cap_hit,
                "fetch_cap_hit": (
                    run_plan["item_count"] > max_fetches_per_shard
                ),
                "page_cap_hit": "max_pages" in stop_reasons,
                "global_safety_limit_hit": False,
                        "network_error_count": int(run_result.get("failed", 0) or 0),
                "parser_error_count": 0,
                        "retryable_errors": int(run_result.get("failed", 0) or 0),
                "pending_fetch": max(
                    0,
                    max(
                        0,
                        int(run_plan["item_count"]) - unknown - rejected,
                    )
                            - int(run_result.get("fetched", 0) or 0)
                            - int(run_result.get("failed", 0) or 0),
                ),
                "date_unknown_count": unknown,
                "archive_missing_count": 0,
                "unique_candidate_count": max(
                    0, int(run_plan["item_count"]) - unknown - rejected
                ),
            }
            status = completion_status(metrics)
            now = datetime.now(UTC).isoformat()
            update = pl.DataFrame(
                [
                    {
                        **shard,
                        "status": status,
                        "pages_scanned": pages_scanned,
                        "pagination_exhausted": pagination_exhausted,
                        "candidate_count": int(run_plan["item_count"]),
                        "unique_candidate_count": metrics[
                            "unique_candidate_count"
                        ],
                        "candidate_cap_hit": metrics["candidate_cap_hit"],
                        "fetch_cap_hit": metrics["fetch_cap_hit"],
                        "page_cap_hit": metrics["page_cap_hit"],
                        "fetch_attempted": int(run_result.get("fetched", 0) or 0)
                        + int(run_result.get("failed", 0) or 0),
                        "fetched": int(run_result.get("fetched", 0) or 0),
                        "failed": int(run_result.get("failed", 0) or 0),
                        "retryable_errors": metrics["retryable_errors"],
                        "document_versions": int(run_result.get("fetched", 0) or 0),
                        "ai_pending_count": int(run_result.get("fetched", 0) or 0),
                        "dedup_pending_count": int(run_result.get("fetched", 0) or 0),
                        "date_unknown_count": unknown,
                        "cross_period_rejected_count": rejected,
                        "checkpoint": run_plan["run_id"],
                        "completion_evidence_json": _json(
                            {
                                **metrics,
                                "run_id": run_plan["run_id"],
                                "stop_reasons": stop_reasons,
                            }
                        ),
                        "started_at": shard.get("started_at") or now,
                        "finished_at": now,
                        "updated_at": now,
                    }
                ],
                infer_schema_length=None,
            )
            _upsert(update, self.shards_path, "shard_id")
            evidence = pl.DataFrame(
                [
                    {
                        "evidence_id": stable_id(
                            shard["shard_id"],
                            run_plan["run_id"],
                            prefix="SHARDEVID",
                        ),
                        "shard_id": shard["shard_id"],
                        "run_id": run_plan["run_id"],
                        "pagination_type": "registered_source",
                        "page_cursor": None,
                        "pages_scanned": pages_scanned,
                        "last_nonempty_page": None,
                        "first_empty_page": None,
                        "next_link_missing": "next_link_missing" in stop_reasons,
                        "repeated_page_hash": None,
                        "pagination_loop_detected": (
                            "pagination_loop" in stop_reasons
                        ),
                        "stop_reason": ";".join(stop_reasons),
                        "exhaustive": pagination_exhausted and not cap_hit,
                        "candidate_date_unknown_count": unknown,
                        "cross_period_rejected_count": rejected,
                        "network_status": (
                            "partial_network"
                            if int(run_result.get("failed", 0) or 0)
                            else "direct_ok"
                        ),
                        "evidence_json": update[0, "completion_evidence_json"],
                        "created_at": now,
                    }
                ],
                infer_schema_length=None,
            )
            _upsert(evidence, self.evidence_path, "evidence_id")
            if cap_hit and shard_start < shard_end:
                children = split_window(shard_start, shard_end)
                child_rows = []
                for child_start, child_end in children:
                    child = dict(update.row(0, named=True))
                    child["shard_id"] = stable_id(
                        shard["shard_id"],
                        child_start.isoformat(),
                        child_end.isoformat(),
                        prefix="SHARD",
                    )
                    child["start_date"] = child_start.isoformat()
                    child["end_date"] = child_end.isoformat()
                    child["status"] = "pending"
                    child["checkpoint"] = None
                    for field in (
                        "pages_scanned",
                        "candidate_count",
                        "unique_candidate_count",
                        "fetch_attempted",
                        "fetched",
                        "failed",
                        "retryable_errors",
                        "permanent_errors",
                        "document_versions",
                        "attachment_count",
                        "date_unknown_count",
                        "cross_period_rejected_count",
                        "archive_missing_count",
                        "ai_pending_count",
                        "dedup_pending_count",
                    ):
                        child[field] = 0
                    for field in (
                        "pagination_exhausted",
                        "candidate_cap_hit",
                        "fetch_cap_hit",
                        "page_cap_hit",
                        "global_safety_limit_hit",
                    ):
                        child[field] = False
                    child["last_page_url"] = None
                    child["completion_evidence_json"] = _json(
                        {
                            "parent_shard_id": shard["shard_id"],
                            "reason": "adaptive_split_child_pending",
                        }
                    )
                    child["created_at"] = now
                    child["started_at"] = None
                    child["finished_at"] = None
                    child_rows.append(child)
                _upsert(
                    pl.DataFrame(child_rows, infer_schema_length=None),
                    self.shards_path,
                    "shard_id",
                )
                parent = update.with_columns(
                    pl.lit("split_parent").alias("status"),
                    pl.lit(
                        _json(
                            {
                                **metrics,
                                "run_id": run_plan["run_id"],
                                "stop_reasons": stop_reasons,
                                "split_children": [
                                    row["shard_id"] for row in child_rows
                                ],
                            }
                        )
                    ).alias("completion_evidence_json"),
                )
                _upsert(parent, self.shards_path, "shard_id")
                status = "split_parent"
            completed += 1
            self._event(
                plan["batch_id"],
                shard["shard_id"],
                status,
                f"候选 {run_plan['item_count']}，成功 {run_result.get('fetched', 0)}，失败 {run_result.get('failed', 0)}",
            )
            if progress:
                progress(index, total, status, shard)
        self.rebuild_progress()
        return {
            **plan,
            "processed_shards": completed,
            "run_ids": run_ids,
            "run_metrics": run_metrics,
            "status": self.city_status(city),
        }

    def apply_postprocess_metrics(
        self, run_metrics: dict[str, dict[str, int]]
    ) -> dict:
        """Persist real per-run postprocess residuals into their leaf shards."""
        if not self.shards_path.exists() or not run_metrics:
            return {"updated_shards": 0}
        frame = read_parquet_snapshot(self.shards_path)
        rows: list[dict] = []
        updated = 0
        for row in frame.iter_rows(named=True):
            metrics = run_metrics.get(str(row.get("checkpoint") or ""))
            if metrics:
                row["ai_pending_count"] = int(metrics.get("ai_pending_count", 0) or 0)
                row["dedup_pending_count"] = int(metrics.get("dedup_pending_count", 0) or 0)
                row["archive_missing_count"] = int(
                    metrics.get("archive_missing_count", row.get("archive_missing_count", 0))
                )
                row["updated_at"] = datetime.now(UTC).isoformat()
                updated += 1
            rows.append(row)
        _atomic_parquet(pl.DataFrame(rows, infer_schema_length=None), self.shards_path)
        self.rebuild_progress()
        return {"updated_shards": updated}

    def rebuild_progress(self) -> dict:
        slots_path = slot_paths(self.settings)[0]
        slots = read_parquet_snapshot(slots_path) if slots_path.exists() else pl.DataFrame()
        shards = (
            read_parquet_snapshot(self.shards_path)
            if self.shards_path.exists()
            else pl.DataFrame()
        )
        if shards.height:
            reclassified: list[dict] = []
            for row in shards.iter_rows(named=True):
                if row.get("status") == "split_parent":
                    reclassified.append(row)
                    continue
                if (
                    row.get("status") == "pending"
                    and not row.get("started_at")
                    and not row.get("checkpoint")
                ):
                    reclassified.append(row)
                    continue
                source_missing = str(row.get("source_id") or "").startswith(
                    "UNRESOLVED:"
                )
                metrics = {
                    **row,
                    "source_missing": source_missing,
                    "source_verified": bool(row.get("source_verified", False)),
                    "network_error_count": int(row.get("failed", 0) or 0),
                    "parser_error_count": int(row.get("permanent_errors", 0) or 0),
                    "pending_fetch": max(
                        0,
                        int(row.get("unique_candidate_count", 0) or 0)
                        - int(row.get("fetched", 0) or 0)
                        - int(row.get("failed", 0) or 0),
                    ),
                }
                row["source_verified"] = metrics["source_verified"]
                row["status"] = completion_status(metrics)
                reclassified.append(row)
            shards = pl.DataFrame(reclassified, infer_schema_length=None)
            _atomic_parquet(shards, self.shards_path)
        if slots.is_empty() and shards.is_empty():
            return {"city_year_rows": 0}
        rows: list[dict] = []
        grouped = (
            shards.filter(
                pl.col("city_id").is_not_null()
                & pl.col("start_date").is_not_null()
            ).with_columns(
                pl.col("start_date").str.slice(0, 4).cast(pl.Int32).alias("year")
            ).group_by(["city_id", "city_name", "province_name", "year"])
            if shards.height
            else []
        )
        for key, group in grouped:
            city_id, city_name, province_name, year = key
            city_slots = (
                slots.filter(pl.col("city_id") == city_id)
                if slots.height
                else pl.DataFrame()
            )
            required = city_slots.height or len(REQUIRED_ROLES)
            resolved = (
                city_slots.filter(pl.col("status") != "unresolved").height
                if city_slots.height
                else 0
            )
            verified = (
                city_slots.filter(pl.col("verified_candidate_count") > 0).height
                if city_slots.height
                else 0
            )
            runnable = group.filter(
                ~pl.col("source_id").str.starts_with("UNRESOLVED:")
                & (pl.col("status") != "split_parent")
            )
            expected = runnable.height
            temporal_complete = runnable.filter(
                pl.col("pagination_exhausted")
                & ~pl.col("candidate_cap_hit")
                & ~pl.col("fetch_cap_hit")
                & ~pl.col("page_cap_hit")
                & ~pl.col("global_safety_limit_hit")
                & (pl.col("failed") == 0)
                & (pl.col("date_unknown_count") == 0)
            ).height
            pagination_complete = runnable.filter(
                pl.col("pagination_exhausted")
            ).height
            error_free = runnable.filter(
                (pl.col("retryable_errors") == 0) & (pl.col("failed") == 0)
            ).height
            archive_total = int(runnable["document_versions"].sum()) if expected else 0
            archive_missing = (
                int(runnable["archive_missing_count"].sum()) if expected else 0
            )
            def pct(value: int | float, denominator: int | float) -> float:
                return (
                    round(value / denominator * 100, 2)
                    if denominator
                    else 0.0
                )
            metrics = {
                "source_slot_coverage_pct": pct(resolved, required),
                "verified_source_coverage_pct": pct(verified, required),
                "temporal_shard_coverage_pct": pct(temporal_complete, expected),
                "pagination_exhaustion_pct": pct(pagination_complete, expected),
                "fetch_success_pct": pct(
                    int(runnable["fetched"].sum()) if expected else 0,
                    int(runnable["fetch_attempted"].sum()) if expected else 0,
                ),
                "error_closure_pct": pct(error_free, expected),
                "archive_completion_pct": (
                    pct(archive_total - archive_missing, archive_total)
                    if archive_total
                    else (100.0 if temporal_complete == expected and expected else 0.0)
                ),
                "text_extraction_pct": (
                    100.0 if temporal_complete == expected and expected else 0.0
                ),
                "ai_processing_pct": pct(
                    int((runnable["ai_pending_count"] == 0).sum())
                    if expected
                    else 0,
                    expected,
                ),
                "dedup_routing_pct": pct(
                    int((runnable["dedup_pending_count"] == 0).sum())
                    if expected
                    else 0,
                    expected,
                ),
                "cap_hit_count": int(
                    (
                        runnable["candidate_cap_hit"]
                        | runnable["fetch_cap_hit"]
                        | runnable["page_cap_hit"]
                        | runnable["global_safety_limit_hit"]
                    ).sum()
                )
                if expected
                else 0,
                "date_unknown_count": int(
                    runnable["date_unknown_count"].sum()
                )
                if expected
                else 0,
                "conflict_count": int(
                    runnable["cross_period_rejected_count"].sum()
                )
                if expected
                else 0,
            }
            overall = min(
                metrics["verified_source_coverage_pct"],
                metrics["temporal_shard_coverage_pct"],
                metrics["pagination_exhaustion_pct"],
                metrics["error_closure_pct"],
                metrics["archive_completion_pct"],
                metrics["ai_processing_pct"],
                metrics["dedup_routing_pct"],
            )
            if can_certify_city_year(metrics):
                status = (
                    "confirmed_zero"
                    if int(runnable["unique_candidate_count"].sum()) == 0
                    else "certified_complete"
                )
            elif metrics["verified_source_coverage_pct"] < 100:
                status = "source_incomplete"
            elif group.filter(pl.col("status") == "partial_network").height:
                status = "partial_network"
            elif metrics["cap_hit_count"]:
                status = "partial_cap"
            elif temporal_complete:
                status = "complete_unverified"
            elif group.filter(pl.col("status") == "pending").height:
                status = "running"
            else:
                status = "not_started"
            rows.append(
                {
                    "city_id": city_id,
                    "city_name": city_name,
                    "province_name": province_name,
                    "year": year,
                    **metrics,
                    "overall_completion_pct": overall,
                    "status": status,
                    "shard_count": group.height,
                    "updated_at": datetime.now(UTC).isoformat(),
                }
            )
        existing_keys = {(str(row["city_id"]), int(row["year"])) for row in rows}
        if slots.height:
            for city in slots.select(
                "city_id", "city_name", "province_name"
            ).unique().iter_rows(named=True):
                city_slots = slots.filter(pl.col("city_id") == city["city_id"])
                required = city_slots.height
                resolved = city_slots.filter(
                    pl.col("status") != "unresolved"
                ).height
                verified = city_slots.filter(
                    pl.col("verified_candidate_count") > 0
                ).height
                for year in range(2018, date.today().year + 1):
                    key = (str(city["city_id"]), year)
                    if key in existing_keys:
                        continue
                    source_pct = round(resolved / required * 100, 2) if required else 0.0
                    verified_pct = round(verified / required * 100, 2) if required else 0.0
                    rows.append(
                        {
                            "city_id": city["city_id"],
                            "city_name": city["city_name"],
                            "province_name": city["province_name"],
                            "year": year,
                            "source_slot_coverage_pct": source_pct,
                            "verified_source_coverage_pct": verified_pct,
                            "temporal_shard_coverage_pct": 0.0,
                            "pagination_exhaustion_pct": 0.0,
                            "fetch_success_pct": 0.0,
                            "error_closure_pct": 0.0,
                            "archive_completion_pct": 0.0,
                            "text_extraction_pct": 0.0,
                            "ai_processing_pct": 0.0,
                            "dedup_routing_pct": 0.0,
                            "cap_hit_count": 0,
                            "date_unknown_count": 0,
                            "conflict_count": 0,
                            "overall_completion_pct": 0.0,
                            "status": (
                                "source_incomplete"
                                if verified_pct < 100
                                else "not_started"
                            ),
                            "shard_count": 0,
                            "updated_at": datetime.now(UTC).isoformat(),
                        }
                    )
        progress = pl.DataFrame(rows, infer_schema_length=None)
        city_year_path = self.settings.curated / "city_year_progress.parquet"
        _atomic_parquet(progress, city_year_path)
        output = self.settings.outputs / "acceptance"
        output.mkdir(parents=True, exist_ok=True)
        progress.write_csv(output / "city_year_completion.csv")
        if shards.height:
            source_year = shards.with_columns(
                pl.col("start_date").str.slice(0, 4).cast(pl.Int32).alias("year")
            ).group_by(["city_id", "source_role", "source_id", "year"]).agg(
                pl.len().alias("shard_count"),
                pl.col("pagination_exhausted")
                .mean()
                .alias("pagination_exhaustion_pct"),
                pl.col("fetched").sum(),
                pl.col("failed").sum(),
                pl.col("status").alias("shard_statuses"),
            ).with_columns(
                (pl.col("pagination_exhaustion_pct") * 100).round(2)
            )
            _atomic_parquet(
                source_year,
                self.settings.curated / "city_source_year_progress.parquet",
            )
        slot_progress = (
            slots.select(
                "slot_id",
                "city_id",
                "city_name",
                "source_role",
                "status",
                "candidate_count",
                "verified_candidate_count",
                "enabled_source_count",
                "updated_at",
            )
            if slots.height
            else pl.DataFrame()
        )
        if slot_progress.height:
            _atomic_parquet(
                slot_progress,
                self.settings.curated / "source_slot_progress.parquet",
            )
        return {"city_year_rows": progress.height}

    def city_status(self, city: str | None = None) -> dict:
        path = self.settings.curated / "city_year_progress.parquet"
        if not path.exists():
            return {"rows": 0, "data": []}
        frame = read_parquet_snapshot(path)
        if city:
            city_row = self.resolve_city(city)
            frame = frame.filter(pl.col("city_id") == city_row["city_id"])
        return {"rows": frame.height, "data": frame.to_dicts()}


def export_progress(
    *,
    format: str,
    city: str | None = None,
    settings: Settings | None = None,
) -> Path:
    settings = settings or Settings.discover()
    crawler = ExhaustiveCrawler(settings)
    crawler.rebuild_progress()
    path = settings.curated / "city_year_progress.parquet"
    frame = read_parquet_snapshot(path) if path.exists() else pl.DataFrame()
    if city and frame.height:
        city_row = crawler.resolve_city(city)
        frame = frame.filter(pl.col("city_id") == city_row["city_id"])
    output = settings.outputs / "progress"
    output.mkdir(parents=True, exist_ok=True)
    target = output / f"city_year_progress.{format}"
    if format == "csv":
        frame.write_csv(target)
    elif format == "json":
        target.write_text(
            json.dumps(frame.to_dicts(), ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )
    else:
        raise ValueError("format must be csv or json")
    return target
