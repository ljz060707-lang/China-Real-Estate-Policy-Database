"""Bounded, resumable orchestration for continuous CRPD synchronization.

This module is deliberately an orchestration layer.  It reuses the existing
source-completion, deterministic source-gate, and crawl pipeline modules.  It
does not let an LLM decide source admission, strict enabling, or database
currentness.
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import threading
import time
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass, field, replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import polars as pl

from policydb.budget import BudgetExceeded, HttpBudgetExceeded
from policydb.crawl.dedup import canonicalize_url
from policydb.crawl.pipeline import CrawlPipeline
from policydb.crawl.registry import load_registry
from policydb.parquet_store import (
    atomic_write_parquet as storage_atomic_write_parquet,
)
from policydb.parquet_store import (
    merge_and_replace_parquet,
    read_parquet_snapshot,
)
from policydb.pdf_pipeline import PDFPipeline, load_pdf_config
from policydb.settings import Settings
from policydb.source_completion import build_slot_work_queue
from policydb.source_discovery import REQUIRED_ROLES, is_reusable_source_entry
from policydb.source_slots import (
    enable_source_strict,
    list_candidates,
    probe_candidates,
    promote_candidate,
    upsert_candidates,
    verify_candidates,
)
from policydb.test_evidence import parse_pytest_report_file
from policydb.transform.normalization import stable_id

# The values are kept as strings because they are persisted in Parquet and
# JSONL.  The rank is only used to reject silent backwards transitions; retry,
# stale, and degraded are explicit operational branches, not progress ranks.
SLOT_STATES = (
    "UNRESOLVED",
    "DISCOVERING",
    "CANDIDATES_FOUND",
    "HUMAN_REVIEW",
    "VERIFIED",
    "ENABLED",
    "CRAWL_READY",
    "BACKFILLING",
    "BACKFILLED",
    "INCREMENTAL_SYNCING",
    "CURRENT",
    "CURRENT_WITH_WARNINGS",
    "RETRY_WAIT",
    "FAILED_RECOVERABLE",
    "BLOCKED",
    "DISABLED",
)

SOURCE_STATES = (
    "DISCOVERED",
    "PROBED",
    "VERIFIED",
    "ENABLED",
    "CRAWL_READY",
    "BACKFILL_RUNNING",
    "BACKFILL_COMPLETE",
    "INCREMENTAL_HEALTHY",
    "STALE",
    "DEGRADED",
    "UNREACHABLE",
    "PARSER_BROKEN",
    "DISABLED",
)

CRAWL_JOB_STATES = (
    "PLANNED",
    "CLAIMED",
    "RUNNING",
    "PAGE_FETCH",
    "ARTICLE_FETCH",
    "PARSE",
    "VALIDATE",
    "UPSERT",
    "CHECKPOINTED",
    "COMPLETED",
    "PARTIAL",
    "RETRY_WAIT",
    "FAILED_RECOVERABLE",
    "FAILED_TERMINAL",
    "CANCELLED",
)

DOCUMENT_STATES = (
    "DISCOVERED",
    "FETCHED",
    "PARSED",
    "VALIDATED",
    "DEDUPED",
    "INSERTED",
    "UPDATED",
    "UNCHANGED",
    "WITHDRAWN",
    "SUPERSEDED",
    "REJECTED",
    "PARSE_FAILED",
)

GLOBAL_SYNC_STATES = (
    "INITIALIZING",
    "SOURCE_COMPLETION",
    "BACKFILLING",
    "CURRENT_WITH_GAPS",
    "CURRENT",
    "STALE",
    "DEGRADED",
    "PAUSED_BUDGET",
    "PAUSED_PROVIDER",
    "BLOCKED_CONFLICT",
    "FAILED",
)

_SLOT_RANK = {
    "UNRESOLVED": 0,
    "DISCOVERING": 1,
    "CANDIDATES_FOUND": 2,
    "HUMAN_REVIEW": 3,
    "VERIFIED": 4,
    "ENABLED": 5,
    "CRAWL_READY": 6,
    "BACKFILLING": 7,
    "BACKFILLED": 8,
    "INCREMENTAL_SYNCING": 9,
    "CURRENT": 10,
    "CURRENT_WITH_WARNINGS": 10,
}
_SOURCE_RANK = {
    "DISCOVERED": 0,
    "PROBED": 1,
    "VERIFIED": 2,
    "ENABLED": 3,
    "CRAWL_READY": 4,
    "BACKFILL_RUNNING": 5,
    "BACKFILL_COMPLETE": 6,
    "INCREMENTAL_HEALTHY": 7,
}
_JOB_RANK = {name: index for index, name in enumerate(CRAWL_JOB_STATES[:10])}

_OPERATIONAL_RETRY_STATES = {
    "RETRY_WAIT",
    "FAILED_RECOVERABLE",
    "STALE",
    "DEGRADED",
    "UNREACHABLE",
    "PARSER_BROKEN",
    "DISABLED",
    "PARTIAL",
    "FAILED_TERMINAL",
    "CANCELLED",
}


class FullSyncError(RuntimeError):
    """Base exception for bounded full-sync orchestration."""


class InvalidTransition(FullSyncError):
    """Raised when a persisted state would silently move backwards."""


class LeaseConflict(FullSyncError):
    """Raised when another live worker owns a source or URL lease."""


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _parse_datetime(value: object) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _retry_wait_active(source: Mapping[str, Any], *, now: datetime | None = None) -> bool:
    next_retry_at = _parse_datetime(source.get("next_retry_at"))
    if next_retry_at is None:
        return False
    return next_retry_at > (now or datetime.now(UTC))


def _json_default(value: object) -> str:
    return str(value)


def _atomic_write_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_bytes(data)
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    _atomic_write_bytes(
        path,
        (json.dumps(payload, ensure_ascii=False, indent=2, default=_json_default) + "\n").encode(
            "utf-8"
        ),
    )


def _atomic_parquet(path: Path, frame: pl.DataFrame) -> None:
    storage_atomic_write_parquet(
        frame,
        path,
        {"module": "full_sync", "run_id": frame[0, "run_id"] if "run_id" in frame.columns and frame.height else None},
    )


def _append_jsonl(path: Path, rows: Iterable[Mapping[str, Any]], *, unique_key: str | None = None) -> int:
    incoming = [dict(row) for row in rows]
    if not incoming:
        return 0
    existing_keys: set[str] = set()
    if unique_key and path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            try:
                item = json.loads(line)
            except json.JSONDecodeError:
                continue
            if item.get(unique_key) is not None:
                existing_keys.add(str(item[unique_key]))
    path.parent.mkdir(parents=True, exist_ok=True)
    written = 0
    with path.open("a", encoding="utf-8") as stream:
        for row in incoming:
            if unique_key and str(row.get(unique_key)) in existing_keys:
                continue
            stream.write(json.dumps(row, ensure_ascii=False, default=_json_default) + "\n")
            if unique_key:
                existing_keys.add(str(row.get(unique_key)))
            written += 1
        stream.flush()
        os.fsync(stream.fileno())
    return written


def _as_dict(value: object) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if hasattr(value, "model_dump"):
        return dict(value.model_dump(mode="json", exclude_none=False))
    return {key: getattr(value, key, None) for key in dir(value) if not key.startswith("_")}


def _norm(value: object) -> str:
    return " ".join(str(value or "").strip().lower().split())


def _empty_frame(columns: Sequence[str]) -> pl.DataFrame:
    return pl.DataFrame({column: pl.Series(column, [], dtype=pl.String) for column in columns})


def transition_allowed(current: str | None, new: str, *, kind: str = "slot") -> bool:
    """Return whether a state transition is explicit and monotonic.

    Retry/degraded branches may leave a healthy state and recover back to the
    prior progress state.  They are allowed only as named operational branches;
    callers still have to persist a reason code.
    """
    if not current or current == new:
        return True
    states = {
        "slot": SLOT_STATES,
        "source": SOURCE_STATES,
        "crawl_job": CRAWL_JOB_STATES,
        "document": DOCUMENT_STATES,
        "global": GLOBAL_SYNC_STATES,
    }.get(kind)
    if states is None or new not in states or current not in states:
        return False
    ranks = {
        "slot": _SLOT_RANK,
        "source": _SOURCE_RANK,
        "crawl_job": _JOB_RANK,
    }.get(kind, {})
    if current in ranks and new in ranks:
        return ranks[new] >= ranks[current]
    if current in _OPERATIONAL_RETRY_STATES:
        return True
    if new in _OPERATIONAL_RETRY_STATES:
        return True
    if kind == "document":
        return new in {"SUPERSEDED", "WITHDRAWN", "REJECTED", "UNCHANGED", "UPDATED"}
    if kind == "global":
        return new in {"CURRENT_WITH_GAPS", "STALE", "DEGRADED", "PAUSED_BUDGET", "PAUSED_PROVIDER", "BLOCKED_CONFLICT", "FAILED"}
    return False


def transition_state(current: str | None, new: str, *, reason_code: str, kind: str = "slot") -> str:
    if not reason_code:
        raise InvalidTransition("state changes require a reason_code")
    if not transition_allowed(current, new, kind=kind):
        raise InvalidTransition(f"illegal {kind} transition: {current!r} -> {new!r}")
    return new


def classify_slot_state(row: Mapping[str, Any], source_state: str | None = None) -> str:
    """Classify a slot from deterministic persisted evidence only."""
    explicit = str(row.get("sync_state") or "").upper()
    if explicit in SLOT_STATES:
        return explicit
    work = str(row.get("work_status") or row.get("coverage_status") or "").lower()
    if work in {"blocked_network", "blocked_parser", "blocked_role_conflict"}:
        return "BLOCKED"
    if work in {"retry_wait", "retrying", "retry_waiting"}:
        return "RETRY_WAIT"
    if work in {"human_review", "HUMAN_REVIEW".lower()} or str(row.get("manual_review_status") or "").lower() in {
        "pending",
        "human_review",
        "requires_human_review",
    }:
        return "HUMAN_REVIEW"
    if str(source_state or "").upper() == "STALE":
        return "CURRENT_WITH_WARNINGS"
    enabled = bool(row.get("is_enabled")) or int(row.get("enabled_source_count") or 0) > 0
    verified = bool(row.get("is_verified")) or int(row.get("verified_candidate_count") or 0) > 0
    candidates = int(row.get("candidate_count") or 0) > 0 or bool(row.get("best_candidate_id"))
    backfill = str(row.get("backfill_status") or "").lower()
    if enabled and source_state in {"CRAWL_READY", "BACKFILL_COMPLETE", "INCREMENTAL_HEALTHY"}:
        if backfill in {"complete", "complete_with_gaps", "backfilled", "backfill_complete"}:
            return "BACKFILLED"
        return "CRAWL_READY"
    if enabled:
        return "ENABLED"
    if verified:
        return "VERIFIED"
    if candidates:
        return "CANDIDATES_FOUND"
    if work in {"discovering", "claimed"}:
        return "DISCOVERING"
    return "UNRESOLVED"


def source_is_crawl_ready(source: object, sync_row: Mapping[str, Any] | None = None) -> bool:
    """Deterministic source gate used by full-sync; it never consults an LLM."""
    item = _as_dict(source)
    if sync_row:
        item = {**item, **dict(sync_row)}
    if not bool(item.get("crawl_enabled")) and not bool(item.get("enabled")):
        return False
    official_status = _norm(item.get("official_status") or item.get("verification_status"))
    if official_status not in {"official", "official_reprint", "verified", "passed"}:
        return False
    if not bool(item.get("official_domain_verified", item.get("verified", False))):
        return False
    health = _norm(item.get("health_status") or item.get("source_status") or "")
    if health not in {"healthy", "ok", "direct_ok", "operational", "current"}:
        return False
    role = str(item.get("agency_type") or item.get("source_role") or "")
    if role not in REQUIRED_ROLES and str(item.get("source_role") or "") not in REQUIRED_ROLES:
        return False
    entries = [
        item.get("list_url"),
        item.get("canonical_list_url"),
        *list(item.get("list_page_urls") or []),
    ]
    return any(
        is_reusable_source_entry(str(url)) and not _looks_like_detail_page(str(url))
        for url in entries
        if url
    )


def _looks_like_detail_page(url: str) -> bool:
    """Reject common article/detail URL shapes at the source gate."""
    parsed = urlsplit(url)
    path = parsed.path.lower().rstrip("/")
    if path.endswith(".pdf"):
        return True
    segments = {segment for segment in path.split("/") if segment}
    if segments & {"detail", "article", "content", "show", "info", "view", "news"}:
        return True
    last = path.rsplit("/", 1)[-1]
    # Government sites commonly publish articles as a deep numeric directory
    # ending in ``index.shtml``/``index.html``.  The filename alone is not
    # enough to distinguish that article from a reusable column index.
    if last.startswith("index.") and any(
        segment.isdigit() and len(segment) >= 4 for segment in path.split("/")[:-1]
    ):
        return True
    return bool(last.isdigit() and len(last) >= 4)


def _is_gazette_history_index(item: Mapping[str, Any]) -> bool:
    """Prefer the official issue index when ranking gazette candidates."""
    role = str(item.get("source_role") or item.get("agency_type") or "")
    path = urlsplit(
        canonicalize_url(str(item.get("candidate_url") or item.get("canonical_url") or ""))
    ).path.lower().rstrip("/")
    return role == "government_gazette" and path == "/so/zcdh/zfgbhistory"


def _candidate_role_identity_score(item: Mapping[str, Any]) -> int:
    """Rank candidates by independent institution/domain evidence.

    ``source_role`` is a slot requirement, not proof that a discovered URL
    belongs to that institution.  A prior run forced the slot role onto a few
    candidates, which made a GJJ page look like a housing-department source
    and a ZJW page look like a provident-fund source.  This small deterministic
    ranking signal prefers the official hostname and visible institution
    identity before historical ``is_verified`` flags that may have been based
    on the same circular registry label.
    """
    role = str(item.get("source_role") or item.get("agency_type") or "").lower()
    urls = [
        item.get("candidate_url"),
        item.get("canonical_url"),
        item.get("list_url"),
        item.get("canonical_list_url"),
        *(item.get("list_page_urls") or []),
    ]
    url = next((str(value) for value in urls if value), "")
    host = (urlsplit(url).hostname or "").lower()
    identity = " ".join(
        str(item.get(field) or "")
        for field in (
            "site_name",
            "source_name",
            "agency_name",
            "department_name",
            "role_match_evidence",
            "official_evidence",
        )
    ).lower()
    haystack = f"{host} {identity}"
    good: tuple[str, ...]
    bad: tuple[str, ...]
    if role == "housing_department":
        good = ("zjw", "zjj", "住房和城乡建设", "住房城乡建设", "住建")
        bad = ("gjj", "公积金")
    elif role == "provident_fund_center":
        good = ("gjj", "公积金", "住房公积金")
        bad = ("zjw", "zjj", "住房和城乡建设", "住建")
    elif role == "natural_resources_department":
        good = ("ghzrzyw", "zrzy", "gtzy", "自然资源", "规划和自然资源")
        bad = ("sthjj", "生态环境", "环保", "environment")
    elif role == "municipal_government":
        good = ("banshi", "市政府", "人民政府", "beijing.gov.cn")
        bad = ()
    elif role == "government_gazette":
        good = ("zfgb", "gongbao", "政府公报", "公报")
        bad = ()
    else:
        return 1
    if any(token in haystack for token in bad):
        return 2
    if any(token in haystack for token in good):
        return 0
    return 1


def _source_selection_priority(row: Mapping[str, Any]) -> tuple[int, int, int, int, int, str]:
    """Return the shared deterministic priority for a city/role source.

    Slot summaries and source execution must describe the same source.  Keep
    the priority in one helper so an older retrying registry row cannot make
    the slot status disagree with the source actually selected for a run.
    """
    state = str(row.get("source_state") or "").upper()
    status = str(row.get("source_status") or "").upper()
    backfill = str(row.get("backfill_status") or "").lower()
    role_identity = _candidate_role_identity_score(row)
    retrying = state == "RETRY_WAIT" or status == "RETRY_WAIT" or backfill == "retry_wait"
    return (
        int(retrying),
        role_identity,
        0 if state in {"ENABLED", "CRAWL_READY", "BACKFILL_COMPLETE", "INCREMENTAL_HEALTHY"} else 1,
        0 if row.get("list_page_urls") or row.get("list_url") else 1,
        0 if row.get("verified") or row.get("official_domain_verified") else 1,
        str(row.get("source_id") or ""),
    )


def classify_source_state(source: object, sync_row: Mapping[str, Any] | None = None, *, now: datetime | None = None) -> str:
    item = {**_as_dict(source), **dict(sync_row or {})}
    if not bool(item.get("crawl_enabled", item.get("enabled", False))):
        return "DISABLED"
    if _norm(item.get("parser_status") or "") in {"broken", "failed", "parser_broken"}:
        return "PARSER_BROKEN"
    if _norm(item.get("health_status") or item.get("source_status") or "") in {"unreachable", "blocked"}:
        return "UNREACHABLE"
    if not bool(item.get("official_domain_verified", item.get("verified", False))):
        return "DISCOVERED"
    if not source_is_crawl_ready(source, sync_row):
        return "VERIFIED" if bool(item.get("verified")) else "PROBED"
    backfill = _norm(item.get("backfill_status"))
    if backfill in {"complete", "complete_with_gaps", "backfilled", "backfill_complete"}:
        freshness = source_freshness_status(item.get("last_successful_crawl_at") or item.get("last_success_at"), str(item.get("source_role") or ""), now=now)
        if freshness == "stale":
            return "STALE"
        if _norm(item.get("health_status")) in {"degraded", "warning"}:
            return "DEGRADED"
        return "INCREMENTAL_HEALTHY"
    return "CRAWL_READY"


def derive_global_status(
    slots: Iterable[Mapping[str, Any]],
    sources: Iterable[Mapping[str, Any]],
    *,
    open_gaps: int = 0,
    checkpoint_conflict: bool = False,
    write_blocked: bool = False,
) -> str:
    """Compute global status from deterministic slot/source evidence."""
    slot_rows = list(slots)
    source_rows = list(sources)
    if checkpoint_conflict or write_blocked:
        return "BLOCKED_CONFLICT"
    if not slot_rows:
        return "INITIALIZING"
    source_states = {str(row.get("source_state") or "").upper() for row in source_rows}
    slot_states = {str(row.get("slot_state") or "").upper() for row in slot_rows}
    if "STALE" in source_states:
        return "STALE"
    if "PARSER_BROKEN" in source_states or "DEGRADED" in source_states or "UNREACHABLE" in source_states:
        return "DEGRADED"
    if "BACKFILL_RUNNING" in source_states or "BACKFILLING" in slot_states:
        return "BACKFILLING"
    if any(state in {"UNRESOLVED", "DISCOVERING", "CANDIDATES_FOUND", "HUMAN_REVIEW", "VERIFIED", "ENABLED", "CRAWL_READY"} for state in slot_states):
        return "SOURCE_COMPLETION"
    if open_gaps:
        return "CURRENT_WITH_GAPS"
    if slot_states and all(state in {"CURRENT", "CURRENT_WITH_WARNINGS", "BACKFILLED", "INCREMENTAL_SYNCING"} for state in slot_states):
        return "CURRENT"
    return "CURRENT_WITH_GAPS"


def role_sla_hours(role: str, *, env: Mapping[str, str] | None = None) -> float:
    env = env or os.environ
    if env.get("POLICYDB_FRESHNESS_SLA_HOURS"):
        try:
            return float(env["POLICYDB_FRESHNESS_SLA_HOURS"])
        except ValueError:
            pass
    defaults = {
        "municipal_government": 24.0,
        "housing_department": 24.0,
        "natural_resources_department": 24.0,
        "provident_fund_center": 24.0,
        "government_gazette": 168.0,
    }
    configured = env.get("POLICYDB_ROLE_SLA_CONFIG")
    if configured:
        try:
            values = json.loads(configured)
            if isinstance(values, Mapping) and role in values:
                return float(values[role])
        except (TypeError, ValueError, json.JSONDecodeError):
            pass
    return defaults.get(role, 72.0)


def source_freshness_status(last_successful_sync: object, role: str, *, now: datetime | None = None) -> str:
    timestamp = _parse_datetime(last_successful_sync)
    if timestamp is None:
        return "unknown"
    now = now or datetime.now(UTC)
    age_hours = (now - timestamp).total_seconds() / 3600
    return "current" if age_hours <= role_sla_hours(role) else "stale"


def build_watermark(previous: Mapping[str, Any] | None = None, *, documents: Iterable[Mapping[str, Any]] = (), list_url: str | None = None, list_content_hash: str | None = None, etag: str | None = None, last_modified: str | None = None, source_response_hash: str | None = None) -> dict[str, Any]:
    """Create a watermark without advancing it until the caller commits data."""
    old = dict(previous or {})
    docs = [dict(row) for row in documents]
    dates = [_parse_datetime(row.get("published_at")) or _parse_datetime(row.get("first_seen_at")) for row in docs]
    dates = [value for value in dates if value]
    urls = [canonicalize_url(str(row.get("canonical_url") or row.get("url") or "")) for row in docs if row.get("canonical_url") or row.get("url")]
    last_url = list_url or (urls[-1] if urls else old.get("last_article_url"))
    max_date = max(dates).isoformat() if dates else old.get("max_published_at")
    first_seen = max((_parse_datetime(row.get("first_seen_at")) for row in docs), default=None)
    return {
        "max_published_at": max_date,
        "max_first_seen_at": first_seen.isoformat() if first_seen else old.get("max_first_seen_at"),
        "last_list_url": list_url or old.get("last_list_url"),
        "last_article_url_hash": hashlib.sha256(last_url.encode("utf-8")).hexdigest() if last_url else old.get("last_article_url_hash"),
        "last_document_number": next((row.get("document_number") for row in reversed(docs) if row.get("document_number")), old.get("last_document_number")),
        "last_list_content_hash": list_content_hash or old.get("last_list_content_hash"),
        "etag": etag or old.get("etag"),
        "last_modified": last_modified or old.get("last_modified"),
        "last_content_hash": next((row.get("content_hash") or row.get("content_sha256") for row in reversed(docs) if row.get("content_hash") or row.get("content_sha256")), old.get("last_content_hash")),
        "source_response_hash": source_response_hash or old.get("source_response_hash"),
        "updated_at": _now(),
    }


def watermark_equal(left: Mapping[str, Any] | None, right: Mapping[str, Any] | None) -> bool:
    keys = ("max_published_at", "max_first_seen_at", "last_list_url", "last_article_url_hash", "last_document_number", "last_list_content_hash", "etag", "last_modified", "last_content_hash", "source_response_hash")
    return all((left or {}).get(key) == (right or {}).get(key) for key in keys)


def canonical_document_key(document: Mapping[str, Any]) -> str:
    """Return a stable document identity; content hash belongs to a version."""
    city = _norm(document.get("city_id") or document.get("city"))
    agency = _norm(document.get("issuing_agency") or document.get("agency"))
    number = _norm(document.get("document_number") or document.get("policy_number"))
    title = _norm(document.get("normalized_title") or document.get("title"))
    published = str(document.get("published_at") or document.get("publication_date") or "")[:10]
    url = canonicalize_url(str(document.get("canonical_url") or document.get("url") or ""))
    # Official numbers are the strongest identity.  URL changes, mirrors and
    # redirects therefore converge when city/agency/number agree.
    identity = [city, agency, number] if number else [city, agency, title, published, url]
    return stable_id("canonical_document", *identity, prefix="DOC")


def document_version_key(document: Mapping[str, Any]) -> str:
    attachment_hash = json.dumps(sorted(document.get("attachment_hashes") or []), ensure_ascii=False)
    return stable_id(
        canonical_document_key(document),
        str(document.get("content_hash") or document.get("content_sha256") or ""),
        str(document.get("metadata_hash") or ""),
        attachment_hash,
        prefix="DOCVER",
    )


def classify_document_change(previous: Mapping[str, Any] | None, current: Mapping[str, Any]) -> str:
    if not previous:
        return "INSERTED"
    if current.get("withdrawn_at") or str(current.get("version_status") or "").upper() == "WITHDRAWN":
        return "WITHDRAWN"
    old_content = previous.get("content_hash") or previous.get("content_sha256")
    new_content = current.get("content_hash") or current.get("content_sha256")
    old_url = canonicalize_url(str(previous.get("canonical_url") or previous.get("url") or ""))
    new_url = canonicalize_url(str(current.get("canonical_url") or current.get("url") or ""))
    if old_content == new_content and old_url == new_url:
        return "UNCHANGED"
    if old_content == new_content and old_url != new_url:
        return "REPRINT"
    old_number = _norm(previous.get("document_number"))
    new_number = _norm(current.get("document_number"))
    if old_number and old_number == new_number:
        return "REVISED"
    return "UPDATED"


def _gap_row(*, city_id: str | None, slot_id: str | None, source_id: str | None, gap_type: str, start_date: str | None = None, end_date: str | None = None, expected_count: int | None = None, observed_count: int | None = None, affected_urls: Sequence[str] = (), severity: str = "medium", reason: str = "") -> dict[str, Any]:
    now = _now()
    return {
        "gap_id": stable_id(source_id or "", gap_type, start_date or "", end_date or "", *sorted(affected_urls), prefix="GAP"),
        "city_id": city_id,
        "slot_id": slot_id,
        "source_id": source_id,
        "gap_type": gap_type,
        "start_date": start_date,
        "end_date": end_date,
        "expected_count": expected_count,
        "observed_count": observed_count,
        "affected_urls": json.dumps(list(affected_urls), ensure_ascii=False),
        "severity": severity,
        "status": "OPEN",
        "repair_attempts": 0,
        "last_attempt_at": None,
        "next_retry_at": None,
        "resolution": reason,
        "created_at": now,
        "resolved_at": None,
    }


def detect_coverage_gaps(documents: Iterable[Mapping[str, Any]] | pl.DataFrame, *, source: Mapping[str, Any] | None = None, expected_start: date | None = None, expected_end: date | None = None, page_numbers: Sequence[int] | None = None) -> list[dict[str, Any]]:
    """Detect explicit historical and document-quality gaps without guessing."""
    rows = documents.to_dicts() if isinstance(documents, pl.DataFrame) else [dict(row) for row in documents]
    source = dict(source or {})
    city_id = source.get("city_id")
    slot_id = source.get("slot_id")
    source_id = source.get("source_id")
    gaps: list[dict[str, Any]] = []
    observed_dates = [_parse_datetime(row.get("published_at") or row.get("publication_date")) for row in rows]
    observed_dates = [value.date() for value in observed_dates if value]
    if expected_start and expected_end:
        observed_years = {value.year for value in observed_dates}
        for year in range(expected_start.year, expected_end.year + 1):
            if year not in observed_years:
                gaps.append(_gap_row(city_id=city_id, slot_id=slot_id, source_id=source_id, gap_type="year_missing", start_date=date(year, 1, 1).isoformat(), end_date=date(year, 12, 31).isoformat(), expected_count=1, observed_count=0, severity="high"))
        if str(source.get("expected_frequency") or "").lower() == "monthly":
            month = date(expected_start.year, expected_start.month, 1)
            while month <= expected_end:
                if not any(value.year == month.year and value.month == month.month for value in observed_dates):
                    gaps.append(_gap_row(city_id=city_id, slot_id=slot_id, source_id=source_id, gap_type="month_missing", start_date=month.isoformat(), end_date=month.isoformat(), expected_count=1, observed_count=0, severity="medium"))
                month = date(month.year + (month.month == 12), 1 if month.month == 12 else month.month + 1, 1)
    if page_numbers:
        pages = sorted({int(value) for value in page_numbers})
        if pages:
            missing = [str(value) for value in range(pages[0], pages[-1] + 1) if value not in pages]
            if missing:
                gaps.append(_gap_row(city_id=city_id, slot_id=slot_id, source_id=source_id, gap_type="page_discontinuity", expected_count=pages[-1] - pages[0] + 1, observed_count=len(pages), affected_urls=missing, severity="medium"))
    for row in rows:
        url = str(row.get("canonical_url") or row.get("url") or "")
        if row.get("parse_status") in {"failed", "error", "parse_failed"}:
            gaps.append(_gap_row(city_id=city_id or row.get("city_id"), slot_id=slot_id or row.get("slot_id"), source_id=source_id or row.get("source_id"), gap_type="parse_failed", affected_urls=[url], severity="high"))
        if not str(row.get("extracted_text") or row.get("body") or row.get("content") or "").strip():
            gaps.append(_gap_row(city_id=city_id or row.get("city_id"), slot_id=slot_id or row.get("slot_id"), source_id=source_id or row.get("source_id"), gap_type="article_body_empty", affected_urls=[url], severity="high"))
        for field_name, gap_type in (("published_at", "publication_date_missing"), ("document_number", "document_number_missing"), ("issuing_agency", "issuing_agency_missing")):
            if not str(row.get(field_name) or "").strip():
                gaps.append(_gap_row(city_id=city_id or row.get("city_id"), slot_id=slot_id or row.get("slot_id"), source_id=source_id or row.get("source_id"), gap_type=gap_type, affected_urls=[url], severity="medium"))
        if row.get("attachment_expected") and not row.get("attachment_hashes") and not row.get("attachments"):
            gaps.append(_gap_row(city_id=city_id or row.get("city_id"), slot_id=slot_id or row.get("slot_id"), source_id=source_id or row.get("source_id"), gap_type="attachment_missing", affected_urls=[url], severity="medium"))
    return list({str(row["gap_id"]): row for row in gaps}.values())


def upsert_coverage_gaps(settings: Settings, rows: Iterable[Mapping[str, Any]]) -> dict[str, int]:
    incoming = [dict(row) for row in rows]
    path = settings.curated / "coverage_gaps.parquet"
    if incoming:
        frame = merge_and_replace_parquet(
            path,
            pl.DataFrame(incoming, infer_schema_length=None),
            ("gap_id",),
            {"module": "full_sync.coverage_gaps"},
        )
    else:
        frame = read_parquet_snapshot(path) if path.exists() else _empty_frame(("gap_id", "status"))
    open_count = int(frame.filter(pl.col("status") == "OPEN").height) if "status" in frame.columns else 0
    return {"incoming": len(incoming), "open": open_count}


def upsert_document_versions(settings: Settings, documents: Iterable[Mapping[str, Any]]) -> dict[str, int]:
    """Idempotently upsert version rows while preserving prior versions."""
    incoming = []
    for raw in documents:
        row = dict(raw)
        row["canonical_document_id"] = row.get("canonical_document_id") or canonical_document_key(row)
        row["version_id"] = row.get("version_id") or document_version_key(row)
        row["document_id"] = row.get("document_id") or row["canonical_document_id"]
        row["version_status"] = row.get("version_status") or "CURRENT"
        row["first_seen_at"] = row.get("first_seen_at") or _now()
        row["last_seen_at"] = _now()
        row["last_changed_at"] = row.get("last_changed_at") or row["first_seen_at"]
        incoming.append(row)
    path = settings.curated / "document_versions.parquet"
    if not incoming:
        return {"inserted": 0, "updated": 0, "unchanged": 0, "withdrawn": 0}
    current_rows = read_parquet_snapshot(path).to_dicts() if path.exists() else []
    by_version = {str(row.get("version_id")): row for row in current_rows}
    by_doc = {}
    for row in current_rows:
        by_doc.setdefault(str(row.get("canonical_document_id")), []).append(row)
    inserted = updated = unchanged = withdrawn = 0
    transitions: list[dict[str, Any]] = []
    for row in incoming:
        version_id = str(row["version_id"])
        previous_version = by_version.get(version_id)
        if previous_version:
            previous_version["last_seen_at"] = row["last_seen_at"]
            by_version[version_id] = previous_version
            unchanged += 1
            continue
        prior_versions = by_doc.get(str(row["canonical_document_id"]), [])
        prior_current = next((item for item in reversed(prior_versions) if str(item.get("version_status")) == "CURRENT"), None)
        if prior_current:
            for item in prior_versions:
                if item is prior_current:
                    item["version_status"] = "SUPERSEDED"
                    transitions.append({"transition_id": stable_id(item.get("version_id"), version_id, prefix="DOCTRANS"), "from_version_id": item.get("version_id"), "to_version_id": version_id, "change_type": classify_document_change(item, row), "created_at": _now()})
        change = classify_document_change(prior_current, row)
        row["version_status"] = "WITHDRAWN" if change == "WITHDRAWN" else ("REVISED" if prior_current and change in {"REVISED", "UPDATED"} else row["version_status"])
        by_version[version_id] = row
        by_doc.setdefault(str(row["canonical_document_id"]), []).append(row)
        if change == "WITHDRAWN":
            withdrawn += 1
        else:
            inserted += 1
        updated += int(prior_current is not None)
    frame = pl.DataFrame(list(by_version.values()), infer_schema_length=None)
    storage_atomic_write_parquet(frame, path, {"module": "full_sync.document_versions"}, key_columns=("version_id",))
    if transitions:
        _append_jsonl(settings.curated / "document_transitions.jsonl", transitions, unique_key="transition_id")
    return {"inserted": inserted, "updated": updated, "unchanged": unchanged, "withdrawn": withdrawn}


@dataclass(slots=True)
class FullSyncConfig:
    scope: str = "all"
    city_id: str | None = None
    slot_id: str | None = None
    source_id: str | None = None
    scope_file: Path | None = None
    discovery_mode: str = "AUTO"
    all_five_source_roles: bool = False
    discover_missing: bool = False
    verify_candidates: bool = False
    enable_ready: bool = False
    backfill: bool = False
    incremental: bool = False
    repair_gaps: bool = False
    until_current: bool = False
    all_remaining: bool = False
    max_slots: int = 20
    max_sources: int = 20
    max_documents: int = 1000
    max_minutes_per_source: int | None = None
    max_list_pages_per_source: int = 20
    max_document_retries: int = 2
    max_attachment_attempts: int = 1
    top_k: int = 3
    concurrency: int = 1
    discovery_concurrency: int = 1
    crawl_concurrency: int = 1
    max_ai_calls: int = 0
    max_search_calls: int = 5
    max_http_calls: int = 100
    budget_usd: float | None = None
    budget_tokens: int | None = None
    rate_limit_per_minute: int = 20
    lookback_days: int = 30
    checkpoint_every: int = 1
    stop_on_error_rate: float = 0.20
    max_consecutive_failures: int = 3
    daily_call_limit: int | None = None
    backfill_from: date | None = None
    backfill_to: date | None = None
    resume: bool = False
    apply: bool = False
    dry_run: bool = False
    confirm_full_sync: bool = False
    output: Path | None = None
    run_id: str | None = None
    report_formats: str = "json,xlsx,parquet"
    pdf_enabled: bool = False
    pdf_discover: bool = True
    pdf_download: bool = True
    pdf_parse: bool = True
    pdf_max_downloads_per_source: int = 20
    pdf_max_downloads_per_job: int = 30

    def validate(self, *, command: str = "run") -> None:
        if self.scope not in {"all", "city", "slot", "source"}:
            raise ValueError("scope must be all, city, slot, or source")
        if self.scope == "city" and not self.city_id:
            raise ValueError("--city-id is required for --scope city")
        if self.scope == "slot" and not self.slot_id:
            raise ValueError("--slot-id is required for --scope slot")
        if self.scope == "source" and not self.source_id:
            raise ValueError("--source-id is required for --scope source")
        if self.discovery_mode.upper() not in {"AUTO", "DISABLED", "SEARCH_ONLY", "AI_ONLY", "SEARCH_AND_AI"}:
            raise ValueError("discovery-mode must be AUTO, DISABLED, SEARCH_ONLY, AI_ONLY, or SEARCH_AND_AI")
        positive = {"max_slots": self.max_slots, "max_sources": self.max_sources, "max_documents": self.max_documents, "top_k": self.top_k, "concurrency": self.concurrency, "discovery_concurrency": self.discovery_concurrency, "crawl_concurrency": self.crawl_concurrency, "rate_limit_per_minute": self.rate_limit_per_minute, "lookback_days": self.lookback_days, "checkpoint_every": self.checkpoint_every, "max_consecutive_failures": self.max_consecutive_failures, "max_list_pages_per_source": self.max_list_pages_per_source, "max_document_retries": self.max_document_retries, "max_attachment_attempts": self.max_attachment_attempts, "pdf_max_downloads_per_source": self.pdf_max_downloads_per_source, "pdf_max_downloads_per_job": self.pdf_max_downloads_per_job}
        if any(int(value) < 1 for value in positive.values()):
            raise ValueError("all execution limits must be positive")
        if self.max_minutes_per_source is not None and int(self.max_minutes_per_source) < 1:
            raise ValueError("max-minutes-per-source must be positive when set")
        if self.max_ai_calls < 0 or self.max_search_calls < 0 or self.max_http_calls < 1:
            raise ValueError("call limits are invalid")
        if not 0 < self.stop_on_error_rate <= 1:
            raise ValueError("stop-on-error-rate must be in (0, 1]")
        if self.budget_usd is not None and self.budget_usd < 0:
            raise ValueError("budget-usd must be non-negative")
        if self.budget_tokens is not None and self.budget_tokens < 0:
            raise ValueError("budget-tokens must be non-negative")
        if command in {"run", "resume", "refresh", "repair"} and self.apply and self.all_remaining and not self.confirm_full_sync:
            raise ValueError("real all-remaining full-sync requires --confirm-full-sync")

    @classmethod
    def from_namespace(cls, args: object, *, command: str) -> FullSyncConfig:
        values = {field_name: getattr(args, field_name, None) for field_name in cls.__dataclass_fields__}
        for field_name in ("max_minutes_per_source", "max_list_pages_per_source", "max_document_retries", "max_attachment_attempts"):
            if values.get(field_name) is None:
                values.pop(field_name, None)
        if values.get("output") is None:
            values.pop("output", None)
        if values.get("run_id") is None:
            values.pop("run_id", None)
        if command == "refresh":
            values["incremental"] = True
            values["repair_gaps"] = bool(getattr(args, "repair_gaps", False))
        if command == "repair":
            values["repair_gaps"] = True
        return cls(**values)


def effective_discovery_mode(config: FullSyncConfig, *, ai_available: bool = True, search_available: bool = True, existing_candidates: bool = False) -> str:
    """Resolve independent AI/search budgets into an explicit mode."""
    requested = str(config.discovery_mode or "AUTO").upper()
    if not config.discover_missing or requested == "DISABLED":
        return "DISABLED"
    if requested == "AUTO":
        if config.max_ai_calls > 0 and config.max_search_calls > 0:
            requested = "SEARCH_AND_AI"
        elif config.max_search_calls > 0:
            requested = "SEARCH_ONLY"
        elif config.max_ai_calls > 0 and existing_candidates:
            requested = "AI_ONLY"
        else:
            return "DISABLED"
    if requested == "SEARCH_AND_AI" and not ai_available and search_available:
        return "SEARCH_ONLY"
    if requested == "AI_ONLY" and not existing_candidates:
        return "AI_ONLY"
    return requested


@dataclass(slots=True)
class BudgetLedger:
    path: Path
    limits: dict[str, int | float | None]
    used: dict[str, int | float] = field(default_factory=lambda: {"ai_calls": 0, "search_calls": 0, "http_calls": 0, "tokens": 0, "usd": 0.0})
    _lock: threading.RLock = field(default_factory=threading.RLock, init=False, repr=False)

    def __post_init__(self) -> None:
        with self._lock:
            if self.path.exists():
                try:
                    saved = json.loads(self.path.read_text(encoding="utf-8-sig"))
                    self.used.update(saved.get("used", {}))
                except (OSError, json.JSONDecodeError):
                    pass
        self.persist()

    def persist(self) -> None:
        with self._lock:
            _atomic_json(self.path, {"limits": self.limits, "used": self.used, "updated_at": _now()})

    def reserve(self, kind: str, amount: int | float = 1) -> None:
        aliases = {"ai": "ai_calls", "search": "search_calls", "http": "http_calls", "token": "tokens", "cost": "usd"}
        key = aliases.get(kind, kind)
        with self._lock:
            limit = self.limits.get(key)
            if limit is not None and float(self.used.get(key, 0)) + float(amount) > float(limit):
                raise BudgetExceeded(f"{key} budget exceeded")
            self.used[key] = self.used.get(key, 0) + amount
            self.persist()

    def consume_actual(self, kind: str, event: Mapping[str, Any] | None = None) -> dict[str, Any]:
        """Atomically count one real API/network attempt.

        Planning, parsing, upsert and checkpoint code must use neither this
        method nor ``reserve``.  ``event`` is copied into the returned audit
        record and never contains request headers or credentials.
        """
        aliases = {"ai": "ai_calls", "search": "search_calls", "http": "http_calls"}
        key = aliases.get(kind, kind)
        with self._lock:
            before = self.used.get(key, 0)
            limit = self.limits.get(key)
            if limit is not None and float(before) + 1 > float(limit):
                if key == "http_calls":
                    raise HttpBudgetExceeded(f"{key} budget exceeded")
                raise BudgetExceeded(f"{key} budget exceeded")
            self.used[key] = before + 1
            after = self.used[key]
            self.persist()
        return {
            **dict(event or {}),
            "budget_before": before,
            "budget_after": after,
            "budget_key": key,
        }

    def record_usage(self, kind: str, amount: int | float) -> dict[str, Any]:
        """Persist provider-reported usage without inventing missing values."""
        aliases = {"token": "tokens", "cost": "usd"}
        key = aliases.get(kind, kind)
        with self._lock:
            before = self.used.get(key, 0)
            self.used[key] = before + amount
            self.persist()
            return {"budget_key": key, "usage_before": before, "usage_after": self.used[key]}


class HttpAttemptRecorder:
    """Count and persist real outbound HTTP attempts exactly once.

    The network layer calls ``before`` immediately before an actual request
    and ``after`` when a response or exception is available.  A started row is
    retained if the process is interrupted; audits count unique attempt IDs,
    not lifecycle rows.
    """

    def __init__(self, ledger: BudgetLedger, path: Path, *, run_id: str, source_id: str, slot_id: str | None, stage: str):
        self.ledger = ledger
        self.path = path
        self.run_id = run_id
        self.source_id = source_id
        self.slot_id = slot_id
        self.stage = stage
        self._attempts: dict[str, dict[str, Any]] = {}
        self._lock = threading.RLock()

    def before(self, event: Mapping[str, Any]) -> str:
        url = str(event.get("url") or "")
        attempt = int(event.get("attempt") or 1)
        attempt_id = stable_id(
            self.run_id,
            self.source_id,
            self.slot_id or "",
            self.stage,
            canonicalize_url(url),
            str(attempt),
            str(time.time_ns()),
            prefix="HTTPATTEMPT",
        )
        counted = self.ledger.consume_actual(
            "http",
            {
                "attempt_id": attempt_id,
                "run_id": self.run_id,
                "source_id": self.source_id,
                "slot_id": self.slot_id,
                "stage": str(event.get("stage") or self.stage),
                "url_hash": hashlib.sha256(canonicalize_url(url).encode("utf-8")).hexdigest(),
                "attempt": attempt,
                "status": "started",
                "phase": "started",
                "started_at": _now(),
            },
        )
        with self._lock:
            row = {**counted, "request_id": attempt_id}
            self._attempts[attempt_id] = row
            _append_jsonl(self.path, [row])
        return attempt_id

    def __call__(self, event: Mapping[str, Any]) -> str | None:
        if str(event.get("phase") or "before") == "before":
            return self.before(event)
        self.after(event)
        return str(event.get("attempt_id") or "") or None

    def after(self, event: Mapping[str, Any]) -> None:
        attempt_id = str(event.get("attempt_id") or "")
        if not attempt_id:
            return
        with self._lock:
            started = dict(self._attempts.get(attempt_id, {}))
            if not started or started.get("phase") == "completed":
                return
            completed = {
                **started,
                "phase": "completed",
                "status": "completed" if event.get("status_code") is not None else "failed",
                "status_code": event.get("status_code"),
                "error_type": event.get("error_type"),
                "error_message": str(event.get("error_message") or "")[:500] or None,
                "completed_at": _now(),
            }
            _append_jsonl(self.path, [completed])
            self._attempts[attempt_id] = completed


class JobLeaseStore:
    """Small file-backed lease store for cross-run source/URL claims."""

    def __init__(self, path: Path, *, run_id: str):
        self.path = path
        self.run_id = run_id
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def _load(self) -> dict[str, dict[str, Any]]:
        if not self.path.exists():
            return {}
        try:
            value = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}
        return value if isinstance(value, dict) else {}

    def _save(self, value: Mapping[str, Any]) -> None:
        _atomic_json(self.path, value)

    def claim(self, resource_type: str, resource_id: str, *, lease_seconds: int = 900) -> dict[str, Any]:
        key = f"{resource_type}:{resource_id}"
        leases = self._load()
        now = datetime.now(UTC)
        current = leases.get(key)
        if current:
            expires = _parse_datetime(current.get("expires_at"))
            if expires and expires > now and current.get("run_id") != self.run_id:
                raise LeaseConflict(f"live lease exists for {key}")
        lease = {"lease_key": key, "resource_type": resource_type, "resource_id": resource_id, "run_id": self.run_id, "worker_id": f"{os.getpid()}", "claimed_at": now.isoformat(), "heartbeat_at": now.isoformat(), "expires_at": (now + timedelta(seconds=lease_seconds)).isoformat(), "status": "CLAIMED"}
        leases[key] = lease
        self._save(leases)
        return lease

    def heartbeat(self, resource_type: str, resource_id: str, *, lease_seconds: int = 900) -> dict[str, Any]:
        lease = self.claim(resource_type, resource_id, lease_seconds=lease_seconds)
        lease["heartbeat_at"] = _now()
        lease["expires_at"] = (datetime.now(UTC) + timedelta(seconds=lease_seconds)).isoformat()
        leases = self._load()
        leases[lease["lease_key"]] = lease
        self._save(leases)
        return lease

    def release(self, resource_type: str, resource_id: str, *, status: str = "RELEASED") -> None:
        key = f"{resource_type}:{resource_id}"
        leases = self._load()
        lease = leases.get(key)
        if lease and lease.get("run_id") == self.run_id:
            lease = {**lease, "status": status, "released_at": _now(), "expires_at": _now()}
            leases[key] = lease
            self._save(leases)

    def recover_stale(self) -> list[dict[str, Any]]:
        leases = self._load()
        now = datetime.now(UTC)
        recovered = []
        for key, lease in leases.items():
            expires = _parse_datetime(lease.get("expires_at"))
            if lease.get("status") == "CLAIMED" and expires and expires <= now:
                recovered.append({**lease, "status": "STALE_RECOVERED", "recovered_at": now.isoformat()})
                leases[key] = recovered[-1]
        if recovered:
            self._save(leases)
        return recovered


def _pipeline_run_with_retry(
    pipeline: CrawlPipeline,
    run_id: str,
    *,
    max_fetches: int,
    retries: int = 2,
    cancel_check=None,
    max_attachment_attempts: int | None = None,
) -> dict[str, Any]:
    """Retry only transient Windows Parquet mapping/share failures.

    The crawler itself remains the owner of fetch and document checkpoints;
    retrying it is safe because it selects only pending items on each attempt.
    Other exceptions are surfaced immediately as recoverable failures.
    """
    for attempt in range(retries + 1):
        try:
            result = pipeline.run(
                run_id,
                max_fetches=max_fetches,
                cancel_check=cancel_check,
                max_attachment_attempts=max_attachment_attempts,
            )
            runs_path = pipeline.settings.curated / "crawl_runs.parquet"
            if runs_path.exists():
                runs = read_parquet_snapshot(runs_path)
                selected = runs.filter(pl.col("run_id").cast(pl.String) == run_id)
                if selected.height:
                    row = selected.row(-1, named=True)
                    result = {
                        **result,
                        "status": str(row.get("status") or "unknown"),
                        "persisted_fetched": int(row.get("fetched_count") or 0),
                        "persisted_failed": int(row.get("failed_count") or 0),
                        "pending": int(row.get("item_count") or 0) - int(row.get("fetched_count") or 0) - int(row.get("failed_count") or 0),
                    }
            return result
        except OSError as exc:
            message = str(exc).lower()
            transient = "1224" in message or "mapped section" in message or "sharing violation" in message
            if not transient or attempt >= retries:
                raise
            time.sleep(0.25 * (attempt + 1))
    raise FullSyncError("pipeline retry loop exhausted")


class SyncStateStore:
    def __init__(self, run_dir: Path, *, run_id: str):
        self.run_dir = run_dir
        self.run_id = run_id
        self.run_dir.mkdir(parents=True, exist_ok=True)
        self.status_path = self.run_dir / "current_status.json"
        self.transitions_path = self.run_dir / "state_transitions.jsonl"
        self.claims_path = self.run_dir / "job_claims.jsonl"
        self.checkpoints_path = self.run_dir / "sync_checkpoints.jsonl"

    def read_status(self) -> dict[str, Any]:
        if not self.status_path.exists():
            return {"run_id": self.run_id, "global_status": "INITIALIZING", "status": "INITIALIZING", "run_started_at": _now()}
        try:
            return json.loads(self.status_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {"run_id": self.run_id, "global_status": "INITIALIZING", "status": "INITIALIZING"}

    def write_status(self, updates: Mapping[str, Any]) -> dict[str, Any]:
        current = self.read_status()
        current.update(dict(updates))
        current["run_id"] = self.run_id
        current["last_progress_at"] = _now()
        current["last_heartbeat_at"] = _now()
        _atomic_json(self.status_path, current)
        return current

    def transition(self, event_type: str, *, reason_code: str, slot_id: str | None = None, source_id: str | None = None, state: str | None = None, **extra: Any) -> dict[str, Any]:
        idempotency_key = hashlib.sha256(json.dumps([self.run_id, event_type, slot_id, source_id, reason_code], ensure_ascii=False).encode("utf-8")).hexdigest()
        event = {"run_id": self.run_id, "slot_id": slot_id, "source_id": source_id, "event_type": event_type, "reason_code": reason_code, "timestamp": _now(), "idempotency_key": idempotency_key, **extra}
        written = _append_jsonl(self.transitions_path, [event], unique_key="idempotency_key")
        if state:
            self.write_status({"current_step": event_type, "state": state})
        elif written:
            self.write_status({"current_step": event_type})
        return event

    def checkpoint(self, checkpoint_type: str, *, resource_id: str | None = None, **payload: Any) -> dict[str, Any]:
        checkpoint = {"checkpoint_id": stable_id(self.run_id, checkpoint_type, resource_id or "", prefix="SYNCCHK"), "run_id": self.run_id, "checkpoint_type": checkpoint_type, "resource_id": resource_id, "created_at": _now(), **payload}
        _append_jsonl(self.checkpoints_path, [checkpoint], unique_key="checkpoint_id")
        return checkpoint

    def completed_resources(self, checkpoint_type: str) -> set[str]:
        if not self.checkpoints_path.exists():
            return set()
        result = set()
        for line in self.checkpoints_path.read_text(encoding="utf-8").splitlines():
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if row.get("checkpoint_type") == checkpoint_type and row.get("resource_id"):
                result.add(str(row["resource_id"]))
        return result


def load_test_evidence(settings: Settings, path: Path | None = None) -> dict[str, Any]:
    candidate = path or (Path(os.environ["POLICYDB_TEST_EVIDENCE_PATH"]) if os.getenv("POLICYDB_TEST_EVIDENCE_PATH") else settings.outputs / "autopilot" / "latest_test_evidence.json")
    if not candidate.exists():
        return {"test_result": "unknown", "overall_status": "unknown", "test_evidence_path": str(candidate)}
    try:
        evidence = json.loads(candidate.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError):
        return {"test_result": "unknown", "overall_status": "unknown", "test_evidence_path": str(candidate)}
    if not isinstance(evidence, dict):
        return {"test_result": "unknown", "overall_status": "unknown", "test_evidence_path": str(candidate)}
    # A detailed pytest report carries process stdout/stderr and a JUnit path.
    # Recompute the tri-state from those immutable artifacts so malformed JUnit
    # does not erase a reliable pytest exit code or terminal summary.
    if any(key in evidence for key in ("stdout", "stderr", "junit", "junit_path", "exit_code")):
        parsed = parse_pytest_report_file(candidate)
        parsed.update({key: value for key, value in evidence.items() if key not in parsed})
        evidence = parsed
    result = str(evidence.get("overall_status") or evidence.get("test_result") or "unknown").lower()
    evidence["test_result"] = "passed" if result in {"passed", "pass", "success"} else "failed" if result in {"failed", "fail", "error"} else "unknown"
    evidence["test_evidence_path"] = str(candidate)
    return evidence


def _scope_filter(queue: pl.DataFrame, config: FullSyncConfig) -> pl.DataFrame:
    if queue.is_empty():
        return queue
    if config.scope_file:
        try:
            payload = json.loads(config.scope_file.read_text(encoding="utf-8-sig"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ValueError(f"invalid scope file: {config.scope_file}") from exc
        slot_ids = {str(value) for value in payload.get("slot_ids", []) if value}
        city_ids = {str(value) for value in payload.get("city_ids", []) if value}
        if payload.get("city_id"):
            city_ids.add(str(payload["city_id"]))
        if slot_ids:
            queue = queue.filter(pl.col("slot_id").cast(pl.String).is_in(sorted(slot_ids)))
        elif city_ids:
            queue = queue.filter(pl.col("city_id").cast(pl.String).is_in(sorted(city_ids)))
    if config.scope == "city":
        return queue.filter(pl.col("city_id").cast(pl.String) == str(config.city_id))
    if config.scope == "slot":
        return queue.filter(pl.col("slot_id").cast(pl.String) == str(config.slot_id))
    return queue


def _load_sync_state(settings: Settings) -> dict[str, dict[str, Any]]:
    path = settings.curated / "source_sync_state.parquet"
    if not path.exists():
        return {}
    return {str(row.get("source_id")): row for row in read_parquet_snapshot(path).to_dicts() if row.get("source_id")}


_ALLOWED_TERMINATION_REASONS = {
    "END_OF_PAGINATION",
    "DATE_BOUNDARY_REACHED",
    "EMPTY_TERMINAL_PAGE",
    "OFFICIAL_EXPLICIT_LAST_PAGE",
    "COMPLETE_WITH_GAPS",
    # Read old persisted evidence without treating it as new evidence.
    "next_page_absent",
    "explicit_last_page_reached",
    "archive_start_reached",
    "configured_start_date_reached",
    "source_declared_end_reached",
    "consecutive_duplicate_pages_threshold",
}


def _has_backfill_completion_evidence(
    settings: Settings,
    source_id: str,
    *,
    run_id: str | None = None,
    period_start: date | None = None,
    period_end: date | None = None,
) -> bool:
    path = settings.curated / "crawl_source_windows.parquet"
    if not path.exists():
        return False
    frame = read_parquet_snapshot(path)
    if frame.is_empty() or "source_id" not in frame.columns:
        return False
    selected = frame.filter(pl.col("source_id").cast(pl.String) == source_id)
    if run_id and "run_id" in selected.columns:
        selected = selected.filter(pl.col("run_id").cast(pl.String) == run_id)
    if period_start and "period_start" in selected.columns:
        selected = selected.filter(pl.col("period_start").cast(pl.String) == period_start.isoformat())
    if period_end and "period_end" in selected.columns:
        selected = selected.filter(pl.col("period_end").cast(pl.String) == period_end.isoformat())
    if selected.is_empty():
        return False
    complete = selected.filter(pl.col("is_complete") == True)  # noqa: E712
    if complete.is_empty():
        return False
    if "coverage_status" in complete.columns:
        complete = complete.filter(pl.col("coverage_status").cast(pl.String).str.starts_with("complete_"))
    if complete.is_empty() or "completion_evidence" not in complete.columns:
        return False
    for evidence_text in complete["completion_evidence"].drop_nulls().to_list():
        try:
            evidence = json.loads(str(evidence_text))
        except json.JSONDecodeError:
            continue
        termination_reason = str(evidence.get("termination_reason") or "")
        evidence_ids = evidence.get("termination_evidence_ids") or []
        if (
            evidence.get("pagination_complete") is True
            and termination_reason in _ALLOWED_TERMINATION_REASONS
            and evidence_ids
            and evidence.get("transaction_committed") is True
            and evidence.get("checkpoint_persisted") is True
            and evidence.get("completion_invariants_passed") is True
        ):
            return True
    return False


def _source_records(settings: Settings, config: FullSyncConfig, sync_state: Mapping[str, Mapping[str, Any]]) -> list[dict[str, Any]]:
    records = []
    scope_source_ids: set[str] = set()
    scope_roles: set[str] = set()
    scope_city_ids: set[str] = set()
    if config.scope_file:
        try:
            scope_payload = json.loads(config.scope_file.read_text(encoding="utf-8-sig"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ValueError(f"invalid scope file: {config.scope_file}") from exc
        if isinstance(scope_payload, Mapping):
            scope_source_ids = {str(value) for value in scope_payload.get("source_ids", []) if value}
            scope_roles = {str(value) for value in scope_payload.get("source_roles", []) if value}
            scope_city_ids = {str(value) for value in scope_payload.get("city_ids", []) if value}
            if scope_payload.get("city_id"):
                scope_city_ids.add(str(scope_payload["city_id"]))
    for source in load_registry(settings):
        row = _as_dict(source)
        if config.source_id and str(row.get("source_id")) != config.source_id:
            continue
        if config.scope == "city" and config.city_id not in {str(value) for value in row.get("city_ids") or []}:
            continue
        source_id = str(row.get("source_id"))
        source_role_values = {
            str(row.get("source_role") or ""),
            str(row.get("agency_type") or ""),
        }
        is_scoped_replacement = bool(
            scope_source_ids
            and scope_city_ids
            and scope_city_ids.intersection({str(value) for value in row.get("city_ids") or []})
            and (not scope_roles or scope_roles.intersection(source_role_values))
        )
        if scope_source_ids and source_id not in scope_source_ids and not is_scoped_replacement:
            continue
        if scope_city_ids and not scope_city_ids.intersection({str(value) for value in row.get("city_ids") or []}):
            continue
        if scope_roles and not scope_roles.intersection({str(row.get("source_role") or ""), str(row.get("agency_type") or "")}):
            continue
        if config.scope == "slot":
            source_slot = row.get("slot_id")
            if source_slot and str(source_slot) != config.slot_id:
                continue
        saved = sync_state.get(str(row.get("source_id")), {})
        for field_name in (
            "backfill_status",
            "next_retry_at",
            "source_status",
            "historical_watermark",
            "incremental_watermark",
            "current_watermark",
            "watermark_audit_id",
            "last_successful_crawl_at",
            "last_incremental_sync_at",
            "freshness_status",
        ):
            if saved.get(field_name) is not None:
                row[field_name] = saved[field_name]
        if str(saved.get("backfill_status") or "").lower() in {"complete", "complete_with_gaps", "backfilled", "backfill_complete"} and not _has_backfill_completion_evidence(settings, str(row.get("source_id"))):
            saved = {**saved, "backfill_status": "partial", "source_status": "CRAWL_READY"}
        row["source_state"] = classify_source_state(row, saved)
        row["slot_id"] = row.get("slot_id") or (str(config.slot_id) if config.scope == "slot" else None)
        row["city_id"] = row.get("city_id") or (row.get("city_ids") or [None])[0]
        row["freshness_status"] = source_freshness_status(row.get("last_successful_crawl_at") or row.get("last_success_at"), str(row.get("source_role") or ""))
        records.append(row)
    return records


def _source_sync_row(
    source: Mapping[str, Any],
    *,
    state: str | None = None,
    backfill_status: str | None = None,
    error: str | None = None,
    overrides: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    now = _now()
    existing = dict(source)
    result = {
        "source_id": existing.get("source_id"),
        "slot_id": existing.get("slot_id"),
        "city_id": existing.get("city_id") or (existing.get("city_ids") or [None])[0],
        "city_name": existing.get("city_name"),
        "source_role": existing.get("source_role"),
        "agency_name": existing.get("agency_name") or existing.get("source_name"),
        "agency_aliases": json.dumps(existing.get("agency_aliases") or [], ensure_ascii=False),
        "official_domain": existing.get("domain"),
        "homepage_url": existing.get("homepage_url"),
        "list_url": (existing.get("list_page_urls") or [None])[0],
        "canonical_list_url": (existing.get("list_page_urls") or [None])[0],
        "parser_type": existing.get("parser_adapter") or existing.get("parser_type"),
        "pagination_type": existing.get("pagination_type"),
        "verified": bool(existing.get("verified", existing.get("official_domain_verified", False))),
        "enabled": bool(existing.get("enabled", existing.get("crawl_enabled", False))),
        "crawl_enabled": bool(existing.get("crawl_enabled", False)),
        "verification_status": existing.get("verification_status") or existing.get("official_status"),
        "verification_evidence_ids": json.dumps(existing.get("verification_evidence_ids") or [], ensure_ascii=False),
        "strict_gate_status": existing.get("strict_gate_status") or ("passed" if source_is_crawl_ready(existing) else "pending"),
        "strict_rejection_reasons": json.dumps(existing.get("strict_rejection_reasons") or ([error] if error else []), ensure_ascii=False),
        "first_verified_at": existing.get("first_verified_at") or existing.get("verified_at"),
        "last_verified_at": existing.get("last_verified_at") or existing.get("verified_at"),
        "last_health_check_at": existing.get("last_health_check_at") or existing.get("last_health_at"),
        "last_successful_crawl_at": existing.get("last_successful_crawl_at") or existing.get("last_success_at"),
        "last_incremental_sync_at": existing.get("last_incremental_sync_at"),
        "last_seen_document_at": existing.get("last_seen_document_at") or existing.get("last_policy_seen_at"),
        "backfill_status": backfill_status or existing.get("backfill_status") or "not_started",
        "backfill_start_date": existing.get("backfill_start_date") or existing.get("coverage_start_date"),
        "backfill_end_date": existing.get("backfill_end_date") or existing.get("coverage_end_date"),
        "backfill_completed_at": existing.get("backfill_completed_at"),
        "current_watermark": json.dumps(existing.get("current_watermark") or {}, ensure_ascii=False, sort_keys=True),
        "historical_watermark": json.dumps(_load_watermark(existing.get("historical_watermark")), ensure_ascii=False, sort_keys=True),
        "incremental_watermark": json.dumps(_load_watermark(existing.get("incremental_watermark")), ensure_ascii=False, sort_keys=True),
        "watermark_audit_id": existing.get("watermark_audit_id"),
        "source_status": state or existing.get("source_status") or "unknown",
        "freshness_status": existing.get("freshness_status") or "unknown",
        "consecutive_failures": int(existing.get("consecutive_failures") or 0),
        "next_retry_at": existing.get("next_retry_at"),
        "created_at": existing.get("created_at") or now,
        "updated_at": now,
        "last_error": error or existing.get("last_error"),
    }
    result.update(dict(overrides or {}))
    return result


def _write_source_sync_state(settings: Settings, rows: Iterable[Mapping[str, Any]]) -> None:
    incoming = [dict(row) for row in rows]
    if not incoming:
        return
    path = settings.curated / "source_sync_state.parquet"
    merge_and_replace_parquet(
        path,
        pl.DataFrame(incoming, infer_schema_length=None),
        ("source_id",),
        {"module": "full_sync.source_sync_state"},
    )


def _load_watermark(value: object) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if value:
        try:
            loaded = json.loads(str(value))
        except json.JSONDecodeError:
            return {}
        return dict(loaded) if isinstance(loaded, Mapping) else {}
    return {}


def _persist_watermark(
    settings: Settings,
    *,
    source: Mapping[str, Any],
    stage: str,
    previous: Mapping[str, Any],
    proposed: Mapping[str, Any],
    evidence_ids: Sequence[str],
    source_state_before: str | None,
    source_state_after: str | None,
    run_id: str,
    job_id: str,
) -> dict[str, Any]:
    transaction_id = stable_id(run_id, job_id, source.get("source_id"), stage, prefix="WATERMARKTX")
    row = {
        "transaction_id": transaction_id,
        "run_id": run_id,
        "job_id": job_id,
        "source_id": source.get("source_id"),
        "stage": stage,
        "previous_value": json.dumps(dict(previous), ensure_ascii=False, sort_keys=True),
        "proposed_value": json.dumps(dict(proposed), ensure_ascii=False, sort_keys=True),
        "committed_value": json.dumps(dict(proposed), ensure_ascii=False, sort_keys=True),
        "update_reason": f"{stage}_transaction_committed",
        "evidence_ids": json.dumps(list(evidence_ids), ensure_ascii=False),
        "source_state_before": source_state_before,
        "source_state_after": source_state_after,
        "created_at": _now(),
    }
    _append_jsonl(settings.curated / "watermark_audit.jsonl", [row], unique_key="transaction_id")
    return row


def _write_excel(frame: pl.DataFrame, path: Path, columns: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if frame.is_empty():
        frame = _empty_frame(columns)
    frame.write_excel(path, autofit=True)


def _read_documents(settings: Settings, source_id: str | None = None) -> list[dict[str, Any]]:
    paths = [settings.curated / "document_versions.parquet", settings.curated / "policy_document_versions.parquet"]
    collected: dict[str, dict[str, Any]] = {}
    for path in paths:
        if path.exists():
            for row in read_parquet_snapshot(path).to_dicts():
                if source_id is not None and str(row.get("source_id")) != source_id:
                    continue
                key = str(row.get("version_id") or row.get("document_version_id") or stable_id(row, prefix="DOCROW"))
                collected[key] = row
    return list(collected.values())


def database_sync_status(
    settings: Settings,
    slots: Iterable[Mapping[str, Any]],
    sources: Iterable[Mapping[str, Any]],
    *,
    open_gaps: int = 0,
    critical_gaps: int = 0,
    last_run: Mapping[str, Any] | None = None,
    last_successful_full_sync: str | None = None,
) -> dict[str, Any]:
    slot_rows = list(slots)
    source_rows = list(sources)
    total_slots = len(slot_rows)
    verified = sum(str(row.get("slot_state")) in {"VERIFIED", "ENABLED", "CRAWL_READY", "BACKFILLED", "CURRENT", "CURRENT_WITH_WARNINGS"} for row in slot_rows)
    enabled = sum(str(row.get("slot_state")) in {"ENABLED", "CRAWL_READY", "BACKFILLED", "CURRENT", "CURRENT_WITH_WARNINGS"} for row in slot_rows)
    ready = sum(str(row.get("slot_state")) in {"CRAWL_READY", "BACKFILLED", "CURRENT", "CURRENT_WITH_WARNINGS"} for row in slot_rows)
    backfilled = sum(str(row.get("slot_state")) in {"BACKFILLED", "CURRENT", "CURRENT_WITH_WARNINGS"} for row in slot_rows)
    current = sum(str(row.get("slot_state")) in {"CURRENT", "CURRENT_WITH_WARNINGS"} for row in slot_rows)
    human = sum(str(row.get("slot_state")) == "HUMAN_REVIEW" for row in slot_rows)
    unresolved = sum(str(row.get("slot_state")) in {"UNRESOLVED", "DISCOVERING", "CANDIDATES_FOUND"} for row in slot_rows)
    retries = sum(str(row.get("slot_state")) == "RETRY_WAIT" for row in slot_rows)
    stale = sum(str(row.get("source_state")) == "STALE" for row in source_rows)
    degraded = sum(str(row.get("source_state")) in {"DEGRADED", "UNREACHABLE", "PARSER_BROKEN"} for row in source_rows)
    source_statuses = [str(row.get("source_status") or row.get("sync_status") or "").upper() for row in source_rows]
    docs = _read_documents(settings)
    parsed = sum(str(row.get("parse_status") or "").lower() in {"ok", "parsed", "success"} for row in docs)
    quality = parsed / len(docs) if docs else 0.0
    backfill_ratio = backfilled / ready if ready else 0.0
    freshness = sum(str(row.get("freshness_status")) == "current" for row in source_rows) / len(source_rows) if source_rows else 0.0
    status = derive_global_status(slot_rows, source_rows, open_gaps=open_gaps)
    return {
        "generated_at": _now(),
        "global_status": status,
        "total_cities": len({str(row.get("city_id")) for row in slot_rows if row.get("city_id")}),
        "total_slots": total_slots,
        "resolved_slots": total_slots - unresolved,
        "verified_slots": verified,
        "enabled_slots": enabled,
        "crawl_ready_slots": ready,
        "backfilled_slots": backfilled,
        "current_slots": current,
        "human_review_slots": human,
        "unresolved_slots": unresolved,
        "retry_wait_slots": retries,
        "degraded_sources": degraded,
        "stale_sources": stale,
        "successful_sources": sum(value in {"SUCCESS", "COMPLETED"} for value in source_statuses),
        "complete_with_gaps_sources": sum(value == "COMPLETE_WITH_GAPS" for value in source_statuses),
        "partial_sources": sum(value == "PARTIAL" for value in source_statuses),
        "paused_budget_sources": sum(value == "PAUSED_BUDGET" for value in source_statuses),
        "retry_wait_sources": sum(value == "RETRY_WAIT" for value in source_statuses),
        "terminal_failed_sources": sum(value in {"FAILED", "FAILED_TERMINAL"} for value in source_statuses),
        "skipped_dependency_sources": sum(value == "SKIPPED_DEPENDENCY" for value in source_statuses),
        "human_review_sources": sum(value == "HUMAN_REVIEW" for value in source_statuses),
        "total_documents": len(docs),
        "documents_added_last_run": int((last_run or {}).get("documents_added", 0)),
        "documents_updated_last_run": int((last_run or {}).get("documents_updated", 0)),
        "documents_withdrawn_last_run": int((last_run or {}).get("documents_withdrawn", 0)),
        "open_gaps": open_gaps,
        "critical_gaps": critical_gaps,
        "oldest_source_update": min((str(row.get("last_successful_crawl_at")) for row in source_rows if row.get("last_successful_crawl_at")), default=None),
        "newest_source_update": max((str(row.get("last_successful_crawl_at")) for row in source_rows if row.get("last_successful_crawl_at")), default=None),
        "coverage_ratio": verified / total_slots if total_slots else 0.0,
        "backfill_ratio": backfill_ratio,
        "freshness_ratio": freshness,
        "data_quality_ratio": quality,
        "last_completed_run": (last_run or {}).get("completed_at"),
        "last_partially_successful_run": (last_run or {}).get("partially_successful_at"),
        "last_successful_full_sync": last_successful_full_sync if last_successful_full_sync is not None else (last_run or {}).get("completed_at"),
        "next_recommended_action": "repair_open_gaps" if open_gaps else "continue_source_completion" if unresolved else "run_incremental_refresh",
    }


def _read_curated_table(settings: Settings, name: str) -> pl.DataFrame:
    path = settings.curated / f"{name}.parquet"
    return read_parquet_snapshot(path) if path.exists() else pl.DataFrame()


def _filter_report_documents(
    settings: Settings,
    *,
    source_ids: set[str],
    city_id: str | None,
) -> pl.DataFrame:
    rows = _read_documents(settings)
    if source_ids:
        rows = [row for row in rows if str(row.get("source_id")) in source_ids]
    if city_id:
        rows = [row for row in rows if str(row.get("city_id")) == city_id or not row.get("city_id")]
    return pl.DataFrame(rows, infer_schema_length=None) if rows else pl.DataFrame()


def build_full_sync_report(
    settings: Settings,
    config: FullSyncConfig,
    *,
    output_dir: Path | None = None,
    status_override: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build one consistent, evidence-backed coverage/completeness snapshot."""

    plan = build_sync_plan(settings, config)
    status = dict(status_override or plan["database_sync_status"])
    sources = [dict(row) for row in plan["sources"]]
    source_ids = {str(row.get("source_id")) for row in sources if row.get("source_id")}
    city_id = str(config.city_id) if config.scope == "city" and config.city_id else None
    slots = [dict(row) for row in plan["slot_rows"]]
    documents = _filter_report_documents(settings, source_ids=source_ids, city_id=city_id)
    items = _read_curated_table(settings, "crawl_items")
    runs = _read_curated_table(settings, "crawl_runs")
    windows = _read_curated_table(settings, "crawl_source_windows")
    attachments = _read_curated_table(settings, "attachments")
    gaps = _read_curated_table(settings, "coverage_gaps")
    if source_ids:
        for name, frame in (("items", items), ("windows", windows), ("attachments", attachments), ("gaps", gaps)):
            if frame.height and "source_id" in frame.columns:
                filtered = frame.filter(pl.col("source_id").cast(pl.String).is_in(sorted(source_ids)))
                if name == "items":
                    items = filtered
                elif name == "windows":
                    windows = filtered
                elif name == "attachments":
                    attachments = filtered
                else:
                    gaps = filtered
    terminal_statuses = {"fetched", "unchanged", "failed"}
    terminal_links = int(items.filter(pl.col("status").is_in(sorted(terminal_statuses))).height) if items.height and "status" in items.columns else 0
    fetched = int(items.filter(pl.col("status").is_in(["fetched", "unchanged"])).height) if items.height and "status" in items.columns else 0
    failed = int(items.filter(pl.col("status") == "failed").height) if items.height and "status" in items.columns else 0
    parsed = int(documents.filter(pl.col("parse_status").cast(pl.String).str.to_lowercase().is_in(["ok", "parsed", "success"])).height) if documents.height and "parse_status" in documents.columns else 0
    validated = int(documents.filter(pl.col("validation_status").cast(pl.String).str.to_lowercase().is_in(["ok", "valid", "validated", "success"])).height) if documents.height and "validation_status" in documents.columns else parsed
    complete_windows = int(windows.filter(pl.col("is_complete") == True).height) if windows.height and "is_complete" in windows.columns else 0  # noqa: E712
    pagination_complete = complete_windows
    attachment_discovered = attachments.height
    attachment_processed = int(attachments.filter(pl.col("status").cast(pl.String).str.to_lowercase().is_in(["processed", "fetched", "complete", "success"])).height) if attachments.height and "status" in attachments.columns else 0
    missing_fields = 0
    field_cells = 0
    for field_name in ("title", "published_at", "issuing_agency", "document_number", "extracted_text"):
        if documents.height and field_name in documents.columns:
            field_cells += documents.height
            missing_fields += int(documents.select(pl.col(field_name).is_null() | (pl.col(field_name).cast(pl.String).str.strip_chars() == "")).to_series().sum())
    observed_dates: list[date] = []
    if documents.height and "published_at" in documents.columns:
        observed_dates = [parsed_date.date() for value in documents["published_at"].drop_nulls().to_list() if (parsed_date := _parse_datetime(value))]
    requested_start = config.backfill_from
    requested_end = config.backfill_to
    months_expected: list[str] = []
    if requested_start and requested_end:
        cursor = requested_start.replace(day=1)
        final = requested_end.replace(day=1)
        while cursor <= final:
            months_expected.append(cursor.strftime("%Y-%m"))
            cursor = date(cursor.year + (cursor.month == 12), 1 if cursor.month == 12 else cursor.month + 1, 1)
    months_observed = sorted({value.strftime("%Y-%m") for value in observed_dates})
    missing_months = sorted(set(months_expected) - set(months_observed))
    open_gaps = int(gaps.filter(pl.col("status") == "OPEN").height) if gaps.height and "status" in gaps.columns else 0
    critical_gaps = int(gaps.filter((pl.col("status") == "OPEN") & (pl.col("severity").cast(pl.String).str.to_lowercase() == "critical")).height) if gaps.height and {"status", "severity"} <= set(gaps.columns) else 0
    expected_month_windows = len(months_expected) * max(len(sources), 1)
    historical_ratio = complete_windows / expected_month_windows if expected_month_windows else (1.0 if not requested_start else 0.0)
    source_ratio = float(status.get("verified_slots", 0)) / float(status.get("total_slots", 0) or 1)
    article_ratio = terminal_links / int(items.height or 1)
    attachment_ratio = attachment_processed / attachment_discovered if attachment_discovered else 1.0
    field_ratio = 1.0 - (missing_fields / field_cells) if field_cells else 0.0
    freshness_ratio = float(status.get("freshness_ratio", 0.0))
    gap_ratio = 1.0 if open_gaps == 0 else 0.0
    overall = round(
        source_ratio * 0.25
        + historical_ratio * 0.30
        + article_ratio * 0.20
        + field_ratio * 0.15
        + freshness_ratio * 0.10,
        6,
    )
    status.update({"open_gaps": open_gaps, "critical_gaps": critical_gaps})
    report = {
        "generated_at": _now(),
        "scope": config.scope,
        "city_id": city_id,
        "requested_date_from": requested_start.isoformat() if requested_start else None,
        "requested_date_to": requested_end.isoformat() if requested_end else None,
        "status": status.get("global_status"),
        "database_sync_status": status,
        "slot_completeness": {
            key: status.get(key)
            for key in ("total_slots", "resolved_slots", "verified_slots", "enabled_slots", "crawl_ready_slots", "backfilled_slots", "current_slots", "human_review_slots", "unresolved_slots")
        },
        "source_completeness": {
            "expected_sources": len(slots),
            "discovered_sources": len(sources),
            "verified_sources": sum(bool(row.get("verified")) for row in sources),
            "enabled_sources": sum(bool(row.get("crawl_enabled")) for row in sources),
            "healthy_sources": sum(str(row.get("health_status")).lower() == "healthy" for row in sources),
            "stale_sources": sum(str(row.get("source_state")) == "STALE" for row in sources),
            "degraded_sources": sum(str(row.get("source_state")) in {"DEGRADED", "UNREACHABLE", "PARSER_BROKEN"} for row in sources),
        },
        "crawl_completeness": {
            "list_pages_expected": len(sources),
            "list_pages_processed": int(windows["page_count"].sum()) if windows.height and "page_count" in windows.columns else 0,
            "pagination_complete_sources": pagination_complete,
            "article_links_discovered": int(items.height),
            "article_links_terminal": terminal_links,
            "articles_fetched": fetched,
            "articles_parsed": parsed,
            "articles_validated": validated,
            "articles_inserted": int(documents.filter(pl.col("version_status").cast(pl.String) == "CURRENT").height) if documents.height and "version_status" in documents.columns else int(documents.height),
            "articles_updated": int(documents.filter(pl.col("version_status").cast(pl.String).is_in(["REVISED", "UPDATED"])).height) if documents.height and "version_status" in documents.columns else 0,
            "articles_unchanged": int(items.filter(pl.col("status") == "unchanged").height) if items.height and "status" in items.columns else 0,
            "articles_failed": failed,
            "attachments_discovered": attachment_discovered,
            "attachments_processed": attachment_processed,
            "attachments_failed": attachment_discovered - attachment_processed,
        },
        "time_coverage": {
            "oldest_document_date": min(observed_dates).isoformat() if observed_dates else None,
            "newest_document_date": max(observed_dates).isoformat() if observed_dates else None,
            "months_expected": months_expected,
            "months_observed": months_observed,
            "missing_months": missing_months,
            "days_since_last_success": None,
            "freshness_sla_status": "current" if freshness_ratio == 1.0 and sources else "unknown" if not sources else "stale_or_missing",
        },
        "data_quality": {
            "missing_title": int(documents["title"].is_null().sum()) if documents.height and "title" in documents.columns else 0,
            "missing_published_at": int(documents["published_at"].is_null().sum()) if documents.height and "published_at" in documents.columns else 0,
            "missing_issuing_agency": int(documents["issuing_agency"].is_null().sum()) if documents.height and "issuing_agency" in documents.columns else 0,
            "missing_document_number": int(documents["document_number"].is_null().sum()) if documents.height and "document_number" in documents.columns else 0,
            "empty_content": int(documents["extracted_text"].fill_null("").cast(pl.String).str.strip_chars().eq("").sum()) if documents.height and "extracted_text" in documents.columns else 0,
            "parse_failed": int(documents.filter(pl.col("parse_status").cast(pl.String).str.to_lowercase().is_in(["failed", "error", "parse_failed"])).height) if documents.height and "parse_status" in documents.columns else 0,
            "duplicate": int(documents.filter(pl.col("version_status").cast(pl.String) == "UNCHANGED").height) if documents.height and "version_status" in documents.columns else 0,
            "reprint": int(documents.filter(pl.col("version_status").cast(pl.String) == "REPRINT").height) if documents.height and "version_status" in documents.columns else 0,
            "revised": int(documents.filter(pl.col("version_status").cast(pl.String).is_in(["REVISED", "UPDATED"])).height) if documents.height and "version_status" in documents.columns else 0,
            "withdrawn": int(documents.filter(pl.col("version_status").cast(pl.String) == "WITHDRAWN").height) if documents.height and "version_status" in documents.columns else 0,
            "field_completeness_ratio": round(field_ratio, 6),
            "parse_success_ratio": round(parsed / int(documents.height or 1), 6),
            "article_terminal_ratio": round(article_ratio, 6),
        },
        "gaps": {
            "open_gaps": open_gaps,
            "critical_gaps": critical_gaps,
            "repairable_gaps": int(gaps.filter(pl.col("status") == "OPEN").height) if gaps.height and "status" in gaps.columns else 0,
            "human_review_gaps": sum(str(row.get("resolution_status") or "").lower() == "human_review" for row in gaps.to_dicts()) if gaps.height else 0,
            "accepted_limitations": missing_months,
        },
        "completeness": {
            "source_coverage_ratio": round(source_ratio, 6),
            "historical_coverage_ratio": round(historical_ratio, 6),
            "article_terminal_ratio": round(article_ratio, 6),
            "attachment_coverage_ratio": round(attachment_ratio, 6),
            "field_completeness_ratio": round(field_ratio, 6),
            "freshness_ratio": round(freshness_ratio, 6),
            "gap_resolution_ratio": round(gap_ratio, 6),
            "overall_completeness": overall,
            "overall_is_display_only": True,
        },
        "evidence": {
            "source_ids": sorted(source_ids),
            "run_count": int(runs.height),
            "window_count": int(windows.height),
            "document_count": int(documents.height),
        },
    }
    target = output_dir or config.output or settings.outputs / "full_sync" / f"report_{datetime.now(UTC):%Y%m%dT%H%M%SZ}"
    target.mkdir(parents=True, exist_ok=True)
    formats = {item.strip().lower() for item in str(config.report_formats).split(",") if item.strip()}
    paths: list[str] = []
    if "json" in formats:
        _atomic_json(target / "full_sync_report.json", report)
        _atomic_json(target / "completeness.json", report["completeness"])
        paths.extend([str(target / "full_sync_report.json"), str(target / "completeness.json")])
    if "parquet" in formats:
        report_frame = pl.DataFrame([report], infer_schema_length=None)
        storage_atomic_write_parquet(report_frame, target / "full_sync_report.parquet", {"module": "full_sync.report"})
        paths.append(str(target / "full_sync_report.parquet"))
    if "xlsx" in formats:
        summary_frame = pl.DataFrame([{
            "metric": key,
            "value": json.dumps(value, ensure_ascii=False, default=str) if isinstance(value, (dict, list)) else value,
        } for key, value in report["completeness"].items()])
        _write_excel(summary_frame, target / "full_sync_report.xlsx", ["metric", "value"])
        paths.append(str(target / "full_sync_report.xlsx"))
    if config.scope == "city" and city_id:
        city_prefix = "beijing" if any(str(row.get("city_name") or "") == "北京市" for row in slots) or city_id.endswith("110100") else city_id.lower()
        _atomic_json(target / f"{city_prefix}_one_year_completeness.json", report["completeness"])
        if "xlsx" in formats:
            _write_excel(summary_frame, target / f"{city_prefix}_one_year_completeness.xlsx", ["metric", "value"])
            _write_excel(pl.DataFrame(slots, infer_schema_length=None) if slots else pl.DataFrame(), target / f"{city_prefix}_source_coverage.xlsx", ["slot_id", "slot_state"])
            _write_excel(pl.DataFrame(documents.to_dicts(), infer_schema_length=None) if documents.height else pl.DataFrame(), target / f"{city_prefix}_document_inventory.xlsx", documents.columns or ["document_id"])
            _write_excel(pl.DataFrame([{"month": month, "document_count": months_observed.count(month)} for month in months_expected], infer_schema_length=None), target / f"{city_prefix}_monthly_counts.xlsx", ["month", "document_count"])
            _write_excel(pl.DataFrame(gaps.to_dicts(), infer_schema_length=None) if gaps.height else pl.DataFrame(), target / f"{city_prefix}_gap_report.xlsx", gaps.columns or ["gap_id"])
            _write_excel(pl.DataFrame(sources, infer_schema_length=None) if sources else pl.DataFrame(), target / f"{city_prefix}_source_health.xlsx", ["source_id", "source_state"])
        if "parquet" in formats:
            storage_atomic_write_parquet(documents, target / f"{city_prefix}_document_inventory.parquet", {"module": "full_sync.report"})
        report["city_output_prefix"] = city_prefix
        _atomic_json(target / f"{city_prefix}_api_usage.json", {
            "provider": "redacted",
            "budget_usage": json.loads((target / "budget_usage.json").read_text(encoding="utf-8")) if (target / "budget_usage.json").exists() else {},
            "secrets_redacted": True,
        })
        _atomic_json(target / f"{city_prefix}_http_usage.json", {
            "crawl_items": int(items.height),
            "terminal_items": terminal_links,
            "http_calls_observed": int(items.height),
            "network_route_policy": "direct_only_for_government_fetch",
        })
        _atomic_json(target / f"{city_prefix}_run_manifest.json", {
            "scope": config.scope,
            "city_id": city_id,
            "date_from": requested_start.isoformat() if requested_start else None,
            "date_to": requested_end.isoformat() if requested_end else None,
            "report_generated_at": report["generated_at"],
            "secrets_redacted": True,
        })
        (target / "resume_instructions.txt").write_text(
            f"python -m policydb.autopilot_cli full-sync resume --output '{target}' --resume\n",
            encoding="utf-8",
        )
        (target / f"{city_prefix.upper()}_ONE_YEAR_REPORT.md").write_text(
            "# 北京市近一年持续同步验收\n\n"
            f"- 日期范围：{requested_start} 至 {requested_end}\n"
            f"- 业务状态：{report['status']}\n"
            f"- 开放缺口：{open_gaps}\n"
            f"- 完整度（展示指标）：{overall}\n\n"
            "该综合完整度仅用于展示；CURRENT 仍由确定性来源、分页、事务和 freshness 规则决定。\n",
            encoding="utf-8",
        )
    return {"report": report, "output_dir": str(target), "paths": paths}


def audit_historical_runs(settings: Settings, output_dir: Path) -> dict[str, Any]:
    """Append diagnoses for known invalid historical runs without editing them."""

    root = settings.outputs / "full_sync"
    repairs: list[dict[str, Any]] = []
    invalid_dependency_run_ids: set[str] = set()
    if root.exists():
        for run_dir in sorted(path for path in root.iterdir() if path.is_dir()):
            summary_path = run_dir / "run_summary.json"
            if not summary_path.exists():
                continue
            try:
                summary = json.loads(summary_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            results = summary.get("source_results") or []
            for source_id in {str(row.get("source_id")) for row in results if row.get("source_id")}:
                source_results = [row for row in results if str(row.get("source_id")) == source_id]
                backfills = [row for row in source_results if row.get("mode") == "backfill"]
                incrementals = [row for row in source_results if row.get("mode") == "incremental"]
                if any(row.get("status") in {"failed_recoverable", "partial"} for row in backfills) and any(row.get("status") == "completed" for row in incrementals):
                    invalid_dependency_run_ids.add(run_dir.name)
                    repairs.append({
                        "repair_id": stable_id(run_dir.name, source_id, "dependency", prefix="HISTREPAIR"),
                        "run_id": run_dir.name,
                        "source_id": source_id,
                        "repair_type": "INVALID_DEPENDENCY_EXECUTION",
                        "source_stage_before": "backfill_failed_or_partial",
                        "dependent_stage": "incremental",
                        "watermark_status": "WATERMARK_SUSPECT",
                        "reason": "incremental ran although backfill was not SUCCESS",
                        "created_at": _now(),
                    })
                if any(row.get("mode") == "backfill" and row.get("status") == "completed" for row in source_results) and not _has_backfill_completion_evidence(settings, source_id, run_id=run_dir.name):
                    repairs.append({
                        "repair_id": stable_id(run_dir.name, source_id, "completion", prefix="HISTREPAIR"),
                        "run_id": run_dir.name,
                        "source_id": source_id,
                        "repair_type": "INVALID_COMPLETION_SUMMARY",
                        "source_stage_before": "completed_without_strict_evidence",
                        "dependent_stage": None,
                        "watermark_status": "UNCHANGED",
                        "reason": "fetched=0 or crawler return did not prove strict pagination completion",
                        "created_at": _now(),
                    })
    if repairs:
        _append_jsonl(output_dir / "historical_state_repairs.jsonl", repairs, unique_key="repair_id")
    provenance_rows: list[dict[str, Any]] = []
    items_path = settings.curated / "crawl_items.parquet"
    versions_path = settings.curated / "policy_document_versions.parquet"
    if invalid_dependency_run_ids and items_path.exists() and versions_path.exists():
        items = read_parquet_snapshot(items_path)
        versions = read_parquet_snapshot(versions_path)
        bad_items = items.filter(pl.col("run_id").cast(pl.String).is_in(sorted(invalid_dependency_run_ids))) if "run_id" in items.columns else pl.DataFrame()
        bad_ids = set(bad_items["item_id"].cast(pl.String).to_list()) if bad_items.height and "item_id" in bad_items.columns else set()
        if bad_ids and "crawl_item_id" in versions.columns:
            for row in versions.filter(pl.col("crawl_item_id").cast(pl.String).is_in(sorted(bad_ids))).to_dicts():
                provenance_rows.append({
                    "audit_id": stable_id(row.get("document_version_id"), "invalid_dependency", prefix="PROVWARN"),
                    "document_version_id": row.get("document_version_id"),
                    "source_id": row.get("source_id"),
                    "run_ids": sorted(invalid_dependency_run_ids),
                    "ingested_during_invalid_dependency_execution": True,
                    "content_retained": True,
                    "warning": "document retained; provenance requires review because incremental followed failed/partial backfill",
                    "created_at": _now(),
                })
    if provenance_rows:
        _append_jsonl(settings.curated / "document_provenance_audit.jsonl", provenance_rows, unique_key="audit_id")
    return {
        "historical_runs_scanned": len([path for path in root.iterdir() if path.is_dir()]) if root.exists() else 0,
        "repairs_appended": len(repairs),
        "invalid_dependency_run_ids": sorted(invalid_dependency_run_ids),
        "provenance_warnings_appended": len(provenance_rows),
    }


def _latest_successful_full_sync(settings: Settings) -> str | None:
    root = settings.outputs / "full_sync"
    if not root.exists():
        return None
    for run_dir in sorted((path for path in root.iterdir() if path.is_dir()), reverse=True):
        path = run_dir / "database_sync_status.json"
        if not path.exists():
            continue
        try:
            row = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        value = row.get("last_successful_full_sync")
        if value:
            return str(value)
    return None


def _open_gap_snapshot(settings: Settings, source_ids: set[str] | None = None) -> dict[str, int]:
    frame = _read_curated_table(settings, "coverage_gaps")
    if source_ids and frame.height and "source_id" in frame.columns:
        frame = frame.filter(pl.col("source_id").cast(pl.String).is_in(sorted(source_ids)))
    if frame.is_empty() or "status" not in frame.columns:
        return {"open": 0, "critical": 0}
    open_frame = frame.filter(pl.col("status") == "OPEN")
    critical = int(open_frame.filter(pl.col("severity").cast(pl.String).str.to_lowercase() == "critical").height) if "severity" in open_frame.columns else 0
    return {"open": int(open_frame.height), "critical": critical}


def _attempt_log_count(path: Path) -> int:
    if not path.exists():
        return 0
    ids: set[str] = set()
    for line in path.read_text(encoding="utf-8-sig").splitlines():
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        attempt_id = row.get("attempt_id") or row.get("request_id")
        if attempt_id:
            ids.add(str(attempt_id))
    return len(ids)


def _consistency_check(
    settings: Settings,
    *,
    run_id: str,
    run_dir: Path | None = None,
    plan: Mapping[str, Any],
    source_results: Sequence[Mapping[str, Any]],
    gaps: Mapping[str, Any],
) -> dict[str, Any]:
    source_ids = {str(row.get("source_id")) for row in plan.get("sources", []) if row.get("source_id")}
    gap_snapshot = _open_gap_snapshot(settings, source_ids)
    checkpoint_count = 0
    checkpoint_path = settings.outputs / "full_sync" / run_id / "sync_checkpoints.jsonl"
    if checkpoint_path.exists():
        checkpoint_count = len([line for line in checkpoint_path.read_text(encoding="utf-8").splitlines() if line.strip()])
    errors: list[dict[str, Any]] = []
    audit_dir = run_dir or (settings.outputs / "full_sync" / run_id)
    ledger_path = audit_dir / "budget_usage.json"
    try:
        ledger_payload = json.loads(ledger_path.read_text(encoding="utf-8-sig")) if ledger_path.exists() else {"used": {}}
    except (OSError, json.JSONDecodeError):
        ledger_payload = {"used": {}}
        errors.append({"code": "budget_ledger_unreadable", "path": str(ledger_path)})
    http_used = int(float((ledger_payload.get("used") or {}).get("http_calls", 0) or 0))
    http_logged = _attempt_log_count(audit_dir / "outbound_http_attempts.jsonl")
    if http_used != http_logged:
        errors.append({"code": "BUDGET_LEDGER_INCONSISTENT", "kind": "http_calls", "ledger_used": http_used, "attempt_log_unique": http_logged})
    if int(gaps.get("open_gaps", gap_snapshot["open"]) or 0) != gap_snapshot["open"]:
        errors.append({"code": "open_gap_count_mismatch", "reported": gaps.get("open_gaps"), "observed": gap_snapshot["open"]})
    if len(plan.get("slot_rows", [])) != int(plan.get("database_sync_status", {}).get("total_slots", 0)):
        errors.append({"code": "slot_count_mismatch"})
    if len(plan.get("sources", [])) != int(plan.get("estimates", {}).get("sources", len(plan.get("sources", [])))) and int(plan.get("estimates", {}).get("sources", 0)) > len(plan.get("sources", [])):
        errors.append({"code": "source_count_mismatch"})
    failed_or_partial = sum(str(row.get("status")) in {"failed", "failed_recoverable", "blocked_conflict", "human_review"} for row in source_results)
    required_backfill_failures = sum(
        str(row.get("mode")) == "backfill" and str(row.get("status")) not in {"completed", "complete_with_gaps"}
        for row in source_results
    )
    return {
        "run_id": run_id,
        "slot_count": len(plan.get("slot_rows", [])),
        "source_count": len(plan.get("sources", [])),
        "source_result_count": len(source_results),
        "failed_or_partial_count": failed_or_partial,
        "required_backfill_failures": required_backfill_failures,
        "open_gaps": gap_snapshot["open"],
        "critical_gaps": gap_snapshot["critical"],
        "checkpoint_count": checkpoint_count,
        "http_calls_ledger_used": http_used,
        "http_attempt_log_unique": http_logged,
        "consistency_errors": errors,
        "passed": not errors,
    }


def build_sync_plan(settings: Settings, config: FullSyncConfig) -> dict[str, Any]:
    config.validate(command="plan")
    queue = _scope_filter(build_slot_work_queue(settings), config)
    sync_state = _load_sync_state(settings)
    sources = _source_records(settings, config, sync_state)
    slot_rows: list[dict[str, Any]] = []
    source_by_city_role: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for source in sources:
        city_key = str(source.get("city_id"))
        for role_key in (source.get("source_role"), source.get("agency_type")):
            if role_key:
                source_by_city_role.setdefault((city_key, str(role_key)), []).append(source)
    for row in queue.to_dicts():
        source_candidates = source_by_city_role.get((str(row.get("city_id")), str(row.get("source_role"))), [])
        source = min(source_candidates, key=_source_selection_priority) if source_candidates else None
        source_state = source.get("source_state") if source else None
        slot_row = {
            **row,
            "source_id": source.get("source_id") if source else None,
            "backfill_status": source.get("backfill_status") if source else None,
            "next_retry_at": source.get("next_retry_at") if source else None,
            "source_status": source.get("source_status") if source else None,
            "crawl_enabled": source.get("crawl_enabled") if source else row.get("crawl_enabled"),
            "source_state": source_state,
        }
        slot_row["slot_state"] = classify_slot_state(slot_row, source_state)
        slot_rows.append(slot_row)
    active_lease_ids: set[str] = set()
    lease_path = settings.jobs / "full_sync_leases.json"
    if lease_path.exists():
        try:
            lease_rows = json.loads(lease_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            lease_rows = {}
        if isinstance(lease_rows, Mapping):
            now = datetime.now(UTC)
            for lease in lease_rows.values():
                if not isinstance(lease, Mapping) or str(lease.get("status") or "") != "CLAIMED":
                    continue
                expires_at = _parse_datetime(lease.get("expires_at"))
                if expires_at is None or expires_at > now:
                    for key in ("resource_id", "slot_id", "source_id"):
                        if lease.get(key):
                            active_lease_ids.add(str(lease[key]))

    def discovery_eligible(row: Mapping[str, Any]) -> bool:
        state = str(row.get("slot_state") or "").upper()
        source_state = str(row.get("source_state") or "").upper()
        if state in {"VERIFIED", "ENABLED", "CRAWL_READY", "BACKFILLED", "CURRENT", "CURRENT_WITH_WARNINGS", "HUMAN_REVIEW"}:
            return False
        if source_state in {"VERIFIED", "ENABLED", "CRAWL_READY", "BACKFILL_RUNNING", "BACKFILL_COMPLETE", "INCREMENTAL_HEALTHY"}:
            return False
        if str(row.get("slot_id") or "") in active_lease_ids or str(row.get("source_id") or "") in active_lease_ids:
            return False
        if any(bool(row.get(key)) for key in ("slots_verified", "is_verified", "verified", "source_verified", "crawl_enabled", "enabled")):
            return False
        if int(row.get("verified_candidate_count") or 0) > 0 or int(row.get("enabled_source_count") or 0) > 0:
            return False
        manual_status = str(row.get("manual_review_status") or row.get("status") or "").lower()
        if manual_status in {"human_review", "pending_human_review", "approved", "rejected"}:
            return False
        if _retry_wait_active(row):
            return False
        return state in {"UNRESOLVED", "DISCOVERING", "CANDIDATES_FOUND"}

    def audit_eligible(row: Mapping[str, Any]) -> bool:
        """Select scoped existing sources for evidence audit only.

        This queue is separate from ``source_discovery_queue``.  It may read
        and search an already verified candidate during a controlled city
        acceptance, but it never claims the normal discovery planner or
        writes a new candidate by itself.
        """
        if not config.all_five_source_roles:
            return False
        if str(row.get("source_role") or "") not in REQUIRED_ROLES:
            return False
        state = str(row.get("slot_state") or "").upper()
        if state in {"HUMAN_REVIEW", "RETRY_WAIT", "BLOCKED"}:
            return False
        if str(row.get("slot_id") or "") in active_lease_ids or str(row.get("source_id") or "") in active_lease_ids:
            return False
        return True

    discovery_queue = [row for row in slot_rows if config.discover_missing and str(config.discovery_mode).upper() != "DISABLED" and discovery_eligible(row)]
    source_audit_queue = [{**row, "audit_only": True, "audit_reason": "existing_source_audit"} for row in slot_rows if config.discover_missing and str(config.discovery_mode).upper() != "DISABLED" and audit_eligible(row)]
    verification_queue = [row for row in slot_rows if row["slot_state"] == "CANDIDATES_FOUND"]
    if config.all_five_source_roles:
        verification_queue = [row for row in slot_rows if row["slot_state"] in {"CANDIDATES_FOUND", "VERIFIED", "ENABLED", "CRAWL_READY"}]
    backfill_queue = [row for row in sources if row.get("source_state") == "CRAWL_READY" or str(row.get("backfill_status") or "").lower() not in {"complete", "backfilled", "backfill_complete"} and source_is_crawl_ready(row)]
    incremental_queue = [row for row in sources if row.get("source_state") in {"BACKFILL_COMPLETE", "INCREMENTAL_HEALTHY", "STALE"}]
    gap_queue = [row for row in sources if row.get("source_state") in {"STALE", "DEGRADED", "PARSER_BROKEN"}]
    doc_queue = [{"source_id": row.get("source_id"), "queue": "document_fetch_queue", "max_documents": config.max_documents} for row in sources if source_is_crawl_ready(row)]
    attachment_queue = [{"source_id": row.get("source_id"), "queue": "attachment_fetch_queue"} for row in sources if source_is_crawl_ready(row)]
    open_gap_count = 0
    gap_path = settings.curated / "coverage_gaps.parquet"
    if gap_path.exists():
        open_gap_count = int(read_parquet_snapshot(gap_path).filter(pl.col("status") == "OPEN").height)
    sync_status = database_sync_status(settings, slot_rows, sources, open_gaps=open_gap_count)
    estimates = {
        "slots": min(len(slot_rows), config.max_slots),
        "sources": min(len(sources), config.max_sources),
        "pages": min(len(sources), config.max_sources) * config.max_documents,
        "documents": min(len(sources), config.max_sources) * config.max_documents,
        "ai_calls": min(len(source_audit_queue if config.all_five_source_roles else discovery_queue), config.max_ai_calls) if config.discover_missing and str(config.discovery_mode).upper() in {"AUTO", "AI_ONLY", "SEARCH_AND_AI"} else 0,
        "search_calls": min(len(source_audit_queue if config.all_five_source_roles else discovery_queue), config.max_search_calls) if config.discover_missing and str(config.discovery_mode).upper() in {"AUTO", "SEARCH_ONLY", "SEARCH_AND_AI"} else 0,
        "http_calls": 0,
    }
    return {"queue": queue, "slot_rows": slot_rows, "sources": sources, "source_discovery_queue": discovery_queue, "source_audit_queue": source_audit_queue, "source_verification_queue": verification_queue, "historical_backfill_queue": backfill_queue, "incremental_refresh_queue": incremental_queue, "gap_repair_queue": gap_queue, "document_fetch_queue": doc_queue, "attachment_fetch_queue": attachment_queue, "database_sync_status": sync_status, "estimates": estimates}


class FullSyncController:
    """Run bounded continuous-sync stages and write resumable evidence."""

    def __init__(self, settings: Settings | None = None, *, config: FullSyncConfig | None = None, output: Path | None = None, run_id: str | None = None):
        self.settings = settings or Settings.discover()
        self.config = config or FullSyncConfig()
        self.run_id = run_id or self.config.run_id or datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        self.run_dir = output or self.config.output or self.settings.outputs / "full_sync" / self.run_id
        self.store = SyncStateStore(self.run_dir, run_id=self.run_id)
        self.ledger = BudgetLedger(self.run_dir / "budget_usage.json", limits={"ai_calls": self.config.max_ai_calls, "search_calls": self.config.max_search_calls, "http_calls": self.config.max_http_calls, "tokens": self.config.budget_tokens, "usd": self.config.budget_usd})
        self.leases = JobLeaseStore(self.settings.jobs / "full_sync_leases.json", run_id=self.run_id)
        self._source_deadline: float | None = None

    def stop_requested(self) -> bool:
        """Return whether this run must yield at its next safe checkpoint."""
        return any(
            path.exists()
            for path in (
                self.run_dir / "STOP_AUTOPILOT",
                self.settings.data_root / "control" / "STOP_FULL_SYNC",
            )
        )

    def _source_cancel_check(self) -> bool:
        if self.stop_requested():
            return True
        return self._source_deadline is not None and time.monotonic() >= self._source_deadline

    @classmethod
    def latest_run_dir(cls, settings: Settings) -> Path | None:
        root = settings.outputs / "full_sync"
        dirs = sorted((path for path in root.iterdir() if path.is_dir()), key=lambda path: path.name) if root.exists() else []
        return dirs[-1] if dirs else None

    def _write_queues(self, plan: Mapping[str, Any]) -> None:
        for name in ("source_discovery_queue", "source_audit_queue", "source_verification_queue", "historical_backfill_queue", "incremental_refresh_queue", "gap_repair_queue", "document_fetch_queue", "attachment_fetch_queue"):
            rows = list(plan[name])
            frame = pl.DataFrame(rows, infer_schema_length=None) if rows else _empty_frame(["queue"])
            _atomic_parquet(self.run_dir / f"{name}.parquet", frame)
            _atomic_json(self.run_dir / f"{name}.json", {"rows": rows})

    def _write_plan_artifacts(self, plan: Mapping[str, Any]) -> dict[str, Any]:
        self._write_queues(plan)
        slot_frame = pl.DataFrame(plan["slot_rows"], infer_schema_length=None) if plan["slot_rows"] else _empty_frame(["slot_id", "slot_state"])
        _atomic_parquet(self.run_dir / "coverage_snapshot.parquet", slot_frame)
        _atomic_json(self.run_dir / "coverage_snapshot.json", {"slots": plan["slot_rows"], "status": plan["database_sync_status"]})
        _atomic_json(self.run_dir / "database_sync_status.json", plan["database_sync_status"])
        _atomic_json(self.run_dir / "provider_health.json", {"llm": "not_called", "search": "not_called", "direct_fetch": "not_called", "secrets_redacted": True})
        _atomic_json(self.run_dir / "source_health.json", {"sources": plan["sources"]})
        _atomic_json(self.run_dir / "full_sync_manifest.json", {"run_id": self.run_id, "created_at": _now(), "config": {key: str(value) if isinstance(value, Path) else value for key, value in asdict(self.config).items()}, "plan_only": True, "paid_api_calls_started": 0})
        _atomic_json(self.run_dir / "run_summary.json", {"run_id": self.run_id, "status": "PLANNED", "plan_only": True, "estimates": plan["estimates"], "global_status": plan["database_sync_status"]["global_status"]})
        _atomic_json(self.run_dir / "failure_summary.json", {"failures": [], "errors": []})
        _atomic_json(self.run_dir / "gap_summary.json", {"open_gaps": plan["database_sync_status"].get("open_gaps", 0)})
        _atomic_json(self.run_dir / "document_change_summary.json", {"inserted": 0, "updated": 0, "unchanged": 0, "withdrawn": 0})
        for name, rows in (("coverage_by_city", plan["slot_rows"]), ("coverage_by_role", plan["slot_rows"]), ("source_health", plan["sources"]), ("open_gaps", [])):
            frame = pl.DataFrame(rows, infer_schema_length=None) if rows else _empty_frame(["status"])
            _write_excel(frame, self.run_dir / f"{name}.xlsx", frame.columns or ["status"])
        _atomic_write_bytes(self.run_dir / "resume_instructions.txt", (f"python -m policydb.autopilot_cli full-sync resume --output '{self.run_dir}' --resume\n").encode())
        self.store.write_status({"global_status": plan["database_sync_status"]["global_status"], "status": plan["database_sync_status"]["global_status"], "planned_slots": plan["estimates"]["slots"], "planned_sources": plan["estimates"]["sources"], "current_batch": {"run_id": self.run_id, "human_review": plan["database_sync_status"]["human_review_slots"]}, "current_slot": None, "current_step": "plan", "ai_calls": 0, "ai_attempts": 0, "search_calls": 0, "http_calls": 0, "candidates": 0, "probes": 0, "human_review": plan["database_sync_status"]["human_review_slots"], "retries": plan["database_sync_status"]["retry_wait_slots"], "verified": plan["database_sync_status"]["verified_slots"], "enabled": plan["database_sync_status"]["enabled_slots"], "unresolved": plan["database_sync_status"]["unresolved_slots"], "latest_error": None, "full_tests_status": load_test_evidence(self.settings).get("test_result", "unknown")})
        return {"run_id": self.run_id, "run_dir": str(self.run_dir), "status": "PLANNED", "plan_only": True, "estimates": plan["estimates"], "global_status": plan["database_sync_status"]["global_status"], "paid_api_calls_started": 0}

    def plan(self) -> dict[str, Any]:
        self.config.validate(command="plan")
        plan = build_sync_plan(self.settings, self.config)
        return self._write_plan_artifacts(plan)

    def _selected_sources(self, plan: Mapping[str, Any]) -> list[dict[str, Any]]:
        sources = [dict(row) for row in plan["sources"] if source_is_crawl_ready(row)]
        completed = self.store.completed_resources("SOURCE_COMPLETED") if self.config.resume else set()
        sources = [row for row in sources if str(row.get("source_id")) not in completed]
        # A slot may have a stale enabled source and a newer strict source.  A
        # bounded run must crawl one deterministic source per city/role; an old
        # RETRY_WAIT source must not block its healthy replacement.
        grouped: dict[tuple[str, str], dict[str, Any]] = {}
        for row in sources:
            city_id = str(row.get("city_id") or (row.get("city_ids") or [""])[0])
            role = str(row.get("source_role") or row.get("agency_type") or "")
            key = (city_id, role)
            current = grouped.get(key)
            role_identity = _candidate_role_identity_score(row)
            if role_identity == 2:
                # A source whose live hostname contradicts its required role
                # must not be used merely because it is enabled.  It remains
                # in the registry and audit history for later human review.
                continue
            priority = _source_selection_priority(row)
            if current is None or priority < current[0]:
                grouped[key] = (priority, row)
        selected = [value[1] for value in grouped.values()]
        return sorted(selected, key=lambda item: (str(item.get("city_id") or ""), str(item.get("source_role") or ""), str(item.get("source_id") or "")))[: self.config.max_sources]

    def _run_discovery_v2(self, plan: Mapping[str, Any]) -> dict[str, Any]:
        from policydb.source_completion_ai_workflow import run_ai_batch

        if not self.config.discover_missing:
            return {"planned": 0, "ai_calls": 0, "search_calls": 0, "candidate_proposals": 0, "applied_candidates": 0, "status": "not_requested", "discovery_mode": "DISABLED"}

        audit_existing = bool(self.config.all_five_source_roles)
        queue_name = "source_audit_queue" if audit_existing else "source_discovery_queue"
        selected = [dict(row) for row in plan[queue_name][: self.config.max_slots]]
        completed_key = "SOURCE_AUDIT_COMPLETED" if audit_existing else "DISCOVERY_COMPLETED"
        completed_slots = self.store.completed_resources(completed_key) if self.config.resume else set()
        selected = [row for row in selected if str(row.get("slot_id")) not in completed_slots]
        if not selected:
            return {"planned": 0, "ai_calls": 0, "search_calls": 0, "candidate_proposals": 0, "applied_candidates": 0, "status": "completed", "discovery_mode": str(self.config.discovery_mode).upper(), "audit_existing": audit_existing}

        attempt_state: dict[str, dict[str, Any]] = {}
        ai_attempt_path = self.run_dir / "ai_attempts.jsonl"
        search_attempt_path = self.run_dir / "search_attempts.jsonl"

        def ai_callback(event: dict[str, Any]) -> str | None:
            event = {**event, "run_id": self.run_id, "slot_id": event.get("slot_id") or current_slot[0], "provider": event.get("provider") or "siliconflow", "model": event.get("model") or self.settings.siliconflow_chat_model or self.settings.glm_model, "prompt_version": event.get("prompt_version") or "source-completion-v1"}
            if event.get("phase") == "before":
                token_limit = self.config.budget_tokens
                if token_limit is not None and float(self.ledger.used.get("tokens", 0)) >= float(token_limit):
                    raise BudgetExceeded("tokens budget exhausted")
                attempt_id = stable_id(self.run_id, event.get("slot_id") or "", event.get("request_id") or "", str(event.get("attempt") or 1), str(time.time_ns()), prefix="AIATTEMPT")
                counted = self.ledger.consume_actual("ai", {"attempt_id": attempt_id, "run_id": self.run_id, "slot_id": event.get("slot_id"), "stage": "source_discovery", "provider": event.get("provider"), "model": event.get("model"), "prompt_version": event.get("prompt_version"), "request_id": event.get("request_id"), "attempt": event.get("attempt"), "status": "started", "started_at": _now()})
                row = {**counted, "request_id": event.get("request_id") or attempt_id, "attempt_id": attempt_id}
                attempt_state[attempt_id] = row
                _append_jsonl(ai_attempt_path, [row])
                self.store.write_status({"current_slot": event.get("slot_id"), "current_step": "ai_request_started", "ai_calls": int(self.ledger.used.get("ai_calls", 0)), "ai_attempts": int(self.ledger.used.get("ai_calls", 0))})
                return attempt_id
            attempt_id = str(event.get("attempt_id") or "")
            if attempt_id and attempt_id in attempt_state:
                prompt_tokens = event.get("prompt_tokens")
                completion_tokens = event.get("completion_tokens")
                if prompt_tokens is not None and completion_tokens is not None and not attempt_state[attempt_id].get("usage_recorded"):
                    self.ledger.record_usage("tokens", int(prompt_tokens) + int(completion_tokens))
                    attempt_state[attempt_id]["usage_recorded"] = True
                if event.get("estimated_cost_usd") is not None and not attempt_state[attempt_id].get("cost_recorded"):
                    self.ledger.record_usage("usd", float(event["estimated_cost_usd"]))
                    attempt_state[attempt_id]["cost_recorded"] = True
                _append_jsonl(ai_attempt_path, [{**attempt_state[attempt_id], "status": event.get("status") or "completed", "phase": "completed", "prompt_tokens": event.get("prompt_tokens"), "completion_tokens": event.get("completion_tokens"), "error_type": event.get("error_type"), "completed_at": _now()}])
            return attempt_id or None

        def search_callback(event: dict[str, Any]) -> str | None:
            event = {**event, "run_id": self.run_id, "slot_id": event.get("slot_id") or current_slot[0]}
            if event.get("phase") == "before":
                attempt_id = stable_id(self.run_id, event.get("slot_id") or "", str(event.get("query") or ""), str(time.time_ns()), prefix="SEARCHATTEMPT")
                counted = self.ledger.consume_actual("search", {"attempt_id": attempt_id, "run_id": self.run_id, "slot_id": event.get("slot_id"), "stage": "source_discovery", "provider": event.get("provider"), "query": event.get("query"), "status": "started", "started_at": _now()})
                row = {**counted, "request_id": attempt_id, "attempt_id": attempt_id}
                attempt_state[attempt_id] = row
                _append_jsonl(search_attempt_path, [row])
                self.store.write_status({"current_slot": event.get("slot_id"), "current_step": "search_started", "search_calls": int(self.ledger.used.get("search_calls", 0))})
                return attempt_id
            attempt_id = str(event.get("attempt_id") or "")
            if attempt_id and attempt_id in attempt_state:
                _append_jsonl(search_attempt_path, [{**attempt_state[attempt_id], "status": "completed" if event.get("status_code") is not None else "failed", "phase": "completed", "status_code": event.get("status_code"), "error_type": event.get("error_type"), "completed_at": _now()}])
            return attempt_id or None

        all_proposals: list[dict[str, Any]] = []
        applied_rows: list[dict[str, Any]] = []
        results: list[dict[str, Any]] = []
        current_slot = [None]
        start_ai = int(self.ledger.used.get("ai_calls", 0))
        start_search = int(self.ledger.used.get("search_calls", 0))
        paused = False
        for row in selected:
            slot_id = str(row.get("slot_id"))
            current_slot[0] = slot_id
            self.store.transition("slot_audit_claimed" if audit_existing else "slot_claimed", reason_code="existing_source_audit" if audit_existing else "full_sync_discovery", slot_id=slot_id, state="DISCOVERING")
            self.store.transition("ai_request_started", reason_code="bounded_source_discovery", slot_id=slot_id)
            self.store.transition("search_started", reason_code="bounded_source_discovery", slot_id=slot_id)
            remaining_ai = max(0, self.config.max_ai_calls - int(self.ledger.used.get("ai_calls", 0)))
            remaining_search = max(0, self.config.max_search_calls - int(self.ledger.used.get("search_calls", 0)))
            try:
                result = run_ai_batch(self.settings, output=self.run_dir / "source_completion" / slot_id, max_slots=1, max_ai_calls=remaining_ai, max_search_calls=remaining_search, concurrency=min(self.config.discovery_concurrency, 1), dry_run=False, apply=False, resume=self.config.resume, slot_id=slot_id, global_audit_root=self.settings.outputs / "autopilot", discovery_mode=self.config.discovery_mode, audit_existing=audit_existing, ai_call_callback=ai_callback, search_call_callback=search_callback)
                results.append(result)
                self.store.write_status({
                    "provider_status": result.get("provider_status", "configured"),
                    "api_balance_status": result.get("api_balance_status", "unknown"),
                    "usage_status": result.get("usage_status", "unavailable"),
                    "tokens": result.get("tokens"),
                    "estimated_cost_usd": result.get("cost"),
                })
                proposal_path = self.run_dir / "source_completion" / slot_id / "candidate_proposals.parquet"
                if proposal_path.exists():
                    proposals = read_parquet_snapshot(proposal_path).to_dicts()
                    all_proposals.extend(proposals)
                    ranked_all = sorted(proposals, key=lambda item: (0 if str(item.get("selection_status") or "") == "selected_top3" else 1, -float(item.get("ai_confidence") or 0.0), str(item.get("candidate_url") or "")))
                    ranked = []
                    for item in ranked_all:
                        candidate_url = canonicalize_url(str(item.get("candidate_url") or ""))
                        host = (urlsplit(candidate_url).hostname or "").lower().removeprefix("www.")
                        if candidate_url.startswith(("http://", "https://")) and (host == "gov.cn" or host.endswith(".gov.cn")) and is_reusable_source_entry(candidate_url) and not _looks_like_detail_page(candidate_url):
                            ranked.append(item)
                        if len(ranked) >= self.config.top_k:
                            break
                    slot_applied_before = len(applied_rows)
                    seen_urls: set[str] = set()
                    for item in ranked:
                        candidate_url = canonicalize_url(str(item.get("candidate_url") or ""))
                        host = (urlsplit(candidate_url).hostname or "").lower().removeprefix("www.")
                        if not candidate_url.startswith(("http://", "https://")) or not (host == "gov.cn" or host.endswith(".gov.cn")) or not is_reusable_source_entry(candidate_url) or _looks_like_detail_page(candidate_url) or candidate_url in seen_urls:
                            continue
                        seen_urls.add(candidate_url)
                        if not audit_existing or self.config.apply:
                            formal = {"candidate_id": stable_id(slot_id, candidate_url, "official_entry_candidate", prefix="SRCCAND"), "slot_id": slot_id, "city_id": row.get("city_id"), "source_role": row.get("source_role"), "candidate_url": candidate_url, "canonical_url": candidate_url, "discovery_method": "ai_assisted_search", "discovery_evidence_url": candidate_url, "discovery_evidence_text": f"AI proposed role={item.get('source_role')}; slot role enforced={row.get('source_role')}; {item.get('candidate_snippet') or ''}"[:2000], "official_domain_evidence": "search result evidence; deterministic verification pending", "city_match_evidence": None, "role_match_evidence": None, "is_verified": False, "is_enabled": False, "manual_review_status": "pending_probe", "generation_batch_id": self.run_id}
                            upsert_candidates([formal], self.settings)
                            applied_rows.append(formal)
                self.store.transition("search_completed", reason_code="search_evidence_persisted", slot_id=slot_id, ai_calls=result.get("ai_calls", 0), search_calls=result.get("search_calls", 0), candidates=result.get("candidate_proposals", 0))
                self.store.transition("ai_request_completed", reason_code="ai_response_audited", slot_id=slot_id, ai_calls=result.get("ai_calls", 0))
                self.store.transition("candidates_ranked", reason_code="deterministic_top3_selection", slot_id=slot_id, candidates=min(self.config.top_k, len(proposals) if proposal_path.exists() else 0))
                self.store.checkpoint(completed_key, resource_id=slot_id, candidate_proposals=result.get("candidate_proposals", 0), applied_candidates=len(applied_rows) - slot_applied_before if proposal_path.exists() else 0)
            except BudgetExceeded as exc:
                paused = True
                results.append({"slot_id": slot_id, "status": "paused_budget", "error_type": type(exc).__name__, "error_message": str(exc)[:500]})
                self.store.transition("budget_paused", reason_code="actual_discovery_budget_exhausted", slot_id=slot_id, state="PAUSED_BUDGET")
                break
            except Exception as exc:
                results.append({"slot_id": slot_id, "status": "failed", "error_type": type(exc).__name__, "error_message": str(exc)[:500]})
                self.store.transition("source_discovery_failed", reason_code=type(exc).__name__, slot_id=slot_id, state="FAILED_RECOVERABLE")

        if all_proposals:
            storage_atomic_write_parquet(pl.DataFrame(all_proposals, infer_schema_length=None), self.run_dir / "search_evidence.parquet", {"module": "full_sync.discovery", "run_id": self.run_id})
            storage_atomic_write_parquet(pl.DataFrame(all_proposals, infer_schema_length=None), self.run_dir / "candidate_proposals.parquet", {"module": "full_sync.discovery", "run_id": self.run_id})
        if applied_rows:
            storage_atomic_write_parquet(pl.DataFrame(applied_rows, infer_schema_length=None), self.run_dir / "applied_candidates.parquet", {"module": "full_sync.discovery", "run_id": self.run_id})
        actual_ai = int(self.ledger.used.get("ai_calls", 0)) - start_ai
        actual_search = int(self.ledger.used.get("search_calls", 0)) - start_search
        token_values = [item.get("tokens") for item in results if item.get("persisted_ai_calls") or item.get("ai_calls")]
        cost_values = [item.get("cost") for item in results if item.get("persisted_ai_calls") or item.get("ai_calls")]
        usage_available = bool(token_values) and all(value is not None for value in token_values)
        cost_available = bool(cost_values) and all(value is not None for value in cost_values)
        status = "paused_budget" if paused else "completed"
        self.store.write_status({"current_batch": {"run_id": self.run_id, "planned": len(selected), "completed": len(results), "human_review": len(applied_rows), "audit_existing": audit_existing}, "current_slot": None, "current_step": "source_audit_completed" if audit_existing else "discovery_completed", "ai_calls": int(self.ledger.used.get("ai_calls", 0)), "ai_attempts": int(self.ledger.used.get("ai_calls", 0)), "search_calls": int(self.ledger.used.get("search_calls", 0)), "candidates": len(all_proposals), "human_review": len(applied_rows), "latest_error": next((item.get("error_message") for item in results if item.get("error_message")), None)})
        return {"planned": len(selected), "ai_calls": actual_ai, "ai_attempts": actual_ai, "search_calls": actual_search, "candidate_proposals": len(all_proposals), "applied_candidates": len(applied_rows), "status": status, "discovery_mode": str(self.config.discovery_mode).upper(), "audit_existing": audit_existing, "tokens": sum(int(value) for value in token_values) if usage_available else None, "cost": sum(float(value) for value in cost_values) if cost_available else None, "usage_status": "available" if usage_available else "unavailable", "results": results, "provider_status": "operational" if any(item.get("persisted_ai_calls") for item in results) else "configured", "api_balance_status": "call_succeeded" if any(item.get("persisted_ai_calls") for item in results) else "unknown"}

    def _run_discovery(self, plan: Mapping[str, Any]) -> dict[str, Any]:
        return self._run_discovery_v2(plan)
        if not self.config.discover_missing:
            return {"planned": 0, "ai_calls": 0, "search_calls": 0, "candidate_proposals": 0, "applied_candidates": 0, "status": "not_requested"}
        if self.config.max_ai_calls == 0 or self.config.max_search_calls == 0:
            return {"planned": min(len(plan["source_discovery_queue"]), self.config.max_slots), "ai_calls": 0, "search_calls": 0, "candidate_proposals": 0, "applied_candidates": 0, "status": "budget_zero"}
        # The existing source-completion workflow is invoked only per bounded
        # slot and with apply=False.  Formal candidates are written by this
        # layer only after deterministic top-k selection.
        from policydb.source_completion_ai_workflow import run_ai_batch

        selected = plan["source_discovery_queue"][: self.config.max_slots]
        results = []
        all_proposals: list[dict[str, Any]] = []
        applied_rows: list[dict[str, Any]] = []
        for row in selected:
            slot_id = str(row.get("slot_id"))
            if self.config.resume and slot_id in self.store.completed_resources("DISCOVERY_COMPLETED"):
                continue
            self.store.transition("slot_claimed", reason_code="full_sync_discovery", slot_id=slot_id, state="DISCOVERING")
            self.store.transition("search_started", reason_code="bounded_source_discovery", slot_id=slot_id)
            try:
                self.ledger.reserve("ai_calls", 1)
                self.ledger.reserve("search_calls", 1)
                result = run_ai_batch(self.settings, output=self.run_dir / "source_completion" / slot_id, max_slots=1, max_ai_calls=1, concurrency=min(self.config.discovery_concurrency, 1), dry_run=False, apply=False, resume=self.config.resume, slot_id=slot_id, global_audit_root=self.settings.outputs / "autopilot")
                results.append(result)
                proposal_path = self.run_dir / "source_completion" / slot_id / "candidate_proposals.parquet"
                if proposal_path.exists():
                    proposals = read_parquet_snapshot(proposal_path).to_dicts()
                    all_proposals.extend(proposals)
                    ranked = sorted(
                        proposals,
                        key=lambda item: (
                            -float(item.get("ai_confidence") or 0.0),
                            str(item.get("candidate_url") or ""),
                        ),
                    )[: self.config.top_k]
                    for item in ranked:
                        candidate_url = str(item.get("candidate_url") or "")
                        formal = {
                            "candidate_id": stable_id(
                                slot_id,
                                canonicalize_url(candidate_url),
                                "official_entry_candidate",
                                prefix="SRCCAND",
                            ),
                            "slot_id": slot_id,
                            "city_id": item.get("city_id"),
                            "source_role": item.get("source_role"),
                            "candidate_url": candidate_url,
                            "canonical_url": canonicalize_url(candidate_url),
                            "discovery_method": "ai_assisted_search",
                            "discovery_evidence_url": candidate_url,
                            "discovery_evidence_text": item.get("candidate_snippet"),
                            "official_domain_evidence": "search result only; deterministic verification pending",
                            "city_match_evidence": None,
                            "role_match_evidence": None,
                            "is_verified": False,
                            "is_enabled": False,
                            "manual_review_status": "pending_probe",
                            "generation_batch_id": self.run_id,
                        }
                        upsert_candidates([formal], self.settings)
                        applied_rows.append(formal)
                self.store.transition("search_completed", reason_code="search_evidence_persisted", slot_id=slot_id, ai_calls=result.get("ai_calls", 0))
                self.store.checkpoint("DISCOVERY_COMPLETED", resource_id=slot_id, candidate_proposals=result.get("candidate_proposals", 0))
            except BudgetExceeded:
                return {"planned": len(selected), "ai_calls": int(self.ledger.used.get("ai_calls", 0)), "search_calls": 0, "status": "paused_budget", "results": results}
            except Exception as exc:
                results.append({"slot_id": slot_id, "status": "failed", "error_type": type(exc).__name__, "error_message": str(exc)[:500]})
        if all_proposals:
            storage_atomic_write_parquet(pl.DataFrame(all_proposals, infer_schema_length=None), self.run_dir / "search_evidence.parquet", {"module": "full_sync.discovery", "run_id": self.run_id})
            storage_atomic_write_parquet(pl.DataFrame(all_proposals, infer_schema_length=None), self.run_dir / "candidate_proposals.parquet", {"module": "full_sync.discovery", "run_id": self.run_id})
        if applied_rows:
            storage_atomic_write_parquet(pl.DataFrame(applied_rows, infer_schema_length=None), self.run_dir / "applied_candidates.parquet", {"module": "full_sync.discovery", "run_id": self.run_id})
        return {
            "planned": len(selected),
            "ai_calls": int(self.ledger.used.get("ai_calls", 0)),
            "search_calls": int(self.ledger.used.get("search_calls", 0)),
            "candidate_proposals": len(all_proposals),
            "applied_candidates": len(applied_rows),
            "status": "completed",
            "results": results,
        }

    def _run_pdf_for_source(self, source: Mapping[str, Any]) -> dict[str, Any]:
        """Run bounded PDF stages without changing HTML source admission."""
        if not self.config.pdf_enabled:
            return {"status": "DISABLED", "ocr_enabled": False}
        try:
            config = replace(
                load_pdf_config(self.settings),
                max_downloads_per_source=self.config.pdf_max_downloads_per_source,
                max_downloads_per_job=self.config.pdf_max_downloads_per_job,
            )
            pipeline = PDFPipeline(self.settings, config=config)
            source_id = str(source.get("source_id") or "")
            result: dict[str, Any] = {"status": "COMPLETED", "ocr_enabled": False}
            if self.config.pdf_discover:
                result["discover"] = pipeline.discover(limit=self.config.pdf_max_downloads_per_job, source_id=source_id, run_id=self.run_id)
            if self.config.pdf_download:
                result["download"] = pipeline.download(limit=self.config.pdf_max_downloads_per_job, source_id=source_id, run_id=self.run_id)
            if self.config.pdf_parse:
                result["parse"] = pipeline.parse(limit=self.config.pdf_max_downloads_per_job, source_id=source_id, run_id=self.run_id)
            result["summary"] = pipeline.summary()
            return result
        except Exception as exc:
            return {"status": "PDF_FAILED_NON_BLOCKING", "error_type": type(exc).__name__, "error": str(exc)[:500], "ocr_enabled": False}

    def _run_source_v2(self, source: Mapping[str, Any], *, mode: str, max_fetches: int) -> dict[str, Any]:
        source_id = str(source.get("source_id"))
        result: dict[str, Any] = {"source_id": source_id, "mode": mode, "status": "planned", "status_category": "PARTIAL", "stage_result": "NOT_RUN", "fetched": 0, "failed": 0, "article_failures": 0, "incremental_skipped_dependency": False}
        try:
            lease = self.leases.claim("source", source_id)
        except LeaseConflict as exc:
            self.store.transition("checkpoint_conflict", reason_code="live_source_lease", source_id=source_id, state="BLOCKED_CONFLICT")
            return {**result, "status": "blocked_conflict", "status_category": "FAILED_TERMINAL", "error_type": type(exc).__name__, "error_message": str(exc)}
        _append_jsonl(self.store.claims_path, [lease], unique_key="lease_key")
        self.store.transition("source_claimed", reason_code=f"{mode}_source_claim", source_id=source_id)
        self.store.transition("crawl_job_started", reason_code=mode, source_id=source_id, state="RUNNING")
        # The registry row is not the authoritative cross-stage checkpoint.
        # An incremental run can start immediately after historical backfill,
        # while the registry still contains the pre-backfill values.  Load the
        # persisted sync row first and carry its historical state forward so a
        # successful incremental update cannot erase backfill completion,
        # watermarks, or the completion timestamp.
        persisted_sync_state = _load_sync_state(self.settings).get(source_id, {})
        preserved_backfill_status = str(
            persisted_sync_state.get("backfill_status")
            or source.get("backfill_status")
            or "not_started"
        )
        preserved_backfill_completed_at = (
            persisted_sync_state.get("backfill_completed_at")
            or source.get("backfill_completed_at")
        )
        preserved_historical_watermark = (
            persisted_sync_state.get("historical_watermark")
            or source.get("historical_watermark")
        )
        preserved_incremental_watermark = (
            persisted_sync_state.get("incremental_watermark")
            or source.get("incremental_watermark")
        )
        preserved_current_watermark = (
            persisted_sync_state.get("current_watermark")
            or source.get("current_watermark")
        )
        _write_source_sync_state(
            self.settings,
            [
                _source_sync_row(
                    source,
                    state="BACKFILL_RUNNING" if mode == "backfill" else "INCREMENTAL_SYNCING",
                    backfill_status=preserved_backfill_status,
                    overrides={
                        "source_status": "RUNNING",
                        "backfill_completed_at": preserved_backfill_completed_at,
                        "historical_watermark": json.dumps(
                            _load_watermark(preserved_historical_watermark),
                            ensure_ascii=False,
                            sort_keys=True,
                        ),
                        "incremental_watermark": json.dumps(
                            _load_watermark(preserved_incremental_watermark),
                            ensure_ascii=False,
                            sort_keys=True,
                        ),
                        "current_watermark": json.dumps(
                            _load_watermark(preserved_current_watermark),
                            ensure_ascii=False,
                            sort_keys=True,
                        ),
                    },
                )
            ],
        )
        start = (self.config.backfill_from or date(2000, 1, 1)) if mode == "backfill" else date.today() - timedelta(days=self.config.lookback_days)
        end = self.config.backfill_to or date.today()
        recorder = HttpAttemptRecorder(self.ledger, self.run_dir / "outbound_http_attempts.jsonl", run_id=self.run_id, source_id=source_id, slot_id=str(source.get("slot_id") or "") or None, stage=mode)
        from policydb.crawl.fetcher import RespectfulFetcher

        fetcher = RespectfulFetcher(user_agent=self.settings.user_agent, timeout=self.settings.request_timeout, connect_timeout=self.settings.connect_timeout, retries=min(self.settings.max_retries, self.config.max_document_retries), rate_limit=self.settings.default_rate_limit, check_robots=self.settings.respect_robots, attempt_callback=recorder)
        pipeline = CrawlPipeline(self.settings, fetcher=fetcher)
        try:
            self._source_deadline = (
                time.monotonic() + (int(self.config.max_minutes_per_source) * 60)
                if self.config.max_minutes_per_source is not None
                else None
            )
            self.store.transition("probe_started", reason_code=f"{mode}_list_discovery", source_id=source_id)
            plan = pipeline.plan(run_type="historical_105" if mode == "backfill" else "official_update", start_date=start, end_date=end, official_first=True, source_ids=[source_id], max_candidates_total=max_fetches, max_candidates_per_source=max_fetches, max_pages_per_source=self.config.max_list_pages_per_source, global_safety_limit=max_fetches, resume=True)
            result.update({"crawl_plan": plan, "status": plan.get("status", "planned")})
            self.store.transition("page_fetch", reason_code=f"{mode}_planned", source_id=source_id)
            if plan.get("status") == "blocked_no_enabled_sources":
                result.update({"status": "skipped_dependency", "status_category": "SKIPPED_DEPENDENCY", "stage_result": "SKIPPED_DEPENDENCY", "reason_code": "no_enabled_source"})
                _write_source_sync_state(self.settings, [_source_sync_row(source, state="CRAWL_READY", backfill_status=preserved_backfill_status, error="no_enabled_source", overrides={"source_status": "SKIPPED_DEPENDENCY"})])
                return result
            if self.config.apply and not self.config.dry_run:
                self.store.transition("article_fetch", reason_code=f"{mode}_apply", source_id=source_id)
                fetched = _pipeline_run_with_retry(
                    pipeline,
                    plan["run_id"],
                    max_fetches=max_fetches,
                    cancel_check=self._source_cancel_check,
                    max_attachment_attempts=self.config.max_attachment_attempts,
                )
                result.update(fetched)
                result["article_failures"] = int(fetched.get("failed") or fetched.get("persisted_failed") or 0)
                if self.config.pdf_enabled:
                    result["pdf"] = self._run_pdf_for_source(source)
                if fetched.get("cancelled"):
                    reason_code = "stop_requested" if self.stop_requested() else "source_time_budget"
                    usable = int(fetched.get("fetched") or fetched.get("persisted_fetched") or 0) > 0
                    result.update(
                        {
                            "status": "partial",
                            "status_category": "PARTIAL_BUT_USABLE" if usable else "PARTIAL_EMPTY",
                            "stage_result": "PARTIAL_BUT_USABLE" if usable else "PARTIAL_EMPTY",
                            "reason_code": reason_code,
                        }
                    )
                    result["next_retry_at"] = (datetime.now(UTC) + timedelta(minutes=15)).isoformat()
                    self.store.transition("source_yielded", reason_code=reason_code, source_id=source_id, state=result["status_category"])
                    self.store.checkpoint("SOURCE_YIELDED", resource_id=source_id, status=result["status_category"], reason_code=reason_code)
                    _write_source_sync_state(
                        self.settings,
                        [_source_sync_row(source, state="CRAWL_READY", backfill_status="partial" if mode == "backfill" else preserved_backfill_status, error=reason_code, overrides={"source_status": result["status_category"], "next_retry_at": result["next_retry_at"]})],
                    )
                    return result
                if fetched.get("budget_paused"):
                    result.update({"status": "paused_budget", "status_category": "PAUSED_BUDGET", "stage_result": "PAUSED_BUDGET", "error_type": "HttpBudgetExceeded", "error_message": "actual HTTP budget exhausted"})
                    self.store.transition("budget_paused", reason_code="actual_http_budget_exhausted", source_id=source_id, state="PAUSED_BUDGET")
                    self.store.checkpoint("BACKFILL_PAUSED_BUDGET" if mode == "backfill" else "INCREMENTAL_PAUSED_BUDGET", resource_id=source_id, status="PAUSED_BUDGET")
                    _write_source_sync_state(self.settings, [_source_sync_row(source, state="CRAWL_READY", backfill_status="paused_budget" if mode == "backfill" else preserved_backfill_status, error="actual_http_budget_exhausted", overrides={"source_status": "PAUSED_BUDGET"})])
                    return result
                completion_evidence_reused = False
                completion_evidence = (
                    _has_backfill_completion_evidence(
                        self.settings,
                        source_id,
                        run_id=str(plan["run_id"]),
                        period_start=start,
                        period_end=end,
                    )
                    if mode == "backfill"
                    else True
                )
                # ``CrawlPipeline.plan(..., resume=True)`` deliberately
                # removes items for a source whose exact period already has
                # a strict complete window.  The resulting no-op crawl has
                # no new window row, so requiring evidence for the new crawl
                # run would turn an idempotent resume into RETRY_WAIT.  Reuse
                # the prior period-scoped evidence only for that explicit
                # zero-item resume case and retain the fact in the audit
                # stream; never treat an empty discovery as complete without
                # a prior strict window.
                if (
                    mode == "backfill"
                    and not completion_evidence
                    and int(plan.get("item_count") or 0) == 0
                ):
                    completion_evidence = _has_backfill_completion_evidence(
                        self.settings,
                        source_id,
                        period_start=start,
                        period_end=end,
                    )
                    completion_evidence_reused = completion_evidence
                    if completion_evidence_reused:
                        result["backfill_completion_reused"] = True
                        result["backfill_completion_reason"] = "reused_existing_strict_completion"
                        self.store.transition(
                            "backfill_reused_completion",
                            reason_code="existing_strict_period_window",
                            source_id=source_id,
                        )
                result["backfill_completion_evidence"] = completion_evidence
                pipeline_status = str(fetched.get("status") or "")
                has_failures = bool(result["article_failures"])
                if mode == "backfill" and completion_evidence and has_failures:
                    result.update({"status": "complete_with_gaps", "status_category": "COMPLETE_WITH_GAPS", "stage_result": "COMPLETE_WITH_GAPS", "backfill_completion_reason": "strict_pagination_with_article_failures"})
                elif pipeline_status == "complete" and completion_evidence and not has_failures:
                    result.update({"status": "completed", "status_category": "SUCCESS", "stage_result": "SUCCESS"})
                else:
                    result.update({"status": "partial", "status_category": "PARTIAL", "stage_result": "PARTIAL", "backfill_completion_reason": "no_complete_pagination_checkpoint" if mode == "backfill" and not completion_evidence else "article_or_pipeline_failures"})
                self.store.transition("probe_completed", reason_code=f"{mode}_list_discovery_finished", source_id=source_id)
                self.store.transition("parse_completed", reason_code=f"{mode}_parse_finished", source_id=source_id)
                self.store.transition("upsert_completed", reason_code=f"{mode}_documents_committed", source_id=source_id)
                self.store.checkpoint("ARTICLE_FETCH_COMPLETED", resource_id=source_id, fetched=fetched.get("fetched", 0), failed=result["article_failures"])
                self.store.checkpoint("UPSERT_COMPLETED", resource_id=source_id)
                if result["status"] in {"completed", "complete_with_gaps"} and (mode != "backfill" or completion_evidence):
                    previous_watermark = _load_watermark(
                        preserved_historical_watermark
                        if mode == "backfill"
                        else preserved_incremental_watermark or preserved_current_watermark
                    )
                    next_watermark = build_watermark(previous_watermark, documents=_read_documents(self.settings, source_id))
                    evidence_ids: list[str] = []
                    windows_path = self.settings.curated / "crawl_source_windows.parquet"
                    if windows_path.exists():
                        windows = read_parquet_snapshot(windows_path)
                        if "run_id" in windows.columns:
                            windows = windows.filter((pl.col("run_id").cast(pl.String) == str(plan["run_id"])) & (pl.col("source_id").cast(pl.String) == source_id))
                        if windows.height and "completion_evidence" in windows.columns:
                            for raw in windows["completion_evidence"].drop_nulls().to_list():
                                try:
                                    evidence_ids.extend(json.loads(str(raw)).get("termination_evidence_ids") or [])
                                except json.JSONDecodeError:
                                    pass
                    watermark_audit = _persist_watermark(self.settings, source=source, stage="historical" if mode == "backfill" else "incremental", previous=previous_watermark, proposed=next_watermark, evidence_ids=sorted(set(map(str, evidence_ids))), source_state_before=str(source.get("source_state") or "CRAWL_READY"), source_state_after="BACKFILL_COMPLETE" if mode == "backfill" else "INCREMENTAL_HEALTHY", run_id=self.run_id, job_id=str(plan["run_id"]))
                    _write_source_sync_state(self.settings, [_source_sync_row(source, state="BACKFILL_COMPLETE" if mode == "backfill" else "INCREMENTAL_HEALTHY", backfill_status="complete_with_gaps" if result["status"] == "complete_with_gaps" else "complete" if mode == "backfill" else preserved_backfill_status, overrides={"source_status": result["status"].upper(), "last_successful_crawl_at": _now(), "backfill_completed_at": _now() if mode == "backfill" else preserved_backfill_completed_at, "historical_watermark": json.dumps(next_watermark if mode == "backfill" else _load_watermark(preserved_historical_watermark), ensure_ascii=False, sort_keys=True), "incremental_watermark": json.dumps(next_watermark if mode == "incremental" else _load_watermark(preserved_incremental_watermark), ensure_ascii=False, sort_keys=True), "current_watermark": json.dumps(next_watermark, ensure_ascii=False, sort_keys=True), "watermark_audit_id": watermark_audit["transaction_id"], "consecutive_failures": 0, "freshness_status": "current"})])
                    self.store.transition("verify_completed", reason_code="strict_transaction_and_checkpoint_committed", source_id=source_id, state="BACKFILL_COMPLETE" if mode == "backfill" else "INCREMENTAL_HEALTHY")
                    self.store.checkpoint("BACKFILL_COMPLETED" if mode == "backfill" else "INCREMENTAL_COMPLETED", resource_id=source_id, status=result["status"], watermark_transaction_id=watermark_audit["transaction_id"])
                elif result["status"] in {"partial", "complete_with_gaps"}:
                    result["next_retry_at"] = (datetime.now(UTC) + timedelta(minutes=15)).isoformat()
            else:
                result.update({"status": "planned", "status_category": "PARTIAL", "stage_result": "PLANNED"})
            if result["status"] in {"partial", "retry_wait"}:
                failures = int(source.get("consecutive_failures") or 0) + 1
                delay = min(24 * 60, 15 * (2 ** max(0, failures - 1)))
                result["next_retry_at"] = (datetime.now(UTC) + timedelta(minutes=delay)).isoformat()
                if failures >= self.config.max_consecutive_failures:
                    result.update({"status": "human_review", "status_category": "HUMAN_REVIEW", "stage_result": "HUMAN_REVIEW", "reason_code": "max_consecutive_failures"})
                else:
                    result.update({"status": "retry_wait", "status_category": "RETRY_WAIT", "stage_result": "RETRY_WAIT"})
                _write_source_sync_state(self.settings, [_source_sync_row(source, state="DEGRADED" if mode != "backfill" else "CRAWL_READY", backfill_status="retry_wait" if mode == "backfill" else preserved_backfill_status, error=str(result.get("backfill_completion_reason") or "source_not_complete"), overrides={"source_status": result["status"].upper(), "consecutive_failures": failures, "next_retry_at": result["next_retry_at"]})])
            if result["status"] in {"completed", "complete_with_gaps"}:
                self.store.checkpoint("SOURCE_COMPLETED", resource_id=source_id, mode=mode, status=result["status"])
            return result
        except HttpBudgetExceeded as exc:
            result.update({"status": "paused_budget", "status_category": "PAUSED_BUDGET", "stage_result": "PAUSED_BUDGET", "error_type": type(exc).__name__, "error_message": str(exc)[:1000]})
            self.store.transition("budget_paused", reason_code="actual_http_budget_exhausted", source_id=source_id, state="PAUSED_BUDGET")
            return result
        except Exception as exc:
            failures = int(source.get("consecutive_failures") or 0) + 1
            delay = min(24 * 60, 15 * (2 ** max(0, failures - 1)))
            next_retry = (datetime.now(UTC) + timedelta(minutes=delay)).isoformat()
            tls_like = "tls" in type(exc).__name__.lower() or "ssl" in str(exc).lower() or "certificate" in str(exc).lower()
            status = "human_review" if failures >= self.config.max_consecutive_failures else "retry_wait"
            result.update({"status": status, "status_category": "HUMAN_REVIEW" if status == "human_review" else "RETRY_WAIT", "stage_result": "HUMAN_REVIEW" if status == "human_review" else "RETRY_WAIT", "error_type": type(exc).__name__, "error_message": str(exc)[:1000], "next_retry_at": next_retry, "tls_failure": tls_like})
            self.store.transition("crawl_job_failed", reason_code="tls_failure" if tls_like else type(exc).__name__, source_id=source_id, state="HUMAN_REVIEW" if status == "human_review" else "RETRY_WAIT")
            _write_source_sync_state(self.settings, [_source_sync_row(source, state="HUMAN_REVIEW" if status == "human_review" else "CRAWL_READY", backfill_status="human_review" if status == "human_review" and mode == "backfill" else "retry_wait" if mode == "backfill" else preserved_backfill_status, error=type(exc).__name__, overrides={"source_status": status.upper(), "consecutive_failures": failures, "next_retry_at": next_retry})])
            return result
        finally:
            self._source_deadline = None
            self.leases.release("source", source_id, status="COMPLETED" if result.get("status") in {"completed", "complete_with_gaps", "planned"} else "RETRY_WAIT" if result.get("status") in {"retry_wait", "partial"} else "FAILED")

    def _run_source(self, source: Mapping[str, Any], *, mode: str, max_fetches: int) -> dict[str, Any]:
        return self._run_source_v2(source, mode=mode, max_fetches=max_fetches)
        source_id = str(source.get("source_id"))
        try:
            lease = self.leases.claim("source", source_id)
        except LeaseConflict as exc:
            self.store.transition("checkpoint_conflict", reason_code="live_source_lease", source_id=source_id, state="BLOCKED_CONFLICT")
            return {"source_id": source_id, "mode": mode, "status": "blocked_conflict", "error_type": type(exc).__name__, "error_message": str(exc)}
        _append_jsonl(self.store.claims_path, [lease], unique_key="lease_key")
        self.store.transition("source_claimed", reason_code=f"{mode}_source_claim", source_id=source_id)
        self.store.transition("crawl_job_started", reason_code=mode, source_id=source_id, state="RUNNING")
        running_state = "BACKFILL_RUNNING" if mode == "backfill" else "INCREMENTAL_HEALTHY"
        _write_source_sync_state(
            self.settings,
            [_source_sync_row(source, state=running_state, backfill_status=source.get("backfill_status"))],
        )
        start = self.config.backfill_from or date(2000, 1, 1) if mode == "backfill" else date.today() - timedelta(days=self.config.lookback_days)
        end = self.config.backfill_to or date.today()
        result: dict[str, Any] = {
            "source_id": source_id,
            "mode": mode,
            "status": "planned",
            "stage_result": "NOT_RUN",
            "fetched": 0,
            "failed": 0,
            "incremental_skipped_dependency": False,
        }
        try:
            self.ledger.reserve("http", 1)
            fetch_limit = min(
                max_fetches,
                max(0, self.config.max_http_calls - int(self.ledger.used.get("http_calls", 0))),
            )
            if fetch_limit < 1:
                raise BudgetExceeded("http_calls budget exhausted before document fetch")
            pipeline = CrawlPipeline(self.settings)
            plan = pipeline.plan(run_type="historical_105" if mode == "backfill" else "official_update", start_date=start, end_date=end, official_first=True, source_ids=[source_id], max_candidates_total=max_fetches, max_candidates_per_source=max_fetches, max_pages_per_source=max(1, min(max_fetches, 20)), global_safety_limit=max_fetches, resume=True)
            result.update({"crawl_plan": plan, "status": plan.get("status", "planned")})
            self.store.transition("page_fetch", reason_code=f"{mode}_planned", source_id=source_id)
            if self.config.apply and not self.config.dry_run and plan.get("status") == "planned":
                self.store.transition("article_fetch", reason_code=f"{mode}_apply", source_id=source_id)
                self.ledger.reserve("http", fetch_limit)
                fetched = _pipeline_run_with_retry(pipeline, plan["run_id"], max_fetches=fetch_limit)
                result.update(fetched)
                pipeline_status = str(fetched.get("status") or "")
                has_failures = bool(int(fetched.get("failed") or 0))
                result["status"] = "completed" if pipeline_status == "complete" and not has_failures else "partial"
                result["stage_result"] = "SUCCESS" if result["status"] == "completed" else "PARTIAL"
                if mode == "backfill":
                    result["backfill_completion_evidence"] = _has_backfill_completion_evidence(
                        self.settings,
                        source_id,
                        run_id=str(plan["run_id"]),
                        period_start=start,
                        period_end=end,
                    )
                    if not result["backfill_completion_evidence"]:
                        result["status"] = "partial"
                        result["stage_result"] = "PARTIAL"
                        result["backfill_completion_reason"] = "no_complete_pagination_checkpoint"
                self.store.transition("parse_completed", reason_code=f"{mode}_parse_finished", source_id=source_id)
                self.store.transition("upsert_completed", reason_code=f"{mode}_documents_committed", source_id=source_id)
                self.store.checkpoint("ARTICLE_FETCH_COMPLETED", resource_id=source_id, fetched=fetched.get("fetched", 0), failed=fetched.get("failed", 0))
                self.store.checkpoint("UPSERT_COMPLETED", resource_id=source_id)
                if result["status"] == "completed" and (mode != "backfill" or result.get("backfill_completion_evidence")):
                    previous_watermark = _load_watermark(
                        source.get("historical_watermark")
                        if mode == "backfill"
                        else source.get("incremental_watermark") or source.get("current_watermark")
                    )
                    next_watermark = build_watermark(
                        previous_watermark,
                        documents=_read_documents(self.settings, source_id),
                    )
                    evidence_ids: list[str] = []
                    windows_path = self.settings.curated / "crawl_source_windows.parquet"
                    if windows_path.exists():
                        windows = read_parquet_snapshot(windows_path)
                        if "run_id" in windows.columns:
                            windows = windows.filter(
                                (pl.col("run_id").cast(pl.String) == str(plan["run_id"]))
                                & (pl.col("source_id").cast(pl.String) == source_id)
                            )
                        if windows.height and "completion_evidence" in windows.columns:
                            for raw in windows["completion_evidence"].drop_nulls().to_list():
                                try:
                                    evidence_ids.extend(json.loads(str(raw)).get("termination_evidence_ids") or [])
                                except json.JSONDecodeError:
                                    continue
                    watermark_audit = _persist_watermark(
                        self.settings,
                        source=source,
                        stage="historical" if mode == "backfill" else "incremental",
                        previous=previous_watermark,
                        proposed=next_watermark,
                        evidence_ids=sorted(set(map(str, evidence_ids))),
                        source_state_before=str(source.get("source_state") or "CRAWL_READY"),
                        source_state_after="BACKFILL_COMPLETE" if mode == "backfill" else "INCREMENTAL_HEALTHY",
                        run_id=self.run_id,
                        job_id=str(plan["run_id"]),
                    )
                    _write_source_sync_state(
                        self.settings,
                        [
                            _source_sync_row(
                                source,
                                state="BACKFILL_COMPLETE" if mode == "backfill" else "INCREMENTAL_HEALTHY",
                                backfill_status="complete" if mode == "backfill" else source.get("backfill_status"),
                                overrides={
                                    "last_successful_crawl_at": _now(),
                                    "last_incremental_sync_at": _now() if mode == "incremental" else source.get("last_incremental_sync_at"),
                                    "backfill_completed_at": _now() if mode == "backfill" else source.get("backfill_completed_at"),
                                    "historical_watermark": json.dumps(next_watermark if mode == "backfill" else _load_watermark(source.get("historical_watermark")), ensure_ascii=False, sort_keys=True),
                                    "incremental_watermark": json.dumps(next_watermark if mode == "incremental" else _load_watermark(source.get("incremental_watermark")), ensure_ascii=False, sort_keys=True),
                                    "current_watermark": json.dumps(next_watermark, ensure_ascii=False, sort_keys=True),
                                    "watermark_audit_id": watermark_audit["transaction_id"],
                                    "consecutive_failures": 0,
                                    "freshness_status": "current",
                                },
                            )
                        ],
                    )
                    self.store.transition(
                        "backfill_completed" if mode == "backfill" else "incremental_completed",
                        reason_code="strict_transaction_and_checkpoint_committed",
                        source_id=source_id,
                        state="BACKFILL_COMPLETE" if mode == "backfill" else "INCREMENTAL_HEALTHY",
                    )
                    self.store.checkpoint(
                        "BACKFILL_COMPLETED" if mode == "backfill" else "INCREMENTAL_COMPLETED",
                        resource_id=source_id,
                        status="SUCCESS",
                        watermark_transaction_id=watermark_audit["transaction_id"],
                    )
                elif mode == "backfill":
                    result["next_retry_at"] = (datetime.now(UTC) + timedelta(minutes=15)).isoformat()
                    result["backfill_status"] = "partial"
                    result["incremental_skipped_dependency"] = True
                    self.store.transition(
                        "backfill_partial",
                        reason_code=str(result.get("backfill_completion_reason") or "backfill_not_success"),
                        source_id=source_id,
                        state="CRAWL_READY",
                    )
                    self.store.checkpoint(
                        "BACKFILL_PARTIAL",
                        resource_id=source_id,
                        status="PARTIAL",
                        reason_code=str(result.get("backfill_completion_reason") or "backfill_not_success"),
                    )
                    _write_source_sync_state(
                        self.settings,
                        [
                            _source_sync_row(
                                source,
                                state="CRAWL_READY",
                                backfill_status="partial",
                                error=str(result.get("backfill_completion_reason") or "backfill_not_success"),
                                overrides={"next_retry_at": result["next_retry_at"]},
                            )
                        ],
                    )
                elif result["status"] != "completed":
                    _write_source_sync_state(
                        self.settings,
                        [
                            _source_sync_row(
                                source,
                                state="DEGRADED",
                                backfill_status=source.get("backfill_status"),
                                error="incremental_stage_not_success",
                                overrides={"next_retry_at": (datetime.now(UTC) + timedelta(minutes=15)).isoformat()},
                            )
                        ],
                    )
            else:
                result["stage_result"] = "PLANNED"
                self.store.checkpoint("BACKFILL_PAGE_COMPLETED" if mode == "backfill" else "INCREMENTAL_COMPLETED", resource_id=source_id, status="planned")
            if result["status"] == "completed" and result["stage_result"] == "SUCCESS":
                self.store.checkpoint("SOURCE_COMPLETED", resource_id=source_id, mode=mode, status=result["status"])
            else:
                self.store.checkpoint("BACKFILL_PAGE_COMPLETED" if mode == "backfill" else "INCREMENTAL_COMPLETED", resource_id=source_id, status=result["status"])
            return result
        except BudgetExceeded as exc:
            result.update({"status": "paused_budget", "error_type": type(exc).__name__, "error_message": str(exc)[:1000]})
            self.store.transition("budget_paused", reason_code="budget_exhausted", source_id=source_id, state="PAUSED_BUDGET")
            return result
        except Exception as exc:
            result.update({"status": "failed_recoverable", "error_type": type(exc).__name__, "error_message": str(exc)[:1000]})
            self.store.transition("crawl_job_failed", reason_code=type(exc).__name__, source_id=source_id, state="FAILED_RECOVERABLE")
            if mode == "backfill":
                result["incremental_skipped_dependency"] = True
                result["stage_result"] = "FAILED_RECOVERABLE"
                result["next_retry_at"] = (datetime.now(UTC) + timedelta(minutes=15)).isoformat()
                self.store.transition("backfill_failed", reason_code=type(exc).__name__, source_id=source_id, state="CRAWL_READY")
                self.store.checkpoint("BACKFILL_FAILED_RECOVERABLE", resource_id=source_id, reason_code=type(exc).__name__)
            _write_source_sync_state(
                self.settings,
                [
                    _source_sync_row(
                        source,
                        state="CRAWL_READY" if mode == "backfill" else "DEGRADED",
                        backfill_status="failed_recoverable" if mode == "backfill" else source.get("backfill_status"),
                        error=type(exc).__name__,
                        overrides={
                            "consecutive_failures": int(source.get("consecutive_failures") or 0) + 1,
                            "next_retry_at": result.get("next_retry_at"),
                        },
                    )
                ],
            )
            return result
        finally:
            self.leases.release("source", source_id, status="COMPLETED" if result.get("status") in {"completed", "planned"} else "FAILED")

    def _run_candidate_verification(self, plan: Mapping[str, Any]) -> dict[str, Any]:
        if not self.config.verify_candidates:
            return {"status": "not_requested", "slots": 0, "probed": 0, "verified": 0, "rejected": 0}
        selected = plan["source_verification_queue"][: self.config.max_slots]
        slot_results: list[dict[str, Any]] = []
        for slot in selected:
            slot_id = str(slot.get("slot_id"))
            candidates = list_candidates(slot_id=slot_id, settings=self.settings)
            if candidates.is_empty():
                continue
            candidate_rows = candidates.to_dicts()
            candidate_rows.sort(
                key=lambda item: (
                    0 if _is_gazette_history_index(item) else 1,
                    _candidate_role_identity_score(item),
                    0 if bool(item.get("is_verified")) else 1,
                    0 if (
                        is_reusable_source_entry(canonicalize_url(str(item.get("candidate_url") or "")))
                        and not _looks_like_detail_page(canonicalize_url(str(item.get("candidate_url") or "")))
                        and str(item.get("page_type") or "") not in {"policy_detail", "content_page", "policy_content_page"}
                        and str(item.get("candidate_kind") or "") != "policy_content_evidence"
                    ) else 1,
                    0 if bool(item.get("entry_eligible")) else 1,
                    0 if str(item.get("manual_review_status") or "").lower() == "pending_probe" else 1,
                    0 if str(item.get("parser_status") or "").lower() in {"ok", "verified", "list_detected", "pagination_detected"} else 1,
                    0 if str(item.get("health_status") or "").lower() in {"healthy", "ok", "direct_ok", "operational"} else 1,
                    0 if not item.get("source_id") else 1,
                    -int(item.get("health_probe_success_count") or 0),
                    str(item.get("candidate_id") or ""),
                )
            )
            candidate_ids: list[str] = []
            seen_canonical_urls: set[str] = set()
            for item in candidate_rows:
                candidate_id = str(item.get("candidate_id") or "")
                canonical_url = canonicalize_url(str(item.get("canonical_url") or item.get("candidate_url") or ""))
                if not candidate_id or canonical_url in seen_canonical_urls:
                    continue
                seen_canonical_urls.add(canonical_url)
                candidate_ids.append(candidate_id)
                if len(candidate_ids) >= self.config.top_k:
                    break
            if not candidate_ids:
                continue
            self.store.transition("candidates_ranked", reason_code="deterministic_candidate_order", slot_id=slot_id)
            self.store.transition("probe_started", reason_code="two_independent_probes", slot_id=slot_id)
            try:
                from policydb.crawl.fetcher import RespectfulFetcher

                probe_source_id = str(candidate_rows[0].get("source_id") or "candidate_probe") if candidate_rows else "candidate_probe"
                recorder = HttpAttemptRecorder(self.ledger, self.run_dir / "outbound_http_attempts.jsonl", run_id=self.run_id, source_id=probe_source_id, slot_id=slot_id, stage="candidate_probe")
                probe_fetcher = RespectfulFetcher(user_agent=self.settings.user_agent, timeout=self.settings.request_timeout, connect_timeout=self.settings.connect_timeout, retries=self.settings.max_retries, rate_limit=self.settings.default_rate_limit, check_robots=self.settings.respect_robots, attempt_callback=recorder)
                probe = probe_candidates(candidate_ids=candidate_ids, rounds=2, settings=self.settings, fetcher=probe_fetcher)
                self.store.transition("probe_completed", reason_code="two_independent_probes_finished", slot_id=slot_id, checked=probe.get("checked", 0))
                verification = verify_candidates(candidate_ids=candidate_ids, run_id=self.run_id, settings=self.settings)
                self.store.transition("verify_completed", reason_code="deterministic_gate_evaluated", slot_id=slot_id, verified=verification.get("verified", 0))
                self.store.checkpoint("PROBE_COMPLETED", resource_id=slot_id, checked=probe.get("checked", 0))
                self.store.checkpoint("VERIFICATION_COMPLETED", resource_id=slot_id, verified=verification.get("verified", 0))
                slot_results.append({"slot_id": slot_id, "candidate_ids": candidate_ids, "probe": probe, "verification": verification})
            except BudgetExceeded as exc:
                slot_results.append({"slot_id": slot_id, "status": "paused_budget", "error": str(exc)})
                break
            except Exception as exc:
                slot_results.append({"slot_id": slot_id, "status": "failed_recoverable", "error_type": type(exc).__name__, "error": str(exc)[:500]})
        return {
            "status": "completed",
            "slots": len(slot_results),
            "probed": sum(int(item.get("probe", {}).get("checked", 0)) for item in slot_results),
            "verified": sum(int(item.get("verification", {}).get("verified", 0)) for item in slot_results),
            "rejected": sum(int(item.get("verification", {}).get("checked", 0)) - int(item.get("verification", {}).get("verified", 0)) for item in slot_results),
            "results": slot_results,
        }

    def _run_enable_ready(self, verification: Mapping[str, Any]) -> dict[str, Any]:
        if not self.config.enable_ready:
            return {"status": "not_requested", "promoted": 0, "enabled": 0, "rejected": []}
        promoted = 0
        enabled = 0
        rejected: list[dict[str, Any]] = []
        for item in verification.get("results", []):
            slot_id = str(item.get("slot_id") or "")
            if not slot_id or not int(item.get("verification", {}).get("verified", 0) or 0):
                continue
            try:
                candidate_frame = list_candidates(slot_id=slot_id, status="verified", settings=self.settings)
                selected_ids = {str(value) for value in item.get("candidate_ids", [])}
                candidate_frame = candidate_frame.filter(pl.col("candidate_id").is_in(sorted(selected_ids))) if selected_ids and not candidate_frame.is_empty() else candidate_frame
                for candidate in candidate_frame.iter_rows(named=True):
                    promotion = promote_candidate(str(candidate["candidate_id"]), settings=self.settings)
                    promoted += 1
                    source_id = str(promotion.get("source_id") or "")
                    if not source_id:
                        continue
                    try:
                        enable_source_strict(source_id, settings=self.settings)
                        enabled += 1
                        self.store.checkpoint("SOURCE_ENABLED", resource_id=source_id)
                    except Exception as exc:
                        rejected.append({"source_id": source_id, "reason_code": type(exc).__name__, "reason": str(exc)[:500]})
            except Exception as exc:
                rejected.append({"slot_id": slot_id, "reason_code": type(exc).__name__, "reason": str(exc)[:500]})
        self.store.transition("strict_enable_completed", reason_code="deterministic_strict_gate", enabled=enabled)
        return {"status": "completed", "promoted": promoted, "enabled": enabled, "rejected": rejected}

    def _run_gap_scan(self, plan: Mapping[str, Any]) -> dict[str, Any]:
        if not self.config.repair_gaps:
            return {"status": "not_requested", "open_gaps": 0}
        all_rows: list[dict[str, Any]] = []
        for source in plan["sources"][: self.config.max_sources]:
            docs = _read_documents(self.settings, str(source.get("source_id")))
            all_rows.extend(detect_coverage_gaps(docs, source=source, expected_start=self.config.backfill_from, expected_end=self.config.backfill_to))
        result = upsert_coverage_gaps(self.settings, all_rows)
        self.store.transition("gap_scan_completed", reason_code="deterministic_gap_scan")
        self.store.checkpoint("GAP_SCAN_COMPLETED", resource_id=self.run_id, incoming=result["incoming"], open_gaps=result["open"])
        return {"status": "completed", "open_gaps": result["open"], "incoming": result["incoming"]}

    def run(self, *, command: str = "run") -> dict[str, Any]:
        self.config.validate(command=command)
        if not self.config.apply or self.config.dry_run:
            return self.plan()
        evidence = load_test_evidence(self.settings)
        if self.config.all_remaining and evidence.get("test_result") != "passed":
            self.store.write_status({"global_status": "BLOCKED_CONFLICT", "status": "BLOCKED_CONFLICT", "full_tests_status": evidence.get("test_result", "unknown")})
            return {"run_id": self.run_id, "run_dir": str(self.run_dir), "status": "BLOCKED_CONFLICT", "exit_code": 10, "reason": "full_sync_requires_passed_test_evidence", "test_evidence": evidence}
        plan = build_sync_plan(self.settings, self.config)
        self._write_plan_artifacts(plan)
        historical_repairs = audit_historical_runs(self.settings, self.run_dir)
        _atomic_json(self.run_dir / "historical_state_repairs.json", historical_repairs)
        self.store.transition("batch_claimed", reason_code="bounded_full_sync_run", state=plan["database_sync_status"]["global_status"])
        discovery = self._run_discovery(plan)
        active_plan = build_sync_plan(self.settings, self.config) if discovery.get("applied_candidates") else plan
        verification = self._run_candidate_verification(active_plan)
        enablement = self._run_enable_ready(verification)
        # Promotion/strict enablement changes the registry.  Rebuild the plan
        # before selecting backfill sources so newly enabled roles participate
        # in the same bounded acceptance run.
        if enablement.get("enabled") or enablement.get("promoted") or verification.get("verified"):
            active_plan = build_sync_plan(self.settings, self.config)
        source_results: list[dict[str, Any]] = []
        # Discovery and deterministic source backfill have independent
        # budgets.  Exhausting the bounded AI/search budget must pause only
        # discovery; it must not discard already verified/enabled sources or
        # prevent their resumable backfill from producing completion evidence.
        selected_sources = self._selected_sources(active_plan)
        if self.config.backfill:
            for source in selected_sources:
                if self.stop_requested():
                    self.store.transition("batch_yielded", reason_code="stop_requested", state="PAUSED_BUDGET")
                    break
                if _retry_wait_active(source):
                    source_id = str(source.get("source_id"))
                    retry_wait = {
                        "source_id": source_id,
                        "mode": "backfill",
                        "status": "retry_wait",
                        "stage_result": "RETRY_WAIT",
                        "incremental_skipped_dependency": True,
                        "reason_code": "next_retry_at_not_reached",
                        "next_retry_at": source.get("next_retry_at"),
                    }
                    source_results.append(retry_wait)
                    self.store.transition(
                        "backfill_retry_wait",
                        reason_code="next_retry_at_not_reached",
                        source_id=source_id,
                        state="RETRY_WAIT",
                        next_retry_at=source.get("next_retry_at"),
                    )
                    self.store.checkpoint(
                        "BACKFILL_RETRY_WAIT",
                        resource_id=source_id,
                        reason_code="next_retry_at_not_reached",
                        next_retry_at=source.get("next_retry_at"),
                    )
                    continue
                source_results.append(self._run_source(source, mode="backfill", max_fetches=self.config.max_documents))
        if self.config.incremental:
            backfill_results = {
                str(item.get("source_id")): item
                for item in source_results
                if item.get("mode") == "backfill"
            }
            for source in selected_sources:
                if self.stop_requested():
                    self.store.transition("batch_yielded", reason_code="stop_requested", state="PAUSED_BUDGET")
                    break
                if self.config.resume and str(source.get("source_id")) in self.store.completed_resources("INCREMENTAL_COMPLETED"):
                    continue
                source_id = str(source.get("source_id"))
                backfill_result = backfill_results.get(source_id)
                if backfill_result and backfill_result.get("status") not in {"completed", "complete_with_gaps"}:
                    skipped = {
                        "source_id": source_id,
                        "mode": "incremental",
                        "status": "skipped_dependency",
                        "stage_result": "SKIPPED_DEPENDENCY",
                        "incremental_skipped_dependency": True,
                        "reason_code": "backfill_not_success",
                        "depends_on": "backfill",
                    }
                    source_results.append(skipped)
                    self.store.transition(
                        "incremental_skipped_dependency",
                        reason_code="backfill_not_success",
                        source_id=source_id,
                        state="SKIPPED_DEPENDENCY",
                    )
                    self.store.checkpoint(
                        "INCREMENTAL_SKIPPED_DEPENDENCY",
                        resource_id=source_id,
                        reason_code="backfill_not_success",
                    )
                    continue
                eligible_without_backfill = str(source.get("source_state")) in {
                    "BACKFILL_COMPLETE",
                    "INCREMENTAL_HEALTHY",
                    "STALE",
                } and _has_backfill_completion_evidence(self.settings, source_id)
                if not backfill_result and not eligible_without_backfill:
                    skipped = {
                        "source_id": source_id,
                        "mode": "incremental",
                        "status": "skipped_dependency",
                        "stage_result": "SKIPPED_DEPENDENCY",
                        "incremental_skipped_dependency": True,
                        "reason_code": "source_not_backfilled",
                        "depends_on": "backfill",
                    }
                    source_results.append(skipped)
                    self.store.transition(
                        "incremental_skipped_dependency",
                        reason_code="source_not_backfilled",
                        source_id=source_id,
                        state="SKIPPED_DEPENDENCY",
                    )
                    self.store.checkpoint(
                        "INCREMENTAL_SKIPPED_DEPENDENCY",
                        resource_id=source_id,
                        reason_code="source_not_backfilled",
                    )
                    continue
                if any(item.get("source_id") == source_id and item.get("status") in {"failed", "failed_recoverable", "partial", "retry_wait", "human_review", "skipped_dependency", "paused_budget"} for item in source_results):
                    continue
                source_results.append(self._run_source(source, mode="incremental", max_fetches=self.config.max_documents))
        gaps = self._run_gap_scan(plan)
        final_plan = build_sync_plan(self.settings, self.config)
        gap_snapshot = _open_gap_snapshot(
            self.settings,
            {str(row.get("source_id")) for row in final_plan["sources"] if row.get("source_id")},
        )
        gaps = {**gaps, "open_gaps": gap_snapshot["open"], "critical_gaps": gap_snapshot["critical"]}
        consistency = _consistency_check(
            self.settings,
            run_id=self.run_id,
            run_dir=self.run_dir,
            plan=final_plan,
            source_results=source_results,
            gaps=gaps,
        )
        failed = sum(result.get("status") in {"failed", "failed_recoverable", "blocked_conflict", "human_review"} for result in source_results)
        completed = sum(result.get("status") in {"completed", "complete_with_gaps", "planned"} for result in source_results)
        partial = sum(result.get("status") == "partial" for result in source_results)
        paused_budget = sum(result.get("status") == "paused_budget" for result in source_results)
        retry_wait = sum(result.get("status") == "retry_wait" for result in source_results)
        skipped_dependency = sum(result.get("status") == "skipped_dependency" for result in source_results)
        human_review = sum(result.get("status") == "human_review" for result in source_results)
        complete_with_gaps = sum(result.get("status") == "complete_with_gaps" for result in source_results)
        self.store.transition("consistency_checked", reason_code="single_snapshot_validation", state="PASSED" if consistency["passed"] else "FAILED_RECOVERABLE", consistency_errors=consistency["consistency_errors"])
        self.store.transition("batch_completed", reason_code="bounded_full_sync_finished", state="COMPLETED" if consistency["passed"] else "FAILED_RECOVERABLE")
        previous_success = _latest_successful_full_sync(self.settings)
        successful_now = not (failed or partial or paused_budget or retry_wait or skipped_dependency or human_review) and consistency["passed"] and consistency["required_backfill_failures"] == 0
        final_status = database_sync_status(
            self.settings,
            final_plan["slot_rows"],
            final_plan["sources"],
            open_gaps=gaps["open_gaps"],
            critical_gaps=gaps["critical_gaps"],
            last_run={
                "documents_added": sum(int(item.get("fetched", 0)) for item in source_results),
                "completed_at": _now() if successful_now else None,
                "partially_successful_at": _now() if not successful_now and source_results else None,
            },
            last_successful_full_sync=_now() if successful_now else previous_success,
        )
        final_status["consistency_errors"] = consistency["consistency_errors"]
        final_status["source_result_counts"] = {"success": completed - complete_with_gaps, "complete_with_gaps": complete_with_gaps, "partial": partial, "paused_budget": paused_budget, "retry_wait": retry_wait, "skipped_dependency": skipped_dependency, "human_review": human_review, "failed_terminal": failed}
        if discovery.get("status") == "paused_budget" or any(item.get("status") == "paused_budget" for item in source_results):
            final_status["global_status"] = "PAUSED_BUDGET"
        elif any(item.get("status") == "blocked_conflict" for item in source_results):
            final_status["global_status"] = "BLOCKED_CONFLICT"
        elif failed or partial or retry_wait or skipped_dependency or human_review:
            final_status["global_status"] = "DEGRADED"
        if consistency["consistency_errors"]:
            final_status["global_status"] = "FAILED_RECOVERABLE"
        gate_reasons: list[str] = []
        if consistency["consistency_errors"]:
            gate_reasons.extend(str(item.get("code") or "consistency_error") for item in consistency["consistency_errors"])
        if int(gaps.get("critical_gaps", 0) or 0) > 0:
            gate_reasons.append("critical_gaps")
        if evidence.get("test_result") != "passed":
            gate_reasons.append(f"full_tests_{evidence.get('test_result', 'unknown')}")
        if self.config.all_five_source_roles:
            observed_roles = {str(row.get("source_role") or "") for row in final_plan["slot_rows"]}
            missing_roles = sorted(set(REQUIRED_ROLES) - observed_roles)
            if missing_roles:
                gate_reasons.append("missing_roles:" + ",".join(missing_roles))
            if len(selected_sources) < len(observed_roles):
                gate_reasons.append("scoped_sources_incomplete")
            # A strict revalidation run may intentionally reuse already
            # audited candidates and sources without starting new discovery.
            # Require fresh AI/search evidence only when this invocation
            # explicitly requested discovery.  This keeps the acceptance gate
            # honest while allowing a no-discovery resume to finish the
            # deterministic verification/backfill stages.
            discovery_required = bool(self.config.discover_missing) and str(self.config.discovery_mode).upper() != "DISABLED"
            if discovery_required:
                if int(discovery.get("ai_calls", 0) or 0) <= 0:
                    gate_reasons.append("ai_calls_not_started")
                if int(discovery.get("search_calls", 0) or 0) <= 0:
                    gate_reasons.append("search_calls_not_started")
            if self.config.backfill:
                backfill_statuses = [str(item.get("status") or "") for item in source_results if str(item.get("mode") or "") == "backfill"]
                if len(backfill_statuses) < len(selected_sources):
                    gate_reasons.append("scoped_backfill_incomplete")
                if any(status not in {"completed", "complete_with_gaps"} for status in backfill_statuses):
                    gate_reasons.append("scoped_backfill_status")
            if int(final_status.get("enabled_slots", 0) or 0) < len(observed_roles):
                gate_reasons.append("scoped_enabled_slots_incomplete")
        go_gate = {"status": "GO" if not gate_reasons else "BLOCKED", "reasons": sorted(set(gate_reasons)), "evaluated_at": _now(), "scope": self.config.scope, "all_five_source_roles": self.config.all_five_source_roles}
        self.store.transition("go_gate_evaluated", reason_code="deterministic_acceptance_gate", state=go_gate["status"], go_gate=go_gate)
        if go_gate["status"] == "BLOCKED":
            self.store.transition("go_gate_blocked", reason_code=go_gate["reasons"][0] if go_gate["reasons"] else "acceptance_gate_blocked", state="BLOCKED", go_gate=go_gate)
        final_status["go_gate"] = go_gate
        latest_error = next((item.get("error_message") for item in source_results if item.get("error_message")), None)
        self.store.write_status({"global_status": final_status["global_status"], "status": final_status["global_status"], "current_batch": {"run_id": self.run_id, "sources": len(selected_sources), "completed": completed, "failed": failed, "human_review": final_status["human_review_slots"]}, "current_slot": None, "current_step": "completed", "go_gate": go_gate, "provider_status": discovery.get("provider_status", "configured"), "api_balance_status": discovery.get("api_balance_status", "unknown"), "ai_calls": discovery.get("ai_calls", 0), "ai_attempts": discovery.get("ai_attempts", discovery.get("planned", 0)), "search_calls": discovery.get("search_calls", 0), "http_calls": int(self.ledger.used.get("http_calls", 0)), "tokens": discovery.get("tokens"), "estimated_cost_usd": discovery.get("cost"), "usage_status": discovery.get("usage_status", "unavailable"), "candidates": discovery.get("applied_candidates", 0), "probes": verification.get("probed", 0), "human_review": final_status["human_review_slots"], "retries": final_status["retry_wait_slots"], "verified": final_status["verified_slots"], "enabled": final_status["enabled_slots"], "unresolved": final_status["unresolved_slots"], "latest_error": latest_error, "full_tests_status": evidence.get("test_result", "unknown")})
        exit_code = 1 if consistency["consistency_errors"] else 20 if self.config.discover_missing and discovery.get("provider_status") == "unavailable" else 10 if go_gate["status"] == "BLOCKED" else 0
        summary = {"run_id": self.run_id, "run_dir": str(self.run_dir), "status": final_status["global_status"], "exit_code": exit_code, "go_gate": go_gate, "planned_slots": plan["estimates"]["slots"], "planned_sources": plan["estimates"]["sources"], "selected_sources": len(selected_sources), "source_results": source_results, "discovery": discovery, "verification": verification, "enablement": enablement, "gaps": gaps, "consistency": consistency, "database_sync_status": final_status, "historical_repairs": historical_repairs, "full_run_started": bool(self.config.all_remaining), "paid_api_calls_started": int(self.ledger.used.get("ai_calls", 0)) + int(self.ledger.used.get("search_calls", 0))}
        _atomic_json(self.run_dir / "database_sync_status.json", final_status)
        _atomic_json(self.run_dir / "run_summary.json", summary)
        _atomic_json(self.run_dir / "sync_run_summary.json", summary)
        _atomic_json(self.run_dir / "backfill_summary.json", {"sources": source_results, "completed": completed, "complete_with_gaps": complete_with_gaps, "partial": partial, "paused_budget": paused_budget, "retry_wait": retry_wait, "skipped_dependency": skipped_dependency, "human_review": human_review, "failed": failed})
        _atomic_json(self.run_dir / "incremental_summary.json", {"sources": [item for item in source_results if item.get("mode") == "incremental"]})
        _atomic_json(self.run_dir / "source_completion_summary.json", discovery)
        _atomic_json(self.run_dir / "gap_summary.json", gaps)
        _atomic_json(self.run_dir / "failure_summary.json", {"failed": failed, "consistency_errors": consistency["consistency_errors"], "results": [item for item in source_results if item.get("status") not in {"completed", "planned"}]})
        _atomic_json(self.run_dir / "consistency_snapshot.json", consistency)
        _atomic_json(self.run_dir / "provider_health.json", {"llm": discovery.get("provider_status", "configured"), "search": "operational" if discovery.get("search_calls", 0) else "not_called", "direct_fetch": "operational" if self.ledger.used.get("http_calls", 0) else "not_called", "api_balance_status": discovery.get("api_balance_status", "unknown"), "tokens": discovery.get("tokens"), "estimated_cost_usd": discovery.get("cost"), "usage_status": discovery.get("usage_status", "unavailable"), "secrets_redacted": True})
        _atomic_json(self.run_dir / "full_sync_manifest.json", {"run_id": self.run_id, "completed_at": _now(), "config": {key: str(value) if isinstance(value, Path) else value for key, value in asdict(self.config).items()}, "paid_api_calls_started": summary["paid_api_calls_started"], "tokens": discovery.get("tokens"), "estimated_cost_usd": discovery.get("cost"), "usage_status": discovery.get("usage_status", "unavailable"), "deterministic_gates_unchanged": True, "llm_can_set_verified": False})
        report_result = build_full_sync_report(
            self.settings,
            self.config,
            output_dir=self.run_dir,
            status_override=final_status,
        )
        _atomic_json(self.run_dir / "report_manifest.json", {"output_dir": report_result["output_dir"], "paths": report_result["paths"]})
        summary["report"] = report_result["output_dir"]
        _atomic_json(self.run_dir / "run_summary.json", summary)
        _atomic_json(self.run_dir / "sync_run_summary.json", summary)
        return summary

    def status(self) -> dict[str, Any]:
        latest = self.latest_run_dir(self.settings)
        if latest and latest != self.run_dir and not self.status_path_exists():
            return SyncStateStore(latest, run_id=latest.name).read_status()
        return self.store.read_status()

    def status_path_exists(self) -> bool:
        return self.store.status_path.exists()

    def refresh(self) -> dict[str, Any]:
        return self.run(command="refresh")

    def repair(self) -> dict[str, Any]:
        return self.run(command="repair")

    def report(self) -> dict[str, Any]:
        return build_full_sync_report(self.settings, self.config, output_dir=self.run_dir)

    def resume(self) -> dict[str, Any]:
        self.config = replace(self.config, resume=True)
        return self.run(command="resume")

    def execute(self, command: str) -> dict[str, Any]:
        if command == "plan":
            return self.plan()
        if command == "status":
            return self.status()
        if command == "resume":
            return self.resume()
        if command == "refresh":
            return self.refresh()
        if command == "repair":
            return self.repair()
        if command == "report":
            return self.report()
        return self.run(command="run")


def git_test_evidence(settings: Settings, *, output: Path | None = None) -> dict[str, Any]:
    """Create the small, non-sensitive test-evidence handoff used by preflight."""
    try:
        commit_sha = subprocess.run(["git", "rev-parse", "HEAD"], cwd=settings.root, capture_output=True, text=True, check=False).stdout.strip()
        dirty = bool(subprocess.run(["git", "status", "--short"], cwd=settings.root, capture_output=True, text=True, check=False).stdout.strip())
    except OSError:
        commit_sha, dirty = None, True
    payload = {"commit_sha": commit_sha, "test_commit_sha": commit_sha, "dirty_worktree": dirty, "timestamp": _now(), "collected": None, "passed": None, "failed": None, "errors": None, "skipped": None, "ruff_status": "unknown", "compileall_status": "unknown", "diff_check_status": "unknown", "overall_status": "unknown"}
    path = output or settings.outputs / "autopilot" / "latest_test_evidence.json"
    _atomic_json(path, payload)
    return payload


__all__ = [
    "BudgetExceeded",
    "BudgetLedger",
    "CRAWL_JOB_STATES",
    "DOCUMENT_STATES",
    "FullSyncConfig",
    "FullSyncController",
    "FullSyncError",
    "GLOBAL_SYNC_STATES",
    "InvalidTransition",
    "JobLeaseStore",
    "LeaseConflict",
    "SLOT_STATES",
    "SOURCE_STATES",
    "SyncStateStore",
    "build_sync_plan",
    "build_full_sync_report",
    "build_watermark",
    "canonical_document_key",
    "classify_document_change",
    "classify_slot_state",
    "classify_source_state",
    "database_sync_status",
    "detect_coverage_gaps",
    "derive_global_status",
    "document_version_key",
    "git_test_evidence",
    "load_test_evidence",
    "role_sla_hours",
    "source_freshness_status",
    "source_is_crawl_ready",
    "transition_allowed",
    "transition_state",
    "upsert_coverage_gaps",
    "upsert_document_versions",
    "watermark_equal",
]
