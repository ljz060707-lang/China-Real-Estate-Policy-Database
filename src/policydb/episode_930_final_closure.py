"""Deterministic root-object closure for the EP930 V3 promotion gate.

This module is deliberately offline.  It consumes persisted rehearsal evidence
and turns the apparent direction/date/recovery counts into auditable root
objects.  It never calls a search provider, an AI provider, or a crawler, and it
does not mutate production tables.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterable, Mapping
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import polars as pl

EPISODE_ID = "EP_2016_930_TIGHTENING"
SCOPE_VERSION = "930-analysis-ready-v1"
SCOPE_HASH = "a751e9a99d405bc91dc4a5c4a19f900b398400e1e88b43f724e034209807a90d"
WINDOW_START = date(2016, 9, 25)
WINDOW_END = date(2016, 10, 10)

_DATE_PATTERNS = (
    re.compile(
        r"自\s*(20\d{2})\s*(?:年|[-/.])\s*(\d{1,2})\s*(?:月|[-/.])\s*"
        r"(\d{1,2})\s*日?\s*(?:起|开始)?\s*(?:施行|执行|实施|生效)"
    ),
    re.compile(
        r"自\s*(20\d{2})\s*年\s*(\d{1,2})\s*月\s*(?:起|开始)?\s*"
        r"(?:施行|执行|实施|生效)"
    ),
)
_DATE_LITERAL = re.compile(r"(?<!\d)(20\d{2})[年\-/\.](\d{1,2})(?:月[\-/\.](\d{1,2})日?)?")
_NUMBER = re.compile(r"(?<![\d.])\d+(?:\.\d+)?")


def _text(value: object) -> str:
    if value is None:
        return ""
    try:
        if value != value:  # NaN
            return ""
    except Exception:
        pass
    return str(value).strip()


def _parse_date(value: object) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = _text(value)
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _json_hash(value: object) -> str:
    payload = json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _numeric_pair(row: Mapping[str, Any]) -> tuple[float, float, str] | None:
    unit = _text(row.get("unit"))
    old = _text(row.get("old_value"))
    new = _text(row.get("new_value"))
    if not unit or not old or not new:
        return None
    old_match = _NUMBER.search(old)
    new_match = _NUMBER.search(new)
    if not old_match or not new_match:
        return None
    # A year or an unlabelled malformed pair is not a policy parameter
    # transition.  The unit must be present in both values when it is textual.
    if unit in {"年", "month", "个月"} and ("年" in old or "年" in new) and not ("年" in old and "年" in new):
        return None
    try:
        return float(old_match.group(0)), float(new_match.group(0)), unit
    except ValueError:
        return None


def derive_direction_v3(row: Mapping[str, Any]) -> dict[str, Any]:
    """Derive direction only from explicit, non-conflicting evidence.

    Existing gate PASS is retained.  Unknown or conflicting text stays open;
    episode direction is never used as a substitute for action direction.
    """

    existing = _text(row.get("action_direction") or row.get("direction"))
    if existing.upper() in {"TIGHTENING", "SUPPORTIVE", "EASING"}:
        normalized = "SUPPORTIVE" if existing.upper() == "EASING" else existing.upper()
        return {
            "direction_state": "PASS",
            "direction": normalized,
            "direction_method": "EXISTING_VERIFIED_ACTION_DIRECTION",
            "direction_evidence_span": existing,
            "direction_rule_id": "V3_EXISTING_ACTION_DIRECTION",
            "direction_confidence": "HIGH",
        }

    text = " ".join(
        _text(row.get(column))
        for column in ("action_text", "official_text_excerpt", "official_text")
    )
    supportive = tuple(term for term in ("降低首付", "放宽", "取消限购", "取消限售", "提高贷款额度", "贷款额度提升", "可上浮", "扩大支持", "支持") if term in text)
    tightening = tuple(term for term in ("提高首付", "提高首付款", "收紧", "限制", "暂停", "严禁", "不得") if term in text)
    if supportive and tightening:
        return {
            "direction_state": "UNKNOWN",
            "direction": None,
            "direction_method": "LEXICAL_CONFLICT",
            "direction_evidence_span": "; ".join((*supportive, *tightening)),
            "direction_rule_id": "V3_NO_CONFLICTING_RULE",
            "direction_confidence": "LOW",
        }
    if supportive:
        return {
            "direction_state": "PASS",
            "direction": "SUPPORTIVE",
            "direction_method": "EXPLICIT_SEMANTIC_ACTION",
            "direction_evidence_span": "; ".join(supportive),
            "direction_rule_id": "V3_EXPLICIT_SUPPORTIVE_TERM",
            "direction_confidence": "MEDIUM",
        }
    if tightening:
        return {
            "direction_state": "PASS",
            "direction": "TIGHTENING",
            "direction_method": "EXPLICIT_SEMANTIC_ACTION",
            "direction_evidence_span": "; ".join(tightening),
            "direction_rule_id": "V3_EXPLICIT_TIGHTENING_TERM",
            "direction_confidence": "MEDIUM",
        }

    pair = _numeric_pair(row)
    policy_type = _text(row.get("policy_type")).upper()
    if pair is not None:
        old, new, unit = pair
        if policy_type in {"COMMERCIAL_DOWNPAYMENT", "PF_DOWNPAYMENT", "DOWNPAYMENT"}:
            direction = "TIGHTENING" if new > old else "SUPPORTIVE" if new < old else None
        elif policy_type in {"PF_LOAN_CEILING", "LOAN_CEILING", "CREDIT_CEILING"}:
            direction = "SUPPORTIVE" if new > old else "TIGHTENING" if new < old else None
        else:
            direction = None
        if direction:
            return {
                "direction_state": "PASS",
                "direction": direction,
                "direction_method": "PARAMETER_TRANSITION",
                "direction_evidence_span": f"{old:g}{unit} -> {new:g}{unit}",
                "direction_parameter_old": old,
                "direction_parameter_new": new,
                "direction_rule_id": "V3_PARAMETER_TRANSITION",
                "direction_confidence": "MEDIUM",
            }

    return {
        "direction_state": "UNKNOWN",
        "direction": None,
        "direction_method": "NO_SAFE_DETERMINISTIC_EVIDENCE",
        "direction_evidence_span": None,
        "direction_rule_id": "V3_REQUIRES_AI_OR_HUMAN_REVIEW",
        "direction_confidence": "LOW",
    }


def _date_from_text(text: str) -> tuple[date | None, str | None, str]:
    for index, pattern in enumerate(_DATE_PATTERNS):
        match = pattern.search(text)
        if not match:
            continue
        year, month = int(match.group(1)), int(match.group(2))
        day = int(match.group(3)) if len(match.groups()) >= 3 and match.group(3) else 1
        try:
            parsed = date(year, month, day)
        except ValueError:
            return None, match.group(0), "DATE_PARSE_FAILURE"
        basis = "EXPLICIT_EFFECTIVE_DATE" if index == 0 else "MONTH_ONLY_EFFECTIVE_DATE"
        return parsed, match.group(0), basis
    return None, None, "NO_EXPLICIT_EFFECTIVE_DATE"


def derive_date_v3(
    action: Mapping[str, Any],
    document: Mapping[str, Any] | None,
    *,
    window_start: date = WINDOW_START,
    window_end: date = WINDOW_END,
) -> dict[str, Any]:
    """Classify dates without inventing an effective date."""

    document = document or {}
    action_text = " ".join(
        _text(action.get(column)) for column in ("action_text", "official_text_excerpt")
    )
    document_text = " ".join(
        _text(document.get(column))
        for column in ("document_title", "official_text", "date_evidence_text")
    )
    effective = _parse_date(action.get("effective_date")) or _parse_date(document.get("effective_date"))
    evidence = _text(action.get("date_evidence_text")) or _text(document.get("date_evidence_text"))
    basis = _text(action.get("effective_date_basis")) or _text(document.get("effective_date_basis"))
    source = "EXISTING_EFFECTIVE_DATE" if effective else None
    if not effective:
        effective, evidence, parsed_basis = _date_from_text(action_text)
        if effective:
            basis = parsed_basis
            source = "ACTION_TEXT"
    if not effective:
        effective, evidence, parsed_basis = _date_from_text(document_text)
        if effective:
            basis = parsed_basis
            source = "DOCUMENT_TEXT"

    publication = _parse_date(action.get("publication_date")) or _parse_date(document.get("publication_date"))
    announcement = _parse_date(action.get("announcement_date")) or _parse_date(document.get("announcement_date"))
    if not effective and publication:
        basis = "PUBLICATION_DATE_FALLBACK"
        source = "PUBLICATION_DATE"
    elif not effective and announcement:
        basis = "ANNOUNCEMENT_DATE_FALLBACK"
        source = "ANNOUNCEMENT_DATE"

    dates = [value for value in (effective, publication, announcement) if value]
    outside = [value for value in dates if value < window_start or value > window_end]
    all_text = f"{action_text} {document_text}"
    has_episode_year = "2016" in all_text
    literal_years = [int(match.group(1)) for match in _DATE_LITERAL.finditer(all_text)]
    if outside and not any(window_start <= value <= window_end for value in dates):
        membership = "OUTSIDE_FROZEN_EPISODE_WINDOW"
    elif literal_years and all(year != 2016 for year in literal_years):
        membership = "OUTSIDE_FROZEN_EPISODE_WINDOW"
    elif dates and not has_episode_year and not any(window_start <= value <= window_end for value in dates):
        membership = "EPISODE_DATE_EVIDENCE_MISSING"
    else:
        membership = "WITHIN_OR_UNRESOLVED"

    if not dates:
        state = "MISSING_DOCUMENT_EVIDENCE"
    elif basis == "MONTH_ONLY_EFFECTIVE_DATE":
        state = "PARTIAL_EFFECTIVE_DATE"
    elif len(set(dates)) > 1 and effective and publication and effective != publication:
        state = "DATE_CONFLICT"
    else:
        state = "PASS"
    return {
        "date_state": state,
        "effective_date": effective.isoformat() if effective else None,
        "publication_date": publication.isoformat() if publication else None,
        "announcement_date": announcement.isoformat() if announcement else None,
        "date_source": source,
        "date_basis": basis or "NO_EXPLICIT_EFFECTIVE_DATE",
        "date_evidence_text": evidence or None,
        "episode_membership_state": membership,
        "date_confidence": "HIGH" if state == "PASS" and basis.startswith("EXPLICIT") else "MEDIUM" if dates else "LOW",
    }


def _join_by_id(frame: pl.DataFrame, column: str) -> dict[str, dict[str, Any]]:
    if frame.is_empty() or column not in frame.columns:
        return {}
    return {
        _text(row.get(column)): row
        for row in frame.iter_rows(named=True)
        if _text(row.get(column))
    }


def build_direction_closure(actions: pl.DataFrame, gate_state: pl.DataFrame) -> pl.DataFrame:
    gate_by_id = _join_by_id(gate_state, "action_id")
    rows: list[dict[str, Any]] = []
    for row in actions.iter_rows(named=True):
        action_id = _text(row.get("action_id"))
        gate = gate_by_id.get(action_id, {})
        derived = derive_direction_v3({**row, **gate})
        if _text(gate.get("post_direction_gate")).upper() == "PASS":
            derived = {
                **derived,
                "direction_state": "PASS",
                "direction_method": "EXISTING_GATE_PASS",
                "direction_rule_id": "V3_RETAIN_EXISTING_GATE",
            }
        rows.append(
            {
                "action_id": action_id,
                "document_id": row.get("document_id"),
                "city": row.get("city"),
                "policy_type": row.get("policy_type"),
                "action_text": row.get("action_text"),
                **derived,
                "prior_direction_gate": gate.get("post_direction_gate"),
                "closure_status": "CLOSED" if derived["direction_state"] == "PASS" else "OPEN_DIRECTION_REVIEW",
            }
        )
    return pl.DataFrame(rows, infer_schema_length=None)


def build_date_closure(
    actions: pl.DataFrame,
    documents: pl.DataFrame,
    gate_state: pl.DataFrame,
    *,
    window_start: date = WINDOW_START,
    window_end: date = WINDOW_END,
) -> pl.DataFrame:
    gate_by_id = _join_by_id(gate_state, "action_id")
    documents_by_id = _join_by_id(documents, "document_id")
    rows: list[dict[str, Any]] = []
    for row in actions.iter_rows(named=True):
        action_id = _text(row.get("action_id"))
        gate = gate_by_id.get(action_id, {})
        derived = derive_date_v3(
            row,
            documents_by_id.get(_text(row.get("document_id"))),
            window_start=window_start,
            window_end=window_end,
        )
        if _text(gate.get("date_gate")).upper() == "PASS":
            derived = {
                **derived,
                "date_state": "PASS",
                "closure_status": "CLOSED_EXISTING_GATE",
            }
        rows.append(
            {
                "action_id": action_id,
                "document_id": row.get("document_id"),
                "city": row.get("city"),
                "action_text": row.get("action_text"),
                **derived,
                "prior_date_gate": gate.get("date_gate"),
                "closure_status": derived.get("closure_status") or ("CLOSED" if derived["date_state"] == "PASS" else "OPEN_DATE_REVIEW"),
            }
        )
    return pl.DataFrame(rows, infer_schema_length=None)


def build_recovery_disposition(recovery: pl.DataFrame) -> pl.DataFrame:
    rows: list[dict[str, Any]] = []
    for row in recovery.iter_rows(named=True):
        required = bool(row.get("recovery_required"))
        if not required:
            disposition = "REUSED_EXISTING_EVIDENCE"
            state = "TERMINAL"
        elif row.get("document_version_id") or row.get("cache_hit"):
            disposition = "REUSED_EXISTING_EVIDENCE"
            state = "TERMINAL"
        elif row.get("real_network_fetch"):
            disposition = "FALLBACK_OFFICIAL_RECOVERY"
            state = "OPEN"
        else:
            disposition = "PREFERRED_SOURCE_RECOVERY"
            state = "OPEN"
        rows.append(
            {
                **row,
                "v3_disposition": disposition,
                "v3_state": state,
                "v3_next_action": "existing_recovery_controller_retry" if state == "OPEN" else "none",
                "v3_network_executed": False,
                "v3_reason_code": "NO_DOCUMENT_VERSION_OR_FETCH_EVIDENCE" if state == "OPEN" else "VALID_EXISTING_PROVENANCE",
            }
        )
    return pl.DataFrame(rows, infer_schema_length=None) if rows else pl.DataFrame()


def _split_blockers(values: Iterable[str]) -> list[str]:
    return sorted({value for value in values if value})


def build_root_objects(
    actions: pl.DataFrame,
    documents: pl.DataFrame,
    gate_state: pl.DataFrame,
    direction: pl.DataFrame,
    dates: pl.DataFrame,
    recovery: pl.DataFrame,
    *,
    api_blocked: bool = True,
) -> pl.DataFrame:
    gate_by_id = _join_by_id(gate_state, "action_id")
    direction_by_id = _join_by_id(direction, "action_id")
    date_by_id = _join_by_id(dates, "action_id")
    document_by_id = _join_by_id(documents, "document_id")
    rows: list[dict[str, Any]] = []
    for action in actions.iter_rows(named=True):
        action_id = _text(action.get("action_id"))
        document_id = _text(action.get("document_id"))
        gate = gate_by_id.get(action_id, {})
        direction_row = direction_by_id.get(action_id, {})
        date_row = date_by_id.get(action_id, {})
        document = document_by_id.get(document_id, {})
        blockers: list[str] = []
        if direction_row.get("direction_state") != "PASS":
            blockers.append("DIRECTION_UNRESOLVED")
        if date_row and date_row.get("date_state") != "PASS":
            blockers.append("DATE_UNRESOLVED")
        if date_row.get("episode_membership_state") in {"OUTSIDE_FROZEN_EPISODE_WINDOW", "EPISODE_DATE_EVIDENCE_MISSING"}:
            blockers.append("EPISODE_MEMBERSHIP_CONFLICT")
        if not document or not bool(document.get("is_formal_eligible", True)):
            blockers.append("OFFICIAL_EVIDENCE_UNRESOLVED")
        if api_blocked:
            blockers.append("API_CERTIFICATION_BLOCKED")
        rows.append(
            {
                "root_object_type": "ACTION",
                "root_object_id": action_id,
                "action_id": action_id,
                "evidence_unit_id": f"EVIDENCE:{document_id}" if document_id else None,
                "document_id": document_id or None,
                "city": action.get("city"),
                "policy_title": document.get("document_title"),
                "action_text": action.get("action_text"),
                "direction_state": direction_row.get("direction_state"),
                "date_state": date_row.get("date_state") if date_row else "NOT_IN_DATE_FAILURE_SET",
                "recovery_state": "NOT_LINKED",
                "official_state": "PASS" if document and document.get("is_formal_eligible", True) else "FAIL",
                "dedup_state": gate.get("dedup_gate"),
                "api_state": "BLOCKED" if api_blocked else "READY",
                "pass1_state": gate.get("api_pass1_status"),
                "pass2_state": gate.get("api_pass2_status"),
                "promotion_state": gate.get("promotion_gate"),
                "root_blocker_count": len(blockers),
                "root_blockers": ";".join(_split_blockers(blockers)),
                "evidence_date_scope": date_row.get("episode_membership_state") if date_row else "NOT_ASSESSED",
            }
        )
    for row in recovery.iter_rows(named=True):
        if not bool(row.get("recovery_required")):
            continue
        queue_id = _text(row.get("queue_item_id"))
        rows.append(
            {
                "root_object_type": "RECOVERY_ITEM",
                "root_object_id": queue_id,
                "action_id": None,
                "evidence_unit_id": None,
                "document_id": None,
                "city": row.get("city"),
                "policy_title": None,
                "action_text": None,
                "direction_state": "NOT_LINKED",
                "date_state": "NOT_LINKED",
                "recovery_state": "OPEN",
                "official_state": "UNRESOLVED",
                "dedup_state": "NOT_LINKED",
                "api_state": "NOT_LINKED",
                "pass1_state": "NOT_LINKED",
                "pass2_state": "NOT_LINKED",
                "promotion_state": "NOT_ELIGIBLE",
                "root_blocker_count": 1,
                "root_blockers": "RECOVERY_REQUIRED",
                "evidence_date_scope": "NOT_LINKED",
            }
        )
    return pl.DataFrame(rows, infer_schema_length=None)


def root_counts(root_objects: pl.DataFrame) -> dict[str, int]:
    if root_objects.is_empty():
        return {"actions": 0, "evidence_units": 0, "documents": 0, "recovery_items": 0}
    action_ids = root_objects.filter(pl.col("root_object_type") == "ACTION").get_column("action_id").drop_nulls()
    evidence_ids = root_objects.get_column("evidence_unit_id").drop_nulls().unique()
    document_ids = root_objects.get_column("document_id").drop_nulls().unique()
    recovery_count = int((root_objects.get_column("root_object_type") == "RECOVERY_ITEM").sum())
    return {
        "actions": action_ids.n_unique(),
        "evidence_units": evidence_ids.n_unique(),
        "documents": document_ids.n_unique(),
        "recovery_items": recovery_count,
    }


def _owner_payload(path: Path) -> dict[str, Any]:
    try:
        raw = path.read_text(encoding="utf-8", errors="replace")
    except OSError as exc:
        return {"read_status": "ERROR", "error_type": type(exc).__name__}
    payload: dict[str, Any] = {"read_status": "TEXT", "content_sha256": hashlib.sha256(raw.encode("utf-8")).hexdigest()}
    try:
        value = json.loads(raw)
    except json.JSONDecodeError:
        value = None
    if isinstance(value, dict):
        payload["read_status"] = "JSON"
        payload["json_keys"] = sorted(str(key) for key in value)
        for key in ("pid", "owner_pid", "lock_owner_pid", "runner_pid", "worker_pid", "writer_pid"):
            if key in value:
                payload[key] = value.get(key)
        for key in ("run_id", "automation_id", "heartbeat_at", "updated_at", "created_at"):
            if key in value:
                payload[key] = value.get(key)
    else:
        payload["owner_fields"] = "UNPARSEABLE"
    return payload


def build_stale_lock_audit(paths: Iterable[Path], *, exclude_pid: int | None = None) -> dict[str, Any]:
    """Audit lock ownership without removing or replacing any lock."""

    try:
        import psutil
    except ImportError:  # pragma: no cover - dependency is part of production env
        psutil = None
    live: dict[int, dict[str, Any]] = {}
    if psutil is not None:
        for process in psutil.process_iter(["pid", "name", "create_time", "cmdline"]):
            pid = int(process.info.get("pid") or 0)
            if not pid or pid == exclude_pid:
                continue
            command = " ".join(process.info.get("cmdline") or [])
            command_lower = command.lower()
            ep930_markers = (
                "episode_930_autorun",
                "episode_930_production",
                "start_episode_930_autonomous",
                "run_episode_930_autonomous",
            )
            crpd_markers = ("crpd_autonomous", "crpd_audited_full_backfill")
            writer_capable_markers = ("backfill_engine.py", "crawl.service")
            if any(marker in command_lower for marker in (*ep930_markers, *crpd_markers, *writer_capable_markers)):
                if any(marker in command_lower for marker in ep930_markers):
                    process_kind = "EP930_PROCESS"
                elif any(marker in command_lower for marker in writer_capable_markers):
                    process_kind = "CRPD_WRITER_CAPABLE"
                else:
                    process_kind = "CRPD_PROCESS"
                if "writer" in command_lower or "duckdb" in command_lower:
                    process_kind = f"{process_kind}_WRITER"
                live[pid] = {
                    "pid": pid,
                    "name": process.info.get("name"),
                    "create_time": process.info.get("create_time"),
                    "process_kind": process_kind,
                    "command_sha256": hashlib.sha256(command.encode("utf-8", errors="replace")).hexdigest(),
                }
    audits: list[dict[str, Any]] = []
    for path in paths:
        item: dict[str, Any] = {"path": str(path), "exists": path.exists()}
        if path.exists():
            item.update({"last_write": datetime.fromtimestamp(path.stat().st_mtime, UTC).isoformat(), "size_bytes": path.stat().st_size})
            owner = _owner_payload(path)
            item["owner_evidence"] = owner
            pid_values = []
            for key in ("pid", "owner_pid", "lock_owner_pid", "runner_pid", "worker_pid", "writer_pid"):
                value = owner.get(key)
                try:
                    if value is not None:
                        pid_values.append(int(value))
                except (TypeError, ValueError):
                    continue
            owners = [live[pid] for pid in sorted(set(pid_values)) if pid in live]
            item["live_owner_pids"] = sorted({owner["pid"] for owner in owners})
            item["live_owner_processes"] = owners
            item["status"] = "LIVE_OWNER" if owners else "STALE_LOCK_CONFIRMED" if not pid_values else "STALE_OWNER_PID"
        else:
            item["status"] = "ABSENT"
        audits.append(item)
    return {
        "audited_at": datetime.now(UTC).isoformat(),
        "process_scan_excluded_pid": exclude_pid,
        "live_relevant_processes": list(live.values()),
        "production_writer_count": sum(1 for item in live.values() if item.get("process_kind", "").endswith("_WRITER")),
        "active_ep930_runner_count": sum(1 for item in live.values() if item.get("process_kind", "").startswith("EP930_PROCESS")),
        "writer_capable_process_count": sum(1 for item in live.values() if item.get("process_kind") == "CRPD_WRITER_CAPABLE"),
        "locks": audits,
        "mutation_performed": False,
        "cleanup_policy": "Use official controller lock acquisition only after each owner is proven dead; no direct rm/unlink in V3.",
    }


def api_certification_summary(
    recovery_state: Mapping[str, Any],
    provider_state: Mapping[str, Any],
    *,
    pass1_success: int = 0,
    pass2_success: int = 0,
    api_calls_this_run: int = 0,
) -> dict[str, Any]:
    phase = _text(recovery_state.get("phase")).upper()
    schema_valid = recovery_state.get("schema_valid") is True
    certified = phase in {"BACKLOG_CONSUMPTION", "STABLE_BACKLOG_CONSUMPTION"} and schema_valid and pass1_success > 0 and pass2_success > 0
    return {
        "provider": provider_state.get("provider") or provider_state.get("provider_name"),
        "model": provider_state.get("model"),
        "provider_status": provider_state.get("status"),
        "recovery_phase": phase or None,
        "schema_valid": schema_valid,
        "last_probe_at": recovery_state.get("last_attempt_at"),
        "next_retry_at": recovery_state.get("next_retry_at"),
        "pass1_success": pass1_success,
        "pass2_success": pass2_success,
        "certification": "CERTIFIED" if certified else "BLOCKED_BY_CERTIFICATION_SEQUENCE",
        "api_calls_this_v3": api_calls_this_run,
        "manual_api_calls_this_v3": 0,
        "cache_reuse_counts_as_probe": False,
        "gate": "SINGLE_PROBE -> MICRO_5 -> MICRO_20 -> backlog",
        "usage_status": "unavailable",
        "tokens": None,
        "cost": None,
    }


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


__all__ = [
    "EPISODE_ID",
    "SCOPE_HASH",
    "SCOPE_VERSION",
    "WINDOW_END",
    "WINDOW_START",
    "api_certification_summary",
    "build_date_closure",
    "build_direction_closure",
    "build_recovery_disposition",
    "build_root_objects",
    "build_stale_lock_audit",
    "derive_date_v3",
    "derive_direction_v3",
    "file_sha256",
    "root_counts",
]
