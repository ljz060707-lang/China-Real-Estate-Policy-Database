"""Read-only diagnostics and routing for source-completion recovery.

This module consumes historical run evidence.  It never edits the source
registry, candidate registry, slot state or verification columns.  Recovery
classes are plans for the next bounded run, not admission decisions.
"""

from __future__ import annotations

import json
import re
from collections import Counter
from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import polars as pl

from policydb.parquet_store import atomic_write_parquet, read_parquet_snapshot
from policydb.settings import Settings
from policydb.source_completion import build_slot_work_queue

RECOVERY_CLASSES = (
    "A_EXACT_LOCAL_AGENCY_EXPECTED",
    "B_PROVINCIAL_CENTRALIZED_AUTHORITY",
    "C_MUNICIPAL_PORTAL_SUBSTITUTE",
    "D_AGENCY_RENAMED_OR_MERGED",
    "E_DOMAIN_MIGRATED_OR_REDIRECTED",
    "F_EXISTING_CANDIDATE_EVIDENCE_INCOMPLETE",
    "G_NETWORK_OR_TLS_RECOVERABLE",
    "H_JS_OR_API_DRIVEN_LIST",
    "I_ROLE_NOT_INDEPENDENTLY_ESTABLISHED",
    "J_CROSS_JURISDICTION_CONFLICT",
    "K_MANUAL_OFFICIAL_RESEARCH_REQUIRED",
    "L_DUPLICATE_OR_SOURCE_BUNDLE_MERGE",
)

_BATCH_RE = re.compile(r"SOURCE525_RESEARCH20_(\d{8}T\d{6}Z)$")
_NETWORK_MARKERS = (
    "network",
    "tls",
    "timeout",
    "http_not_200",
    "403",
    "429",
    "connection",
    "ssl",
    "proxy",
)
_RENAME_MARKERS = ("rename", "renamed", "merged", "旧称", "更名", "撤并", "合并")
_MIGRATION_MARKERS = ("redirect", "migrat", "old_domain", "旧域名", "域名迁移")
_JS_MARKERS = ("ajax", "api", "json", "javascript", "dynamic", "query", "app")
_CONFLICT_MARKERS = ("conflict", "mismatch", "wrong", "jurisdiction", "冲突", "不一致")


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _read_frame(path: Path) -> pl.DataFrame:
    if not path.exists():
        return pl.DataFrame()
    try:
        return read_parquet_snapshot(path)
    except Exception:
        try:
            return pl.read_parquet(path)
        except Exception:
            return pl.DataFrame()


def _official(url: str) -> bool:
    host = (urlsplit(url).hostname or "").lower().removeprefix("www.")
    return host == "gov.cn" or host.endswith(".gov.cn")


def _domain(url: str) -> str:
    return (urlsplit(url).hostname or "").lower().removeprefix("www.")


def _flatten_reasons(frame: pl.DataFrame) -> Counter[str]:
    counts: Counter[str] = Counter()
    if frame.is_empty() or "prefilter_reason_codes" not in frame.columns:
        return counts
    for value in frame["prefilter_reason_codes"].to_list():
        if isinstance(value, list):
            counts.update(str(item) for item in value if item)
        elif value:
            try:
                parsed = json.loads(str(value))
                counts.update(str(item) for item in parsed if item)
            except json.JSONDecodeError:
                counts.update(item.strip() for item in str(value).split(",") if item.strip())
    return counts


def _tail_zero_slots(summary: dict[str, Any], *, limit: int = 12) -> set[str]:
    tail: list[str] = []
    for item in reversed(summary.get("slot_results") or []):
        applied = int(item.get("applied_candidates") or 0)
        probed = int(item.get("probed_candidates") or 0)
        review = int(item.get("human_review") or 0)
        if applied or probed or review:
            break
        if item.get("slot_id"):
            tail.append(str(item["slot_id"]))
        if len(tail) >= limit:
            break
    return set(tail)


def batch_directories(outputs_root: Path, *, limit: int = 5) -> list[Path]:
    """Return the latest timestamped research batches, excluding recovery labels."""

    candidates: list[tuple[datetime, Path]] = []
    for path in outputs_root.iterdir() if outputs_root.exists() else []:
        if not path.is_dir() or "RECOVERY" in path.name or not (path / "run_summary.json").exists():
            continue
        match = _BATCH_RE.search(path.name)
        if not match:
            continue
        try:
            timestamp = datetime.strptime(match.group(1), "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC)
        except ValueError:
            continue
        candidates.append((timestamp, path))
    candidates.sort(key=lambda item: item[0])
    return [path for _, path in candidates[-limit:]]


def _slot_evidence(run_dir: Path, slot_id: str) -> dict[str, Any]:
    slot_dir = run_dir / "slot_runs" / slot_id
    proposal = _read_frame(slot_dir / "candidate_proposals.parquet")
    prefilter = _read_frame(slot_dir / "candidate_prefilter_before_enrichment.parquet")
    enrichment = _read_frame(slot_dir / "evidence_enrichment.parquet")
    slot_summary = _read_json(slot_dir / "run_summary.json")
    cost_summary = _read_json(slot_dir / "ai_cost_summary.json")
    urls = []
    titles = []
    if proposal.height and "candidate_url" in proposal.columns:
        urls = [str(value) for value in proposal["candidate_url"].to_list() if value]
        if "candidate_title" in proposal.columns:
            titles = [str(value) for value in proposal["candidate_title"].to_list() if value]
    domains = {_domain(url) for url in urls if _domain(url)}
    official_count = sum(_official(url) for url in urls)
    reason_counts = _flatten_reasons(prefilter)
    status_counts = Counter()
    if prefilter.height and "prefilter_status" in prefilter.columns:
        status_counts.update(str(value) for value in prefilter["prefilter_status"].to_list() if value)
    enrichment_success = 0
    enrichment_failure = 0
    if enrichment.height and "status" in enrichment.columns:
        enrichment_success = sum(str(value) in {"completed", "derived_same_domain_link"} for value in enrichment["status"].to_list())
        enrichment_failure = sum(str(value) == "failed" for value in enrichment["status"].to_list())
    search_evidence = _read_frame(slot_dir / "search_evidence.parquet")
    search_errors = []
    if search_evidence.height and "error_type" in search_evidence.columns:
        search_errors = [str(value) for value in search_evidence["error_type"].to_list() if value]
    run_mode = str(slot_summary.get("effective_discovery_mode") or "")
    proposal_count = int(proposal.height or slot_summary.get("candidate_proposals") or 0)
    if proposal_count == 0:
        if int(cost_summary.get("ai_calls") or 0) == 0 and run_mode == "SEARCH_ONLY":
            dominant = "ai_budget_exhausted_after_previous_slots"
        elif search_errors:
            dominant = "search_provider_error"
        else:
            dominant = "no_search_result"
    else:
        dominant = reason_counts.most_common(1)[0][0] if reason_counts else "deterministic_prefilter_or_no_entry_evidence"
    best_url = next((url for url in urls if _official(url)), urls[0] if urls else None)
    best_title = titles[0] if titles else None
    return {
        "proposal_count": proposal_count,
        "unique_domain_count": len(domains),
        "official_candidate_count": int(official_count),
        "evidence_enrichment_count": int(status_counts.get("evidence_enrichment_probe", 0)),
        "enrichment_success_count": int(enrichment_success),
        "enrichment_failure_count": int(enrichment_failure),
        "selected_top3_count": int(slot_summary.get("applied_candidates") or 0),
        "applied_count": int(slot_summary.get("applied_candidates") or 0),
        "probe_count": int(slot_summary.get("probed_candidates") or 0),
        "city_evidence_fail_count": int(reason_counts.get("city_evidence_missing", 0)),
        "role_evidence_fail_count": int(reason_counts.get("role_evidence_missing", 0)),
        "detail_page_fail_count": int(reason_counts.get("detail_or_legal_page", 0) + reason_counts.get("policy_detail_or_content", 0)),
        "network_fail_count": int(sum(reason_counts.get(marker, 0) for marker in _NETWORK_MARKERS) + len(search_errors)),
        "duplicate_fail_count": int(sum(value for key, value in reason_counts.items() if "duplicate" in key.lower())),
        "manual_review_count": int(slot_summary.get("human_review") or 0),
        "best_candidate_url": best_url,
        "best_candidate_title": best_title,
        "dominant_failure_reason": dominant,
        "search_error_types": sorted(set(search_errors)),
        "effective_discovery_mode": run_mode,
        "ai_calls": int(cost_summary.get("ai_calls") or slot_summary.get("ai_calls") or 0),
        "search_calls": int(slot_summary.get("search_calls") or 0),
        "prefilter_reason_counts": dict(reason_counts),
    }


def _strategy_for(row: dict[str, Any]) -> str:
    reason = str(row.get("dominant_failure_reason") or "")
    reasons = set(row.get("prefilter_reason_counts", {}).keys())
    if any(marker in reason.lower() for marker in _NETWORK_MARKERS) or row.get("network_fail_count", 0):
        return "retry direct official URL with browser headers after cooldown; preserve failure evidence"
    if row.get("detail_page_fail_count", 0):
        return "exclude detail/PDF results and route to same-domain list, sitemap or catalogue"
    if row.get("city_evidence_fail_count", 0) and row.get("role_evidence_fail_count", 0):
        return "page-enrich official weak candidates, then expand same-domain policy links"
    if "role_evidence_missing" in reasons:
        return "route through agency aliases, portal directory and provincial/vertical authority"
    if reason == "ai_budget_exhausted_after_previous_slots":
        return "resume from checkpoint with a new bounded AI budget; do not re-spend unchanged queries"
    if reason == "search_provider_error":
        return "retry the recorded provider errors with cooldown and an approved fallback"
    return "run levelled official discovery and retain all search evidence before deterministic prefilter"


def classify_recovery_slot(row: dict[str, Any], *, now: datetime | None = None) -> dict[str, Any]:
    """Assign one deterministic recovery route without asserting source absence."""

    now = now or datetime.now(UTC)
    work_status = str(row.get("work_status") or "").lower()
    text = " ".join(str(row.get(key) or "") for key in ("dominant_failure_reason", "best_candidate_url", "failure_gates", "recommended_action")).lower()
    rename_text = " ".join(
        str(row.get(key) or "")
        for key in ("dominant_failure_reason", "failure_gates", "matched_historical_alias", "alias_source", "reason")
    ).lower()
    conflict_text = " ".join(
        str(row.get(key) or "")
        for key in ("dominant_failure_reason", "failure_gates", "reason", "work_status")
    ).lower()
    source_role = str(row.get("source_role") or "")
    proposal_count = int(row.get("proposal_count") or 0)
    recovery_class = "K_MANUAL_OFFICIAL_RESEARCH_REQUIRED"
    reason = "no deterministic recovery route established from retained evidence"
    priority = 90
    if any(marker in conflict_text for marker in _CONFLICT_MARKERS) or bool(row.get("has_cross_jurisdiction_conflict")):
        recovery_class, reason, priority = "J_CROSS_JURISDICTION_CONFLICT", "city or jurisdiction evidence conflicts", 80
    elif work_status == "blocked_role_conflict" or (
        int(row.get("role_evidence_fail_count") or 0) > 0
        and int(row.get("city_evidence_fail_count") or 0) == 0
        and proposal_count > 0
    ):
        recovery_class, reason, priority = "I_ROLE_NOT_INDEPENDENTLY_ESTABLISHED", "role evidence is not independently established", 75
    elif any(marker in rename_text for marker in _RENAME_MARKERS):
        recovery_class, reason, priority = "D_AGENCY_RENAMED_OR_MERGED", "institution rename/merge evidence requires historical alias routing", 65
    elif any(marker in text for marker in _MIGRATION_MARKERS):
        recovery_class, reason, priority = "E_DOMAIN_MIGRATED_OR_REDIRECTED", "historical or redirected official domain requires bounded redirect evidence", 60
    elif row.get("duplicate_fail_count", 0) > 0:
        recovery_class, reason, priority = "L_DUPLICATE_OR_SOURCE_BUNDLE_MERGE", "candidate evidence is duplicated within a source bundle", 70
    elif any(marker in text for marker in _JS_MARKERS):
        recovery_class, reason, priority = "H_JS_OR_API_DRIVEN_LIST", "official list appears to require a JS/API-aware parser path", 55
    elif row.get("network_fail_count", 0) > 0:
        recovery_class, reason, priority = "G_NETWORK_OR_TLS_RECOVERABLE", "recorded network/TLS/provider failure may be retried directly", 50
    elif proposal_count > 0 and (row.get("city_evidence_fail_count", 0) or row.get("role_evidence_fail_count", 0)):
        recovery_class, reason, priority = "F_EXISTING_CANDIDATE_EVIDENCE_INCOMPLETE", "official candidate exists but page city/role evidence is incomplete", 20
    elif source_role == "municipal_government" or "portal" in text or "zwfw" in text:
        recovery_class, reason, priority = "C_MUNICIPAL_PORTAL_SUBSTITUTE", "municipal portal may provide the role through an official directory", 30
    elif ".gov.cn" in text and source_role in {"provident_fund_center", "natural_resources_department", "government_gazette"}:
        recovery_class, reason, priority = "B_PROVINCIAL_CENTRALIZED_AUTHORITY", "vertical or provincial official authority may cover the city", 35
    elif proposal_count == 0 and work_status in {"no_candidate_discoverable", "no_candidate_manual_research", "failed_recoverable"}:
        recovery_class, reason, priority = "A_EXACT_LOCAL_AGENCY_EXPECTED", "no source was proven absent; search local agency and portal directories", 40
    if recovery_class == "K_MANUAL_OFFICIAL_RESEARCH_REQUIRED" and work_status in {"no_candidate_manual_research", "human_review"}:
        priority = 100
    cooldown = None
    if row.get("consecutive_zero_yield", 0) or row.get("dominant_failure_reason") in {"search_provider_error", "ai_budget_exhausted_after_previous_slots"}:
        cooldown = (now + timedelta(hours=1)).isoformat()
    return {
        **row,
        "recovery_class": recovery_class,
        "reason": reason,
        "evidence_ids": json.dumps([item for item in (row.get("slot_id"), row.get("best_candidate_url"), row.get("dominant_failure_reason")) if item], ensure_ascii=False),
        "next_strategy": _strategy_for(row),
        "retry_priority": priority,
        "cooldown_until": cooldown,
    }


def build_zero_yield_diagnostics(
    settings: Settings,
    *,
    run_dirs: Iterable[Path] | None = None,
    latest_zero_limit: int = 12,
) -> tuple[pl.DataFrame, pl.DataFrame, dict[str, Any]]:
    """Build diagnostics for the latest five batches and remaining slots."""

    selected_runs = list(run_dirs or batch_directories(settings.outputs / "autopilot", limit=5))
    queue = build_slot_work_queue(settings)
    queue_by_slot = {str(row["slot_id"]): row for row in queue.iter_rows(named=True)}
    latest_zero: set[str] = set()
    if selected_runs:
        latest_zero = _tail_zero_slots(_read_json(selected_runs[-1] / "run_summary.json"), limit=latest_zero_limit)
    rows: list[dict[str, Any]] = []
    for run_dir in selected_runs:
        summary = _read_json(run_dir / "run_summary.json")
        for item in summary.get("slot_results") or []:
            slot_id = str(item.get("slot_id") or "")
            if not slot_id:
                continue
            base = queue_by_slot.get(slot_id, {"slot_id": slot_id})
            evidence = _slot_evidence(run_dir, slot_id)
            row = {
                "run_id": run_dir.name,
                "run_timestamp": run_dir.name.rsplit("_", 1)[-1],
                "slot_id": slot_id,
                "city_id": base.get("city_id"),
                "city_name": base.get("city_name"),
                "province_name": base.get("province_name"),
                "source_role": base.get("source_role"),
                "current_status": base.get("work_status"),
                "candidate_count_current": base.get("candidate_count"),
                **evidence,
                "is_latest_zero_yield_tail": slot_id in latest_zero,
                "consecutive_zero_yield": 1 if slot_id in latest_zero else 0,
                "last_attempt_at": _read_json(run_dir / "current_status.json").get("last_progress_at"),
                "attempt_count": 1,
                "last_query_hash": None,
                "candidate_set_hash": None,
                "enrichment_set_hash": None,
            }
            rows.append(classify_recovery_slot(row))
    diagnostic = pl.DataFrame(rows, infer_schema_length=None) if rows else pl.DataFrame()
    # Keep one latest evidence row per slot for the remaining-slot recovery map.
    latest_by_slot: dict[str, dict[str, Any]] = {}
    for row in rows:
        current = latest_by_slot.get(str(row["slot_id"]))
        if current is None or str(row.get("run_timestamp") or "") > str(current.get("run_timestamp") or ""):
            latest_by_slot[str(row["slot_id"])] = row
    remaining: list[dict[str, Any]] = []
    for base in queue.iter_rows(named=True):
        slot_id = str(base["slot_id"])
        if int(base.get("verified_candidate_count") or 0) > 0 or int(base.get("enabled_source_count") or 0) > 0:
            continue
        evidence = latest_by_slot.get(slot_id, {})
        remaining.append(
            classify_recovery_slot(
                {
                    **base,
                    **{key: value for key, value in evidence.items() if key not in {"slot_id", "city_id", "city_name", "province_name", "source_role"}},
                    "proposal_count": evidence.get("proposal_count", base.get("candidate_count", 0)),
                    "dominant_failure_reason": evidence.get("dominant_failure_reason", "no_historical_batch_evidence"),
                    "prefilter_reason_counts": evidence.get("prefilter_reason_counts", {}),
                }
            )
        )
    recovery = pl.DataFrame(remaining, infer_schema_length=None) if remaining else pl.DataFrame()
    reason_counts = Counter(str(row.get("dominant_failure_reason") or "unknown") for row in rows)
    class_counts = Counter(str(row.get("recovery_class") or "unknown") for row in remaining)
    meta = {
        "generated_at": datetime.now(UTC).isoformat(),
        "batch_dirs": [str(path) for path in selected_runs],
        "latest_zero_yield_tail_slots": sorted(latest_zero),
        "diagnostic_rows": len(rows),
        "remaining_recovery_rows": len(remaining),
        "reason_counts": dict(reason_counts),
        "recovery_class_counts": dict(class_counts),
    }
    return diagnostic, recovery, meta


def manual_research_queue(recovery: pl.DataFrame) -> pl.DataFrame:
    """Retain only decisions that cannot be safely automated."""

    if recovery.is_empty():
        return recovery
    keep = {"D_AGENCY_RENAMED_OR_MERGED", "I_ROLE_NOT_INDEPENDENTLY_ESTABLISHED", "J_CROSS_JURISDICTION_CONFLICT", "K_MANUAL_OFFICIAL_RESEARCH_REQUIRED"}
    rows = []
    for row in recovery.iter_rows(named=True):
        if row.get("recovery_class") not in keep:
            continue
        rows.append(
            {
                "slot_id": row.get("slot_id"),
                "city_id": row.get("city_id"),
                "city_name": row.get("city_name"),
                "source_role": row.get("source_role"),
                "options": json.dumps([row.get("best_candidate_url"), "official portal substitute", "provincial/vertical authority", "defer"], ensure_ascii=False),
                "evidence": row.get("evidence_ids"),
                "conflict": row.get("reason"),
                "question": "Which official entry should be researched or retained for this required role, and what evidence resolves the ambiguity?",
                "recommendation": row.get("next_strategy"),
                "impact": "Decision controls only the candidate research route; deterministic verification remains mandatory.",
                "recovery_class": row.get("recovery_class"),
                "researched_at": datetime.now(UTC).isoformat(),
            }
        )
    return pl.DataFrame(rows, infer_schema_length=None) if rows else pl.DataFrame()


def write_zero_yield_recovery(settings: Settings, *, output: Path | None = None) -> dict[str, Any]:
    """Publish diagnostic and recovery artifacts without mutating source state."""

    output = output or settings.outputs / "acceptance" / f"source_completion_zero_yield_recovery_{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}"
    output.mkdir(parents=True, exist_ok=False)
    diagnostic, recovery, meta = build_zero_yield_diagnostics(settings)
    atomic_write_parquet(diagnostic, output / "ZERO_YIELD_SLOT_DIAGNOSTIC.parquet", {"job_id": "zero-yield-diagnostic"})
    atomic_write_parquet(recovery, output / "RECOVERY_CLASSIFICATION.parquet", {"job_id": "source-recovery-classification"})
    manual = manual_research_queue(recovery)
    atomic_write_parquet(manual, output / "MANUAL_OFFICIAL_RESEARCH_QUEUE.parquet", {"job_id": "manual-official-research-queue"})
    (output / "ZERO_YIELD_REASON_COUNTS.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    class_counts = meta.get("recovery_class_counts", {})
    lines = [
        "# CRPD zero-yield recovery plan",
        "",
        f"Generated: {meta['generated_at']}",
        f"Historical batches inspected: {len(meta['batch_dirs'])}",
        f"Latest zero-yield tail: {len(meta['latest_zero_yield_tail_slots'])} slots",
        f"Remaining recovery rows: {meta['remaining_recovery_rows']}",
        "",
        "## Recovery classes",
        "",
    ]
    lines.extend(f"- {name}: {int(class_counts.get(name, 0))}" for name in RECOVERY_CLASSES)
    lines.extend(
        [
            "",
            "## Rules",
            "",
            "- Search and AI output remains evidence; no row in this plan is verified or enabled.",
            "- Existing candidates are enriched before a new AI call is considered.",
            "- Same-domain links are retained as page-enrichment evidence and still require URL, city, role, parser, pagination and two-probe gates.",
            "- A lack of an exact local agency result is not evidence that the required source is absent.",
            "- Manual queue rows are limited to rename, role/jurisdiction conflict and unresolved official research decisions.",
        ]
    )
    (output / "ZERO_YIELD_RECOVERY_PLAN.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return {"output": str(output), **meta, "manual_review_rows": manual.height}
