"""Apply completed manual source review through the deterministic source gates.

This is intentionally a narrow, resumable adapter around the existing source
registry/candidate pipeline.  A manual ACCEPT is evidence for routing only; it
never writes ``is_verified`` or ``is_enabled`` directly.  All admission state
must be produced by the existing two-probe, deterministic verification,
promotion, and strict-enable functions in :mod:`policydb.source_slots`.

The script writes only run-scoped audit/control artifacts outside the code
repository.  It is safe to re-run with the same ``--output-dir``: completed
review keys and completed candidate actions are read from the checkpoint and
not repeated.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import shutil
import traceback
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import polars as pl

from policydb.crawl.dedup import canonicalize_url
from policydb.parquet_store import read_parquet_snapshot
from policydb.settings import Settings
from policydb.source_slots import (
    audit_525,
    enable_source_strict,
    list_candidates,
    probe_candidates,
    promote_candidate,
    seed_candidates_from_registry,
    upsert_candidates,
    verify_candidates,
)
from policydb.transform.normalization import stable_id

UTC_STAMP = "%Y%m%dT%H%M%SZ"
ACCEPT = "ACCEPT"
REJECT = "REJECT"
NEEDS_RESEARCH = "NEEDS_RESEARCH"
DECISIONS = {ACCEPT, REJECT, NEEDS_RESEARCH}
CHECK_FIELDS = ("official_check", "city_check", "role_check", "stable_list_check")


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


def stamp() -> str:
    return datetime.now(UTC).strftime(UTC_STAMP)


def json_default(value: object) -> str:
    return str(value)


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temp.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=json_default) + "\n",
        encoding="utf-8",
    )
    os.replace(temp, path)


def append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False, default=json_default) + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def truth(value: object) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y"}


def safe_int(value: object) -> int:
    try:
        return int(float(value or 0))
    except (TypeError, ValueError):
        return 0


def canonical(value: object) -> str:
    text = str(value or "").strip()
    return canonicalize_url(text) if text else ""


def current_audit(settings: Settings) -> dict[str, Any]:
    """Refresh the slot materialization audit without seeding or mutating registry."""
    result = audit_525(settings)
    result["observed_at"] = utc_now()
    return result


def latest_completed_review(settings: Settings, explicit: Path | None) -> Path:
    root = settings.outputs / "manual_source_review"
    if explicit:
        path = explicit.resolve()
        if not path.is_dir():
            raise FileNotFoundError(path)
        return path
    if not root.exists():
        raise FileNotFoundError(f"manual review root does not exist: {root}")
    candidates: list[tuple[float, Path]] = []
    for path in root.iterdir():
        checkpoint = path / "MANUAL_REVIEW_CHECKPOINT.json"
        if not path.is_dir() or not checkpoint.exists():
            continue
        try:
            payload = json.loads(checkpoint.read_text(encoding="utf-8-sig"))
        except (OSError, json.JSONDecodeError):
            continue
        if payload.get("status") == "completed":
            candidates.append((path.stat().st_mtime, path))
    if not candidates:
        raise FileNotFoundError("no completed manual review directory found")
    return max(candidates, key=lambda item: item[0])[1]


def load_review_bundle(review_dir: Path) -> tuple[list[dict[str, str]], list[dict[str, str]], dict[str, Any]]:
    queue_path = review_dir / "MANUAL_SOURCE_REVIEW_QUEUE.csv"
    decisions_path = review_dir / "MANUAL_REVIEW_DECISIONS.csv"
    checkpoint_path = review_dir / "MANUAL_REVIEW_CHECKPOINT.json"
    for path in (queue_path, decisions_path, checkpoint_path):
        if not path.exists():
            raise FileNotFoundError(path)
    queue = read_csv(queue_path)
    decisions = read_csv(decisions_path)
    checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8-sig"))
    return queue, decisions, checkpoint


def candidate_proposal_index(settings: Settings, wanted: set[tuple[str, str]]) -> dict[tuple[str, str], dict[str, Any]]:
    """Index historical proposal evidence by slot and canonical URL.

    Proposal evidence is read-only.  The index deliberately keeps the newest
    row for each identity and never treats a proposal row as a formal
    candidate until the caller calls ``upsert_candidates``.
    """
    index: dict[tuple[str, str], dict[str, Any]] = {}
    root = settings.outputs / "autopilot"
    if not root.exists():
        return index
    files = sorted(
        root.glob("*/candidate_proposals.parquet"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    for path in files:
        try:
            frame = pl.read_parquet(path)
        except Exception:
            continue
        if frame.is_empty() or not {"slot_id", "candidate_url"}.issubset(frame.columns):
            continue
        for row in frame.to_dicts():
            key = (str(row.get("slot_id") or ""), canonical(row.get("canonical_url") or row.get("candidate_url")))
            if key not in wanted or key in index:
                continue
            row["proposal_source_path"] = str(path)
            index[key] = row
    return index


def backup_paths(settings: Settings) -> list[Path]:
    paths = [
        settings.curated / "source_candidates.parquet",
        settings.curated / "source_candidate_evidence.parquet",
        settings.curated / "source_registry.parquet",
        settings.curated / "source_requirement_slots.parquet",
        settings.curated / "source_sync_state.parquet",
        settings.root / "data" / "reference" / "source_registry.yaml",
        settings.outputs / "acceptance" / "source_525_audit.csv",
        settings.root / ".env.example",
    ]
    return [path for path in paths if path.exists()]


def make_backup(settings: Settings, output_dir: Path) -> dict[str, Any]:
    backup_dir = output_dir / "pre_apply_backup"
    backup_dir.mkdir(parents=True, exist_ok=True)
    records: list[dict[str, Any]] = []
    for source in backup_paths(settings):
        relative = source.name
        if source.parent == settings.curated:
            relative = str(Path("curated") / source.name)
        elif source.parent == settings.root / "data" / "reference":
            relative = str(Path("data_reference") / source.name)
        elif source.parent == settings.outputs / "acceptance":
            relative = str(Path("acceptance") / source.name)
        target = backup_dir / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)
        records.append({
            "source": str(source),
            "backup": str(target),
            "sha256": sha256(source),
            "size": source.stat().st_size,
        })
    manifest = {"created_at": utc_now(), "records": records}
    write_json(backup_dir / "MANIFEST.json", manifest)
    return manifest


def validate_bundle(
    queue: list[dict[str, str]],
    decisions: list[dict[str, str]],
    checkpoint: dict[str, Any],
    current_rows: list[dict[str, Any]],
    proposal_index: dict[tuple[str, str], dict[str, Any]],
    audit: dict[str, Any],
) -> dict[str, Any]:
    queue_by_key = {str(row.get("review_key") or ""): row for row in queue}
    decision_keys = [str(row.get("review_key") or "") for row in decisions]
    current_by_id = {str(row.get("candidate_id") or ""): row for row in current_rows}
    current_by_key = {
        (str(row.get("slot_id") or ""), canonical(row.get("canonical_url") or row.get("candidate_url"))): row
        for row in current_rows
    }
    errors: list[dict[str, Any]] = []
    duplicate_keys = [key for key, count in Counter(decision_keys).items() if count > 1]
    if len(queue) != len(decisions):
        errors.append({"type": "row_count_mismatch", "queue": len(queue), "decisions": len(decisions)})
    if duplicate_keys:
        errors.append({"type": "duplicate_review_keys", "keys": duplicate_keys})
    missing_keys = sorted(set(queue_by_key) - set(decision_keys))
    extra_keys = sorted(set(decision_keys) - set(queue_by_key))
    if missing_keys:
        errors.append({"type": "missing_decisions", "keys": missing_keys})
    if extra_keys:
        errors.append({"type": "unknown_review_keys", "keys": extra_keys})

    counts: Counter[str] = Counter()
    invalid: list[dict[str, Any]] = []
    identity_rows: list[dict[str, Any]] = []
    for decision in decisions:
        key = str(decision.get("review_key") or "")
        queue_row = queue_by_key.get(key, {})
        final = str(decision.get("final_decision") or "").strip().upper()
        counts[final] += 1
        checks = {field: str(decision.get(field) or "").strip().lower() for field in CHECK_FIELDS}
        if final not in DECISIONS:
            invalid.append({"review_key": key, "type": "invalid_decision", "value": final})
        if final == ACCEPT and any(value != "y" for value in checks.values()):
            invalid.append({"review_key": key, "type": "accept_checks_not_all_y", "checks": checks})
        if final != ACCEPT and all(value == "y" for value in checks.values()):
            invalid.append({"review_key": key, "type": "non_accept_all_checks_y", "decision": final})
        slot_id = str(decision.get("slot_id") or queue_row.get("slot_id") or "")
        candidate_id = str(decision.get("candidate_id") or queue_row.get("candidate_id") or "")
        candidate_url = str(decision.get("candidate_url") or queue_row.get("candidate_url") or "")
        key_by_url = (slot_id, canonical(candidate_url))
        current = current_by_id.get(candidate_id) or current_by_key.get(key_by_url)
        if candidate_id.startswith("proposal:") and not current and key_by_url not in proposal_index:
            invalid.append({
                "review_key": key,
                "type": "proposal_identity_missing",
                "candidate_id": candidate_id,
                "slot_id": slot_id,
                "canonical_url": key_by_url[1],
            })
        identity_rows.append({
            "review_key": key,
            "decision": final,
            "candidate_id": candidate_id,
            "slot_id": slot_id,
            "candidate_url": candidate_url,
            "canonical_url": key_by_url[1],
            "current_candidate_id": current.get("candidate_id") if current else None,
            "current_candidate_verified": bool(current and current.get("is_verified")),
            "current_candidate_enabled": bool(current and current.get("is_enabled")),
            "proposal_source_path": proposal_index.get(key_by_url, {}).get("proposal_source_path"),
            "reviewed_at": decision.get("reviewed_at"),
        })
    return {
        "status": "VALID" if not errors and not invalid else "INVALID",
        "review_dir": None,
        "queue_rows": len(queue),
        "decision_rows": len(decisions),
        "completed_review_keys": len(checkpoint.get("completed_review_keys") or []),
        "checkpoint_status": checkpoint.get("status"),
        "decision_counts": dict(counts),
        "invalid_rows": invalid,
        "errors": errors,
        "identity_rows": identity_rows,
        "current_candidate_rows": len(current_rows),
        "current_audit": audit,
        "proposal_identity_matches": sum(bool(row.get("proposal_source_path")) for row in identity_rows),
        "current_candidate_matches": sum(bool(row.get("current_candidate_id")) for row in identity_rows),
    }


def diagnostic(settings: Settings, review_dir: Path, output_dir: Path) -> dict[str, Any]:
    queue, decisions, checkpoint = load_review_bundle(review_dir)
    current_rows = list_candidates(settings=settings).to_dicts()
    wanted = {
        (str(row.get("slot_id") or ""), canonical(row.get("candidate_url")))
        for row in queue
        if row.get("candidate_url")
    }
    proposals = candidate_proposal_index(settings, wanted)
    audit = current_audit(settings)
    result = validate_bundle(queue, decisions, checkpoint, current_rows, proposals, audit)
    result.update({
        "created_at": utc_now(),
        "review_dir": str(review_dir),
        "queue_path": str(review_dir / "MANUAL_SOURCE_REVIEW_QUEUE.csv"),
        "decisions_path": str(review_dir / "MANUAL_REVIEW_DECISIONS.csv"),
        "checkpoint_path": str(review_dir / "MANUAL_REVIEW_CHECKPOINT.json"),
        "data_root": str(settings.data_root),
        "database": str(settings.database),
        "curated": str(settings.curated),
    })
    write_json(output_dir / "MANUAL_REVIEW_IMPORT_DIAGNOSTIC.json", result)
    return result


def read_checkpoint(output_dir: Path) -> dict[str, Any]:
    path = output_dir / "MANUAL_REVIEW_APPLY_CHECKPOINT.json"
    if not path.exists():
        return {"status": "not_started", "completed_review_keys": [], "events": []}
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"invalid apply checkpoint: {path}") from exc


def save_checkpoint(output_dir: Path, checkpoint: dict[str, Any]) -> None:
    checkpoint["updated_at"] = utc_now()
    write_json(output_dir / "MANUAL_REVIEW_APPLY_CHECKPOINT.json", checkpoint)


def row_for_formal_candidate(
    decision: dict[str, str],
    queue_row: dict[str, str],
    current: dict[str, Any] | None,
    proposal: dict[str, Any] | None,
) -> dict[str, Any]:
    """Build a candidate upsert row without admission flags."""
    base = dict(current or {})
    evidence = dict(proposal or {})
    merged = {**evidence, **queue_row, **base}
    slot_id = str(decision.get("slot_id") or queue_row.get("slot_id") or merged.get("slot_id") or "")
    candidate_url = str(decision.get("candidate_url") or queue_row.get("candidate_url") or merged.get("candidate_url") or "")
    role = str(decision.get("source_role") or queue_row.get("source_role") or merged.get("source_role") or "")
    candidate_kind = str(merged.get("candidate_kind") or "official_entry_candidate")
    candidate_id = str((current or {}).get("candidate_id") or "")
    if not candidate_id or candidate_id.startswith("proposal:"):
        candidate_id = stable_id(slot_id, canonical(candidate_url), candidate_kind, prefix="SRCCAND")
    page_type = str(merged.get("page_type") or "")
    entry_eligible = merged.get("entry_eligible")
    row: dict[str, Any] = {
        "candidate_id": candidate_id,
        "city_id": str(decision.get("city_id") or queue_row.get("city_id") or merged.get("city_id") or ""),
        "source_role": role,
        "candidate_url": candidate_url,
        "candidate_title": merged.get("candidate_title"),
        "page_title": merged.get("page_title"),
        "page_heading": merged.get("page_heading"),
        "breadcrumb": merged.get("breadcrumb"),
        "page_text_excerpt": merged.get("page_text_excerpt"),
        "page_city_evidence": merged.get("page_city_evidence") or merged.get("city_evidence"),
        "page_role_evidence": merged.get("page_role_evidence") or merged.get("role_evidence"),
        "page_agency_evidence": merged.get("page_agency_evidence") or merged.get("agency_evidence"),
        "page_entry_type_evidence": merged.get("page_entry_type_evidence") or merged.get("entry_type_evidence"),
        "page_pagination_evidence": merged.get("page_pagination_evidence"),
        "page_final_url": merged.get("page_final_url") or merged.get("final_url"),
        "page_http_status": merged.get("page_http_status") or merged.get("http_status"),
        "page_network_route": merged.get("page_network_route") or merged.get("network_route"),
        "page_response_sha256": merged.get("page_response_sha256"),
        "discovery_method": "manual_review_import",
        "discovery_evidence_url": merged.get("discovery_evidence_url") or candidate_url,
        "discovery_evidence_text": merged.get("discovery_evidence_text") or merged.get("search_evidence"),
        "official_domain_evidence": merged.get("official_domain_evidence"),
        "city_match_evidence": merged.get("city_match_evidence") or merged.get("city_evidence"),
        "role_match_evidence": merged.get("role_match_evidence") or merged.get("role_evidence"),
        "candidate_kind": candidate_kind,
        "page_type": page_type or None,
        "entry_eligible": entry_eligible if entry_eligible is not None else False,
        "is_official": truth(merged.get("is_official")),
        "is_verified": False,
        "is_enabled": False,
        "manual_review_status": "manual_accept_pending_probe",
        "health_status": str(merged.get("health_status") or "pending"),
        "parser_status": str(merged.get("parser_status") or "pending"),
        "network_route": str(merged.get("network_route") or "unknown"),
        "http_status": merged.get("http_status"),
        "final_url": merged.get("final_url"),
        "pagination_strategy": merged.get("pagination_strategy") or "unknown",
        "health_probe_count": safe_int(merged.get("health_probe_count")),
        "health_probe_success_count": safe_int(merged.get("health_probe_success_count")),
        "probe_evidence_json": merged.get("probe_evidence_json") or "[]",
        "last_checked_at": merged.get("last_checked_at"),
        "notes": f"manual_review_key={decision.get('review_key')}; manual_decision=ACCEPT",
    }
    return row


def apply_reject_or_research(
    settings: Settings,
    decision: dict[str, str],
    queue_row: dict[str, str],
    current: dict[str, Any] | None,
    output_dir: Path,
) -> dict[str, Any]:
    final = str(decision.get("final_decision") or "").strip().upper()
    result: dict[str, Any] = {
        "review_key": decision.get("review_key"),
        "decision": final,
        "candidate_id": decision.get("candidate_id"),
        "slot_id": decision.get("slot_id"),
        "candidate_url": decision.get("candidate_url"),
        "status": "preserved_audit_only",
        "verified_changed": False,
        "enabled_changed": False,
    }
    if final == REJECT and current and not truth(current.get("is_verified")) and not truth(current.get("is_enabled")):
        row = dict(current)
        row["manual_review_status"] = "excluded_manual_reject"
        row["notes"] = f"manual_review_key={decision.get('review_key')}; manual_decision=REJECT"
        # This is an exclusion/audit update, not an admission mutation.  It
        # does not set either strict flag and cannot downgrade a resolved row.
        upsert_candidates([row], settings, authoritative_review=True)
        result["status"] = "candidate_excluded_with_audit"
    append_jsonl(output_dir / "manual_review_decisions_applied.jsonl", {
        **result,
        "reviewer_note": decision.get("reviewer_note"),
        "reviewed_at": decision.get("reviewed_at"),
        "queue_evidence": {
            key: queue_row.get(key)
            for key in ("official_domain_evidence", "city_evidence", "role_evidence", "entry_type_evidence", "page_evidence")
        },
        "recorded_at": utc_now(),
    })
    return result


def apply_one_accept(
    settings: Settings,
    decision: dict[str, str],
    queue_row: dict[str, str],
    current: dict[str, Any] | None,
    proposal: dict[str, Any] | None,
    output_dir: Path,
) -> dict[str, Any]:
    review_key = str(decision.get("review_key") or "")
    result: dict[str, Any] = {
        "review_key": review_key,
        "decision": ACCEPT,
        "slot_id": decision.get("slot_id"),
        "candidate_id_from_review": decision.get("candidate_id"),
        "candidate_url": decision.get("candidate_url"),
        "status": "started",
        "probe": None,
        "verification": None,
        "promotion": None,
        "strict_enable": None,
    }
    append_jsonl(output_dir / "manual_review_apply_events.jsonl", {
        "event": "accept_pipeline_started",
        "review_key": review_key,
        "timestamp": utc_now(),
    })
    formal_row = row_for_formal_candidate(decision, queue_row, current, proposal)
    formal_id = str(formal_row["candidate_id"])
    result["formal_candidate_id"] = formal_id
    # Formal import is always a non-admission upsert.  Ordinary merge preserves
    # any historical true flags, while the new row explicitly carries false.
    upsert_result = upsert_candidates([formal_row], settings, authoritative_review=False)
    result["formal_upsert"] = upsert_result
    append_jsonl(output_dir / "manual_review_apply_events.jsonl", {
        "event": "formal_candidate_upserted",
        "review_key": review_key,
        "candidate_id": formal_id,
        "timestamp": utc_now(),
    })

    # A slot may become resolved through an earlier ACCEPT in this same run.
    # Do not probe duplicate acceptances after strict resolution.
    slot_frame = read_parquet_snapshot(settings.curated / "source_requirement_slots.parquet")
    slot_rows = slot_frame.filter(pl.col("slot_id") == str(decision.get("slot_id")))
    if slot_rows.height == 1 and safe_int(slot_rows[0, "verified_candidate_count"]) > 0:
        result.update({"status": "resolved_by_other_candidate", "skipped_probe": True})
        return result

    probe_result = probe_candidates(
        candidate_ids=[formal_id],
        rounds=2,
        settings=settings,
    )
    result["probe"] = probe_result
    append_jsonl(output_dir / "manual_review_apply_events.jsonl", {
        "event": "double_probe_completed",
        "review_key": review_key,
        "candidate_id": formal_id,
        "probe": {key: value for key, value in probe_result.items() if key != "verification"},
        "timestamp": utc_now(),
    })
    verification = verify_candidates(
        candidate_ids=[formal_id],
        run_id=f"manual_review_{stamp()}",
        settings=settings,
    )
    result["verification"] = verification
    candidate_frame = list_candidates(candidate_id=formal_id, settings=settings)
    if candidate_frame.height != 1 or not truth(candidate_frame[0, "is_verified"]):
        result["status"] = "strict_verification_rejected"
        result["gate_reasons"] = verification.get("reason_code_counts") if isinstance(verification, dict) else None
        append_jsonl(output_dir / "manual_review_apply_events.jsonl", {
            "event": "deterministic_verification_rejected",
            "review_key": review_key,
            "candidate_id": formal_id,
            "reason_code_counts": result.get("gate_reasons"),
            "timestamp": utc_now(),
        })
        return result
    promotion = promote_candidate(formal_id, settings=settings)
    result["promotion"] = promotion
    source_id = str(promotion["source_id"])
    append_jsonl(output_dir / "manual_review_apply_events.jsonl", {
        "event": "candidate_promoted_verified",
        "review_key": review_key,
        "candidate_id": formal_id,
        "source_id": source_id,
        "timestamp": utc_now(),
    })
    try:
        strict_enable = enable_source_strict(source_id, settings=settings)
        result["strict_enable"] = strict_enable
        result["status"] = "strict_enabled"
        append_jsonl(output_dir / "manual_review_apply_events.jsonl", {
            "event": "strict_enable_completed",
            "review_key": review_key,
            "candidate_id": formal_id,
            "source_id": source_id,
            "timestamp": utc_now(),
        })
    except Exception as exc:
        result["status"] = "verified_promoted_enable_rejected"
        result["strict_enable"] = {"error_type": type(exc).__name__, "error_message": str(exc)[:1000]}
        append_jsonl(output_dir / "manual_review_apply_events.jsonl", {
            "event": "strict_enable_rejected",
            "review_key": review_key,
            "candidate_id": formal_id,
            "source_id": source_id,
            "error_type": type(exc).__name__,
            "error_message": str(exc)[:1000],
            "timestamp": utc_now(),
        })
    return result


def apply_reviews(settings: Settings, review_dir: Path, output_dir: Path, *, allow_invalid: bool = False) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    queue, decisions, review_checkpoint = load_review_bundle(review_dir)
    current_rows = list_candidates(settings=settings).to_dicts()
    current_by_id = {str(row.get("candidate_id") or ""): row for row in current_rows}
    current_by_key = {
        (str(row.get("slot_id") or ""), canonical(row.get("canonical_url") or row.get("candidate_url"))): row
        for row in current_rows
    }
    wanted = {(str(row.get("slot_id") or ""), canonical(row.get("candidate_url"))) for row in queue}
    proposal_index = candidate_proposal_index(settings, wanted)
    audit_before = current_audit(settings)
    diag = validate_bundle(queue, decisions, review_checkpoint, current_rows, proposal_index, audit_before)
    diag["review_dir"] = str(review_dir)
    diag["created_at"] = utc_now()
    write_json(output_dir / "MANUAL_REVIEW_IMPORT_DIAGNOSTIC.json", diag)
    if diag["status"] != "VALID" and not allow_invalid:
        raise RuntimeError("manual review import diagnostic is invalid; inspect MANUAL_REVIEW_IMPORT_DIAGNOSTIC.json")

    backup = make_backup(settings, output_dir)
    checkpoint = read_checkpoint(output_dir)
    completed = set(str(key) for key in checkpoint.get("completed_review_keys") or [])
    checkpoint.update({
        "status": "running",
        "review_dir": str(review_dir),
        "started_at": checkpoint.get("started_at") or utc_now(),
        "completed_review_keys": sorted(completed),
        "backup_manifest": backup,
        "audit_before": audit_before,
    })
    save_checkpoint(output_dir, checkpoint)
    queue_by_key = {str(row.get("review_key") or ""): row for row in queue}
    results: list[dict[str, Any]] = []
    for decision in decisions:
        review_key = str(decision.get("review_key") or "")
        if review_key in completed:
            continue
        queue_row = queue_by_key.get(review_key, {})
        slot_id = str(decision.get("slot_id") or queue_row.get("slot_id") or "")
        candidate_id = str(decision.get("candidate_id") or queue_row.get("candidate_id") or "")
        url_key = (slot_id, canonical(decision.get("candidate_url") or queue_row.get("candidate_url")))
        current = current_by_id.get(candidate_id) or current_by_key.get(url_key)
        proposal = proposal_index.get(url_key)
        final = str(decision.get("final_decision") or "").strip().upper()
        try:
            if final == ACCEPT:
                item = apply_one_accept(settings, decision, queue_row, current, proposal, output_dir)
            else:
                item = apply_reject_or_research(settings, decision, queue_row, current, output_dir)
            item["reviewed_at"] = decision.get("reviewed_at")
            results.append(item)
            append_jsonl(output_dir / "manual_review_results.jsonl", {**item, "recorded_at": utc_now()})
            completed.add(review_key)
            checkpoint["completed_review_keys"] = sorted(completed)
            checkpoint["last_result"] = item
            checkpoint["processed_count"] = len(completed)
            save_checkpoint(output_dir, checkpoint)
        except Exception as exc:
            item = {
                "review_key": review_key,
                "decision": final,
                "slot_id": slot_id,
                "candidate_id_from_review": candidate_id,
                "status": "error",
                "error_type": type(exc).__name__,
                "error_message": str(exc)[:2000],
                "traceback": traceback.format_exc(limit=12),
            }
            append_jsonl(output_dir / "manual_review_results.jsonl", {**item, "recorded_at": utc_now()})
            checkpoint.update({"status": "blocked", "last_error": item})
            save_checkpoint(output_dir, checkpoint)
            raise
    audit_after = current_audit(settings)
    summary = {
        "status": "completed",
        "completed_at": utc_now(),
        "review_dir": str(review_dir),
        "output_dir": str(output_dir),
        "review_counts": dict(Counter(str(item.get("decision") or "") for item in results)),
        "result_status_counts": dict(Counter(str(item.get("status") or "") for item in results)),
        "strict_verified_added": max(0, int(audit_after.get("slots_verified", 0)) - int(audit_before.get("slots_verified", 0))),
        "strict_enabled_added": max(0, int(audit_after.get("slots_enabled", 0)) - int(audit_before.get("slots_enabled", 0))),
        "audit_before": audit_before,
        "audit_after": audit_after,
        "results": results,
        "manual_review_checkpoint": review_checkpoint,
        "no_direct_admission_write": True,
    }
    write_json(output_dir / "MANUAL_REVIEW_APPLY_RESULT.json", summary)
    checkpoint.update({"status": "completed", "completed_at": summary["completed_at"], "audit_after": audit_after})
    save_checkpoint(output_dir, checkpoint)
    return summary


def repair_pending_enabled(settings: Settings, output_dir: Path) -> dict[str, Any]:
    """Reconcile historical enabled registry rows through the strict pipeline.

    This does not disable or directly rewrite any source flag.  It materializes
    the existing registry entry as a formal candidate, probes it twice, lets
    ``verify_candidates`` decide admission, and then re-enters the normal
    promotion/strict-enable path.  The operation is deliberately serialized.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    before = current_audit(settings)
    slots = read_parquet_snapshot(settings.curated / "source_requirement_slots.parquet")
    pending = slots.filter(
        (pl.col("enabled_source_count") > 0)
        & (pl.col("verified_enabled_source_count") == 0)
    ).to_dicts()
    results: list[dict[str, Any]] = []
    for slot in pending:
        slot_result: dict[str, Any] = {
            "slot_id": slot.get("slot_id"),
            "city_id": slot.get("city_id"),
            "source_role": slot.get("source_role"),
            "source_id": slot.get("preferred_source_id"),
            "status": "started",
        }
        try:
            seeded = seed_candidates_from_registry(
                settings,
                source_id=str(slot.get("preferred_source_id") or ""),
                slot_id=str(slot.get("slot_id") or ""),
                write=True,
            )
            planned = [
                item
                for item in seeded.get("planned_candidates") or []
                if str(item.get("slot_id")) == str(slot.get("slot_id"))
            ]
            slot_result["seed"] = {
                "planned_count": len(planned),
                "candidate_ids": [str(item.get("candidate_id")) for item in planned],
            }
            if not planned:
                slot_result["status"] = "no_registry_candidate"
                results.append(slot_result)
                append_jsonl(output_dir / "pending_enabled_repair.jsonl", {**slot_result, "recorded_at": utc_now()})
                continue
            candidate_results: list[dict[str, Any]] = []
            for planned_row in planned:
                candidate_id = str(planned_row.get("candidate_id") or "")
                item: dict[str, Any] = {"candidate_id": candidate_id, "status": "started"}
                try:
                    item["probe"] = probe_candidates(
                        candidate_ids=[candidate_id], rounds=2, settings=settings
                    )
                    item["verification"] = verify_candidates(
                        candidate_ids=[candidate_id],
                        run_id=f"pending_enabled_repair_{stamp()}",
                        settings=settings,
                    )
                    current = list_candidates(candidate_id=candidate_id, settings=settings)
                    if current.height == 1 and truth(current[0, "is_verified"]):
                        item["promotion"] = promote_candidate(candidate_id, settings=settings)
                        source_id = str(item["promotion"]["source_id"])
                        try:
                            item["strict_enable"] = enable_source_strict(source_id, settings=settings)
                            item["status"] = "strict_enabled"
                        except Exception as exc:
                            item["status"] = "verified_promoted_enable_rejected"
                            item["strict_enable"] = {
                                "error_type": type(exc).__name__,
                                "error_message": str(exc)[:1000],
                            }
                    else:
                        item["status"] = "strict_verification_rejected"
                except Exception as exc:
                    item["status"] = "error"
                    item["error_type"] = type(exc).__name__
                    item["error_message"] = str(exc)[:2000]
                candidate_results.append(item)
                append_jsonl(output_dir / "pending_enabled_repair.jsonl", {
                    "slot_id": slot.get("slot_id"),
                    "source_id": slot.get("preferred_source_id"),
                    **item,
                    "recorded_at": utc_now(),
                })
            slot_result["candidate_results"] = candidate_results
            slot_result["status"] = (
                "strict_enabled"
                if any(item.get("status") == "strict_enabled" for item in candidate_results)
                else "strict_verification_rejected"
            )
        except Exception as exc:
            slot_result.update({
                "status": "error",
                "error_type": type(exc).__name__,
                "error_message": str(exc)[:2000],
            })
        results.append(slot_result)
        write_json(output_dir / "PENDING_ENABLED_REPAIR_PROGRESS.json", {
            "updated_at": utc_now(),
            "processed_slots": len(results),
            "total_pending_slots": len(pending),
            "last_result": slot_result,
        })
    after = current_audit(settings)
    summary = {
        "status": "completed",
        "created_at": utc_now(),
        "before": before,
        "after": after,
        "pending_slots_before": len(pending),
        "strict_enabled_slots": sum(item.get("status") == "strict_enabled" for item in results),
        "result_status_counts": dict(Counter(str(item.get("status") or "") for item in results)),
        "results": results,
        "no_direct_admission_write": True,
    }
    write_json(output_dir / "PENDING_ENABLED_REPAIR_RESULT.json", summary)
    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("diagnose", "apply", "repair-pending"))
    parser.add_argument("--review-dir", type=Path)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--allow-invalid", action="store_true")
    parser.add_argument("--data-root", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.data_root:
        os.environ["CRPD_DATA_ROOT"] = str(args.data_root.resolve())
    settings = Settings.discover()
    review_dir = latest_completed_review(settings, args.review_dir)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    if args.command == "diagnose":
        result = diagnostic(settings, review_dir, args.output_dir)
    elif args.command == "apply":
        result = apply_reviews(settings, review_dir, args.output_dir, allow_invalid=args.allow_invalid)
    else:
        result = repair_pending_enabled(settings, args.output_dir)
    print(json.dumps({
        "status": result.get("status"),
        "review_dir": str(review_dir),
        "output_dir": str(args.output_dir),
        "decision_counts": result.get("decision_counts") or result.get("review_counts"),
        "audit_before": result.get("audit_before") or result.get("current_audit"),
        "audit_after": result.get("audit_after"),
    }, ensure_ascii=False, indent=2, default=json_default))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
