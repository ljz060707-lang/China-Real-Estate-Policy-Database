"""Bounded recovery for existing candidates only.

This helper deliberately does not discover, rank, or upsert candidates.  It
reuses the production probe, deterministic verification, promotion, and strict
enablement functions for an explicitly supplied candidate list.
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from policydb.settings import Settings
from policydb.source_slots import (
    build_requirement_slots,
    enable_source_strict,
    list_candidates,
    probe_candidates,
    promote_candidate,
    verify_candidates,
)


def now() -> str:
    return datetime.now(UTC).isoformat()


def atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, default=str) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def append_jsonl(path: Path, value: Any) -> None:
    with path.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(value, ensure_ascii=False, default=str) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--candidate-id", action="append", required=True)
    parser.add_argument("--rounds", type=int, default=2)
    args = parser.parse_args()

    settings = Settings.discover()
    run_dir = Path(args.run_dir).resolve()
    run_dir.mkdir(parents=True, exist_ok=False)
    status_path = run_dir / "current_status.json"
    results_path = run_dir / "candidate_recovery_results.jsonl"
    state: dict[str, Any] = {
        "run_id": args.run_id,
        "mode": "FAST_TRACK_TO_400",
        "strategy": "existing_candidate_probe_resume",
        "apply": True,
        "selected_candidate_ids": list(args.candidate_id),
        "selected_slots": [],
        "current_step": "planning",
        "processed": 0,
        "verified_added": 0,
        "enabled_added": 0,
        "latest_error": None,
        "last_progress_at": now(),
    }
    atomic_json(status_path, state)

    initial = list_candidates(settings=settings)
    selected_rows = [
        row
        for row in initial.to_dicts()
        if str(row.get("candidate_id") or "") in set(args.candidate_id)
    ]
    state["selected_slots"] = sorted(
        {str(row.get("slot_id") or "") for row in selected_rows if row.get("slot_id")}
    )
    state["current_step"] = "probe_verify_promote_enable"
    atomic_json(status_path, state)

    for index, candidate_id in enumerate(args.candidate_id, start=1):
        item: dict[str, Any] = {
            "run_id": args.run_id,
            "candidate_id": candidate_id,
            "started_at": now(),
            "status": "started",
        }
        try:
            item["probe"] = probe_candidates(
                candidate_ids=[candidate_id], rounds=args.rounds, settings=settings
            )
            item["verification"] = verify_candidates(
                candidate_id=candidate_id, run_id=args.run_id, settings=settings
            )
            current = list_candidates(candidate_id=candidate_id, settings=settings)
            if current.height != 1:
                raise RuntimeError(f"candidate disappeared after verification: {candidate_id}")
            row = current.row(0, named=True)
            item["is_verified"] = bool(row.get("is_verified"))
            item["is_enabled"] = bool(row.get("is_enabled"))
            if item["is_verified"]:
                promotion = promote_candidate(candidate_id, settings=settings)
                item["promotion"] = promotion
                source_id = str(promotion.get("source_id") or "")
                if not source_id:
                    raise RuntimeError("promotion returned no source_id")
                item["enablement"] = enable_source_strict(source_id, settings=settings)
                item["enabled"] = True
                state["verified_added"] = int(state["verified_added"]) + 1
                state["enabled_added"] = int(state["enabled_added"]) + 1
            else:
                item["enabled"] = False
            item["status"] = "completed"
        except Exception as exc:  # preserve the failure beside the audit trail
            item.update(
                {
                    "status": "failed",
                    "error_type": type(exc).__name__,
                    "error_message": str(exc)[:1000],
                }
            )
            state["latest_error"] = {
                "candidate_id": candidate_id,
                "error_type": type(exc).__name__,
                "error_message": str(exc)[:500],
            }
        item["completed_at"] = now()
        append_jsonl(results_path, item)
        state["processed"] = index
        state["last_progress_at"] = now()
        atomic_json(status_path, state)

    state["current_step"] = "audit_525"
    state["audit_after"] = build_requirement_slots(settings)
    state["completed_at"] = now()
    atomic_json(status_path, state)
    atomic_json(run_dir / "run_summary.json", state)
    return 0 if not state.get("latest_error") else 2


if __name__ == "__main__":
    raise SystemExit(main())
