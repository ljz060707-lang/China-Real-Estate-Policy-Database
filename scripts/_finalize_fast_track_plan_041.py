"""Finalize a completed bounded domain batch into the fast-track plan assets.

This updates only the plan's append-only attempt ledger and its priority
metadata.  It never mutates source candidates, registry rows, or slot status.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from policydb.parquet_store import atomic_write_parquet


def now() -> str:
    return datetime.now(UTC).isoformat()


def digest(value: object) -> str:
    return hashlib.sha256(str(value).encode("utf-8")).hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--plan-dir", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--strategy", required=True)
    args = parser.parse_args()

    plan = Path(args.plan_dir).resolve()
    run_dir = plan / "domain_batches" / args.run_id
    status = json.loads((run_dir / "current_status.json").read_text(encoding="utf-8"))
    selected = [str(value) for value in status.get("selected_slots") or []]
    if not selected:
        raise RuntimeError("batch has no selected slots")

    backup = plan / "asset_backups" / f"before_domain_batch_{args.run_id}"
    backup.mkdir(parents=True, exist_ok=False)
    asset_names = [
        "FAST_TRACK_PRIORITY_QUEUE.parquet",
        "SLOT_ATTEMPT_LEDGER.parquet",
        "CITY_OFFICIAL_DOMAIN_INVENTORY.parquet",
    ]
    for name in asset_names:
        shutil.copy2(plan / name, backup / name)

    queue_path = plan / "FAST_TRACK_PRIORITY_QUEUE.parquet"
    ledger_path = plan / "SLOT_ATTEMPT_LEDGER.parquet"
    queue = pl.read_parquet(queue_path)
    ledger = pl.read_parquet(ledger_path)
    queue_rows = {str(row["slot_id"]): row for row in queue.to_dicts()}
    new_rows: list[dict] = []
    for slot_id in selected:
        row = queue_rows.get(slot_id)
        if row is None:
            raise RuntimeError(f"selected slot missing from priority queue: {slot_id}")
        domains = row.get("verified_domains") or []
        new_rows.append(
            {
                "slot_id": slot_id,
                "strategy": args.strategy,
                "query_hash": digest(f"{args.run_id}|{slot_id}|{args.strategy}|{domains}"),
                "candidate_set_hash": str(row.get("candidate_set_hash") or ""),
                "page_evidence_hash": str(row.get("page_evidence_hash") or ""),
                "attempt_count": int(row.get("prior_attempt_count") or 0) + 1,
                "last_attempt": now(),
                "result": "no_retained_candidate",
                "dominant_blocker": "no_retained_candidate",
                "run_id": args.run_id,
                "source": "fast_track_domain_finalizer",
                "prior_attempt_count": int(row.get("prior_attempt_count") or 0),
                "verified_domain_count": int(row.get("verified_domain_count") or 0),
                "verified_domains": domains,
                "priority_tier": str(row.get("priority_tier") or ""),
            }
        )

    additions = pl.DataFrame(new_rows, schema=ledger.schema, strict=False)
    combined = pl.concat([ledger, additions], how="vertical_relaxed")
    atomic_write_parquet(combined, ledger_path, {"module": "fast_track.plan_finalizer", "run_id": args.run_id})

    updated_queue_rows = []
    for row in queue.to_dicts():
        slot_id = str(row["slot_id"])
        if slot_id in selected:
            row["prior_attempt_count"] = int(row.get("prior_attempt_count") or 0) + 1
            row["dominant_blocker"] = "no_retained_candidate"
            row["strategy"] = args.strategy
        updated_queue_rows.append(row)
    updated_queue = pl.DataFrame(updated_queue_rows, schema=queue.schema, strict=False)
    atomic_write_parquet(updated_queue, queue_path, {"module": "fast_track.priority_queue", "run_id": args.run_id})

    summary = {
        "run_id": args.run_id,
        "strategy": args.strategy,
        "selected_slots": selected,
        "result": "no_retained_candidate",
        "strict_added": 0,
        "backup": str(backup),
        "ledger_rows_added": len(new_rows),
        "finalized_at": now(),
    }
    (run_dir / "plan_finalizer_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
