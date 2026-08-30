"""CRPD storage quarantine executor — evidence-preserving, idempotent, no deletes.

Reads CRPD_STORAGE_REORGANIZATION_PLAN.csv (QUARANTINE_CANDIDATE rows) and moves
each candidate into <data_root>/quarantine/<stamp>/<relpath>. Every file is
SHA-256 hashed before the move and verified after it; a JSONL manifest records
each action. Nothing is ever deleted. Re-running is safe: files already
present at the target with a matching hash are skipped, and source files that
are gone are recorded as already handled.

Evidence dirs (e.g. temp\\crpd_takeover) are excluded and preserved in outputs.

Usage:
  python scripts/quarantine_candidates.py            # dry run (no changes)
  python scripts/quarantine_candidates.py --apply    # execute
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path

PLAN = Path(__file__).resolve().parents[1] / "CRPD_STORAGE_REORGANIZATION_PLAN.csv"
DATA_ROOT = Path(r"E:\Data Set\CRPD")
QUARANTINE_ROOT = DATA_ROOT / "quarantine"
EXCLUDED_TOP_LEVEL = {"crpd_takeover"}  # takeover evidence moved to outputs/
CHUNK = 1024 * 1024


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(CHUNK), b""):
            digest.update(block)
    return digest.hexdigest()


def load_candidates() -> list[dict]:
    rows = []
    with open(PLAN, encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            if row.get("operation") == "QUARANTINE_CANDIDATE":
                rows.append(row)
    return rows


def live_policydb_processes() -> list[int]:
    """Refuse to run while any policydb worker process is alive."""
    import psutil

    pids: list[int] = []
    for process in psutil.process_iter(["pid", "cmdline"]):
        try:
            cmdline = " ".join(process.info.get("cmdline") or [])
        except (psutil.AccessDenied, psutil.NoSuchProcess):
            continue
        if "policydb" in cmdline.lower() and "quarantine_candidates" not in cmdline.lower():
            pids.append(process.info["pid"])
    return pids


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="execute moves (default: dry run)")
    args = parser.parse_args()

    if args.apply:
        pids = live_policydb_processes()
        if pids:
            print(f"ABORT: live policydb processes detected: {pids}", file=sys.stderr)
            return 3

    candidates = load_candidates()
    excluded = [
        row for row in candidates
        if any(part in EXCLUDED_TOP_LEVEL for part in Path(row["current_path"]).parts)
    ]
    candidates = [row for row in candidates if row not in excluded]

    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    target_root = QUARANTINE_ROOT / stamp
    manifest_path = target_root / "QUARANTINE_MANIFEST.jsonl"

    counters = {"moved": 0, "verified": 0, "skipped_existing": 0, "already_handled": 0,
                "conflict": 0, "failed": 0, "excluded": len(excluded)}
    failures: list[dict] = []
    conflicts: list[dict] = []

    if not args.apply:
        print(f"DRY RUN: {len(candidates)} quarantine candidates, "
              f"{len(excluded)} excluded (crpd_takeover evidence)")
        print(f"target: {target_root}")
        for row in candidates[:5]:
            print("  ", row["current_path"], row["size"], "bytes")
        print("no changes made (use --apply to execute)")
        return 0

    target_root.mkdir(parents=True, exist_ok=True)
    manifest_handle = manifest_path.open("a", encoding="utf-8")
    try:
        for index, row in enumerate(candidates, start=1):
            relative = Path(row["current_path"])
            source = DATA_ROOT / relative
            target = target_root / relative
            if not source.exists():
                counters["already_handled"] += 1
                continue
            if target.exists():
                if sha256(target) == sha256(source):
                    counters["skipped_existing"] += 1
                    continue
                counters["conflict"] += 1
                conflicts.append({"path": str(relative), "reason": "target exists with different content"})
                continue
            try:
                digest = sha256(source)
                target.parent.mkdir(parents=True, exist_ok=True)
                os.replace(source, target)  # atomic same-volume move
                if sha256(target) != digest:
                    raise OSError("SHA-256 verification failed after move")
                counters["moved"] += 1
                counters["verified"] += 1
                manifest_handle.write(
                    json.dumps(
                        {
                            "at": datetime.now(UTC).isoformat(),
                            "source": str(source),
                            "target": str(target),
                            "sha256": digest,
                            "size": row["size"],
                            "category": row["category"],
                        },
                        ensure_ascii=False,
                    )
                    + "\n"
                )
            except Exception as exc:  # noqa: BLE001 — record, never abort the run
                counters["failed"] += 1
                failures.append({"path": str(relative), "error": f"{type(exc).__name__}: {exc}"})
            if index % 20000 == 0:
                print(f"progress: {index}/{len(candidates)} {counters}", flush=True)
    finally:
        manifest_handle.close()

    summary = {
        "created_at": datetime.now(UTC).isoformat(),
        "stamp": stamp,
        "candidates_total": len(candidates),
        "counters": counters,
        "manifest": str(manifest_path),
        "conflicts": conflicts[:50],
        "failures": failures[:50],
        "deleted_any_file": False,
    }
    summary_path = target_root / "QUARANTINE_SUMMARY.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0 if counters["conflict"] == 0 and counters["failed"] == 0 else 2


if __name__ == "__main__":
    sys.exit(main())
