"""CRPD quarantine disposition execution.

Reads CRPD_QUARANTINE_DISPOSITION.csv and:
  MOVE rows (A/E/D)  -> copy -> sha256 verify -> atomic rename to E targets
  DELETE_SAFE rows   -> move into E:\\Data Set\\CRPD\\delete_staging\\<RUN>\\
                        with DELETE_MANIFEST entries (no direct deletion)
Canonical-per-hash kept; duplicates/zero-byte staged for deletion.

Usage:
  python scripts/execute_quarantine_disposition.py --disposition <csv> [--apply]
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import shutil
import sys
from datetime import UTC, datetime
from pathlib import Path

DATA_ROOT = Path(r"E:\Data Set\CRPD")
DELETE_STAGING = DATA_ROOT / "delete_staging" / f"QUARANTINE_{datetime.now(UTC):%Y%m%dT%H%M%SZ}"


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def move_verified(source: Path, target: Path) -> None:
    """copy -> verify -> atomically place -> remove source (cross-volume safe)."""
    if target.exists():
        if sha256(target) == sha256(source):
            source.unlink(missing_ok=True)
            return
        raise FileExistsError(f"target conflict: {target}")
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(target.name + ".tmp")
    shutil.copy2(source, temporary)
    if sha256(temporary) != sha256(source):
        temporary.unlink(missing_ok=True)
        raise OSError(f"verify failed: {source}")
    os.replace(temporary, target)
    source.unlink(missing_ok=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--disposition", required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, ValueError):
        pass

    with open(args.disposition, encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))
    counters = {"moved": 0, "staged_delete": 0, "skipped": 0, "failed": 0}
    failures: list[dict] = []
    delete_manifest: list[dict] = []

    for row in rows:
        source = Path(row["source"])
        action = row["action"]
        category = row["category"]
        if not source.exists():
            counters["skipped"] += 1
            continue
        if action == "MOVE_E_EVIDENCE" or action == "MOVE_E_TEST_ARTIFACTS" or action == "MOVE_E_LOGS":
            target = Path(row["target"])
            if not target:
                target = DATA_ROOT / "test_artifacts" / "quarantine_archive" / source.name
            if not args.apply:
                print(f"WOULD MOVE {source} -> {target}")
                continue
            try:
                move_verified(source, target)
                counters["moved"] += 1
            except Exception as exc:  # noqa: BLE001
                counters["failed"] += 1
                failures.append({"source": str(source), "error": f"{type(exc).__name__}: {exc}"})
        elif action == "DELETE_SAFE":
            if not args.apply:
                print(f"WOULD STAGE DELETE {source}")
                continue
            try:
                target = DELETE_STAGING / row["sha256"][:8] / source.name
                move_verified(source, target)
                counters["staged_delete"] += 1
                delete_manifest.append(
                    {
                        "source": str(source),
                        "staged": str(target),
                        "sha256": row["sha256"],
                        "size": row["size"],
                        "category": category,
                        "at": datetime.now(UTC).isoformat(),
                    }
                )
            except Exception as exc:  # noqa: BLE001
                counters["failed"] += 1
                failures.append({"source": str(source), "error": f"{type(exc).__name__}: {exc}"})
        else:
            counters["skipped"] += 1

    if args.apply:
        if delete_manifest:
            DELETE_STAGING.mkdir(parents=True, exist_ok=True)
            (DELETE_STAGING / "DELETE_MANIFEST.json").write_text(
                json.dumps(delete_manifest, ensure_ascii=False, indent=2), encoding="utf-8"
            )
        summary_path = Path(args.disposition).parent / "execution_summary.json"
        summary_path.write_text(
            json.dumps(
                {
                    "created_at": datetime.now(UTC).isoformat(),
                    "counters": counters,
                    "failures": failures,
                    "delete_staging": str(DELETE_STAGING),
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
    print(json.dumps({"counters": counters, "failures": failures[:20]}, ensure_ascii=False, indent=2))
    return 0 if not failures else 2


if __name__ == "__main__":
    raise SystemExit(main())
