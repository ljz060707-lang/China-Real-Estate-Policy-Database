"""CRPD final storage execution — moves to E + DELETE_SAFE staging.

Reads CRPD_FINAL_STORAGE_ACTIONS.csv and executes:
  MOVE    -> copy -> sha256 verify -> atomic rename to target (same volume)
  DELETE  -> move into E:\\Data Set\\CRPD\\delete_staging\\<RUN_ID>\\ with a
             DELETE_MANIFEST entry (permanent deletion only after confirmation)
Files/dirs marked KEEP_REPO are never touched. Idempotent: targets that
already exist with identical hash are skipped; conflicts abort that row.

Usage:
  python scripts/execute_final_storage.py --actions <csv> [--apply]
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
DELETE_STAGING = DATA_ROOT / "delete_staging" / f"FINAL_{datetime.now(UTC):%Y%m%dT%H%M%SZ}"


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def move_verified(source: Path, target: Path) -> None:
    """copy -> verify -> atomically place -> remove source (cross-volume safe)."""
    if source.is_dir():
        if target.exists():
            return
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.with_name(target.name + ".tmp")
        shutil.copytree(source, temporary)
        source_files = sum(1 for p in source.rglob("*") if p.is_file())
        target_files = sum(1 for p in temporary.rglob("*") if p.is_file())
        if source_files != target_files:
            shutil.rmtree(temporary, ignore_errors=True)
            raise OSError(f"dir copy count mismatch: {source}")
        os.replace(temporary, target)
        shutil.rmtree(source, ignore_errors=True)
        return
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
    parser.add_argument("--actions", required=True, help="path to CRPD_FINAL_STORAGE_ACTIONS.csv")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, ValueError):
        pass

    with open(args.actions, encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))
    counters = {"moved": 0, "staged_delete": 0, "skipped": 0, "failed": 0}
    failures: list[dict] = []
    delete_manifest: list[dict] = []

    for row in rows:
        source = Path(row["source_path"])
        category = row["category"]
        operation = row["operation"]
        if not source.exists():
            counters["skipped"] += 1
            continue
        if operation == "MOVE":
            target = Path(row["target_path"]) / source.name
            if not args.apply:
                print(f"WOULD MOVE {source} -> {target}")
                continue
            try:
                move_verified(source, target)
                counters["moved"] += 1
            except Exception as exc:  # noqa: BLE001
                counters["failed"] += 1
                failures.append({"source": str(source), "error": f"{type(exc).__name__}: {exc}"})
        elif operation == "DELETE" and category == "DELETE_SAFE":
            if not args.apply:
                print(f"WOULD STAGE DELETE {source}")
                continue
            try:
                target = DELETE_STAGING / source.name
                move_verified(source, target)
                counters["staged_delete"] += 1
                delete_manifest.append(
                    {
                        "source": str(source),
                        "staged": str(target),
                        "sha256": sha256(target) if target.is_file() else "",
                        "size": target.stat().st_size if target.is_file() else -1,
                        "category": category,
                        "reason": row["reason"],
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
            manifest_path = DELETE_STAGING / "DELETE_MANIFEST.json"
            manifest_path.write_text(
                json.dumps(delete_manifest, ensure_ascii=False, indent=2), encoding="utf-8"
            )
        summary_path = Path(args.actions).parent / "execution_summary.json"
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
