"""CRPD production cutover — atomic, reversible pointer switch.

Creates E:\\Data Set\\CRPD\\production\\{current, previous, releases, pointer.json}:
  current  (junction) -> CRPD_REBUILD_20260820T154746Z (new baseline)
  releases\\CRPD_1.0.0 (junction) -> rebuild release dir (immutable)
  pointer.json records current/previous/release/switch time — rollback =
  repointing the junction. Old production untouched.

Refuses to run unless the candidate validation manifest says VALID.
"""
from __future__ import annotations

import json
import sys
from datetime import UTC, datetime
from pathlib import Path

DATA_ROOT = Path(r"E:\Data Set\CRPD")
REBUILD = DATA_ROOT / "production_rebuild" / "CRPD_REBUILD_20260820T154746Z"
PROD = DATA_ROOT / "production"
VALIDATION = Path(r"E:\Data Set\CRPD\reports\runs\CRPD_CUTOVER_20260821") / "CRPD_CUTOVER_CANDIDATE_VALIDATION.json"


def main() -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, ValueError):
        pass

    if not VALIDATION.exists():
        print("ABORT: candidate validation missing", file=sys.stderr)
        return 3
    verdict = json.loads(VALIDATION.read_text(encoding="utf-8"))
    if verdict.get("candidate") != "VALID":
        print(f"ABORT: candidate {verdict.get('candidate')}", file=sys.stderr)
        return 3

    PROD.mkdir(parents=True, exist_ok=True)
    current = PROD / "current"
    if current.exists():
        print(f"ABORT: production current already exists: {current}", file=sys.stderr)
        return 3

    import subprocess

    subprocess.run(["cmd", "/c", "mklink", "/J", str(current), str(REBUILD)], check=True)
    releases = PROD / "releases"
    releases.mkdir(parents=True, exist_ok=True)
    release_link = releases / "CRPD_1.0.0"
    release_src = REBUILD / "data" / "releases" / "CRPD_RELEASE_1.0.0"
    subprocess.run(["cmd", "/c", "mklink", "/J", str(release_link), str(release_src)], check=True)

    pointer = {
        "schema": "CRPD_PRODUCTION_POINTER",
        "switched_at": datetime.now(UTC).isoformat(),
        "current": str(current),
        "current_resolves_to": str(REBUILD),
        "previous": str(DATA_ROOT),
        "previous_note": "old production root retained intact (database/policydb.duckdb hash 2d46d87d...)",
        "release": "CRPD_RELEASE_1.0.0",
        "release_link": str(release_link),
        "immutable": True,
        "rollback": "delete junction current + releases\\CRPD_1.0.0, restore pointer.previous",
    }
    (PROD / "pointer.json").write_text(json.dumps(pointer, ensure_ascii=False, indent=2), encoding="utf-8")

    record = {"created_at": datetime.now(UTC).isoformat(), "pointer": pointer,
              "candidate_validation": verdict.get("checked_at")}
    out = Path(r"E:\Data Set\CRPD\reports\runs\CRPD_CUTOVER_20260821") / "CRPD_CUTOVER_RECORD.json"
    out.write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(pointer, ensure_ascii=False, indent=2))
    print("CUTOVER_COMPLETE")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
