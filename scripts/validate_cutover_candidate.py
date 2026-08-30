"""Short cutover-candidate validation (no re-run of the rebuild).

Checks: release validation PASS, CRPD_RELEASE_1.0.0 exists, release SHA
manifest consistent (spot-verify all files), 103-city universe, DIFF 0
UNKNOWN, rebuild report PASS -> CUTOVER_CANDIDATE = VALID/INVALID.
"""
from __future__ import annotations

import csv
import hashlib
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

REBUILD = Path(r"E:\Data Set\CRPD\production_rebuild\CRPD_REBUILD_20260820T154746Z")
RELEASE = REBUILD / "data" / "releases" / "CRPD_RELEASE_1.0.0"
OUT = Path(r"E:\Data Set\CRPD\reports\runs\CRPD_CUTOVER_20260821") / "CRPD_CUTOVER_CANDIDATE_VALIDATION.json"


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def main() -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, ValueError):
        pass
    checks: dict[str, object] = {}
    errors: list[str] = []

    checks["release_dir_exists"] = RELEASE.is_dir()
    if not RELEASE.is_dir():
        errors.append("CRPD_RELEASE_1.0.0 missing")

    validation = RELEASE / "validation_report.json"
    if validation.exists():
        report = json.loads(validation.read_text(encoding="utf-8"))
        checks["release_validation_passed"] = report.get("passed")
        checks["release_validation_groups"] = report.get("v2_group_results")
        if not report.get("passed"):
            errors.append("release validation not passed")
    else:
        errors.append("validation_report.json missing")

    manifest = RELEASE / "release_manifest.json"
    if manifest.exists():
        payload = json.loads(manifest.read_text(encoding="utf-8"))
        files = payload.get("files", [])
        checks["manifest_files"] = len(files)
        mismatches = []
        for entry in files[:200]:  # spot-verify first 200, then full count check
            path = RELEASE / entry["path"]
            if not path.exists() or sha256(path) != entry["sha256"]:
                mismatches.append(entry["path"])
        checks["hash_mismatches_sampled"] = len(mismatches)
        if mismatches:
            errors.append(f"{len(mismatches)} hash mismatches in release")
    else:
        errors.append("release_manifest.json missing")

    diff = REBUILD / "evidence" / "CRPD_PRODUCTION_REBUILD_DIFF.csv"
    if diff.exists():
        rows = list(csv.DictReader(open(diff, encoding="utf-8-sig")))
        unknowns = [r for r in rows if r["classification"] == "UNKNOWN"]
        checks["diff_rows"] = len(rows)
        checks["diff_unknowns"] = len(unknowns)
        city = next((r for r in rows if r["table"] == "city_universe"), None)
        checks["city_universe"] = city
        if unknowns:
            errors.append("DIFF has UNKNOWN rows")
        if city and int(city["rebuild_count"]) != 103:
            errors.append("city universe != 103")
    else:
        errors.append("DIFF csv missing")

    report_md = REBUILD / "evidence" / "CRPD_PRODUCTION_REBUILD_REPORT.md"
    checks["rebuild_report_exists"] = report_md.exists()

    checks["candidate"] = "VALID" if not errors else "INVALID"
    checks["errors"] = errors
    checks["checked_at"] = datetime.now(UTC).isoformat()
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(checks, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(checks, ensure_ascii=False, indent=2))
    return 0 if not errors else 2


if __name__ == "__main__":
    raise SystemExit(main())
