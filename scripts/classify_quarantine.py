"""CRPD quarantine disposition classifier (read-only).

Reads the frozen quarantine manifest (127,174 rows, SHA-256 verified) and
classifies every file into A–G:

  A UNIQUE_EVIDENCE    -> keep; move to E evidence (deduped canonical set)
  B DUPLICATE_EVIDENCE -> safe-delete candidate (identical sha256 to kept row)
  C REPRODUCIBLE_CACHE -> DELETE_SAFE (run-state/cache inside workspaces)
  D TEST_ARTIFACT      -> keep canonical; move to E test_artifacts archive
  E LOG                -> move to E logs/archive
  F TEMP               -> DELETE_SAFE (0-byte / generic temp)
  G UNKNOWN_REFERENCE  -> remains in quarantine for manual review

Outputs: CRPD_QUARANTINE_DISPOSITION.csv + summary JSON (in reports/runs).
"""
from __future__ import annotations

import csv
import json
import sys
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

MANIFEST = Path(r"E:\Data Set\CRPD\quarantine\20260819T171832Z\QUARANTINE_MANIFEST.jsonl")
DATA_ROOT = Path(r"E:\Data Set\CRPD")
OUT_DIR = DATA_ROOT / "reports" / "runs" / f"QUARANTINE_DISPOSITION_{datetime.now(UTC):%Y%m%dT%H%M%SZ}"

LOG_SUFFIXES = {".log", ".stderr.log", ".stdout.log"}
TEST_WORKSPACE_MARKERS = (
    "pytest", "ai_repair", "ai_autonomous_repair", "episode_930", "ep930",
    "dashboard_redesign", "dashboard_", "source_completion", "full_sync",
    "publish_", "autopilot_", "continuous_sync", "formal_ingest", "glm_",
    "exhaustive_", "pdf_pipeline", "promote_versions", "storage_", "recent_",
    "scope_probe", "crawl_finalize", "autonomous_controller", "2026081",
    "2026080", "archive_glm_test", "path_config_verify", "source_probe",
)


def classify(source: str, size: int, is_duplicate: bool, keep_duplicate: bool) -> tuple[str, str]:
    """Return (category, action)."""
    path = Path(source)
    name = path.name.lower()
    suffix = path.suffix.lower()
    if is_duplicate and not keep_duplicate:
        return "B_DUPLICATE_EVIDENCE", "DELETE_SAFE"
    if size == 0:
        return "F_TEMP", "DELETE_SAFE"
    if suffix in {".log"} or name.endswith((".stderr.log", ".stdout.log")):
        return "E_LOG", "MOVE_E_LOGS"
    if suffix in {".pyc", ".pyo"} or name in {"python.exe", "policydb.exe"}:
        return "F_TEMP", "DELETE_SAFE"
    if suffix in {".html", ".pdf", ".doc", ".docx", ".xls", ".xlsx", ".png", ".jpg"}:
        return "A_UNIQUE_EVIDENCE", "MOVE_E_EVIDENCE"
    if any(marker in source for marker in TEST_WORKSPACE_MARKERS):
        return "D_TEST_ARTIFACT", "MOVE_E_TEST_ARTIFACTS"
    if suffix == ".py" and "\\tmp\\" in source:
        # one-off network/diagnostic probe scripts from tmp\ -> test artifacts
        return "D_TEST_ARTIFACT", "MOVE_E_TEST_ARTIFACTS"
    if suffix in {".json", ".jsonl", ".yaml", ".yml", ".toml", ".txt", ".md"}:
        return "C_REPRODUCIBLE_CACHE", "DELETE_SAFE"
    return "G_UNKNOWN_REFERENCE", "KEEP_QUARANTINE"


def main() -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, ValueError):
        pass
    rows = []
    with open(MANIFEST, encoding="utf-8") as handle:
        for line in handle:
            rows.append(json.loads(line))
    print(f"manifest rows: {len(rows)}")

    # First-seen canonical per sha256 (deterministic order = manifest order).
    canonical: dict[str, dict] = {}
    counts: Counter[str] = Counter()
    for row in rows:
        digest = row["sha256"]
        if digest not in canonical:
            canonical[digest] = row
            counts["unique"] += 1
        else:
            counts["duplicate_rows"] += 1

    disposition: list[dict] = []
    category_counts: Counter[str] = Counter()
    for row in rows:
        digest = row["sha256"]
        keep = row is canonical[digest]
        # Operate on the CURRENT location (quarantine/<stamp>/<relpath>), not
        # the pre-quarantine source recorded in the manifest.
        current = str(Path(row["target"]))
        category, action = classify(
            current, int(row["size"]), is_duplicate=(not keep), keep_duplicate=keep
        )
        if category == "A_UNIQUE_EVIDENCE" and not keep:
            category = "B_DUPLICATE_EVIDENCE"
            action = "DELETE_SAFE"
        category_counts[category] += 1
        disposition.append(
            {
                "source": current,
                "size": row["size"],
                "sha256": digest,
                "category": category,
                "action": action,
                "target": "",
            }
        )

    # Logs and evidence get concrete targets (hash-prefixed to avoid basename
    # collisions across run workspaces).
    stamp = "20260819T171832Z"
    for entry in disposition:
        digest = entry["sha256"][:8]
        if entry["category"] == "E_LOG":
            entry["target"] = str(
                DATA_ROOT / "logs" / "archive" / stamp / digest / Path(entry["source"]).name
            )
        elif entry["category"] == "A_UNIQUE_EVIDENCE":
            entry["target"] = str(
                DATA_ROOT / "evidence" / "quarantine_archive" / stamp / digest / Path(entry["source"]).name
            )
        elif entry["category"] == "D_TEST_ARTIFACT":
            entry["target"] = str(
                DATA_ROOT / "test_artifacts" / "quarantine_archive" / stamp / digest / Path(entry["source"]).name
            )

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out = OUT_DIR / "CRPD_QUARANTINE_DISPOSITION.csv"
    with open(out, "w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(
            handle, fieldnames=["source", "size", "sha256", "category", "action", "target"]
        )
        writer.writeheader()
        writer.writerows(disposition)
    summary = {
        "created_at": datetime.now(UTC).isoformat(),
        "manifest_rows": len(rows),
        "unique_hashes": len(canonical),
        "by_category": dict(category_counts),
        "total_bytes": sum(int(r["size"]) for r in rows),
        "out": str(out),
    }
    (OUT_DIR / "disposition_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
