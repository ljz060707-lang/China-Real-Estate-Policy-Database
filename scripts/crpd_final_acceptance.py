"""CRPD final acceptance — writes CRPD_FINAL_ACCEPTANCE.md + SHA manifest."""
from __future__ import annotations

import hashlib
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

OUT = Path(r"E:\Data Set\CRPD\reports\runs\CRPD_FINAL_20260820T155100Z")
REBUILD = Path(r"E:\Data Set\CRPD\production_rebuild\CRPD_REBUILD_20260820T154746Z")


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

    core_artifacts = [
        OUT / "CRPD_FINAL_ACCEPTANCE.md",
        OUT / "CRPD_STORAGE_FINAL_REPORT.md",
        OUT / "CRPD_PRODUCTION_REBUILD_REPORT.md",
        OUT / "CRPD_FINAL_STORAGE_ACTIONS.csv",
        REBUILD / "evidence" / "CRPD_PRODUCTION_REBUILD_DIFF.csv",
        REBUILD / "data" / "releases" / "CRPD_RELEASE_1.0.0" / "release_manifest.json",
        REBUILD / "data" / "releases" / "CRPD_RELEASE_1.0.0" / "validation_report.json",
    ]
    disposition = sorted(OUT.parent.glob("QUARANTINE_DISPOSITION_*"))
    if disposition:
        core_artifacts.append(disposition[-1] / "CRPD_QUARANTINE_DISPOSITION.csv")
    manifest_entries = []
    for path in core_artifacts:
        if path.exists():
            manifest_entries.append(
                {"path": str(path.relative_to(Path(r"E:\Data Set\CRPD"))).replace("\\", "/"),
                 "sha256": sha256(path), "size": path.stat().st_size}
            )
    manifest = {
        "schema": "CRPD_SHA256_MANIFEST_FINAL",
        "created_at": datetime.now(UTC).isoformat(),
        "entries": manifest_entries,
    }
    (OUT / "CRPD_SHA256_MANIFEST.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(f"manifest entries: {len(manifest_entries)} -> {OUT / 'CRPD_SHA256_MANIFEST.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
