from __future__ import annotations

import hashlib
import json
import shutil
from datetime import datetime
from pathlib import Path

from policydb.settings import Settings


def sha256(path: Path) -> str:
    digest = hashlib.sha256()

    with path.open("rb") as stream:
        for chunk in iter(
            lambda: stream.read(1024 * 1024),
            b"",
        ):
            digest.update(chunk)

    return digest.hexdigest()


def latest(folder: Path, pattern: str) -> Path | None:
    matches = sorted(
        folder.glob(pattern),
        key=lambda path: path.stat().st_mtime,
    )
    return matches[-1] if matches else None


def main() -> None:
    settings = Settings.discover()
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    snapshot = (
        settings.outputs
        / "acceptance"
        / f"stable_baseline_{stamp}"
    )
    snapshot.mkdir(parents=True, exist_ok=False)

    required = {
        "source_requirement_slots.parquet":
            settings.curated / "source_requirement_slots.parquet",
        "source_candidates.parquet":
            settings.curated / "source_candidates.parquet",
        "source_registry.parquet":
            settings.curated / "source_registry.parquet",
        "source_registry.yaml":
            settings.root / "data" / "reference" / "source_registry.yaml",
        "source_525_audit.csv":
            settings.outputs / "acceptance" / "source_525_audit.csv",
        "source_525_action_queue.csv":
            settings.outputs / "acceptance" / "source_525_action_queue.csv",
        "department_entry_candidate_review.csv":
            settings.outputs / "acceptance"
            / "department_entry_candidate_review.csv",
        "department_entry_slot_shortlist.csv":
            settings.outputs / "acceptance"
            / "department_entry_slot_shortlist.csv",
        "source_candidate_audit.parquet":
            settings.outputs / "source_candidates"
            / "source_candidate_audit.parquet",
    }

    optional = {
        "final_source_alignment.json": latest(
            settings.outputs / "acceptance" / "final_source_alignment",
            "final_source_alignment_*.json",
        ),
        "candidate_variant_cleanup.json": latest(
            settings.outputs / "acceptance" / "candidate_variant_cleanup",
            "candidate_variant_cleanup_*.json",
        ),
        "cross_slot_cleanup.json": latest(
            settings.outputs / "acceptance" / "cross_slot_cleanup",
            "cross_slot_cleanup_*.json",
        ),
        "registry_cleanup.json": latest(
            settings.outputs / "acceptance" / "registry_cleanup",
            "registry_cleanup_result_*.json",
        ),
        "source_slots.py":
            settings.root / "src" / "policydb" / "source_slots.py",
        "cli.py":
            settings.root / "src" / "policydb" / "cli.py",
    }

    missing = [
        str(path)
        for path in required.values()
        if not path.exists()
    ]

    if missing:
        print("缺少必要文件：")
        print("\n".join(missing))
        raise SystemExit("未创建完整基线。")

    manifest = {
        "created_at": datetime.now().isoformat(),
        "repository": str(settings.root),
        "data_root": str(settings.data_root),
        "curated": str(settings.curated),
        "outputs": str(settings.outputs),
        "files": [],
    }

    sources = {
        **required,
        **{
            name: path
            for name, path in optional.items()
            if path is not None and path.exists()
        },
    }

    for name, source in sources.items():
        destination = snapshot / name
        shutil.copy2(source, destination)

        manifest["files"].append(
            {
                "name": name,
                "source": str(source),
                "size_bytes": destination.stat().st_size,
                "sha256": sha256(destination),
                "source_modified_at":
                    datetime.fromtimestamp(
                        source.stat().st_mtime
                    ).isoformat(),
            }
        )

        print(f"COPIED {name}")

    manifest_path = snapshot / "baseline_manifest.json"
    manifest_path.write_text(
        json.dumps(
            manifest,
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    print("=" * 76)
    print(f"Stable baseline: {snapshot}")
    print(f"Manifest:        {manifest_path}")
    print(f"Files:           {len(manifest['files'])}")


if __name__ == "__main__":
    main()
