"""CRPD final storage classification (read-only) — repo/C/D non-source files.

Classifies actionable files into the unified categories and writes
CRPD_FINAL_STORAGE_ACTIONS.csv (actionable rows only — not a 239k-row
inventory). Nothing is moved or deleted by this script.

Categories:
  KEEP_REPO / MOVE_E_RESULTS / MOVE_E_REPORT / MOVE_E_TEST_ARTIFACT /
  MOVE_E_LOG / MOVE_E_RAW_EVIDENCE / MOVE_E_RELEASE / MOVE_E_ARCHIVE /
  QUARANTINE_REVIEW / DELETE_SAFE / LEGACY_COMPAT_REQUIRED
"""
from __future__ import annotations

import csv
import hashlib
import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path

REPO = Path(os.environ.get("CRPD_HOME", r"E:\policy-database"))
DATA_ROOT = Path(os.environ.get("CRPD_DATA_ROOT", r"E:\Data Set\CRPD"))
ANALYSIS = Path(os.environ.get("CRPD_ANALYSIS_ROOT", DATA_ROOT / "analysis"))
OUT_DIR = DATA_ROOT / "reports" / "runs" / f"CRPD_FINAL_{datetime.now(UTC):%Y%m%dT%H%M%SZ}"

KEEP_REPO_DIRS = {"src", "tests", "scripts", "migrations", "docs", "config", ".github"}
KEEP_REPO_FILES = {
    "pyproject.toml", "README.md", "CONTEXT.md", ".gitignore", "AGENTS.md",
    "uv.lock", "requirements.txt", "pytest.ini", ".python-version",
    ".env", ".env.example", ".gitattributes", "Makefile", "runtime.txt",
    "CHANGELOG_V2.md", "SOURCE_COMPLETION_TO_525_README.md",
    "start_policydb.cmd", "首次安装.bat", "打开房地产政策数据库.bat",
    "关闭房地产政策数据库.bat",
}

DELETE_SAFE_DIRS = {".pytest_cache", "__pycache__", ".uv-cache", ".uv-cache-local", ".test-tmp", ".mypy_cache", ".ruff_cache"}

RESULT_EXTENSIONS = {".csv", ".parquet", ".xlsx", ".dta", ".json", ".html", ".png"}


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def classify_repo_file(path: Path) -> tuple[str, str, str]:
    """Return (category, target, reason) for one repo file."""
    name = path.name
    if name in KEEP_REPO_FILES:
        return "KEEP_REPO", "", "required repo file"
    if path.parent == REPO and name.startswith("CRPD_"):
        if name in {"CRPD_CODEBASE_MAP.md", "CRPD_ARCHITECTURE.md", "CRPD_DEPENDENCY_GRAPH.md",
                    "CRPD_CRAWL_ARCHITECTURE.md", "CRPD_DATA_MODEL.md", "CRPD_SOURCE_GOVERNANCE.md",
                    "CRPD_POLICY_ONTOLOGY.yaml", "CRPD_KEYWORD_LEXICON.yaml", "CONTEXT.md"}:
            return "KEEP_REPO", "docs/", "permanent architecture/config doc"
        if name.endswith(".md"):
            return "MOVE_E_REPORT", str(OUT_DIR / "reports"), "run/acceptance report"
        if name.endswith((".csv", ".xlsx", ".json")):
            return "MOVE_E_RESULTS", str(OUT_DIR / "results"), "run result artifact"
        if name.endswith(".yaml"):
            return "KEEP_REPO", "data/reference/", "reference config"
    if name.endswith((".pyc", ".pyo")):
        return "DELETE_SAFE", "", "compiled bytecode"
    if path.parent.name == "__pycache__":
        return "DELETE_SAFE", "", "python cache"
    if name.endswith((".log",)) or name.startswith(("pytest",)) and path.suffix in {".txt", ".log", ".html"}:
        return "MOVE_E_LOG", str(OUT_DIR / "logs"), "log/test output"
    if path.suffix in {".cmd", ".bat"} or name in {"Makefile", ".gitattributes", "runtime.txt"}:
        return "KEEP_REPO", "", "dev/ops launcher or config"
    return "QUARANTINE_REVIEW", "", "unclassified"


def main() -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, ValueError):
        pass
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    rows: list[dict] = []

    # 1. Repo top-level files (CRPD_* results, etc.)
    for path in sorted(REPO.iterdir()):
        if not path.is_file():
            continue
        category, target, reason = classify_repo_file(path)
        if category == "KEEP_REPO":
            continue
        rows.append(
            {
                "source_path": str(path),
                "size": path.stat().st_size,
                "category": category,
                "operation": "MOVE" if category.startswith("MOVE") else ("DELETE" if category == "DELETE_SAFE" else "REVIEW"),
                "target_path": target,
                "referenced_by": "",
                "sha256": sha256(path),
                "unique_content": "yes",
                "safe_to_delete": "no",
                "reason": reason,
            }
        )

    # 2. Repo dirs by top-level name (sizes; the executor moves/removes dirs)
    for path in sorted(REPO.iterdir()):
        if not path.is_dir() or path.name in KEEP_REPO_DIRS or path.name == ".git":
            continue
        size = sum(p.stat().st_size for p in path.rglob("*") if p.is_file())
        category = "KEEP_REPO"
        target = ""
        reason = ""
        if path.name in DELETE_SAFE_DIRS or path.name.startswith(".test-tmp") or path.name.startswith(".pytest"):
            category = "DELETE_SAFE"
            reason = "regenerable test/cache artifact"
        elif path.name == ".venv-1":
            category = "DELETE_SAFE"
            reason = "old virtualenv (current .venv untouched)"
        elif path.name == ".venv":
            category = "KEEP_REPO"
            reason = "current virtualenv in use"
        if category != "KEEP_REPO":
            rows.append(
                {
                    "source_path": str(path),
                    "size": size,
                    "category": category,
                    "operation": "MOVE" if category.startswith("MOVE") else "DELETE",
                    "target_path": target,
                    "referenced_by": "",
                    "sha256": "",
                    "unique_content": "dir",
                    "safe_to_delete": "no" if category.startswith("MOVE") else "after_verify",
                    "reason": reason,
                }
            )

    # 3. Optional external analysis result dirs (CRPD_ANALYSIS_ROOT)
    if ANALYSIS.exists():
        for name in ("results", "handoff"):
            path = ANALYSIS / name
            if path.is_dir():
                size = sum(p.stat().st_size for p in path.rglob("*") if p.is_file())
                rows.append(
                    {
                        "source_path": str(path),
                        "size": size,
                        "category": "MOVE_E_ARCHIVE" if name == "handoff" else "MOVE_E_RESULTS",
                        "operation": "MOVE",
                        "target_path": str(DATA_ROOT / "outputs" / ("handoff" if name == "handoff" else "analysis_results")),
                        "referenced_by": "",
                        "sha256": "",
                        "unique_content": "dir",
                        "safe_to_delete": "no",
                        "reason": "analysis results / frozen handoff bundle -> E outputs",
                    }
                )

    # 4. Repo data/ subdirs (reference configs stay; the rest moves to E)
    repo_data = REPO / "data"
    data_map = {
        "reference/backups": ("MOVE_E_ARCHIVE", str(DATA_ROOT / "archive" / "repo_reference_backups"), "registry YAML backups history"),
        "raw": ("MOVE_E_RAW_EVIDENCE", str(DATA_ROOT / "raw" / "repo_legacy"), "repo-local raw evidence"),
        "releases": ("MOVE_E_RELEASE", str(DATA_ROOT / "releases" / "historical" / "repo_legacy"), "historical repo releases"),
        "curated": ("MOVE_E_RESULTS", str(DATA_ROOT / "outputs" / "repo_legacy" / "curated"), "repo-local curated parquet"),
        "staging": ("MOVE_E_RESULTS", str(DATA_ROOT / "outputs" / "repo_legacy" / "staging"), "excel staging"),
        "logs": ("MOVE_E_LOG", str(DATA_ROOT / "logs" / "repo_legacy"), "repo-local logs"),
        "annotations": ("MOVE_E_RESULTS", str(DATA_ROOT / "outputs" / "repo_legacy" / "annotations"), "annotations"),
        "work": ("MOVE_E_RESULTS", str(DATA_ROOT / "outputs" / "repo_legacy" / "work"), "work dir"),
        "interim": ("MOVE_E_RESULTS", str(DATA_ROOT / "outputs" / "repo_legacy" / "interim"), "interim"),
        "research": ("MOVE_E_RESULTS", str(DATA_ROOT / "outputs" / "repo_legacy" / "research"), "research"),
    }
    for sub, (category, target, reason) in data_map.items():
        path = repo_data / sub
        if path.is_dir():
            size = sum(p.stat().st_size for p in path.rglob("*") if p.is_file())
            rows.append(
                {
                    "source_path": str(path),
                    "size": size,
                    "category": category,
                    "operation": "MOVE",
                    "target_path": target,
                    "referenced_by": "",
                    "sha256": "",
                    "unique_content": "dir",
                    "safe_to_delete": "no",
                    "reason": reason,
                }
            )
    # Repo-local dev database -> E snapshot (not deleted)
    for name in ("policydb.duckdb", "policydb.version.json"):
        path = REPO / "database" / name
        if path.is_file():
            rows.append(
                {
                    "source_path": str(path),
                    "size": path.stat().st_size,
                    "category": "MOVE_E_RESULTS",
                    "operation": "MOVE",
                    "target_path": str(DATA_ROOT / "database" / "snapshots" / "repo_dev"),
                    "referenced_by": "",
                    "sha256": sha256(path),
                    "unique_content": "yes",
                    "safe_to_delete": "no",
                    "reason": "repo-local dev database -> E snapshot",
                }
            )
    # Repo outputs dir
    for sub in ("outputs",):
        path = REPO / sub
        if path.is_dir():
            size = sum(p.stat().st_size for p in path.rglob("*") if p.is_file())
            rows.append(
                {
                    "source_path": str(path),
                    "size": size,
                    "category": "MOVE_E_RESULTS",
                    "operation": "MOVE",
                    "target_path": str(DATA_ROOT / "outputs" / "repo_legacy" / "outputs"),
                    "referenced_by": "",
                    "sha256": "",
                    "unique_content": "dir",
                    "safe_to_delete": "no",
                    "reason": "repo-local outputs -> E outputs",
                }
            )

    rows.sort(key=lambda r: (-r["size"], r["source_path"]))
    out = OUT_DIR / "CRPD_FINAL_STORAGE_ACTIONS.csv"
    with open(out, "w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["source_path", "size", "category", "operation", "target_path",
                        "referenced_by", "sha256", "unique_content", "safe_to_delete", "reason"],
        )
        writer.writeheader()
        writer.writerows(rows)
    summary = {
        "created_at": datetime.now(UTC).isoformat(),
        "actionable_rows": len(rows),
        "total_bytes": sum(r["size"] for r in rows),
        "by_category": {name: sum(1 for r in rows if r["category"] == name) for name in sorted({r["category"] for r in rows})},
        "out": str(out),
    }
    (OUT_DIR / "classification_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
