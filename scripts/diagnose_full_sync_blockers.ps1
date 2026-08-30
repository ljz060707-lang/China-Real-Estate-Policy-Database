#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
from collections.abc import Iterable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

DEFAULT_REPO = Path(__file__).resolve().parents[1]
DEFAULT_DATA_ROOT = Path(r"E:\Data Set\CRPD")

TEXT_SUFFIXES = {".json", ".jsonl", ".log", ".txt", ".py", ".toml", ".yaml", ".yml", ".ps1"}
SEARCH_KEYWORDS = (
    "budget_zero",
    "SEARCH_PROVIDER",
    "SEARCH_API_KEY",
    "SERPAPI",
    "TAVILY",
    "BRAVE",
    "BING",
    "GOOGLE_SEARCH",
    "max_search_calls",
    "search_calls",
)
FAILURE_KEYWORDS = (
    "budget_zero",
    "backfill_not_success",
    "required_backfill_failures",
    "pagination_complete",
    "date_boundary_reached",
    "articles_failed",
    "TLS",
    "SSL",
    "certificate",
    "timeout",
    "timed out",
    "403",
    "404",
    "429",
    "FAILED",
    "PARTIAL",
    "reason_code",
    "latest_error",
)


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def read_text(path: Path) -> str:
    for encoding in ("utf-8-sig", "utf-8", "gb18030"):
        try:
            return path.read_text(encoding=encoding, errors="strict")
        except (UnicodeDecodeError, OSError):
            continue
    return path.read_text(encoding="utf-8", errors="replace")


def read_json(path: Path) -> Any:
    try:
        return json.loads(read_text(path))
    except (json.JSONDecodeError, OSError):
        return None


def embedded_json(text: str) -> Any:
    decoder = json.JSONDecoder()
    for match in re.finditer(r"\{", text):
        try:
            obj, _ = decoder.raw_decode(text[match.start():])
            return obj
        except json.JSONDecodeError:
            continue
    return None


def latest_directory(root: Path, pattern: str = "*") -> Path | None:
    if not root.exists():
        return None
    items = [p for p in root.glob(pattern) if p.is_dir()]
    if not items:
        return None
    return max(items, key=lambda p: p.stat().st_mtime)


def iter_dict_nodes(obj: Any) -> Iterable[dict[str, Any]]:
    if isinstance(obj, dict):
        yield obj
        for value in obj.values():
            yield from iter_dict_nodes(value)
    elif isinstance(obj, list):
        for item in obj:
            yield from iter_dict_nodes(item)


def source_rows(run_json: Any) -> list[dict[str, Any]]:
    fields = (
        "source_id",
        "city_id",
        "city_name",
        "mode",
        "status",
        "stage_result",
        "reason_code",
        "depends_on",
        "url",
        "articles_seen",
        "articles_added",
        "articles_failed",
        "pages_fetched",
        "pagination_complete",
        "date_boundary_reached",
        "error",
        "latest_error",
    )
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()

    for node in iter_dict_nodes(run_json):
        source_id = node.get("source_id")
        if not source_id:
            continue
        row = {field: node.get(field) for field in fields}
        identity = json.dumps(row, ensure_ascii=False, sort_keys=True, default=str)
        if identity in seen:
            continue
        seen.add(identity)
        rows.append(row)

    return rows


def is_failure_row(row: dict[str, Any]) -> bool:
    joined = " ".join(
        str(row.get(key) or "")
        for key in ("status", "stage_result", "reason_code", "error", "latest_error")
    ).lower()
    return any(word in joined for word in ("fail", "partial", "degraded", "skipped", "not_success"))


def is_backfill_row(row: dict[str, Any]) -> bool:
    return (
        str(row.get("mode") or "").lower() == "backfill"
        or str(row.get("reason_code") or "").lower() == "backfill_not_success"
        or str(row.get("depends_on") or "").lower() == "backfill"
    )


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8-sig")
        return

    fieldnames: list[str] = []
    for row in rows:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)

    with path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def scan_evidence(root: Path | None, limit: int = 2000) -> list[dict[str, Any]]:
    if root is None or not root.exists():
        return []

    pattern = re.compile("|".join(re.escape(x) for x in FAILURE_KEYWORDS), re.IGNORECASE)
    hits: list[dict[str, Any]] = []

    for path in root.rglob("*"):
        if len(hits) >= limit:
            break
        if not path.is_file() or path.suffix.lower() not in TEXT_SUFFIXES:
            continue
        try:
            for line_no, line in enumerate(read_text(path).splitlines(), start=1):
                if pattern.search(line):
                    hits.append(
                        {
                            "file": str(path),
                            "line_number": line_no,
                            "line": line.strip()[:500],
                        }
                    )
                    if len(hits) >= limit:
                        break
        except OSError:
            continue

    return hits


def scan_code(repo: Path) -> list[dict[str, Any]]:
    roots = [repo / "src", repo / "scripts", repo / "config"]
    pattern = re.compile("|".join(re.escape(x) for x in SEARCH_KEYWORDS), re.IGNORECASE)
    hits: list[dict[str, Any]] = []

    for root in roots:
        if not root.exists():
            continue
        for path in root.rglob("*"):
            if not path.is_file() or path.suffix.lower() not in TEXT_SUFFIXES:
                continue
            try:
                for line_no, line in enumerate(read_text(path).splitlines(), start=1):
                    if pattern.search(line):
                        hits.append(
                            {
                                "file": str(path.relative_to(repo)),
                                "line_number": line_no,
                                "line": line.strip()[:500],
                            }
                        )
            except OSError:
                continue

    return hits


def provider_environment() -> list[dict[str, Any]]:
    pattern = re.compile(r"SEARCH|SERP|TAVILY|BRAVE|BING|GOOGLE|OPENAI|LLM|API_KEY", re.I)
    rows = []
    for name, value in sorted(os.environ.items()):
        if pattern.search(name):
            rows.append(
                {
                    "name": name,
                    "configured": bool(value and value.strip()),
                    "value_length": len(value or ""),
                }
            )
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(description="Read-only diagnosis of CRPD full-sync blockers.")
    parser.add_argument("--repo", type=Path, default=DEFAULT_REPO)
    parser.add_argument("--data-root", type=Path, default=DEFAULT_DATA_ROOT)
    args = parser.parse_args()

    repo = args.repo.resolve()
    data_root = args.data_root.resolve()

    automation_root = data_root / "outputs" / "continuous_full_sync"
    full_sync_root = data_root / "outputs" / "full_sync"
    stop_file = data_root / "control" / "STOP_FULL_SYNC"
    output_dir = data_root / "outputs" / "diagnostics" / f"full_sync_blockers_{utc_stamp()}"
    output_dir.mkdir(parents=True, exist_ok=True)

    if not repo.exists():
        raise FileNotFoundError(f"Repository not found: {repo}")

    latest_automation = latest_directory(automation_root)
    if latest_automation is None:
        raise FileNotFoundError(f"No automation directory found under: {automation_root}")

    latest_cycle = latest_directory(latest_automation, "cycle_*")
    if latest_cycle is None:
        raise FileNotFoundError(f"No cycle directory found under: {latest_automation}")

    plan_path = latest_cycle / "plan.json"
    run_stdout_path = latest_cycle / "run.stdout.log"

    plan = read_json(plan_path)
    if not isinstance(plan, dict):
        raise RuntimeError(f"Cannot parse plan JSON: {plan_path}")

    run_text = read_text(run_stdout_path)
    run_json = embedded_json(run_text)
    if not isinstance(run_json, dict):
        raise RuntimeError(f"Cannot parse JSON from: {run_stdout_path}")

    reported_run_dir = run_json.get("report")
    plan_run_dir = plan.get("run_dir")

    actual_run_dir: Path | None = None
    for candidate in (reported_run_dir, plan_run_dir):
        if candidate:
            path = Path(str(candidate))
            if path.exists():
                actual_run_dir = path
                break
    if actual_run_dir is None:
        actual_run_dir = latest_directory(full_sync_root)

    rows = source_rows(run_json)
    backfill_rows = [row for row in rows if is_backfill_row(row)]
    failure_rows = [row for row in rows if is_failure_row(row)]

    evidence_hits = scan_evidence(actual_run_dir)
    code_hits = scan_code(repo)
    env_rows = provider_environment()

    write_csv(output_dir / "all_source_results.csv", rows)
    write_csv(output_dir / "backfill_results.csv", backfill_rows)
    write_csv(output_dir / "failed_or_blocked_sources.csv", failure_rows)
    write_csv(output_dir / "run_failure_evidence.csv", evidence_hits)
    write_csv(output_dir / "search_budget_code_matches.csv", code_hits)
    write_csv(output_dir / "provider_environment.csv", env_rows)

    discovery = run_json.get("discovery") or {}
    verification = run_json.get("verification") or {}
    enablement = run_json.get("enablement") or {}
    consistency = run_json.get("consistency") or {}
    db_status = run_json.get("database_sync_status") or {}

    configured_provider_vars = [row["name"] for row in env_rows if row.get("configured")]
    unique_backfill_sources = sorted(
        {str(row["source_id"]) for row in backfill_rows if row.get("source_id")}
    )
    unique_failure_sources = sorted(
        {str(row["source_id"]) for row in failure_rows if row.get("source_id")}
    )

    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "automation": {
            "stop_file_exists": stop_file.exists(),
            "latest_automation_dir": str(latest_automation),
            "latest_cycle": latest_cycle.name,
            "plan_run_id": plan.get("run_id"),
            "actual_run_dir": str(actual_run_dir) if actual_run_dir else None,
        },
        "current_status": {
            "global_status": db_status.get("global_status"),
            "next_recommended_action": db_status.get("next_recommended_action"),
            "documents_total": db_status.get("total_documents"),
            "documents_added_last_run": db_status.get("documents_added_last_run"),
            "open_gaps": db_status.get("open_gaps"),
            "critical_gaps": db_status.get("critical_gaps"),
        },
        "discovery": {
            "status": discovery.get("status"),
            "planned": discovery.get("planned"),
            "search_calls": discovery.get("search_calls"),
            "candidate_proposals": discovery.get("candidate_proposals"),
            "configured_provider_variables": configured_provider_vars,
        },
        "verification": {
            "slots": verification.get("slots"),
            "probed": verification.get("probed"),
            "verified": verification.get("verified"),
            "enabled_this_run": enablement.get("enabled"),
        },
        "backfill": {
            "required_backfill_failures": consistency.get("required_backfill_failures"),
            "unique_backfill_source_count": len(unique_backfill_sources),
            "unique_backfill_sources": unique_backfill_sources,
        },
        "consistency": {
            "passed": consistency.get("passed"),
            "errors": consistency.get("consistency_errors"),
        },
        "diagnostics": {
            "all_source_results": str(output_dir / "all_source_results.csv"),
            "backfill_results": str(output_dir / "backfill_results.csv"),
            "failed_sources": str(output_dir / "failed_or_blocked_sources.csv"),
            "failure_evidence": str(output_dir / "run_failure_evidence.csv"),
            "provider_environment": str(output_dir / "provider_environment.csv"),
            "search_budget_code_matches": str(output_dir / "search_budget_code_matches.csv"),
        },
    }

    summary_path = output_dir / "BLOCKER_SUMMARY.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2, default=str), encoding="utf-8")

    print("=" * 62)
    print("CRPD FULL-SYNC BLOCKER DIAGNOSTICS")
    print("=" * 62)
    print(f"Latest automation       : {latest_automation}")
    print(f"Latest cycle            : {latest_cycle.name}")
    print(f"Actual run directory    : {actual_run_dir}")
    print(f"Stop file exists        : {stop_file.exists()}")
    print()
    print(f"Global status           : {db_status.get('global_status')}")
    print(f"Next recommended action : {db_status.get('next_recommended_action')}")
    print(f"Discovery status        : {discovery.get('status')}")
    print(f"Discovery planned       : {discovery.get('planned')}")
    print(f"Actual search calls     : {discovery.get('search_calls')}")
    print(f"Candidate proposals     : {discovery.get('candidate_proposals')}")
    print(f"Configured provider vars: {len(configured_provider_vars)}")
    print(f"Verification slots      : {verification.get('slots')}")
    print(f"Verification probed     : {verification.get('probed')}")
    print(f"Verification succeeded  : {verification.get('verified')}")
    print(f"Enabled this run        : {enablement.get('enabled')}")
    print(f"Backfill failures       : {consistency.get('required_backfill_failures')}")
    print(f"Backfill source count   : {len(unique_backfill_sources)}")
    print(f"Failure source count    : {len(unique_failure_sources)}")
    print(f"Documents added last run: {db_status.get('documents_added_last_run')}")
    print(f"Critical gaps           : {db_status.get('critical_gaps')}")
    print(f"Consistency passed      : {consistency.get('passed')}")
    print()
    print(f"Evidence directory      : {output_dir}")
    print(f"Summary                 : {summary_path}")
    print()
    print("Backfill source IDs:")
    if unique_backfill_sources:
        for source_id in unique_backfill_sources:
            print(f"  - {source_id}")
    else:
        print("  (No source_id extracted from the structured output.)")

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        raise SystemExit(130)
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
