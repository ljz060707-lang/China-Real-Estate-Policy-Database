from __future__ import annotations

import csv
import json
import os
import re
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parents[1]
DATA_ROOT = Path(r"E:\Data Set\CRPD")

AUTOMATION_ROOT = DATA_ROOT / "outputs" / "continuous_full_sync"
FULL_SYNC_ROOT = DATA_ROOT / "outputs" / "full_sync"
STOP_FILE = DATA_ROOT / "control" / "STOP_FULL_SYNC"

SOURCE_FIELDS = (
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

FAILURE_TERMS = (
    "budget_zero",
    "backfill_not_success",
    "required_backfill_failures",
    "pagination_complete",
    "date_boundary_reached",
    "articles_failed",
    "tls",
    "ssl",
    "certificate",
    "timeout",
    "timed out",
    "403",
    "404",
    "429",
    "failed",
    "partial",
    "reason_code",
    "latest_error",
)

SEARCH_TERMS = (
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

TEXT_SUFFIXES = {
    ".json",
    ".jsonl",
    ".log",
    ".txt",
    ".py",
    ".ps1",
    ".toml",
    ".yaml",
    ".yml",
}


def read_text(path: Path) -> str:
    for encoding in ("utf-8-sig", "utf-8", "gb18030"):
        try:
            return path.read_text(encoding=encoding)
        except UnicodeDecodeError:
            continue

    return path.read_text(encoding="utf-8", errors="replace")


def read_json(path: Path) -> Any:
    try:
        return json.loads(read_text(path))
    except (OSError, json.JSONDecodeError):
        return None


def parse_embedded_json(text: str) -> Any:
    decoder = json.JSONDecoder()

    for match in re.finditer(r"\{", text):
        try:
            value, _ = decoder.raw_decode(text[match.start():])
            return value
        except json.JSONDecodeError:
            continue

    return None


def latest_directory(root: Path, pattern: str = "*") -> Path | None:
    if not root.exists():
        return None

    candidates = [
        path
        for path in root.glob(pattern)
        if path.is_dir()
    ]

    if not candidates:
        return None

    return max(
        candidates,
        key=lambda path: path.stat().st_mtime,
    )


def walk_nodes(value: Any):
    if isinstance(value, dict):
        yield value

        for child in value.values():
            yield from walk_nodes(child)

    elif isinstance(value, list):
        for child in value:
            yield from walk_nodes(child)


def extract_source_rows(run_json: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()

    for node in walk_nodes(run_json):
        if not node.get("source_id"):
            continue

        row = {
            field: node.get(field)
            for field in SOURCE_FIELDS
        }

        signature = json.dumps(
            row,
            ensure_ascii=False,
            sort_keys=True,
            default=str,
        )

        if signature in seen:
            continue

        seen.add(signature)
        rows.append(row)

    return rows


def is_backfill_related(row: dict[str, Any]) -> bool:
    return (
        str(row.get("mode") or "").lower() == "backfill"
        or str(row.get("reason_code") or "").lower()
        == "backfill_not_success"
        or str(row.get("depends_on") or "").lower()
        == "backfill"
    )


def is_failed_or_blocked(row: dict[str, Any]) -> bool:
    text = " ".join(
        str(row.get(field) or "")
        for field in (
            "status",
            "stage_result",
            "reason_code",
            "error",
            "latest_error",
        )
    ).lower()

    return any(
        token in text
        for token in (
            "fail",
            "partial",
            "degraded",
            "skipped",
            "not_success",
        )
    )


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8-sig")
        return

    fieldnames: list[str] = []

    for row in rows:
        for field in row:
            if field not in fieldnames:
                fieldnames.append(field)

    with path.open(
        "w",
        newline="",
        encoding="utf-8-sig",
    ) as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=fieldnames,
            extrasaction="ignore",
        )
        writer.writeheader()
        writer.writerows(rows)


def scan_files(
    root: Path | None,
    terms: tuple[str, ...],
    limit: int = 2000,
) -> list[dict[str, Any]]:
    if root is None or not root.exists():
        return []

    pattern = re.compile(
        "|".join(re.escape(term) for term in terms),
        re.IGNORECASE,
    )

    results: list[dict[str, Any]] = []

    for path in root.rglob("*"):
        if len(results) >= limit:
            break

        if (
            not path.is_file()
            or path.suffix.lower() not in TEXT_SUFFIXES
        ):
            continue

        try:
            lines = read_text(path).splitlines()
        except OSError:
            continue

        for line_number, line in enumerate(lines, start=1):
            if not pattern.search(line):
                continue

            results.append(
                {
                    "file": str(path),
                    "line_number": line_number,
                    "line": line.strip()[:500],
                }
            )

            if len(results) >= limit:
                break

    return results


def scan_repository() -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []

    for root in (
        REPO / "src",
        REPO / "scripts",
        REPO / "config",
    ):
        if not root.exists():
            continue

        for row in scan_files(
            root,
            SEARCH_TERMS,
            limit=5000,
        ):
            try:
                row["file"] = str(
                    Path(row["file"]).relative_to(REPO)
                )
            except ValueError:
                pass

            results.append(row)

    return results


def provider_environment() -> list[dict[str, Any]]:
    pattern = re.compile(
        r"SEARCH|SERP|TAVILY|BRAVE|BING|GOOGLE|"
        r"OPENAI|LLM|API_KEY",
        re.IGNORECASE,
    )

    return [
        {
            "name": name,
            "configured": bool(value and value.strip()),
            "value_length": len(value or ""),
        }
        for name, value in sorted(os.environ.items())
        if pattern.search(name)
    ]


def main() -> int:
    if not REPO.exists():
        raise FileNotFoundError(
            f"Repository not found: {REPO}"
        )

    automation = latest_directory(AUTOMATION_ROOT)

    if automation is None:
        raise FileNotFoundError(
            f"No automation directory: {AUTOMATION_ROOT}"
        )

    # cycle_9999 是自动化结束时的最终 status/report 目录，
    # 不属于正常抓取轮次。选择最近一个具有有效计划和运行日志的 cycle。
    cycle_candidates = sorted(
        (
            path
            for path in automation.glob("cycle_*")
            if path.is_dir()
            and path.name != "cycle_9999"
        ),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )

    cycle = None
    plan = None
    plan_path = None
    run_path = None

    for candidate in cycle_candidates:
        candidate_plan_path = candidate / "plan.json"
        candidate_run_path = candidate / "run.stdout.log"
        candidate_plan = read_json(candidate_plan_path)

        if (
            isinstance(candidate_plan, dict)
            and candidate_run_path.exists()
        ):
            cycle = candidate
            plan = candidate_plan
            plan_path = candidate_plan_path
            run_path = candidate_run_path
            break

    if (
        cycle is None
        or plan is None
        or plan_path is None
        or run_path is None
    ):
        raise FileNotFoundError(
            "No completed runnable cycle with valid plan.json "
            f"and run.stdout.log under: {automation}"
        )

    run_json = parse_embedded_json(
        read_text(run_path)
    )

    if not isinstance(run_json, dict):
        raise RuntimeError(
            f"Cannot parse run JSON: {run_path}"
        )

    actual_run_dir: Path | None = None

    for candidate in (
        run_json.get("report"),
        plan.get("run_dir"),
    ):
        if not candidate:
            continue

        candidate_path = Path(str(candidate))

        if candidate_path.exists():
            actual_run_dir = candidate_path
            break

    if actual_run_dir is None:
        actual_run_dir = latest_directory(FULL_SYNC_ROOT)

    stamp = datetime.now(
        UTC
    ).strftime("%Y%m%dT%H%M%SZ")

    output_dir = (
        DATA_ROOT
        / "outputs"
        / "diagnostics"
        / f"full_sync_blockers_{stamp}"
    )
    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    all_sources = extract_source_rows(run_json)

    backfill_sources = [
        row
        for row in all_sources
        if is_backfill_related(row)
    ]

    failed_sources = [
        row
        for row in all_sources
        if is_failed_or_blocked(row)
    ]

    failure_evidence = scan_files(
        actual_run_dir,
        FAILURE_TERMS,
    )

    code_matches = scan_repository()
    env_rows = provider_environment()

    write_csv(
        output_dir / "all_source_results.csv",
        all_sources,
    )
    write_csv(
        output_dir / "backfill_results.csv",
        backfill_sources,
    )
    write_csv(
        output_dir / "failed_or_blocked_sources.csv",
        failed_sources,
    )
    write_csv(
        output_dir / "run_failure_evidence.csv",
        failure_evidence,
    )
    write_csv(
        output_dir / "search_budget_code_matches.csv",
        code_matches,
    )
    write_csv(
        output_dir / "provider_environment.csv",
        env_rows,
    )

    discovery = run_json.get("discovery") or {}
    verification = run_json.get("verification") or {}
    enablement = run_json.get("enablement") or {}
    consistency = run_json.get("consistency") or {}
    database_status = (
        run_json.get("database_sync_status") or {}
    )

    unique_backfill_sources = sorted(
        {
            str(row["source_id"])
            for row in backfill_sources
            if row.get("source_id")
        }
    )

    unique_failed_sources = sorted(
        {
            str(row["source_id"])
            for row in failed_sources
            if row.get("source_id")
        }
    )

    configured_variables = [
        row["name"]
        for row in env_rows
        if row["configured"]
    ]

    summary = {
        "generated_at": datetime.now(
            UTC
        ).isoformat(),
        "automation": {
            "stop_file_exists": STOP_FILE.exists(),
            "latest_automation_dir": str(automation),
            "latest_cycle": cycle.name,
            "plan_run_id": plan.get("run_id"),
            "actual_run_dir": (
                str(actual_run_dir)
                if actual_run_dir
                else None
            ),
        },
        "current_status": {
            "global_status": database_status.get(
                "global_status"
            ),
            "next_recommended_action": (
                database_status.get(
                    "next_recommended_action"
                )
            ),
            "documents_total": database_status.get(
                "total_documents"
            ),
            "documents_added_last_run": (
                database_status.get(
                    "documents_added_last_run"
                )
            ),
            "open_gaps": database_status.get(
                "open_gaps"
            ),
            "critical_gaps": database_status.get(
                "critical_gaps"
            ),
        },
        "discovery": {
            "status": discovery.get("status"),
            "planned": discovery.get("planned"),
            "search_calls": discovery.get(
                "search_calls"
            ),
            "candidate_proposals": discovery.get(
                "candidate_proposals"
            ),
            "configured_provider_variables": (
                configured_variables
            ),
        },
        "verification": {
            "slots": verification.get("slots"),
            "probed": verification.get("probed"),
            "verified": verification.get(
                "verified"
            ),
            "enabled_this_run": enablement.get(
                "enabled"
            ),
        },
        "backfill": {
            "required_backfill_failures": (
                consistency.get(
                    "required_backfill_failures"
                )
            ),
            "unique_backfill_source_count": len(
                unique_backfill_sources
            ),
            "unique_backfill_sources": (
                unique_backfill_sources
            ),
        },
        "consistency": {
            "passed": consistency.get("passed"),
            "errors": consistency.get(
                "consistency_errors"
            ),
        },
        "diagnostics": {
            "output_dir": str(output_dir),
            "backfill_results": str(
                output_dir / "backfill_results.csv"
            ),
            "failure_evidence": str(
                output_dir / "run_failure_evidence.csv"
            ),
            "provider_environment": str(
                output_dir / "provider_environment.csv"
            ),
            "search_budget_code_matches": str(
                output_dir
                / "search_budget_code_matches.csv"
            ),
        },
    }

    summary_path = (
        output_dir
        / "BLOCKER_SUMMARY.json"
    )

    summary_path.write_text(
        json.dumps(
            summary,
            ensure_ascii=False,
            indent=2,
            default=str,
        ),
        encoding="utf-8",
    )

    print("=" * 66)
    print("CRPD FULL-SYNC BLOCKER DIAGNOSTICS")
    print("=" * 66)
    print(f"Latest automation       : {automation}")
    print(f"Latest cycle            : {cycle.name}")
    print(f"Actual run directory    : {actual_run_dir}")
    print(f"Stop file exists        : {STOP_FILE.exists()}")
    print()
    print(
        "Global status           : "
        f"{database_status.get('global_status')}"
    )
    print(
        "Next recommended action : "
        f"{database_status.get('next_recommended_action')}"
    )
    print(
        "Discovery status        : "
        f"{discovery.get('status')}"
    )
    print(
        "Discovery planned       : "
        f"{discovery.get('planned')}"
    )
    print(
        "Actual search calls     : "
        f"{discovery.get('search_calls')}"
    )
    print(
        "Candidate proposals     : "
        f"{discovery.get('candidate_proposals')}"
    )
    print(
        "Configured provider vars: "
        f"{len(configured_variables)}"
    )
    print(
        "Verification slots      : "
        f"{verification.get('slots')}"
    )
    print(
        "Verification probed     : "
        f"{verification.get('probed')}"
    )
    print(
        "Verification succeeded  : "
        f"{verification.get('verified')}"
    )
    print(
        "Enabled this run        : "
        f"{enablement.get('enabled')}"
    )
    print(
        "Backfill failures       : "
        f"{consistency.get('required_backfill_failures')}"
    )
    print(
        "Backfill source count   : "
        f"{len(unique_backfill_sources)}"
    )
    print(
        "Failed source count     : "
        f"{len(unique_failed_sources)}"
    )
    print(
        "Documents added last run: "
        f"{database_status.get('documents_added_last_run')}"
    )
    print(
        "Critical gaps           : "
        f"{database_status.get('critical_gaps')}"
    )
    print(
        "Consistency passed      : "
        f"{consistency.get('passed')}"
    )
    print()
    print(f"Evidence directory      : {output_dir}")
    print(f"Summary                 : {summary_path}")
    print()
    print("Backfill source IDs:")

    if unique_backfill_sources:
        for source_id in unique_backfill_sources:
            print(f"  - {source_id}")
    else:
        print("  (No source IDs extracted.)")

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print(
            "\nInterrupted.",
            file=sys.stderr,
        )
        raise SystemExit(130) from None
    except Exception as exc:
        print(
            f"ERROR: {exc}",
            file=sys.stderr,
        )
        raise SystemExit(1) from None
