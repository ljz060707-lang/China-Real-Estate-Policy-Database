from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO = Path(r"D:\Codex\projects\Documents-Codex\2026-07-13\text-20260705-xlsx-text-data-raw\policy-database")
DATA_ROOT = Path(r"D:\Data Set\CRPD")
EXPECTED_SLOTS = 525

CONTINUOUS_ROOT = DATA_ROOT / "outputs" / "continuous_full_sync"
FULL_SYNC_ROOT = DATA_ROOT / "outputs" / "full_sync"
MONITOR_ROOT = DATA_ROOT / "outputs" / "monitoring"
LOCK_FILE = DATA_ROOT / "locks" / "all_cities_since_2018.lock"
STOP_FILE = DATA_ROOT / "control" / "STOP_FULL_SYNC"

JSON_NAMES = {
    "automation": ["AUTOMATION_STATE.json"],
    "status": ["database_sync_status.json", "sync_status.json"],
    "summary": [
        "final_engineering_audit.json",
        "run_summary.json",
        "sync_run_summary.json",
    ],
    "gaps": ["gap_summary.json", "coverage_gap_summary.json"],
    "completeness": [
        "completeness.json",
        "all_city_completeness.json",
        "beijing_one_year_completeness.json",
    ],
    "source_health": ["source_health.json", "source_health_summary.json"],
}

ERROR_TERMS = (
    "ERROR",
    "FAILED",
    "TlsError",
    "SSLError",
    "PermissionError",
    "WinError",
    "checkpoint_conflict",
    "duplicate_inserts",
    "Traceback",
    "FAILED_RECOVERABLE",
    "FAILED_TERMINAL",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read-only monitor for CRPD full-sync progress and health."
    )
    parser.add_argument(
        "--watch",
        action="store_true",
        help="Refresh continuously until Ctrl+C.",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=30,
        help="Refresh interval in seconds (default: 30).",
    )
    parser.add_argument(
        "--max-errors",
        type=int,
        default=15,
        help="Maximum recent error lines to show (default: 15).",
    )
    return parser.parse_args()


def read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8-sig", errors="replace")
    except (OSError, UnicodeError):
        return ""


def read_json(path: Path | None) -> Any | None:
    if path is None or not path.is_file():
        return None
    text = read_text(path)
    if not text.strip():
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return extract_json(text)


def extract_json(text: str) -> Any | None:
    start = text.find("{")
    end = text.rfind("}")
    if start < 0 or end <= start:
        return None
    try:
        return json.loads(text[start : end + 1])
    except json.JSONDecodeError:
        return None


def run_status() -> dict[str, Any]:
    command = [
        sys.executable,
        "-m",
        "policydb.autopilot_cli",
        "full-sync",
        "status",
        "--scope",
        "all",
    ]
    try:
        result = subprocess.run(
            command,
            cwd=REPO,
            text=True,
            encoding="utf-8",
            errors="replace",
            capture_output=True,
            timeout=180,
            check=False,
        )
        return {
            "exit_code": result.returncode,
            "stdout": result.stdout,
            "stderr": result.stderr,
            "json": extract_json(result.stdout),
        }
    except (OSError, subprocess.SubprocessError) as exc:
        return {
            "exit_code": -1,
            "stdout": "",
            "stderr": str(exc),
            "json": None,
        }


def iter_files(root: Path) -> Iterable[Path]:
    if not root.is_dir():
        return ()
    try:
        return (path for path in root.rglob("*") if path.is_file())
    except OSError:
        return ()


def latest_named_file(names: list[str]) -> Path | None:
    name_set = {name.lower() for name in names}
    candidates: list[Path] = []
    for root in (CONTINUOUS_ROOT, FULL_SYNC_ROOT):
        for path in iter_files(root):
            if path.name.lower() in name_set:
                candidates.append(path)
    if not candidates:
        return None
    return max(candidates, key=lambda path: safe_mtime(path))


def safe_mtime(path: Path) -> float:
    try:
        return path.stat().st_mtime
    except OSError:
        return 0.0


def find_value(obj: Any, names: Iterable[str]) -> Any | None:
    wanted = {name.lower() for name in names}
    if isinstance(obj, dict):
        for key, value in obj.items():
            if str(key).lower() in wanted and value is not None:
                return value
        for value in obj.values():
            found = find_value(value, wanted)
            if found is not None:
                return found
    elif isinstance(obj, (list, tuple)):
        for value in obj:
            found = find_value(value, wanted)
            if found is not None:
                return found
    return None


def first_value(objects: Iterable[Any], *names: str) -> Any | None:
    for obj in objects:
        if obj is None:
            continue
        found = find_value(obj, names)
        if found is not None:
            return found
    return None


def to_number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(str(value).replace(",", "").strip())
    except (ValueError, TypeError):
        return None


def to_ratio(value: Any) -> float | None:
    number = to_number(value)
    if number is None:
        return None
    if 1 < number <= 100:
        number /= 100
    return max(0.0, min(1.0, number))


def fmt_number(value: Any) -> str:
    number = to_number(value)
    return "N/A" if number is None else f"{number:,.0f}"


def fmt_percent(value: Any) -> str:
    ratio = to_ratio(value)
    return "N/A" if ratio is None else f"{ratio * 100:,.1f}%"


def fmt_text(value: Any) -> str:
    if value is None or str(value).strip() == "":
        return "N/A"
    return str(value)


def progress_bar(value: Any, width: int = 25) -> str:
    ratio = to_ratio(value)
    if ratio is None:
        return "[" + "?" * width + "]"
    filled = round(ratio * width)
    filled = max(0, min(width, filled))
    return "[" + "#" * filled + "-" * (width - filled) + "]"


def process_alive(pid: Any) -> bool:
    number = to_number(pid)
    if number is None or number <= 0:
        return False
    if os.name != "nt":
        try:
            os.kill(int(number), 0)
            return True
        except OSError:
            return False
    try:
        result = subprocess.run(
            ["tasklist", "/FI", f"PID eq {int(number)}", "/FO", "CSV", "/NH"],
            text=True,
            encoding="utf-8",
            errors="replace",
            capture_output=True,
            timeout=15,
            check=False,
        )
        output = result.stdout.strip().lower()
        return str(int(number)) in output and "no tasks" not in output
    except (OSError, subprocess.SubprocessError):
        return False


def get_process_state() -> dict[str, Any]:
    lock = read_json(LOCK_FILE)
    pid = find_value(lock, ("pid",)) if lock is not None else None
    return {
        "lock_exists": LOCK_FILE.is_file(),
        "pid": pid,
        "alive": process_alive(pid),
        "stop_requested": STOP_FILE.is_file(),
    }


def health_status(
    source_coverage: Any,
    historical_coverage: Any,
    terminal_ratio: Any,
    field_completeness: Any,
    freshness: Any,
    open_gaps: Any,
    critical_gaps: Any,
    conflicts: Any,
    duplicates: Any,
    failed_sources: Any,
) -> dict[str, Any]:
    critical_values = (critical_gaps, conflicts, duplicates)
    if any((to_number(value) or 0) > 0 for value in critical_values):
        return {
            "status": "CRITICAL",
            "score": 0.0,
            "reason": "critical gap、checkpoint 冲突或重复插入不为 0",
        }

    ratios = [
        to_ratio(value)
        for value in (
            source_coverage,
            historical_coverage,
            terminal_ratio,
            field_completeness,
            freshness,
        )
    ]
    ratios = [value for value in ratios if value is not None]
    if not ratios:
        return {
            "status": "UNKNOWN",
            "score": None,
            "reason": "当前状态文件没有提供足够的覆盖率字段",
        }

    score = sum(ratios) / len(ratios) * 100
    score -= min(20.0, (to_number(open_gaps) or 0.0) * 0.1)
    score -= min(25.0, (to_number(failed_sources) or 0.0) * 2.0)
    score = round(max(0.0, score), 1)
    status = "HEALTHY" if score >= 85 else "DEGRADED" if score >= 60 else "CRITICAL"
    return {
        "status": status,
        "score": score,
        "reason": "覆盖率均值扣除开放缺口和失败来源惩罚",
    }


def recent_errors(limit: int) -> list[dict[str, str]]:
    files: list[Path] = []
    for root in (CONTINUOUS_ROOT, FULL_SYNC_ROOT):
        for path in iter_files(root):
            if path.suffix.lower() in {".log", ".json", ".jsonl", ".txt"}:
                files.append(path)
    files.sort(key=safe_mtime, reverse=True)

    hits: list[dict[str, str]] = []
    for path in files[:40]:
        text = read_text(path)
        for line in text.splitlines():
            if any(term.lower() in line.lower() for term in ERROR_TERMS):
                hits.append(
                    {
                        "file": str(path),
                        "line": line.strip()[:350],
                        "modified_at": datetime.fromtimestamp(
                            safe_mtime(path), tz=UTC
                        ).isoformat(),
                    }
                )
                if len(hits) >= limit:
                    return hits
    return hits


def print_metric(label: str, value: Any, kind: str = "number") -> None:
    if kind == "ratio":
        display = fmt_percent(value)
    elif kind == "text":
        display = fmt_text(value)
    else:
        display = fmt_number(value)
    print(f"{label:<30} {display:>18}")


def clear_screen() -> None:
    os.system("cls" if os.name == "nt" else "clear")


def show_dashboard(max_errors: int) -> dict[str, Any]:
    status_result = run_status()
    process = get_process_state()

    files = {
        category: latest_named_file(names) for category, names in JSON_NAMES.items()
    }
    objects: list[Any] = [status_result["json"]]
    for path in files.values():
        value = read_json(path)
        if value is not None:
            objects.append(value)

    total_slots = first_value(objects, "total_slots", "required_slots", "slot_count")
    total_slots = EXPECTED_SLOTS if total_slots is None else total_slots
    resolved = first_value(objects, "resolved_slots", "slots_resolved")
    verified = first_value(objects, "verified_slots", "slots_verified")
    enabled = first_value(
        objects, "enabled_slots", "strict_enabled_slots", "slots_enabled"
    )
    ready = first_value(objects, "crawl_ready_slots", "ready_slots")
    backfilled = first_value(objects, "backfilled_slots", "slots_backfilled")
    current = first_value(objects, "current_slots", "slots_current")
    unresolved = first_value(objects, "unresolved_slots", "slots_unresolved")
    human_review = first_value(objects, "human_review_slots", "manual_review_slots")

    documents = first_value(objects, "total_documents", "documents_total", "document_count")
    added = first_value(objects, "documents_added", "added_documents", "inserted_documents")
    updated = first_value(objects, "documents_updated", "updated_documents")
    unchanged = first_value(objects, "documents_unchanged", "unchanged_documents")
    failed_documents = first_value(objects, "documents_failed", "failed_documents")
    discovered_links = first_value(objects, "article_links_discovered", "discovered_article_links")
    terminal_links = first_value(objects, "article_links_terminal", "terminal_article_links")

    source_coverage = first_value(objects, "source_coverage_ratio", "source_coverage")
    historical_coverage = first_value(
        objects, "historical_coverage_ratio", "historical_coverage", "backfill_ratio"
    )
    terminal_ratio = first_value(objects, "article_terminal_ratio", "terminal_ratio")
    field_completeness = first_value(
        objects, "field_completeness_ratio", "field_completeness"
    )
    parse_success = first_value(objects, "parse_success_ratio", "parse_success")
    freshness = first_value(objects, "freshness_ratio", "freshness")
    overall_completeness = first_value(
        objects, "overall_completeness", "overall_completeness_ratio"
    )

    if terminal_ratio is None:
        discovered_number = to_number(discovered_links)
        terminal_number = to_number(terminal_links)
        if discovered_number and terminal_number is not None:
            terminal_ratio = terminal_number / discovered_number

    open_gaps = first_value(objects, "open_gaps", "open_gap_count")
    critical_gaps = first_value(objects, "critical_gaps", "critical_gap_count")
    repairable_gaps = first_value(objects, "repairable_gaps")
    failed_sources = first_value(objects, "failed_sources", "source_failures")
    degraded_sources = first_value(objects, "degraded_sources")
    conflicts = first_value(objects, "checkpoint_conflicts", "checkpoint_conflict_count")
    duplicates = first_value(objects, "duplicate_inserts", "duplicate_insert_count")

    cycle = first_value(objects, "cycle", "current_cycle")
    run_id = first_value(objects, "run_id", "current_run_id")
    global_status = first_value(objects, "global_status", "status")
    planned_slots = first_value(objects, "planned_slots")
    planned_sources = first_value(objects, "planned_sources")

    total_number = to_number(total_slots)

    def slot_ratio(value: Any) -> float | None:
        number = to_number(value)
        if total_number is None or total_number <= 0 or number is None:
            return None
        return number / total_number

    verified_ratio = slot_ratio(verified)
    enabled_ratio = slot_ratio(enabled)
    backfill_progress = slot_ratio(backfilled)
    current_ratio = slot_ratio(current)

    health = health_status(
        source_coverage,
        historical_coverage,
        terminal_ratio,
        field_completeness,
        freshness,
        open_gaps,
        critical_gaps,
        conflicts,
        duplicates,
        failed_sources,
    )
    errors = recent_errors(max_errors)

    print("=" * 64)
    print("CRPD 全量抓取进度与成果健康度")
    print("=" * 64)
    print(f"检查时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"总体健康：{health['status']}  分数：{fmt_text(health['score'])}")
    print(f"判定依据：{health['reason']}")
    print()

    print("【运行状态】")
    print_metric("自动化锁存在", process["lock_exists"], "text")
    print_metric("自动化进程存活", process["alive"], "text")
    print_metric("PID", process["pid"])
    print_metric("安全停止已请求", process["stop_requested"], "text")
    print_metric("cycle", cycle)
    print_metric("run_id", run_id, "text")
    print_metric("global_status", global_status, "text")
    print_metric("status 退出码", status_result["exit_code"])
    print()

    print("【525 个来源槽位】")
    print_metric("总槽位", total_slots)
    print_metric("已解析", resolved)
    print_metric("已验证", verified)
    print_metric("严格启用", enabled)
    print_metric("crawl ready", ready)
    print_metric("已历史回溯", backfilled)
    print_metric("CURRENT", current)
    print_metric("未解析", unresolved)
    print_metric("人工审核", human_review)
    print(f"验证进度   {progress_bar(verified_ratio)} {fmt_percent(verified_ratio)}")
    print(f"启用进度   {progress_bar(enabled_ratio)} {fmt_percent(enabled_ratio)}")
    print(f"回溯进度   {progress_bar(backfill_progress)} {fmt_percent(backfill_progress)}")
    print(f"CURRENT    {progress_bar(current_ratio)} {fmt_percent(current_ratio)}")
    print()

    print("【本轮与累计成果】")
    print_metric("本轮计划槽位", planned_slots)
    print_metric("本轮计划来源", planned_sources)
    print_metric("累计政策文档", documents)
    print_metric("新增文档", added)
    print_metric("更新版本", updated)
    print_metric("内容未变化", unchanged)
    print_metric("失败文档", failed_documents)
    print_metric("发现文章链接", discovered_links)
    print_metric("终态文章链接", terminal_links)
    print()

    print("【成果质量】")
    print_metric("来源覆盖度", source_coverage, "ratio")
    print_metric("历史覆盖度", historical_coverage, "ratio")
    print_metric("文章终态率", terminal_ratio, "ratio")
    print_metric("字段完整率", field_completeness, "ratio")
    print_metric("解析成功率", parse_success, "ratio")
    print_metric("新鲜度", freshness, "ratio")
    print_metric("综合完整度", overall_completeness, "ratio")
    print()

    print("【风险门禁】")
    print_metric("开放 gaps", open_gaps)
    print_metric("critical gaps", critical_gaps)
    print_metric("可自动修复 gaps", repairable_gaps)
    print_metric("失败来源", failed_sources)
    print_metric("降级来源", degraded_sources)
    print_metric("checkpoint 冲突", conflicts)
    print_metric("重复插入", duplicates)
    print()

    print("【最近错误线索】")
    if errors:
        for item in errors:
            print(f"- {item['line']}")
    else:
        print("未在最近日志中检出高风险关键词。")

    if status_result["stderr"].strip() and status_result["exit_code"] not in (0, 10):
        print()
        print("【status stderr】")
        print(status_result["stderr"].strip())

    snapshot = {
        "generated_at": datetime.now(UTC).isoformat(),
        "process": process,
        "run": {
            "cycle": cycle,
            "run_id": run_id,
            "global_status": global_status,
            "status_exit_code": status_result["exit_code"],
        },
        "slots": {
            "total": total_slots,
            "resolved": resolved,
            "verified": verified,
            "enabled": enabled,
            "crawl_ready": ready,
            "backfilled": backfilled,
            "current": current,
            "unresolved": unresolved,
            "human_review": human_review,
            "verified_ratio": verified_ratio,
            "enabled_ratio": enabled_ratio,
            "backfill_ratio": backfill_progress,
            "current_ratio": current_ratio,
        },
        "documents": {
            "total": documents,
            "added": added,
            "updated": updated,
            "unchanged": unchanged,
            "failed": failed_documents,
            "links_discovered": discovered_links,
            "links_terminal": terminal_links,
        },
        "quality": {
            "source_coverage_ratio": source_coverage,
            "historical_coverage_ratio": historical_coverage,
            "article_terminal_ratio": terminal_ratio,
            "field_completeness_ratio": field_completeness,
            "parse_success_ratio": parse_success,
            "freshness_ratio": freshness,
            "overall_completeness": overall_completeness,
        },
        "risks": {
            "open_gaps": open_gaps,
            "critical_gaps": critical_gaps,
            "repairable_gaps": repairable_gaps,
            "failed_sources": failed_sources,
            "degraded_sources": degraded_sources,
            "checkpoint_conflicts": conflicts,
            "duplicate_inserts": duplicates,
        },
        "health": health,
        "source_files": {key: str(value) if value else None for key, value in files.items()},
        "recent_errors": errors,
    }

    MONITOR_ROOT.mkdir(parents=True, exist_ok=True)
    snapshot_path = MONITOR_ROOT / "CURRENT_HEALTH_SNAPSHOT.json"
    snapshot_path.write_text(
        json.dumps(snapshot, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    print()
    print(f"监控快照：{snapshot_path}")
    return snapshot


def main() -> int:
    args = parse_args()
    if args.interval < 5:
        print("--interval 至少为 5 秒。", file=sys.stderr)
        return 2
    if not REPO.is_dir():
        print(f"仓库不存在：{REPO}", file=sys.stderr)
        return 2
    MONITOR_ROOT.mkdir(parents=True, exist_ok=True)

    try:
        while True:
            if args.watch:
                clear_screen()
            show_dashboard(max_errors=max(0, args.max_errors))
            if not args.watch:
                return 0
            print(f"\n{args.interval} 秒后刷新；按 Ctrl+C 退出。")
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\n监控已停止。")
        return 0
    except Exception as exc:  # noqa: BLE001
        failure = {
            "generated_at": datetime.now(UTC).isoformat(),
            "monitor_status": "FAILED",
            "error": repr(exc),
        }
        MONITOR_ROOT.mkdir(parents=True, exist_ok=True)
        (MONITOR_ROOT / "MONITOR_FAILURE.json").write_text(
            json.dumps(failure, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        print(f"监控失败：{exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
