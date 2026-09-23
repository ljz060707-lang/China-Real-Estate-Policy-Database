"""Read-only CRPD city/year progress monitor with rate-based ETA.

The crawler owns the curated data.  This monitor only reads Parquet/state files
and writes a tiny sampling file below ``runtime/progress_monitor`` so that ETA
is based on observed progress between refreshes.  It never calls the API and
never writes the database or crawler checkpoints.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import polars as pl

DEFAULT_DATA_ROOT = Path(r"E:\Data Set\CRPD")
DEFAULT_PROJECT_ROOT = Path(r"E:\policy-database")
STATE_FILES = {
    "RECENT_5Y": "RECENT_5Y_FULL_CRAWL_STATE.json",
    "JURISDICTIONAL": "JURISDICTIONAL_BACKFILL_STATE.json",
}
COMPLETE_STATUSES = {"certified_complete", "confirmed_zero", "complete"}


def _configure_output() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _parse_timestamp(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.astimezone()
    return parsed.astimezone()


def _processes() -> list[dict[str, Any]]:
    command = (
        "$p=Get-CimInstance Win32_Process -ErrorAction SilentlyContinue | "
        "Where-Object {$_.ProcessId -ne $PID -and $_.CommandLine -and $_.CommandLine -match "
        "'(?i)(run_jurisdictional_backfill|run_recent_5y_direct|policydb.exe.*crawl)'} | "
        "Select-Object ProcessId,ParentProcessId,Name,CommandLine; "
        "@($p)|ConvertTo-Json -Compress -Depth 3"
    )
    try:
        result = subprocess.run(
            ["powershell.exe", "-NoLogo", "-NoProfile", "-NonInteractive", "-Command", command],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=5,
            check=False,
        )
        raw = json.loads(result.stdout or "[]")
    except (OSError, subprocess.SubprocessError, json.JSONDecodeError):
        return []
    rows = raw if isinstance(raw, list) else [raw]
    return [row for row in rows if isinstance(row, dict)]


def _mode_from_processes(processes: list[dict[str, Any]]) -> str | None:
    commands = "\n".join(str(row.get("CommandLine") or "") for row in processes).lower()
    if "jurisdictional" in commands:
        return "JURISDICTIONAL"
    if "recent_5y" in commands or "exhaustive-city" in commands:
        return "RECENT_5Y"
    return None


def _state_context(data_root: Path) -> dict[str, Any]:
    states = {
        mode: _read_json(data_root / "automation" / filename)
        for mode, filename in STATE_FILES.items()
    }
    processes = _processes()
    active_mode = _mode_from_processes(processes)
    if active_mode:
        selected_mode = active_mode
    else:
        candidates = [
            (mode, _parse_timestamp(state.get("updated_at")))
            for mode, state in states.items()
            if state
        ]
        selected_mode = max(candidates, key=lambda item: item[1] or datetime.min.astimezone())[0] if candidates else None
    state = states.get(selected_mode or "", {})
    status = "RUNNING" if processes else str(state.get("status") or "UNKNOWN")
    return {
        "mode": selected_mode or "UNKNOWN",
        "status": status,
        "state": state,
        "states": states,
        "processes": processes,
        "active": bool(processes),
    }


def _load_activity(data_root: Path, state: dict[str, Any]) -> tuple[dict[tuple[str, int], dict[str, Any]], str | None]:
    path = data_root / "curated" / "crawl_shards.parquet"
    if not path.exists():
        return {}, "NOT_AVAILABLE"
    try:
        frame = pl.read_parquet(path)
        required = {"city_id", "city_name", "province_name", "start_date", "pagination_exhausted", "fetched", "document_versions"}
        missing = sorted(required.difference(frame.columns))
        if missing:
            return {}, f"INVALID_SCHEMA: {','.join(missing)}"
        if "status" in frame.columns:
            frame = frame.filter(pl.col("status").fill_null("") != "split_parent")
        start_date = str(state.get("start_date") or "")[:10]
        end_date = state.get("end_date")
        if start_date and "end_date" in frame.columns:
            frame = frame.filter(pl.col("end_date").cast(pl.String) >= start_date)
        if end_date and end_date != "today":
            frame = frame.filter(pl.col("start_date").cast(pl.String) <= str(end_date)[:10])
        frame = frame.with_columns(
            pl.col("start_date").cast(pl.String).str.slice(0, 4).cast(pl.Int32, strict=False).alias("_year")
        )
        frame = frame.filter(pl.col("_year").is_not_null())
        aggregations = [
            pl.len().alias("shard_total"),
            pl.col("pagination_exhausted").fill_null(False).cast(pl.Int64).sum().alias("shards_exhausted"),
            pl.col("fetched").fill_null(0).cast(pl.Int64).sum().alias("fetched"),
            pl.col("document_versions").fill_null(0).cast(pl.Int64).sum().alias("document_versions"),
        ]
        if "unique_candidate_count" in frame.columns:
            aggregations.append(pl.col("unique_candidate_count").fill_null(0).cast(pl.Int64).sum().alias("candidates"))
        else:
            aggregations.append(pl.lit(0).alias("candidates"))
        grouped = frame.group_by(["city_id", "city_name", "province_name", "_year"]).agg(aggregations)
    except Exception as exc:  # noqa: BLE001 - monitor must survive atomic crawler swaps
        return {}, f"UNREADABLE: {type(exc).__name__}"

    activity: dict[tuple[str, int], dict[str, Any]] = {}
    for row in grouped.iter_rows(named=True):
        total = int(row.get("shard_total") or 0)
        done = int(row.get("shards_exhausted") or 0)
        activity[(str(row.get("city_id") or ""), int(row["_year"]))] = {
            "city_name": row.get("city_name") or row.get("city_id"),
            "province_name": row.get("province_name") or "",
            "total": total,
            "done": done,
            "pct": round(done / total * 100, 2) if total else None,
            "fetched": int(row.get("fetched") or 0),
            "documents": int(row.get("document_versions") or 0),
            "candidates": int(row.get("candidates") or 0),
        }
    return activity, None


def _load_strict_progress(data_root: Path) -> dict[tuple[str, int], dict[str, Any]]:
    path = data_root / "curated" / "city_year_progress.parquet"
    if not path.exists():
        return {}
    try:
        frame = pl.read_parquet(path)
    except Exception:  # noqa: BLE001 - stale snapshots must not stop the monitor
        return {}
    required = {"city_id", "year", "overall_completion_pct", "status"}
    if not required.issubset(frame.columns):
        return {}
    return {
        (str(row.get("city_id") or ""), int(row["year"])): row
        for row in frame.iter_rows(named=True)
        if row.get("year") is not None
    }


def _bar(value: Any, width: int = 14) -> str:
    if value is None:
        return "[??????????????]   --.-%"
    try:
        number = max(0.0, min(100.0, float(value)))
    except (TypeError, ValueError):
        return "[??????????????]   --.-%"
    filled = int(round(width * number / 100.0))
    return f"[{('=' * filled).ljust(width, '.')}] {number:5.1f}%"


def _fmt_duration(hours: float | None) -> str:
    if hours is None or hours < 0:
        return "CALIBRATING"
    minutes = int(round(hours * 60))
    if minutes <= 0:
        return "<1 min"
    days, minutes = divmod(minutes, 1440)
    hours_part, minutes = divmod(minutes, 60)
    if days:
        return f"{days}d {hours_part}h {minutes}m"
    if hours_part:
        return f"{hours_part}h {minutes}m"
    return f"{minutes}m"


def _fmt_eta(eta: datetime | None, *, state: str) -> str:
    if eta is None:
        return state
    return eta.astimezone().strftime("%Y-%m-%d %H:%M:%S %z")


def _sample_path(data_root: Path) -> Path:
    return data_root / "runtime" / "progress_monitor" / "city_year_eta.json"


def _load_sample(path: Path) -> dict[str, Any]:
    return _read_json(path)


def _save_sample(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(temporary, path)


def _eta_for(done: int, total: int, previous: dict[str, Any], now: datetime, active: bool) -> tuple[str, float | None, datetime | None]:
    if total <= 0:
        return "NO_WORK_OBSERVED", None, None
    if done >= total:
        return "COMPLETE", 0.0, now
    if not active:
        return "PAUSED_OR_FAILED", None, None
    old_done = previous.get("done")
    old_at = _parse_timestamp(previous.get("observed_at"))
    if old_done is None or old_at is None:
        return "CALIBRATING", None, None
    elapsed = (now - old_at).total_seconds() / 3600
    delta = done - int(old_done)
    if elapsed <= 0 or delta <= 0:
        return "CALIBRATING", None, None
    rate = delta / elapsed
    eta = now + timedelta(hours=(total - done) / rate)
    return _fmt_eta(eta, state="CALIBRATING"), rate, eta


def _year_window(state: dict[str, Any]) -> list[int]:
    start = str(state.get("start_date") or "")[:4]
    end_value = state.get("end_date") or date.today().isoformat()
    end = str(date.today().year if end_value == "today" else end_value)[:4]
    try:
        start_year, end_year = int(start), int(end)
    except ValueError:
        return []
    return list(range(start_year, end_year + 1)) if end_year >= start_year else []


def render(data_root: Path) -> str:
    context = _state_context(data_root)
    state = context["state"]
    activity, activity_error = _load_activity(data_root, state)
    strict = _load_strict_progress(data_root)
    years = _year_window(state)
    now = datetime.now().astimezone()
    previous = _load_sample(_sample_path(data_root))

    year_stats: dict[int, dict[str, int]] = {}
    for (_city_id, year), row in activity.items():
        stats = year_stats.setdefault(year, {"done": 0, "total": 0, "docs": 0, "fetched": 0})
        stats["done"] += int(row["done"])
        stats["total"] += int(row["total"])
        stats["docs"] += int(row["documents"])
        stats["fetched"] += int(row["fetched"])
    total_done = sum(item["done"] for item in year_stats.values())
    total_work = sum(item["total"] for item in year_stats.values())
    old_years = previous.get("years") if isinstance(previous.get("years"), dict) else {}
    overall_status, rate, eta = _eta_for(total_done, total_work, previous, now, context["active"])

    sample = {
        "observed_at": now.isoformat(),
        "run_id": state.get("run_id"),
        "mode": context["mode"],
        "done": total_done,
        "total": total_work,
        "years": {str(year): {"done": item["done"], "total": item["total"]} for year, item in year_stats.items()},
    }
    _save_sample(_sample_path(data_root), sample)

    process_rows = context["processes"]
    pids = ",".join(str(row.get("ProcessId")) for row in process_rows if row.get("ProcessId")) or "NONE"
    latest_curated = max(
        (
            item.stat().st_mtime
            for item in (data_root / "curated").glob("*.parquet")
            if item.is_file()
        ),
        default=0,
    )
    latest_text = datetime.fromtimestamp(latest_curated).astimezone().strftime("%Y-%m-%d %H:%M:%S %z") if latest_curated else "N/A"

    lines = [
        "=" * 118,
        "CRPD LIVE CITY × YEAR PROGRESS + ETA (READ-ONLY)",
        "=" * 118,
        f"刷新时间: {now.strftime('%Y-%m-%d %H:%M:%S %z')}",
        f"实际服务: {context['mode']} | 状态: {context['status']} | Run: {state.get('run_id') or 'UNKNOWN'}",
        f"实际 crawler PID: {pids} | 当前有效进程数: {len(process_rows)}",
        f"最新 curated Parquet 写入: {latest_text}",
        f"总抓取进度: {_bar(total_done / total_work * 100 if total_work else None, 22)}  shards={total_done}/{total_work}",
        f"总速率: {rate:.1f} shards/hour" if rate is not None else "总速率: CALIBRATING",
        f"预计完成: {_fmt_eta(eta, state=overall_status)}",
        f"预计剩余: {_fmt_duration((eta - now).total_seconds() / 3600) if eta else overall_status}",
        f"数据范围: {years[0] if years else 'UNKNOWN'}..{years[-1] if years else 'UNKNOWN'} | monitor sample: {_sample_path(data_root)}",
        "",
        "YEAR SUMMARY (FETCH/SHARD COMPLETION; 不等同于 strict quality completion)",
    ]
    for year in years:
        item = year_stats.get(year, {"done": 0, "total": 0, "docs": 0, "fetched": 0})
        old = old_years.get(str(year), {}) if isinstance(old_years, dict) else {}
        state_text, year_rate, year_eta = _eta_for(item["done"], item["total"], old, now, context["active"])
        eta_text = _fmt_eta(year_eta, state=state_text)
        rate_text = f"{year_rate:.1f}/h" if year_rate is not None else "CALIBRATING"
        lines.append(
            f"{year}: {_bar(item['done'] / item['total'] * 100 if item['total'] else None)} "
            f"shards={item['done']}/{item['total']} docs={item['docs']} fetches={item['fetched']} "
            f"rate={rate_text} ETA={eta_text}"
        )
    if activity_error:
        lines.append(f"crawl_shards 读取状态: {activity_error}；不填充假进度。")

    city_map: dict[str, dict[str, Any]] = {}
    for (city_id, year), row in activity.items():
        city = city_map.setdefault(
            city_id,
            {"name": row["city_name"], "province": row["province_name"], "years": {}},
        )
        city["years"][year] = row
    for (city_id, year), row in strict.items():
        city = city_map.setdefault(city_id, {"name": city_id, "province": "", "years": {}})
        city["years"].setdefault(year, {})["strict"] = row

    lines.extend([
        "",
        "CITY × YEAR PROGRESS",
        "每格显示抓取分片进度；strict 是质量状态，两者分开；? 表示尚未观测。",
        "城市".ljust(14) + " " + " ".join(str(year).center(28) for year in years),
        "-" * (15 + 29 * len(years)),
    ])
    for city_id, city in sorted(city_map.items(), key=lambda item: (str(item[1]["province"]), str(item[1]["name"]))):
        cells = []
        for year in years:
            item = city["years"].get(year, {})
            pct = item.get("pct")
            strict_row = item.get("strict") or {}
            strict_pct = strict_row.get("overall_completion_pct")
            strict_text = "--" if strict_pct is None else f"{float(strict_pct):.1f}%"
            status = str(strict_row.get("status") or "UNOBSERVED")[:10]
            cells.append(
                f"{_bar(pct, 8)} docs={int(item.get('documents') or 0)} "
                f"strict={strict_text} {status}"
            )
        lines.append(str(city["name"] or city_id)[:14].ljust(14) + " " + " ".join(cells))
    lines.extend([
        "",
        "Ctrl+C 只关闭监控，不停止 crawler；监控不调用 API、不写数据库。",
        "ETA 规则：至少两次刷新且分片完成数真实增加后才计算；进程停止时显示 PAUSED_OR_FAILED。",
    ])
    return "\n".join(lines)


def main() -> int:
    _configure_output()
    parser = argparse.ArgumentParser(description="CRPD read-only city/year progress and ETA monitor")
    parser.add_argument("--data-root", type=Path, default=DEFAULT_DATA_ROOT)
    args = parser.parse_args()
    print(render(args.data_root))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
