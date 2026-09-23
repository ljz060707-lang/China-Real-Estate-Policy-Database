"""Render read-only city-by-year progress for the recent-five-year crawl."""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any

import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DATA_ROOT = Path(r"E:\Data Set\CRPD")
PROGRESS_FILE = "city_year_progress.parquet"
COMPLETE_STATUSES = {"certified_complete", "confirmed_zero", "complete"}


def _configure_output() -> None:
    """Keep Chinese city names readable when invoked by Windows PowerShell."""

    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        value = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _as_year(value: Any, *, end: bool = False) -> int | None:
    if value is None:
        return None
    text = str(value).strip().lower()
    if text == "today":
        return date.today().year
    try:
        return date.fromisoformat(text[:10]).year
    except ValueError:
        try:
            return int(text[:4])
        except ValueError:
            return None


def configured_years(config_path: Path) -> list[int]:
    """Return years from the active crawl configuration, not from hardcoded UI data."""

    config = _read_json(config_path)
    crawl = config.get("crawl") if isinstance(config.get("crawl"), dict) else {}
    start = _as_year(crawl.get("start_date") or config.get("start_date"))
    end = _as_year(crawl.get("end_date") or config.get("end_date"), end=True)
    if start is None or end is None or end < start:
        return []
    return list(range(start, end + 1))


def _bar(value: Any, *, width: int = 14) -> str:
    if value is None:
        return "[??????????????]   --.-%"
    try:
        number = max(0.0, min(100.0, float(value)))
    except (TypeError, ValueError):
        return "[??????????????]   --.-%"
    filled = int(round(width * number / 100.0))
    return f"[{('=' * filled).ljust(width, '.')}] {number:5.1f}%"


def _status_code(value: Any) -> str:
    status = str(value or "UNOBSERVED").upper()
    if status.lower() in COMPLETE_STATUSES:
        return "COMPLETE"
    if status == "RUNNING":
        return "RUNNING"
    if status.startswith("PARTIAL"):
        return "PARTIAL"
    if status == "SOURCE_INCOMPLETE":
        return "INCOMPLETE"
    if status == "NOT_STARTED":
        return "NOT_STARTED"
    return status[:12]


def _load_progress(path: Path) -> tuple[dict[str, dict[int, dict[str, Any]]], str | None]:
    if not path.exists():
        return {}, "NOT_AVAILABLE"
    try:
        frame = pl.read_parquet(path)
    except Exception as exc:  # noqa: BLE001 - a monitor must keep refreshing after a swap
        return {}, f"UNREADABLE: {type(exc).__name__}"
    required = {"city_id", "city_name", "province_name", "year", "overall_completion_pct", "status"}
    missing = sorted(required.difference(frame.columns))
    if missing:
        return {}, f"INVALID_SCHEMA: {','.join(missing)}"
    result: dict[str, dict[int, dict[str, Any]]] = {}
    for row in frame.iter_rows(named=True):
        city_id = str(row.get("city_id") or "")
        try:
            year = int(row["year"])
        except (TypeError, ValueError):
            continue
        result.setdefault(city_id, {})[year] = row
    return result, None


def _load_shard_progress(path: Path, *, start_date: str | None = None, end_date: str | None = None) -> tuple[dict[tuple[str, int], dict[str, Any]], str | None]:
    """Read durable crawl activity separately from strict quality completion."""

    if not path.exists():
        return {}, "NOT_AVAILABLE"
    try:
        frame = pl.read_parquet(path)
        required = {
            "city_id",
            "city_name",
            "province_name",
            "start_date",
            "pagination_exhausted",
            "fetched",
            "document_versions",
        }
        missing = sorted(required.difference(frame.columns))
        if missing:
            return {}, f"INVALID_SCHEMA: {','.join(missing)}"
        if "status" in frame.columns:
            frame = frame.filter(pl.col("status").fill_null("") != "split_parent")
        if start_date and "end_date" in frame.columns:
            frame = frame.filter(pl.col("end_date").cast(pl.String) >= start_date)
        if end_date:
            frame = frame.filter(pl.col("start_date").cast(pl.String) <= end_date)
        frame = frame.with_columns(
            pl.col("start_date")
            .cast(pl.String)
            .str.slice(0, 4)
            .cast(pl.Int32, strict=False)
            .alias("_year")
        )
        grouped = frame.filter(pl.col("_year").is_not_null()).group_by(
            ["city_id", "city_name", "province_name", "_year"]
        ).agg(
            pl.len().alias("shard_total"),
            pl.col("pagination_exhausted").fill_null(False).cast(pl.Int64).sum().alias("shards_exhausted"),
            pl.col("fetched").fill_null(0).cast(pl.Int64).sum().alias("fetched"),
            pl.col("document_versions").fill_null(0).cast(pl.Int64).sum().alias("document_versions"),
            pl.col("unique_candidate_count").fill_null(0).cast(pl.Int64).sum().alias("candidates")
            if "unique_candidate_count" in frame.columns
            else pl.lit(0).alias("candidates"),
            pl.col("failed").fill_null(0).cast(pl.Int64).sum().alias("failed")
            if "failed" in frame.columns
            else pl.lit(0).alias("failed"),
        )
    except Exception as exc:  # noqa: BLE001 - a monitor must survive atomic crawler swaps
        return {}, f"UNREADABLE: {type(exc).__name__}"

    result: dict[tuple[str, int], dict[str, Any]] = {}
    for row in grouped.iter_rows(named=True):
        shard_total = int(row.get("shard_total") or 0)
        exhausted = int(row.get("shards_exhausted") or 0)
        result[(str(row.get("city_id") or ""), int(row["_year"]))] = {
            "city_name": row.get("city_name"),
            "province_name": row.get("province_name"),
            "shard_total": shard_total,
            "shards_exhausted": exhausted,
            "checkpoint_pct": round(exhausted / shard_total * 100, 2) if shard_total else None,
            "fetched": int(row.get("fetched") or 0),
            "document_versions": int(row.get("document_versions") or 0),
            "candidates": int(row.get("candidates") or 0),
            "failed": int(row.get("failed") or 0),
        }
    return result, None


def _latest_formal_log(data_root: Path) -> Path | None:
    root = data_root / "logs" / "audited_full_backfill"
    if not root.exists():
        return None
    directories = [item for item in root.iterdir() if item.is_dir()]
    if not directories:
        return None
    latest = max(directories, key=lambda item: item.stat().st_mtime)
    path = latest / "master.log"
    return path if path.exists() else None


def _live_context(data_root: Path) -> dict[str, Any]:
    state = _read_json(data_root / "automation" / "RECENT_5Y_FULL_CRAWL_STATE.json")
    log_path = _latest_formal_log(data_root)
    lines: list[str] = []
    if log_path:
        try:
            with log_path.open("r", encoding="utf-8", errors="replace") as handle:
                lines = handle.readlines()[-500:]
        except OSError:
            lines = []
    current_city = state.get("current_city")
    current_index = state.get("current_city_index")
    city_total = state.get("city_total") or 105
    for line in lines:
        match = re.search(r"\[(\d+)/(\d+)\].*?开始：(.+?)（CITY_[A-Z0-9]+）", line)
        if match:
            current_index = int(match.group(1))
            city_total = int(match.group(2))
            current_city = match.group(3).strip()
        match = re.search(r"开始：policydb crawl exhaustive-city --city (.+?) --from", line)
        if match:
            current_city = match.group(1).strip()
    completed = sum("城市阶段结束" in line for line in lines)
    return {
        "run_id": state.get("run_id") or "UNKNOWN",
        "status": state.get("status") or "UNKNOWN",
        "reason": state.get("reason") or "",
        "current_city": current_city or "UNKNOWN",
        "current_index": current_index,
        "city_total": city_total,
        "completed_cities": completed,
        "updated_at": state.get("updated_at") or "UNKNOWN",
        "log_path": str(log_path) if log_path else "NOT_FOUND",
        "fetch_concurrency": state.get("fetch_concurrency") or "NOT_RECORDED",
        "per_host_concurrency": state.get("per_host_concurrency") or "NOT_RECORDED",
        "api_concurrency": state.get("api_concurrency") or "NOT_RECORDED",
    }


def render(data_root: Path, config_path: Path) -> str:
    records, error = _load_progress(data_root / "curated" / PROGRESS_FILE)
    state_path = data_root / "automation" / "RECENT_5Y_FULL_CRAWL_STATE.json"
    state = _read_json(state_path)
    effective_config = state_path if state.get("start_date") and state.get("end_date") else config_path
    years = configured_years(effective_config)
    config = _read_json(effective_config)
    window = config.get("crawl", config)
    start_date = window.get("start_date")
    end_date = window.get("end_date")
    if end_date == "today":
        end_date = date.today().isoformat()
    crawl_records, crawl_error = _load_shard_progress(data_root / "curated" / "crawl_shards.parquet", start_date=start_date, end_date=end_date)
    context = _live_context(data_root)
    now = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S %z")
    lines = [
        "=" * 110,
        "CRPD RECENT 5-YEAR CITY × YEAR PROGRESS (READ-ONLY)",
        "=" * 110,
        f"刷新时间: {now}",
        f"Run: {context['run_id']} | 状态: {context['status']} | 当前城市: {context['current_city']}",
        f"城市阶段: {context['completed_cities']}/{context['city_total']} completed | 数据范围: {years[0] if years else 'UNKNOWN'}..{years[-1] if years else 'UNKNOWN'}",
        f"运行配置: fetch_concurrency={context['fetch_concurrency']} | per_host_concurrency={context['per_host_concurrency']} | api_concurrency={context['api_concurrency']} | writer_policy=SINGLE (不是实时进程计数)",
        f"Progress source: {data_root / 'curated' / PROGRESS_FILE}",
        f"Crawl source: {data_root / 'curated' / 'crawl_shards.parquet'}",
        f"Formal log: {context['log_path']}",
        "",
        "CRAWL CHECKPOINT (分片/抓取层；不等同于严格质量完成度)",
        "口径：仅当前日期窗口的叶子分片，排除 split_parent；docs为分片文档引用（含缓存复用），不是去重政策总数。",
        "分类保持禁用；strict质量门槛保留，不能将strict=0解释为未抓取。",
    ]
    for name in (PROGRESS_FILE, "crawl_shards.parquet"):
        path = data_root / "curated" / name
        if path.exists():
            updated = datetime.fromtimestamp(path.stat().st_mtime).astimezone()
            age = (datetime.now().astimezone() - updated).total_seconds()
            lines.append(f"{name}: updated={updated.isoformat(timespec='seconds')} age={age:.0f}s" + (" SNAPSHOT_STALE (不是worker故障判定)" if age > 900 else ""))
    if error:
        lines.append(f"city_year_progress: {error} (不填充假进度；下一次刷新会重试)")
        return "\n".join(lines)
    if crawl_error:
        lines.append(f"crawl_shards: {crawl_error} (不填充假进度；下一次刷新会重试)")

    city_map: dict[str, dict[str, Any]] = {}
    for city_id, values in records.items():
        first = next(iter(values.values()))
        city_map[city_id] = {
            "city_id": city_id,
            "city_name": first.get("city_name") or city_id,
            "province_name": first.get("province_name") or "",
            "years": values,
        }
    for (city_id, _year), activity in crawl_records.items():
        city_map.setdefault(
            city_id,
            {
                "city_id": city_id,
                "city_name": activity.get("city_name") or city_id,
                "province_name": activity.get("province_name") or "",
                "years": {},
            },
        )
    cities = sorted(city_map.values(), key=lambda item: (str(item["province_name"]), str(item["city_name"])))
    for year in years:
        activities = [activity for (city_id, activity_year), activity in crawl_records.items() if activity_year == year]
        shard_total = sum(int(activity["shard_total"]) for activity in activities)
        shards_exhausted = sum(int(activity["shards_exhausted"]) for activity in activities)
        fetched = sum(int(activity["fetched"]) for activity in activities)
        documents = sum(int(activity["document_versions"]) for activity in activities)
        checkpoint_pct = shards_exhausted / shard_total * 100 if shard_total else None
        strict_rows = [item["years"].get(year) for item in cities if item["years"].get(year)]
        strict_complete = sum(
            str(row.get("status") or "").lower() in COMPLETE_STATUSES for row in strict_rows
        )
        lines.append(
            f"{year}: {_bar(checkpoint_pct)} shards={shards_exhausted}/{shard_total} "
            f"docs={documents} new_fetches={fetched} strict_complete={strict_complete}/{len(cities)}"
        )

    lines.extend(
        [
            "",
            "CITY × YEAR PROGRESS",
            "(每格 FETCH=已完成分片比例；strict=严格质量完成度；?=尚未观测，不等同于0)",
        ]
    )
    if not years:
        lines.append("配置未提供有效年份范围。")
        return "\n".join(lines)
    header = "城市".ljust(12) + " " + " ".join(str(year).center(29) for year in years)
    lines.append(header)
    lines.append("-" * len(header))
    for city in cities:
        city_name = str(city["city_name"] or city["city_id"] or "UNKNOWN")
        cells: list[str] = []
        for year in years:
            row = city["years"].get(year)
            activity = crawl_records.get((str(city["city_id"]), year))
            strict_value = row.get("overall_completion_pct") if row else None
            strict_text = "--" if strict_value is None else f"{float(strict_value):.1f}%"
            strict_status = _status_code(row.get("status") if row else "UNOBSERVED")
            if activity is None:
                cells.append(f"{_bar(None, width=10)} UNOBSERVED strict={strict_text} {strict_status}")
            else:
                cells.append(
                    f"{_bar(activity['checkpoint_pct'], width=10)} FETCH docs={activity['document_versions']} "
                    f"shards={activity['shards_exhausted']}/{activity['shard_total']} "
                    f"strict={strict_text} {strict_status}"
                )
        lines.append(city_name[:12].ljust(12) + " " + " ".join(cells))
    lines.extend(
        [
            "",
            "Ctrl+C 只关闭监控窗口，不停止计划任务或 crawler。",
            "本监控只读 Parquet/状态/日志；不调用 API，不执行数据库写入，不刷新或重建队列。",
        ]
    )
    return "\n".join(lines)


def main() -> int:
    _configure_output()
    parser = argparse.ArgumentParser(description="Read-only recent-five-year city/year progress monitor")
    parser.add_argument("--data-root", type=Path, default=DEFAULT_DATA_ROOT)
    parser.add_argument("--config", type=Path, default=REPO_ROOT / "config" / "crpd_autonomous.json")
    parser.add_argument("--once", action="store_true", help="render once; the PowerShell wrapper supplies the refresh loop")
    args = parser.parse_args()
    print(render(args.data_root, args.config))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
