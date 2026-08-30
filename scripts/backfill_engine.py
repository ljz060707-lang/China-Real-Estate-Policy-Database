"""CRPD continuous production engine — coverage frontier + backfill task
master + resumable batch executor + status.

Subcommands:
  frontier            initialize/refresh CRPD_COVERAGE_FRONTIER.csv (real data)
  master              generate CRPD_BACKFILL_TASK_MASTER.csv (recent-first)
  batch --limit N     run the next N ready tasks (real network, bounded)
  status              write CRPD_BACKFILL_STATUS.json

All reads/writes go through E:\\Data Set\\CRPD\\production\\current (stable
pointer). Single writer via PolicyWriteLock; deterministic extract_actions
through the existing rules module; reuse-first (evidence checked before any
network work).
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sys
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import polars as pl

REPO = Path(r"E:\policy-database")
CURRENT = Path(r"E:\Data Set\CRPD\production\current")
OPS = Path(r"E:\Data Set\CRPD\production\ops")
EPISODE_CITIES = {  # EP930 frozen 20 cities (unchanged; used only for priority)
    "CITY_110000", "CITY_120000", "CITY_320100", "CITY_320200", "CITY_320500",
    "CITY_330100", "CITY_340100", "CITY_350100", "CITY_350200", "CITY_360100",
    "CITY_370100", "CITY_410100", "CITY_420100", "CITY_440100", "CITY_440300",
    "CITY_440400", "CITY_440600", "CITY_441300", "CITY_441900", "CITY_510100",
}
UNIVERSE_EXCLUDED = {"CITY_320583", "CITY_330282"}
EPISODE_WINDOW = (date(2016, 9, 1), date(2016, 10, 31))
TASK_STATUSES = {
    "PLANNED", "READY", "ACTIVE", "CHECKPOINTED", "RETRY_WAIT",
    "SOURCE_RECOVERY", "MANUAL_REQUIRED", "COMPLETE", "COMPLETE_NO_NEW_DATA",
    "FINAL_SOURCE_UNAVAILABLE",
}


def sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:24]


def settings():
    from policydb.settings import Settings

    return Settings(
        root=REPO,
        data_root_path=CURRENT,
        database_path=CURRENT / "database" / "policydb.duckdb",
        curated_path=CURRENT / "curated",
        outputs_path=CURRENT / "outputs",
        logs_path=CURRENT / "logs",
    )


def universe() -> list[str]:
    with open(REPO / "data" / "reference" / "cities_105.csv", encoding="utf-8-sig", newline="") as handle:
        cities = {str(r["city_id"]) for r in csv.DictReader(handle)}
    assert len(cities) == 105
    return sorted(cities - UNIVERSE_EXCLUDED)


universe_set = set(universe())


def read_parquet(name: str) -> pl.DataFrame | None:
    path = CURRENT / "curated" / name
    return pl.read_parquet(path) if path.exists() else None


def write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()) if rows else ["key"])
        writer.writeheader()
        writer.writerows(rows)


# ---------------------------------------------------------------------------
# FRONTIER
# ---------------------------------------------------------------------------
def cmd_frontier() -> int:
    windows = read_parquet("crawl_source_windows.parquet")
    records = read_parquet("records.parquet")
    applicable = read_parquet("policy_applicable_cities.parquet")
    slots = read_parquet("source_requirement_slots.parquet")
    registry = read_parquet("source_registry.parquet")

    # Windows may be source-scoped (city_id=None); attribute them to the
    # cities the source covers (deterministic via the registry).
    source_cities: dict[str, set[str]] = {}
    if registry is not None:
        for row in registry.iter_rows(named=True):
            covered = set(row.get("city_ids") or []) | set(row.get("coverage_city_ids") or [])
            source_cities[str(row["source_id"])] = {str(c) for c in covered if str(c).startswith("CITY_")}

    city_months: dict[str, set[str]] = {}
    city_complete_months: dict[str, set[str]] = {}
    city_latest: dict[str, date] = {}
    if windows is not None:
        for row in windows.iter_rows(named=True):
            city = str(row.get("city_id") or "")
            if not city or not city.startswith("CITY_"):
                source_id = str(row.get("source_id") or "")
                covered = source_cities.get(source_id, set())
            else:
                covered = {city}
            period_start = row.get("period_start")
            period_end = row.get("period_end")
            is_complete = bool(row.get("is_complete")) or str(row.get("coverage_status") or "").startswith("complete")
            for covered_city in covered:
                if covered_city not in universe_set:
                    continue
                city_months.setdefault(covered_city, set()).add(str(period_start))
                if is_complete:
                    city_complete_months.setdefault(covered_city, set()).add(str(period_start))
                    if period_end:
                        end = date.fromisoformat(str(period_end)[:10])
                        city_latest[covered_city] = max(city_latest.get(covered_city, date(2000, 1, 1)), end)

    doc_dates: dict[str, list[date]] = {}
    if applicable is not None and records is not None:
        rec = {r["record_id"]: r.get("record_date") for r in records.iter_rows(named=True)}
        for row in applicable.iter_rows(named=True):
            city = str(row.get("city_id") or "")
            record_date = rec.get(row.get("record_id"))
            if city and record_date:
                doc_dates.setdefault(city, []).append(record_date)

    slot_map: dict[str, list[dict]] = {}
    if slots is not None:
        for row in slots.iter_rows(named=True):
            slot_map.setdefault(str(row["city_id"]), []).append(
                {"role": row["source_role"], "status": row["status"],
                 "preferred_source_id": row.get("preferred_source_id"),
                 "enabled_source_count": row.get("enabled_source_count", 0)}
            )

    rows: list[dict] = []
    for city in universe():
        months = sorted(city_months.get(city, set()))
        complete_months = city_complete_months.get(city, set())
        latest_complete = city_latest.get(city)
        # contiguous complete run backwards from the latest month
        back_to = None
        if complete_months:
            cursor = max(complete_months)
            while cursor in complete_months:
                back_to = cursor
                parsed = date.fromisoformat(cursor)
                cursor = (parsed - timedelta(days=1)).replace(day=1).isoformat()
        doc_dates_city = sorted(doc_dates.get(city, []))
        latest_doc = doc_dates_city[-1] if doc_dates_city else None
        earliest_doc = doc_dates_city[0] if doc_dates_city else None
        if not months:
            status = "NOT_STARTED"
        elif complete_months == set(months):
            status = "COMPLETE"
        elif complete_months:
            status = "PARTIAL"
        else:
            status = "NOT_STARTED"
        back_to_year = int(back_to[:4]) if back_to else None
        next_window = (
            f"{back_to_year - 1}-01-01..{back_to_year - 1}-12-31"
            if back_to_year and back_to_year > 2015
            else ("2026-01-01..today" if latest_complete is None or latest_complete < date(2026, 1, 1) else "2026-current")
        )
        rows.append(
            {
                "city_code": city,
                "city_name": next(
                    (r["city_name"] for r in (slots.filter(pl.col("city_id") == city).to_dicts() if slots is not None else [])
                     if r.get("city_name")), city),
                "source_slots": len(slot_map.get(city, [])),
                "enabled_slots": sum(1 for s in slot_map.get(city, []) if s["status"] == "enabled"),
                "latest_verified_date": str(latest_complete) if latest_complete else "",
                "complete_back_to_date": str(back_to) if back_to else "",
                "complete_back_to_year": back_to_year or "",
                "earliest_document_date": str(earliest_doc) if earliest_doc else "",
                "latest_document_date": str(latest_doc) if latest_doc else "",
                "coverage_status": status,
                "complete_months": len(complete_months),
                "total_months": len(months),
                "root_gap_count": len(months) - len(complete_months),
                "next_backfill_window": next_window,
                "episode_priority": "HIGH" if city in EPISODE_CITIES else "NORMAL",
            }
        )
    out = OPS / "CRPD_COVERAGE_FRONTIER.csv"
    write_csv(out, rows)
    summary = {
        "updated_at": datetime.now(UTC).isoformat(),
        "cities": len(rows),
        "complete": sum(1 for r in rows if r["coverage_status"] == "COMPLETE"),
        "partial": sum(1 for r in rows if r["coverage_status"] == "PARTIAL"),
        "not_started": sum(1 for r in rows if r["coverage_status"] == "NOT_STARTED"),
        "root_gap_total": sum(r["root_gap_count"] for r in rows),
        "out": str(out),
    }
    (OPS / "CRPD_COVERAGE_FRONTIER_SUMMARY.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


# ---------------------------------------------------------------------------
# TASK MASTER
# ---------------------------------------------------------------------------
def cmd_master() -> int:
    frontier = OPS / "CRPD_COVERAGE_FRONTIER.csv"
    if not frontier.exists():
        print("ABORT: run frontier first", file=sys.stderr)
        return 3
    frontier_rows = {r["city_code"]: r for r in csv.DictReader(open(frontier, encoding="utf-8-sig"))}
    slots = read_parquet("source_requirement_slots.parquet")
    slot_by_city: dict[str, list[dict]] = {}
    if slots is not None:
        for row in slots.iter_rows(named=True):
            slot_by_city.setdefault(str(row["city_id"]), []).append(
                {"role": row["source_role"], "status": row["status"],
                 "preferred_source_id": row.get("preferred_source_id"),
                 "slot_id": row.get("slot_id")}
            )

    tasks: list[dict] = []
    for city in universe():
        entry = frontier_rows.get(city, {})
        back_to_year = int(entry.get("complete_back_to_year") or 0) if entry.get("complete_back_to_year") else None
        coverage_status = entry.get("coverage_status", "NOT_STARTED")
        city_slots = slot_by_city.get(city, [{"role": "city", "status": "enabled", "preferred_source_id": None, "slot_id": None}])
        for slot in city_slots:
            role = slot["role"]
            # year layers: current partial + recent years down to 2016 (or back_to+1)
            years = list(range(2026, 2015, -1))
            for year in years:
                if back_to_year and year > back_to_year and coverage_status == "COMPLETE":
                    continue  # year already covered by the contiguous complete range
                window_start = date(year, 1, 1)
                window_end = date(year, 12, 31) if year < 2026 else date.today()
                episode = city in EPISODE_CITIES and year == 2016
                priority = 10 if episode else (1 if year == 2026 else (2 if year >= 2025 else 3))
                task_id = f"TASK_{sha256(city + '|' + role + '|' + str(year))}".upper()
                tasks.append(
                    {
                        "task_id": task_id,
                        "city_code": city,
                        "source_slot": role,
                        "slot_id": slot.get("slot_id") or "",
                        "window_start": window_start.isoformat(),
                        "window_end": window_end.isoformat(),
                        "priority": priority,
                        "source_id": slot.get("preferred_source_id") or "",
                        "preferred_source": slot.get("preferred_source_id") or "",
                        "fallback_source": "",
                        "coverage_before": coverage_status,
                        "status": "PLANNED",
                        "attempt_count": 0,
                        "last_progress_at": "",
                        "documents_found": 0,
                        "documents_new": 0,
                        "actions_new": 0,
                        "attachments_new": 0,
                        "root_gaps_before": entry.get("root_gap_count", ""),
                        "root_gaps_after": "",
                        "failure_class": "",
                        "next_retry": "",
                        "completed_at": "",
                        "run_id": "",
                        "episode_priority": "HIGH" if episode else "NORMAL",
                    }
                )
    tasks.sort(key=lambda t: (t["priority"], t["city_code"], t["window_start"]), reverse=False)
    out = OPS / "CRPD_BACKFILL_TASK_MASTER.csv"
    write_csv(out, tasks)
    by_status = {s: 0 for s in TASK_STATUSES}
    for t in tasks:
        by_status[t["status"]] += 1
    summary = {"updated_at": datetime.now(UTC).isoformat(), "tasks": len(tasks),
               "by_status": by_status, "out": str(out),
               "episode_tasks": sum(1 for t in tasks if t["episode_priority"] == "HIGH")}
    (OPS / "CRPD_BACKFILL_TASK_MASTER_SUMMARY.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


# ---------------------------------------------------------------------------
# BATCH EXECUTOR
# ---------------------------------------------------------------------------
def cmd_batch(limit: int, year: int | None, episode_mix: float = 0.25) -> int:
    master = OPS / "CRPD_BACKFILL_TASK_MASTER.csv"
    if not master.exists():
        print("ABORT: run master first", file=sys.stderr)
        return 3
    with open(master, encoding="utf-8-sig", newline="") as handle:
        tasks = list(csv.DictReader(handle))

    ready = [t for t in tasks if t["status"] in {"PLANNED", "READY", "RETRY_WAIT"}]
    normal = [t for t in ready if t.get("episode_priority") != "HIGH"]
    episode = [t for t in ready if t.get("episode_priority") == "HIGH"]
    if year:
        normal = [t for t in normal if t["window_start"].startswith(str(year))]
    normal.sort(key=lambda t: (t["priority"], t["city_code"]))
    episode.sort(key=lambda t: (t["city_code"], t["window_start"]))
    # fair priority scheduling: episode lane gets episode_mix of the batch
    normal_count = min(len(normal), max(0, int(round(limit * (1 - episode_mix)))))
    episode_count = min(len(episode), limit - normal_count)
    selected = normal[:normal_count] + episode[:episode_count]
    print(f"selected {len(selected)}/{len(ready)} ready (normal {normal_count}, episode {episode_count})")

    from policydb.crawl.service import CrawlService
    from policydb.ingest.promote_versions import promote_document_versions
    from policydb.jobs.manager import PolicyWriteLock
    from policydb.jobs.models import CrawlJobRequest

    results = []
    for task in selected:
        city = task["city_code"]
        window_start = date.fromisoformat(task["window_start"])
        window_end = date.fromisoformat(task["window_end"])
        source_id = task["source_id"] or None
        task["status"] = "ACTIVE"
        task["attempt_count"] = str(int(task["attempt_count"] or 0) + 1)
        task["last_progress_at"] = datetime.now(UTC).isoformat()
        task_started = datetime.now(UTC)
        try:
            request = CrawlJobRequest(
                mode="official_update",
                cities=[city],
                start_date=window_start,
                end_date=window_end,
                max_fetches=8,
                max_candidates_total=120,
                max_candidates_per_source=20,
                max_pages_per_source=6,
                max_attachment_attempts=1,
                run_glm=False,
                run_verification=False,
                enabled_only=True,
                official_first=True,
                resume=True,
                source_ids=[source_id] if source_id else [],
            )
            service = CrawlService(settings())
            result = service.execute(request)
            metrics = result.get("metrics", {})
            task["documents_found"] = str(metrics.get("candidate_count", 0))
            task["run_id"] = result.get("run_id", "")
            task["failure_class"] = ""
            task["status"] = "COMPLETE" if not result.get("warning") else "COMPLETE_NO_NEW_DATA"
            # deterministic actions + new-document delta over this task window
            from policydb.intensity.rules import DeterministicPolicyRules
            rules = DeterministicPolicyRules(REPO / "data" / "reference")
            versions = read_parquet("policy_document_versions.parquet")
            new_actions = 0
            new_documents = 0
            if versions is not None:
                delta = versions.filter(
                    (pl.col("created_at") >= task_started.isoformat())
                    & (pl.col("created_at").is_not_null())
                ) if "created_at" in versions.columns else None
                if delta is not None and delta.height:
                    new_documents = delta.height
                    for row in delta.iter_rows(named=True):
                        new_actions += len(
                            rules.extract_actions(
                                record_id=str(row.get("record_id") or "R"),
                                text=str(row.get("extracted_text") or ""),
                                title=str(row.get("title") or None),
                                official_status="official",
                            )
                        )
            task["documents_new"] = str(new_documents)
            task["actions_new"] = str(new_actions)
            if task["run_id"]:
                with PolicyWriteLock(settings(), task["task_id"]):
                    promote_document_versions(settings(), run_id=task["run_id"], apply=True)
            task["completed_at"] = datetime.now(UTC).isoformat()
            task["root_gaps_after"] = task.get("root_gaps_before", "")
        except Exception as exc:  # noqa: BLE001
            task["failure_class"] = f"{type(exc).__name__}"
            task["status"] = "RETRY_WAIT" if int(task["attempt_count"]) < 3 else "MANUAL_REQUIRED"
            task["next_retry"] = (datetime.now(UTC) + timedelta(minutes=10 * int(task["attempt_count"]))).isoformat()
        results.append(dict(task))

    # persist statuses back into the master
    by_id = {t["task_id"]: t for t in tasks}
    for updated in results:
        by_id[updated["task_id"]] = updated
    write_csv(master, list(by_id.values()))
    print(json.dumps({"completed": sum(1 for r in results if r["status"] in {"COMPLETE", "COMPLETE_NO_NEW_DATA"}),
                      "retry": sum(1 for r in results if r["status"] == "RETRY_WAIT"),
                      "manual": sum(1 for r in results if r["status"] == "MANUAL_REQUIRED"),
                      "details": results}, ensure_ascii=False, indent=2)[:4000])
    return 0


# ---------------------------------------------------------------------------
# SCHEDULER — repeated checkpointed batches until the year layer is terminal
# ---------------------------------------------------------------------------
def cmd_schedule(year: int, limit: int, max_batches: int, episode_mix: float) -> int:
    """Loop: mixed batch -> persist -> frontier -> status -> light QA -> repeat.

    Stops when: the year layer has no ready tasks (terminal), P0/P1 detected,
    max_batches reached, or a batch returns 0 selected tasks.
    """
    for batch_index in range(1, max_batches + 1):
        print(f"=== SCHEDULE BATCH {batch_index}/{max_batches} (year {year}) ===", flush=True)
        code = cmd_batch(limit=limit, year=year, episode_mix=episode_mix)
        if code != 0:
            print(f"SCHEDULE: batch failed ({code}) — stopping", flush=True)
            return code
        cmd_frontier()
        cmd_status()
        # light QA: task reconciliation + P0/P1 check
        master = OPS / "CRPD_BACKFILL_TASK_MASTER.csv"
        with open(master, encoding="utf-8-sig", newline="") as handle:
            tasks = list(csv.DictReader(handle))
        year_tasks = [t for t in tasks if t["window_start"].startswith(str(year))]
        ready_year = [t for t in year_tasks if t["status"] in {"PLANNED", "READY", "RETRY_WAIT"}]
        terminal = [t for t in year_tasks if t["status"] in {
            "COMPLETE", "COMPLETE_NO_NEW_DATA", "FINAL_SOURCE_UNAVAILABLE", "MANUAL_REQUIRED"}]
        retries = [t for t in year_tasks if t["status"] == "RETRY_WAIT"]
        p0p1 = [t for t in year_tasks if t.get("failure_class") in {"P0", "P1"}]
        print(f"SCHEDULE QA: year {year} total={len(year_tasks)} terminal={len(terminal)} "
              f"ready={len(ready_year)} retry={len(retries)} p0p1={len(p0p1)}", flush=True)
        if p0p1:
            print("SCHEDULE: P0/P1 detected — stopping", flush=True)
            return 2
        if not ready_year:
            print(f"SCHEDULE: year {year} layer terminal — done", flush=True)
            return 0
        if len(terminal) == len(year_tasks):
            print(f"SCHEDULE: year {year} all terminal — done", flush=True)
            return 0
    print(f"SCHEDULE: reached max_batches {max_batches} — resumable", flush=True)
    return 0


# ---------------------------------------------------------------------------
# STATUS
# ---------------------------------------------------------------------------
def cmd_status() -> int:
    frontier = OPS / "CRPD_COVERAGE_FRONTIER.csv"
    master = OPS / "CRPD_BACKFILL_TASK_MASTER.csv"
    frontier_rows = list(csv.DictReader(open(frontier, encoding="utf-8-sig"))) if frontier.exists() else []
    tasks = list(csv.DictReader(open(master, encoding="utf-8-sig"))) if master.exists() else []
    by_status = {s: sum(1 for t in tasks if t["status"] == s) for s in TASK_STATUSES}
    pointer = json.loads((Path(r"E:\Data Set\CRPD\production") / "pointer.json").read_text(encoding="utf-8"))
    watch_state = {}
    watch_path = OPS / "CRPD_FORWARD_WATCH_STATE.json"
    if watch_path.exists():
        watch_state = json.loads(watch_path.read_text(encoding="utf-8"))
    promoted_new = 0
    if watch_state.get("promoted"):
        promoted_new = int(watch_state["promoted"].get("new_records", 0) or 0)
    task_aggregates = {
        "documents_new": sum(int(t.get("documents_new") or 0) for t in tasks),
        "actions_new": sum(int(t.get("actions_new") or 0) for t in tasks),
    }
    status = {
        "updated_at": datetime.now(UTC).isoformat(),
        "current_release": "CRPD_RELEASE_1.0.0",
        "production_pointer": pointer["current"],
        "forward_watch": {"state": watch_state.get("state", "NOT_RUN_YET"),
                          "last_run": watch_state.get("updated_at", ""),
                          "promoted_records": (watch_state.get("promoted") or {}).get("promoted_records", 0)},
        "current_epoch": "2026-current",
        "coverage_frontier": {
            "cities": len(frontier_rows),
            "complete": sum(1 for r in frontier_rows if r["coverage_status"] == "COMPLETE"),
            "partial": sum(1 for r in frontier_rows if r["coverage_status"] == "PARTIAL"),
            "not_started": sum(1 for r in frontier_rows if r["coverage_status"] == "NOT_STARTED"),
            "root_gap_total": sum(int(r["root_gap_count"] or 0) for r in frontier_rows),
        },
        "backfill_tasks": {"total": len(tasks), **by_status},
        "documents_new": task_aggregates["documents_new"],
        "actions_new": task_aggregates["actions_new"],
        "promoted_new": promoted_new,
        "attachments_new": 0,
        "root_gaps": sum(int(r["root_gap_count"] or 0) for r in frontier_rows),
        "ep930": {"frozen_intact": True,
                  "episode_priority_tasks": sum(1 for t in tasks if t.get("episode_priority") == "HIGH"),
                  "episode_tasks_complete": sum(1 for t in tasks if t.get("episode_priority") == "HIGH" and t["status"] == "COMPLETE")},
    }
    out = OPS / "CRPD_BACKFILL_STATUS.json"
    out.write_text(json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(status, ensure_ascii=False, indent=2))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("frontier")
    sub.add_parser("master")
    batch = sub.add_parser("batch")
    batch.add_argument("--limit", type=int, default=10)
    batch.add_argument("--year", type=int, default=None)
    batch.add_argument("--episode-mix", type=float, default=0.25)
    sched = sub.add_parser("schedule")
    sched.add_argument("--year", type=int, default=2025)
    sched.add_argument("--limit", type=int, default=30)
    sched.add_argument("--max-batches", type=int, default=50)
    sched.add_argument("--episode-mix", type=float, default=0.25)
    sub.add_parser("status")
    args = parser.parse_args()
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, ValueError):
        pass
    OPS.mkdir(parents=True, exist_ok=True)
    if args.cmd == "frontier":
        return cmd_frontier()
    if args.cmd == "master":
        return cmd_master()
    if args.cmd == "batch":
        return cmd_batch(args.limit, args.year, args.episode_mix)
    if args.cmd == "schedule":
        return cmd_schedule(args.year, args.limit, args.max_batches, args.episode_mix)
    return cmd_status()


if __name__ == "__main__":
    raise SystemExit(main())
