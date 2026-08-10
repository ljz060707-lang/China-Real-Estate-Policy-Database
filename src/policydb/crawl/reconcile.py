from __future__ import annotations

import os
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from policydb.crawl.checkpoint import append_unique
from policydb.parquet_store import read_parquet_snapshot
from policydb.settings import Settings


def _path(settings: Settings, name: str) -> Path:
    return settings.curated / f"{name}.parquet"


def _backup_dirs(settings: Settings) -> list[str]:
    roots = [settings.backups]
    found: set[str] = set()
    for root in roots:
        if not root.exists():
            continue
        for path in root.glob("pre_full_inference_final_repair_*"):
            if path.is_dir():
                found.add(str(path.resolve()))
    return sorted(found)


def _active_processes(run_id: str) -> list[dict]:
    """Find another local process that explicitly names this run id."""
    try:
        import psutil  # type: ignore
    except Exception:
        return []
    result: list[dict] = []
    current = os.getpid()
    for process in psutil.process_iter(["pid", "name", "cmdline"]):
        if process.info.get("pid") == current:
            continue
        command = " ".join(process.info.get("cmdline") or [])
        name = str(process.info.get("name") or "").lower()
        # The CLI's parent shell necessarily repeats --run-id; it is not a
        # crawler worker.  Ignore one-shot -c diagnostics for the same reason.
        if name in {"powershell.exe", "pwsh.exe", "cmd.exe"}:
            continue
        if "reconcile_run_status" in command or "-c" in command:
            continue
        if run_id in command:
            result.append(
                {
                    "pid": process.info.get("pid"),
                    "name": process.info.get("name"),
                    "command_contains_run_id": True,
                }
            )
    return result


def reconcile_run_status(
    settings: Settings | None = None,
    *,
    run_id: str | None = None,
    apply: bool = False,
) -> dict:
    """Reconcile only safe planned runs whose checkpoint is already terminal.

    The default is a read-only plan.  Applying requires an external immutable
    backup marker and no process that names the selected run id.
    """
    settings = settings or Settings.discover()
    runs_path = _path(settings, "crawl_runs")
    checkpoints_path = _path(settings, "crawl_checkpoints")
    items_path = _path(settings, "crawl_items")
    errors_path = _path(settings, "fetch_errors")
    if not runs_path.exists():
        return {
            "apply": apply,
            "candidate_count": 0,
            "repaired_count": 0,
            "skipped_count": 0,
            "conflict_count": 0,
            "reasons": [{"reason": "crawl_runs_missing"}],
        }
    runs = read_parquet_snapshot(runs_path)
    checkpoints = read_parquet_snapshot(checkpoints_path) if checkpoints_path.exists() else pl.DataFrame()
    items = read_parquet_snapshot(items_path) if items_path.exists() else pl.DataFrame()
    errors = read_parquet_snapshot(errors_path) if errors_path.exists() else pl.DataFrame()
    selected = runs.filter(pl.col("run_id") == run_id) if run_id else runs
    before: list[dict] = []
    after: list[dict] = []
    repairs: list[dict] = []
    reasons: list[dict] = []
    conflicts = 0
    active_by_run: dict[str, list[dict]] = {}
    for row in selected.iter_rows(named=True):
        current_id = str(row["run_id"])
        checkpoint = (
            checkpoints.filter(pl.col("run_id") == current_id)
            if checkpoints.height
            else pl.DataFrame()
        )
        checkpoint_id = (
            str(checkpoint[0, "checkpoint_id"])
            if checkpoint.height and "checkpoint_id" in checkpoint.columns
            else None
        )
        cp_status = str(checkpoint[0, "status"]) if checkpoint.height else None
        before.append({"run": row, "checkpoint_status": cp_status})
        if str(row.get("status") or "") != "planned":
            reasons.append({"run_id": current_id, "reason": "run_not_planned"})
            continue
        if cp_status not in {"complete", "cancelled"}:
            reasons.append({"run_id": current_id, "reason": "checkpoint_not_terminal", "checkpoint_status": cp_status})
            continue
        active = _active_processes(current_id)
        active_by_run[current_id] = active
        if active:
            conflicts += 1
            reasons.append({"run_id": current_id, "reason": "active_process", "processes": active})
            continue
        if not apply:
            reasons.append({"run_id": current_id, "reason": "dry_run_candidate"})
        run_items = items.filter(pl.col("run_id") == current_id) if items.height else pl.DataFrame()
        fetched = run_items.filter(pl.col("status").is_in(["fetched", "unchanged"])).height if run_items.height else 0
        failed = run_items.filter(pl.col("status") == "failed").height if run_items.height else 0
        error_count = errors.filter(pl.col("run_id") == current_id).height if errors.height else 0
        update = dict(row)
        checkpoint_updated = str(checkpoint[0, "updated_at"]) if checkpoint.height and "updated_at" in checkpoint.columns else datetime.now(UTC).isoformat()
        update.update({
            "status": cp_status,
            "item_count": run_items.height,
            "fetched_count": fetched,
            "failed_count": failed,
            "finished_at": checkpoint_updated,
            "updated_at": datetime.now(UTC).isoformat(),
        })
        repair = {
            "run_id": current_id,
            "checkpoint_id": checkpoint_id,
            "status": cp_status,
            "item_count": run_items.height,
            "fetched_count": fetched,
            "failed_count": failed,
            "fetch_error_count": error_count,
            "before_status": row.get("status"),
        }
        repairs.append(repair)
        after.append({"run": update, "checkpoint_status": cp_status})
    backups = _backup_dirs(settings)
    backup_confirmed = bool(backups)
    if apply and not backup_confirmed:
        reasons.append({"reason": "immutable_backup_missing"})
        conflicts += len(repairs)
    if apply and backup_confirmed and repairs:
        append_unique(runs_path, [item["run"] for item in after], "run_id")
    return {
        "apply": apply,
        "run_id": run_id,
        "candidate_count": len(repairs),
        "repaired_count": len(repairs) if apply and backup_confirmed else 0,
        "skipped_count": max(selected.height - len(repairs), 0),
        "conflict_count": conflicts,
        "backup_confirmed": backup_confirmed,
        "backup_dirs": backups,
        "active_processes": active_by_run,
        "before": before,
        "after": after,
        "repairs": repairs,
        "reasons": reasons,
    }
