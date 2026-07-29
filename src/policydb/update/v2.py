from __future__ import annotations

from datetime import date, timedelta

import yaml

from policydb.jobs import CrawlJobRequest, JobManager
from policydb.settings import Settings

LAYERS = {"daily", "weekly", "monthly", "quarterly"}


def load_update_schedule(settings: Settings | None = None) -> dict:
    settings = settings or Settings.discover()
    path = settings.root / "data" / "reference" / "update_schedule.yaml"
    if not path.exists():
        legacy = settings.root / "config" / "update_schedule.yaml"
        if legacy.exists():
            path = legacy
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def build_update_request(layer: str, settings: Settings | None = None) -> CrawlJobRequest:
    settings = settings or Settings.discover()
    if layer not in LAYERS:
        raise ValueError(f"unknown update layer: {layer}")
    config = load_update_schedule(settings)["layers"][layer]
    today = date.today()
    start = today - timedelta(days=int(config["overlap_days"]))
    return CrawlJobRequest(
        mode=str(config["mode"]),
        start_date=start,
        end_date=today,
        max_candidates=int(config["max_candidates"]),
        max_fetches=int(config["max_fetches"]),
        run_glm=bool(config["run_glm"]),
        run_verification=bool(config["run_verification"]),
        rebuild_database=bool(config["rebuild_database"]),
        run_validation=bool(config["run_validation"]),
        processing_mode=str(config["processing_mode"]),
    )


def start_update(layer: str, settings: Settings | None = None) -> dict:
    settings = settings or Settings.discover()
    manager = JobManager(settings)
    state = manager.create(build_update_request(layer, settings))
    started = manager.start(state.job_id)
    return {"job_id": state.job_id, "pid": started.pid, "layer": layer, "status": started.status}
