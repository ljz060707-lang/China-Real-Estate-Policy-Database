from __future__ import annotations

import argparse
import hashlib
import json
import time
import urllib.request
from pathlib import Path

import psutil

from policydb import PolicyDB
from policydb.jobs import CrawlJobRequest, JobManager
from policydb.settings import Settings

TERMINAL = {"completed", "completed_with_warnings", "failed", "cancelled"}


def sha256(path: Path) -> str | None:
    if not path.exists():
        return None
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def health(url: str) -> tuple[int, float]:
    started = time.perf_counter()
    with urllib.request.urlopen(url, timeout=2) as response:
        status = response.status
    return status, time.perf_counter() - started


def dashboard_process(port: int) -> psutil.Process | None:
    for connection in psutil.net_connections(kind="tcp"):
        if connection.status == psutil.CONN_LISTEN and connection.laddr.port == port:
            return psutil.Process(connection.pid) if connection.pid else None
    return None


def performance_summary(path: Path) -> dict:
    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    return {
        "worker_cpu_peak": max((row["cpu_percent"] for row in rows), default=0),
        "worker_rss_peak": max((row["rss_bytes"] for row in rows), default=0),
        "worker_threads_peak": max((row["thread_count"] for row in rows), default=0),
        "worker_read_bytes": max((row.get("read_bytes", 0) for row in rows), default=0),
        "worker_write_bytes": max((row.get("write_bytes", 0) for row in rows), default=0),
    }


def run_case(
    manager: JobManager,
    candidates: int,
    fetches: int,
    health_url: str,
    dashboard: psutil.Process | None,
    processing_mode: str = "staged_only",
) -> dict:
    request = CrawlJobRequest(
        mode="smart",
        demo_mode=True,
        max_candidates=candidates,
        max_fetches=fetches,
        processing_mode=processing_mode,
    )
    database_hash = sha256(manager.settings.database)
    state = manager.create(request)
    started = time.perf_counter()
    manager.start(state.job_id)
    start_elapsed = time.perf_counter() - started
    health_samples = []
    dashboard_cpu = []
    if dashboard:
        dashboard.cpu_percent(None)
    while True:
        current = manager.inspect_state(state.job_id)
        status, elapsed = health(health_url)
        health_samples.append({"status": status, "seconds": elapsed})
        if dashboard:
            dashboard_cpu.append(dashboard.cpu_percent(None))
        if current.status in TERMINAL:
            break
        time.sleep(0.1)
    PolicyDB.open(manager.settings.root)._query("SELECT count(*) FROM records").item()
    performance = performance_summary(manager.job_dir(state.job_id) / "performance.jsonl")
    return {
        "job_id": state.job_id,
        "candidate_count": candidates,
        "fetch_count": fetches,
        "processing_mode": processing_mode,
        "start_seconds": start_elapsed,
        "total_seconds": time.perf_counter() - started,
        "status": current.status,
        "health_all_200": all(item["status"] == 200 for item in health_samples),
        "health_latency_max_seconds": max(item["seconds"] for item in health_samples),
        "dashboard_cpu_peak": max(dashboard_cpu, default=0),
        "stable_database_unchanged": sha256(manager.settings.database) == database_hash,
        **performance,
    }


def run_cancel_case(manager: JobManager, health_url: str) -> dict:
    state = manager.create(
        CrawlJobRequest(
            mode="smart",
            demo_mode=True,
            max_candidates=1000,
            max_fetches=1000,
            processing_mode="staged_only",
        )
    )
    manager.start(state.job_id)
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        current = manager.load_state(state.job_id)
        if current.processed_count >= 20:
            break
        time.sleep(0.05)
    started = time.perf_counter()
    manager.cancel(state.job_id)
    samples = []
    while True:
        current = manager.inspect_state(state.job_id)
        samples.append(health(health_url)[0])
        if current.status in TERMINAL:
            break
        time.sleep(0.05)
    return {
        "job_id": state.job_id,
        "status": current.status,
        "cancel_seconds": time.perf_counter() - started,
        "processed_before_stop": current.processed_count,
        "health_all_200": all(status == 200 for status in samples),
        "workspace_preserved": manager.workspace_dir(state.job_id).exists(),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=str(Path(__file__).resolve().parents[1]))
    parser.add_argument("--port", type=int, default=8501)
    args = parser.parse_args()
    settings = Settings.discover(args.root)
    manager = JobManager(settings)
    health_url = f"http://127.0.0.1:{args.port}/_stcore/health"
    dashboard = dashboard_process(args.port)
    result = {
        "cases": [
            run_case(manager, 5, 5, health_url, dashboard),
            run_case(manager, 100, 100, health_url, dashboard),
            run_case(manager, 1000, 100, health_url, dashboard),
            run_case(manager, 5, 5, health_url, dashboard, "glm_verify"),
        ],
        "cancel": run_cancel_case(manager, health_url),
    }
    output = settings.root / "outputs" / "acceptance" / "crawl_responsiveness.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2))
    if not all(case["start_seconds"] < 1 and case["health_all_200"] for case in result["cases"]):
        raise SystemExit(1)
    if result["cancel"]["status"] != "cancelled":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
