from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path

import duckdb
import pytest

from policydb.jobs import CrawlJobRequest, JobManager
from policydb.jobs.worker import run_job
from policydb.query import database as database_module
from policydb.settings import Settings


def _root(tmp_path: Path) -> Path:
    root = tmp_path / "policy-database"
    (root / "data" / "reference").mkdir(parents=True)
    (root / "data" / "curated").mkdir(parents=True)
    (root / "database").mkdir()
    (root / "pyproject.toml").write_text("[project]\nname='test'\n", encoding="utf-8")
    return root


def _wait(manager: JobManager, job_id: str, timeout: float = 15) -> object:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        state = manager.inspect_state(job_id)
        if state.status in {"completed", "completed_with_warnings", "failed", "cancelled"}:
            return state
        time.sleep(0.05)
    raise AssertionError(f"job did not finish: {manager.load_state(job_id)}")


def test_lightweight_estimate_does_not_construct_service():
    request = CrawlJobRequest(mode="smart", cities=["武汉市"], topics=["限购"])
    started = time.perf_counter()
    estimate = request.estimate(12)
    assert time.perf_counter() - started < 0.1
    assert estimate["source_count"] == 12 and estimate["query_count"] == 8


def test_background_start_returns_under_one_second(tmp_path):
    manager = JobManager(Settings(root=_root(tmp_path)))
    request = CrawlJobRequest(
        mode="smart",
        demo_mode=True,
        max_candidates=5,
        max_fetches=5,
        processing_mode="staged_only",
    )
    state = manager.create(request)
    started = time.perf_counter()
    manager.start(state.job_id)
    elapsed = time.perf_counter() - started
    final = _wait(manager, state.job_id)
    assert elapsed < 1.0
    assert final.status == "completed_with_warnings"
    assert (manager.job_dir(state.job_id) / "stderr.log").exists()
    assert (manager.job_dir(state.job_id) / "performance.jsonl").exists()


def test_staged_job_uses_workspace_and_preserves_curated(tmp_path):
    root = _root(tmp_path)
    sentinel = root / "data" / "curated" / "sentinel.bin"
    sentinel.write_bytes(b"stable")
    before = hashlib.sha256(sentinel.read_bytes()).hexdigest()
    settings = Settings(root=root)
    manager = JobManager(settings)
    state = manager.create(
        CrawlJobRequest(
            mode="smart",
            demo_mode=True,
            max_candidates=100,
            max_fetches=100,
            processing_mode="staged_only",
        )
    )
    result = run_job(state.job_id, settings)
    workspace = manager.workspace_dir(state.job_id)
    assert result["metrics"]["fetched"] == 100
    assert (workspace / "crawl_items.parquet").exists()
    assert (workspace / "policy_document_versions.parquet").exists()
    assert hashlib.sha256(sentinel.read_bytes()).hexdigest() == before


def test_state_updates_are_throttled(tmp_path):
    manager = JobManager(Settings(root=_root(tmp_path)))
    state = manager.create(CrawlJobRequest(mode="smart"))
    for index in range(100):
        manager.update(
            state.job_id,
            status="fetching",
            stage="fetching",
            progress_current=index,
            progress_total=100,
            processed_count=index,
            message=f"item {index}",
        )
    events = (manager.job_dir(state.job_id) / "events.jsonl").read_text(
        encoding="utf-8"
    ).splitlines()
    assert len(events) < 20


def test_cooperative_cancel_preserves_workspace(tmp_path):
    manager = JobManager(Settings(root=_root(tmp_path)))
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
    deadline = time.monotonic() + 8
    while time.monotonic() < deadline:
        current = manager.load_state(state.job_id)
        if current.stage == "fetching" and current.processed_count > 0:
            break
        time.sleep(0.05)
    cancel_started = time.monotonic()
    manager.cancel(state.job_id)
    final = _wait(manager, state.job_id, timeout=5)
    assert final.status == "cancelled"
    assert time.monotonic() - cancel_started < 5
    assert manager.workspace_dir(state.job_id).exists()


def test_performance_log_has_bounded_worker_threads(tmp_path):
    settings = Settings(root=_root(tmp_path))
    manager = JobManager(settings)
    state = manager.create(
        CrawlJobRequest(
            mode="smart",
            demo_mode=True,
            max_candidates=5,
            max_fetches=5,
            processing_mode="staged_only",
        )
    )
    manager.start(state.job_id)
    _wait(manager, state.job_id)
    samples = [
        json.loads(line)
        for line in (manager.job_dir(state.job_id) / "performance.jsonl")
        .read_text(encoding="utf-8")
        .splitlines()
    ]
    assert samples and max(sample["thread_count"] for sample in samples) <= 16


def test_atomic_database_rebuild_replaces_only_after_validation(tmp_path, monkeypatch):
    root = _root(tmp_path)
    target = root / "database" / "policydb.duckdb"
    with duckdb.connect(str(target)) as connection:
        connection.execute("CREATE TABLE records(id INTEGER)")
        connection.execute("INSERT INTO records VALUES (1)")
        connection.execute("CREATE VIEW v_data_quality AS SELECT 1 ok")

    def fake_build(settings, *, materialize_geography=True):
        del materialize_geography
        with duckdb.connect(str(settings.database)) as connection:
            connection.execute("INSERT INTO records VALUES (2)")
        return settings.database

    monkeypatch.setattr(database_module, "build_database", fake_build)
    database_module.build_database_atomic(Settings(root=root), "JOB_TEST")
    with duckdb.connect(str(target), read_only=True) as connection:
        assert connection.execute("SELECT count(*) FROM records").fetchone()[0] == 2


def test_database_swap_failure_keeps_old_database(tmp_path, monkeypatch):
    root = _root(tmp_path)
    target = root / "database" / "policydb.duckdb"
    with duckdb.connect(str(target)) as connection:
        connection.execute("CREATE TABLE records(id INTEGER)")
        connection.execute("INSERT INTO records VALUES (1)")
        connection.execute("CREATE VIEW v_data_quality AS SELECT 1 ok")

    monkeypatch.setattr(
        database_module,
        "build_database",
        lambda settings, **_: settings.database,
    )
    original_replace = database_module.os.replace

    def deny_database_replace(source, destination):
        if Path(destination) == target:
            raise PermissionError("busy")
        return original_replace(source, destination)

    monkeypatch.setattr(database_module.os, "replace", deny_database_replace)
    with pytest.raises(database_module.DatabaseSwapDeferred):
        database_module.build_database_atomic(Settings(root=root), "JOB_BUSY")
    with duckdb.connect(str(target), read_only=True) as connection:
        assert connection.execute("SELECT count(*) FROM records").fetchone()[0] == 1
