from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path

import polars as pl
import pytest

from policydb.episode_930_autorun import Episode930AutoRunner
from policydb.parquet_store import atomic_write_parquet
from policydb.settings import Settings


def _settings(tmp_path: Path) -> Settings:
    data = tmp_path / "data"
    (data / "control").mkdir(parents=True)
    (data / "logs").mkdir(parents=True)
    (data / "outputs").mkdir(parents=True)
    return Settings(
        root=tmp_path,
        data_root_path=data,
        outputs_path=data / "outputs",
        logs_path=data / "logs",
        control_path=data / "control",
    )


def test_runner_reads_queue_and_does_not_create_network_work(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    output = settings.outputs / "special_projects" / "2016_930"
    output.mkdir(parents=True)
    atomic_write_parquet(
        pl.DataFrame(
            [
                {"queue_item_id": "q1", "status": "PENDING"},
                {"queue_item_id": "q2", "status": "CRAWL_COMPLETED"},
                {"queue_item_id": "q3", "status": "RUNNING"},
                {"queue_item_id": "q4", "status": "FAILED"},
            ]
        ),
        output / "930_TASK_QUEUE.parquet",
        {"test": "autorun"},
        key_columns=("queue_item_id",),
    )
    runner = Episode930AutoRunner(settings, output=output)
    assert runner._queue_summary() == {
        "total": 4,
        "completed": 1,
        "pending": 1,
        "running": 1,
        "failed": 1,
    }
    request = runner._request()
    assert request.mode == "historical_episode_930"
    assert request.episode_id == "EP_2016_930_TIGHTENING"
    assert request.run_glm is False


def test_runner_stop_file_exits_without_starting_worker(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    output = settings.outputs / "special_projects" / "2016_930"
    stop = settings.control / "STOP_FULL_SYNC"
    stop.write_text("test-stop", encoding="utf-8")
    result = Episode930AutoRunner(
        settings,
        output=output,
        poll_seconds=2,
    ).run()
    assert result["status"] == "STOPPED"
    assert result["exit_code"] == 21
    assert result["current_job_id"] is None
    assert not (settings.logs / "policydb-write.lock").exists()
    events = (output / "930_AUTORUN_EVENTS.jsonl").read_text(encoding="utf-8")
    assert "runner_stopped_by_request" in events


def test_episode_specific_stop_file_is_respected(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    output = settings.outputs / "special_projects" / "2016_930"
    (settings.control / "STOP_EPISODE_930").write_text("test-stop", encoding="utf-8")
    runner = Episode930AutoRunner(settings, output=output)
    assert runner.stop_requested() is True


def test_runner_never_starts_while_formal_writer_lock_is_live(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    output = settings.outputs / "special_projects" / "2016_930"
    lock = settings.logs / "policydb-write.lock"
    lock.write_text(json.dumps({"pid": 999999999, "job_id": "JOB_EXISTING"}), encoding="utf-8")
    runner = Episode930AutoRunner(settings, output=output, poll_seconds=2)
    info = runner._active_write_lock()
    assert info is not None
    assert info["job_id"] == "JOB_EXISTING"
    assert info["path"] == str(lock)


def test_runner_does_not_call_failed_queue_complete(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    output = settings.outputs / "special_projects" / "2016_930"
    output.mkdir(parents=True)
    atomic_write_parquet(
        pl.DataFrame(
            [
                {"queue_item_id": "q1", "status": "FAILED"},
                {"queue_item_id": "q2", "status": "COMPLETED"},
            ]
        ),
        output / "930_TASK_QUEUE.parquet",
        {"test": "failed-queue"},
        key_columns=("queue_item_id",),
    )
    result = Episode930AutoRunner(settings, output=output).run()
    assert result["status"] == "BLOCKED"
    assert result["reason_code"] == "QUEUE_FAILED_ITEMS"
    assert result["exit_code"] == 20


def test_queue_complete_does_not_exit_while_final_convergence_is_pending(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = _settings(tmp_path)
    output = settings.outputs / "special_projects" / "2016_930"
    output.mkdir(parents=True)
    atomic_write_parquet(
        pl.DataFrame([{"queue_item_id": "q1", "status": "CRAWL_COMPLETED"}]),
        output / "930_TASK_QUEUE.parquet",
        {"test": "final-convergence-pending"},
        key_columns=("queue_item_id",),
    )
    (output / "930_MONITOR_SNAPSHOT.json").write_text(
        json.dumps(
            {
                "api_health": {
                    "pass1_waiting": 1,
                    "pass2_not_yet_eligible": 1,
                    "next_retry_at": "2000-01-01T00:00:00+00:00",
                }
            }
        ),
        encoding="utf-8",
    )

    runner = Episode930AutoRunner(settings, output=output)
    convergence = runner._final_convergence_state()
    assert convergence["pending"] is True
    assert convergence["due"] is True

    def start_would_be_reached(_manager):
        raise RuntimeError("formal convergence worker would be started")

    monkeypatch.setattr(runner, "_start_or_adopt", start_would_be_reached)
    with pytest.raises(RuntimeError, match="formal convergence worker"):
        runner.run()

    state = json.loads((output / "930_AUTORUN_STATE.json").read_text(encoding="utf-8"))
    assert state["status"] == "RUNNING"


def test_max_cycles_applies_to_this_resume_invocation_not_persisted_total(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = _settings(tmp_path)
    output = settings.outputs / "special_projects" / "2016_930"
    output.mkdir(parents=True)
    atomic_write_parquet(
        pl.DataFrame([{"queue_item_id": "q1", "status": "CRAWL_COMPLETED"}]),
        output / "930_TASK_QUEUE.parquet",
        {"test": "max-cycles-resume"},
        key_columns=("queue_item_id",),
    )
    (output / "930_MONITOR_SNAPSHOT.json").write_text(
        json.dumps(
            {
                "api_health": {
                    "pass1_waiting": 1,
                    "next_retry_at": "2000-01-01T00:00:00+00:00",
                }
            }
        ),
        encoding="utf-8",
    )
    (output / "930_AUTORUN_STATE.json").write_text(
        json.dumps({"cycles_started": 673}),
        encoding="utf-8",
    )
    runner = Episode930AutoRunner(settings, output=output, max_cycles=1)

    def start_would_be_reached(_manager):
        raise RuntimeError("resumed convergence cycle should be allowed")

    monkeypatch.setattr(runner, "_start_or_adopt", start_would_be_reached)
    with pytest.raises(RuntimeError, match="resumed convergence cycle"):
        runner.run()


def test_unchanged_final_convergence_waits_for_cooldown(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    output = settings.outputs / "special_projects" / "2016_930"
    output.mkdir(parents=True)
    (output / "930_MONITOR_SNAPSHOT.json").write_text(
        json.dumps(
            {
                "api_health": {
                    "pass1_waiting": 1,
                    "next_retry_at": "2000-01-01T00:00:00+00:00",
                    "recovery_phase": "BACKOFF_SINGLE_PROBE",
                }
            }
        ),
        encoding="utf-8",
    )
    runner = Episode930AutoRunner(settings, output=output)
    first = runner._final_convergence_state()
    (output / "930_AUTORUN_STATE.json").write_text(
        json.dumps(
            {
                "final_convergence_last_attempt_at": datetime.now(UTC).isoformat(),
                "final_convergence_fingerprint": first["fingerprint"],
            }
        ),
        encoding="utf-8",
    )

    cooled = runner._final_convergence_state()
    assert cooled["pending"] is True
    assert cooled["cooldown_active"] is True
    assert cooled["due"] is False


def test_runner_state_refreshes_live_pid_and_progress_timestamp(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    output = settings.outputs / "special_projects" / "2016_930"
    output.mkdir(parents=True)
    (output / "930_PROGRESS_SNAPSHOT.json").write_text(
        json.dumps({"last_real_progress_at": "2026-08-14T00:00:00+00:00"}),
        encoding="utf-8",
    )
    runner = Episode930AutoRunner(settings, output=output)
    (output / "930_AUTORUN_STATE.json").write_text(
        json.dumps({"runner_pid": 1, "last_real_progress_at": "old"}),
        encoding="utf-8",
    )
    state = runner._write_state(status="RUNNING")
    assert state["runner_pid"] == os.getpid()
    assert state["last_real_progress_at"] == "2026-08-14T00:00:00+00:00"
