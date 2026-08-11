from __future__ import annotations

import json
from pathlib import Path

import polars as pl

import scripts.crpd_autonomous_controller as controller
from scripts.crpd_autonomous_controller import (
    Paths,
    command_specs,
    coverage_summary,
    crawl_shard_summary,
    crawl_stage_semantics,
    current_log_status,
    default_config,
    install,
    next_stage_after,
    redact,
    resume,
    stage_has_structured_validation_warning,
    stop,
    supervisor_decision,
)


def make_paths(tmp_path: Path) -> tuple[Paths, dict]:
    project = tmp_path / "project"
    data = tmp_path / "data"
    (project / ".venv" / "Scripts").mkdir(parents=True)
    (data / "database").mkdir(parents=True)
    config_path = project / "config.json"
    paths = Paths(project=project, data=data, config=config_path)
    config = default_config(project, data)
    config_path.write_text(json.dumps(config), encoding="utf-8")
    return paths, config


def test_install_stop_and_resume_are_persistent(tmp_path: Path) -> None:
    paths, config = make_paths(tmp_path)

    assert install(paths, config) == 0
    assert paths.master.exists()
    assert paths.lock.exists()
    assert stop(paths, "test") == 0
    assert paths.stop.exists()
    assert resume(paths) == 0
    assert not paths.stop.exists()


def test_coverage_requires_explicit_saturation_gate(tmp_path: Path) -> None:
    paths, config = make_paths(tmp_path)
    install(paths, config)
    summary = paths.data / "outputs" / "coverage" / "LATEST_COVERAGE_SUMMARY.json"
    summary.parent.mkdir(parents=True, exist_ok=True)

    summary.write_text(json.dumps({"city_count": 105}), encoding="utf-8")
    assert coverage_summary(paths)["saturated"] is False
    summary.write_text(json.dumps({"WEB_CRAWL_SATURATED": True}), encoding="utf-8")
    assert coverage_summary(paths)["saturated"] is True


def test_stage_machine_and_redaction() -> None:
    assert next_stage_after("COVERAGE_AUDIT", False) == "RECOVER_MISSING"
    assert next_stage_after("COVERAGE_AUDIT", True) == "PDF_STAGE"
    assert "secret-value" not in redact("Authorization: Bearer secret-value")


def test_legacy_handoff_distinguishes_terminal_failure_from_pending_summary(tmp_path: Path) -> None:
    paths, config = make_paths(tmp_path)
    install(paths, config)
    run_dir = paths.data / "logs" / "audited_full_backfill" / "legacy-run"
    run_dir.mkdir(parents=True)
    master = run_dir / "master.log"
    checkpoint = paths.data / "curated" / "crawl_shards.parquet"
    checkpoint.parent.mkdir(parents=True, exist_ok=True)
    checkpoint.write_bytes(b"checkpoint")

    master.write_text("[ERROR] 阶段失败（exit=1）：crawl_CITY_320100\n", encoding="utf-8")
    failed = current_log_status(paths)
    assert failed["status"] == "TERMINAL_FAILED"
    assert failed["safe_ended"] is True
    assert failed["reason"] == "LEGACY_RUN_TERMINAL_FAILED"

    master.write_text("阶段执行中，尚无总结标记\n", encoding="utf-8")
    pending = current_log_status(paths)
    assert pending["status"] == "TRUE_SUMMARY_PENDING"
    assert pending["safe_ended"] is False

    master.write_text("补扫流程结束\n", encoding="utf-8")
    successful = current_log_status(paths)
    assert successful["status"] == "SUCCESSFUL_END"
    assert successful["safe_ended"] is True

    active = current_log_status(paths, active=True)
    assert active["status"] == "ACTIVE"
    assert active["safe_ended"] is False


def test_terminal_failed_handoff_starts_a_fresh_run_id(tmp_path: Path, monkeypatch) -> None:
    paths, config = make_paths(tmp_path)
    install(paths, config)
    paths.master.write_text(
        json.dumps(
            {
                "automation_id": "AUTO_TEST",
                "status": "RETRY_WAIT",
                "stage": "CRAWL",
                "next_stage": "CRAWL",
                "run_id": "RUN_OLD",
            }
        ),
        encoding="utf-8",
    )
    empty_processes = {
        "current_backfill": [],
        "legacy_supervisor": [],
        "external_writers": [],
        "autonomous_workers": [],
    }
    monkeypatch.setattr(controller, "process_snapshot", lambda: empty_processes)
    monkeypatch.setattr(controller, "disk_status", lambda *_args: {"status": "OK"})
    monkeypatch.setattr(
        controller,
        "current_log_status",
        lambda *_args, **_kwargs: {
            "status": "TERMINAL_FAILED",
            "safe_ended": True,
            "reason": "LEGACY_RUN_TERMINAL_FAILED",
        },
    )
    monkeypatch.setattr(controller, "run_id", lambda *_args: "RUN_NEW")

    class FakeLock:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    monkeypatch.setattr(controller, "FileLock", lambda *_args, **_kwargs: FakeLock())
    seen = {}

    class FakeProcess:
        pid = 4321

    def fake_popen(command, **_kwargs):
        seen["command"] = command
        return FakeProcess()

    monkeypatch.setattr(controller.subprocess, "Popen", fake_popen)

    assert supervisor_decision(paths, config) == 0
    state = json.loads(paths.master.read_text(encoding="utf-8"))
    assert state["run_id"] == "RUN_NEW"
    assert "--run-id" in seen["command"]
    assert seen["command"][seen["command"].index("--run-id") + 1] == "RUN_NEW"
    assert state["last_error"] is None


def test_incomplete_validation_report_is_a_warning_not_a_command_failure(tmp_path: Path) -> None:
    paths, config = make_paths(tmp_path)
    install(paths, config)
    log = paths.supervisor_logs / "RUN_TEST_NORMALIZE_2.log"
    log.write_text(
        """[2026-08-10T00:00:00Z] COMMAND validate --group all
{
  "validation_group": "all",
  "record_count": 10,
  "passed": false,
  "v2_group_results": {"coverage": false, "release": false}
}
[2026-08-10T00:00:01Z] EXIT_CODE 1
""",
        encoding="utf-8",
    )
    assert stage_has_structured_validation_warning(paths, "RUN_TEST", "NORMALIZE_2") is True


def test_crawl_again_uses_existing_audited_backfill_script(tmp_path: Path) -> None:
    paths, config = make_paths(tmp_path)
    install(paths, config)
    (paths.project / ".venv" / "Scripts" / "python.exe").touch()
    (paths.project / ".venv" / "Scripts" / "policydb.exe").touch()
    script = paths.project / "scripts" / "CRPD_Audited_Full_Backfill.ps1"
    script.parent.mkdir(parents=True)
    script.touch()

    specs = command_specs(paths, config, "CRAWL_AGAIN", "RUN_TEST")
    assert len(specs) == 1
    command = specs[0]
    assert "CRPD_Audited_Full_Backfill.ps1" in command[command.index("-File") + 1]
    assert "-ExistingSourcesOnly" in command
    assert "-SkipAI" in command
    assert command_specs(paths, config, "RECOVER_MISSING", "RUN_TEST")[0][2:] == [
        "recover-missing",
        "--max-fetches",
        "20",
    ]


def test_recovery_zero_work_is_blocked_when_pending_shards_remain(tmp_path: Path) -> None:
    paths, config = make_paths(tmp_path)
    install(paths, config)
    shard_path = paths.data / "curated" / "crawl_shards.parquet"
    shard_path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame([{"shard_id": "S1", "status": "pending"}]).write_parquet(shard_path)
    log = paths.supervisor_logs / "RUN_TEST_RECOVER_MISSING_1.log"
    log.write_text(
        '{"metrics":{"source_count":0,"candidate_count":0,"fetched":0,"failed":0,"document_versions":0},"warning":true}\n',
        encoding="utf-8",
    )
    before_shards = crawl_shard_summary(paths)
    before_progress = {"status": "MISSING", "rows": None, "latest_created_at": None}
    ok, reason, details = crawl_stage_semantics(
        paths,
        run="RUN_TEST",
        stage="RECOVER_MISSING",
        command_stage="RECOVER_MISSING_1",
        argv=["policydb.exe", "crawl", "recover-missing"],
        code=0,
        before_shards=before_shards,
        before_progress=before_progress,
    )
    assert ok is False
    assert reason == "NO_PROGRESS_PENDING_WORK"
    assert details["shards_after"]["pending"] == 1


def test_coverage_audit_routes_to_real_crawl_when_shards_are_pending() -> None:
    assert next_stage_after("COVERAGE_AUDIT", False, True) == "CRAWL_AGAIN"
    assert next_stage_after("COVERAGE_AUDIT", False, False) == "RECOVER_MISSING"
