from __future__ import annotations

import json
from pathlib import Path

from scripts.crpd_autonomous_controller import (
    Paths,
    coverage_summary,
    default_config,
    install,
    next_stage_after,
    redact,
    resume,
    stop,
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

