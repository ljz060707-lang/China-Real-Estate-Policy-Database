import shutil
from datetime import date

from policydb.rolling_24m import (
    Rolling24MConfig,
    _stop_requested,
    build_rolling_queue,
    rolling_window,
    target_months,
)
from policydb.settings import Settings


def _settings(root, tmp_path):
    project = tmp_path / "repo"
    reference = project / "data" / "reference"
    reference.mkdir(parents=True)
    for name in ("cities_105.csv", "source_registry.yaml"):
        shutil.copy2(root / "data" / "reference" / name, reference / name)
    return Settings(
        root=project,
        data_root_path=tmp_path / "data",
        outputs_path=tmp_path / "outputs",
        automation_path=tmp_path / "automation",
    )


def test_rolling_window_has_dynamic_24_month_buckets():
    start, end = rolling_window(date(2026, 8, 13))
    assert start == date(2024, 8, 13)
    assert end == date(2026, 8, 13)
    assert len(target_months(start, end)) == 24


def test_rolling_queue_is_source_session_scoped_and_resume_idempotent(root, tmp_path):
    settings = _settings(root, tmp_path)
    config = Rolling24MConfig.default(today=date(2026, 8, 13), max_items=2)
    first = build_rolling_queue(settings, config=config, resume=True)
    second = build_rolling_queue(settings, config=config, resume=True)
    assert first.height > 0
    assert first.height == second.height
    assert first[0, "window_start"] == "2024-08-13"
    assert json_months(second[0, "target_months"]) == 24
    assert second[0, "status"] == "PENDING"


def test_rolling_run_honors_global_safe_stop_sentinel(root, tmp_path):
    settings = _settings(root, tmp_path)
    assert _stop_requested(settings) is False

    settings.automation.mkdir(parents=True, exist_ok=True)
    (settings.automation / "STOP").write_text("requested", encoding="utf-8")
    assert _stop_requested(settings) is True


def json_months(value):
    import json

    return len(json.loads(value))
