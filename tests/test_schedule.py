from __future__ import annotations

from types import SimpleNamespace

from policydb.schedule import install_windows_schedule, schedule_status
from policydb.settings import Settings


def test_schedule_preview_does_not_mutate_windows(tmp_path):
    calls = []

    def runner(*args, **kwargs):
        calls.append((args, kwargs))
        return SimpleNamespace(returncode=0)

    result = install_windows_schedule(
        Settings(root=tmp_path), confirm=False, runner=runner
    )
    assert result["confirmation_required"]
    assert calls == []


def test_schedule_status_reports_missing_tasks():
    def runner(command, **kwargs):
        return SimpleNamespace(returncode=1 if "weekly" in command[3] else 0)

    result = schedule_status(runner=runner)
    assert not result["all_installed"]
    assert result["tasks"]["weekly"]["status"] == "not_installed"
