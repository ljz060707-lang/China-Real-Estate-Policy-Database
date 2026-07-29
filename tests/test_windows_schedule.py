from types import SimpleNamespace

from policydb.schedule import install_windows_schedule
from policydb.settings import Settings


def test_windows_schedule_requires_confirmation(tmp_path):
    calls = []
    result = install_windows_schedule(
        Settings(root=tmp_path),
        confirm=False,
        runner=lambda *a, **k: calls.append(a) or SimpleNamespace(returncode=0),
    )
    assert result["confirmation_required"] and not calls
    assert all(name.startswith("CRPD-") for name in result["task_names"])
