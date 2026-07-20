from __future__ import annotations

from pathlib import Path

from app.setup_wizard import initial_setup_status, needs_initial_setup

ROOT = Path(__file__).resolve().parents[1]


def test_windows_launcher_files_exist():
    expected = [
        "打开房地产政策数据库.bat",
        "关闭房地产政策数据库.bat",
        "首次安装.bat",
        "start_policydb.cmd",
        "scripts/launch_dashboard.ps1",
        "scripts/stop_dashboard.ps1",
        "scripts/first_setup.ps1",
        "scripts/create_desktop_shortcut.ps1",
        ".runtime/.gitkeep",
    ]
    assert all((ROOT / relative).exists() for relative in expected)


def test_batch_files_are_independent_of_working_directory():
    for name in ("关闭房地产政策数据库.bat", "首次安装.bat"):
        content = (ROOT / name).read_text(encoding="utf-8")
        assert "%~dp0" in content
        assert "-ExecutionPolicy Bypass" in content
    wrapper = (ROOT / "打开房地产政策数据库.bat").read_text(encoding="ascii")
    command = (ROOT / "start_policydb.cmd").read_text(encoding="ascii")
    assert "%~dp0start_policydb.cmd" in wrapper
    assert "%~dp0" in command and "-ExecutionPolicy Bypass" in command
    assert "pause" in command and "EXIT_CODE" in command


def test_launcher_uses_health_check_and_persistent_state():
    content = (ROOT / "scripts/launch_dashboard.ps1").read_text(encoding="utf-8")
    assert "/_stcore/health" in content
    assert '"dashboard.pid"' in content
    assert '"dashboard.port"' in content
    assert '"dashboard.log"' in content
    assert "Start-BackgroundDashboard" in content
    assert "Resolve-ProjectPython" in content
    assert '".venv-1\\Scripts\\python.exe"' in content
    assert '"launcher.log"' in content
    assert '"dashboard.process.json"' in content
    assert "--server.fileWatcherType=none" in content
    assert "--runner.fastReruns=false" in content


def test_windows_command_entries_use_crlf():
    for name in ("打开房地产政策数据库.bat", "start_policydb.cmd"):
        payload = (ROOT / name).read_bytes()
        assert b"\r\n" in payload
        assert b"\n" not in payload.replace(b"\r\n", b"")


def test_first_setup_uses_official_uv_and_does_not_guess_excel():
    content = (ROOT / "scripts/first_setup.ps1").read_text(encoding="utf-8")
    assert "https://astral.sh/uv/install.ps1" in content
    assert "sync --all-extras" in content
    assert "data\\curated" in content
    assert "policydb.duckdb" in content
    assert "Desktop" not in content
    assert "*.xlsx" not in content


def test_runtime_is_ignored_but_gitkeep_is_retained():
    content = (ROOT / ".gitignore").read_text(encoding="utf-8")
    assert ".runtime/*" in content
    assert "!.runtime/.gitkeep" in content


def test_setup_wizard_detects_missing_and_complete_state(tmp_path):
    assert needs_initial_setup(tmp_path)
    (tmp_path / "database").mkdir()
    (tmp_path / "database" / "policydb.duckdb").touch()
    (tmp_path / "data" / "curated").mkdir(parents=True)
    (tmp_path / "data" / "curated" / "records.parquet").touch()
    assert initial_setup_status(tmp_path) == {
        "database_ready": True,
        "curated_ready": True,
    }
    assert not needs_initial_setup(tmp_path)


def test_setup_wizard_preserves_cli_dashboard_import_order():
    dashboard = (ROOT / "app/dashboard.py").read_text(encoding="utf-8")
    guard = dashboard.index("if needs_initial_setup(ROOT):")
    database_open = dashboard.index("db = open_database()")
    assert guard < database_open
