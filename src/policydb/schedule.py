from __future__ import annotations

import subprocess
from pathlib import Path

from policydb.settings import Settings

TASKS = {
    "daily": ("DAILY", None, "02:30", "run_daily_update.ps1"),
    "weekly": ("WEEKLY", "SUN", "03:00", "run_weekly_update.ps1"),
    "monthly": ("MONTHLY", "1", "03:30", "run_monthly_update.ps1"),
}


def _task_name(layer: str) -> str:
    return f"PolicyDB-V2-{layer}"


def schedule_status(runner=subprocess.run) -> dict:
    tasks = {}
    for layer in TASKS:
        try:
            result = runner(
                ["schtasks.exe", "/Query", "/TN", _task_name(layer), "/FO", "CSV", "/V"],
                capture_output=True,
                text=True,
                shell=False,
                check=False,
            )
            returncode = result.returncode
        except OSError:
            returncode = 127
        tasks[layer] = {
            "installed": returncode == 0,
            "status": "installed" if returncode == 0 else "not_installed",
        }
    return {"tasks": tasks, "all_installed": all(item["installed"] for item in tasks.values())}


def install_windows_schedule(
    settings: Settings | None = None, *, confirm: bool = False, runner=subprocess.run
) -> dict:
    settings = settings or Settings.discover()
    commands = []
    for layer, (schedule, day, start, script_name) in TASKS.items():
        script = settings.root / "scripts" / script_name
        action = (
            "powershell.exe -NoProfile -NonInteractive -ExecutionPolicy Bypass "
            f'-File "{script}"'
        )
        command = [
            "schtasks.exe",
            "/Create",
            "/F",
            "/TN",
            _task_name(layer),
            "/TR",
            action,
            "/SC",
            schedule,
            "/ST",
            start,
        ]
        if schedule == "WEEKLY":
            command.extend(["/D", str(day)])
        elif schedule == "MONTHLY":
            command.extend(["/D", str(day)])
        commands.append(command)
    if not confirm:
        return {
            "installed": False,
            "confirmation_required": True,
            "task_names": [_task_name(layer) for layer in TASKS],
        }
    results = []
    for command in commands:
        result = runner(
            command, capture_output=True, text=True, shell=False, check=False
        )
        results.append({"task": command[4], "returncode": result.returncode})
    return {
        "installed": all(item["returncode"] == 0 for item in results),
        "confirmation_required": False,
        "results": results,
    }


def remove_windows_schedule(*, confirm: bool = False, runner=subprocess.run) -> dict:
    if not confirm:
        return {"removed": False, "confirmation_required": True}
    results = []
    for layer in TASKS:
        result = runner(
            ["schtasks.exe", "/Delete", "/F", "/TN", _task_name(layer)],
            capture_output=True,
            text=True,
            shell=False,
            check=False,
        )
        results.append({"task": _task_name(layer), "returncode": result.returncode})
    return {"removed": True, "confirmation_required": False, "results": results}


def runner_path(settings: Settings, layer: str) -> Path:
    return settings.root / "scripts" / TASKS[layer][3]
