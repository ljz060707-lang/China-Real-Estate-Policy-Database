"""Regression checks for the jurisdictional PowerShell native-command boundary."""

from __future__ import annotations

import base64
import shutil
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
LAUNCHER = REPO_ROOT / "scripts" / "run_jurisdictional_backfill.ps1"


def test_launcher_has_a_nonfatal_native_output_boundary() -> None:
    text = LAUNCHER.read_text(encoding="utf-8")
    assert "function Invoke-NativeCapture" in text
    helper_start = text.index("function Invoke-NativeCapture")
    helper_end = text.index("$resultCode = 0", helper_start)
    helper = text[helper_start:helper_end]
    assert "Start-Process" in helper
    assert "RedirectStandardOutput" in helper
    assert "RedirectStandardError" in helper
    assert ".ExitCode" in helper
    assert "Invoke-NativeCapture -Arguments $discoveryArgs" in text
    assert "Invoke-NativeCapture -Arguments $crawlArgs" in text


@pytest.mark.skipif(shutil.which("powershell.exe") is None, reason="Windows PowerShell is required")
def test_warning_on_native_stderr_does_not_fail_zero_exit() -> None:
    probe = r"""
$childScript = Join-Path $env:TEMP 'crpd-launcher-contract-child.py'
$stdout = Join-Path $env:TEMP 'crpd-launcher-contract.stdout.tmp'
$stderr = Join-Path $env:TEMP 'crpd-launcher-contract.stderr.tmp'
Set-Content -LiteralPath $childScript -Value "import sys; print('warning', file=sys.stderr); sys.exit(0)" -Encoding UTF8
$child = Start-Process -FilePath 'E:\policy-database\.venv\Scripts\python.exe' -ArgumentList @($childScript) -RedirectStandardOutput $stdout -RedirectStandardError $stderr -NoNewWindow -PassThru -Wait
$exitCode = [int]$child.ExitCode
if (Test-Path $stdout) { Get-Content -LiteralPath $stdout | Out-Host }
if (Test-Path $stderr) { Get-Content -LiteralPath $stderr | Out-Host }
Remove-Item -LiteralPath $childScript, $stdout, $stderr -Force -ErrorAction SilentlyContinue
if ($exitCode -ne 0) { exit $exitCode }
exit 0
""".strip()
    encoded = base64.b64encode(probe.encode("utf-16le")).decode("ascii")
    result = subprocess.run(
        ["powershell.exe", "-NoLogo", "-NoProfile", "-NonInteractive", "-EncodedCommand", encoded],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
        timeout=10,
    )
    assert result.returncode == 0, result.stdout + result.stderr
