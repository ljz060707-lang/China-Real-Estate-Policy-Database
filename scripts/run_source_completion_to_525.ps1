[CmdletBinding()]
param(
    [string]$Repo = "D:\Codex\projects\Documents-Codex\2026-07-13\text-20260705-xlsx-text-data-raw\policy-database",
    [string]$DataRoot = "E:\Data Set\CRPD",
    [string]$Provider = "siliconflow",
    [int]$BatchSlots = 20,
    [int]$MaxAiCalls = 20,
    [int]$Concurrency = 4,
    [int]$MaxCycles = 200,
    [int]$MaxStagnantCycles = 6,
    [switch]$NewRun,
    [switch]$StatusOnly,
    [switch]$Apply
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$Utf8NoBom = New-Object System.Text.UTF8Encoding($false)
[Console]::InputEncoding = $Utf8NoBom
[Console]::OutputEncoding = $Utf8NoBom
$OutputEncoding = $Utf8NoBom
$env:PYTHONUTF8 = "1"
$env:PYTHONIOENCODING = "utf-8"
$env:CRPD_DATA_ROOT = $DataRoot
chcp 65001 | Out-Null

Set-Location -LiteralPath $Repo

$Python = Join-Path $Repo ".venv\Scripts\python.exe"
$Controller = Join-Path $Repo "scripts\run_source_completion_to_525.py"

if (-not (Test-Path -LiteralPath $Python)) {
    throw "Python not found: $Python"
}
if (-not (Test-Path -LiteralPath $Controller)) {
    throw "Controller not found: $Controller"
}

& $Python -m py_compile $Controller
if ($LASTEXITCODE -ne 0) {
    throw "Controller py_compile failed: $LASTEXITCODE"
}

$Arguments = @(
    $Controller,
    "--repo", $Repo,
    "--data-root", $DataRoot,
    "--provider", $Provider,
    "--batch-slots", [string]$BatchSlots,
    "--max-ai-calls", [string]$MaxAiCalls,
    "--concurrency", [string]$Concurrency,
    "--max-cycles", [string]$MaxCycles,
    "--max-stagnant-cycles", [string]$MaxStagnantCycles
)

if ($NewRun) {
    $Arguments += "--new-run"
}
if ($StatusOnly) {
    $Arguments += "--status-only"
}
if ($Apply) {
    $Arguments += "--apply"
}

Write-Host ("=" * 78)
Write-Host "CRPD SOURCE COMPLETION TO 525"
Write-Host "Repository : $Repo"
Write-Host "Data root  : $DataRoot"
Write-Host "Apply      : $Apply"
Write-Host "Stop file  : $(Join-Path $DataRoot 'control\STOP_SOURCE_COMPLETION_TO_525')"
Write-Host ("=" * 78)

& $Python @Arguments
$ExitCode = $LASTEXITCODE

Write-Host ""
Write-Host "Controller exit code: $ExitCode"

switch ($ExitCode) {
    0  { Write-Host "Completed successfully or plan/status finished." }
    10 { Write-Host "Batch completed but 525 has not yet been reached." }
    20 { Write-Warning "Provider remained blocked after bounded retries." }
    21 { Write-Warning "Stopped by STOP_SOURCE_COMPLETION_TO_525." }
    30 { Write-Warning "Stopped safely with BLOCKERS.json after deterministic/manual/external stagnation." }
    31 { Write-Error "Preflight or fatal controller failure." }
    32 { Write-Error "A strict integrity gate failed." }
    default { Write-Error "Unexpected controller exit code: $ExitCode" }
}

exit $ExitCode
