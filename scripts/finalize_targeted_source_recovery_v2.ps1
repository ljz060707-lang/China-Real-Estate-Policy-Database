[CmdletBinding()]
param(
    [string]$Repo = "D:\Codex\projects\Documents-Codex\2026-07-13\text-20260705-xlsx-text-data-raw\policy-database"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

Set-Location $Repo

$Utf8NoBom = New-Object System.Text.UTF8Encoding($false)
[Console]::InputEncoding = $Utf8NoBom
[Console]::OutputEncoding = $Utf8NoBom
$OutputEncoding = $Utf8NoBom
$env:PYTHONUTF8 = "1"
$env:PYTHONIOENCODING = "utf-8"
$env:CRPD_DATA_ROOT = "D:\Data Set\CRPD"
chcp 65001 | Out-Null

$Python = Join-Path $Repo ".venv\Scripts\python.exe"
$PolicyDB = Join-Path $Repo ".venv\Scripts\policydb.exe"
$Recovery = Join-Path $Repo "scripts\run_targeted_source_recovery.py"

if (-not (Test-Path $Python)) {
    throw "Python not found: $Python"
}
if (-not (Test-Path $Recovery)) {
    throw "Recovery script not found: $Recovery"
}

Write-Host ("=" * 78)
Write-Host "Compile v2 recovery script"
Write-Host ("=" * 78)
& $Python -m py_compile $Recovery
if ($LASTEXITCODE -ne 0) {
    throw "py_compile failed: $LASTEXITCODE"
}

Write-Host ("=" * 78)
Write-Host "Finalize current 28-candidate recovery without new network probes"
Write-Host ("=" * 78)
& $Python $Recovery --apply --finalize-only
$RecoveryExit = $LASTEXITCODE
if ($RecoveryExit -ne 0) {
    throw "Recovery finalization failed: $RecoveryExit"
}

Write-Host ("=" * 78)
Write-Host "Run final source audits"
Write-Host ("=" * 78)
& $PolicyDB sources audit-525 --no-seed-registry
if ($LASTEXITCODE -ne 0) {
    throw "audit-525 failed: $LASTEXITCODE"
}

$AuditScripts = @(
    "audit_post_dedupe_conflicts.py",
    "audit_verified_probe_integrity.py",
    "build_source_525_action_queue.py",
    "build_department_entry_review.py",
    "build_department_entry_slot_shortlist.py"
)

foreach ($Name in $AuditScripts) {
    $Path = Join-Path $Repo ("scripts\" + $Name)
    if (-not (Test-Path $Path)) {
        Write-Warning "Skipped missing script: $Path"
        continue
    }

    & $Python $Path
    if ($LASTEXITCODE -ne 0) {
        throw "$Name failed: $LASTEXITCODE"
    }
}

Write-Host ("=" * 78)
Write-Host "FINALIZATION COMPLETE"
Write-Host "No full crawl was started."
Write-Host ("=" * 78)
