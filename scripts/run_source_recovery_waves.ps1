$ErrorActionPreference = "Stop"

$Utf8NoBom = New-Object System.Text.UTF8Encoding($false)
[Console]::InputEncoding = $Utf8NoBom
[Console]::OutputEncoding = $Utf8NoBom
$OutputEncoding = $Utf8NoBom

$env:CRPD_DATA_ROOT = "D:\Data Set\CRPD"
$env:PYTHONUTF8 = "1"
$env:PYTHONIOENCODING = "utf-8"

$Repo = "D:\Codex\projects\Documents-Codex\2026-07-13\text-20260705-xlsx-text-data-raw\policy-database"
$Python = Join-Path $Repo ".venv\Scripts\python.exe"
$Pipeline = Join-Path $Repo "scripts\run_targeted_source_recovery.py"

$QuarantineQueue = "D:\Data Set\CRPD\outputs\acceptance\real_reprobe_queue\quarantined_candidates_real_reprobe.csv"
$DepartmentQueue = "D:\Data Set\CRPD\outputs\acceptance\department_entry_slot_shortlist.csv"

Set-Location $Repo

if (-not (Test-Path -LiteralPath $Python)) {
    throw "Python not found: $Python"
}

if (-not (Test-Path -LiteralPath $Pipeline)) {
    throw "Pipeline script not found: $Pipeline"
}

Write-Host "=== Phase 1: quarantined real reprobe ===" -ForegroundColor Cyan

& $Python $Pipeline `
    --queue $QuarantineQueue `
    --expected-count 28 `
    --rounds 2 `
    --candidate-timeout-seconds 300 `
    --post-audit-timeout-seconds 600 `
    --apply

if ($LASTEXITCODE -ne 0) {
    throw "Phase 1 stopped with exit code $LASTEXITCODE. Review its final_report.json before continuing."
}

if (-not (Test-Path -LiteralPath $DepartmentQueue)) {
    throw "Department shortlist was not regenerated: $DepartmentQueue"
}

$DepartmentRows = @(
    Import-Csv -LiteralPath $DepartmentQueue -Encoding UTF8
)

if ($DepartmentRows.Count -gt 0) {
    Write-Host "=== Phase 2: current department-entry shortlist ===" -ForegroundColor Cyan
    Write-Host "Queue rows: $($DepartmentRows.Count)"

    & $Python $Pipeline `
        --queue $DepartmentQueue `
        --expected-count 0 `
        --rounds 2 `
        --candidate-timeout-seconds 300 `
        --post-audit-timeout-seconds 600 `
        --apply

    if ($LASTEXITCODE -ne 0) {
        throw "Phase 2 stopped with exit code $LASTEXITCODE. Review its final_report.json."
    }
}
else {
    Write-Host "No remaining department-entry shortlist rows." -ForegroundColor Green
}

Write-Host "=== Final read-only audits ===" -ForegroundColor Cyan

& (Join-Path $Repo ".venv\Scripts\policydb.exe") `
    sources audit-525 `
    --no-seed-registry

if ($LASTEXITCODE -ne 0) {
    throw "audit-525 failed with exit code $LASTEXITCODE"
}

& $Python (Join-Path $Repo "scripts\audit_post_dedupe_conflicts.py")
if ($LASTEXITCODE -ne 0) {
    throw "post-dedupe audit failed with exit code $LASTEXITCODE"
}

& $Python (Join-Path $Repo "scripts\audit_verified_probe_integrity.py")
if ($LASTEXITCODE -ne 0) {
    throw "probe-integrity audit failed with exit code $LASTEXITCODE"
}

Write-Host "SOURCE RECOVERY WAVES COMPLETE" -ForegroundColor Green
Write-Host "No global promote/enable or full crawl was started." -ForegroundColor Yellow
