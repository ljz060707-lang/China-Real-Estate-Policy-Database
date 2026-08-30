param(
    [string]$ProjectRoot = (Split-Path -Parent $PSScriptRoot),
    [string]$DataRoot = "E:\Data Set\CRPD"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
$env:CRPD_DATA_ROOT = $DataRoot
$PolicyDb = Join-Path $ProjectRoot ".venv\Scripts\policydb.exe"
if (-not (Test-Path -LiteralPath $PolicyDb)) {
    throw "policydb executable not found: $PolicyDb"
}
& $PolicyDb supervisor status --stale-minutes 30
exit $LASTEXITCODE
