param(
    [Parameter(Position = 0, ValueFromRemainingArguments = $true)]
    [string[]]$CommandArgs,
    [string]$ProjectRoot = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
if ([string]::IsNullOrWhiteSpace($ProjectRoot)) {
    $ProjectRoot = Split-Path -Parent $PSScriptRoot
}
$PolicyDb = Join-Path $ProjectRoot ".venv\Scripts\policydb.exe"
if (-not (Test-Path -LiteralPath $PolicyDb)) {
    throw "policydb executable not found: $PolicyDb"
}

$env:CRPD_GOVERNMENT_ROUTE = "direct"
$env:HTTP_PROXY = $null
$env:HTTPS_PROXY = $null
$env:ALL_PROXY = $null
$env:http_proxy = $null
$env:https_proxy = $null
$env:all_proxy = $null
$env:NO_PROXY = "*"
$env:no_proxy = "*"

if (-not $CommandArgs -or $CommandArgs.Count -eq 0) {
    throw "Pass a policydb command, for example: network probe-direct --url https://www.nanjing.gov.cn/"
}
& $PolicyDb @CommandArgs
exit $LASTEXITCODE
