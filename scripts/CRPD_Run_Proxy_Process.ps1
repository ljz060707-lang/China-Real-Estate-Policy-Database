param(
    [Parameter(Position = 0, ValueFromRemainingArguments = $true)]
    [string[]]$CommandArgs,
    [string]$ProjectRoot = "",
    [string]$ProxyUrl = "http://127.0.0.1:7897"
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

$env:CRPD_PROXY_URL = $ProxyUrl
$env:CRPD_AI_PROXY_URL = $ProxyUrl
$env:CRPD_SEARCH_PROXY_URL = $ProxyUrl
$env:CRPD_GOVERNMENT_ROUTE = "direct"
if ([string]::IsNullOrWhiteSpace($env:SEARCH_PROVIDERS)) {
    $env:SEARCH_PROVIDERS = "DuckDuckGoHTML"
}

if (-not $CommandArgs -or $CommandArgs.Count -eq 0) {
    & $PolicyDb network probe-proxy
}
else {
    & $PolicyDb @CommandArgs
}
exit $LASTEXITCODE
