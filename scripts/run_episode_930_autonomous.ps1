[CmdletBinding()]
param(
    [string]$ProjectRoot = '',
    [string]$DataRoot = 'E:\Data Set\CRPD',
    [int]$CityLimit = 5,
    [int]$MaxAiCalls = 10,
    [int]$MaxFetches = 30,
    [int]$PollSeconds = 15,
    [int]$MaxCycles = 0
)

$ErrorActionPreference = 'Stop'
if ([string]::IsNullOrWhiteSpace($ProjectRoot)) { $ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path }
$python = Join-Path $ProjectRoot '.venv\Scripts\python.exe'
if (-not (Test-Path -LiteralPath $python)) { throw "Project virtualenv missing: $python" }
$env:POLICYDB_ROOT = $ProjectRoot
$env:CRPD_DATA_ROOT = $DataRoot
Set-Location -LiteralPath $ProjectRoot
& $python -m policydb.episode_930_autorun `
    --city-limit $CityLimit `
    --max-ai-calls $MaxAiCalls `
    --max-fetches $MaxFetches `
    --poll-seconds $PollSeconds `
    --max-cycles $MaxCycles
exit $LASTEXITCODE
