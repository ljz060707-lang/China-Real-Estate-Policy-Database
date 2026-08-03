param([int]$Port = 0)
$ErrorActionPreference = "Stop"
& (Join-Path $PSScriptRoot "stop_dashboard.ps1")
& (Join-Path $PSScriptRoot "start_dashboard.ps1") -Port $Port -NoBrowser
exit $LASTEXITCODE
