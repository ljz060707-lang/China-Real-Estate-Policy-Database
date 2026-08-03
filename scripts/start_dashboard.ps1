param(
    [int]$Port = 0,
    [switch]$NoBrowser
)

$ErrorActionPreference = "Stop"
$launcher = Join-Path $PSScriptRoot "launch_dashboard.ps1"
if (-not (Test-Path -LiteralPath $launcher)) { throw "launch_dashboard.ps1 not found" }
if ($NoBrowser) {
    & $launcher -Port $Port -NoBrowser -NoGui
}
else {
    & $launcher -Port $Port -NoGui
}
exit $LASTEXITCODE
