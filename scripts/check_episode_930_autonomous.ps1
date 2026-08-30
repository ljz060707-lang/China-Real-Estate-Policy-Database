[CmdletBinding()]
param(
    [string]$DataRoot = 'E:\Data Set\CRPD'
)

$ErrorActionPreference = 'Stop'
$root = Join-Path $DataRoot 'outputs\special_projects\2016_930'
$state = Join-Path $root '930_AUTORUN_STATE.json'
$snapshot = Join-Path $root '930_PROGRESS_SNAPSHOT.json'
$lock = Join-Path $root '930_AUTORUN.lock'
if (Test-Path -LiteralPath $state) { Get-Content -LiteralPath $state -Raw }
elseif (Test-Path -LiteralPath $snapshot) { Get-Content -LiteralPath $snapshot -Raw }
else { Write-Output '{"status":"NOT_STARTED","episode_id":"EP_2016_930_TIGHTENING"}' }
Write-Output ("AUTORUN_LOCK=" + [bool](Test-Path -LiteralPath $lock))
Write-Output ("STOP_FULL_SYNC=" + [bool](Test-Path -LiteralPath (Join-Path $DataRoot 'control\STOP_FULL_SYNC')))
Write-Output ("STOP_AUTOPILOT=" + [bool](Test-Path -LiteralPath (Join-Path $DataRoot 'control\STOP_AUTOPILOT')))
Write-Output ("STOP_EPISODE_930=" + [bool](Test-Path -LiteralPath (Join-Path $DataRoot 'control\STOP_EPISODE_930')))
