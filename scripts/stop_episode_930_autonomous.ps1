[CmdletBinding()]
param(
    [string]$DataRoot = 'E:\Data Set\CRPD'
)

$ErrorActionPreference = 'Stop'
$control = Join-Path $DataRoot 'control'
New-Item -ItemType Directory -Path $control -Force | Out-Null
$stop = Join-Path $control 'STOP_EPISODE_930'
Set-Content -LiteralPath $stop -Value ((Get-Date).ToUniversalTime().ToString('o')) -Encoding utf8
Write-Output ("STOP_REQUESTED=" + $stop)
Write-Output 'The current JobManager worker will be cancelled at its next safe boundary; no process is killed.'
