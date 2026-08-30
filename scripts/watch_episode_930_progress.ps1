[CmdletBinding()]
param(
    [string]$ProjectRoot = '',
    [string]$DataRoot = 'E:\Data Set\CRPD',
    [ValidateRange(5, 300)][int]$RefreshSeconds = 10
)

$ErrorActionPreference = 'Stop'
if ([string]::IsNullOrWhiteSpace($ProjectRoot)) { $ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path }
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$python = Join-Path $ProjectRoot '.venv\Scripts\python.exe'
if (-not (Test-Path -LiteralPath $python)) { throw "Project virtualenv missing: $python" }
while ($true) {
    Clear-Host
    & $python (Join-Path $ProjectRoot 'scripts\episode_930_monitor.py') --data-root $DataRoot --once
    if ($LASTEXITCODE -ne 0) { Write-Host "Monitor read failed; production is not stopped." -ForegroundColor Yellow }
    Start-Sleep -Seconds $RefreshSeconds
}
