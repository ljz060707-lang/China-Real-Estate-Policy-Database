[CmdletBinding()]
param(
    [string]$ProjectRoot = 'E:\policy-database',
    [string]$DataRoot = 'E:\Data Set\CRPD',
    [ValidateRange(60, 3600)][int]$RefreshSeconds = 60
)

$ErrorActionPreference = 'Stop'
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$python = Join-Path $ProjectRoot '.venv\Scripts\python.exe'
$monitor = Join-Path $ProjectRoot 'scripts\monitor_recent_5y_city_year.py'
$config = Join-Path $ProjectRoot 'config\crpd_autonomous.json'
if (-not (Test-Path -LiteralPath $python)) { throw "Project virtualenv missing: $python" }
if (-not (Test-Path -LiteralPath $monitor)) { throw "Monitor script missing: $monitor" }

while ($true) {
    Clear-Host
    & $python $monitor --data-root $DataRoot --config $config --once
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Monitor read failed; production crawler is not stopped. Next retry in $RefreshSeconds seconds." -ForegroundColor Yellow
    }
    Write-Host "`nNext refresh in $RefreshSeconds seconds. Ctrl+C closes monitor only."
    Start-Sleep -Seconds $RefreshSeconds
}
