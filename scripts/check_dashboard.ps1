$ErrorActionPreference = "Stop"
$Root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$Runtime = Join-Path $Root ".runtime"
$pidPath = Join-Path $Runtime "dashboard.pid"
$portPath = Join-Path $Runtime "dashboard.port"
$pidValue = if (Test-Path $pidPath) { [int](Get-Content $pidPath -Raw) } else { 0 }
$portValue = if (Test-Path $portPath) { [int](Get-Content $portPath -Raw) } else { 0 }
$process = if ($pidValue -gt 0) { Get-Process -Id $pidValue -ErrorAction SilentlyContinue } else { $null }
$healthy = $false
if ($portValue -gt 0) {
    try { $healthy = (Invoke-WebRequest -UseBasicParsing -Uri "http://127.0.0.1:$portValue/_stcore/health" -TimeoutSec 2).Content.Trim().ToLowerInvariant() -eq "ok" } catch { $healthy = $false }
}
[PSCustomObject]@{
    status = if ($healthy) { "RUNNING" } elseif ($process) { "STARTING_OR_UNHEALTHY" } else { "STOPPED" }
    pid = $pidValue
    process_alive = [bool]$process
    port = $portValue
    health = $healthy
    local_url = if ($portValue) { "http://127.0.0.1:$portValue" } else { $null }
    log = (Join-Path $Runtime "dashboard.log")
} | ConvertTo-Json -Depth 4
