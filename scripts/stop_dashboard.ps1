$ErrorActionPreference = "Stop"
$Root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$Runtime = Join-Path $Root ".runtime"
$PidFile = Join-Path $Runtime "dashboard.pid"
$PortFile = Join-Path $Runtime "dashboard.port"
$StartedFile = Join-Path $Runtime "dashboard.started"

function Test-DashboardHealth {
    param([int]$HealthPort)
    if ($HealthPort -lt 1) { return $false }
    try {
        $response = Invoke-WebRequest -UseBasicParsing -Uri "http://127.0.0.1:$HealthPort/_stcore/health" -TimeoutSec 2
        return $response.StatusCode -eq 200 -and $response.Content.Trim().ToLowerInvariant() -eq "ok"
    }
    catch { return $false }
}

$dashboardPid = 0
$dashboardPort = 0
$startedTicks = 0L
if (Test-Path -LiteralPath $PidFile) {
    [void][int]::TryParse((Get-Content -LiteralPath $PidFile -Raw).Trim(), [ref]$dashboardPid)
}
if (Test-Path -LiteralPath $PortFile) {
    [void][int]::TryParse((Get-Content -LiteralPath $PortFile -Raw).Trim(), [ref]$dashboardPort)
}
if (Test-Path -LiteralPath $StartedFile) {
    [void][long]::TryParse((Get-Content -LiteralPath $StartedFile -Raw).Trim(), [ref]$startedTicks)
}

if ($dashboardPid -le 0) {
    Remove-Item -LiteralPath $PidFile, $PortFile, $StartedFile -Force -ErrorAction SilentlyContinue
    Write-Host "[完成] 网站当前没有运行。" -ForegroundColor Green
    exit 0
}

$process = Get-Process -Id $dashboardPid -ErrorAction SilentlyContinue
if ($null -eq $process) {
    Remove-Item -LiteralPath $PidFile, $PortFile, $StartedFile -Force -ErrorAction SilentlyContinue
    Write-Host "[完成] 网站进程已经退出，已清理旧状态。" -ForegroundColor Green
    exit 0
}

$verified = $false
try {
    $details = Get-CimInstance Win32_Process -Filter "ProcessId=$dashboardPid" -ErrorAction Stop
    $commandLine = [string]$details.CommandLine
    $verified = $commandLine.Contains("streamlit") -and $commandLine.Contains("dashboard.py")
}
catch {
    $actualTicks = $process.StartTime.ToUniversalTime().Ticks
    $sameStart = $startedTicks -gt 0 -and [Math]::Abs($actualTicks - $startedTicks) -lt 20000000L
    $expectedHost = $process.ProcessName -in @("python", "pythonw")
    $verified = $sameStart -and $expectedHost -and (Test-DashboardHealth $dashboardPort)
}
if (-not $verified) {
    Write-Host "[错误] PID $dashboardPid 不再属于本项目，为避免关闭其他程序，本次未执行终止。" -ForegroundColor Red
    throw "运行状态文件可能已过期，请检查 $PidFile"
}

Write-Host "[处理中] 正在关闭房地产政策数据库……" -ForegroundColor Cyan
Stop-Process -Id $dashboardPid -Force -ErrorAction Stop
for ($attempt = 1; $attempt -le 15; $attempt++) {
    if (-not (Test-DashboardHealth $dashboardPort)) { break }
    Start-Sleep -Seconds 1
}
Remove-Item -LiteralPath $PidFile, $PortFile, $StartedFile -Force -ErrorAction SilentlyContinue
Write-Host "[完成] 网站已关闭。" -ForegroundColor Green
exit 0
