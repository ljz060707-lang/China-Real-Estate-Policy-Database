param(
    [int]$Port = 0,
    [switch]$NoBrowser
)

$ErrorActionPreference = "Stop"
$Root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$Runtime = Join-Path $Root ".runtime"
$PidFile = Join-Path $Runtime "dashboard.pid"
$PortFile = Join-Path $Runtime "dashboard.port"
$StartedFile = Join-Path $Runtime "dashboard.started"
$LogFile = Join-Path $Runtime "dashboard.log"
$OutputLogFile = Join-Path $Runtime "dashboard.output.log"
$Python = Join-Path $Root ".venv\Scripts\python.exe"
$Dashboard = Join-Path $Root "app\dashboard.py"

function Write-StateFile {
    param([string]$Path, [string]$Value)
    $temp = "$Path.$PID.tmp"
    [IO.File]::WriteAllText($temp, $Value, [Text.UTF8Encoding]::new($false))
    Move-Item -LiteralPath $temp -Destination $Path -Force
}

function Test-DashboardHealth {
    param([int]$HealthPort)
    if ($HealthPort -lt 1) { return $false }
    try {
        $response = Invoke-WebRequest -UseBasicParsing -Uri "http://127.0.0.1:$HealthPort/_stcore/health" -TimeoutSec 2
        return $response.StatusCode -eq 200 -and $response.Content.Trim().ToLowerInvariant() -eq "ok"
    }
    catch {
        return $false
    }
}

function Test-PortFree {
    param([int]$CandidatePort)
    $listener = $null
    try {
        $listener = [Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback, $CandidatePort)
        $listener.Start()
        return $true
    }
    catch {
        return $false
    }
    finally {
        if ($null -ne $listener) { $listener.Stop() }
    }
}

function Open-Dashboard {
    param([int]$OpenPort)
    $url = "http://127.0.0.1:$OpenPort"
    Write-Host "[完成] 正在打开房地产政策数据库：$url" -ForegroundColor Green
    if (-not $NoBrowser) {
        try {
            $browserStart = [Diagnostics.ProcessStartInfo]::new()
            $browserStart.FileName = $url
            $browserStart.UseShellExecute = $true
            [void][Diagnostics.Process]::Start($browserStart)
        }
        catch {
            Write-Host "[提示] 浏览器未能自动打开，请手工访问：$url" -ForegroundColor Yellow
        }
    }
}

function Start-BackgroundWorker {
    param([string]$PythonPath, [int]$WorkerPort)
    # 某些托管环境同时提供 Path 和 PATH，Start-Process 会把它们误判为重复键。
    # 在当前启动器进程内合并一次；后台进程仍会继承其余配置和密钥环境变量。
    $currentPath = [Environment]::GetEnvironmentVariable("Path", "Process")
    [Environment]::SetEnvironmentVariable("PATH", $null, "Process")
    [Environment]::SetEnvironmentVariable("Path", $currentPath, "Process")
    $streamlitArgs = @(
        "-m", "streamlit", "run", ('"{0}"' -f $Dashboard),
        "--server.address=127.0.0.1",
        "--server.port=$WorkerPort",
        "--server.headless=true",
        "--server.fileWatcherType=none",
        "--server.runOnSave=false",
        "--runner.fastReruns=false",
        "--browser.gatherUsageStats=false"
    )
    return Start-Process -FilePath $PythonPath -ArgumentList $streamlitArgs -WorkingDirectory $Root `
        -RedirectStandardOutput $OutputLogFile -RedirectStandardError $LogFile `
        -WindowStyle Hidden -PassThru
}

New-Item -ItemType Directory -Path $Runtime -Force | Out-Null

Write-Host "[1/4] 正在检查网站状态……" -ForegroundColor Cyan
$savedPort = 0
if (Test-Path -LiteralPath $PortFile) {
    [void][int]::TryParse((Get-Content -LiteralPath $PortFile -Raw).Trim(), [ref]$savedPort)
}
if (Test-DashboardHealth $savedPort) {
    Write-Host "[信息] 网站已经运行，无需重复启动。"
    Open-Dashboard $savedPort
    exit 0
}

if (-not (Test-Path -LiteralPath $Python)) {
    Write-Host "[信息] 尚未完成环境安装，正在打开首次安装程序。" -ForegroundColor Yellow
    & (Join-Path $PSScriptRoot "first_setup.ps1")
    exit $LASTEXITCODE
}
if (-not (Test-Path -LiteralPath $Dashboard)) {
    throw "未找到网站入口：$Dashboard"
}

Write-Host "[2/4] 正在选择可用端口……" -ForegroundColor Cyan
$selectedPort = 0
$preferredPorts = @()
if ($savedPort -ge 1024 -and $savedPort -le 65535) { $preferredPorts += $savedPort }
$preferredPorts += 8501..8599
foreach ($candidate in ($preferredPorts | Select-Object -Unique)) {
    if (Test-PortFree $candidate) {
        $selectedPort = $candidate
        break
    }
}
if ($selectedPort -eq 0) {
    throw "8501—8599 端口均被占用，请关闭占用程序后重试。"
}

if ((Test-Path -LiteralPath $LogFile) -and (Get-Item -LiteralPath $LogFile).Length -gt 10MB) {
    Move-Item -LiteralPath $LogFile -Destination (Join-Path $Runtime "dashboard.previous.log") -Force
}
Remove-Item -LiteralPath $OutputLogFile -Force -ErrorAction SilentlyContinue

Write-Host "[3/4] 正在后台启动网站……" -ForegroundColor Cyan
$env:POLICYDB_ROOT = $Root
if (-not $env:POLARS_MAX_THREADS) { $env:POLARS_MAX_THREADS = "2" }
if (-not $env:OMP_NUM_THREADS) { $env:OMP_NUM_THREADS = "2" }
if (-not $env:ARROW_NUM_THREADS) { $env:ARROW_NUM_THREADS = "2" }
$process = Start-BackgroundWorker -PythonPath $Python -WorkerPort $selectedPort
if ($null -eq $process) { throw "无法创建 Dashboard 后台进程。" }
Write-StateFile -Path $PidFile -Value ([string]$process.Id)
Write-StateFile -Path $PortFile -Value ([string]$selectedPort)
Write-StateFile -Path $StartedFile -Value ([string]$process.StartTime.ToUniversalTime().Ticks)

Write-Host "[4/4] 正在等待网站就绪……" -ForegroundColor Cyan
$ready = $false
for ($attempt = 1; $attempt -le 60; $attempt++) {
    if (Test-DashboardHealth $selectedPort) {
        $ready = $true
        break
    }
    if ($process.HasExited) { break }
    Start-Sleep -Seconds 1
}
if (-not $ready) {
    if (-not $process.HasExited) { Stop-Process -Id $process.Id -Force -ErrorAction SilentlyContinue }
    Remove-Item -LiteralPath $PidFile, $PortFile, $StartedFile -Force -ErrorAction SilentlyContinue
    Write-Host "[错误] 网站未能在60秒内启动。最近日志如下：" -ForegroundColor Red
    if (Test-Path -LiteralPath $LogFile) { Get-Content -LiteralPath $LogFile -Tail 30 }
    if (Test-Path -LiteralPath $OutputLogFile) { Get-Content -LiteralPath $OutputLogFile -Tail 20 }
    throw "Dashboard 启动失败，请查看 $LogFile"
}

Open-Dashboard $selectedPort
exit 0
