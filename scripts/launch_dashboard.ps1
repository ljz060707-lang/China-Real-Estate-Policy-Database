param(
    [int]$Port = 0,
    [switch]$NoBrowser,
    [switch]$NoGui
)

$ErrorActionPreference = "Stop"
$Root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$Runtime = Join-Path $Root ".runtime"
$PidFile = Join-Path $Runtime "dashboard.pid"
$PortFile = Join-Path $Runtime "dashboard.port"
$StartedFile = Join-Path $Runtime "dashboard.started"
$ProcessFile = Join-Path $Runtime "dashboard.process.json"
$LauncherLog = Join-Path $Runtime "launcher.log"
$DashboardLog = Join-Path $Runtime "dashboard.log"
$OutputLog = Join-Path $Runtime "dashboard.output.log"
$Dashboard = Join-Path $Root "app\dashboard.py"
New-Item -ItemType Directory -Path $Runtime -Force | Out-Null

function Write-LauncherLog {
    param([string]$Message)
    $safe = $Message -replace '(?i)(Bearer\s+)[^\s]+', '$1[REDACTED]'
    $safe = $safe -replace '(?i)(API_KEY|TOKEN|PASSWORD|AUTHORIZATION)\s*[=:]\s*[^\s]+', '$1=[REDACTED]'
    Add-Content -LiteralPath $LauncherLog -Value "$(Get-Date -Format o) $safe" -Encoding UTF8
}

function Write-StateFile {
    param([string]$Path, [string]$Value)
    $temp = "$Path.$PID.tmp"
    [IO.File]::WriteAllText($temp, $Value, [Text.UTF8Encoding]::new($false))
    Move-Item -LiteralPath $temp -Destination $Path -Force
}

function Resolve-ProjectPython {
    param(
        [string]$ProjectRoot = $Root,
        [switch]$SkipDependencyCheck
    )
    $preferencesPath = Join-Path $ProjectRoot "data\reference\user_preferences.json"
    $explicit = $null
    if (Test-Path -LiteralPath $preferencesPath) {
        try {
            $preferences = Get-Content -LiteralPath $preferencesPath -Raw -Encoding UTF8 | ConvertFrom-Json
            $explicit = [string]$preferences.project_python_path
        }
        catch {
            Write-LauncherLog "user_preferences parse failed: $($_.Exception.Message)"
        }
    }
    $candidates = @(
        [PSCustomObject]@{ Path = (Join-Path $ProjectRoot ".venv\Scripts\python.exe"); Name = ".venv"; Source = "project_default" },
        [PSCustomObject]@{ Path = (Join-Path $ProjectRoot ".venv-1\Scripts\python.exe"); Name = ".venv-1"; Source = "project_fallback" }
    )
    if ($explicit) {
        $candidates += [PSCustomObject]@{ Path = $explicit; Name = "user"; Source = "user_preferences" }
    }
    foreach ($candidate in $candidates) {
        if (-not (Test-Path -LiteralPath $candidate.Path)) { continue }
        if ($SkipDependencyCheck) {
            return [PSCustomObject]@{
                python_path = [IO.Path]::GetFullPath($candidate.Path)
                environment_name = $candidate.Name
                resolution_source = $candidate.Source
                python_version = "test"
                streamlit_available = $true
                policydb_available = $true
            }
        }
        $version = (& $candidate.Path --version 2>&1 | Out-String).Trim()
        $versionExit = $LASTEXITCODE
        $check = (& $candidate.Path -c "import importlib.util; print(int(importlib.util.find_spec('streamlit') is not None), int(importlib.util.find_spec('policydb') is not None))" 2>&1 | Out-String).Trim()
        $checkExit = $LASTEXITCODE
        $parts = $check -split '\s+'
        return [PSCustomObject]@{
            python_path = [IO.Path]::GetFullPath($candidate.Path)
            environment_name = $candidate.Name
            resolution_source = $candidate.Source
            python_version = if ($versionExit -eq 0) { $version } else { "unavailable" }
            streamlit_available = ($checkExit -eq 0 -and $parts.Count -ge 2 -and $parts[0] -eq "1")
            policydb_available = ($checkExit -eq 0 -and $parts.Count -ge 2 -and $parts[1] -eq "1")
        }
    }
    return $null
}

function Test-DashboardHealth {
    param([int]$HealthPort)
    if ($HealthPort -lt 1) { return $false }
    try {
        $response = Invoke-WebRequest -UseBasicParsing -Uri "http://127.0.0.1:$HealthPort/_stcore/health" -TimeoutSec 2
        return $response.StatusCode -eq 200 -and $response.Content.Trim().ToLowerInvariant() -eq "ok"
    }
    catch { return $false }
}

function Test-PortFree {
    param([int]$CandidatePort)
    $listener = $null
    try {
        $listener = [Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback, $CandidatePort)
        $listener.Start()
        return $true
    }
    catch { return $false }
    finally { if ($null -ne $listener) { $listener.Stop() } }
}

function Clear-StaleRuntime {
    Remove-Item -LiteralPath $PidFile, $PortFile, $StartedFile, $ProcessFile -Force -ErrorAction SilentlyContinue
}

function Test-ExistingDashboard {
    param([PSCustomObject]$PythonInfo)
    if (-not (Test-Path -LiteralPath $PidFile) -or -not (Test-Path -LiteralPath $PortFile)) {
        return $null
    }
    $savedPid = 0
    $savedPort = 0
    [void][int]::TryParse((Get-Content -LiteralPath $PidFile -Raw).Trim(), [ref]$savedPid)
    [void][int]::TryParse((Get-Content -LiteralPath $PortFile -Raw).Trim(), [ref]$savedPort)
    try { $process = Get-Process -Id $savedPid -ErrorAction Stop } catch { $process = $null }
    $metadataMatches = $false
    if (Test-Path -LiteralPath $ProcessFile) {
        try {
            $metadata = Get-Content -LiteralPath $ProcessFile -Raw -Encoding UTF8 | ConvertFrom-Json
            $metadataMatches = (
                [int]$metadata.pid -eq $savedPid -and
                [int]$metadata.port -eq $savedPort -and
                [string]$metadata.command_signature -eq "policydb-streamlit-dashboard"
            )
        }
        catch { $metadataMatches = $false }
    }
    $pathMatches = $false
    if ($process) {
        try { $pathMatches = [IO.Path]::GetFullPath($process.Path) -eq $PythonInfo.python_path } catch { $pathMatches = $process.ProcessName -like "python*" }
    }
    if ($process -and $pathMatches -and $metadataMatches -and (Test-DashboardHealth $savedPort)) {
        return [PSCustomObject]@{ Pid = $savedPid; Port = $savedPort }
    }
    Write-LauncherLog "stale runtime cleared pid=$savedPid port=$savedPort process_alive=$([bool]$process) path_matches=$pathMatches metadata_matches=$metadataMatches"
    Clear-StaleRuntime
    return $null
}

function Open-Dashboard {
    param([int]$OpenPort)
    $url = "http://127.0.0.1:$OpenPort"
    Write-Host "[完成] 正在打开房地产政策数据库：$url" -ForegroundColor Green
    if ($NoBrowser) { Write-LauncherLog "browser skipped url=$url"; return }
    try {
        $info = [Diagnostics.ProcessStartInfo]::new()
        $info.FileName = $url
        $info.UseShellExecute = $true
        [void][Diagnostics.Process]::Start($info)
        Write-LauncherLog "browser open succeeded url=$url"
    }
    catch {
        Write-LauncherLog "browser open failed type=$($_.Exception.GetType().Name) message=$($_.Exception.Message)"
        Write-Host "[提示] 浏览器未能自动打开，请手工访问：$url" -ForegroundColor Yellow
    }
}

function Show-LauncherError {
    param([string]$Summary, [string]$PythonPath)
    if ($NoGui -or -not [Environment]::UserInteractive) { return }
    try {
        Add-Type -AssemblyName System.Windows.Forms
        Add-Type -AssemblyName System.Drawing
        $form = [Windows.Forms.Form]::new()
        $form.Text = "房地产政策数据库 - 启动失败"
        $form.Size = [Drawing.Size]::new(680, 300)
        $form.StartPosition = "CenterScreen"
        $form.TopMost = $true
        $label = [Windows.Forms.Label]::new()
        $label.Location = [Drawing.Point]::new(20, 20)
        $label.Size = [Drawing.Size]::new(625, 150)
        $label.Text = "启动失败`r`n`r`n错误概要：$Summary`r`nPython：$PythonPath`r`n日志：$LauncherLog"
        $form.Controls.Add($label)
        $buttons = @(
            @{ Text = "打开日志"; X = 20; Action = { if (Test-Path $LauncherLog) { Start-Process notepad.exe -ArgumentList ('"{0}"' -f $LauncherLog) } } },
            @{ Text = "打开项目目录"; X = 160; Action = { Start-Process explorer.exe -ArgumentList ('"{0}"' -f $Root) } },
            @{ Text = "重新修复环境"; X = 320; Action = { Start-Process "$env:SystemRoot\System32\WindowsPowerShell\v1.0\powershell.exe" -ArgumentList @("-NoProfile", "-ExecutionPolicy", "Bypass", "-File", ('"{0}"' -f (Join-Path $PSScriptRoot "first_setup.ps1"))) } },
            @{ Text = "关闭"; X = 500; Action = { $form.Close() } }
        )
        foreach ($item in $buttons) {
            $button = [Windows.Forms.Button]::new()
            $button.Text = $item.Text
            $button.Location = [Drawing.Point]::new($item.X, 195)
            $button.Size = [Drawing.Size]::new(125, 34)
            $button.Add_Click($item.Action)
            $form.Controls.Add($button)
        }
        [void]$form.ShowDialog()
    }
    catch { Write-LauncherLog "error dialog failed: $($_.Exception.Message)" }
}

function Start-BackgroundDashboard {
    param([string]$PythonPath, [int]$WorkerPort)
    $currentPath = [Environment]::GetEnvironmentVariable("Path", "Process")
    [Environment]::SetEnvironmentVariable("PATH", $null, "Process")
    [Environment]::SetEnvironmentVariable("Path", $currentPath, "Process")
    $arguments = @(
        "-m", "streamlit", "run", ('"{0}"' -f $Dashboard),
        "--server.address=127.0.0.1", "--server.port=$WorkerPort", "--server.headless=true",
        "--server.fileWatcherType=none", "--server.runOnSave=false", "--runner.fastReruns=false",
        "--browser.gatherUsageStats=false"
    )
    Start-Process -FilePath $PythonPath -ArgumentList $arguments -WorkingDirectory $Root `
        -RedirectStandardOutput $OutputLog -RedirectStandardError $DashboardLog `
        -WindowStyle Hidden -PassThru
}

function Invoke-Launcher {
    Write-LauncherLog "===== launcher start ====="
    Write-LauncherLog "windows=$([Environment]::OSVersion.VersionString) powershell=$($PSVersionTable.PSVersion) root=$Root"
    Write-LauncherLog "bat_path=$(Join-Path $Root '打开房地产政策数据库.bat') script=$PSCommandPath"
    $python = Resolve-ProjectPython
    if ($null -eq $python) {
        Write-LauncherLog "no project python found; starting first setup"
        & (Join-Path $PSScriptRoot "first_setup.ps1") -SkipLaunch
        if ($LASTEXITCODE -ne 0) { throw "首次安装未完成，退出代码 $LASTEXITCODE" }
        $python = Resolve-ProjectPython
    }
    if ($null -eq $python) { throw "首次安装后仍未找到项目 Python。" }
    Write-LauncherLog "environment=$($python.environment_name) source=$($python.resolution_source) python=$($python.python_path) version=$($python.python_version)"
    Write-LauncherLog "streamlit_import=$($python.streamlit_available) policydb_import=$($python.policydb_available)"
    if (-not $python.streamlit_available -or -not $python.policydb_available) {
        throw '检测到环境，但项目依赖不完整。请使用重新修复环境。'
    }
    if (-not (Test-Path -LiteralPath $Dashboard)) { throw "未找到网站入口：$Dashboard" }
    Write-LauncherLog "database_exists=$(Test-Path (Join-Path $Root 'database\policydb.duckdb')) curated_exists=$(Test-Path (Join-Path $Root 'data\curated'))"
    $existing = Test-ExistingDashboard $python
    if ($existing) {
        Write-LauncherLog "existing dashboard reused pid=$($existing.Pid) port=$($existing.Port)"
        Open-Dashboard $existing.Port
        return 0
    }
    $ports = if ($Port -gt 0) { @($Port) } else { @(8501..8599) }
    $selectedPort = ($ports | Where-Object { Test-PortFree $_ } | Select-Object -First 1)
    if (-not $selectedPort) { throw "8501—8599 端口均被占用。" }
    Write-LauncherLog "selected_port=$selectedPort"
    if ((Test-Path $DashboardLog) -and (Get-Item $DashboardLog).Length -gt 10MB) {
        Move-Item $DashboardLog (Join-Path $Runtime "dashboard.previous.log") -Force
    }
    Remove-Item $OutputLog -Force -ErrorAction SilentlyContinue
    $env:POLICYDB_ROOT = $Root
    $env:POLARS_MAX_THREADS = "2"
    $env:OMP_NUM_THREADS = "1"
    $env:ARROW_NUM_THREADS = "2"
    $env:OPENBLAS_NUM_THREADS = "1"
    $env:MKL_NUM_THREADS = "1"
    $env:NUMEXPR_NUM_THREADS = "1"
    Write-LauncherLog "command=$($python.python_path) -m streamlit run app\dashboard.py --server.port=$selectedPort"
    $process = Start-BackgroundDashboard $python.python_path $selectedPort
    if ($null -eq $process) { throw "无法创建 Dashboard 后台进程。" }
    Write-StateFile $PidFile ([string]$process.Id)
    Write-StateFile $PortFile ([string]$selectedPort)
    Write-StateFile $StartedFile ([string]$process.StartTime.ToUniversalTime().Ticks)
    $metadata = @{
        pid = $process.Id; port = $selectedPort; started_at = $process.StartTime.ToUniversalTime().ToString("o")
        python_path = $python.python_path; command_signature = "policydb-streamlit-dashboard"
    } | ConvertTo-Json
    Write-StateFile $ProcessFile $metadata
    Write-LauncherLog "process_started pid=$($process.Id)"
    for ($attempt = 1; $attempt -le 60; $attempt++) {
        $healthy = Test-DashboardHealth $selectedPort
        Write-LauncherLog "health_attempt=$attempt port=$selectedPort healthy=$healthy exited=$($process.HasExited)"
        if ($healthy) {
            Open-Dashboard $selectedPort
            Write-LauncherLog "launcher exit_code=0"
            return 0
        }
        if ($process.HasExited) {
            Write-LauncherLog "dashboard exited code=$($process.ExitCode)"
            break
        }
        Start-Sleep -Seconds 1
    }
    if (-not $process.HasExited) { Stop-Process -Id $process.Id -Force -ErrorAction SilentlyContinue }
    Clear-StaleRuntime
    throw "Dashboard 未能完成健康检查；请查看 $DashboardLog"
}

if ($env:POLICYDB_LAUNCHER_LIBRARY_ONLY -eq "1") { return }

$selectedPython = "未选择"
try {
    $resolved = Resolve-ProjectPython
    if ($resolved) { $selectedPython = $resolved.python_path }
    $exitCode = Invoke-Launcher
}
catch {
    $exitCode = 1
    $summary = $_.Exception.Message
    Write-LauncherLog "exception_type=$($_.Exception.GetType().FullName) message=$summary"
    Write-LauncherLog "stack=$($_.ScriptStackTrace)"
    Write-LauncherLog "launcher exit_code=1"
    Write-Host "[错误] 网站启动失败：$summary" -ForegroundColor Red
    Write-Host "[日志] $LauncherLog" -ForegroundColor Yellow
    Show-LauncherError $summary $selectedPython
}
exit $exitCode
