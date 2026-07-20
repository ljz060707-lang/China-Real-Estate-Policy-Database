$ErrorActionPreference = "Stop"
$Root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$Runtime = Join-Path $Root ".runtime"
$PowerShell = "$env:SystemRoot\System32\WindowsPowerShell\v1.0\powershell.exe"
$results = [Collections.Generic.List[object]]::new()
$failures = 0

function Add-Result {
    param([string]$Name, [bool]$Passed, [string]$Detail)
    $script:results.Add([PSCustomObject]@{ Test = $Name; Passed = $Passed; Detail = $Detail })
    if (-not $Passed) { $script:failures++ }
}

function Invoke-PowerShellScript {
    param([string]$Path, [string]$Arguments = "")
    $start = [Diagnostics.ProcessStartInfo]::new()
    $start.FileName = $PowerShell
    $start.Arguments = "-NoProfile -ExecutionPolicy Bypass -File `"$Path`" $Arguments"
    $start.UseShellExecute = $false
    $start.CreateNoWindow = $true
    $process = [Diagnostics.Process]::Start($start)
    $process.WaitForExit()
    return $process.ExitCode
}

$env:POLICYDB_LAUNCHER_LIBRARY_ONLY = "1"
. (Join-Path $PSScriptRoot "launch_dashboard.ps1")
Remove-Item Env:POLICYDB_LAUNCHER_LIBRARY_ONLY

$testRoot = Join-Path $Runtime "launcher-tests-路径"
Remove-Item $testRoot -Recurse -Force -ErrorAction SilentlyContinue
New-Item (Join-Path $testRoot "data\reference") -ItemType Directory -Force | Out-Null

try {
    New-Item (Join-Path $testRoot ".venv\Scripts") -ItemType Directory -Force | Out-Null
    New-Item (Join-Path $testRoot ".venv\Scripts\python.exe") -ItemType File -Force | Out-Null
    $resolved = Resolve-ProjectPython $testRoot -SkipDependencyCheck
    Add-Result "only .venv" ($resolved.environment_name -eq ".venv") $resolved.python_path

    Remove-Item (Join-Path $testRoot ".venv") -Recurse -Force
    New-Item (Join-Path $testRoot ".venv-1\Scripts") -ItemType Directory -Force | Out-Null
    New-Item (Join-Path $testRoot ".venv-1\Scripts\python.exe") -ItemType File -Force | Out-Null
    $resolved = Resolve-ProjectPython $testRoot -SkipDependencyCheck
    Add-Result "only .venv-1" ($resolved.environment_name -eq ".venv-1") $resolved.python_path

    New-Item (Join-Path $testRoot ".venv\Scripts") -ItemType Directory -Force | Out-Null
    New-Item (Join-Path $testRoot ".venv\Scripts\python.exe") -ItemType File -Force | Out-Null
    $resolved = Resolve-ProjectPython $testRoot -SkipDependencyCheck
    Add-Result "both prefer .venv" ($resolved.environment_name -eq ".venv") $resolved.python_path
    Add-Result "Chinese path" ($resolved.python_path -like "*launcher-tests-路径*") $resolved.python_path
    Add-Result "uv not required with existing env" ($null -ne $resolved) "resolver does not invoke uv"

    Remove-Item (Join-Path $testRoot ".venv") -Recurse -Force
    Remove-Item (Join-Path $testRoot ".venv-1") -Recurse -Force
    Add-Result "no env triggers setup" ($null -eq (Resolve-ProjectPython $testRoot -SkipDependencyCheck)) "no interpreter"

    $cmd = Get-Content (Join-Path $Root "start_policydb.cmd") -Raw
    Add-Result "BAT preserves error" ($cmd -match "pause" -and $cmd -match "EXIT_CODE") "pause and exit propagation"

    [void](Invoke-PowerShellScript (Join-Path $PSScriptRoot "stop_dashboard.ps1"))
    $launchExit = Invoke-PowerShellScript (Join-Path $PSScriptRoot "launch_dashboard.ps1") "-NoBrowser -NoGui"
    $firstPid = (Get-Content (Join-Path $Runtime "dashboard.pid") -Raw).Trim()
    $firstPort = (Get-Content (Join-Path $Runtime "dashboard.port") -Raw).Trim()
    $healthy = Test-DashboardHealth ([int]$firstPort)
    Add-Result "successful launch" ($launchExit -eq 0 -and $healthy) "pid=$firstPid port=$firstPort"
    Add-Result "launcher log generated" (Test-Path (Join-Path $Runtime "launcher.log")) (Join-Path $Runtime "launcher.log")

    [void](Invoke-PowerShellScript (Join-Path $PSScriptRoot "launch_dashboard.ps1") "-NoBrowser -NoGui")
    $secondPid = (Get-Content (Join-Path $Runtime "dashboard.pid") -Raw).Trim()
    Add-Result "second launch reuses process" ($firstPid -eq $secondPid) "first=$firstPid second=$secondPid"
    [void](Invoke-PowerShellScript (Join-Path $PSScriptRoot "stop_dashboard.ps1"))

    $listener = [Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback, 8501)
    $listener.Start()
    try {
        [void](Invoke-PowerShellScript (Join-Path $PSScriptRoot "launch_dashboard.ps1") "-NoBrowser -NoGui")
        $fallbackPort = [int](Get-Content (Join-Path $Runtime "dashboard.port") -Raw).Trim()
        Add-Result "8501 occupied uses next port" ($fallbackPort -eq 8502) "port=$fallbackPort"
    }
    finally {
        $listener.Stop()
        [void](Invoke-PowerShellScript (Join-Path $PSScriptRoot "stop_dashboard.ps1"))
    }

    [IO.File]::WriteAllText((Join-Path $Runtime "dashboard.pid"), "99999999")
    [IO.File]::WriteAllText((Join-Path $Runtime "dashboard.port"), "8501")
    [void](Invoke-PowerShellScript (Join-Path $PSScriptRoot "launch_dashboard.ps1") "-NoBrowser -NoGui")
    $freshPid = (Get-Content (Join-Path $Runtime "dashboard.pid") -Raw).Trim()
    Add-Result "stale PID cleaned" ($freshPid -ne "99999999") "new pid=$freshPid"
    [void](Invoke-PowerShellScript (Join-Path $PSScriptRoot "stop_dashboard.ps1"))
}
finally {
    Remove-Item $testRoot -Recurse -Force -ErrorAction SilentlyContinue
}

$results | Format-Table -AutoSize
if ($failures -gt 0) {
    Write-Error "$failures launcher tests failed"
    exit 1
}
Write-Host "All launcher tests passed." -ForegroundColor Green
exit 0
