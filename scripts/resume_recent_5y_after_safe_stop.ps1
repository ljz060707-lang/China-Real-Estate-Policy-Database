param(
    [int]$ObservedLauncherPid = 0,
    [string]$TaskName = "CRPD-Recent-5Y-Full-Crawl",
    [string]$DataRoot = "E:\Data Set\CRPD",
    [ValidateRange(5, 300)][int]$PollSeconds = 30
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$DataRoot = [System.IO.Path]::GetFullPath($DataRoot)
$automationRoot = Join-Path $DataRoot "automation"
$controlRoot = Join-Path $DataRoot "control"
$logPath = Join-Path $automationRoot "recent_5y_safe_handoff.log"
$stopFullSync = Join-Path $controlRoot "STOP_FULL_SYNC"
$stopAutopilot = Join-Path $controlRoot "STOP_AUTOPILOT"
$lockPath = Join-Path $automationRoot "FAST_5Y_WRITER.lock"
New-Item -ItemType Directory -Force -Path $automationRoot, $controlRoot | Out-Null

function Write-HandoffLog {
    param(
        [Parameter(Mandatory = $true)][string]$Message,
        [ValidateSet("INFO", "WARN", "ERROR")][string]$Level = "INFO"
    )
    $line = "{0} [{1}] {2}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $Level, $Message
    Add-Content -LiteralPath $logPath -Value $line -Encoding UTF8
}

function Get-ActiveRecentProcesses {
    return @(Get-CimInstance Win32_Process | Where-Object {
        $_.ProcessId -ne $PID -and $_.CommandLine -and (
            $_.CommandLine.IndexOf("run_recent_5y_direct.ps1", [System.StringComparison]::OrdinalIgnoreCase) -ge 0 -or
            $_.CommandLine.IndexOf("CRPD_Audited_Full_Backfill.ps1", [System.StringComparison]::OrdinalIgnoreCase) -ge 0 -or
            $_.CommandLine.IndexOf("policydb_entrypoint.py", [System.StringComparison]::OrdinalIgnoreCase) -ge 0
        )
    })
}

function Test-LockAvailable {
    $stream = $null
    try {
        $stream = [System.IO.File]::Open(
            $lockPath,
            [System.IO.FileMode]::OpenOrCreate,
            [System.IO.FileAccess]::ReadWrite,
            [System.IO.FileShare]::None
        )
        return $true
    }
    catch [System.IO.IOException] {
        return $false
    }
    finally {
        if ($null -ne $stream) {
            $stream.Dispose()
        }
    }
}

Write-HandoffLog "Safe handoff watcher started; observed_launcher_pid=$ObservedLauncherPid task=$TaskName"
while ($true) {
    $launcherAlive = $false
    if ($ObservedLauncherPid -gt 0) {
        $launcherAlive = $null -ne (Get-Process -Id $ObservedLauncherPid -ErrorAction SilentlyContinue)
    }
    $active = @(Get-ActiveRecentProcesses)
    if ($launcherAlive -or $active.Count -gt 0) {
        Start-Sleep -Seconds $PollSeconds
        continue
    }

    if (Test-Path -LiteralPath $stopAutopilot) {
        Write-HandoffLog "STOP_AUTOPILOT is present; leaving STOP_FULL_SYNC and not starting another task." "WARN"
        exit 21
    }
    $task = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
    if ($null -eq $task) {
        Write-HandoffLog "Scheduled task is missing; leaving the stop marker in place." "ERROR"
        exit 1
    }
    if ([string]$task.State -eq "Running") {
        Start-Sleep -Seconds $PollSeconds
        continue
    }
    if (-not (Test-LockAvailable)) {
        Write-HandoffLog "Writer lock is still busy after process exit; not starting a duplicate task." "WARN"
        exit 20
    }

    if (Test-Path -LiteralPath $stopFullSync) {
        Remove-Item -LiteralPath $stopFullSync -Force
        Write-HandoffLog "Removed the known safe-stop marker after the old chain released the writer lock."
    }
    Start-ScheduledTask -TaskName $TaskName
    Write-HandoffLog "Started scheduled task $TaskName with the updated concurrent full-flow action."
    exit 0
}
