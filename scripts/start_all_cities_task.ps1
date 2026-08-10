[CmdletBinding()]
param(
    [string]$TaskName = "CRPD-All-Cities-Since-2018"
)

$Utf8 = New-Object System.Text.UTF8Encoding($false)
[Console]::InputEncoding = $Utf8
[Console]::OutputEncoding = $Utf8
$OutputEncoding = $Utf8
$ErrorActionPreference = "Stop"
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$dataRoot = "E:\Data Set\CRPD"
$stopFile = Join-Path $dataRoot "control\STOP_FULL_SYNC"
$lockPath = Join-Path $dataRoot "jobs\all_cities_since_2018.lock"

New-Item -ItemType Directory -Force -Path (Split-Path $stopFile -Parent), (Split-Path $lockPath -Parent) | Out-Null

$task = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
if (-not $task) {
    throw "scheduled task is not deployed: $TaskName"
}

$action = @($task.Actions | Select-Object -First 1)
$actionText = "$($action.Execute) $($action.Arguments)"
foreach ($requiredToken in @("-NoLogo", "-NoProfile", "-NonInteractive", "-WindowStyle Hidden", "-ExecutionPolicy Bypass")) {
    if ($actionText -notmatch [regex]::Escape($requiredToken)) {
        throw "scheduled task action is missing required token: $requiredToken"
    }
}
$taskXml = Export-ScheduledTask -TaskName $TaskName
if ($taskXml -match '<StopOnIdleEnd>true</StopOnIdleEnd>' -or $taskXml -match '<RunOnlyIfIdle>true</RunOnlyIfIdle>' -or $taskXml -match '<IdleDuration>PT[1-9]') {
    throw "scheduled task still has an automatic idle-stop setting"
}

Remove-Item -LiteralPath $stopFile -Force -ErrorAction SilentlyContinue

if (Test-Path -LiteralPath $lockPath) {
    $lockText = Get-Content -LiteralPath $lockPath -Raw -Encoding UTF8 -ErrorAction SilentlyContinue
    $pidMatch = [regex]::Match($lockText, "PID=(\d+)")
    $live = $false
    if ($pidMatch.Success) {
        try { $null = Get-Process -Id ([int]$pidMatch.Groups[1].Value) -ErrorAction Stop; $live = $true } catch { $live = $false }
    }
    if ($live) {
        Write-Output ("runner already active; pid=" + $pidMatch.Groups[1].Value)
        exit 0
    }
    Remove-Item -LiteralPath $lockPath -Force -ErrorAction SilentlyContinue
}

$liveRunner = @(Get-CimInstance Win32_Process -ErrorAction SilentlyContinue | Where-Object {
    $_.CommandLine -and $_.CommandLine -match 'run_all_cities_since_2018\.ps1'
})
if ($liveRunner.Count -gt 0) {
    Write-Output ("independent runner already detected; pids=" + (($liveRunner | ForEach-Object { $_.ProcessId }) -join ','))
    exit 0
}

if ([string]$task.State -eq "Running") {
    Write-Output "scheduled task is already Running"
    exit 0
}

Start-ScheduledTask -TaskName $TaskName
$running = $false
for ($attempt = 0; $attempt -lt 30; $attempt++) {
    Start-Sleep -Seconds 1
    $currentTask = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
    if ($currentTask -and [string]$currentTask.State -eq "Running") {
        $running = $true
        break
    }
}
if (-not $running) {
    $info = Get-ScheduledTaskInfo -TaskName $TaskName -ErrorAction SilentlyContinue
    throw "scheduled task did not enter Running within 30 seconds; last_result=$($info.LastTaskResult)"
}
$info = Get-ScheduledTaskInfo -TaskName $TaskName
$liveRunner = @(Get-CimInstance Win32_Process -ErrorAction SilentlyContinue | Where-Object {
    $_.CommandLine -and $_.CommandLine -match 'run_all_cities_since_2018\.ps1'
})
[ordered]@{
    task_name = $TaskName
    task_state = [string](Get-ScheduledTask -TaskName $TaskName).State
    last_run_time = $info.LastRunTime
    next_run_time = $info.NextRunTime
    action = "Start-ScheduledTask"
    stop_file_present = Test-Path -LiteralPath $stopFile
    starter_pid = $PID
    runner_pids = @($liveRunner | ForEach-Object { $_.ProcessId })
    starter_exited_after_start = $true
} | ConvertTo-Json -Depth 5
