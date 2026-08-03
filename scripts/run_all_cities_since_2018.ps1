[CmdletBinding()]
param(
    [switch]$Once
)

$ErrorActionPreference = "Stop"
$Utf8 = New-Object System.Text.UTF8Encoding($false)
[Console]::InputEncoding = $Utf8
[Console]::OutputEncoding = $Utf8
$OutputEncoding = $Utf8
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$dataRoot = "D:\Data Set\CRPD"
$pythonPath = Join-Path $repoRoot ".venv\Scripts\python.exe"
$configPath = Join-Path $repoRoot "config\all_cities_since_2018.yaml"
$automationRoot = Join-Path $dataRoot "outputs\all_cities_since_2018"
$stopFile = Join-Path $dataRoot "control\STOP_FULL_SYNC"
$lockPath = Join-Path $dataRoot "jobs\all_cities_since_2018.lock"
$taskName = "CRPD-All-Cities-Since-2018"

if (-not (Test-Path -LiteralPath $pythonPath)) {
    throw "fixed project virtual environment is missing: $pythonPath"
}

$env:POLICYDB_ROOT = $repoRoot
$env:CRPD_DATA_ROOT = $dataRoot
$env:PYTHONUNBUFFERED = "1"

function Read-JsonSafe {
    param([Parameter(Mandatory = $true)][string]$Path)
    if (-not (Test-Path -LiteralPath $Path)) { return $null }
    try {
        return (Get-Content -LiteralPath $Path -Raw -Encoding UTF8 | ConvertFrom-Json)
    }
    catch {
        return $null
    }
}

function Get-JsonValue {
    param(
        [object]$Object,
        [Parameter(Mandatory = $true)][string]$Name,
        [object]$Default = $null
    )
    if ($null -ne $Object -and $null -ne $Object.PSObject.Properties[$Name]) {
        return $Object.$Name
    }
    return $Default
}

function Write-AtomicJson {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][object]$Value
    )
    $parent = Split-Path -Parent $Path
    New-Item -ItemType Directory -Force -Path $parent | Out-Null
    $temporary = "$Path.$PID.$([guid]::NewGuid().ToString('N')).tmp"
    $encoding = New-Object System.Text.UTF8Encoding($false)
    try {
        [System.IO.File]::WriteAllText($temporary, ($Value | ConvertTo-Json -Depth 16), $encoding)
        Move-Item -LiteralPath $temporary -Destination $Path -Force
    }
    finally {
        if (Test-Path -LiteralPath $temporary) {
            Remove-Item -LiteralPath $temporary -Force -ErrorAction SilentlyContinue
        }
    }
}

function Write-RunnerLog {
    param([Parameter(Mandatory = $true)][string]$Message)
    if (-not $script:RunnerLogPath) { return }
    try {
        $line = "[$([DateTime]::UtcNow.ToString('o'))] $Message$([Environment]::NewLine)"
        [System.IO.File]::AppendAllText($script:RunnerLogPath, $line, $Utf8)
    }
    catch {
        # Logging must never turn a recoverable database run into a new failure.
    }
}

function Test-ProcessAlive {
    param([object]$ProcessId)
    try {
        $pidValue = [int]$ProcessId
        if ($pidValue -le 0) { return $false }
        $null = Get-Process -Id $pidValue -ErrorAction Stop
        return $true
    }
    catch {
        return $false
    }
}

function Add-AutomationTransition {
    param(
        [Parameter(Mandatory = $true)][string]$EventType,
        [Parameter(Mandatory = $true)][string]$ReasonCode,
        [string]$Cycle,
        [string]$RunId,
        [int]$ExitCode = 0
    )
    $event = [ordered]@{
        automation_id = $script:AutomationState.automation_id
        run_id = $RunId
        cycle = $Cycle
        event_type = $EventType
        reason_code = $ReasonCode
        timestamp = [DateTime]::UtcNow.ToString("o")
        idempotency_key = ([Security.Cryptography.SHA256]::Create().ComputeHash([Text.Encoding]::UTF8.GetBytes("$($script:AutomationState.automation_id)|$Cycle|$EventType|$ReasonCode")) | ForEach-Object { $_.ToString("x2") }) -join ""
        exit_code = $ExitCode
    }
    $path = Join-Path $script:AutomationDir "automation_transitions.jsonl"
    $line = (($event | ConvertTo-Json -Compress -Depth 10) + [Environment]::NewLine)
    $bytes = [Text.Encoding]::UTF8.GetBytes($line)
    $stream = [System.IO.File]::Open($path, [System.IO.FileMode]::OpenOrCreate, [System.IO.FileAccess]::ReadWrite, [System.IO.FileShare]::Read)
    try {
        $stream.Seek(0, [System.IO.SeekOrigin]::End) | Out-Null
        $stream.Write($bytes, 0, $bytes.Length)
        $stream.Flush($true)
    }
    finally {
        $stream.Dispose()
    }
}

function Set-AutomationState {
    param([hashtable]$Updates)
    foreach ($key in $Updates.Keys) {
        $script:AutomationState[$key] = $Updates[$key]
    }
    $script:AutomationState.last_heartbeat_at = [DateTime]::UtcNow.ToString("o")
    Write-AtomicJson -Path (Join-Path $script:AutomationDir "automation_state.json") -Value $script:AutomationState
    Write-AtomicJson -Path (Join-Path $automationRoot "current_automation.json") -Value ([ordered]@{
        automation_id = $script:AutomationState.automation_id
        automation_dir = $script:AutomationDir
        updated_at = $script:AutomationState.last_heartbeat_at
    })
}

function Update-CycleRunnerExit {
    param(
        [Parameter(Mandatory = $true)][long]$ExitCode,
        [Parameter(Mandatory = $true)][string]$ExitReason,
        [Parameter(Mandatory = $true)][string]$ExitTime,
        [bool]$Unexpected = $false
    )
    $cycleDir = [string](Get-JsonValue $script:AutomationState "current_cycle_dir" "")
    if (-not $cycleDir -or -not (Test-Path -LiteralPath $cycleDir)) { return }
    $statusPath = Join-Path $cycleDir "current_status.json"
    $current = Read-JsonSafe $statusPath
    if ($current) {
        $current | Add-Member -NotePropertyName runner_exit_code -NotePropertyValue $ExitCode -Force
        $current | Add-Member -NotePropertyName runner_exit_reason -NotePropertyValue $ExitReason -Force
        $current | Add-Member -NotePropertyName runner_exit_time -NotePropertyValue $ExitTime -Force
        $current | Add-Member -NotePropertyName runner_checkpoint_preserved -NotePropertyValue $true -Force
        $current | Add-Member -NotePropertyName runner_restart_recommended -NotePropertyValue $Unexpected -Force
        if ($Unexpected) {
            $current | Add-Member -NotePropertyName runner_status -NotePropertyValue "STOPPED_UNEXPECTEDLY" -Force
        }
        Write-AtomicJson -Path $statusPath -Value $current
    }
}

function Write-RunnerExit {
    param(
        [Parameter(Mandatory = $true)][long]$ExitCode,
        [Parameter(Mandatory = $true)][string]$ExitReason,
        [bool]$CheckpointPreserved = $true,
        [bool]$RestartRecommended = $false
    )
    if ($script:ExitRecordWritten) { return }
    $script:ExitRecordWritten = $true
    $exitTime = [DateTime]::UtcNow.ToString("o")
    $record = [ordered]@{
        automation_id = (Get-JsonValue $script:AutomationState "automation_id" $null)
        exit_code = $ExitCode
        exit_reason = $ExitReason
        exit_time = $exitTime
        last_cycle = (Get-JsonValue $script:AutomationState "current_cycle" $null)
        last_run_id = (Get-JsonValue $script:AutomationState "current_run_id" $null)
        checkpoint_preserved = $CheckpointPreserved
        restart_recommended = $RestartRecommended
        runner_pid = $PID
    }
    try {
        $script:AutomationState.exit_code = $ExitCode
        $script:AutomationState.exit_reason = $ExitReason
        $script:AutomationState.exit_time = $exitTime
        $script:AutomationState.checkpoint_preserved = $CheckpointPreserved
        $script:AutomationState.restart_recommended = $RestartRecommended
        $script:AutomationState.runner_pid = $null
        $script:AutomationState.finished_at = if (Get-JsonValue $script:AutomationState "finished_at" $null) { $script:AutomationState.finished_at } else { $exitTime }
        Write-AtomicJson -Path (Join-Path $script:AutomationDir "automation_state.json") -Value $script:AutomationState
        Write-AtomicJson -Path (Join-Path $script:AutomationDir "runner_exit.json") -Value $record
        $cycleDir = [string](Get-JsonValue $script:AutomationState "current_cycle_dir" "")
        if ($cycleDir -and (Test-Path -LiteralPath $cycleDir)) {
            Write-AtomicJson -Path (Join-Path $cycleDir "runner_exit.json") -Value $record
        }
        $historyLine = (($record | ConvertTo-Json -Compress -Depth 10) + [Environment]::NewLine)
        [System.IO.File]::AppendAllText((Join-Path $script:AutomationDir "runner_exit_history.jsonl"), $historyLine, $Utf8)
        Update-CycleRunnerExit -ExitCode $ExitCode -ExitReason $ExitReason -ExitTime $exitTime -Unexpected $RestartRecommended
    }
    catch {
        Write-RunnerLog ("failed to persist runner exit evidence: " + $_.Exception.Message)
    }
}

function Write-PreviousInterruptionEvidence {
    param([object]$PreviousPid, [string]$PreviousStatus)
    $recordPath = Join-Path $script:AutomationDir "runner_exit.json"
    if (Test-Path -LiteralPath $recordPath) { return }
    $exitTime = [DateTime]::UtcNow.ToString("o")
    $record = [ordered]@{
        automation_id = (Get-JsonValue $script:AutomationState "automation_id" $null)
        exit_code = $null
        exit_reason = "external_termination_or_process_loss"
        exit_time = $exitTime
        last_cycle = (Get-JsonValue $script:AutomationState "current_cycle" $null)
        last_run_id = (Get-JsonValue $script:AutomationState "current_run_id" $null)
        checkpoint_preserved = $true
        restart_recommended = $true
        previous_runner_pid = $PreviousPid
        previous_status = $PreviousStatus
        observed_at_resume = $true
    }
    try {
        Write-AtomicJson -Path $recordPath -Value $record
        $historyLine = (($record | ConvertTo-Json -Compress -Depth 10) + [Environment]::NewLine)
        [System.IO.File]::AppendAllText((Join-Path $script:AutomationDir "runner_exit_history.jsonl"), $historyLine, $Utf8)
        $script:AutomationState.previous_exit_reason = $record.exit_reason
        $script:AutomationState.previous_exit_observed_at = $exitTime
        $script:AutomationState.previous_runner_pid = $PreviousPid
        $script:AutomationState.previous_checkpoint_preserved = $true
        $script:AutomationState.status = "STOPPED_UNEXPECTEDLY"
        $script:AutomationState.current_step = "previous_exit_reconciled"
        $script:AutomationState.latest_error = $record.exit_reason
        Write-AtomicJson -Path (Join-Path $script:AutomationDir "automation_state.json") -Value $script:AutomationState
        Update-CycleRunnerExit -ExitCode 3221225786 -ExitReason $record.exit_reason -ExitTime $exitTime -Unexpected $true
    }
    catch {
        Write-RunnerLog ("failed to persist previous interruption evidence: " + $_.Exception.Message)
    }
}

function Test-StopRequested {
    return (Test-Path -LiteralPath $stopFile)
}

function Invoke-CrpdCommand {
    param(
        [Parameter(Mandatory = $true)][string]$Step,
        [Parameter(Mandatory = $true)][string]$CycleDir,
        [Parameter(Mandatory = $true)][string]$RunId,
        [Parameter(Mandatory = $true)][string[]]$Arguments
    )
    if (Test-StopRequested) {
        throw "STOP_REQUESTED"
    }
    $stdoutPath = Join-Path $CycleDir "$Step.stdout.log"
    $stderrPath = Join-Path $CycleDir "$Step.stderr.log"
    Set-AutomationState @{ current_step = $Step; current_cycle_dir = $CycleDir; current_run_id = $RunId; latest_error = $null }
    Add-AutomationTransition -EventType "${Step}_started" -ReasonCode "scheduled_task_step" -Cycle (Split-Path $CycleDir -Leaf) -RunId $RunId
    Write-RunnerLog ("step_started step=$Step cycle=$CycleDir run_id=$RunId")
    $exitCode = 1
    Push-Location $repoRoot
    try {
        & $pythonPath @Arguments 1>> $stdoutPath 2>> $stderrPath
        $exitCode = [int]$LASTEXITCODE
    }
    finally {
        Pop-Location
    }
    Add-AutomationTransition -EventType "${Step}_completed" -ReasonCode "process_exit" -Cycle (Split-Path $CycleDir -Leaf) -RunId $RunId -ExitCode $exitCode
    Write-RunnerLog ("step_completed step=$Step exit_code=$exitCode")
    return $exitCode
}

function New-FullSyncArguments {
    param(
        [Parameter(Mandatory = $true)][string]$Command,
        [Parameter(Mandatory = $true)][string]$CycleDir,
        [Parameter(Mandatory = $true)][string]$RunId,
        [switch]$Apply
    )
    $backfillTo = (Get-Date).ToUniversalTime().ToString("yyyy-MM-dd")
    $arguments = @(
        "-m", "policydb.autopilot_cli", "full-sync", $Command,
        "--scope", "all",
        "--discovery-mode", "SEARCH_AND_AI",
        "--discover-missing",
        "--verify-candidates",
        "--enable-ready",
        "--backfill",
        "--incremental",
        "--repair-gaps",
        "--until-current",
        "--max-slots", "20",
        "--max-sources", "20",
        "--max-documents", "1000",
        "--top-k", "3",
        "--concurrency", "1",
        "--discovery-concurrency", "1",
        "--crawl-concurrency", "1",
        "--max-ai-calls", "40",
        "--max-search-calls", "100",
        "--max-http-calls", "50000",
        "--budget-tokens", "5000000",
        "--rate-limit-per-minute", "20",
        "--lookback-days", "30",
        "--max-consecutive-failures", "3",
        "--backfill-from", "2018-01-01",
        "--backfill-to", $backfillTo,
        "--output", $CycleDir,
        "--run-id", $RunId,
        "--format", "json,xlsx,parquet"
    )
    if ($Apply) { $arguments += "--apply" }
    if ($Command -in @("run", "resume")) { $arguments += "--resume" }
    return ,$arguments
}

function Invoke-CheckpointRepair {
    param([Parameter(Mandatory = $true)][string]$CycleDir, [Parameter(Mandatory = $true)][string]$RunId)
    $arguments = @("-m", "policydb.autopilot_cli", "repair-checkpoints", "--run-dir", $CycleDir, "--apply")
    return Invoke-CrpdCommand -Step "repair" -CycleDir $CycleDir -RunId $RunId -Arguments $arguments
}

function Update-StateFromCycle {
    param([Parameter(Mandatory = $true)][string]$CycleDir)
    $current = Read-JsonSafe (Join-Path $CycleDir "current_status.json")
    $database = Read-JsonSafe (Join-Path $CycleDir "database_sync_status.json")
    $budget = Read-JsonSafe (Join-Path $CycleDir "budget_usage.json")
    $provider = Read-JsonSafe (Join-Path $CycleDir "provider_health.json")
    $used = Get-JsonValue $budget "used" ([pscustomobject]@{})
    $limits = Get-JsonValue $budget "limits" ([pscustomobject]@{})
    $updates = @{
        global_status = (Get-JsonValue $database "global_status" (Get-JsonValue $current "global_status" "UNKNOWN"))
        resolved = [int](Get-JsonValue $database "resolved_slots" (Get-JsonValue $current "resolved" 0))
        total_slots = [int](Get-JsonValue $database "total_slots" 525)
        verified = [int](Get-JsonValue $database "verified_slots" (Get-JsonValue $current "verified" 0))
        enabled = [int](Get-JsonValue $database "enabled_slots" (Get-JsonValue $current "enabled" 0))
        backfilled = [int](Get-JsonValue $database "backfilled_slots" 0)
        current = [int](Get-JsonValue $database "current_slots" 0)
        unresolved = [int](Get-JsonValue $database "unresolved_slots" (Get-JsonValue $current "unresolved" 0))
        documents = [int](Get-JsonValue $database "total_documents" 0)
        documents_added_last_run = [int](Get-JsonValue $database "documents_added_last_run" 0)
        open_gaps = [int](Get-JsonValue $database "open_gaps" 0)
        critical_gaps = [int](Get-JsonValue $database "critical_gaps" 0)
        ai_calls = [int](Get-JsonValue $current "ai_calls" (Get-JsonValue $used "ai_calls" 0))
        ai_tokens = (Get-JsonValue $current "tokens" (Get-JsonValue $used "tokens" $null))
        estimated_cost_usd = (Get-JsonValue $current "estimated_cost_usd" $null)
        usage_status = (Get-JsonValue $current "usage_status" (Get-JsonValue $provider "usage_status" "unavailable"))
        search_calls = [int](Get-JsonValue $current "search_calls" (Get-JsonValue $used "search_calls" 0))
        http_calls = [int](Get-JsonValue $current "http_calls" (Get-JsonValue $used "http_calls" 0))
        max_http_calls = [int](Get-JsonValue $limits "http_calls" 50000)
        latest_error = (Get-JsonValue $current "latest_error" $null)
        last_progress_at = (Get-JsonValue $current "last_progress_at" $script:AutomationState.last_progress_at)
        last_heartbeat_at = (Get-JsonValue $current "last_heartbeat_at" ([DateTime]::UtcNow.ToString("o")))
    }
    Set-AutomationState $updates
    return [pscustomobject]@{ current = $current; database = $database; budget = $budget; provider = $provider }
}

function Wait-WithHeartbeat {
    param([int]$Seconds)
    for ($second = 0; $second -lt $Seconds; $second += 10) {
        if (Test-StopRequested) { return $false }
        Start-Sleep -Seconds ([Math]::Min(10, $Seconds - $second))
        Set-AutomationState @{ last_progress_at = $script:AutomationState.last_progress_at }
    }
    return $true
}

New-Item -ItemType Directory -Force -Path $automationRoot, (Split-Path $stopFile -Parent), (Split-Path $lockPath -Parent) | Out-Null
$existingAutomationDirs = @(Get-ChildItem -LiteralPath $automationRoot -Directory -ErrorAction SilentlyContinue | Where-Object { Test-Path -LiteralPath (Join-Path $_.FullName "automation_state.json") } | Sort-Object LastWriteTime -Descending)
$automationDir = if ($existingAutomationDirs.Count -gt 0) { $existingAutomationDirs[0].FullName } else { $null }
$existingState = if ($automationDir) { Read-JsonSafe (Join-Path $automationDir "automation_state.json") } else { $null }
$automationId = if ($existingState -and (Get-JsonValue $existingState "automation_id" $null)) { [string](Get-JsonValue $existingState "automation_id" $null) } else { "AUTO_" + [DateTime]::UtcNow.ToString("yyyyMMddTHHmmssZ") }
if (-not $automationDir) {
    $automationDir = Join-Path $automationRoot $automationId
}
New-Item -ItemType Directory -Force -Path $automationDir | Out-Null

$script:AutomationDir = $automationDir
$script:RunnerLogPath = Join-Path $automationDir "runner.log"
$script:RunnerTranscriptPath = Join-Path $automationDir "runner.transcript.log"
$script:TranscriptStarted = $false
$script:ExitRecordWritten = $false
$script:ExitReason = "normal_exit"
$script:AutomationState = [ordered]@{
    automation_id = $automationId
    task_name = $taskName
    repo_root = $repoRoot
    data_root = $dataRoot
    config_path = $configPath
    status = "STARTING"
    runner_pid = $PID
    provider = "siliconflow"
    model = "zai-org/GLM-5.2"
    current_cycle = $null
    current_cycle_dir = $null
    current_run_id = $null
    current_step = "startup"
    total_slots = 525
    resolved = 0
    verified = 0
    enabled = 0
    backfilled = 0
    current = 0
    unresolved = 525
    documents = 0
    documents_added_last_run = 0
    open_gaps = 0
    critical_gaps = 0
    ai_calls = 0
    ai_tokens = $null
    estimated_cost_usd = $null
    usage_status = "unavailable"
    search_calls = 0
    http_calls = 0
    max_http_calls = 50000
    latest_error = $null
    no_progress_cycles = 0
    progress_signature = $null
    started_at = [DateTime]::UtcNow.ToString("o")
    last_progress_at = [DateTime]::UtcNow.ToString("o")
    last_heartbeat_at = [DateTime]::UtcNow.ToString("o")
    stalled_after_hours = 6
    stop_file = $stopFile
    exit_code = $null
    exit_reason = $null
    exit_time = $null
    checkpoint_preserved = $null
    restart_recommended = $null
}
if ($existingState) {
    foreach ($property in $existingState.PSObject.Properties) {
        $script:AutomationState[$property.Name] = $property.Value
    }
}
$previousStatus = [string](Get-JsonValue $existingState "status" "")
$previousPid = Get-JsonValue $existingState "runner_pid" $null
$previousCycleDir = [string](Get-JsonValue $existingState "current_cycle_dir" "")
$previousPidAlive = Test-ProcessAlive $previousPid
if ($previousCycleDir -and (Test-Path -LiteralPath $previousCycleDir)) {
    try {
        Update-StateFromCycle -CycleDir $previousCycleDir | Out-Null
    }
    catch {
        Write-RunnerLog ("checkpoint state reconciliation failed: " + $_.Exception.Message)
    }
}
if ($previousStatus -in @("STARTING", "RUNNING", "SOURCE_COMPLETION", "DISCOVERING", "CRAWLING", "BACKFILLING", "INCREMENTAL", "REPAIRING") -and $previousPid -and -not $previousPidAlive) {
    Write-PreviousInterruptionEvidence -PreviousPid $previousPid -PreviousStatus $previousStatus
}
$script:AutomationState.runner_pid = $PID
$script:AutomationState.status = if (Test-StopRequested) { "STOPPED" } else { "RUNNING" }
$script:AutomationState.current_step = if (Test-StopRequested) { "stop_requested_pending_start" } else { "startup" }
$script:AutomationState.last_heartbeat_at = [DateTime]::UtcNow.ToString("o")
try {
    Start-Transcript -Path $script:RunnerTranscriptPath -Append -Force | Out-Null
    $script:TranscriptStarted = $true
}
catch {
    Write-RunnerLog ("PowerShell transcript unavailable: " + $_.Exception.Message)
}
Write-RunnerLog ("runner_started pid=$PID automation_id=$automationId resumed_cycle=$previousCycleDir stop_requested=$(Test-StopRequested)")

$mutex = New-Object System.Threading.Mutex($false, "Global\CRPD_All_Cities_Since_2018")
$ownsMutex = $false
$ownsProcessLock = $false
$lockStream = $null
$finalExitCode = 0
$script:NoProgressCycles = [int](Get-JsonValue $existingState "no_progress_cycles" 0)

try {
    try {
        $ownsMutex = $mutex.WaitOne(0)
    }
    catch [System.Threading.AbandonedMutexException] {
        $ownsMutex = $true
    }
    if (-not $ownsMutex) {
        Write-Output "another task instance owns the process mutex; no work started"
        $script:ExitReason = "duplicate_instance_no_work"
        $finalExitCode = 0
    }
    else {
        Write-AtomicJson -Path (Join-Path $automationDir "automation_state.json") -Value $script:AutomationState
        Add-AutomationTransition -EventType "automation_started" -ReasonCode "scheduled_task_start" -Cycle $null -RunId $null
        $lockStream = [System.IO.File]::Open($lockPath, [System.IO.FileMode]::Create, [System.IO.FileAccess]::Write, [System.IO.FileShare]::Read)
        $ownsProcessLock = $true
        $lockBytes = [Text.Encoding]::UTF8.GetBytes(("PID=" + $PID + "`nAUTOMATION_ID=" + $automationId + "`n"))
        $lockStream.Write($lockBytes, 0, $lockBytes.Length)
        $lockStream.Flush($true)

        while ($true) {
            if (Test-StopRequested) {
                Set-AutomationState @{ status = "STOPPED"; current_step = "stop_requested"; runner_pid = $PID }
                Add-AutomationTransition -EventType "automation_stopped" -ReasonCode "STOP_FULL_SYNC_present" -Cycle $script:AutomationState.current_cycle -RunId $script:AutomationState.current_run_id
                $script:ExitReason = "STOP_FULL_SYNC_present"
                $finalExitCode = 21
                break
            }

            $reuseCycle = $false
            $cycleDir = [string](Get-JsonValue $script:AutomationState "current_cycle_dir" "")
            $priorStep = [string](Get-JsonValue $script:AutomationState "current_step" "")
            if ($cycleDir -and (Test-Path -LiteralPath $cycleDir) -and $priorStep -notin @("cycle_completed", "completed")) {
                $reuseCycle = $true
            }
            if (-not $reuseCycle) {
                $cycleNumbers = @(Get-ChildItem -LiteralPath $automationDir -Directory -Filter "cycle_*" -ErrorAction SilentlyContinue | ForEach-Object { if ($_.Name -match '^cycle_(\d+)$') { [int]$Matches[1] } })
                $nextNumber = if ($cycleNumbers.Count -gt 0) { [int](($cycleNumbers | Measure-Object -Maximum).Maximum + 1) } else { 1 }
                $cycleName = "cycle_{0:D4}" -f $nextNumber
                $cycleDir = Join-Path $automationDir $cycleName
                New-Item -ItemType Directory -Force -Path $cycleDir | Out-Null
                $runId = "$automationId`_$cycleName"
                Set-AutomationState @{ current_cycle = $cycleName; current_cycle_dir = $cycleDir; current_run_id = $runId; current_step = "cycle_start"; latest_error = $null }
                Add-AutomationTransition -EventType "cycle_started" -ReasonCode "bounded_resume_cycle" -Cycle $cycleName -RunId $runId
            }
            else {
                $cycleName = Split-Path $cycleDir -Leaf
                $runId = [string](Get-JsonValue $script:AutomationState "current_run_id" "$automationId`_$cycleName")
                Set-AutomationState @{ current_cycle = $cycleName; current_cycle_dir = $cycleDir; current_run_id = $runId; current_step = "cycle_resume" }
                Add-AutomationTransition -EventType "cycle_resumed" -ReasonCode "checkpoint_resume" -Cycle $cycleName -RunId $runId
            }

            if (-not (Test-Path -LiteralPath (Join-Path $cycleDir "coverage_snapshot.json"))) {
                $planArguments = New-FullSyncArguments -Command "plan" -CycleDir $cycleDir -RunId $runId
                $planExit = Invoke-CrpdCommand -Step "plan" -CycleDir $cycleDir -RunId $runId -Arguments $planArguments
                if ($planExit -notin @(0, 10)) {
                    throw "plan failed with exit code $planExit"
                }
            }
            if (Test-StopRequested) { continue }

            $runCommand = if ($reuseCycle) { "resume" } else { "run" }
            $runArguments = New-FullSyncArguments -Command $runCommand -CycleDir $cycleDir -RunId $runId -Apply
            $runExit = Invoke-CrpdCommand -Step $runCommand -CycleDir $cycleDir -RunId $runId -Arguments $runArguments
            $statusArguments = New-FullSyncArguments -Command "status" -CycleDir $cycleDir -RunId $runId
            $statusExit = Invoke-CrpdCommand -Step "status" -CycleDir $cycleDir -RunId $runId -Arguments $statusArguments
            if ($runExit -in @(0, 10)) {
                $repairExit = Invoke-CheckpointRepair -CycleDir $cycleDir -RunId $runId
                if ($repairExit -ne 0) { throw "checkpoint repair failed with exit code $repairExit" }
            }
            $reportArguments = New-FullSyncArguments -Command "report" -CycleDir $cycleDir -RunId $runId
            $reportExit = Invoke-CrpdCommand -Step "report" -CycleDir $cycleDir -RunId $runId -Arguments $reportArguments
            if ($reportExit -ne 0) { throw "report failed with exit code $reportExit" }
            $cycleData = Update-StateFromCycle -CycleDir $cycleDir
            $database = $cycleData.database
            $criticalGaps = [int](Get-JsonValue $database "critical_gaps" 0)
            $consistencyErrors = @(Get-JsonValue $database "consistency_errors" @())
            $markers = @("BUDGET_LEDGER_INCONSISTENT", "DUPLICATE_INSERT", "CHECKPOINT_CONFLICT", "SCHEMA_DRIFT", "DATABASE_WRITE_ERROR")
            $logText = ""
            foreach ($log in @(Get-ChildItem -LiteralPath $cycleDir -Filter "*.stderr.log" -ErrorAction SilentlyContinue)) {
                $logText += (Get-Content -LiteralPath $log.FullName -Raw -ErrorAction SilentlyContinue)
            }
            $markerHit = $markers | Where-Object { $logText -match [regex]::Escape($_) }
            if ($criticalGaps -gt 0 -or $consistencyErrors.Count -gt 0 -or $markerHit) {
                $reason = if ($criticalGaps -gt 0) { "critical_gaps" } elseif ($consistencyErrors.Count -gt 0) { "consistency_errors" } else { [string]$markerHit[0] }
                Set-AutomationState @{ status = "FAILED"; current_step = "health_gate_failed"; latest_error = $reason; runner_pid = $PID }
                Add-AutomationTransition -EventType "automation_failed" -ReasonCode $reason -Cycle $cycleName -RunId $runId -ExitCode 1
                $script:ExitReason = $reason
                $finalExitCode = 1
                break
            }
            if (Test-StopRequested) {
                Set-AutomationState @{ status = "STOPPED"; current_step = "stop_after_cycle"; runner_pid = $PID }
                Add-AutomationTransition -EventType "automation_stopped" -ReasonCode "STOP_FULL_SYNC_after_atomic_cycle" -Cycle $cycleName -RunId $runId -ExitCode 21
                $script:ExitReason = "STOP_FULL_SYNC_after_atomic_cycle"
                $finalExitCode = 21
                break
            }
            if ($runExit -eq 20 -or [string](Get-JsonValue $database "global_status" "") -eq "PAUSED_BUDGET") {
                Set-AutomationState @{ status = "PAUSED_BUDGET"; current_step = "provider_or_budget_backoff"; latest_error = "provider_or_budget_backoff" }
                Add-AutomationTransition -EventType "cycle_paused" -ReasonCode "provider_or_budget_backoff" -Cycle $cycleName -RunId $runId -ExitCode $runExit
                if ($Once -or -not (Wait-WithHeartbeat -Seconds 900)) {
                    $script:ExitReason = if (Test-StopRequested) { "STOP_FULL_SYNC_during_budget_backoff" } else { "provider_or_budget_backoff" }
                    $finalExitCode = if (Test-StopRequested) { 21 } else { 0 }
                    break
                }
                continue
            }
            if ($runExit -notin @(0, 10)) {
                Set-AutomationState @{ status = "FAILED"; current_step = "cycle_failed"; latest_error = "full-sync exit code $runExit"; runner_pid = $PID }
                Add-AutomationTransition -EventType "automation_failed" -ReasonCode "full_sync_error" -Cycle $cycleName -RunId $runId -ExitCode $runExit
                $script:ExitReason = "full_sync_error"
                $finalExitCode = 1
                break
            }

            $signature = "$($script:AutomationState.resolved)|$($script:AutomationState.verified)|$($script:AutomationState.enabled)|$($script:AutomationState.backfilled)|$($script:AutomationState.documents)|$($script:AutomationState.ai_calls)|$($script:AutomationState.search_calls)|$($script:AutomationState.http_calls)"
            if ([string](Get-JsonValue $script:AutomationState "progress_signature" "") -eq $signature) {
                $script:NoProgressCycles++
            }
            else {
                $script:NoProgressCycles = 0
                $script:AutomationState.last_progress_at = [DateTime]::UtcNow.ToString("o")
            }
            Set-AutomationState @{ status = "RUNNING"; current_step = "cycle_completed"; progress_signature = $signature; no_progress_cycles = $script:NoProgressCycles }
            Add-AutomationTransition -EventType "cycle_completed" -ReasonCode "batch_successful" -Cycle $cycleName -RunId $runId -ExitCode $runExit
            if ($script:AutomationState.total_slots -eq 525 -and $script:AutomationState.unresolved -eq 0 -and $script:AutomationState.verified -eq 525 -and $script:AutomationState.enabled -eq 525 -and $script:AutomationState.backfilled -eq 525) {
                Set-AutomationState @{ status = "COMPLETED"; current_step = "all_cities_complete"; finished_at = [DateTime]::UtcNow.ToString("o"); runner_pid = $PID }
                Add-AutomationTransition -EventType "automation_completed" -ReasonCode "525_slots_verified_enabled_backfilled" -Cycle $cycleName -RunId $runId
                $script:ExitReason = "all_cities_complete"
                $finalExitCode = 0
                break
            }
            if ($script:NoProgressCycles -ge 3) {
                Set-AutomationState @{ status = "STALLED"; current_step = "no_business_progress"; latest_error = "three consecutive cycles without progress"; finished_at = [DateTime]::UtcNow.ToString("o"); runner_pid = $PID }
                Add-AutomationTransition -EventType "automation_stopped" -ReasonCode "three_consecutive_no_progress_cycles" -Cycle $cycleName -RunId $runId
                $script:ExitReason = "three_consecutive_no_progress_cycles"
                $finalExitCode = 0
                break
            }
            if ($Once) {
                Set-AutomationState @{ status = "PAUSED_AFTER_CYCLE"; current_step = "once_completed"; runner_pid = $PID }
                $script:ExitReason = "once_completed"
                $finalExitCode = 0
                break
            }
            if (-not (Wait-WithHeartbeat -Seconds 300)) {
                Set-AutomationState @{ status = "STOPPED"; current_step = "stop_during_backoff"; runner_pid = $PID }
                $script:ExitReason = "STOP_FULL_SYNC_during_backoff"
                $finalExitCode = 21
                break
            }
        }
    }
}
catch {
    $message = $_.Exception.Message
    if ($message -eq "STOP_REQUESTED" -or (Test-StopRequested)) {
        try {
            Set-AutomationState @{ status = "STOPPED"; current_step = "stop_requested"; latest_error = $null; runner_pid = $PID }
            Add-AutomationTransition -EventType "automation_stopped" -ReasonCode "STOP_FULL_SYNC" -Cycle $script:AutomationState.current_cycle -RunId $script:AutomationState.current_run_id -ExitCode 21
        }
        catch { Write-RunnerLog ("failed to persist STOP state: " + $_.Exception.Message) }
        $script:ExitReason = "STOP_FULL_SYNC"
        $finalExitCode = 21
    }
    else {
        try {
            Set-AutomationState @{ status = "FAILED"; current_step = "unhandled_exception"; latest_error = $message.Substring(0, [Math]::Min(1000, $message.Length)); runner_pid = $PID }
            Add-AutomationTransition -EventType "automation_failed" -ReasonCode "unhandled_exception" -Cycle $script:AutomationState.current_cycle -RunId $script:AutomationState.current_run_id -ExitCode 1
        }
        catch { Write-RunnerLog ("failed to persist unhandled exception: " + $_.Exception.Message) }
        Write-RunnerLog ("unhandled_exception: " + $message)
        $script:ExitReason = "unhandled_exception"
        $finalExitCode = 1
    }
}
finally {
    if ($ownsMutex) {
        try {
            $restartRecommended = [bool]($finalExitCode -notin @(0, 10, 21) -and -not (Test-StopRequested))
            Write-RunnerExit -ExitCode $finalExitCode -ExitReason $script:ExitReason -CheckpointPreserved $true -RestartRecommended $restartRecommended
        }
        catch { Write-RunnerLog ("failed to finalize runner exit: " + $_.Exception.Message) }
    }
    if ($lockStream) { $lockStream.Dispose() }
    if ($ownsMutex) { $mutex.ReleaseMutex() }
    $mutex.Dispose()
    if ($ownsProcessLock -and (Test-Path -LiteralPath $lockPath)) { Remove-Item -LiteralPath $lockPath -Force -ErrorAction SilentlyContinue }
    if ($script:TranscriptStarted) {
        try { Stop-Transcript | Out-Null } catch { }
    }
}
exit $finalExitCode
