[CmdletBinding()]
param(
    [string]$TaskName = "CRPD-All-Cities-Since-2018"
)

$Utf8 = New-Object System.Text.UTF8Encoding($false)
[Console]::InputEncoding = $Utf8
[Console]::OutputEncoding = $Utf8
$OutputEncoding = $Utf8
$ErrorActionPreference = "Continue"
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$dataRoot = "D:\Data Set\CRPD"
$pythonPath = Join-Path $repoRoot ".venv\Scripts\python.exe"
$monitorPath = Join-Path $repoRoot "scripts\monitor_all_cities_since_2018.py"
$task = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
$taskInfo = if ($task) { Get-ScheduledTaskInfo -TaskName $TaskName -ErrorAction SilentlyContinue } else { $null }
$automationRoot = Join-Path $dataRoot "outputs\all_cities_since_2018"
$monitorJson = & $pythonPath $monitorPath "--automation-root" $automationRoot 2>&1
$monitorExit = [int]$LASTEXITCODE
$health = $null
try { $health = ($monitorJson -join [Environment]::NewLine | ConvertFrom-Json) } catch { }
$pidValue = if ($health) { $health.runner_pid_actual } else { $null }

Write-Output "计划任务状态: $([string]$(if ($task) { $task.State } else { 'NotFound' }))"
Write-Output "任务名称: $TaskName"
Write-Output "声明状态: $($health.declared_status)"
Write-Output "有效状态: $($health.effective_status)"
Write-Output "runner PID (记录): $($health.runner_pid_recorded)"
Write-Output "runner PID (实时): $($health.runner_pid_actual)"
Write-Output "runner 进程存在: $($health.runner_process_running)"
Write-Output "Python worker 数量: $($health.worker_process_count)"
Write-Output "孤立/异常 runner: $($health.orphaned_runner)"
if ($taskInfo) {
    Write-Output "最近运行: $($taskInfo.LastRunTime)"
    Write-Output "下次运行: $($taskInfo.NextRunTime)"
    Write-Output "上次结果: $($taskInfo.LastTaskResult)"
    Write-Output "上次结果(hex): $($health.last_task_result_hex)"
}
if ($health) {
    Write-Output "当前 automation_id: $($health.automation_id)"
    Write-Output "当前 cycle: $($health.cycle)"
    Write-Output "当前 run_id: $($health.run_id)"
    Write-Output "global_status: $($health.global_status)"
    Write-Output "525 个槽位进度: $($health.slots.resolved)/$($health.slots.total) resolved; verified=$($health.slots.verified); enabled=$($health.slots.enabled); backfilled=$($health.slots.backfilled); current=$($health.slots.current); unresolved=$($health.slots.unresolved)"
    Write-Output "documents: $($health.documents.total); documents_added_last_run=$($health.documents.added_last_run)"
    Write-Output "open_gaps: $($health.gaps.open); critical_gaps: $($health.gaps.critical)"
    Write-Output "AI calls: $($health.ai.calls); AI tokens: $($health.ai.tokens); estimated cost: $($health.ai.estimated_cost_usd)"
    Write-Output "search calls: $($health.search_calls)"
    Write-Output "HTTP calls: $($health.http.used)/$($health.http.limit)"
    Write-Output "最近错误: $($health.latest_error)"
    Write-Output "最近进展时间: $($health.last_progress_at)"
    Write-Output "最近 heartbeat: $($health.last_heartbeat_at)"
    Write-Output "heartbeat age(s): $($health.heartbeat_age_seconds)"
    Write-Output "current_status 更新时间: $($health.last_status_update_at)"
    Write-Output "status age(s): $($health.status_age_seconds)"
    Write-Output "是否 STALLED: $($health.stalled)"
    Write-Output "是否存在 STOP 文件: $($health.stop_file_present)"
    Write-Output "是否需要重启: $($health.restart_required)"
    Write-Output "health_gate: $($health.health_gate)"
    Write-Output "--- machine health JSON ---"
    $health | ConvertTo-Json -Depth 12
}
else {
    Write-Output ($monitorJson -join [Environment]::NewLine)
}
if ($monitorExit -ne 0 -or -not $task -or ($health -and $health.health_gate -eq "BLOCKED")) { exit 10 }
exit 0
