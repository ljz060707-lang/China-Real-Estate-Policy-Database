[CmdletBinding()]
param(
    [string]$ProjectRoot = 'D:\Codex\projects\Documents-Codex\2026-07-13\text-20260705-xlsx-text-data-raw\policy-database',
    [string]$DataRoot = 'E:\Data Set\CRPD',
    [switch]$StartTask = $true
)

$ErrorActionPreference = 'Stop'
$python = Join-Path $ProjectRoot '.venv\Scripts\python.exe'
$controller = Join-Path $ProjectRoot 'scripts\crpd_autonomous_controller.py'
$config = Join-Path $ProjectRoot 'config\crpd_autonomous.json'
$taskName = 'CRPD_Autonomous_Database_Completion'
$supervisor = Join-Path $ProjectRoot 'scripts\CRPD_Autonomous_Supervisor.ps1'

& $python $controller install --project-root $ProjectRoot --data-root $DataRoot --config $config
if ($LASTEXITCODE -ne 0) { throw "Autonomous state install failed: $LASTEXITCODE" }
& $python $controller 'dry-run' --project-root $ProjectRoot --data-root $DataRoot --config $config
if ($LASTEXITCODE -ne 0) { throw "Autonomous dry-run failed: $LASTEXITCODE" }

$actionArguments = "-NoLogo -NoProfile -NonInteractive -ExecutionPolicy Bypass -WindowStyle Hidden -File `"$supervisor`" -ProjectRoot `"$ProjectRoot`" -DataRoot `"$DataRoot`""
$action = New-ScheduledTaskAction -Execute 'powershell.exe' -Argument $actionArguments -WorkingDirectory $ProjectRoot
$start = (Get-Date).AddMinutes(1)
$interval = New-TimeSpan -Minutes 30
$duration = New-TimeSpan -Days 3650
$trigger = New-ScheduledTaskTrigger -Once -At $start -RepetitionInterval $interval -RepetitionDuration $duration
$settings = New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew -StartWhenAvailable -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -ExecutionTimeLimit ([TimeSpan]::Zero)
$identity = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
$principal = New-ScheduledTaskPrincipal -UserId $identity -LogonType Interactive -RunLevel Limited
$task = Register-ScheduledTask -TaskName $taskName -Action $action -Trigger $trigger -Settings $settings -Principal $principal -Force
# New-ScheduledTaskTrigger requires a duration parameter on this Windows build.
# Remove that optional XML element immediately so repetition has no scheduler
# expiry; the worker itself stops only on completion, a health gate, or STOP.
$registeredXml = Export-ScheduledTask -TaskName $taskName
$registeredXml = $registeredXml -replace '<Duration>[^<]+</Duration>', ''
$registeredXml = $registeredXml -replace '<StopAtDurationEnd>[^<]+</StopAtDurationEnd>', ''
$task = Register-ScheduledTask -TaskName $taskName -Xml $registeredXml -Force
$taskState = (Get-ScheduledTask -TaskName $taskName).State
$actionText = "powershell.exe $actionArguments"
& $python $controller record-task --project-root $ProjectRoot --data-root $DataRoot --config $config --task-name $taskName --action '__GENERATE__' --interval-minutes 30 --task-state ([string]$taskState)
if ($LASTEXITCODE -ne 0) { throw "Task metadata write failed: $LASTEXITCODE" }

if ($StartTask) {
    Start-ScheduledTask -TaskName $taskName
    Start-Sleep -Seconds 3
}
$info = Get-ScheduledTaskInfo -TaskName $taskName
[pscustomobject]@{
    task_name = $taskName
    task_state = (Get-ScheduledTask -TaskName $taskName).State
    last_run_time = $info.LastRunTime
    last_task_result = $info.LastTaskResult
    next_run_time = $info.NextRunTime
    action = $actionText
    current_run_protection = (Test-Path -LiteralPath (Join-Path $DataRoot 'logs\audited_full_backfill'))
} | ConvertTo-Json -Depth 5
