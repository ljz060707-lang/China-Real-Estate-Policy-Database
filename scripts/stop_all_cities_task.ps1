[CmdletBinding()]
param(
    [string]$TaskName = "CRPD-All-Cities-Since-2018",
    [int]$WaitSeconds = 900
)

$Utf8 = New-Object System.Text.UTF8Encoding($false)
[Console]::InputEncoding = $Utf8
[Console]::OutputEncoding = $Utf8
$OutputEncoding = $Utf8
$ErrorActionPreference = "Stop"
$dataRoot = "E:\Data Set\CRPD"
$stopFile = Join-Path $dataRoot "control\STOP_FULL_SYNC"
$lockPath = Join-Path $dataRoot "jobs\all_cities_since_2018.lock"
New-Item -ItemType Directory -Force -Path (Split-Path $stopFile -Parent) | Out-Null
[System.IO.File]::WriteAllText($stopFile, ("operator_stop_request " + [DateTime]::UtcNow.ToString("o") + "`n"), $Utf8)

$deadline = (Get-Date).ToUniversalTime().AddSeconds($WaitSeconds)
while ((Get-Date).ToUniversalTime() -lt $deadline) {
    if (-not (Test-Path -LiteralPath $lockPath)) {
        [ordered]@{ task_name = $TaskName; stop_file = $stopFile; status = "STOPPED_AFTER_CHECKPOINT"; process_killed = $false } | ConvertTo-Json -Depth 5
        exit 0
    }
    Start-Sleep -Seconds 10
}

[ordered]@{ task_name = $TaskName; stop_file = $stopFile; status = "STOP_REQUESTED_WAITING_FOR_CURRENT_ATOMIC_STEP"; process_killed = $false; wait_seconds = $WaitSeconds } | ConvertTo-Json -Depth 5
exit 10
