[CmdletBinding()]
param(
    [string]$ProjectRoot = "E:\policy-database",
    [string]$DataRoot = "E:\Data Set\CRPD",
    [string]$TaskName = "CRPD-Recent-5Y-Full-Crawl"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ProjectRoot = [System.IO.Path]::GetFullPath($ProjectRoot)
$DataRoot = [System.IO.Path]::GetFullPath($DataRoot)
$automationRoot = Join-Path $DataRoot "automation"
$controlRoot = Join-Path $DataRoot "control"
$stopPath = Join-Path $controlRoot "STOP_FULL_SYNC"
$lockPath = Join-Path $automationRoot "FAST_5Y_WRITER.lock"
$logPath = Join-Path $automationRoot "COLLECTION_ONLY_SAFE_RESUME.log"
$jurisdictionalScript = Join-Path $ProjectRoot "scripts\run_jurisdictional_backfill.ps1"

New-Item -ItemType Directory -Force -Path $automationRoot, $controlRoot | Out-Null

function Write-ResumeLog {
    param([Parameter(Mandatory = $true)][string]$Message)
    $line = "{0} [INFO] {1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $Message
    Add-Content -LiteralPath $logPath -Value $line -Encoding UTF8
}

function Get-ProductionProcesses {
    return @(Get-CimInstance Win32_Process | Where-Object {
        $_.ProcessId -ne $PID -and $_.CommandLine -and (
            $_.CommandLine.IndexOf("run_recent_5y_direct.ps1", [System.StringComparison]::OrdinalIgnoreCase) -ge 0 -or
            $_.CommandLine.IndexOf("CRPD_Audited_Full_Backfill.ps1", [System.StringComparison]::OrdinalIgnoreCase) -ge 0 -or
            $_.CommandLine.IndexOf("policydb_entrypoint.py", [System.StringComparison]::OrdinalIgnoreCase) -ge 0 -or
            $_.CommandLine.IndexOf("policydb.autopilot_cli", [System.StringComparison]::OrdinalIgnoreCase) -ge 0
        )
    })
}

function Test-LockFree {
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
    catch {
        return $false
    }
    finally {
        if ($null -ne $stream) { $stream.Dispose() }
    }
}

Write-ResumeLog "Collection-only safe-resume monitor started."
while (Test-Path -LiteralPath $stopPath) {
    $active = @(Get-ProductionProcesses)
    if ($active.Count -gt 0) {
        Write-ResumeLog ("Waiting for safe boundary; production process count={0}." -f $active.Count)
        Start-Sleep -Seconds 30
        continue
    }
    if (-not (Test-LockFree)) {
        Write-ResumeLog "Writer lock is still held; retrying."
        Start-Sleep -Seconds 30
        continue
    }

    $task = Get-ScheduledTask -TaskName $TaskName -ErrorAction Stop
    if ($task.State -eq "Running") {
        Write-ResumeLog "Scheduled task is still marked Running; retrying handoff."
        Start-Sleep -Seconds 15
        continue
    }

    try {
        Remove-Item -LiteralPath $stopPath -Force
        Start-ScheduledTask -TaskName $TaskName
        Write-ResumeLog "STOP_FULL_SYNC removed and collection-only city task started."
        $cityTaskObserved = $false
        for ($attempt = 1; $attempt -le 12; $attempt++) {
            if (@(Get-ProductionProcesses).Count -gt 0) {
                $cityTaskObserved = $true
                break
            }
            Start-Sleep -Seconds 5
        }
        if (-not $cityTaskObserved) {
            throw "City task did not expose a live production process after scheduled start."
        }
        if (-not (Test-Path -LiteralPath $jurisdictionalScript)) {
            throw "Missing jurisdictional launcher: $jurisdictionalScript"
        }
        Start-Process -FilePath "powershell.exe" -WindowStyle Hidden -ArgumentList @(
            "-NoLogo", "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass",
            "-File", $jurisdictionalScript,
            "-ProjectRoot", $ProjectRoot,
            "-DataRoot", $DataRoot,
            "-SkipAI"
        ) | Out-Null
        Write-ResumeLog "Unique no-classification national/provincial waiter started."
        break
    }
    catch {
        if (-not (Test-Path -LiteralPath $stopPath)) {
            New-Item -ItemType File -Path $stopPath | Out-Null
        }
        Write-ResumeLog ("Safe handoff failed; STOP_FULL_SYNC restored: {0}" -f $_.Exception.Message)
        Start-Sleep -Seconds 30
    }
}

Write-ResumeLog "Collection-only safe-resume monitor exited."
