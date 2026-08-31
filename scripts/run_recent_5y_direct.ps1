[CmdletBinding()]
param(
    [string]$ProjectRoot = "",
    [string]$DataRoot = "E:\Data Set\CRPD",
    [string]$StartDate = "2021-09-01",
    [string]$EndDate = "today",
    [int]$MaxPagesPerSource = 3000,
    [int]$MaxCandidatesPerShard = 100000,
    [int]$MaxFetchesPerShard = 100000,
    [int]$NetworkRetryPasses = 2
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)

if ([string]::IsNullOrWhiteSpace($ProjectRoot)) {
    $ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
}
$ProjectRoot = [System.IO.Path]::GetFullPath($ProjectRoot)
$DataRoot = [System.IO.Path]::GetFullPath($DataRoot)

$fullScript = Join-Path $ProjectRoot "scripts\CRPD_Audited_Full_Backfill.ps1"
$resumeScript = Join-Path $ProjectRoot "scripts\CRPD_Autonomous_Resume.ps1"
$python = Join-Path $ProjectRoot ".venv\Scripts\python.exe"
$automationRoot = Join-Path $DataRoot "automation"
$controlRoot = Join-Path $DataRoot "control"
$logRoot = Join-Path $DataRoot "logs\recent_5y_full_crawl"
$lockPath = Join-Path $automationRoot "FAST_5Y_WRITER.lock"
$statePath = Join-Path $automationRoot "RECENT_5Y_FULL_CRAWL_STATE.json"
$runId = "RECENT5Y_{0}_{1}" -f (Get-Date -Format "yyyyMMdd_HHmmss"), ([guid]::NewGuid().ToString("N").Substring(0, 8))
$runLogRoot = Join-Path $logRoot $runId
$runLog = Join-Path $runLogRoot "launcher.log"
$stopFullSync = Join-Path $controlRoot "STOP_FULL_SYNC"
$stopAutopilot = Join-Path $controlRoot "STOP_AUTOPILOT"
$automationStop = Join-Path $automationRoot "STOP"

foreach ($path in @($fullScript, $resumeScript, $python)) {
    if (-not (Test-Path -LiteralPath $path)) {
        throw "Required runtime path is missing: $path"
    }
}
New-Item -ItemType Directory -Force -Path $runLogRoot, $automationRoot, $controlRoot | Out-Null

# Government fetching is direct by construction.  These changes are process-local
# and do not alter the user's Windows-wide proxy or VPN configuration.
$env:POLICYDB_ROOT = $ProjectRoot
$env:CRPD_DATA_ROOT = $DataRoot
$env:CRPD_GOVERNMENT_ROUTE = "direct"
$env:NO_PROXY = "*"
foreach ($name in @(
        "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy",
        "CRPD_PROXY_URL", "CRPD_AI_PROXY_URL", "CRPD_SEARCH_PROXY_URL"
    )) {
    Remove-Item -Path ("Env:" + $name) -ErrorAction SilentlyContinue
}

function Write-RunLog {
    param(
        [Parameter(Mandatory = $true)][string]$Message,
        [ValidateSet("INFO", "WARN", "ERROR")][string]$Level = "INFO"
    )
    $line = "{0} [{1}] {2}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $Level, $Message
    Add-Content -LiteralPath $runLog -Value $line -Encoding UTF8
    Write-Output $line
}

function Write-State {
    param(
        [Parameter(Mandatory = $true)][string]$Status,
        [string]$Reason = "",
        [int]$ExitCode = 0
    )
    $payload = [ordered]@{
        run_id = $runId
        status = $Status
        reason = $Reason
        project_root = $ProjectRoot
        data_root = $DataRoot
        start_date = $StartDate
        end_date = $EndDate
        network_route = "direct"
        proxy_inheritance = "disabled_for_process"
        existing_sources_only = $true
        skip_ai = $true
        max_pages_per_source = $MaxPagesPerSource
        max_candidates_per_shard = $MaxCandidatesPerShard
        max_fetches_per_shard = $MaxFetchesPerShard
        network_retry_passes = $NetworkRetryPasses
        exit_code = $ExitCode
        updated_at = (Get-Date).ToUniversalTime().ToString("o")
    }
    $temporary = "{0}.{1}.tmp" -f $statePath, $PID
    $json = $payload | ConvertTo-Json -Depth 8
    [System.IO.File]::WriteAllText($temporary, $json, [System.Text.UTF8Encoding]::new($false))
    Move-Item -LiteralPath $temporary -Destination $statePath -Force
}

function Test-StopRequested {
    return (Test-Path -LiteralPath $stopFullSync) -or
        (Test-Path -LiteralPath $stopAutopilot)
}

function Get-ProductionProcesses {
    try {
        return @(Get-CimInstance Win32_Process | Where-Object {
            $_.Name -match "^python(w)?\.exe$" -and $_.CommandLine -and (
                $_.CommandLine.IndexOf("crpd_autonomous_controller.py", [System.StringComparison]::OrdinalIgnoreCase) -ge 0 -or
                $_.CommandLine.IndexOf("policydb.autopilot_cli", [System.StringComparison]::OrdinalIgnoreCase) -ge 0
            )
        } | Select-Object ProcessId, ParentProcessId, Name, CommandLine)
    }
    catch {
        throw "Unable to inspect production processes safely: $($_.Exception.Message)"
    }
}

$resultCode = 0
$lockStream = $null
try {
    Write-State -Status "WAITING_SINGLE_WRITER" -Reason "waiting_for_existing_E_runtime_to_release"
    Write-RunLog "Recent-5Y launcher queued; range=$StartDate..$EndDate; data=$DataRoot"

    while ($true) {
        if (Test-StopRequested) {
            Write-RunLog "Stop file present; preserving checkpoint and not starting recent-5Y crawl." "WARN"
            Write-State -Status "STOPPED" -Reason "stop_file_present"
            break
        }

        $active = @(Get-ProductionProcesses)
        if ($active.Count -gt 0) {
            Write-RunLog ("Waiting for existing production chain: {0} process(es)." -f $active.Count)
            Start-Sleep -Seconds 30
            continue
        }

        try {
            $lockStream = [System.IO.File]::Open(
                $lockPath,
                [System.IO.FileMode]::OpenOrCreate,
                [System.IO.FileAccess]::ReadWrite,
                [System.IO.FileShare]::None
            )
        }
        catch {
            Write-RunLog "Single-writer handoff lock is busy; retrying." "WARN"
            Start-Sleep -Seconds 30
            continue
        }

        try {
            $activeAfterLock = @(Get-ProductionProcesses)
            if ($activeAfterLock.Count -gt 0) {
                Write-RunLog "A production process appeared during handoff; releasing launch lock." "WARN"
                continue
            }

            Write-RunLog "Existing production chain is gone; resuming E checkpoint before full crawl."
            & powershell.exe -NoLogo -NoProfile -NonInteractive -ExecutionPolicy Bypass `
                -File $resumeScript -ProjectRoot $ProjectRoot -DataRoot $DataRoot
            if ($LASTEXITCODE -ne 0) {
                throw "Autonomous resume failed with exit code $LASTEXITCODE"
            }

            Write-State -Status "RUNNING" -Reason "official_recent_5y_full_backfill_started"
            Write-RunLog "Starting formal audited crawler with direct government route."
            & powershell.exe -NoLogo -NoProfile -NonInteractive -ExecutionPolicy Bypass `
                -File $fullScript `
                -ProjectRoot $ProjectRoot `
                -DataRoot $DataRoot `
                -StartDate $StartDate `
                -EndDate $EndDate `
                -MaxPagesPerSource $MaxPagesPerSource `
                -MaxCandidatesPerShard $MaxCandidatesPerShard `
                -MaxFetchesPerShard $MaxFetchesPerShard `
                -NetworkRetryPasses $NetworkRetryPasses `
                -ExistingSourcesOnly `
                -SkipAI
            $resultCode = [int]$LASTEXITCODE
            if (Test-StopRequested) {
                Write-State -Status "PAUSED" -Reason "safe_stop_after_checkpoint" -ExitCode $resultCode
                Write-RunLog "Formal crawler reached a safe stop boundary; checkpoint retained and run can resume." "WARN"
            }
            elseif ($resultCode -eq 0) {
                Write-State -Status "COMPLETED" -Reason "formal_backfill_exit_zero" -ExitCode $resultCode
                Write-RunLog "Recent-5Y formal crawler exited successfully."
            }
            else {
                Write-State -Status "FAILED_RECOVERABLE" -Reason "formal_backfill_exit_nonzero" -ExitCode $resultCode
                Write-RunLog "Recent-5Y formal crawler exited with code $resultCode; checkpoint retained." "ERROR"
            }
            break
        }
        finally {
            if ($null -ne $lockStream) {
                $lockStream.Dispose()
                $lockStream = $null
            }
        }
    }
}
catch {
    $resultCode = 1
    Write-RunLog $_.Exception.Message "ERROR"
    try { Write-State -Status "FAILED_RECOVERABLE" -Reason $_.Exception.Message -ExitCode $resultCode } catch {}
}
finally {
    if ($null -ne $lockStream) {
        $lockStream.Dispose()
    }
}
exit $resultCode
