[CmdletBinding()]
param(
    [string]$ProjectRoot = "",
    [string]$DataRoot = "E:\Data Set\CRPD",
    [string]$StartDate = "2021-09-01",
    [string]$EndDate = "today",
    [ValidateRange(1, 16)][int]$FetchConcurrency = 12,
    [ValidateRange(1, 2)][int]$PerHostConcurrency = 1,
    [ValidateRange(1, 8)][int]$ApiConcurrency = 6,
    [switch]$SkipAI,
    [switch]$NoSearch
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)

if ([string]::IsNullOrWhiteSpace($ProjectRoot)) {
    $ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
}
$ProjectRoot = [System.IO.Path]::GetFullPath($ProjectRoot)
$DataRoot = [System.IO.Path]::GetFullPath($DataRoot)
$python = Join-Path $ProjectRoot ".venv\Scripts\python.exe"
$cli = Join-Path $ProjectRoot ".venv\Scripts\policydb.exe"

# Collection-only mode is deliberate: search may assist source discovery,
# but semantic/API classification is disabled for this fast-ingest stage.
$classificationEnabled = $false
$automationRoot = Join-Path $DataRoot "automation"
$controlRoot = Join-Path $DataRoot "control"
$logRoot = Join-Path $DataRoot "logs\jurisdictional_backfill"
$lockPath = Join-Path $automationRoot "FAST_5Y_WRITER.lock"
$statePath = Join-Path $automationRoot "JURISDICTIONAL_BACKFILL_STATE.json"
$runId = "JURISDICTIONAL_{0}_{1}" -f (Get-Date -Format "yyyyMMdd_HHmmss"), ([guid]::NewGuid().ToString("N").Substring(0, 8))
$runLogRoot = Join-Path $logRoot $runId
$runLog = Join-Path $runLogRoot "launcher.log"

foreach ($path in @($python, $cli)) {
    if (-not (Test-Path -LiteralPath $path)) { throw "Required runtime path is missing: $path" }
}
New-Item -ItemType Directory -Force -Path $runLogRoot, $automationRoot, $controlRoot | Out-Null

# This child inherits no proxy configuration for government HTTP.  API/search
# credentials remain in the existing SecretStore/environment and are never
# included in this command line or in the state file.
$env:POLICYDB_ROOT = $ProjectRoot
$env:CRPD_DATA_ROOT = $DataRoot
$env:CRPD_GOVERNMENT_ROUTE = "direct"
$env:POLICYDB_ADAPTIVE_THROTTLE = "1"
$env:NO_PROXY = "*"
foreach ($name in @(
        "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy",
        "CRPD_PROXY_URL", "CRPD_AI_PROXY_URL", "CRPD_SEARCH_PROXY_URL"
    )) {
    Remove-Item -Path ("Env:" + $name) -ErrorAction SilentlyContinue
}

function Write-RunLog {
    param([Parameter(Mandatory = $true)][string]$Message,
        [ValidateSet("INFO", "WARN", "ERROR")][string]$Level = "INFO")
    $line = "{0} [{1}] {2}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $Level, $Message
    Add-Content -LiteralPath $runLog -Value $line -Encoding UTF8
    Write-Output $line
}

function Invoke-NativeCapture {
    param([Parameter(Mandatory = $true)][string[]]$Arguments)

    # Windows PowerShell turns native stderr into an ErrorRecord when it is
    # merged with stdout.  Keep the streams separate so benign Python/library
    # warnings cannot terminate this wrapper; the child exit code is the only
    # failure signal used by the launcher.
    $stdoutPath = "{0}.{1}.stdout.tmp" -f $runLog, $PID
    $stderrPath = "{0}.{1}.stderr.tmp" -f $runLog, $PID
    try {
        Remove-Item -LiteralPath $stdoutPath, $stderrPath -Force -ErrorAction SilentlyContinue
        $child = Start-Process -FilePath $cli -ArgumentList $Arguments -WorkingDirectory $ProjectRoot -RedirectStandardOutput $stdoutPath -RedirectStandardError $stderrPath -NoNewWindow -PassThru -Wait
        foreach ($path in @($stdoutPath, $stderrPath)) {
            if (Test-Path -LiteralPath $path) {
                $content = @(Get-Content -LiteralPath $path -Encoding UTF8 -ErrorAction SilentlyContinue)
                if ($content.Count -gt 0) {
                    $content | Add-Content -LiteralPath $runLog -Encoding UTF8
                    $content | Write-Host
                }
            }
        }
        return [int]$child.ExitCode
    }
    finally {
        Remove-Item -LiteralPath $stdoutPath, $stderrPath -Force -ErrorAction SilentlyContinue
    }
}

function Write-State {
    param([Parameter(Mandatory = $true)][string]$Status, [string]$Reason = "", [int]$ExitCode = 0)
    $payload = [ordered]@{
        run_id = $runId
        status = $Status
        reason = $Reason
        project_root = $ProjectRoot
        data_root = $DataRoot
        start_date = $StartDate
        end_date = $EndDate
        scope_types = @("national", "provincial")
        network_route = "direct"
        fetch_concurrency = $FetchConcurrency
        per_host_concurrency = $PerHostConcurrency
        api_concurrency = $ApiConcurrency
        skip_ai = $true
        classification_enabled = $classificationEnabled
        postprocess_mode = "CRAWL_ONLY"
        search_enabled = -not [bool]$NoSearch
        single_writer_lock = $lockPath
        exit_code = $ExitCode
        updated_at = (Get-Date).ToUniversalTime().ToString("o")
    }
    $temporary = "{0}.{1}.tmp" -f $statePath, $PID
    [System.IO.File]::WriteAllText($temporary, ($payload | ConvertTo-Json -Depth 8), [System.Text.UTF8Encoding]::new($false))
    Move-Item -LiteralPath $temporary -Destination $statePath -Force
}

function Test-StopRequested {
    return (Test-Path -LiteralPath (Join-Path $controlRoot "STOP_FULL_SYNC")) -or
        (Test-Path -LiteralPath (Join-Path $controlRoot "STOP_AUTOPILOT")) -or
        (Test-Path -LiteralPath (Join-Path $automationRoot "STOP_RECENT_5Y"))
}

function Get-ExistingProductionProcesses {
    try {
        return @(Get-CimInstance Win32_Process | Where-Object {
            $_.ProcessId -ne $PID -and $_.CommandLine -and
            $_.CommandLine -notmatch "run_jurisdictional_backfill\.ps1" -and
            $_.CommandLine -match "run_recent_5y_direct\.ps1|CRPD_Audited_Full_Backfill\.ps1|CRPD_Autonomous_Resume\.ps1|policydb\.autopilot|crpd_autonomous_controller\.py|policydb_entrypoint\.py"
        } | Select-Object ProcessId, ParentProcessId, Name, CommandLine)
    }
    catch { throw "Unable to inspect existing CRPD processes safely: $($_.Exception.Message)" }
}

$resultCode = 0
$lockStream = $null
try {
    Write-State -Status "WAITING_SINGLE_WRITER" -Reason "waiting_for_city_crawl_and_existing_production_chain"
    Write-RunLog "Jurisdictional follow-up queued; range=$StartDate..$EndDate; data=$DataRoot"
    while ($true) {
        if (Test-StopRequested) {
            Write-State -Status "STOPPED" -Reason "stop_file_present"
            Write-RunLog "Stop file present; no jurisdictional writer started." "WARN"
            break
        }
        $active = @(Get-ExistingProductionProcesses)
        if ($active.Count -gt 0) {
            Write-RunLog ("Waiting for existing production chain: {0} process(es)." -f $active.Count)
            Start-Sleep -Seconds 30
            continue
        }
        try {
            $lockStream = [System.IO.File]::Open(
                $lockPath, [System.IO.FileMode]::OpenOrCreate,
                [System.IO.FileAccess]::ReadWrite, [System.IO.FileShare]::None
            )
        }
        catch {
            Write-RunLog "Single-writer handoff lock is busy; retrying." "WARN"
            Start-Sleep -Seconds 30
            continue
        }
        try {
            $activeAfterLock = @(Get-ExistingProductionProcesses)
            if ($activeAfterLock.Count -gt 0) {
                Write-RunLog "A production process appeared during handoff; releasing launch lock." "WARN"
                continue
            }
            Write-State -Status "RUNNING" -Reason "jurisdictional_scope_started"
            Write-RunLog "Discovering and applying national/provincial official sources."
            $discoveryArgs = @("sources", "discover-jurisdictional", "--levels", "national,provincial", "--apply")
            if ($NoSearch) { $discoveryArgs += "--no-use-search" } else { $discoveryArgs += "--use-search" }
            $discoveryExitCode = Invoke-NativeCapture -Arguments $discoveryArgs
            if ($discoveryExitCode -ne 0) { throw "Jurisdictional source discovery failed with exit code $discoveryExitCode" }

            Write-RunLog "Starting bounded jurisdictional crawl: fetch=$FetchConcurrency per_host=$PerHostConcurrency api=$ApiConcurrency."
            $crawlArgs = @(
                "crawl", "jurisdictional", "--from", $StartDate, "--to", $EndDate,
                "--levels", "national,provincial", "--max-pages-per-source", "3000",
                "--max-candidates-per-source", "100000", "--max-candidates-total", "300000",
                "--max-fetches", "300000", "--fetch-concurrency", [string]$FetchConcurrency,
                "--per-host-concurrency", [string]$PerHostConcurrency,
                "--api-concurrency", [string]$ApiConcurrency, "--resume", "--archive"
            )
            $crawlArgs += "--no-run-ai"
            $resultCode = Invoke-NativeCapture -Arguments $crawlArgs
            if ($resultCode -eq 0) {
                Write-State -Status "COMPLETED" -Reason "jurisdictional_scope_finished" -ExitCode $resultCode
                Write-RunLog "Jurisdictional scope completed; historical evidence retained."
            }
            else {
                Write-State -Status "FAILED_RECOVERABLE" -Reason "jurisdictional_scope_exit_nonzero" -ExitCode $resultCode
                Write-RunLog "Jurisdictional scope exited with code $resultCode; checkpoints retained." "ERROR"
            }
            break
        }
        finally {
            if ($null -ne $lockStream) { $lockStream.Dispose(); $lockStream = $null }
        }
    }
}
catch {
    $resultCode = 1
    Write-RunLog $_.Exception.Message "ERROR"
    try { Write-State -Status "FAILED_RECOVERABLE" -Reason $_.Exception.Message -ExitCode $resultCode } catch {}
}
finally {
    if ($null -ne $lockStream) { $lockStream.Dispose() }
}
exit $resultCode
