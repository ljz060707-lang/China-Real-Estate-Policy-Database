[CmdletBinding()]
param(
    [string]$ProjectRoot = 'E:\policy-database',
    [string]$DataRoot = 'E:\Data Set\CRPD',
    [string]$TaskName = 'CRPD-Recent-5Y-Full-Crawl'
)
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$ProjectRoot = [IO.Path]::GetFullPath($ProjectRoot)
$DataRoot = [IO.Path]::GetFullPath($DataRoot)
$stop = Join-Path $DataRoot 'control\STOP_FULL_SYNC'
$token = 'QUALITY_V3_MAINTENANCE_' + [guid]::NewGuid().ToString('N')
$stamp = Get-Date -Format 'yyyyMMdd_HHmmss'
$audit = Join-Path $DataRoot "outputs\acceptance\crawl_quality_v3_handoff_$stamp"
New-Item -ItemType Directory -Path $audit -ErrorAction Stop | Out-Null
$statusFile = Join-Path $audit 'handoff.json'
$guard = $null
$writer = $null
$stopRemoved = $false
function Set-HandoffState([string]$State, [string]$Reason) {
    $stateValue = @{state=$State; reason=$Reason; updated_at=[DateTime]::UtcNow.ToString('o'); pid=$PID; audit=$audit; maintenance_token=$token}
    $temporary = "$statusFile.tmp"
    $stateValue | ConvertTo-Json | Set-Content -LiteralPath $temporary -Encoding UTF8
    Move-Item -LiteralPath $temporary -Destination $statusFile -Force
    Add-Content -LiteralPath (Join-Path $audit 'handoff.log') -Value "$([DateTime]::UtcNow.ToString('o')) $State $Reason" -Encoding UTF8
}
function Active-Production {
    return @(Get-CimInstance Win32_Process | Where-Object {
        $_.ProcessId -ne $PID -and $_.Name -match '^(pythonw?|powershell|pwsh)\.exe$' -and $_.CommandLine -and
        $_.CommandLine -match 'policydb_entrypoint\.py|policydb\.autopilot_cli|crpd_autonomous_controller\.py|run_recent_5y_direct\.ps1|CRPD_Audited_Full_Backfill\.ps1|run_jurisdictional_backfill\.ps1'
    })
}
try {
    $guardPath = Join-Path $DataRoot 'automation\QUALITY_V3_MAINTENANCE.lock'
    $guard = [IO.File]::Open($guardPath, 'OpenOrCreate', 'ReadWrite', 'None')
    # Do not adopt or remove an existing user/other-controller STOP.
    $stream = [IO.File]::Open($stop, 'CreateNew', 'Write', 'None')
    try {
        $bytes = [Text.Encoding]::UTF8.GetBytes($token)
        $stream.Write($bytes, 0, $bytes.Length)
        $stream.Flush($true)
    } finally { $stream.Dispose() }
    Set-HandoffState 'WAITING_SAFE_BOUNDARY' 'Own STOP registered; current writer finishes naturally; no process termination.'
    while ($true) {
        if ((Get-Content -LiteralPath $stop -Raw) -ne $token) { throw 'STOP ownership changed; refusing automatic resume.' }
        if (@(Active-Production).Count -gt 0) { Start-Sleep -Seconds 30; continue }
        if ((Get-ScheduledTask -TaskName $TaskName).State -eq 'Running') { Start-Sleep -Seconds 15; continue }
        try { $writer = [IO.File]::Open((Join-Path $DataRoot 'automation\FAST_5Y_WRITER.lock'), 'OpenOrCreate', 'ReadWrite', 'None') }
        catch { Start-Sleep -Seconds 30; continue }
        if (@(Active-Production).Count -gt 0) { $writer.Dispose(); $writer=$null; Start-Sleep -Seconds 30; continue }
        break
    }
    Set-HandoffState 'APPLYING' 'No production chain; exclusive writer handoff lock held.'
    $env:CRPD_MAINTENANCE_OWNER_PID = [string]$PID
    $env:PYTHONIOENCODING = 'utf-8'
    Set-Location $ProjectRoot
    $python = Join-Path $ProjectRoot '.venv\Scripts\python.exe'
    & $python -m policydb.crawl.reparse --root $ProjectRoot --output (Join-Path $audit 'repair') --apply *> (Join-Path $audit 'repair.log')
    if ($LASTEXITCODE -ne 0) { throw "Offline repair failed; exit=$LASTEXITCODE; STOP retained." }
    $summary = Get-Content -LiteralPath (Join-Path $audit 'repair\summary.json') -Raw | ConvertFrom-Json
    if ($summary.unresolved -ne 0) { throw 'Unresolved raw evidence; keeping STOP for review.' }
    $writer.Dispose(); $writer=$null
    if ((Get-Content -LiteralPath $stop -Raw) -ne $token) { throw 'STOP ownership changed; repair retained, automatic resume refused.' }
    Remove-Item -LiteralPath $stop
    $stopRemoved = $true
    Start-ScheduledTask -TaskName $TaskName
    Set-HandoffState 'RESUME_REQUESTED' 'Repair audited and original snapshot retained; scheduled city task requested.'
    $observed = $false
    for ($i=0; $i -lt 24; $i++) {
        if ((Get-ScheduledTask -TaskName $TaskName).State -eq 'Running' -and @(Active-Production).Count -gt 0) { $observed=$true; break }
        Start-Sleep -Seconds 5
    }
    if (-not $observed) { throw 'Task resume not observed; see scheduled task history. No duplicate launcher started.' }
    Set-HandoffState 'RESUMED' 'Scheduled crawler is independently running with parser v3 on next Python start.'
} catch {
    if ($stopRemoved -and -not (Test-Path -LiteralPath $stop)) {
        $stream = [IO.File]::Open($stop, 'CreateNew', 'Write', 'None')
        try {
            $bytes = [Text.Encoding]::UTF8.GetBytes($token)
            $stream.Write($bytes, 0, $bytes.Length)
            $stream.Flush($true)
        } finally { $stream.Dispose() }
    }
    Set-HandoffState 'BLOCKED' $_.Exception.Message
    exit 1
} finally {
    if ($null -ne $writer) { $writer.Dispose() }
    if ($null -ne $guard) { $guard.Dispose() }
}
