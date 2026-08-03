param(
    [switch]$Watch,
    [ValidateRange(5, 3600)]
    [int]$IntervalSeconds = 30
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

# CRPD 全量抓取只读监控
$Repo = "D:\Codex\projects\Documents-Codex\2026-07-13\text-20260705-xlsx-text-data-raw\policy-database"
$DataRoot = "D:\Data Set\CRPD"
$ExpectedSlots = 525

$ContinuousRoot = Join-Path $DataRoot "outputs\continuous_full_sync"
$FullSyncRoot = Join-Path $DataRoot "outputs\full_sync"
$MonitorRoot = Join-Path $DataRoot "outputs\monitoring"
$LockFile = Join-Path $DataRoot "locks\all_cities_since_2018.lock"
$StopFile = Join-Path $DataRoot "control\STOP_FULL_SYNC"

if (-not (Test-Path -LiteralPath $Repo -PathType Container)) {
    throw "仓库不存在：$Repo"
}

Set-Location -LiteralPath $Repo
$Root = [IO.Path]::GetFullPath(
    (& git rev-parse --show-toplevel 2>&1 | Out-String).Trim()
).TrimEnd([char[]]@('\', '/'))

$Python = Join-Path $Root ".venv\Scripts\python.exe"
if (-not (Test-Path -LiteralPath $Python -PathType Leaf)) {
    throw "虚拟环境 Python 不存在：$Python"
}

New-Item -ItemType Directory -Path $MonitorRoot -Force | Out-Null
$env:PYTHONUTF8 = "1"
$env:PYTHONIOENCODING = "utf-8"

function Read-TextSafe {
    param([string]$Path)

    if (
        [string]::IsNullOrWhiteSpace($Path) -or
        -not (Test-Path -LiteralPath $Path -PathType Leaf)
    ) {
        return ""
    }

    try {
        return Get-Content -LiteralPath $Path -Raw -Encoding UTF8
    }
    catch {
        return ""
    }
}

function Read-JsonSafe {
    param([string]$Path)

    $Text = Read-TextSafe $Path
    if ([string]::IsNullOrWhiteSpace($Text)) {
        return $null
    }

    try {
        return $Text | ConvertFrom-Json
    }
    catch {
        return $null
    }
}

function ConvertFrom-EmbeddedJson {
    param([string]$Text)

    if ([string]::IsNullOrWhiteSpace($Text)) {
        return $null
    }

    $Start = $Text.IndexOf("{")
    $End = $Text.LastIndexOf("}")

    if ($Start -lt 0 -or $End -le $Start) {
        return $null
    }

    try {
        return $Text.Substring($Start, $End - $Start + 1) |
            ConvertFrom-Json
    }
    catch {
        return $null
    }
}

function Invoke-Status {
    $OutFile = Join-Path $MonitorRoot "status.stdout.log"
    $ErrFile = Join-Path $MonitorRoot "status.stderr.log"

    Remove-Item $OutFile, $ErrFile -Force -ErrorAction SilentlyContinue

    $Process = Start-Process `
        -FilePath $Python `
        -ArgumentList @(
            "-m", "policydb.autopilot_cli",
            "full-sync", "status",
            "--scope", "all"
        ) `
        -WorkingDirectory $Root `
        -RedirectStandardOutput $OutFile `
        -RedirectStandardError $ErrFile `
        -NoNewWindow `
        -Wait `
        -PassThru

    $Stdout = Read-TextSafe $OutFile
    $Stderr = Read-TextSafe $ErrFile

    return [PSCustomObject]@{
        ExitCode = $Process.ExitCode
        Stdout = $Stdout
        Stderr = $Stderr
        Json = ConvertFrom-EmbeddedJson $Stdout
    }
}

function Get-LatestNamedFile {
    param([string[]]$Names)

    $Files = @()

    foreach ($Base in @($ContinuousRoot, $FullSyncRoot)) {
        if (-not (Test-Path -LiteralPath $Base -PathType Container)) {
            continue
        }

        foreach ($Name in $Names) {
            $Files += @(
                Get-ChildItem `
                    -LiteralPath $Base `
                    -Recurse `
                    -File `
                    -Filter $Name `
                    -ErrorAction SilentlyContinue
            )
        }
    }

    return $Files |
        Sort-Object LastWriteTimeUtc -Descending |
        Select-Object -First 1
}

function Find-Value {
    param(
        [AllowNull()]$Object,
        [string[]]$Names
    )

    if ($null -eq $Object) {
        return $null
    }

    if ($Object -is [PSCustomObject]) {
        foreach ($Name in $Names) {
            $Property = $Object.PSObject.Properties |
                Where-Object { $_.Name -ieq $Name } |
                Select-Object -First 1

            if ($null -ne $Property) {
                return $Property.Value
            }
        }

        foreach ($Property in $Object.PSObject.Properties) {
            $Found = Find-Value $Property.Value $Names
            if ($null -ne $Found) {
                return $Found
            }
        }

        return $null
    }

    if ($Object -is [System.Collections.IDictionary]) {
        foreach ($Name in $Names) {
            foreach ($Key in $Object.Keys) {
                if ([string]$Key -ieq $Name) {
                    return $Object[$Key]
                }
            }
        }

        foreach ($Key in $Object.Keys) {
            $Found = Find-Value $Object[$Key] $Names
            if ($null -ne $Found) {
                return $Found
            }
        }

        return $null
    }

    if (
        $Object -is [System.Collections.IEnumerable] -and
        -not ($Object -is [string])
    ) {
        foreach ($Item in $Object) {
            $Found = Find-Value $Item $Names
            if ($null -ne $Found) {
                return $Found
            }
        }
    }

    return $null
}

function First-Value {
    param(
        [object[]]$Objects,
        [string[]]$Names
    )

    foreach ($Object in $Objects) {
        $Found = Find-Value $Object $Names
        if ($null -ne $Found) {
            return $Found
        }
    }

    return $null
}

function To-Number {
    param([AllowNull()]$Value)

    if ($null -eq $Value) {
        return $null
    }

    $Number = 0.0
    if (
        [double]::TryParse(
            [string]$Value,
            [Globalization.NumberStyles]::Any,
            [Globalization.CultureInfo]::InvariantCulture,
            [ref]$Number
        )
    ) {
        return $Number
    }

    return $null
}

function To-Ratio {
    param([AllowNull()]$Value)

    $Number = To-Number $Value
    if ($null -eq $Number) {
        return $null
    }

    if ($Number -gt 1 -and $Number -le 100) {
        return $Number / 100
    }

    return [Math]::Max(0, [Math]::Min(1, $Number))
}

function Fmt-Number {
    param([AllowNull()]$Value)

    $Number = To-Number $Value
    if ($null -eq $Number) {
        return "N/A"
    }

    return $Number.ToString(
        "N0",
        [Globalization.CultureInfo]::InvariantCulture
    )
}

function Fmt-Percent {
    param([AllowNull()]$Value)

    $Ratio = To-Ratio $Value
    if ($null -eq $Ratio) {
        return "N/A"
    }

    return ("{0:P1}" -f $Ratio)
}

function Progress-Bar {
    param([AllowNull()]$Value)

    $Width = 25
    $Ratio = To-Ratio $Value

    if ($null -eq $Ratio) {
        return "[" + ("?" * $Width) + "]"
    }

    $Filled = [Math]::Round($Ratio * $Width)
    return "[" + ("#" * $Filled) + ("-" * ($Width - $Filled)) + "]"
}

function Show-Metric {
    param(
        [string]$Name,
        [AllowNull()]$Value,
        [ValidateSet("number", "ratio", "text")]
        [string]$Type = "number"
    )

    if ($Type -eq "ratio") {
        $Display = Fmt-Percent $Value
    }
    elseif ($Type -eq "text") {
        if ($null -eq $Value -or [string]::IsNullOrWhiteSpace([string]$Value)) {
            $Display = "N/A"
        }
        else {
            $Display = [string]$Value
        }
    }
    else {
        $Display = Fmt-Number $Value
    }

    Write-Host ("{0,-28} {1,18}" -f $Name, $Display)
}

function Get-ProcessState {
    $Lock = Read-JsonSafe $LockFile
    $PidValue = $null
    $Alive = $false

    if ($null -ne $Lock) {
        $PidValue = Find-Value $Lock @("pid")

        if ($null -ne $PidValue) {
            $Process = Get-Process `
                -Id ([int]$PidValue) `
                -ErrorAction SilentlyContinue

            if ($null -ne $Process -and -not $Process.HasExited) {
                $Alive = $true
            }
        }
    }

    return [PSCustomObject]@{
        LockExists = Test-Path -LiteralPath $LockFile
        Pid = $PidValue
        Alive = $Alive
        StopRequested = Test-Path -LiteralPath $StopFile
    }
}

function Get-Health {
    param(
        [AllowNull()]$SourceCoverage,
        [AllowNull()]$HistoricalCoverage,
        [AllowNull()]$TerminalRatio,
        [AllowNull()]$FieldCompleteness,
        [AllowNull()]$Freshness,
        [AllowNull()]$OpenGaps,
        [AllowNull()]$CriticalGaps,
        [AllowNull()]$Conflicts,
        [AllowNull()]$Duplicates
    )

    if (
        ((To-Number $CriticalGaps) -gt 0) -or
        ((To-Number $Conflicts) -gt 0) -or
        ((To-Number $Duplicates) -gt 0)
    ) {
        return [PSCustomObject]@{
            Status = "CRITICAL"
            Score = 0
        }
    }

    $Values = @()

    foreach ($Value in @(
        $SourceCoverage,
        $HistoricalCoverage,
        $TerminalRatio,
        $FieldCompleteness,
        $Freshness
    )) {
        $Ratio = To-Ratio $Value
        if ($null -ne $Ratio) {
            $Values += $Ratio
        }
    }

    if ($Values.Count -eq 0) {
        return [PSCustomObject]@{
            Status = "UNKNOWN"
            Score = $null
        }
    }

    $Score = ($Values | Measure-Object -Average).Average * 100
    $GapCount = To-Number $OpenGaps

    if ($null -ne $GapCount) {
        $Score -= [Math]::Min(20, $GapCount * 0.1)
    }

    $Score = [Math]::Max(0, [Math]::Round($Score, 1))

    if ($Score -ge 85) {
        $Status = "HEALTHY"
    }
    elseif ($Score -ge 60) {
        $Status = "DEGRADED"
    }
    else {
        $Status = "CRITICAL"
    }

    return [PSCustomObject]@{
        Status = $Status
        Score = $Score
    }
}

function Show-Dashboard {
    Clear-Host

    $Status = Invoke-Status
    $ProcessState = Get-ProcessState

    $AutomationFile = Get-LatestNamedFile @("AUTOMATION_STATE.json")
    $SyncFile = Get-LatestNamedFile @(
        "database_sync_status.json",
        "sync_status.json"
    )
    $AuditFile = Get-LatestNamedFile @(
        "final_engineering_audit.json",
        "run_summary.json",
        "sync_run_summary.json"
    )
    $GapFile = Get-LatestNamedFile @(
        "gap_summary.json",
        "coverage_gap_summary.json"
    )

    $Objects = @($Status.Json)

    foreach ($File in @(
        $AutomationFile,
        $SyncFile,
        $AuditFile,
        $GapFile
    )) {
        if ($null -ne $File) {
            $Json = Read-JsonSafe $File.FullName
            if ($null -ne $Json) {
                $Objects += $Json
            }
        }
    }

    $Total = First-Value $Objects @(
        "total_slots",
        "required_slots",
        "slot_count"
    )
    if ($null -eq $Total) {
        $Total = $ExpectedSlots
    }

    $Resolved = First-Value $Objects @("resolved_slots", "slots_resolved")
    $Verified = First-Value $Objects @("verified_slots", "slots_verified")
    $Enabled = First-Value $Objects @(
        "enabled_slots",
        "strict_enabled_slots",
        "slots_enabled"
    )
    $Backfilled = First-Value $Objects @(
        "backfilled_slots",
        "slots_backfilled"
    )
    $Current = First-Value $Objects @("current_slots", "slots_current")
    $Unresolved = First-Value $Objects @(
        "unresolved_slots",
        "slots_unresolved"
    )

    $Documents = First-Value $Objects @(
        "total_documents",
        "documents_total",
        "document_count"
    )
    $Added = First-Value $Objects @(
        "documents_added",
        "added_documents",
        "inserted_documents"
    )
    $Updated = First-Value $Objects @(
        "documents_updated",
        "updated_documents"
    )
    $FailedDocuments = First-Value $Objects @(
        "documents_failed",
        "failed_documents"
    )

    $SourceCoverage = First-Value $Objects @(
        "source_coverage_ratio",
        "source_coverage"
    )
    $HistoricalCoverage = First-Value $Objects @(
        "historical_coverage_ratio",
        "historical_coverage",
        "backfill_ratio"
    )
    $TerminalRatio = First-Value $Objects @(
        "article_terminal_ratio",
        "terminal_ratio"
    )
    $FieldCompleteness = First-Value $Objects @(
        "field_completeness_ratio",
        "field_completeness"
    )
    $Freshness = First-Value $Objects @("freshness_ratio", "freshness")

    $OpenGaps = First-Value $Objects @("open_gaps", "open_gap_count")
    $CriticalGaps = First-Value $Objects @(
        "critical_gaps",
        "critical_gap_count"
    )
    $FailedSources = First-Value $Objects @(
        "failed_sources",
        "source_failures"
    )
    $Conflicts = First-Value $Objects @(
        "checkpoint_conflicts",
        "checkpoint_conflict_count"
    )
    $Duplicates = First-Value $Objects @(
        "duplicate_inserts",
        "duplicate_insert_count"
    )

    $Cycle = First-Value $Objects @("cycle", "current_cycle")
    $RunId = First-Value $Objects @("run_id", "current_run_id")
    $GlobalStatus = First-Value $Objects @("global_status", "status")

    $TotalNumber = To-Number $Total
    $VerifiedRatio = $null
    $EnabledRatio = $null
    $BackfillRatio = $null
    $CurrentRatio = $null

    if ($null -ne $TotalNumber -and $TotalNumber -gt 0) {
        $Number = To-Number $Verified
        if ($null -ne $Number) {
            $VerifiedRatio = $Number / $TotalNumber
        }

        $Number = To-Number $Enabled
        if ($null -ne $Number) {
            $EnabledRatio = $Number / $TotalNumber
        }

        $Number = To-Number $Backfilled
        if ($null -ne $Number) {
            $BackfillRatio = $Number / $TotalNumber
        }

        $Number = To-Number $Current
        if ($null -ne $Number) {
            $CurrentRatio = $Number / $TotalNumber
        }
    }

    $Health = Get-Health `
        $SourceCoverage `
        $HistoricalCoverage `
        $TerminalRatio `
        $FieldCompleteness `
        $Freshness `
        $OpenGaps `
        $CriticalGaps `
        $Conflicts `
        $Duplicates

    if ($Health.Status -eq "HEALTHY") {
        $Color = "Green"
    }
    elseif ($Health.Status -eq "DEGRADED") {
        $Color = "Yellow"
    }
    elseif ($Health.Status -eq "CRITICAL") {
        $Color = "Red"
    }
    else {
        $Color = "Gray"
    }

    Write-Host "========================================================" -ForegroundColor Cyan
    Write-Host "CRPD 全量抓取进度与成果健康度" -ForegroundColor Cyan
    Write-Host "========================================================" -ForegroundColor Cyan
    Write-Host "时间：$((Get-Date).ToString('yyyy-MM-dd HH:mm:ss'))"
    Write-Host "健康：$($Health.Status)  分数：$($Health.Score)" -ForegroundColor $Color
    Write-Host ""

    Write-Host "【运行】" -ForegroundColor Cyan
    Show-Metric "自动化进程存活" $ProcessState.Alive text
    Show-Metric "PID" $ProcessState.Pid
    Show-Metric "停止文件存在" $ProcessState.StopRequested text
    Show-Metric "cycle" $Cycle
    Show-Metric "run_id" $RunId text
    Show-Metric "global_status" $GlobalStatus text
    Show-Metric "status 退出码" $Status.ExitCode
    Write-Host ""

    Write-Host "【525 槽位】" -ForegroundColor Cyan
    Show-Metric "总槽位" $Total
    Show-Metric "已解析" $Resolved
    Show-Metric "已验证" $Verified
    Show-Metric "严格启用" $Enabled
    Show-Metric "已回溯" $Backfilled
    Show-Metric "CURRENT" $Current
    Show-Metric "未解析" $Unresolved

    Write-Host ("验证    {0} {1}" -f (Progress-Bar $VerifiedRatio), (Fmt-Percent $VerifiedRatio))
    Write-Host ("启用    {0} {1}" -f (Progress-Bar $EnabledRatio), (Fmt-Percent $EnabledRatio))
    Write-Host ("回溯    {0} {1}" -f (Progress-Bar $BackfillRatio), (Fmt-Percent $BackfillRatio))
    Write-Host ("CURRENT {0} {1}" -f (Progress-Bar $CurrentRatio), (Fmt-Percent $CurrentRatio))
    Write-Host ""

    Write-Host "【成果】" -ForegroundColor Cyan
    Show-Metric "累计政策文档" $Documents
    Show-Metric "新增文档" $Added
    Show-Metric "更新版本" $Updated
    Show-Metric "失败文档" $FailedDocuments
    Write-Host ""

    Write-Host "【质量】" -ForegroundColor Cyan
    Show-Metric "来源覆盖度" $SourceCoverage ratio
    Show-Metric "历史覆盖度" $HistoricalCoverage ratio
    Show-Metric "文章终态率" $TerminalRatio ratio
    Show-Metric "字段完整率" $FieldCompleteness ratio
    Show-Metric "新鲜度" $Freshness ratio
    Write-Host ""

    Write-Host "【风险】" -ForegroundColor Cyan
    Show-Metric "开放 gaps" $OpenGaps
    Show-Metric "critical gaps" $CriticalGaps
    Show-Metric "失败来源" $FailedSources
    Show-Metric "checkpoint 冲突" $Conflicts
    Show-Metric "重复插入" $Duplicates

    if (
        -not [string]::IsNullOrWhiteSpace($Status.Stderr) -and
        $Status.ExitCode -notin @(0, 10)
    ) {
        Write-Host ""
        Write-Host "status stderr：" -ForegroundColor Red
        Write-Host $Status.Stderr.Trim()
    }

    $Snapshot = [ordered]@{
        generated_at = (Get-Date).ToUniversalTime().ToString("o")
        run = [ordered]@{
            process_alive = $ProcessState.Alive
            pid = $ProcessState.Pid
            cycle = $Cycle
            run_id = $RunId
            global_status = $GlobalStatus
            status_exit_code = $Status.ExitCode
        }
        slots = [ordered]@{
            total = $Total
            resolved = $Resolved
            verified = $Verified
            enabled = $Enabled
            backfilled = $Backfilled
            current = $Current
            unresolved = $Unresolved
            verified_ratio = $VerifiedRatio
            enabled_ratio = $EnabledRatio
            backfill_ratio = $BackfillRatio
            current_ratio = $CurrentRatio
        }
        documents = [ordered]@{
            total = $Documents
            added = $Added
            updated = $Updated
            failed = $FailedDocuments
        }
        quality = [ordered]@{
            source_coverage = $SourceCoverage
            historical_coverage = $HistoricalCoverage
            terminal_ratio = $TerminalRatio
            field_completeness = $FieldCompleteness
            freshness = $Freshness
        }
        risks = [ordered]@{
            open_gaps = $OpenGaps
            critical_gaps = $CriticalGaps
            failed_sources = $FailedSources
            checkpoint_conflicts = $Conflicts
            duplicate_inserts = $Duplicates
        }
        health = [ordered]@{
            status = $Health.Status
            score = $Health.Score
        }
    }

    $Snapshot |
        ConvertTo-Json -Depth 15 |
        Set-Content `
            -LiteralPath (
                Join-Path $MonitorRoot "CURRENT_HEALTH_SNAPSHOT.json"
            ) `
            -Encoding UTF8

    if ($Watch) {
        Write-Host ""
        Write-Host "$IntervalSeconds 秒后刷新；按 Ctrl+C 退出。" -ForegroundColor DarkGray
    }
}

do {
    try {
        Show-Dashboard
    }
    catch {
        Write-Host "监控失败：$($_.Exception.Message)" -ForegroundColor Red
    }

    if ($Watch) {
        Start-Sleep -Seconds $IntervalSeconds
    }
}
while ($Watch)