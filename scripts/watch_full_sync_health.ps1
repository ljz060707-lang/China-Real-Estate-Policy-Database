param(
    [switch]$Watch,

    [ValidateRange(5, 3600)]
    [int]$IntervalSeconds = 30,

    [ValidateRange(1, 1000)]
    [int]$MaxErrorLines = 30,

    [switch]$GenerateReport
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

# ============================================================
# CRPD 全量抓取进度与健康度只读监控
#
# 默认：单次检查
# -Watch：持续刷新
# -GenerateReport：每轮额外调用 full-sync report
#
# 不启动抓取
# 不修改数据库
# 不调用 AI
# ============================================================

$Repo = "E:\policy-database"
$DataRoot = "E:\Data Set\CRPD"

$DateFrom = "2018-01-01"
$ExpectedSlots = 525

$ContinuousRoot = Join-Path $DataRoot "outputs\continuous_full_sync"
$FullSyncRoot = Join-Path $DataRoot "outputs\full_sync"
$ControlDir = Join-Path $DataRoot "control"
$LockDir = Join-Path $DataRoot "locks"

$StopFile = Join-Path $ControlDir "STOP_FULL_SYNC"
$LockFile = Join-Path $LockDir "all_cities_since_2018.lock"

$MonitorRoot = Join-Path $DataRoot "outputs\monitoring"

# ============================================================
# 初始化
# ============================================================

if (-not (Test-Path -LiteralPath $Repo -PathType Container)) {
    throw "仓库目录不存在：$Repo"
}

Set-Location -LiteralPath $Repo

$RootRaw = (
    git rev-parse --show-toplevel 2>&1 |
    Out-String
).Trim()

if ($LASTEXITCODE -ne 0) {
    throw "当前目录不是有效 Git 仓库：$Repo"
}

$Root = [IO.Path]::GetFullPath(
    $RootRaw
).TrimEnd([char[]]@('\', '/'))

$Python = Join-Path $Root ".venv\Scripts\python.exe"

if (-not (Test-Path -LiteralPath $Python -PathType Leaf)) {
    throw "虚拟环境 Python 不存在：$Python"
}

New-Item `
    -ItemType Directory `
    -Path $MonitorRoot `
    -Force |
    Out-Null

$env:PYTHONUTF8 = "1"
$env:PYTHONIOENCODING = "utf-8"

# ============================================================
# 通用函数
# ============================================================

function ConvertFrom-EmbeddedJson {
    param(
        [AllowEmptyString()]
        [string]$Text
    )

    if ([string]::IsNullOrWhiteSpace($Text)) {
        return $null
    }

    $Start = $Text.IndexOf("{")
    $End = $Text.LastIndexOf("}")

    if ($Start -lt 0 -or $End -le $Start) {
        return $null
    }

    try {
        return $Text.Substring(
            $Start,
            $End - $Start + 1
        ) | ConvertFrom-Json
    }
    catch {
        return $null
    }
}

function Read-JsonSafely {
    param(
        [AllowNull()]
        [string]$Path
    )

    if (
        [string]::IsNullOrWhiteSpace($Path) -or
        -not (Test-Path -LiteralPath $Path -PathType Leaf)
    ) {
        return $null
    }

    try {
        return Get-Content `
            -LiteralPath $Path `
            -Raw `
            -Encoding utf8 |
            ConvertFrom-Json
    }
    catch {
        return $null
    }
}

function Invoke-StatusCommand {
    $StdoutPath = Join-Path $MonitorRoot "status_current.stdout.log"
    $StderrPath = Join-Path $MonitorRoot "status_current.stderr.log"

    Remove-Item `
        -LiteralPath $StdoutPath, $StderrPath `
        -Force `
        -ErrorAction SilentlyContinue

    $Process = Start-Process `
        -FilePath $Python `
        -ArgumentList @(
            "-m",
            "policydb.autopilot_cli",
            "full-sync",
            "status",
            "--scope",
            "all"
        ) `
        -WorkingDirectory $Root `
        -RedirectStandardOutput $StdoutPath `
        -RedirectStandardError $StderrPath `
        -NoNewWindow `
        -Wait `
        -PassThru

    $Stdout = if (Test-Path -LiteralPath $StdoutPath) {
        Get-Content -LiteralPath $StdoutPath -Raw -Encoding utf8
    }
    else {
        ""
    }

    $Stderr = if (Test-Path -LiteralPath $StderrPath) {
        Get-Content -LiteralPath $StderrPath -Raw -Encoding utf8
    }
    else {
        ""
    }

    return [PSCustomObject]@{
        ExitCode = $Process.ExitCode
        Stdout   = $Stdout
        Stderr   = $Stderr
        Json     = ConvertFrom-EmbeddedJson -Text $Stdout
    }
}

function Invoke-ReportCommand {
    $DateTo = (Get-Date).ToString("yyyy-MM-dd")

    $StdoutPath = Join-Path $MonitorRoot "report_current.stdout.log"
    $StderrPath = Join-Path $MonitorRoot "report_current.stderr.log"

    Remove-Item `
        -LiteralPath $StdoutPath, $StderrPath `
        -Force `
        -ErrorAction SilentlyContinue

    $Process = Start-Process `
        -FilePath $Python `
        -ArgumentList @(
            "-m",
            "policydb.autopilot_cli",
            "full-sync",
            "report",
            "--scope",
            "all",
            "--date-from",
            $DateFrom,
            "--date-to",
            $DateTo,
            "--format",
            "json"
        ) `
        -WorkingDirectory $Root `
        -RedirectStandardOutput $StdoutPath `
        -RedirectStandardError $StderrPath `
        -NoNewWindow `
        -Wait `
        -PassThru

    return [PSCustomObject]@{
        ExitCode = $Process.ExitCode
        Stdout   = if (Test-Path $StdoutPath) {
            Get-Content $StdoutPath -Raw -Encoding utf8
        }
        else {
            ""
        }
        Stderr   = if (Test-Path $StderrPath) {
            Get-Content $StderrPath -Raw -Encoding utf8
        }
        else {
            ""
        }
    }
}

function Get-LatestFile {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$Roots,

        [Parameter(Mandatory = $true)]
        [string[]]$Names
    )

    $Candidates = @()

    foreach ($SearchRoot in $Roots) {
        if (-not (Test-Path -LiteralPath $SearchRoot)) {
            continue
        }

        foreach ($Name in $Names) {
            $Candidates += @(
                Get-ChildItem `
                    -LiteralPath $SearchRoot `
                    -Recurse `
                    -File `
                    -Filter $Name `
                    -ErrorAction SilentlyContinue
            )
        }
    }

    return $Candidates |
        Sort-Object LastWriteTimeUtc -Descending |
        Select-Object -First 1
}

function Get-LatestRunDirectory {
    $Roots = @(
        $ContinuousRoot,
        $FullSyncRoot
    )

    $Directories = @()

    foreach ($SearchRoot in $Roots) {
        if (Test-Path -LiteralPath $SearchRoot) {
            $Directories += @(
                Get-ChildItem `
                    -LiteralPath $SearchRoot `
                    -Directory `
                    -Recurse `
                    -ErrorAction SilentlyContinue
            )
        }
    }

    return $Directories |
        Where-Object {
            Get-ChildItem `
                -LiteralPath $_.FullName `
                -File `
                -ErrorAction SilentlyContinue |
            Select-Object -First 1
        } |
        Sort-Object LastWriteTimeUtc -Descending |
        Select-Object -First 1
}

function Find-ValueRecursive {
    param(
        [AllowNull()]
        $Object,

        [Parameter(Mandatory = $true)]
        [string[]]$CandidateNames
    )

    if ($null -eq $Object) {
        return $null
    }

    if ($Object -is [System.Collections.IDictionary]) {
        foreach ($Name in $CandidateNames) {
            foreach ($Key in $Object.Keys) {
                if (
                    [string]$Key -ieq $Name -and
                    $null -ne $Object[$Key]
                ) {
                    return $Object[$Key]
                }
            }
        }

        foreach ($Key in $Object.Keys) {
            $Nested = Find-ValueRecursive `
                -Object $Object[$Key] `
                -CandidateNames $CandidateNames

            if ($null -ne $Nested) {
                return $Nested
            }
        }

        return $null
    }

    if ($Object -is [pscustomobject]) {
        foreach ($Name in $CandidateNames) {
            $Property = $Object.PSObject.Properties |
                Where-Object {
                    $_.Name -ieq $Name
                } |
                Select-Object -First 1

            if ($null -ne $Property -and $null -ne $Property.Value) {
                return $Property.Value
            }
        }

        foreach ($Property in $Object.PSObject.Properties) {
            $Nested = Find-ValueRecursive `
                -Object $Property.Value `
                -CandidateNames $CandidateNames

            if ($null -ne $Nested) {
                return $Nested
            }
        }

        return $null
    }

    if (
        $Object -is [System.Collections.IEnumerable] -and
        -not ($Object -is [string])
    ) {
        foreach ($Item in $Object) {
            $Nested = Find-ValueRecursive `
                -Object $Item `
                -CandidateNames $CandidateNames

            if ($null -ne $Nested) {
                return $Nested
            }
        }
    }

    return $null
}

function Convert-ToNumber {
    param(
        [AllowNull()]
        $Value
    )

    if ($null -eq $Value) {
        return $null
    }

    $Result = 0.0

    if (
        [double]::TryParse(
            [string]$Value,
            [Globalization.NumberStyles]::Any,
            [Globalization.CultureInfo]::InvariantCulture,
            [ref]$Result
        )
    ) {
        return $Result
    }

    return $null
}

function Convert-ToRatio {
    param(
        [AllowNull()]
        $Value
    )

    $Number = Convert-ToNumber -Value $Value

    if ($null -eq $Number) {
        return $null
    }

    if ($Number -gt 1.0 -and $Number -le 100.0) {
        return $Number / 100.0
    }

    if ($Number -lt 0) {
        return 0.0
    }

    if ($Number -gt 1.0) {
        return 1.0
    }

    return $Number
}

function Format-Number {
    param(
        [AllowNull()]
        $Value,

        [int]$Decimals = 0
    )

    $Number = Convert-ToNumber -Value $Value

    if ($null -eq $Number) {
        return "N/A"
    }

    return $Number.ToString(
        "N$Decimals",
        [Globalization.CultureInfo]::InvariantCulture
    )
}

function Format-Percent {
    param(
        [AllowNull()]
        $Value
    )

    $Ratio = Convert-ToRatio -Value $Value

    if ($null -eq $Ratio) {
        return "N/A"
    }

    return ("{0:P1}" -f $Ratio)
}

function New-ProgressBar {
    param(
        [AllowNull()]
        $Value,

        [int]$Width = 30
    )

    $Ratio = Convert-ToRatio -Value $Value

    if ($null -eq $Ratio) {
        return ("[" + ("?" * $Width) + "]")
    }

    $Filled = [Math]::Round($Ratio * $Width)
    $Filled = [Math]::Max(0, [Math]::Min($Width, $Filled))

    $Empty = $Width - $Filled

    return (
        "[" +
        ("#" * $Filled) +
        ("-" * $Empty) +
        "]"
    )
}

function Get-ProcessState {
    $Lock = Read-JsonSafely -Path $LockFile
    $ProcessAlive = $false
    $LockPid = $null
    $ProcessName = $null
    $StartedAt = $null

    if ($null -ne $Lock) {
        $LockPid = Find-ValueRecursive `
            -Object $Lock `
            -CandidateNames @("pid")

        $StartedAt = Find-ValueRecursive `
            -Object $Lock `
            -CandidateNames @(
                "started_at",
                "process_start_time"
            )

        if ($null -ne $LockPid) {
            $Process = Get-Process `
                -Id ([int]$LockPid) `
                -ErrorAction SilentlyContinue

            if ($null -ne $Process -and -not $Process.HasExited) {
                $ProcessAlive = $true
                $ProcessName = $Process.ProcessName
            }
        }
    }

    return [PSCustomObject]@{
        LockExists   = Test-Path -LiteralPath $LockFile
        Pid          = $LockPid
        ProcessAlive = $ProcessAlive
        ProcessName  = $ProcessName
        StartedAt    = $StartedAt
        StopRequested = Test-Path -LiteralPath $StopFile
    }
}

function Get-RecentErrors {
    param(
        [AllowNull()]
        [string]$LatestRunPath,

        [int]$Limit = 30
    )

    $Roots = @(
        $LatestRunPath,
        $ContinuousRoot,
        $FullSyncRoot
    ) | Where-Object {
        -not [string]::IsNullOrWhiteSpace($_) -and
        (Test-Path -LiteralPath $_)
    }

    $Patterns = @(
        "ERROR",
        "FAILED",
        "TlsError",
        "SSLError",
        "PermissionError",
        "WinError",
        "checkpoint_conflict",
        "duplicate_inserts",
        "Traceback",
        "FAILED_RECOVERABLE",
        "FAILED_TERMINAL"
    )

    $LogFiles = @()

    foreach ($SearchRoot in $Roots) {
        $LogFiles += @(
            Get-ChildItem `
                -LiteralPath $SearchRoot `
                -Recurse `
                -File `
                -ErrorAction SilentlyContinue |
            Where-Object {
                $_.Extension -in @(
                    ".log",
                    ".json",
                    ".jsonl",
                    ".txt"
                )
            }
        )
    }

    $LatestLogs = $LogFiles |
        Sort-Object LastWriteTimeUtc -Descending |
        Select-Object -First 40

    $Hits = @()

    foreach ($File in $LatestLogs) {
        try {
            $Matches = Select-String `
                -LiteralPath $File.FullName `
                -Pattern $Patterns `
                -SimpleMatch `
                -ErrorAction SilentlyContinue

            foreach ($Match in $Matches) {
                $Line = $Match.Line.Trim()

                if ($Line.Length -gt 300) {
                    $Line = $Line.Substring(0, 300) + "..."
                }

                $Hits += [PSCustomObject]@{
                    Time = $File.LastWriteTime
                    File = $File.FullName
                    Line = $Line
                }
            }
        }
        catch {
            continue
        }
    }

    return $Hits |
        Sort-Object Time -Descending |
        Select-Object -First $Limit
}

function Get-HealthStatus {
    param(
        [AllowNull()]
        $SourceCoverage,

        [AllowNull()]
        $HistoricalCoverage,

        [AllowNull()]
        $ArticleTerminal,

        [AllowNull()]
        $FieldCompleteness,

        [AllowNull()]
        $Freshness,

        [AllowNull()]
        $OpenGaps,

        [AllowNull()]
        $CriticalGaps,

        [AllowNull()]
        $FailedSources,

        [AllowNull()]
        $CheckpointConflicts,

        [AllowNull()]
        $DuplicateInserts
    )

    $CriticalGapNumber = Convert-ToNumber $CriticalGaps
    $ConflictNumber = Convert-ToNumber $CheckpointConflicts
    $DuplicateNumber = Convert-ToNumber $DuplicateInserts

    if (
        ($null -ne $CriticalGapNumber -and $CriticalGapNumber -gt 0) -or
        ($null -ne $ConflictNumber -and $ConflictNumber -gt 0) -or
        ($null -ne $DuplicateNumber -and $DuplicateNumber -gt 0)
    ) {
        return [PSCustomObject]@{
            Status = "CRITICAL"
            Score  = 0
            Reason = "存在 critical gap、checkpoint 冲突或重复插入"
        }
    }

    $Components = @()

    foreach ($Value in @(
        $SourceCoverage,
        $HistoricalCoverage,
        $ArticleTerminal,
        $FieldCompleteness,
        $Freshness
    )) {
        $Ratio = Convert-ToRatio $Value

        if ($null -ne $Ratio) {
            $Components += $Ratio
        }
    }

    if ($Components.Count -eq 0) {
        return [PSCustomObject]@{
            Status = "UNKNOWN"
            Score  = $null
            Reason = "缺少足够的健康度指标"
        }
    }

    $BaseScore = (
        ($Components | Measure-Object -Average).Average
    ) * 100

    $OpenGapNumber = Convert-ToNumber $OpenGaps
    $FailedNumber = Convert-ToNumber $FailedSources

    $Penalty = 0.0

    if ($null -ne $OpenGapNumber) {
        $Penalty += [Math]::Min(20, $OpenGapNumber * 0.10)
    }

    if ($null -ne $FailedNumber) {
        $Penalty += [Math]::Min(25, $FailedNumber * 2.0)
    }

    $Score = [Math]::Max(
        0,
        [Math]::Round($BaseScore - $Penalty, 1)
    )

    $Status = if ($Score -ge 85) {
        "HEALTHY"
    }
    elseif ($Score -ge 60) {
        "DEGRADED"
    }
    else {
        "CRITICAL"
    }

    return [PSCustomObject]@{
        Status = $Status
        Score  = $Score
        Reason = "基于覆盖度、回溯、终态、字段完整性、新鲜度和缺口的运营评分"
    }
}

function Write-Metric {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Label,

        [AllowNull()]
        $Value,

        [ValidateSet("number", "ratio", "text")]
        [string]$Type = "number"
    )

    $Formatted = switch ($Type) {
        "ratio" {
            Format-Percent $Value
        }
        "text" {
            if ($null -eq $Value) {
                "N/A"
            }
            else {
                [string]$Value
            }
        }
        default {
            Format-Number $Value
        }
    }

    Write-Host (
        "{0,-34} {1,15}" -f $Label, $Formatted
    )
}

# ============================================================
# 单次监控
# ============================================================

function Show-CRPDHealth {
    Clear-Host

    $Now = Get-Date
    $ProcessState = Get-ProcessState
    $StatusCommand = Invoke-StatusCommand

    if ($GenerateReport) {
        $ReportResult = Invoke-ReportCommand

        if ($ReportResult.ExitCode -notin @(0, 10)) {
            Write-Warning (
                "full-sync report 返回退出码 " +
                $ReportResult.ExitCode
            )
        }
    }

    $LatestRunDirectory = Get-LatestRunDirectory

    $LatestRunPath = if ($null -ne $LatestRunDirectory) {
        $LatestRunDirectory.FullName
    }
    else {
        $null
    }

    $AutomationStateFile = Get-LatestFile `
        -Roots @($ContinuousRoot) `
        -Names @("AUTOMATION_STATE.json")

    $DatabaseStatusFile = Get-LatestFile `
        -Roots @($ContinuousRoot, $FullSyncRoot) `
        -Names @(
            "database_sync_status.json",
            "sync_status.json"
        )

    $RunSummaryFile = Get-LatestFile `
        -Roots @($ContinuousRoot, $FullSyncRoot) `
        -Names @(
            "run_summary.json",
            "sync_run_summary.json"
        )

    $GapSummaryFile = Get-LatestFile `
        -Roots @($ContinuousRoot, $FullSyncRoot) `
        -Names @(
            "gap_summary.json",
            "coverage_gap_summary.json"
        )

    $CompletenessFile = Get-LatestFile `
        -Roots @($ContinuousRoot, $FullSyncRoot) `
        -Names @(
            "completeness.json",
            "all_city_completeness.json",
            "beijing_one_year_completeness.json"
        )

    $SourceHealthFile = Get-LatestFile `
        -Roots @($ContinuousRoot, $FullSyncRoot) `
        -Names @(
            "source_health.json",
            "source_health_summary.json"
        )

    $AutomationState = if ($null -ne $AutomationStateFile) {
        Read-JsonSafely $AutomationStateFile.FullName
    }
    else {
        $null
    }

    $DatabaseStatus = if ($null -ne $DatabaseStatusFile) {
        Read-JsonSafely $DatabaseStatusFile.FullName
    }
    else {
        $null
    }

    $RunSummary = if ($null -ne $RunSummaryFile) {
        Read-JsonSafely $RunSummaryFile.FullName
    }
    else {
        $null
    }

    $GapSummary = if ($null -ne $GapSummaryFile) {
        Read-JsonSafely $GapSummaryFile.FullName
    }
    else {
        $null
    }

    $Completeness = if ($null -ne $CompletenessFile) {
        Read-JsonSafely $CompletenessFile.FullName
    }
    else {
        $null
    }

    $SourceHealth = if ($null -ne $SourceHealthFile) {
        Read-JsonSafely $SourceHealthFile.FullName
    }
    else {
        $null
    }

    # 将多个来源组成一个搜索集合
    $Objects = @(
        $StatusCommand.Json,
        $DatabaseStatus,
        $RunSummary,
        $GapSummary,
        $Completeness,
        $SourceHealth,
        $AutomationState
    ) | Where-Object {
        $null -ne $_
    }

    function Find-InAll {
        param([string[]]$Names)

        foreach ($Object in $Objects) {
            $Value = Find-ValueRecursive `
                -Object $Object `
                -CandidateNames $Names

            if ($null -ne $Value) {
                return $Value
            }
        }

        return $null
    }

    # ========================================================
    # 槽位与来源
    # ========================================================

    $TotalSlots = Find-InAll @(
        "total_slots",
        "required_slots",
        "slot_count"
    )

    if ($null -eq $TotalSlots) {
        $TotalSlots = $ExpectedSlots
    }

    $ResolvedSlots = Find-InAll @(
        "resolved_slots",
        "slots_resolved"
    )

    $VerifiedSlots = Find-InAll @(
        "verified_slots",
        "slots_verified"
    )

    $EnabledSlots = Find-InAll @(
        "enabled_slots",
        "strict_enabled_slots",
        "slots_enabled"
    )

    $CrawlReadySlots = Find-InAll @(
        "crawl_ready_slots",
        "ready_slots"
    )

    $BackfilledSlots = Find-InAll @(
        "backfilled_slots",
        "slots_backfilled"
    )

    $CurrentSlots = Find-InAll @(
        "current_slots",
        "slots_current"
    )

    $UnresolvedSlots = Find-InAll @(
        "unresolved_slots",
        "slots_unresolved"
    )

    $HumanReviewSlots = Find-InAll @(
        "human_review_slots",
        "manual_review_slots"
    )

    # ========================================================
    # 文档和抓取
    # ========================================================

    $DocumentsTotal = Find-InAll @(
        "total_documents",
        "documents_total",
        "document_count"
    )

    $DocumentsAdded = Find-InAll @(
        "documents_added",
        "added_documents",
        "inserted_documents"
    )

    $DocumentsUpdated = Find-InAll @(
        "documents_updated",
        "updated_documents"
    )

    $DocumentsUnchanged = Find-InAll @(
        "documents_unchanged",
        "unchanged_documents"
    )

    $DocumentsFailed = Find-InAll @(
        "documents_failed",
        "failed_documents"
    )

    $DiscoveredLinks = Find-InAll @(
        "article_links_discovered",
        "discovered_article_links"
    )

    $TerminalLinks = Find-InAll @(
        "article_links_terminal",
        "terminal_article_links"
    )

    $AttachmentsDiscovered = Find-InAll @(
        "attachments_discovered"
    )

    $AttachmentsProcessed = Find-InAll @(
        "attachments_processed"
    )

    # ========================================================
    # 质量和完整度
    # ========================================================

    $SourceCoverage = Find-InAll @(
        "source_coverage_ratio",
        "source_coverage"
    )

    $HistoricalCoverage = Find-InAll @(
        "historical_coverage_ratio",
        "historical_coverage",
        "backfill_ratio"
    )

    $ArticleTerminalRatio = Find-InAll @(
        "article_terminal_ratio",
        "terminal_ratio"
    )

    $AttachmentCoverage = Find-InAll @(
        "attachment_coverage_ratio",
        "attachment_coverage"
    )

    $FieldCompleteness = Find-InAll @(
        "field_completeness_ratio",
        "field_completeness"
    )

    $FreshnessRatio = Find-InAll @(
        "freshness_ratio",
        "freshness"
    )

    $ParseSuccessRatio = Find-InAll @(
        "parse_success_ratio",
        "parse_success"
    )

    $OverallCompleteness = Find-InAll @(
        "overall_completeness",
        "overall_completeness_ratio"
    )

    # ========================================================
    # 缺口和异常
    # ========================================================

    $OpenGaps = Find-InAll @(
        "open_gaps",
        "open_gap_count"
    )

    $CriticalGaps = Find-InAll @(
        "critical_gaps",
        "critical_gap_count"
    )

    $RepairableGaps = Find-InAll @(
        "repairable_gaps"
    )

    $HumanReviewGaps = Find-InAll @(
        "human_review_gaps"
    )

    $FailedSources = Find-InAll @(
        "failed_sources",
        "source_failures"
    )

    $DegradedSources = Find-InAll @(
        "degraded_sources"
    )

    $StaleSources = Find-InAll @(
        "stale_sources"
    )

    $CheckpointConflicts = Find-InAll @(
        "checkpoint_conflicts",
        "checkpoint_conflict_count"
    )

    $DuplicateInserts = Find-InAll @(
        "duplicate_inserts",
        "duplicate_insert_count"
    )

    # ========================================================
    # 运行进度
    # ========================================================

    $Cycle = Find-InAll @(
        "cycle",
        "current_cycle"
    )

    $PlannedSlots = Find-InAll @(
        "planned_slots"
    )

    $PlannedSources = Find-InAll @(
        "planned_sources"
    )

    $RunId = Find-InAll @(
        "run_id",
        "current_run_id"
    )

    $GlobalStatus = Find-InAll @(
        "global_status",
        "status"
    )

    $LastCompletedRun = Find-InAll @(
        "last_completed_run"
    )

    $LastSuccessfulSync = Find-InAll @(
        "last_successful_full_sync"
    )

    # ========================================================
    # 计算派生进度
    # ========================================================

    $CurrentProgress = $null
    $BackfillProgress = $null
    $VerifiedProgress = $null
    $EnabledProgress = $null

    $TotalSlotNumber = Convert-ToNumber $TotalSlots

    if ($null -ne $TotalSlotNumber -and $TotalSlotNumber -gt 0) {
        $CurrentNumber = Convert-ToNumber $CurrentSlots
        $BackfilledNumber = Convert-ToNumber $BackfilledSlots
        $VerifiedNumber = Convert-ToNumber $VerifiedSlots
        $EnabledNumber = Convert-ToNumber $EnabledSlots

        if ($null -ne $CurrentNumber) {
            $CurrentProgress = $CurrentNumber / $TotalSlotNumber
        }

        if ($null -ne $BackfilledNumber) {
            $BackfillProgress = $BackfilledNumber / $TotalSlotNumber
        }

        if ($null -ne $VerifiedNumber) {
            $VerifiedProgress = $VerifiedNumber / $TotalSlotNumber
        }

        if ($null -ne $EnabledNumber) {
            $EnabledProgress = $EnabledNumber / $TotalSlotNumber
        }
    }

    if (
        $null -eq $ArticleTerminalRatio -and
        $null -ne (Convert-ToNumber $DiscoveredLinks) -and
        (Convert-ToNumber $DiscoveredLinks) -gt 0 -and
        $null -ne (Convert-ToNumber $TerminalLinks)
    ) {
        $ArticleTerminalRatio = (
            Convert-ToNumber $TerminalLinks
        ) / (
            Convert-ToNumber $DiscoveredLinks
        )
    }

    if (
        $null -eq $AttachmentCoverage -and
        $null -ne (Convert-ToNumber $AttachmentsDiscovered) -and
        (Convert-ToNumber $AttachmentsDiscovered) -gt 0 -and
        $null -ne (Convert-ToNumber $AttachmentsProcessed)
    ) {
        $AttachmentCoverage = (
            Convert-ToNumber $AttachmentsProcessed
        ) / (
            Convert-ToNumber $AttachmentsDiscovered
        )
    }

    $Health = Get-HealthStatus `
        -SourceCoverage $SourceCoverage `
        -HistoricalCoverage $HistoricalCoverage `
        -ArticleTerminal $ArticleTerminalRatio `
        -FieldCompleteness $FieldCompleteness `
        -Freshness $FreshnessRatio `
        -OpenGaps $OpenGaps `
        -CriticalGaps $CriticalGaps `
        -FailedSources $FailedSources `
        -CheckpointConflicts $CheckpointConflicts `
        -DuplicateInserts $DuplicateInserts

    # ========================================================
    # 控制台显示
    # ========================================================

    $HealthColor = switch ($Health.Status) {
        "HEALTHY"  { "Green" }
        "DEGRADED" { "Yellow" }
        "CRITICAL" { "Red" }
        default    { "Gray" }
    }

    Write-Host "============================================================" `
        -ForegroundColor Cyan
    Write-Host "CRPD 全量抓取进度与成果健康度" `
        -ForegroundColor Cyan
    Write-Host "============================================================" `
        -ForegroundColor Cyan

    Write-Host (
        "检查时间：{0}" -f $Now.ToString("yyyy-MM-dd HH:mm:ss")
    )

    Write-Host (
        "监控范围：全部城市，{0} 至当前" -f $DateFrom
    )

    Write-Host (
        "总体健康：{0}  分数：{1}" -f
        $Health.Status,
        $(if ($null -eq $Health.Score) {
            "N/A"
        }
        else {
            $Health.Score
        })
    ) -ForegroundColor $HealthColor

    Write-Host "说明：$($Health.Reason)"
    Write-Host ""

    Write-Host "【自动化进程】" -ForegroundColor Cyan

    Write-Metric `
        -Label "锁文件存在" `
        -Value $ProcessState.LockExists `
        -Type text

    Write-Metric `
        -Label "自动化进程存活" `
        -Value $ProcessState.ProcessAlive `
        -Type text

    Write-Metric `
        -Label "PID" `
        -Value $ProcessState.Pid `
        -Type number

    Write-Metric `
        -Label "安全停止已请求" `
        -Value $ProcessState.StopRequested `
        -Type text

    Write-Metric `
        -Label "当前 cycle" `
        -Value $Cycle `
        -Type number

    Write-Metric `
        -Label "当前 run_id" `
        -Value $RunId `
        -Type text

    Write-Metric `
        -Label "全局状态" `
        -Value $GlobalStatus `
        -Type text

    Write-Metric `
        -Label "status 命令退出码" `
        -Value $StatusCommand.ExitCode `
        -Type number

    Write-Host ""
    Write-Host "【槽位推进】" -ForegroundColor Cyan

    Write-Metric "总计划槽位" $TotalSlots
    Write-Metric "已解析槽位" $ResolvedSlots
    Write-Metric "已验证槽位" $VerifiedSlots
    Write-Metric "strict enabled 槽位" $EnabledSlots
    Write-Metric "crawl ready 槽位" $CrawlReadySlots
    Write-Metric "已历史回溯槽位" $BackfilledSlots
    Write-Metric "CURRENT 槽位" $CurrentSlots
    Write-Metric "未解析槽位" $UnresolvedSlots
    Write-Metric "人工审核槽位" $HumanReviewSlots

    Write-Host ""
    Write-Host (
        "验证进度   {0} {1}" -f
        (New-ProgressBar $VerifiedProgress),
        (Format-Percent $VerifiedProgress)
    )

    Write-Host (
        "启用进度   {0} {1}" -f
        (New-ProgressBar $EnabledProgress),
        (Format-Percent $EnabledProgress)
    )

    Write-Host (
        "回溯进度   {0} {1}" -f
        (New-ProgressBar $BackfillProgress),
        (Format-Percent $BackfillProgress)
    )

    Write-Host (
        "CURRENT    {0} {1}" -f
        (New-ProgressBar $CurrentProgress),
        (Format-Percent $CurrentProgress)
    )

    Write-Host ""
    Write-Host "【本轮与累计成果】" -ForegroundColor Cyan

    Write-Metric "本轮计划槽位" $PlannedSlots
    Write-Metric "本轮计划来源" $PlannedSources
    Write-Metric "累计政策文档" $DocumentsTotal
    Write-Metric "新增文档" $DocumentsAdded
    Write-Metric "更新文档版本" $DocumentsUpdated
    Write-Metric "内容未变化" $DocumentsUnchanged
    Write-Metric "失败文档" $DocumentsFailed
    Write-Metric "发现文章链接" $DiscoveredLinks
    Write-Metric "终态文章链接" $TerminalLinks
    Write-Metric "发现附件" $AttachmentsDiscovered
    Write-Metric "已处理附件" $AttachmentsProcessed

    Write-Host ""
    Write-Host "【完整度与数据质量】" -ForegroundColor Cyan

    Write-Metric "来源覆盖度" $SourceCoverage ratio
    Write-Metric "历史覆盖度" $HistoricalCoverage ratio
    Write-Metric "文章终态率" $ArticleTerminalRatio ratio
    Write-Metric "附件处理率" $AttachmentCoverage ratio
    Write-Metric "字段完整率" $FieldCompleteness ratio
    Write-Metric "解析成功率" $ParseSuccessRatio ratio
    Write-Metric "新鲜度" $FreshnessRatio ratio
    Write-Metric "综合完整度" $OverallCompleteness ratio

    Write-Host ""
    Write-Host "【缺口与风险】" -ForegroundColor Cyan

    Write-Metric "开放 gaps" $OpenGaps
    Write-Metric "critical gaps" $CriticalGaps
    Write-Metric "可自动修复 gaps" $RepairableGaps
    Write-Metric "人工审核 gaps" $HumanReviewGaps
    Write-Metric "失败来源" $FailedSources
    Write-Metric "降级来源" $DegradedSources
    Write-Metric "过期来源" $StaleSources
    Write-Metric "checkpoint 冲突" $CheckpointConflicts
    Write-Metric "重复插入" $DuplicateInserts

    Write-Host ""
    Write-Host "【时间与文件】" -ForegroundColor Cyan

    Write-Metric "最近完成运行" $LastCompletedRun text
    Write-Metric "最近成功全量同步" $LastSuccessfulSync text
    Write-Metric "最近运行目录" $LatestRunPath text

    # ========================================================
    # 最近错误
    # ========================================================

    $RecentErrors = Get-RecentErrors `
        -LatestRunPath $LatestRunPath `
        -Limit $MaxErrorLines

    Write-Host ""
    Write-Host "【最近错误/警告线索】" -ForegroundColor Cyan

    if ($RecentErrors.Count -eq 0) {
        Write-Host "未在最近日志中发现高风险关键词。" `
            -ForegroundColor Green
    }
    else {
        foreach ($ErrorItem in $RecentErrors) {
            Write-Host (
                "[{0}] {1}" -f
                $ErrorItem.Time.ToString("MM-dd HH:mm:ss"),
                $ErrorItem.Line
            ) -ForegroundColor Yellow
        }
    }

    # ========================================================
    # 写监控快照
    # ========================================================

    $Snapshot = [ordered]@{
        generated_at = $Now.ToUniversalTime().ToString("o")

        process = [ordered]@{
            lock_exists = $ProcessState.LockExists
            pid = $ProcessState.Pid
            process_alive = $ProcessState.ProcessAlive
            process_name = $ProcessState.ProcessName
            started_at = $ProcessState.StartedAt
            stop_requested = $ProcessState.StopRequested
        }

        run = [ordered]@{
            run_id = $RunId
            cycle = $Cycle
            global_status = $GlobalStatus
            status_exit_code = $StatusCommand.ExitCode
            latest_run_path = $LatestRunPath
            last_completed_run = $LastCompletedRun
            last_successful_full_sync = $LastSuccessfulSync
        }

        slots = [ordered]@{
            total = $TotalSlots
            resolved = $ResolvedSlots
            verified = $VerifiedSlots
            enabled = $EnabledSlots
            crawl_ready = $CrawlReadySlots
            backfilled = $BackfilledSlots
            current = $CurrentSlots
            unresolved = $UnresolvedSlots
            human_review = $HumanReviewSlots

            verified_progress = $VerifiedProgress
            enabled_progress = $EnabledProgress
            backfill_progress = $BackfillProgress
            current_progress = $CurrentProgress
        }

        documents = [ordered]@{
            total = $DocumentsTotal
            added = $DocumentsAdded
            updated = $DocumentsUpdated
            unchanged = $DocumentsUnchanged
            failed = $DocumentsFailed
            links_discovered = $DiscoveredLinks
            links_terminal = $TerminalLinks
            attachments_discovered = $AttachmentsDiscovered
            attachments_processed = $AttachmentsProcessed
        }

        quality = [ordered]@{
            source_coverage_ratio = $SourceCoverage
            historical_coverage_ratio = $HistoricalCoverage
            article_terminal_ratio = $ArticleTerminalRatio
            attachment_coverage_ratio = $AttachmentCoverage
            field_completeness_ratio = $FieldCompleteness
            parse_success_ratio = $ParseSuccessRatio
            freshness_ratio = $FreshnessRatio
            overall_completeness = $OverallCompleteness
        }

        gaps = [ordered]@{
            open = $OpenGaps
            critical = $CriticalGaps
            repairable = $RepairableGaps
            human_review = $HumanReviewGaps
        }

        risks = [ordered]@{
            failed_sources = $FailedSources
            degraded_sources = $DegradedSources
            stale_sources = $StaleSources
            checkpoint_conflicts = $CheckpointConflicts
            duplicate_inserts = $DuplicateInserts
        }

        health = [ordered]@{
            status = $Health.Status
            score = $Health.Score
            explanation = $Health.Reason
        }

        source_files = [ordered]@{
            automation_state = if ($AutomationStateFile) {
                $AutomationStateFile.FullName
            }
            else {
                $null
            }

            database_status = if ($DatabaseStatusFile) {
                $DatabaseStatusFile.FullName
            }
            else {
                $null
            }

            run_summary = if ($RunSummaryFile) {
                $RunSummaryFile.FullName
            }
            else {
                $null
            }

            gap_summary = if ($GapSummaryFile) {
                $GapSummaryFile.FullName
            }
            else {
                $null
            }

            completeness = if ($CompletenessFile) {
                $CompletenessFile.FullName
            }
            else {
                $null
            }

            source_health = if ($SourceHealthFile) {
                $SourceHealthFile.FullName
            }
            else {
                $null
            }
        }

        recent_errors = @(
            $RecentErrors | ForEach-Object {
                [ordered]@{
                    time = $_.Time.ToUniversalTime().ToString("o")
                    file = $_.File
                    line = $_.Line
                }
            }
        )
    }

    $CurrentSnapshotPath = Join-Path `
        $MonitorRoot `
        "CURRENT_HEALTH_SNAPSHOT.json"

    $TimestampSnapshotPath = Join-Path `
        $MonitorRoot `
        (
            "health_snapshot_{0}.json" -f
            $Now.ToUniversalTime().ToString("yyyyMMddTHHmmssZ")
        )

    $Snapshot |
        ConvertTo-Json -Depth 30 |
        Set-Content `
            -LiteralPath $CurrentSnapshotPath `
            -Encoding utf8

    $Snapshot |
        ConvertTo-Json -Depth 30 |
        Set-Content `
            -LiteralPath $TimestampSnapshotPath `
            -Encoding utf8

    Write-Host ""
    Write-Host "监控快照：" -ForegroundColor Cyan
    Write-Host $CurrentSnapshotPath

    if ($Watch) {
        Write-Host ""
        Write-Host (
            "{0} 秒后刷新；按 Ctrl+C 退出监控。" -f
            $IntervalSeconds
        ) -ForegroundColor DarkGray
    }
}

# ============================================================
# 主循环
# ============================================================

do {
    try {
        Show-CRPDHealth
    }
    catch {
        Write-Host ""
        Write-Host "监控本轮失败：" -ForegroundColor Red
        Write-Host $_.Exception.Message -ForegroundColor Red

        $FailureSnapshot = [ordered]@{
            generated_at = (
                Get-Date
            ).ToUniversalTime().ToString("o")

            monitor_status = "FAILED"
            error = $_.Exception.Message
            stack = $_.ScriptStackTrace
        }

        $FailureSnapshot |
            ConvertTo-Json -Depth 10 |
            Set-Content `
                -LiteralPath (
                    Join-Path $MonitorRoot "MONITOR_FAILURE.json"
                ) `
                -Encoding utf8
    }

    if ($Watch) {
        Start-Sleep -Seconds $IntervalSeconds
    }
}
while ($Watch)