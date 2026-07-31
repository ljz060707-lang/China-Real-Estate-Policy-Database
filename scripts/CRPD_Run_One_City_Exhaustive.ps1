param(
    [Parameter(Mandatory = $true)]
    [string]$CityName,

    [string]$StartDate = "2018-01-01",
    [string]$EndDate = "",

    [string]$ProjectRoot = "C:\Users\ljz52\Documents\Codex\2026-07-13\text-20260705-xlsx-text-data-raw\policy-database",
    [string]$DataRoot = "D:\Data Set\CRPD",

    [int]$MaxPagesPerSource = 1000,
    [int]$MaxCandidatesPerSource = 10000,
    [int]$MaxCandidatesTotal = 10000,
    [int]$MaxFetches = 10000,

    [int]$RetryCount = 3,
    [int]$RetryDelaySeconds = 30,

    [bool]$Resume = $true,
    [switch]$SkipAI
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if ([string]::IsNullOrWhiteSpace($EndDate)) {
    $EndDate = (Get-Date).ToString("yyyy-MM-dd")
}

$ProjectRoot = [System.IO.Path]::GetFullPath($ProjectRoot)
$PythonExe = Join-Path $ProjectRoot ".venv\Scripts\python.exe"
$CityFile = Join-Path $ProjectRoot "data\reference\cities_105.csv"

if (-not (Test-Path $PythonExe)) {
    throw "未找到项目解释器：$PythonExe。请先在项目根目录运行 uv sync --all-extras。"
}
if (-not (Test-Path $CityFile)) {
    throw "未找到105城市清单：$CityFile"
}
if (-not (Test-Path $DataRoot)) {
    throw "D盘数据根目录不可用：$DataRoot"
}

$CityRow = Import-Csv -Path $CityFile |
    Where-Object {
        $_.city_name -eq $CityName -or
        $_.city_name_short -eq $CityName -or
        $_.city_id -eq $CityName
    } |
    Select-Object -First 1

if (-not $CityRow) {
    throw "城市不在 cities_105.csv 中：$CityName"
}

$CanonicalCityName = $CityRow.city_name
$CityId = $CityRow.city_id
$ProvinceName = $CityRow.province_name
$SafeCity = ($CanonicalCityName -replace '[\\/:*?"<>| ]', '_')

$JobRoot = Join-Path $DataRoot "jobs\city_full_search\$CityId"
$LogRoot = Join-Path $DataRoot "logs\city_full_search\$CityId"
$StatePath = Join-Path $JobRoot "shard_state.csv"
$SummaryPath = Join-Path $JobRoot "city_summary.json"

New-Item -ItemType Directory -Force -Path $JobRoot | Out-Null
New-Item -ItemType Directory -Force -Path $LogRoot | Out-Null

function Write-RunLog {
    param(
        [string]$Message,
        [ValidateSet("INFO", "WARN", "ERROR")]
        [string]$Level = "INFO"
    )
    $Line = "{0} [{1}] [{2}] {3}" -f (
        Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    ), $Level, $CanonicalCityName, $Message
    Write-Host $Line
    Add-Content -Path (Join-Path $LogRoot "master.log") -Value $Line -Encoding UTF8
}

if (-not ("CRPDCityKeepAwakeV1" -as [type])) {
    Add-Type @"
using System;
using System.Runtime.InteropServices;
public static class CRPDCityKeepAwakeV1 {
    private const uint ES_CONTINUOUS = 0x80000000;
    private const uint ES_SYSTEM_REQUIRED = 0x00000001;
    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern uint SetThreadExecutionState(uint esFlags);
    public static uint Start() { return SetThreadExecutionState(ES_CONTINUOUS | ES_SYSTEM_REQUIRED); }
    public static uint Stop()  { return SetThreadExecutionState(ES_CONTINUOUS); }
}
"@
}

function Invoke-NativeLogged {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$Arguments,
        [Parameter(Mandatory = $true)]
        [string]$LogFile
    )

    $PreviousPreference = $ErrorActionPreference
    $Output = @()
    $ExitCode = 1
    try {
        $ErrorActionPreference = "Continue"
        $Output = @(
            & $PythonExe -m policydb.cli @Arguments 2>&1 |
                Tee-Object -FilePath $LogFile -Append
        )
        $ExitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $PreviousPreference
    }

    return [pscustomobject]@{
        ExitCode = $ExitCode
        Text = ($Output | Out-String)
    }
}

function ConvertFrom-LastJsonObject {
    param([string]$Text)

    if ([string]::IsNullOrWhiteSpace($Text)) {
        return $null
    }

    $FirstObject = $Text.IndexOf("{")
    $LastObject = $Text.LastIndexOf("}")
    if ($FirstObject -ge 0 -and $LastObject -gt $FirstObject) {
        $Candidate = $Text.Substring($FirstObject, $LastObject - $FirstObject + 1)
        try {
            return $Candidate | ConvertFrom-Json
        }
        catch {}
    }

    $FirstArray = $Text.IndexOf("[")
    $LastArray = $Text.LastIndexOf("]")
    if ($FirstArray -ge 0 -and $LastArray -gt $FirstArray) {
        $Candidate = $Text.Substring($FirstArray, $LastArray - $FirstArray + 1)
        try {
            return $Candidate | ConvertFrom-Json
        }
        catch {}
    }

    return $null
}

function Get-ExistingShard {
    param(
        [string]$ShardStart,
        [string]$ShardEnd
    )

    if (-not $Resume -or -not (Test-Path $StatePath)) {
        return $null
    }

    return Import-Csv $StatePath |
        Where-Object {
            $_.start_date -eq $ShardStart -and
            $_.end_date -eq $ShardEnd -and
            $_.terminal_state -eq "accepted"
        } |
        Select-Object -Last 1
}

function Add-ShardState {
    param(
        [string]$TaskId,
        [string]$ShardStart,
        [string]$ShardEnd,
        [string]$JobId,
        [string]$JobStatus,
        [long]$CandidateCount,
        [long]$Fetched,
        [long]$Failed,
        [long]$DocumentVersions,
        [bool]$CapHit,
        [string]$TerminalState,
        [string]$Note
    )

    $Row = [pscustomobject]@{
        task_id = $TaskId
        city_id = $CityId
        city_name = $CanonicalCityName
        province_name = $ProvinceName
        start_date = $ShardStart
        end_date = $ShardEnd
        job_id = $JobId
        job_status = $JobStatus
        candidate_count = $CandidateCount
        fetched = $Fetched
        failed = $Failed
        document_versions = $DocumentVersions
        cap_hit = $CapHit
        terminal_state = $TerminalState
        note = $Note
        recorded_at = (Get-Date).ToString("o")
    }

    if (Test-Path $StatePath) {
        $Row | Export-Csv -Path $StatePath -NoTypeInformation -Encoding UTF8 -Append
    }
    else {
        $Row | Export-Csv -Path $StatePath -NoTypeInformation -Encoding UTF8
    }
}

function Invoke-PolicyDbStage {
    param(
        [string[]]$Arguments,
        [string]$Stage,
        [bool]$ContinueOnError = $false
    )

    $SafeStage = $Stage -replace '[\\/:*?"<>| ]', '_'
    $LogFile = Join-Path $LogRoot "$SafeStage.log"
    Write-RunLog "开始：policydb $($Arguments -join ' ')"

    $Result = Invoke-NativeLogged -Arguments $Arguments -LogFile $LogFile
    if ($Result.ExitCode -ne 0) {
        $Message = "阶段失败（exit=$($Result.ExitCode)）：$Stage；日志：$LogFile"
        Write-RunLog $Message "ERROR"
        if (-not $ContinueOnError) {
            throw $Message
        }
        return $false
    }

    Write-RunLog "完成：$Stage"
    return $true
}

function Invoke-CrawlShard {
    param(
        [datetime]$From,
        [datetime]$To,
        [int]$Depth = 0
    )

    $ShardStart = $From.ToString("yyyy-MM-dd")
    $ShardEnd = $To.ToString("yyyy-MM-dd")
    $TaskId = "{0}_{1}_{2}" -f $CityId, $ShardStart, $ShardEnd
    $Existing = Get-ExistingShard -ShardStart $ShardStart -ShardEnd $ShardEnd

    if ($Existing) {
        Write-RunLog "跳过已完成分片：$ShardStart 至 $ShardEnd"
        return
    }

    $Stage = "crawl_{0}_{1}_{2}" -f $SafeCity, $ShardStart, $ShardEnd
    $LogFile = Join-Path $LogRoot "$Stage.log"

    $Args = @(
        "crawl", "historical",
        "--from", $ShardStart,
        "--to", $ShardEnd,
        "--cities", $CanonicalCityName,
        "--max-pages-per-source", [string]$MaxPagesPerSource,
        "--max-candidates-per-source", [string]$MaxCandidatesPerSource,
        "--max-candidates-total", [string]$MaxCandidatesTotal,
        "--max-fetches", [string]$MaxFetches,
        "--resume"
    )

    $Attempt = 0
    $CommandResult = $null
    do {
        $Attempt += 1
        Write-RunLog "抓取分片：$ShardStart 至 $ShardEnd；尝试 $Attempt/$RetryCount"
        $CommandResult = Invoke-NativeLogged -Arguments $Args -LogFile $LogFile
        if ($CommandResult.ExitCode -eq 0) {
            break
        }
        if ($Attempt -lt $RetryCount) {
            Start-Sleep -Seconds ($RetryDelaySeconds * $Attempt)
        }
    } while ($Attempt -lt $RetryCount)

    if ($CommandResult.ExitCode -ne 0) {
        Add-ShardState -TaskId $TaskId -ShardStart $ShardStart -ShardEnd $ShardEnd `
            -JobId "" -JobStatus "command_failed" -CandidateCount 0 -Fetched 0 `
            -Failed 0 -DocumentVersions 0 -CapHit $false -TerminalState "failed" `
            -Note "命令连续失败；查看 $LogFile"
        Write-RunLog "分片失败：$ShardStart 至 $ShardEnd" "ERROR"
        return
    }

    $CommandJson = ConvertFrom-LastJsonObject -Text $CommandResult.Text
    $JobId = ""
    if ($CommandJson -and $CommandJson.job_id) {
        $JobId = [string]$CommandJson.job_id
    }

    $JobStatus = "unknown"
    $CandidateCount = 0L
    $Fetched = 0L
    $Failed = 0L
    $DocumentVersions = 0L

    if ($JobId) {
        $StatusLog = Join-Path $LogRoot "status_$JobId.log"
        $StatusResult = Invoke-NativeLogged `
            -Arguments @("jobs", "status", "--job-id", $JobId) `
            -LogFile $StatusLog

        if ($StatusResult.ExitCode -eq 0) {
            $State = ConvertFrom-LastJsonObject -Text $StatusResult.Text
            if ($State) {
                if ($State.status) { $JobStatus = [string]$State.status }
                if ($State.counters) {
                    if ($null -ne $State.counters.candidate_count) {
                        $CandidateCount = [long]$State.counters.candidate_count
                    }
                    if ($null -ne $State.counters.fetched) {
                        $Fetched = [long]$State.counters.fetched
                    }
                    if ($null -ne $State.counters.failed) {
                        $Failed = [long]$State.counters.failed
                    }
                    if ($null -ne $State.counters.document_versions) {
                        $DocumentVersions = [long]$State.counters.document_versions
                    }
                }
            }
        }
    }

    # A hit at either hard ceiling means the interval cannot be accepted as exhaustive.
    # Use >= because exact equality is the common truncation signal.
    $CapHit = (
        $CandidateCount -ge $MaxCandidatesTotal -or
        $Fetched -ge $MaxFetches
    )

    $SpanDays = [int](($To.Date - $From.Date).TotalDays) + 1
    if ($CapHit -and $SpanDays -gt 1) {
        Add-ShardState -TaskId $TaskId -ShardStart $ShardStart -ShardEnd $ShardEnd `
            -JobId $JobId -JobStatus $JobStatus -CandidateCount $CandidateCount `
            -Fetched $Fetched -Failed $Failed -DocumentVersions $DocumentVersions `
            -CapHit $true -TerminalState "split" `
            -Note "达到候选或抓取上限，自动拆分更小日期区间。"

        $LeftDays = [math]::Floor($SpanDays / 2)
        $Middle = $From.Date.AddDays($LeftDays - 1)
        $RightStart = $Middle.AddDays(1)

        Write-RunLog "分片达到上限，拆分：$ShardStart—$($Middle.ToString('yyyy-MM-dd'))；$($RightStart.ToString('yyyy-MM-dd'))—$ShardEnd" "WARN"
        Invoke-CrawlShard -From $From.Date -To $Middle -Depth ($Depth + 1)
        Invoke-CrawlShard -From $RightStart -To $To.Date -Depth ($Depth + 1)
        return
    }

    $TerminalState = "accepted"
    $Note = "命令已完成；当前代码仍需依靠覆盖窗口证据判断是否真正穷尽。"

    if ($CapHit -and $SpanDays -eq 1) {
        $TerminalState = "daily_cap_hit"
        $Note = "单日仍达到10000上限，不能认定完整；需要按来源ID继续拆分。"
    }
    elseif ($JobStatus -notlike "completed*") {
        $TerminalState = "needs_review"
        $Note = "任务状态不是completed；需要检查抓取报告。"
    }
    elseif ($Failed -gt 0) {
        $TerminalState = "needs_review"
        $Note = "存在抓取失败项；需要后续recover-missing或重试。"
    }

    Add-ShardState -TaskId $TaskId -ShardStart $ShardStart -ShardEnd $ShardEnd `
        -JobId $JobId -JobStatus $JobStatus -CandidateCount $CandidateCount `
        -Fetched $Fetched -Failed $Failed -DocumentVersions $DocumentVersions `
        -CapHit $CapHit -TerminalState $TerminalState -Note $Note

    Write-RunLog (
        "分片结束：$ShardStart 至 $ShardEnd；status=$JobStatus；" +
        "candidates=$CandidateCount；fetched=$Fetched；failed=$Failed；" +
        "documents=$DocumentVersions；terminal=$TerminalState"
    )
}

function Invoke-AIPipeline {
    Write-RunLog "开始城市级归档、AI分类、复核、去重和覆盖重建。"

    $Stages = @(
        @{ Name = "archive_sync"; Args = @("archive", "sync") },
        @{ Name = "ai_classify"; Args = @("ai", "classify") },
        @{ Name = "ai_verify"; Args = @("ai", "verify") },
        @{ Name = "taxonomy_build"; Args = @("taxonomy", "build") },
        @{ Name = "ai_deduplicate"; Args = @("ai", "deduplicate") },
        @{ Name = "ai_route_pools"; Args = @("ai", "route-pools") },
        @{ Name = "coverage_build"; Args = @("coverage", "build") },
        @{ Name = "build_database"; Args = @("build-database") },
        @{ Name = "validate"; Args = @("validate") }
    )

    foreach ($Stage in $Stages) {
        [void](Invoke-PolicyDbStage `
            -Arguments $Stage.Args `
            -Stage ("{0}_{1}" -f $SafeCity, $Stage.Name) `
            -ContinueOnError $true)
    }
}

function Get-MonthIntervals {
    param(
        [datetime]$Start,
        [datetime]$End
    )

    $Cursor = Get-Date -Year $Start.Year -Month $Start.Month -Day 1
    $Intervals = @()

    while ($Cursor -le $End.Date) {
        $MonthStart = $Cursor
        if ($MonthStart -lt $Start.Date) {
            $MonthStart = $Start.Date
        }

        $MonthEnd = $Cursor.AddMonths(1).AddDays(-1)
        if ($MonthEnd -gt $End.Date) {
            $MonthEnd = $End.Date
        }

        $Intervals += [pscustomobject]@{
            Start = $MonthStart
            End = $MonthEnd
        }

        $Cursor = $Cursor.AddMonths(1)
    }

    return $Intervals
}

$Start = [datetime]::ParseExact($StartDate, "yyyy-MM-dd", $null)
$End = [datetime]::ParseExact($EndDate, "yyyy-MM-dd", $null)
if ($Start -gt $End) {
    throw "StartDate不能晚于EndDate。"
}

[void][CRPDCityKeepAwakeV1]::Start()
try {
    Set-Location $ProjectRoot

    Write-RunLog "城市独立历史任务开始。"
    Write-RunLog "城市：$CanonicalCityName（$CityId），省份：$ProvinceName"
    Write-RunLog "时间：$StartDate 至 $EndDate"
    Write-RunLog "规则：先按月；达到10000候选或抓取上限后自动二分，最小到单日。"

    [void](Invoke-PolicyDbStage `
        -Arguments @("storage", "verify", "--target", $DataRoot) `
        -Stage "${SafeCity}_storage_verify")

    [void](Invoke-PolicyDbStage `
        -Arguments @("ai", "test") `
        -Stage "${SafeCity}_ai_test")

    $Intervals = Get-MonthIntervals -Start $Start -End $End
    foreach ($Interval in $Intervals) {
        Invoke-CrawlShard -From $Interval.Start -To $Interval.End
    }

    if (-not $SkipAI) {
        Invoke-AIPipeline
    }

    $Rows = @()
    if (Test-Path $StatePath) {
        $Rows = @(Import-Csv $StatePath)
    }

    $Summary = [ordered]@{
        city_id = $CityId
        city_name = $CanonicalCityName
        province_name = $ProvinceName
        start_date = $StartDate
        end_date = $EndDate
        shard_rows = $Rows.Count
        accepted = @($Rows | Where-Object terminal_state -eq "accepted").Count
        split = @($Rows | Where-Object terminal_state -eq "split").Count
        needs_review = @($Rows | Where-Object terminal_state -eq "needs_review").Count
        failed = @($Rows | Where-Object terminal_state -eq "failed").Count
        daily_cap_hit = @($Rows | Where-Object terminal_state -eq "daily_cap_hit").Count
        total_candidates = [long](($Rows | Measure-Object -Property candidate_count -Sum).Sum)
        total_fetched = [long](($Rows | Measure-Object -Property fetched -Sum).Sum)
        total_document_versions = [long](($Rows | Measure-Object -Property document_versions -Sum).Sum)
        completed_at = (Get-Date).ToString("o")
        exhaustive_certified = $false
        certification_note = "当前任务按城市、月份自适应拆分并避免10000条截断，但只有覆盖窗口写入分页耗尽、无上限命中、无待处理错误后，才能认定可审计全量。"
    }

    $Summary | ConvertTo-Json -Depth 6 |
        Set-Content -Path $SummaryPath -Encoding UTF8

    Write-RunLog "城市任务结束；摘要：$SummaryPath"
}
finally {
    [void][CRPDCityKeepAwakeV1]::Stop()
}
