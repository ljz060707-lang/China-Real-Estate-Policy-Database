param(
    [int]$StartYear = 2018,
    [int]$EndYear = (Get-Date).Year,
    [int]$MaxPagesPerSource = 300,
    [int]$MaxCandidatesPerSource = 3000,
    [int]$MaxCandidatesTotal = 100000,
    [int]$MaxFetches = 10000,
    [int]$RecoveryFetches = 1000,
    [int]$PauseSecondsBetweenYears = 20,
    [switch]$SkipSeedBacktrack,
    [switch]$SkipFinalGlobalAI
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$Root = (Get-Location).Path
if (-not (Test-Path (Join-Path $Root "pyproject.toml"))) {
    throw "请先 cd 到 CRPD 项目根目录后再运行本脚本。"
}

$DataRoot = "D:\Data Set\CRPD"
$RunStamp = Get-Date -Format "yyyyMMdd_HHmmss"
$LogRoot = Join-Path $DataRoot "logs\historical_full_ai\$RunStamp"
$StateRoot = Join-Path $DataRoot "jobs"
$StatePath = Join-Path $StateRoot "historical_full_ai_state.json"
$FailurePath = Join-Path $LogRoot "failures.csv"

New-Item -ItemType Directory -Force -Path $LogRoot, $StateRoot | Out-Null
"stage,year,time,error" | Set-Content -Encoding UTF8 $FailurePath

if (-not ("CRPDKeepAwake" -as [type])) {
    Add-Type @"
using System;
using System.Runtime.InteropServices;
public static class CRPDKeepAwake {
    [DllImport("kernel32.dll", SetLastError = true)]
    public static extern uint SetThreadExecutionState(uint esFlags);
}
"@
}
[void][CRPDKeepAwake]::SetThreadExecutionState([uint32]0x80000001)

function Write-RunLog {
    param([string]$Message, [string]$Level = "INFO")
    $line = "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') [$Level] $Message"
    Write-Host $line
    Add-Content -Encoding UTF8 -Path (Join-Path $LogRoot "master.log") -Value $line
}

function Invoke-PolicyDb {
    param(
        [Parameter(Mandatory = $true)][string[]]$CommandArgs,
        [Parameter(Mandatory = $true)][string]$Stage
    )

    $safeStage = $Stage -replace '[\\/:*?"<>| ]', "_"
    $logFile = Join-Path $LogRoot "$safeStage.log"

    Write-RunLog "开始：uv run policydb $($CommandArgs -join ' ')"
    & uv run policydb @CommandArgs 2>&1 |
        Tee-Object -FilePath $logFile -Append |
        Out-Host

    $exitCode = $LASTEXITCODE
    if ($exitCode -ne 0) {
        throw "命令失败（exit=$exitCode）：policydb $($CommandArgs -join ' ')"
    }
    Write-RunLog "完成：$Stage"
}

function Get-RecentJobs {
    $raw = (& uv run policydb update status --limit 20 2>&1 | Out-String)
    if ($LASTEXITCODE -ne 0) {
        throw "无法读取任务状态。"
    }
    return @($raw | ConvertFrom-Json)
}

function Get-LatestHistoricalJob {
    $jobs = Get-RecentJobs
    return $jobs |
        Where-Object { $_.mode -eq "historical_105" } |
        Sort-Object { [datetime]$_.created_at } -Descending |
        Select-Object -First 1
}

function Save-State {
    param([hashtable]$State)
    $State.last_updated = (Get-Date).ToString("o")
    $State | ConvertTo-Json -Depth 10 |
        Set-Content -Encoding UTF8 -Path $StatePath
}

function Load-State {
    if (Test-Path $StatePath) {
        $existing = Get-Content -Raw -Encoding UTF8 $StatePath | ConvertFrom-Json
        return @{
            completed_years = @($existing.completed_years | ForEach-Object { [int]$_ })
            last_updated = [string]$existing.last_updated
        }
    }
    return @{
        completed_years = @()
        last_updated = ""
    }
}

function Invoke-YearAI {
    param(
        [Parameter(Mandatory = $true)][string]$RunId,
        [Parameter(Mandatory = $true)][int]$Year
    )

    Invoke-PolicyDb @("archive", "sync") "year_${Year}_archive_sync"
    Invoke-PolicyDb @("ai", "classify", "--run-id", $RunId) "year_${Year}_ai_classify"
    Invoke-PolicyDb @("ai", "verify", "--run-id", $RunId) "year_${Year}_ai_verify"
    Invoke-PolicyDb @("taxonomy", "build") "year_${Year}_taxonomy"
    Invoke-PolicyDb @("ai", "deduplicate") "year_${Year}_deduplicate"
    Invoke-PolicyDb @("ai", "route-pools") "year_${Year}_route_pools"
    Invoke-PolicyDb @("confidence", "build") "year_${Year}_confidence"
    Invoke-PolicyDb @("review", "auto") "year_${Year}_review_auto"
    Invoke-PolicyDb @("coverage", "build") "year_${Year}_coverage"
    Invoke-PolicyDb @("build-database") "year_${Year}_build_database"
    Invoke-PolicyDb @("validate") "year_${Year}_validate"
}

try {
    Write-RunLog "CRPD 2018年以来已知官方来源全量抓取与AI处理开始。"
    Write-RunLog "项目：$Root"
    Write-RunLog "数据根目录：$DataRoot"
    Write-RunLog "年份：$StartYear-$EndYear"

    if ($StartYear -lt 2018) {
        throw "本脚本默认研究范围从2018年开始。"
    }
    if ($EndYear -lt $StartYear -or $EndYear -gt (Get-Date).Year) {
        throw "EndYear 设置无效。"
    }

    $activeStatuses = @(
        "queued", "preparing", "discovering", "fetching", "parsing",
        "deduplicating", "enriching", "verifying", "rebuilding",
        "validating", "reporting"
    )
    $activeJobs = Get-RecentJobs | Where-Object { $_.status -in $activeStatuses }
    if ($activeJobs) {
        $ids = ($activeJobs | ForEach-Object { $_.job_id }) -join ", "
        throw "检测到仍在运行的任务：$ids。请等待或取消后再启动全量脚本。"
    }

    Invoke-PolicyDb @("storage", "verify", "--target", $DataRoot) "00_storage_verify"
    Invoke-PolicyDb @("sources", "validate-registry") "01_sources_validate"
    Invoke-PolicyDb @("ai", "test") "02_ai_test"

    if (-not $SkipSeedBacktrack) {
        Invoke-PolicyDb @(
            "crawl", "seed-backtrack",
            "--from", "2018-01-01",
            "--to", "today",
            "--max-fetches", [string]$MaxFetches
        ) "03_seed_backtrack"

        Invoke-PolicyDb @("archive", "sync") "04_seed_archive_sync"
    }

    $state = Load-State

    foreach ($Year in $StartYear..$EndYear) {
        if ($Year -in $state.completed_years) {
            Write-RunLog "跳过已完成年份：$Year"
            continue
        }

        $fromDate = "$Year-01-01"
        $toDate = if ($Year -eq (Get-Date).Year) {
            (Get-Date).ToString("yyyy-MM-dd")
        } else {
            "$Year-12-31"
        }

        try {
            Write-RunLog "开始历史抓取：$Year（$fromDate 至 $toDate）"

            Invoke-PolicyDb @(
                "crawl", "historical",
                "--from", $fromDate,
                "--to", $toDate,
                "--max-pages-per-source", [string]$MaxPagesPerSource,
                "--max-candidates-per-source", [string]$MaxCandidatesPerSource,
                "--max-candidates-total", [string]$MaxCandidatesTotal,
                "--max-fetches", [string]$MaxFetches,
                "--resume"
            ) "year_${Year}_crawl"

            $job = Get-LatestHistoricalJob
            if (-not $job -or -not $job.run_id) {
                throw "未找到 $Year 对应的 historical_105 run_id。"
            }

            Write-RunLog (
                "年份 $Year 抓取结果：run_id=$($job.run_id)，" +
                "candidate=$($job.counters.candidate_count)，" +
                "fetched=$($job.counters.fetched)，failed=$($job.counters.failed)"
            )

            if ($job.status -notin @("completed", "completed_with_warnings")) {
                throw "年份 $Year 任务状态异常：$($job.status)"
            }

            Invoke-YearAI -RunId ([string]$job.run_id) -Year $Year

            $state.completed_years = @($state.completed_years + $Year | Sort-Object -Unique)
            Save-State $state
            Write-RunLog "年份 $Year 已完成抓取、归档、AI分类复核、池路由和校验。" "SUCCESS"
        }
        catch {
            $errorText = $_.Exception.Message.Replace('"', "'")
            Add-Content -Encoding UTF8 -Path $FailurePath -Value (
                "year,$Year,$(Get-Date -Format 'o'),`"$errorText`""
            )
            Write-RunLog "年份 $Year 失败：$errorText；脚本将继续下一个年份。" "ERROR"
        }

        if ($PauseSecondsBetweenYears -gt 0) {
            Write-RunLog "等待 $PauseSecondsBetweenYears 秒后继续。"
            Start-Sleep -Seconds $PauseSecondsBetweenYears
        }
    }

    Write-RunLog "开始缺失正文与归档恢复。"
    try {
        Invoke-PolicyDb @(
            "crawl", "recover-missing",
            "--max-fetches", [string]$RecoveryFetches
        ) "90_recover_missing"
    }
    catch {
        Write-RunLog "recover-missing 出现警告：$($_.Exception.Message)" "WARN"
    }

    Invoke-PolicyDb @("archive", "recover-missing") "91_archive_recover_missing"

    if (-not $SkipFinalGlobalAI) {
        Write-RunLog "开始全库AI补处理；此阶段可能持续较长时间。"
        Invoke-PolicyDb @("ai", "classify") "92_global_ai_classify"
        Invoke-PolicyDb @("ai", "verify") "93_global_ai_verify"
        Invoke-PolicyDb @("taxonomy", "build") "94_global_taxonomy"
        Invoke-PolicyDb @("ai", "deduplicate") "95_global_deduplicate"
        Invoke-PolicyDb @("ai", "route-pools") "96_global_route_pools"
        Invoke-PolicyDb @("confidence", "build") "97_global_confidence"
        Invoke-PolicyDb @("review", "auto") "98_global_review_auto"
    }

    Invoke-PolicyDb @("coverage", "build") "99_coverage_build"
    Invoke-PolicyDb @("build-database") "100_build_database"
    Invoke-PolicyDb @("validate") "101_validate"
    Invoke-PolicyDb @("crawl", "audit") "102_crawl_audit"

    try {
        Invoke-PolicyDb @("archive", "audit") "103_archive_audit"
    }
    catch {
        Write-RunLog "archive audit 报告存在失败项，请后续查看日志：$($_.Exception.Message)" "WARN"
    }

    Write-RunLog "全量脚本执行结束。日志目录：$LogRoot" "SUCCESS"
    Write-RunLog "失败任务清单：$FailurePath"
    Write-RunLog "断点状态：$StatePath"
}
finally {
    [void][CRPDKeepAwake]::SetThreadExecutionState([uint32]0x80000000)
}
