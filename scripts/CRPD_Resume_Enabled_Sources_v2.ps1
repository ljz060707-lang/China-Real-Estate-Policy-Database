param(
    [string]$ProjectRoot = "",
    [string]$DataRoot = "E:\Data Set\CRPD",

    [int]$StartCityIndex = 1,
    [int]$EndCityIndex = 105,
    [int]$StartYear = 2018,
    [int]$EndYear = 0,

    [int]$MaxPagesPerSource = 300,
    [int]$MaxCandidatesPerShard = 5000,
    [int]$MaxFetchesPerShard = 5000,

    [int]$MaxSplitPasses = 20,
    [int]$NetworkRetryPasses = 2,
    [int]$TimeoutMinutesPerCityYear = 180,
    [int]$PostProcessTimeoutMinutes = 720,
    [int]$HeartbeatSeconds = 30,

    [switch]$RunSourceHealthCheck,
    [switch]$DiagnoseEachCity,
    [switch]$SkipAI,
    [switch]$SkipArchiveRecovery,
    [switch]$StopOnCityYearFailure
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)

if ([string]::IsNullOrWhiteSpace($ProjectRoot)) {
    $ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
}

if ($EndYear -le 0) {
    $EndYear = (Get-Date).Year
}

$ProjectRoot = [System.IO.Path]::GetFullPath($ProjectRoot)
$PolicyDbExe = Join-Path $ProjectRoot ".venv\Scripts\policydb.exe"
$PythonExe = Join-Path $ProjectRoot ".venv\Scripts\python.exe"
$CityFile = Join-Path $ProjectRoot "data\reference\cities_105.csv"
$ShardPath = Join-Path $DataRoot "curated\crawl_shards.parquet"

foreach ($RequiredPath in @($PolicyDbExe, $PythonExe, $CityFile)) {
    if (-not (Test-Path $RequiredPath)) {
        throw "缺少必需文件：$RequiredPath"
    }
}

if ($StartYear -gt $EndYear) {
    throw "StartYear不能晚于EndYear。"
}

$Stamp = "{0}_{1}" -f (Get-Date -Format "yyyyMMdd_HHmmss"), ([guid]::NewGuid().ToString("N").Substring(0, 8))
$LogRoot = Join-Path $DataRoot "logs\enabled_source_backfill_v2\$Stamp"
$StateRoot = Join-Path $DataRoot "jobs\enabled_source_backfill_v2"
$AcceptanceRoot = Join-Path $DataRoot "outputs\acceptance"

New-Item -ItemType Directory -Force -Path $LogRoot, $StateRoot, $AcceptanceRoot |
    Out-Null

$MasterLog = Join-Path $LogRoot "master.log"
$ControllerState = Join-Path $StateRoot "controller_state.csv"
$StatusHelper = Join-Path $LogRoot "read_shard_status.py"

@'
from __future__ import annotations

import json
import sys
from pathlib import Path

import polars as pl

path = Path(sys.argv[1])
city_id = sys.argv[2]
year = int(sys.argv[3])

empty = {
    "rows": 0,
    "pending": 0,
    "running": 0,
    "partial_network": 0,
    "partial_parser": 0,
    "partial_cap": 0,
    "partial_archive": 0,
    "partial_temporal": 0,
    "failed": 0,
    "source_incomplete": 0,
    "complete_policy_found": 0,
    "confirmed_zero": 0,
    "complete_unverified": 0,
    "fetched": 0,
    "failed_fetches": 0,
    "documents": 0,
    "date_unknown": 0,
    "cap_hits": 0,
}

if not path.exists():
    print(json.dumps(empty, ensure_ascii=False))
    raise SystemExit(0)

frame = pl.read_parquet(path)
if frame.is_empty():
    print(json.dumps(empty, ensure_ascii=False))
    raise SystemExit(0)

frame = frame.filter(
    (pl.col("city_id") == city_id)
    & (pl.col("start_date").cast(pl.String).str.slice(0, 4).cast(pl.Int32) == year)
)

result = dict(empty)
result["rows"] = frame.height

for status in (
    "pending",
    "running",
    "partial_network",
    "partial_parser",
    "partial_cap",
    "partial_archive",
    "partial_temporal",
    "failed",
    "source_incomplete",
    "complete_policy_found",
    "confirmed_zero",
    "complete_unverified",
):
    result[status] = frame.filter(pl.col("status") == status).height

def total(column: str) -> int:
    return (
        int(frame[column].fill_null(0).sum())
        if column in frame.columns and frame.height
        else 0
    )

result["fetched"] = total("fetched")
result["failed_fetches"] = total("failed")
result["documents"] = total("document_versions")
result["date_unknown"] = total("date_unknown_count")

cap_columns = [
    name for name in (
        "candidate_cap_hit",
        "fetch_cap_hit",
        "page_cap_hit",
        "global_safety_limit_hit",
    )
    if name in frame.columns
]
if cap_columns:
    expression = pl.lit(False)
    for name in cap_columns:
        expression = expression | pl.col(name).fill_null(False)
    result["cap_hits"] = frame.filter(expression).height

print(json.dumps(result, ensure_ascii=False))
'@ | Set-Content -Path $StatusHelper -Encoding UTF8

function Write-RunLog {
    param(
        [string]$Message,
        [ValidateSet("INFO", "WARN", "ERROR")]
        [string]$Level = "INFO"
    )

    $Line = "{0} [{1}] {2}" -f (
        Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    ), $Level, $Message

    Write-Host $Line
    Add-Content -Path $MasterLog -Value $Line -Encoding UTF8
}

function Add-ControllerState {
    param(
        [int]$CityIndex,
        [object]$City,
        [int]$Year,
        [string]$Phase,
        [string]$Status,
        [string]$Message = ""
    )

    $Row = [pscustomobject]@{
        city_index = $CityIndex
        city_id = [string]$City.city_id
        city_name = [string]$City.city_name
        province_name = [string]$City.province_name
        year = $Year
        phase = $Phase
        status = $Status
        message = $Message
        recorded_at = (Get-Date).ToString("o")
    }

    if (Test-Path $ControllerState) {
        $Row | Export-Csv `
            -Path $ControllerState `
            -NoTypeInformation `
            -Encoding UTF8 `
            -Append
    }
    else {
        $Row | Export-Csv `
            -Path $ControllerState `
            -NoTypeInformation `
            -Encoding UTF8
    }
}

function Quote-NativeArgument {
    param([string]$Value)

    if ($Value -notmatch '[\s"]') {
        return $Value
    }

    return '"' + ($Value -replace '(\\*)"', '$1$1\"' -replace '(\\+)$', '$1$1') + '"'
}

function Invoke-PolicyDbTimed {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$Arguments,

        [Parameter(Mandatory = $true)]
        [string]$Stage,

        [int]$TimeoutMinutes = 180,
        [switch]$ContinueOnError
    )

    $SafeStage = $Stage -replace '[\\/:*?"<>| ]', "_"
    $StdOut = Join-Path $LogRoot "$SafeStage.stdout.log"
    $StdErr = Join-Path $LogRoot "$SafeStage.stderr.log"

    $ArgumentLine = (
        $Arguments |
            ForEach-Object { Quote-NativeArgument ([string]$_) }
    ) -join " "

    Write-RunLog "开始：policydb $($Arguments -join ' ')"
    Write-RunLog "阶段超时：$TimeoutMinutes 分钟；stdout=$StdOut"

    $StartInfo = @{
        FilePath = $PolicyDbExe
        ArgumentList = $ArgumentLine
        WorkingDirectory = $ProjectRoot
        RedirectStandardOutput = $StdOut
        RedirectStandardError = $StdErr
        PassThru = $true
        WindowStyle = "Hidden"
    }

    $Process = Start-Process @StartInfo
    $StartedAt = Get-Date
    $LastTail = ""

    while (-not $Process.HasExited) {
        Start-Sleep -Seconds $HeartbeatSeconds
        $Process.Refresh()

        $Elapsed = (Get-Date) - $StartedAt
        $OutBytes = if (Test-Path $StdOut) {
            (Get-Item $StdOut).Length
        }
        else {
            0
        }
        $ErrBytes = if (Test-Path $StdErr) {
            (Get-Item $StdErr).Length
        }
        else {
            0
        }

        $Tail = ""
        if (Test-Path $StdOut) {
            $Tail = (
                Get-Content $StdOut -Encoding UTF8 -Tail 1 -ErrorAction SilentlyContinue
            )
        }
        if (-not $Tail -and (Test-Path $StdErr)) {
            $Tail = (
                Get-Content $StdErr -Encoding UTF8 -Tail 1 -ErrorAction SilentlyContinue
            )
        }

        Write-RunLog (
            "$Stage 运行中：elapsed=$([math]::Round($Elapsed.TotalMinutes, 1))min, " +
            "stdout=$OutBytes B, stderr=$ErrBytes B"
        )

        if ($Tail -and $Tail -ne $LastTail) {
            Write-Host "  ↳ $Tail"
            $LastTail = $Tail
        }

        if ($Elapsed.TotalMinutes -ge $TimeoutMinutes) {
            Write-RunLog "$Stage 超过超时限制，终止进程树 PID=$($Process.Id)" "ERROR"
            & taskkill.exe /PID $Process.Id /T /F 2>$null | Out-Null
            Start-Sleep -Seconds 2

            if (-not $ContinueOnError) {
                throw "阶段超时：$Stage"
            }

            return [pscustomobject]@{
                ExitCode = 124
                TimedOut = $true
                StdOut = $StdOut
                StdErr = $StdErr
                Text = ""
            }
        }
    }

    $Process.WaitForExit()
    $ExitCode = $Process.ExitCode
    $Text = if (Test-Path $StdOut) {
        Get-Content $StdOut -Encoding UTF8 -Raw
    }
    else {
        ""
    }

    if (Test-Path $StdErr) {
        $ErrorText = Get-Content $StdErr -Encoding UTF8 -Raw
        if ($ErrorText) {
            Add-Content -Path $StdOut -Value "`n--- STDERR ---`n$ErrorText" -Encoding UTF8
        }
    }

    if ($ExitCode -ne 0) {
        $Message = "阶段失败（exit=$ExitCode）：$Stage"
        Write-RunLog $Message "ERROR"

        if (-not $ContinueOnError) {
            throw $Message
        }
    }
    else {
        Write-RunLog "完成：$Stage"
    }

    return [pscustomobject]@{
        ExitCode = $ExitCode
        TimedOut = $false
        StdOut = $StdOut
        StdErr = $StdErr
        Text = $Text
    }
}

function ConvertFrom-LastJson {
    param([string]$Text)

    if ([string]::IsNullOrWhiteSpace($Text)) {
        return $null
    }

    $ObjectStart = $Text.IndexOf("{")
    $ObjectEnd = $Text.LastIndexOf("}")
    if ($ObjectStart -ge 0 -and $ObjectEnd -gt $ObjectStart) {
        try {
            return $Text.Substring(
                $ObjectStart,
                $ObjectEnd - $ObjectStart + 1
            ) | ConvertFrom-Json
        }
        catch {}
    }

    return $null
}

function Get-CityYearShardStatus {
    param(
        [string]$CityId,
        [int]$Year
    )

    $Raw = & $PythonExe $StatusHelper $ShardPath $CityId $Year
    if ($LASTEXITCODE -ne 0) {
        throw "无法读取 $CityId/$Year 分片状态。"
    }

    return $Raw | ConvertFrom-Json
}

function Get-YearDateRange {
    param([int]$Year)

    $From = "{0}-01-01" -f $Year
    $CurrentYear = (Get-Date).Year

    if ($Year -eq $CurrentYear) {
        $To = "today"
    }
    else {
        $To = "{0}-12-31" -f $Year
    }

    return [pscustomobject]@{
        From = $From
        To = $To
    }
}

function Invoke-CityYearPass {
    param(
        [object]$City,
        [int]$Year,
        [string]$StageSuffix,
        [switch]$RetryErrors
    )

    $Range = Get-YearDateRange -Year $Year

    $Arguments = @(
        "crawl", "exhaustive-city",
        "--city", [string]$City.city_name,
        "--from", $Range.From,
        "--to", $Range.To,
        "--max-pages-per-source", [string]$MaxPagesPerSource,
        "--max-candidates-per-shard", [string]$MaxCandidatesPerShard,
        "--max-fetches-per-shard", [string]$MaxFetchesPerShard,
        "--resume",
        "--no-run-ai",
        "--archive",
        "--sequential"
    )

    if ($RetryErrors) {
        $Arguments += "--retry-errors"
    }
    else {
        $Arguments += "--no-retry-errors"
    }

    return Invoke-PolicyDbTimed `
        -Arguments $Arguments `
        -Stage ("crawl_{0}_{1}_{2}" -f $City.city_id, $Year, $StageSuffix) `
        -TimeoutMinutes $TimeoutMinutesPerCityYear `
        -ContinueOnError
}

function Write-CityYearStatus {
    param(
        [object]$City,
        [int]$Year,
        [object]$Counts
    )

    $Message = (
        (
            "{0} {1}：rows={2}, pending={3}, completed={4}, zero={5}, " +
            "network={6}, parser={7}, cap={8}, failed={9}, source_missing={10}, " +
            "fetched={11}, documents={12}, unknown_date={13}"
        ) -f @(
            $City.city_name,
            $Year,
            $Counts.rows,
            $Counts.pending,
            $Counts.complete_policy_found,
            $Counts.confirmed_zero,
            $Counts.partial_network,
            $Counts.partial_parser,
            $Counts.partial_cap,
            $Counts.failed,
            $Counts.source_incomplete,
            $Counts.fetched,
            $Counts.documents,
            $Counts.date_unknown
        )
    )
    Write-RunLog $Message
}

function Run-CityYear {
    param(
        [object]$City,
        [int]$CityIndex,
        [int]$TotalCities,
        [int]$Year
    )

    $CityName = [string]$City.city_name
    $CityId = [string]$City.city_id

    Write-RunLog "[$CityIndex/$TotalCities] 开始：$CityName，年份=$Year"
    Add-ControllerState `
        -CityIndex $CityIndex `
        -City $City `
        -Year $Year `
        -Phase "crawl" `
        -Status "started"

    $Counts = Get-CityYearShardStatus -CityId $CityId -Year $Year
    Write-CityYearStatus -City $City -Year $Year -Counts $Counts

    $RetryableBefore = (
        [int]$Counts.partial_network +
        [int]$Counts.partial_parser +
        [int]$Counts.failed
    )

    $NeedsInitialPass = (
        [int]$Counts.rows -eq 0 -or
        [int]$Counts.pending -gt 0
    )

    if ($NeedsInitialPass) {
        $Initial = Invoke-CityYearPass `
            -City $City `
            -Year $Year `
            -StageSuffix "initial"

        if ($Initial.ExitCode -ne 0) {
            Add-ControllerState `
                -CityIndex $CityIndex `
                -City $City `
                -Year $Year `
                -Phase "crawl" `
                -Status "initial_failed" `
                -Message "exit=$($Initial.ExitCode)"

            if ($StopOnCityYearFailure) {
                throw "$CityName $Year 初始扫描失败。"
            }
        }
    }
    elseif ($RetryableBefore -eq 0) {
        Write-RunLog "$CityName $Year 已无pending和可重试错误，跳过初始扫描。"
    }

    # 命中上限后创建的子分片只会在下一次相同命令中继续处理。
    for ($Pass = 1; $Pass -le $MaxSplitPasses; $Pass++) {
        $Counts = Get-CityYearShardStatus -CityId $CityId -Year $Year
        Write-CityYearStatus -City $City -Year $Year -Counts $Counts

        if ([int]$Counts.pending -le 0) {
            break
        }

        $PassResult = Invoke-CityYearPass `
            -City $City `
            -Year $Year `
            -StageSuffix ("splitpass_{0:D2}" -f $Pass)

        if ($PassResult.ExitCode -ne 0) {
            Write-RunLog "$CityName $Year 第$Pass次分片续扫失败。" "WARN"
            break
        }
    }

    for ($Retry = 1; $Retry -le $NetworkRetryPasses; $Retry++) {
        $Counts = Get-CityYearShardStatus -CityId $CityId -Year $Year
        $Retryable = (
            [int]$Counts.partial_network +
            [int]$Counts.partial_parser +
            [int]$Counts.failed
        )

        if ($Retryable -le 0) {
            break
        }

        Write-RunLog (
            "$CityName $Year 存在$Retryable个可重试异常分片，" +
            "执行重试 $Retry/$NetworkRetryPasses。"
        ) "WARN"

        $RetryResult = Invoke-CityYearPass `
            -City $City `
            -Year $Year `
            -StageSuffix ("retry_{0:D2}" -f $Retry) `
            -RetryErrors

        if ($RetryResult.ExitCode -ne 0) {
            break
        }
    }

    $Final = Get-CityYearShardStatus -CityId $CityId -Year $Year
    Write-CityYearStatus -City $City -Year $Year -Counts $Final

    $Status = if (
        [int]$Final.pending -eq 0 -and
        [int]$Final.partial_network -eq 0 -and
        [int]$Final.partial_parser -eq 0 -and
        [int]$Final.failed -eq 0
    ) {
        "controller_finished"
    }
    else {
        "controller_partial"
    }

    Add-ControllerState `
        -CityIndex $CityIndex `
        -City $City `
        -Year $Year `
        -Phase "crawl" `
        -Status $Status `
        -Message (
            "pending={0};network={1};parser={2};failed={3};cap={4};source_incomplete={5}" -f @(
                $Final.pending,
                $Final.partial_network,
                $Final.partial_parser,
                $Final.failed,
                $Final.partial_cap,
                $Final.source_incomplete
            )
        )
}

Set-Location $ProjectRoot
Write-RunLog "从现有已启用来源补扫阶段继续。日志：$LogRoot"

# 这里不再运行seed-record-candidates，避免重复卡在阶段11。
[void](Invoke-PolicyDbTimed `
    -Arguments @("--help") `
    -Stage "00_cli_help" `
    -TimeoutMinutes 5)

[void](Invoke-PolicyDbTimed `
    -Arguments @("storage", "verify", "--target", $DataRoot) `
    -Stage "01_storage_verify" `
    -TimeoutMinutes 10)

[void](Invoke-PolicyDbTimed `
    -Arguments @("sources", "validate-registry") `
    -Stage "02_registry_validate" `
    -TimeoutMinutes 10)

if ($RunSourceHealthCheck) {
    [void](Invoke-PolicyDbTimed `
        -Arguments @("sources", "health-all") `
        -Stage "03_source_health" `
        -TimeoutMinutes 180 `
        -ContinueOnError)
}

[void](Invoke-PolicyDbTimed `
    -Arguments @("sources", "verify-candidates") `
    -Stage "04_verify_candidates" `
    -TimeoutMinutes 60 `
    -ContinueOnError)

$AuditResult = Invoke-PolicyDbTimed `
    -Arguments @("sources", "audit-525") `
    -Stage "05_source_audit" `
    -TimeoutMinutes 30

$Audit = ConvertFrom-LastJson -Text $AuditResult.Text
if ($Audit) {
    $Audit | ConvertTo-Json -Depth 10 |
        Set-Content `
            -Path (Join-Path $AcceptanceRoot "source_525_before_enabled_backfill.json") `
            -Encoding UTF8

    Write-RunLog (
        "来源审计：required=$($Audit.required_slots), " +
        "candidate=$($Audit.slots_with_candidate), " +
        "verified=$($Audit.slots_with_verified_candidate), " +
        "enabled=$($Audit.slots_with_enabled_source), " +
        "verified_pct=$($Audit.verified_coverage_pct)%"
    )

    if ([double]$Audit.verified_coverage_pct -lt 100) {
        Write-RunLog (
            "来源核验率不足100%。本脚本只补扫source_registry中已启用来源，" +
            "不能据此宣称525槽位全量完成。"
        ) "WARN"
    }
}

# 先做一次南京网络诊断；逐城市诊断可通过-DiagnoseEachCity开启。
[void](Invoke-PolicyDbTimed `
    -Arguments @("network", "diagnose", "--city", "南京市") `
    -Stage "06_network_nanjing" `
    -TimeoutMinutes 10 `
    -ContinueOnError)

$Cities = @(
    Import-Csv $CityFile |
        Where-Object { $_.is_large_city_105 -eq "true" } |
        Sort-Object `
            @{ Expression = { [int]$_.city_tier_existing }; Ascending = $true },
            @{ Expression = { $_.province_code }; Ascending = $true },
            @{ Expression = { $_.city_code }; Ascending = $true }
)

if ($Cities.Count -eq 0) {
    throw "未读取到105城市。"
}

if ($StartCityIndex -lt 1) {
    $StartCityIndex = 1
}
if ($EndCityIndex -gt $Cities.Count) {
    $EndCityIndex = $Cities.Count
}
if ($StartCityIndex -gt $EndCityIndex) {
    throw "StartCityIndex不能大于EndCityIndex。"
}

for ($CityIndex = $StartCityIndex; $CityIndex -le $EndCityIndex; $CityIndex++) {
    $City = $Cities[$CityIndex - 1]

    if ($DiagnoseEachCity) {
        [void](Invoke-PolicyDbTimed `
            -Arguments @("network", "diagnose", "--city", [string]$City.city_name) `
            -Stage ("network_{0}" -f $City.city_id) `
            -TimeoutMinutes 10 `
            -ContinueOnError)
    }

    for ($Year = $StartYear; $Year -le $EndYear; $Year++) {
        try {
            Run-CityYear `
                -City $City `
                -CityIndex $CityIndex `
                -TotalCities $Cities.Count `
                -Year $Year
        }
        catch {
            $Message = $_.Exception.Message
            Write-RunLog "$($City.city_name) $Year 控制器异常：$Message" "ERROR"

            Add-ControllerState `
                -CityIndex $CityIndex `
                -City $City `
                -Year $Year `
                -Phase "crawl" `
                -Status "controller_error" `
                -Message $Message

            if ($StopOnCityYearFailure) {
                throw
            }
        }
    }

    [void](Invoke-PolicyDbTimed `
        -Arguments @("progress", "status", "--city", [string]$City.city_name) `
        -Stage ("progress_{0}" -f $City.city_id) `
        -TimeoutMinutes 30 `
        -ContinueOnError)
}

Write-RunLog "全部指定城市—年份扫描阶段结束，开始归档与后处理。"

# 抓取命令中的--archive和--run-ai当前不能替代显式后处理，故在这里逐项执行。
[void](Invoke-PolicyDbTimed `
    -Arguments @("archive", "sync") `
    -Stage "90_archive_sync" `
    -TimeoutMinutes $PostProcessTimeoutMinutes `
    -ContinueOnError)

if (-not $SkipArchiveRecovery) {
    [void](Invoke-PolicyDbTimed `
        -Arguments @("archive", "recover-missing") `
        -Stage "91_archive_recover_missing" `
        -TimeoutMinutes $PostProcessTimeoutMinutes `
        -ContinueOnError)
}

if (-not $SkipAI) {
    [void](Invoke-PolicyDbTimed `
        -Arguments @("ai", "test") `
        -Stage "92_ai_test" `
        -TimeoutMinutes 10 `
        -ContinueOnError)

    [void](Invoke-PolicyDbTimed `
        -Arguments @("ai", "classify") `
        -Stage "93_ai_classify" `
        -TimeoutMinutes $PostProcessTimeoutMinutes `
        -ContinueOnError)

    [void](Invoke-PolicyDbTimed `
        -Arguments @("ai", "verify") `
        -Stage "94_ai_verify" `
        -TimeoutMinutes $PostProcessTimeoutMinutes `
        -ContinueOnError)
}

[void](Invoke-PolicyDbTimed `
    -Arguments @("taxonomy", "build") `
    -Stage "95_taxonomy_build" `
    -TimeoutMinutes 120 `
    -ContinueOnError)

[void](Invoke-PolicyDbTimed `
    -Arguments @("ai", "deduplicate") `
    -Stage "96_deterministic_deduplicate" `
    -TimeoutMinutes 240 `
    -ContinueOnError)

[void](Invoke-PolicyDbTimed `
    -Arguments @("ai", "route-pools") `
    -Stage "97_route_pools" `
    -TimeoutMinutes 120 `
    -ContinueOnError)

[void](Invoke-PolicyDbTimed `
    -Arguments @("confidence", "build") `
    -Stage "98_confidence_build" `
    -TimeoutMinutes 120 `
    -ContinueOnError)

[void](Invoke-PolicyDbTimed `
    -Arguments @("coverage", "build") `
    -Stage "99_coverage_build" `
    -TimeoutMinutes 120 `
    -ContinueOnError)

[void](Invoke-PolicyDbTimed `
    -Arguments @("build-database") `
    -Stage "100_build_database" `
    -TimeoutMinutes 240 `
    -ContinueOnError)

[void](Invoke-PolicyDbTimed `
    -Arguments @("validate") `
    -Stage "101_validate" `
    -TimeoutMinutes 120 `
    -ContinueOnError)

[void](Invoke-PolicyDbTimed `
    -Arguments @("audit", "exhaustive") `
    -Stage "102_exhaustive_acceptance" `
    -TimeoutMinutes 120 `
    -ContinueOnError)

[void](Invoke-PolicyDbTimed `
    -Arguments @("progress", "export", "--format", "csv") `
    -Stage "103_progress_csv" `
    -TimeoutMinutes 60 `
    -ContinueOnError)

[void](Invoke-PolicyDbTimed `
    -Arguments @("progress", "export", "--format", "json") `
    -Stage "104_progress_json" `
    -TimeoutMinutes 60 `
    -ContinueOnError)

Write-RunLog (
    "流程结束。最终以以下文件判断完成度：" +
    " $AcceptanceRoot\exhaustive_crawl_acceptance.json；" +
    " $AcceptanceRoot\city_year_completion.csv"
)
