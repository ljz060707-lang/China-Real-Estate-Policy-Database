param(
    [string]$ProjectRoot = "C:\Users\ljz52\Documents\Codex\2026-07-13\text-20260705-xlsx-text-data-raw\policy-database",
    [string]$DataRoot = "E:\Data Set\CRPD",
    [string]$StartDate = "2018-01-01",
    [string]$EndDate = "today",

    [int]$MaxPagesPerSource = 3000,
    [int]$MaxCandidatesPerShard = 100000,
    [int]$MaxFetchesPerShard = 100000,
    [int]$NetworkRetryPasses = 2,

    [switch]$DiscoverSources,
    [switch]$ApplyDiscoveredSources,
    [switch]$AllowUnverifiedSources,
    [switch]$ExistingSourcesOnly,
    [switch]$SkipAI,
    [switch]$PilotOnly
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)

$ProjectRoot = [System.IO.Path]::GetFullPath($ProjectRoot)
$PolicyDbExe = Join-Path $ProjectRoot ".venv\Scripts\policydb.exe"
$PythonExe = Join-Path $ProjectRoot ".venv\Scripts\python.exe"
$CityFile = Join-Path $ProjectRoot "data\reference\cities_105.csv"

if (-not (Test-Path $PolicyDbExe)) {
    throw "未找到CLI入口：$PolicyDbExe。请先在项目根目录运行 uv sync --all-extras。"
}
if (-not (Test-Path $PythonExe)) {
    throw "未找到项目Python：$PythonExe"
}
if (-not (Test-Path $CityFile)) {
    throw "未找到105城市清单：$CityFile"
}

$Stamp = "{0}_{1}" -f (Get-Date -Format "yyyyMMdd_HHmmss"), ([guid]::NewGuid().ToString("N").Substring(0, 8))
$LogRoot = Join-Path $DataRoot "logs\audited_full_backfill\$Stamp"
$AcceptanceRoot = Join-Path $DataRoot "outputs\acceptance"
New-Item -ItemType Directory -Force -Path $LogRoot, $AcceptanceRoot | Out-Null
$MasterLog = Join-Path $LogRoot "master.log"

function Write-RunLog {
    param(
        [string]$Message,
        [ValidateSet("INFO", "WARN", "ERROR")]
        [string]$Level = "INFO"
    )
    $Line = "{0} [{1}] {2}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $Level, $Message
    Write-Host $Line
    Add-Content -Path $MasterLog -Value $Line -Encoding UTF8
}

function Invoke-PolicyDb {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$Arguments,
        [Parameter(Mandatory = $true)]
        [string]$Stage,
        [switch]$ContinueOnError
    )

    $SafeStage = $Stage -replace '[\\/:*?"<>| ]', "_"
    $LogFile = Join-Path $LogRoot "$SafeStage.log"
    Write-RunLog "开始：policydb $($Arguments -join ' ')"

    $PreviousPreference = $ErrorActionPreference
    $ExitCode = 1
    $Output = @()
    try {
        $ErrorActionPreference = "Continue"
        $Output = @(
            & $PolicyDbExe @Arguments 2>&1 |
                Tee-Object -FilePath $LogFile -Append |
                ForEach-Object {
                    Write-Host $_
                    $_
                }
        )
        $ExitCode = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $PreviousPreference
    }

    $Text = $Output | Out-String
    if ($ExitCode -ne 0) {
        $Message = "阶段失败（exit=$ExitCode）：$Stage；日志：$LogFile"
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
        Text = $Text
        LogFile = $LogFile
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
            return $Text.Substring($ObjectStart, $ObjectEnd - $ObjectStart + 1) |
                ConvertFrom-Json
        }
        catch {}
    }

    $ArrayStart = $Text.IndexOf("[")
    $ArrayEnd = $Text.LastIndexOf("]")
    if ($ArrayStart -ge 0 -and $ArrayEnd -gt $ArrayStart) {
        try {
            return $Text.Substring($ArrayStart, $ArrayEnd - $ArrayStart + 1) |
                ConvertFrom-Json
        }
        catch {}
    }

    return $null
}

function Get-CityShardCounts {
    param([string]$CityId)

    $Code = @'
from pathlib import Path
import json
import sys
import polars as pl

path = Path(sys.argv[1])
city_id = sys.argv[2]
if not path.exists():
    print(json.dumps({"rows": 0, "pending": 0, "partial_network": 0,
                      "partial_cap": 0, "failed": 0}, ensure_ascii=False))
    raise SystemExit(0)

frame = pl.read_parquet(path).filter(pl.col("city_id") == city_id)
result = {"rows": frame.height}
for status in ("pending", "partial_network", "partial_cap", "failed",
               "source_incomplete", "complete_policy_found",
               "confirmed_zero", "complete_unverified"):
    result[status] = frame.filter(pl.col("status") == status).height
print(json.dumps(result, ensure_ascii=False))
'@

    $TempScript = Join-Path $env:TEMP "crpd_city_shard_status_$PID.py"
    Set-Content -Path $TempScript -Value $Code -Encoding UTF8
    try {
        $ShardPath = Join-Path $DataRoot "curated\crawl_shards.parquet"
        $Raw = & $PythonExe $TempScript $ShardPath $CityId
        if ($LASTEXITCODE -ne 0) {
            throw "读取城市分片状态失败：$CityId"
        }
        return $Raw | ConvertFrom-Json
    }
    finally {
        Remove-Item $TempScript -Force -ErrorAction SilentlyContinue
    }
}

function Run-City {
    param([object]$City, [int]$Index, [int]$Total)

    $CityName = [string]$City.city_name
    $CityId = [string]$City.city_id
    Write-RunLog "[$Index/$Total] 开始：$CityName（$CityId）"

    [void](Invoke-PolicyDb `
        -Arguments @("network", "diagnose", "--city", $CityName) `
        -Stage ("network_{0}" -f $CityId) `
        -ContinueOnError)

    $Arguments = @(
        "crawl", "exhaustive-city",
        "--city", $CityName,
        "--from", $StartDate,
        "--to", $EndDate,
        "--max-pages-per-source", [string]$MaxPagesPerSource,
        "--max-candidates-per-shard", [string]$MaxCandidatesPerShard,
        "--max-fetches-per-shard", [string]$MaxFetchesPerShard,
        "--resume",
        "--no-run-ai",
        "--archive"
    )

    $Result = Invoke-PolicyDb `
        -Arguments $Arguments `
        -Stage ("crawl_{0}_initial" -f $CityId) `
        -ContinueOnError

    if ($Result.ExitCode -ne 0) {
        Write-RunLog "城市初始扫描失败，保留状态并继续下一城市：$CityName" "ERROR"
        return
    }

    # 新增的自适应子分片只会在下一次resume中处理，因此循环到无pending。
    for ($Pass = 1; $Pass -le 20; $Pass++) {
        $Counts = Get-CityShardCounts -CityId $CityId
        Write-RunLog (
            "$CityName 分片状态：rows=$($Counts.rows), pending=$($Counts.pending), " +
            "partial_cap=$($Counts.partial_cap), partial_network=$($Counts.partial_network), " +
            "failed=$($Counts.failed)"
        )

        if ([int]$Counts.pending -le 0) {
            break
        }

        [void](Invoke-PolicyDb `
            -Arguments @(
                "crawl", "exhaustive-resume",
                "--city", $CityName,
                "--from", $StartDate,
                "--to", $EndDate
            ) `
            -Stage ("crawl_{0}_resume_{1:D2}" -f $CityId, $Pass) `
            -ContinueOnError)
    }

    for ($Retry = 1; $Retry -le $NetworkRetryPasses; $Retry++) {
        $Counts = Get-CityShardCounts -CityId $CityId
        $Retryable = [int]$Counts.partial_network + [int]$Counts.failed
        if ($Retryable -le 0) {
            break
        }

        Write-RunLog "$CityName 存在 $Retryable 个失败/网络异常分片，执行重试 $Retry/$NetworkRetryPasses。" "WARN"
        [void](Invoke-PolicyDb `
            -Arguments @(
                "crawl", "exhaustive-retry",
                "--city", $CityName,
                "--from", $StartDate,
                "--to", $EndDate
            ) `
            -Stage ("crawl_{0}_retry_{1:D2}" -f $CityId, $Retry) `
            -ContinueOnError)
    }

    [void](Invoke-PolicyDb `
        -Arguments @("progress", "status", "--city", $CityName) `
        -Stage ("progress_{0}" -f $CityId) `
        -ContinueOnError)

    Write-RunLog "[$Index/$Total] 城市阶段结束：$CityName"
}

Set-Location $ProjectRoot
Write-RunLog "CRPD可审计补扫开始。日志：$LogRoot"

# 前置验证
[void](Invoke-PolicyDb -Arguments @("--help") -Stage "00_cli_help")
[void](Invoke-PolicyDb -Arguments @("storage", "verify", "--target", $DataRoot) -Stage "01_storage")
[void](Invoke-PolicyDb -Arguments @("sources", "validate-registry") -Stage "02_registry")

if (-not $SkipAI) {
    [void](Invoke-PolicyDb -Arguments @("ai", "test") -Stage "03_ai_test")
}

if ($ExistingSourcesOnly) {
    Write-RunLog "ExistingSourcesOnly已启用：跳过来源候选种子、来源发现、候选验证和525完整来源门禁，直接复用当前已启用来源进行历史抓取。"
}
else {
    # 建立来源候选和525槽位
    [void](Invoke-PolicyDb -Arguments @("sources", "audit-525") -Stage "10_source_audit_initial")
    [void](Invoke-PolicyDb -Arguments @("sources", "seed-record-candidates") -Stage "11_seed_candidates")

    if ($DiscoverSources) {
        $DiscoverArgs = @("sources", "discover-all")
        if ($ApplyDiscoveredSources) {
            $DiscoverArgs += "--apply"
        }
        [void](Invoke-PolicyDb `
            -Arguments $DiscoverArgs `
            -Stage "12_discover_all" `
            -ContinueOnError)

        if ($ApplyDiscoveredSources) {
            [void](Invoke-PolicyDb `
                -Arguments @("sources", "evaluate") `
                -Stage "13_evaluate_sources" `
                -ContinueOnError)

            # 命令单次上限100；重复执行不会启用不满足推荐条件的来源。
            for ($Round = 1; $Round -le 6; $Round++) {
                [void](Invoke-PolicyDb `
                    -Arguments @("sources", "enable-recommended", "--limit", "100") `
                    -Stage ("14_enable_recommended_{0:D2}" -f $Round) `
                    -ContinueOnError)
            }
        }
    }

    [void](Invoke-PolicyDb -Arguments @("sources", "verify-candidates") -Stage "15_verify_candidates")
    $AuditResult = Invoke-PolicyDb -Arguments @("sources", "audit-525") -Stage "16_source_audit_final"
    $Audit = ConvertFrom-LastJson -Text $AuditResult.Text

    if (-not $Audit) {
        throw "无法解析sources audit-525输出。"
    }

    $Audit | ConvertTo-Json -Depth 10 |
        Set-Content -Path (Join-Path $AcceptanceRoot "source_525_pre_backfill.json") -Encoding UTF8

    Write-RunLog (
        "来源槽位：required=$($Audit.required_slots), candidate=$($Audit.slots_with_candidate), " +
        "verified=$($Audit.slots_with_verified_candidate), enabled=$($Audit.slots_with_enabled_source), " +
        "verified_pct=$($Audit.verified_coverage_pct)%"
    )

    if ([double]$Audit.verified_coverage_pct -lt 100 -and -not $AllowUnverifiedSources) {
        throw (
            "当前verified source coverage为$($Audit.verified_coverage_pct)%，不满足可审计全量条件。" +
            "脚本已停止，避免把部分来源扫描称为全量。人工核验并启用来源后重跑；" +
            "仅用于先抓已有来源时可显式添加 -AllowUnverifiedSources。"
        )
    }
}

# 南京单月验收
[void](Invoke-PolicyDb `
    -Arguments @(
        "network", "diagnose", "--city", "南京市"
    ) `
    -Stage "20_nanjing_network" `
    -ContinueOnError)

[void](Invoke-PolicyDb `
    -Arguments @(
        "crawl", "exhaustive-city",
        "--city", "南京市",
        "--from", "2023-02-01",
        "--to", "2023-02-28",
        "--max-pages-per-source", [string]$MaxPagesPerSource,
        "--max-candidates-per-shard", [string]$MaxCandidatesPerShard,
        "--max-fetches-per-shard", [string]$MaxFetchesPerShard,
        "--resume",
        "--no-run-ai",
        "--archive"
    ) `
    -Stage "21_nanjing_2023_02" `
    -ContinueOnError)

[void](Invoke-PolicyDb `
    -Arguments @("audit", "exhaustive") `
    -Stage "22_nanjing_acceptance" `
    -ContinueOnError)

if ($PilotOnly) {
    Write-RunLog "PilotOnly已启用，南京单月验收后结束。"
    exit 0
}

# 逐城市顺序补扫
$Cities = @(
    Import-Csv $CityFile |
        Where-Object { $_.is_large_city_105 -eq "true" } |
        Sort-Object `
            @{ Expression = { [int]$_.city_tier_existing }; Ascending = $true },
            @{ Expression = { $_.province_code }; Ascending = $true },
            @{ Expression = { $_.city_code }; Ascending = $true }
)

for ($Index = 0; $Index -lt $Cities.Count; $Index++) {
    try {
        Run-City -City $Cities[$Index] -Index ($Index + 1) -Total $Cities.Count
    }
    catch {
        Write-RunLog "城市异常：$($Cities[$Index].city_name)；$($_.Exception.Message)" "ERROR"
    }
}

# 全局归档与后处理。当前exhaustive-city的--run-ai只记录请求，故在这里显式执行。
[void](Invoke-PolicyDb -Arguments @("archive", "sync") -Stage "90_archive_sync" -ContinueOnError)
[void](Invoke-PolicyDb -Arguments @("archive", "recover-missing") -Stage "91_archive_recover" -ContinueOnError)

if (-not $SkipAI) {
    [void](Invoke-PolicyDb -Arguments @("ai", "classify") -Stage "92_ai_classify" -ContinueOnError)
    [void](Invoke-PolicyDb -Arguments @("ai", "verify") -Stage "93_ai_verify" -ContinueOnError)
}

[void](Invoke-PolicyDb -Arguments @("taxonomy", "build") -Stage "94_taxonomy" -ContinueOnError)
[void](Invoke-PolicyDb -Arguments @("ai", "deduplicate") -Stage "95_deduplicate" -ContinueOnError)
[void](Invoke-PolicyDb -Arguments @("ai", "route-pools") -Stage "96_route_pools" -ContinueOnError)
[void](Invoke-PolicyDb -Arguments @("confidence", "build") -Stage "97_confidence" -ContinueOnError)
[void](Invoke-PolicyDb -Arguments @("coverage", "build") -Stage "98_coverage" -ContinueOnError)
[void](Invoke-PolicyDb -Arguments @("build-database") -Stage "99_build_database" -ContinueOnError)
[void](Invoke-PolicyDb -Arguments @("validate") -Stage "100_validate" -ContinueOnError)
[void](Invoke-PolicyDb -Arguments @("audit", "exhaustive") -Stage "101_acceptance" -ContinueOnError)
[void](Invoke-PolicyDb -Arguments @("progress", "export", "--format", "csv") -Stage "102_progress_csv" -ContinueOnError)
[void](Invoke-PolicyDb -Arguments @("progress", "export", "--format", "json") -Stage "103_progress_json" -ContinueOnError)

Write-RunLog "补扫流程结束。请以exhaustive_crawl_acceptance.json和city_year_completion.csv判断完整度。"
