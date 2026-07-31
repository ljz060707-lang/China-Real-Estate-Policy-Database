param(
    [string]$StartDate = "2018-01-01",
    [string]$EndDate = "",

    [string]$ProjectRoot = "C:\Users\ljz52\Documents\Codex\2026-07-13\text-20260705-xlsx-text-data-raw\policy-database",
    [string]$DataRoot = "D:\Data Set\CRPD",

    [int]$StartIndex = 1,
    [int]$EndIndex = 105,

    [int]$MaxPagesPerSource = 1000,
    [int]$MaxCandidatesPerSource = 10000,
    [int]$MaxCandidatesTotal = 10000,
    [int]$MaxFetches = 10000,

    [switch]$SkipAI,
    [switch]$ContinueAfterCityFailure
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if ([string]::IsNullOrWhiteSpace($EndDate)) {
    $EndDate = (Get-Date).ToString("yyyy-MM-dd")
}

$ProjectRoot = [System.IO.Path]::GetFullPath($ProjectRoot)
$CityFile = Join-Path $ProjectRoot "data\reference\cities_105.csv"
$OneCityScript = Join-Path $ProjectRoot "scripts\CRPD_Run_One_City_Exhaustive.ps1"

if (-not (Test-Path $CityFile)) {
    throw "未找到城市清单：$CityFile"
}
if (-not (Test-Path $OneCityScript)) {
    throw "未找到单城市脚本：$OneCityScript"
}

$Cities = @(
    Import-Csv $CityFile |
        Where-Object { $_.is_large_city_105 -eq "true" } |
        Sort-Object `
            @{ Expression = { [int]$_.city_tier_existing }; Ascending = $true },
            @{ Expression = { $_.province_code }; Ascending = $true },
            @{ Expression = { $_.city_code }; Ascending = $true }
)

if ($Cities.Count -ne 105) {
    Write-Warning "cities_105.csv 当前读取到 $($Cities.Count) 个目标城市，不等于105；脚本仍按真实清单执行。"
}

if ($StartIndex -lt 1) { $StartIndex = 1 }
if ($EndIndex -gt $Cities.Count) { $EndIndex = $Cities.Count }
if ($StartIndex -gt $EndIndex) {
    throw "StartIndex不能大于EndIndex。"
}

$QueueRoot = Join-Path $DataRoot "jobs\city_full_search"
$QueueLog = Join-Path $DataRoot "logs\city_full_search\all_cities_master.log"
$QueueState = Join-Path $QueueRoot "city_queue_state.csv"
New-Item -ItemType Directory -Force -Path $QueueRoot | Out-Null
New-Item -ItemType Directory -Force -Path (Split-Path $QueueLog -Parent) | Out-Null

function Write-MasterLog {
    param([string]$Message, [string]$Level = "INFO")
    $Line = "{0} [{1}] {2}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $Level, $Message
    Write-Host $Line
    Add-Content -Path $QueueLog -Value $Line -Encoding UTF8
}

function Add-QueueState {
    param(
        [int]$Index,
        [object]$City,
        [string]$Status,
        [string]$Message
    )

    $Row = [pscustomobject]@{
        index = $Index
        city_id = $City.city_id
        city_name = $City.city_name
        province_name = $City.province_name
        status = $Status
        message = $Message
        recorded_at = (Get-Date).ToString("o")
    }

    if (Test-Path $QueueState) {
        $Row | Export-Csv -Path $QueueState -NoTypeInformation -Encoding UTF8 -Append
    }
    else {
        $Row | Export-Csv -Path $QueueState -NoTypeInformation -Encoding UTF8
    }
}

Write-MasterLog "105城市顺序任务开始：索引 $StartIndex—$EndIndex；时间 $StartDate—$EndDate"

for ($i = $StartIndex; $i -le $EndIndex; $i++) {
    $City = $Cities[$i - 1]

    $AlreadyCompleted = $false
    if (Test-Path $QueueState) {
        $AlreadyCompleted = [bool](
            Import-Csv $QueueState |
                Where-Object {
                    $_.city_id -eq $City.city_id -and $_.status -eq "completed"
                } |
                Select-Object -Last 1
        )
    }

    if ($AlreadyCompleted) {
        Write-MasterLog "[$i/$($Cities.Count)] 跳过已完成城市：$($City.city_name)"
        continue
    }

    Write-MasterLog "[$i/$($Cities.Count)] 开始城市：$($City.city_name)（$($City.province_name)）"
    Add-QueueState -Index $i -City $City -Status "started" -Message "开始执行"

    $Arguments = @{
        CityName = $City.city_name
        StartDate = $StartDate
        EndDate = $EndDate
        ProjectRoot = $ProjectRoot
        DataRoot = $DataRoot
        MaxPagesPerSource = $MaxPagesPerSource
        MaxCandidatesPerSource = $MaxCandidatesPerSource
        MaxCandidatesTotal = $MaxCandidatesTotal
        MaxFetches = $MaxFetches
        Resume = $true
    }

    if ($SkipAI) {
        $Arguments["SkipAI"] = $true
    }

    try {
        & $OneCityScript @Arguments
        Add-QueueState -Index $i -City $City -Status "completed" -Message "城市任务命令完成"
        Write-MasterLog "[$i/$($Cities.Count)] 城市完成：$($City.city_name)"
    }
    catch {
        $Message = $_.Exception.Message
        Add-QueueState -Index $i -City $City -Status "failed" -Message $Message
        Write-MasterLog "[$i/$($Cities.Count)] 城市失败：$($City.city_name)；$Message" "ERROR"

        if (-not $ContinueAfterCityFailure) {
            throw
        }
    }

    Start-Sleep -Seconds 20
}

Write-MasterLog "本次城市队列执行结束。"
