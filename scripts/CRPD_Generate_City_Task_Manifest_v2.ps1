param(
    [string]$ProjectRoot = "C:\Users\ljz52\Documents\Codex\2026-07-13\text-20260705-xlsx-text-data-raw\policy-database",
    [string]$DataRoot = "D:\Data Set\CRPD",
    [string]$StartDate = "2018-01-01",
    [string]$EndDate = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if ([string]::IsNullOrWhiteSpace($EndDate)) {
    $EndDate = (Get-Date).ToString("yyyy-MM-dd")
}

$CityFile = Join-Path $ProjectRoot "data\reference\cities_105.csv"
if (-not (Test-Path $CityFile)) {
    throw "未找到城市清单：$CityFile"
}

$Cities = @(
    Import-Csv $CityFile |
        Where-Object { $_.is_large_city_105 -eq "true" } |
        Sort-Object `
            @{ Expression = { [int]$_.city_tier_existing }; Ascending = $true },
            @{ Expression = { $_.province_code }; Ascending = $true },
            @{ Expression = { $_.city_code }; Ascending = $true }
)

$OutputRoot = Join-Path $DataRoot "jobs\city_full_search"
$ManifestPath = Join-Path $OutputRoot "city_task_manifest.csv"
New-Item -ItemType Directory -Force -Path $OutputRoot | Out-Null

$Index = 0
$Rows = foreach ($City in $Cities) {
    $Index += 1

    $Command = (
        ".\scripts\CRPD_Run_One_City_Exhaustive_v2.ps1 " +
        "-CityName `"$($City.city_name)`" " +
        "-StartDate `"$StartDate`" " +
        "-EndDate `"$EndDate`""
    )

    [pscustomobject]@{
        index = $Index
        task_id = "CITY_TASK_{0:D3}_{1}" -f $Index, $City.city_id
        city_id = $City.city_id
        city_name = $City.city_name
        province_name = $City.province_name
        province_code = $City.province_code
        city_code = $City.city_code
        city_tier = $City.city_tier_existing
        city_scale_2020 = $City.city_scale_2020
        start_date = $StartDate
        end_date = $EndDate
        initial_partition = "city-month"
        adaptive_split = "month_to_half_to_day"
        max_candidates_per_leaf = 10000
        max_fetches_per_leaf = 10000
        ai_after_city = $true
        status = "pending"
        command = $Command
    }
}

$Rows | Export-Csv -Path $ManifestPath -NoTypeInformation -Encoding UTF8
Write-Host "已生成 $($Rows.Count) 个城市独立任务：$ManifestPath"
