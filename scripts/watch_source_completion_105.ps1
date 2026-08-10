param(
    [string]$Repo = "D:\Codex\projects\Documents-Codex\2026-07-13\text-20260705-xlsx-text-data-raw\policy-database",
    [string]$DataRoot = "E:\Data Set\CRPD",
    [int]$RefreshSeconds = 10
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "SilentlyContinue"

$Python = Join-Path $Repo ".venv\Scripts\python.exe"
$MetricsScript = Join-Path $Repo "scripts\source105_metrics.py"
$StatusFile = Join-Path $DataRoot "control\source_completion_105\status.json"

function New-Bar {
    param(
        [int]$Value,
        [int]$Total,
        [int]$Width = 32
    )

    if ($Total -le 0) {
        return "[NO DATA]"
    }

    $Value = [Math]::Max(0, [Math]::Min($Value, $Total))
    $Ratio = [double]$Value / [double]$Total
    $Percent = [int][Math]::Round($Ratio * 100.0)

    $Filled = [int][Math]::Floor($Ratio * $Width)
    $Empty = $Width - $Filled

    return (
        "[" +
        ("#" * $Filled) +
        ("-" * $Empty) +
        "] $Value/$Total  $Percent%"
    )
}

while ($true) {
    $Status = $null
    $Metrics = $null

    if (Test-Path $StatusFile) {
        try {
            $Status = Get-Content `
                -LiteralPath $StatusFile `
                -Raw `
                -Encoding UTF8 |
                ConvertFrom-Json
        }
        catch {
            $Status = $null
        }
    }

    try {
        $MetricsText = & $Python $MetricsScript 2>$null
        $Metrics = $MetricsText | ConvertFrom-Json
    }
    catch {
        $Metrics = $null
    }

    $Processes = @(
        Get-CimInstance Win32_Process |
        Where-Object {
            $_.CommandLine -match (
                "run_source_completion_105|" +
                "discover-all|" +
                "probe-candidates|" +
                "verify-candidates|" +
                "promote-verified|" +
                "enable-verified"
            )
        }
    )

    Clear-Host

    Write-Host "============================================================" -ForegroundColor DarkCyan
    Write-Host " CRPD SOURCE COMPLETION - 105 CITIES" -ForegroundColor Cyan
    Write-Host "============================================================" -ForegroundColor DarkCyan
    Write-Host (" Updated:       {0}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"))

    if ($Processes.Count -gt 0) {
        Write-Host (
            " Worker:        RUNNING  PID=" +
            (($Processes.ProcessId) -join ",")
        ) -ForegroundColor Green
    }
    else {
        Write-Host " Worker:        NOT RUNNING" -ForegroundColor Yellow
    }

    if ($null -ne $Status) {
        Write-Host (" Run ID:        {0}" -f $Status.run_id)
        Write-Host (" Run status:    {0}" -f $Status.status)
        Write-Host (" Current stage: {0}" -f $Status.stage_name)
        Write-Host (
            " Stage:         " +
            (New-Bar `
                ([int]$Status.stage_index) `
                ([int]$Status.stage_total))
        )
    }
    else {
        Write-Host " Run status:    STATUS FILE NOT FOUND"
    }

    Write-Host ""
    Write-Host " SOURCE DATA PROGRESS" -ForegroundColor Cyan

    if ($null -ne $Metrics) {
        Write-Host (
            " Candidate:     " +
            (New-Bar ([int]$Metrics.candidate_slots) 525)
        )

        Write-Host (
            " Probed slots:  " +
            (New-Bar ([int]$Metrics.probed_slots) 525)
        )

        Write-Host (
            " Verified:      " +
            (New-Bar ([int]$Metrics.verified_slots) 525)
        )

        Write-Host (
            " Enabled:       " +
            (New-Bar ([int]$Metrics.enabled_slots) 525)
        )

        Write-Host ""
        Write-Host (
            " Candidate cities: " +
            (New-Bar ([int]$Metrics.candidate_cities) 105)
        )

        Write-Host (
            " Verified cities:  " +
            (New-Bar ([int]$Metrics.verified_cities) 105)
        )

        Write-Host (
            " Enabled cities:   " +
            (New-Bar ([int]$Metrics.enabled_cities) 105)
        )

        Write-Host ""
        Write-Host (
            " Candidate records: {0}; probed records: {1}" -f
            $Metrics.candidate_records,
            $Metrics.probed_candidates
        )
    }
    else {
        Write-Host " Metrics unavailable." -ForegroundColor Yellow
    }

    if (
        $null -ne $Status -and
        $Status.log_path -and
        (Test-Path $Status.log_path)
    ) {
        Write-Host ""
        Write-Host " LATEST OUTPUT" -ForegroundColor Cyan

        Get-Content `
            -LiteralPath $Status.log_path `
            -Tail 4 `
            -ErrorAction SilentlyContinue |
            ForEach-Object {
                Write-Host (" " + $_)
            }
    }

    if (
        $null -ne $Status -and
        $Status.status -eq "FAILED"
    ) {
        Write-Host ""
        Write-Host (" ERROR: {0}" -f $Status.error) -ForegroundColor Red
    }

    Write-Host ""
    Write-Host (
        " Refresh every {0}s. Ctrl+C closes monitor only." -f
        $RefreshSeconds
    ) -ForegroundColor DarkGray

    Start-Sleep -Seconds $RefreshSeconds
}
