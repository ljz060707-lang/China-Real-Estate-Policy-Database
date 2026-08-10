param(
    [string]$Repo = "D:\Codex\projects\Documents-Codex\2026-07-13\text-20260705-xlsx-text-data-raw\policy-database",
    [string]$DataRoot = "E:\Data Set\CRPD",
    [ValidateRange(1,105)]
    [int]$CityLimit = 105,
    [switch]$DiscoveryOnly
)

Set-StrictMode -Version Latest

# Native decoding warnings must not terminate the whole workflow.
# Real failures are determined only by the process exit code.
$ErrorActionPreference = "Continue"

$Utf8NoBom = New-Object System.Text.UTF8Encoding($false)
[Console]::InputEncoding = $Utf8NoBom
[Console]::OutputEncoding = $Utf8NoBom
$OutputEncoding = $Utf8NoBom

$env:PYTHONUTF8 = "1"
$env:PYTHONIOENCODING = "utf-8"
$env:CRPD_DATA_ROOT = $DataRoot

try {
    chcp 65001 | Out-Null
}
catch {
}

$PolicyDB = Join-Path $Repo ".venv\Scripts\policydb.exe"
$Python = Join-Path $Repo ".venv\Scripts\python.exe"
$NativeWrapper = Join-Path $Repo "scripts\native_capture.py"
$ControlDir = Join-Path $DataRoot "control\source_completion_105"
$RunId = "SOURCE105_" + (Get-Date -Format "yyyyMMdd_HHmmss")
$RunDir = Join-Path $DataRoot "outputs\source_completion_105\$RunId"
$LogDir = Join-Path $RunDir "logs"
$StatusFile = Join-Path $ControlDir "status.json"

New-Item -ItemType Directory -Force -Path $ControlDir | Out-Null
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null

Set-Location $Repo

$Stages = @(
    @{
        Name = "Verify storage"
        Args = @("storage", "verify", "--target", $DataRoot)
    },
    @{
        Name = "Build 525-slot matrix"
        Args = @("sources", "complete-matrix")
    },
    @{
        Name = "Discover official candidates"
        Args = @(
            "sources", "discover-all",
            "--city-limit", [string]$CityLimit,
            "--apply"
        )
    },
    @{
        Name = "Pre-verify candidates"
        Args = @("sources", "verify-candidates")
    },
    @{
        Name = "Probe candidates over network"
        Args = @("sources", "probe-candidates", "--rounds", "2")
    },
    @{
        Name = "Final deterministic verification"
        Args = @("sources", "verify-candidates")
    },
    @{
        Name = "Promote verified candidates"
        Args = @("sources", "promote-verified")
    },
    @{
        Name = "Enable verified sources"
        Args = @("sources", "enable-verified")
    },
    @{
        Name = "Evaluate enabled source health"
        Args = @("sources", "health-all")
    },
    @{
        Name = "Refresh source matrix"
        Args = @("sources", "complete-matrix")
    },
    @{
        Name = "Generate final 525-slot audit"
        Args = @("sources", "audit-525", "--no-seed-registry")
    }
)

if ($DiscoveryOnly) {
    $Stages = @($Stages[0], $Stages[1], $Stages[2])
}

$script:StartedAt = (Get-Date).ToString("o")
$script:CurrentLog = ""
$script:CurrentStage = ""

function Write-RunStatus {
    param(
        [string]$Status,
        [int]$StageIndex,
        [string]$StageName,
        [string]$LogPath = "",
        [int]$ExitCode = 0,
        [string]$ErrorMessage = ""
    )

    $Payload = [ordered]@{
        run_id       = $RunId
        status       = $Status
        stage_index  = $StageIndex
        stage_total  = $Stages.Count
        stage_name   = $StageName
        city_limit   = $CityLimit
        discovery_only = [bool]$DiscoveryOnly
        started_at   = $script:StartedAt
        updated_at   = (Get-Date).ToString("o")
        log_path     = $LogPath
        exit_code    = $ExitCode
        error        = $ErrorMessage
        run_dir      = $RunDir
    }

    $Temporary = "$StatusFile.tmp"

    $Payload |
        ConvertTo-Json -Depth 6 |
        Set-Content -LiteralPath $Temporary -Encoding UTF8

    Move-Item `
        -LiteralPath $Temporary `
        -Destination $StatusFile `
        -Force
}

function Invoke-Stage {
    param(
        [int]$Index,
        [string]$Name,
        [string[]]$Arguments
    )

    $SafeName = $Name -replace '[^A-Za-z0-9_-]', "_"
    $LogPath = Join-Path $LogDir (
        "{0:D2}_{1}.log" -f $Index, $SafeName
    )

    $script:CurrentLog = $LogPath
    $script:CurrentStage = $Name

    Write-RunStatus `
        -Status "RUNNING" `
        -StageIndex $Index `
        -StageName $Name `
        -LogPath $LogPath

    Write-Host ""
    Write-Host "[$Index/$($Stages.Count)] $Name" -ForegroundColor Cyan
    Write-Host "$PolicyDB $($Arguments -join ' ')" -ForegroundColor DarkGray

    $global:LASTEXITCODE = 0

    # Do not turn native output decoding warnings into terminating exceptions.
    & $Python $NativeWrapper --log $LogPath -- $PolicyDB @Arguments

    $Code = $LASTEXITCODE

    if ($null -eq $Code) {
        $Code = 0
    }

    if ($Code -ne 0) {
        $Tail = @(
            Get-Content `
                -LiteralPath $LogPath `
                -Tail 20 `
                -ErrorAction SilentlyContinue
        ) -join "`n"

        throw (
            "$Name failed; exit_code=$Code`n" +
            "Log: $LogPath`n" +
            $Tail
        )
    }

    Write-RunStatus `
        -Status "RUNNING" `
        -StageIndex $Index `
        -StageName "$Name completed" `
        -LogPath $LogPath
}

Write-RunStatus `
    -Status "STARTING" `
    -StageIndex 0 `
    -StageName "Preparing workflow"

try {
    for ($i = 0; $i -lt $Stages.Count; $i++) {
        $Stage = $Stages[$i]

        Invoke-Stage `
            -Index ($i + 1) `
            -Name $Stage.Name `
            -Arguments $Stage.Args
    }

    Write-RunStatus `
        -Status "COMPLETED" `
        -StageIndex $Stages.Count `
        -StageName "Workflow completed" `
        -LogPath $script:CurrentLog

    Write-Host ""
    Write-Host "Workflow completed." -ForegroundColor Green
    Write-Host "Run directory: $RunDir" -ForegroundColor Cyan
}
catch {
    Write-RunStatus `
        -Status "FAILED" `
        -StageIndex ([Math]::Max(1, $i + 1)) `
        -StageName $script:CurrentStage `
        -LogPath $script:CurrentLog `
        -ExitCode 1 `
        -ErrorMessage $_.Exception.Message

    Write-Host ""
    Write-Host $_.Exception.Message -ForegroundColor Red
    exit 1
}

