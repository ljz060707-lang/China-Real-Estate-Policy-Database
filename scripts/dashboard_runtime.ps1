param(
    [switch]$Json
)

$ErrorActionPreference = "Stop"
$DashboardProjectRoot = if ($env:POLICYDB_ROOT) {
    (Resolve-Path -LiteralPath $env:POLICYDB_ROOT).Path
}
else {
    (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
}
$DashboardDataRoot = if ($env:CRPD_DATA_ROOT) { $env:CRPD_DATA_ROOT } else { "E:\Data Set\CRPD" }
$DashboardRuntimeRoot = Join-Path $DashboardDataRoot "runtime\dashboard"
$DashboardLegacyRuntimeRoot = Join-Path $DashboardProjectRoot ".runtime"
$DashboardRuntimeFiles = @(
    "dashboard.pid",
    "dashboard.port",
    "dashboard.started",
    "dashboard.process.json",
    "launcher.log",
    "dashboard.log",
    "dashboard.output.log",
    "dashboard.previous.log"
)

function Get-DashboardRuntimeDirectory {
    param([switch]$ForWrite)

    if ($ForWrite) {
        return $DashboardRuntimeRoot
    }
    if (Test-Path -LiteralPath $DashboardRuntimeRoot -PathType Container) {
        return $DashboardRuntimeRoot
    }
    if (Test-Path -LiteralPath $DashboardLegacyRuntimeRoot -PathType Container) {
        return $DashboardLegacyRuntimeRoot
    }
    return $DashboardRuntimeRoot
}

function Get-DashboardRuntimePath {
    param(
        [Parameter(Mandatory = $true)][string]$Name,
        [switch]$ForWrite
    )

    if ($Name -notin $DashboardRuntimeFiles) {
        throw "Unknown dashboard runtime file: $Name"
    }
    $newPath = Join-Path $DashboardRuntimeRoot $Name
    if ($ForWrite -or (Test-Path -LiteralPath $newPath)) {
        return $newPath
    }
    $legacyPath = Join-Path $DashboardLegacyRuntimeRoot $Name
    if (Test-Path -LiteralPath $legacyPath) {
        return $legacyPath
    }
    return $newPath
}

function Ensure-DashboardRuntime {
    New-Item -ItemType Directory -Path (Get-DashboardRuntimeDirectory -ForWrite) -Force | Out-Null
    return Get-DashboardRuntimeDirectory -ForWrite
}

function Get-DashboardRuntimeInfo {
    $selected = Get-DashboardRuntimeDirectory
    $files = @{}
    foreach ($name in $DashboardRuntimeFiles) {
        $newPath = Join-Path $DashboardRuntimeRoot $name
        $legacyPath = Join-Path $DashboardLegacyRuntimeRoot $name
        $selectedPath = if (Test-Path -LiteralPath $newPath) { $newPath } elseif (Test-Path -LiteralPath $legacyPath) { $legacyPath } else { $newPath }
        $files[$name] = @{
            path = $selectedPath
            new_path = $newPath
            legacy_path = $legacyPath
            exists = Test-Path -LiteralPath $selectedPath
        }
    }
    return [ordered]@{
        project_root = $DashboardProjectRoot
        data_root = $DashboardDataRoot
        write_root = $DashboardRuntimeRoot
        legacy_root = $DashboardLegacyRuntimeRoot
        selected_root = $selected
        files = $files
    }
}

if ($MyInvocation.InvocationName -ne ".") {
    if ($Json) {
        Get-DashboardRuntimeInfo | ConvertTo-Json -Depth 6
    }
    else {
        Get-DashboardRuntimeInfo
    }
}
