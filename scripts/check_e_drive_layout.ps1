[CmdletBinding()]
param(
    [string]$ProjectRoot = '',
    [string]$DataRoot = 'E:\Data Set\CRPD'
)

$ErrorActionPreference = 'Stop'
if ([string]::IsNullOrWhiteSpace($ProjectRoot)) {
    $ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
}

$required = @(
    $ProjectRoot,
    (Join-Path $ProjectRoot '.git'),
    (Join-Path $ProjectRoot '.venv\Scripts\python.exe'),
    (Join-Path $ProjectRoot 'src'),
    (Join-Path $ProjectRoot 'app'),
    (Join-Path $ProjectRoot 'scripts'),
    (Join-Path $ProjectRoot 'config'),
    (Join-Path $ProjectRoot 'tests'),
    (Join-Path $DataRoot 'database'),
    (Join-Path $DataRoot 'outputs'),
    (Join-Path $DataRoot 'control')
)

$missing = @($required | Where-Object { -not (Test-Path -LiteralPath $_) })
$dataLinks = @{}
foreach ($name in @('curated', 'raw', 'staging')) {
    $path = Join-Path (Join-Path $ProjectRoot 'data') $name
    $item = Get-Item -LiteralPath $path -Force -ErrorAction SilentlyContinue
    $dataLinks[$name] = [ordered]@{
        exists = ($null -ne $item)
        link_type = if ($item) { [string]$item.LinkType } else { $null }
        target = if ($item) { [string]$item.Target } else { $null }
    }
}

$dotenvPresent = Test-Path -LiteralPath (Join-Path $ProjectRoot '.env')
$result = [ordered]@{
    project_root = $ProjectRoot
    data_root = $DataRoot
    git_head = (& git -C $ProjectRoot rev-parse HEAD 2>$null).Trim()
    git_dirty_entries = @(& git -C $ProjectRoot status --porcelain=v1 2>$null).Count
    dotenv_present = $dotenvPresent
    missing_paths = $missing
    data_links = $dataLinks
    status = if ($missing.Count -eq 0 -and -not $dotenvPresent) { 'PASS' } else { 'FAIL' }
}
$result | ConvertTo-Json -Depth 8
if ($missing.Count -gt 0 -or $dotenvPresent) { exit 1 }
exit 0
