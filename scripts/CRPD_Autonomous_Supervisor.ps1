[CmdletBinding()]
param(
    [string]$ProjectRoot = 'D:\Codex\projects\Documents-Codex\2026-07-13\text-20260705-xlsx-text-data-raw\policy-database',
    [string]$DataRoot = 'E:\Data Set\CRPD'
)

$ErrorActionPreference = 'Stop'
$python = Join-Path $ProjectRoot '.venv\Scripts\python.exe'
$controller = Join-Path $ProjectRoot 'scripts\crpd_autonomous_controller.py'
$config = Join-Path $ProjectRoot 'config\crpd_autonomous.json'
if (-not (Test-Path -LiteralPath $python)) { throw "Project virtualenv missing: $python" }
if (-not (Test-Path -LiteralPath $controller)) { throw "Autonomous controller missing: $controller" }
& $python $controller supervisor --project-root $ProjectRoot --data-root $DataRoot --config $config
exit $LASTEXITCODE
