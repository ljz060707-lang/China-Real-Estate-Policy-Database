[CmdletBinding()]
param(
    [string]$ProjectRoot = 'D:\Codex\projects\Documents-Codex\2026-07-13\text-20260705-xlsx-text-data-raw\policy-database',
    [string]$DataRoot = 'E:\Data Set\CRPD',
    [string]$Stage = 'CRAWL',
    [string]$RunId = ''
)

$ErrorActionPreference = 'Stop'
$python = Join-Path $ProjectRoot '.venv\Scripts\python.exe'
$controller = Join-Path $ProjectRoot 'scripts\crpd_autonomous_controller.py'
$config = Join-Path $ProjectRoot 'config\crpd_autonomous.json'
$arguments = @($controller, 'worker', '--project-root', $ProjectRoot, '--data-root', $DataRoot, '--config', $config, '--stage', $Stage)
if ($RunId) { $arguments += @('--run-id', $RunId) }
& $python @arguments
exit $LASTEXITCODE
