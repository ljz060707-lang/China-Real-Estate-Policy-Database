[CmdletBinding()]
param(
    [string]$ProjectRoot = '',
    [string]$DataRoot = 'E:\Data Set\CRPD'
)

$ErrorActionPreference = 'Stop'
if ([string]::IsNullOrWhiteSpace($ProjectRoot)) { $ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path }
$python = Join-Path $ProjectRoot '.venv\Scripts\python.exe'
$controller = Join-Path $ProjectRoot 'scripts\crpd_autonomous_controller.py'
$config = Join-Path $ProjectRoot 'config\crpd_autonomous.json'
& $python $controller status --project-root $ProjectRoot --data-root $DataRoot --config $config
exit $LASTEXITCODE
