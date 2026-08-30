[CmdletBinding()]
param(
    [string]$ProjectRoot = '',
    [string]$DataRoot = 'E:\Data Set\CRPD'
)

$ErrorActionPreference = 'Stop'
if ([string]::IsNullOrWhiteSpace($ProjectRoot)) { $ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path }
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$python = Join-Path $ProjectRoot '.venv\Scripts\python.exe'
if (-not (Test-Path -LiteralPath $python)) { throw "Project virtualenv missing: $python" }
& $python (Join-Path $ProjectRoot 'scripts\audit_episode_930_blockers.py') --data-root $DataRoot
exit $LASTEXITCODE
