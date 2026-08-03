param([Parameter(Mandatory=$true)][string]$RunId)
$ErrorActionPreference = 'Stop'
$repoRoot = (git rev-parse --show-toplevel).Trim()
Set-Location $repoRoot
$env:POLICYDB_ROOT = $repoRoot
$env:CRPD_DATA_ROOT = 'D:\Data Set\CRPD'
$pythonPath = Join-Path $repoRoot '.venv\Scripts\python.exe'
if (-not (Test-Path -LiteralPath $pythonPath)) { $pythonPath = (Get-Command python -ErrorAction Stop).Source }
& $pythonPath -m policydb.autopilot_cli status --run-id $RunId
exit $LASTEXITCODE
