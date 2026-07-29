$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
$Python = Join-Path $Root ".venv\Scripts\python.exe"
if (-not (Test-Path $Python)) { $Python = Join-Path $Root ".venv-1\Scripts\python.exe" }
if (-not (Test-Path $Python)) { throw "Project Python was not found." }
$LogDir = Join-Path $Root "data\logs\scheduled_updates"
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
$Log = Join-Path $LogDir "daily.log"
& $Python -m policydb.cli update daily *>> $Log
exit $LASTEXITCODE
