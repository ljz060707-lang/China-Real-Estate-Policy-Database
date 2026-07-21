param([switch]$Enable)
$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
$Python = Join-Path $Root ".venv\Scripts\python.exe"
if (-not (Test-Path $Python)) { $Python = Join-Path $Root ".venv-1\Scripts\python.exe" }
if (-not (Test-Path $Python)) { throw "Project Python was not found. Run first_setup.ps1 first." }
$Schedule = @{
  daily = "DAILY /ST 02:30"
  weekly = "WEEKLY /D SUN /ST 03:00"
  monthly = "MONTHLY /D 1 /ST 03:30"
  quarterly = "MONTHLY /D 1 /M JAN,APR,JUL,OCT /ST 04:00"
}
foreach ($Layer in $Schedule.Keys) {
  $Name = "PolicyDB-V2-$Layer"
  $Action = "`"$Python`" -m policydb.cli update $Layer"
  $Args = "/Create /F /TN `"$Name`" /TR `"$Action`" /SC " + $Schedule[$Layer]
  if (-not $Enable) { Write-Host "Preview: schtasks $Args"; continue }
  Start-Process schtasks.exe -ArgumentList $Args -Wait -NoNewWindow
}
if (-not $Enable) { Write-Host "Tasks remain disabled. To install: .\scripts\install_update_tasks.ps1 -Enable" }
