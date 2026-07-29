param([switch]$Enable)
$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
$Python = Join-Path $Root ".venv\Scripts\python.exe"
if (-not (Test-Path $Python)) { $Python = Join-Path $Root ".venv-1\Scripts\python.exe" }
if (-not (Test-Path $Python)) { throw "Project Python was not found. Run first_setup.ps1 first." }

$Schedule = @{
  daily = @{ Rule = "DAILY /ST 02:30"; Script = "run_daily_update.ps1" }
  weekly = @{ Rule = "WEEKLY /D SUN /ST 03:00"; Script = "run_weekly_update.ps1" }
  monthly = @{ Rule = "MONTHLY /D 1 /ST 03:30"; Script = "run_monthly_update.ps1" }
  quarterly = @{ Rule = "MONTHLY /D 1 /M JAN,APR,JUL,OCT /ST 04:00"; Script = "run_quarterly_update.ps1" }
}

foreach ($Layer in $Schedule.Keys) {
  $Name = "PolicyDB-V2-$Layer"
  $Runner = Join-Path $PSScriptRoot $Schedule[$Layer].Script
  $Action = "powershell.exe -NoProfile -NonInteractive -ExecutionPolicy Bypass -File `"$Runner`""
  $RuleArgs = $Schedule[$Layer].Rule -split " "
  $TaskArgs = @("/Create", "/F", "/TN", $Name, "/TR", $Action, "/SC") + $RuleArgs
  Write-Host "Preview: schtasks /Create /F /TN '$Name' /TR '$Action' /SC $($Schedule[$Layer].Rule)"
}

if (-not $Enable) {
  Write-Host "Tasks remain disabled. Re-run with -Enable after reviewing the preview."
  exit 0
}

$Answer = Read-Host "Type ENABLE to install these four disabled-by-default schedules"
if ($Answer -ne "ENABLE") { Write-Host "Cancelled."; exit 0 }
foreach ($Layer in $Schedule.Keys) {
  $Name = "PolicyDB-V2-$Layer"
  $Runner = Join-Path $PSScriptRoot $Schedule[$Layer].Script
  $Action = "powershell.exe -NoProfile -NonInteractive -ExecutionPolicy Bypass -File `"$Runner`""
  $RuleArgs = $Schedule[$Layer].Rule -split " "
  $TaskArgs = @("/Create", "/F", "/TN", $Name, "/TR", $Action, "/SC") + $RuleArgs
  & schtasks.exe @TaskArgs
  if ($LASTEXITCODE -ne 0) { throw "schtasks failed for $Name with exit code $LASTEXITCODE" }
}
