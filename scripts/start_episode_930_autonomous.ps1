[CmdletBinding()]
param(
    [string]$ProjectRoot = '',
    [string]$DataRoot = 'E:\Data Set\CRPD',
    [int]$CityLimit = 5,
    [int]$MaxAiCalls = 10,
    [int]$MaxFetches = 30,
    [int]$PollSeconds = 15,
    [int]$MaxCycles = 0
)

$ErrorActionPreference = 'Stop'
if ([string]::IsNullOrWhiteSpace($ProjectRoot)) { $ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path }
$python = Join-Path $ProjectRoot '.venv\Scripts\python.exe'
$output = Join-Path $DataRoot 'outputs\special_projects\2016_930'
$stop = Join-Path $DataRoot 'control\STOP_EPISODE_930'
if (-not (Test-Path -LiteralPath $python)) { throw "Project virtualenv missing: $python" }
if (Test-Path -LiteralPath $stop) { Remove-Item -LiteralPath $stop -Force }
$lock = Join-Path $output '930_AUTORUN.lock'
if (Test-Path -LiteralPath $lock) { throw "930 autorunner already appears active: $lock" }
$stdout = Join-Path $output '930_AUTORUN.stdout.log'
$stderr = Join-Path $output '930_AUTORUN.stderr.log'
$argLine = '-m policydb.episode_930_autorun --output "' + $output + '" --city-limit ' + $CityLimit + ' --max-ai-calls ' + $MaxAiCalls + ' --max-fetches ' + $MaxFetches + ' --poll-seconds ' + $PollSeconds + ' --max-cycles ' + $MaxCycles
$env:POLICYDB_ROOT = $ProjectRoot
$env:CRPD_DATA_ROOT = $DataRoot
$process = Start-Process -FilePath $python -ArgumentList $argLine -WorkingDirectory $ProjectRoot -RedirectStandardOutput $stdout -RedirectStandardError $stderr -WindowStyle Hidden -PassThru
Write-Output ("RUNNER_PID=" + $process.Id)
Write-Output ("STATE=" + (Join-Path $output '930_AUTORUN_STATE.json'))
Write-Output ("CHECK=.\scripts\check_episode_930_autonomous.ps1")
