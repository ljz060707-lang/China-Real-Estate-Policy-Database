param(
  [Parameter(Mandatory=$true)][string]$RunId,
  [ValidateSet('source-completion','source-verification','source-enable','full-readiness','full-crawl','archive','dedup','ai-enrichment','acceptance','source-to-full')]
  [string]$Mode = 'source-to-full',
  [string]$Provider = 'siliconflow',
  [int]$MaxSlots = 3,
  [int]$MaxAiCalls = 3,
  [int]$Concurrency = 1,
  [switch]$AutoFullCrawl
)
$ErrorActionPreference = 'Stop'
$repoRoot = (git rev-parse --show-toplevel).Trim()
Set-Location $repoRoot
$env:POLICYDB_ROOT = $repoRoot
$env:CRPD_DATA_ROOT = 'E:\Data Set\CRPD'
$pythonPath = Join-Path $repoRoot '.venv\Scripts\python.exe'
if (-not (Test-Path -LiteralPath $pythonPath)) { $pythonPath = (Get-Command python -ErrorAction Stop).Source }
$argsList = @('-m','policydb.autopilot_cli','resume','--run-id',$RunId,'--mode',$Mode,'--provider',$Provider,'--max-slots',$MaxSlots,'--max-ai-calls',$MaxAiCalls,'--concurrency',$Concurrency,'--config',(Join-Path $repoRoot 'config\autopilot.yaml'))
if ($AutoFullCrawl) { $argsList += '--auto-full-crawl' }
& $pythonPath @argsList
$autopilotExitCode = $LASTEXITCODE
if ($autopilotExitCode -eq 10) {
  Write-Host '本批来源补全成功；525门槛尚未达到，可继续下一批。'
}
exit $autopilotExitCode
