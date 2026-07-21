param([switch]$Enable)
& (Join-Path $PSScriptRoot "install_update_schedule.ps1") -Enable:$Enable
exit $LASTEXITCODE
