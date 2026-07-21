$ErrorActionPreference = "Stop"
foreach ($Layer in @("daily", "weekly", "monthly", "quarterly")) {
  schtasks.exe /Delete /F /TN "PolicyDB-V2-$Layer" 2>$null
}
