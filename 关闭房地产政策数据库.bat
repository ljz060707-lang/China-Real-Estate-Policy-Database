@echo off
chcp 65001 >nul
setlocal
set "ROOT=%~dp0"
set "POWERSHELL=%SystemRoot%\System32\WindowsPowerShell\v1.0\powershell.exe"

if not exist "%POWERSHELL%" (
  echo [错误] 未找到 Windows PowerShell。
  pause
  exit /b 1
)

"%POWERSHELL%" -NoProfile -ExecutionPolicy Bypass -File "%ROOT%scripts\stop_dashboard.ps1"
if errorlevel 1 (
  echo.
  echo [错误] 网站未能安全关闭，请阅读上方提示。
  pause
  exit /b 1
)
echo.
pause
exit /b 0
