@echo off
setlocal
set "ROOT=%~dp0"
set "POWERSHELL=%SystemRoot%\System32\WindowsPowerShell\v1.0\powershell.exe"

if not exist "%POWERSHELL%" (
  echo [ERROR] Windows PowerShell was not found.
  pause
  exit /b 1
)

"%POWERSHELL%" -NoProfile -ExecutionPolicy Bypass -File "%ROOT%scripts\launch_dashboard.ps1"
set "EXIT_CODE=%errorlevel%"
if not "%EXIT_CODE%"=="0" (
  echo.
  echo [ERROR] Website startup failed. The window will remain open.
  echo [LOG] "%ROOT%.runtime\launcher.log"
  pause
)
exit /b %EXIT_CODE%
