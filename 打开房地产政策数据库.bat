@echo off
chcp 65001 >nul
setlocal
set "ROOT=%~dp0"
set "POWERSHELL=%SystemRoot%\System32\WindowsPowerShell\v1.0\powershell.exe"

if not exist "%POWERSHELL%" (
  echo [错误] 未找到 Windows PowerShell，无法启动数据库网站。
  pause
  exit /b 1
)

"%POWERSHELL%" -NoProfile -ExecutionPolicy Bypass -File "%ROOT%scripts\launch_dashboard.ps1"
if errorlevel 1 (
  echo.
  echo [错误] 网站启动失败。请查看 .runtime\dashboard.log。
  pause
  exit /b 1
)
exit /b 0
