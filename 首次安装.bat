@echo off
chcp 65001 >nul
setlocal
set "ROOT=%~dp0"
set "POWERSHELL=%SystemRoot%\System32\WindowsPowerShell\v1.0\powershell.exe"

if not exist "%POWERSHELL%" (
  echo [错误] 未找到 Windows PowerShell 5.1 或更高版本。
  pause
  exit /b 1
)

"%POWERSHELL%" -NoProfile -ExecutionPolicy Bypass -File "%ROOT%scripts\first_setup.ps1"
if errorlevel 1 (
  echo.
  echo [错误] 首次安装未完成。请根据上方信息修复后重试。
  pause
  exit /b 1
)
exit /b 0
