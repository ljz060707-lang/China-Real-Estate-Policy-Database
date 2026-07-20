$ErrorActionPreference = "Stop"
$Root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$Target = Join-Path $Root "打开房地产政策数据库.bat"
if (-not (Test-Path -LiteralPath $Target)) { throw "未找到启动文件：$Target" }

$desktop = [Environment]::GetFolderPath("Desktop")
if (-not $desktop) { throw "无法定位当前用户的桌面目录。" }
$shortcutPath = Join-Path $desktop "房地产政策数据库.lnk"
$shell = New-Object -ComObject WScript.Shell
$shortcut = $shell.CreateShortcut($shortcutPath)
$shortcut.TargetPath = $Target
$shortcut.WorkingDirectory = $Root
$shortcut.Description = "打开中国房地产政策数据库"
$shortcut.IconLocation = "$env:SystemRoot\System32\shell32.dll,220"
$shortcut.Save()

Write-Host "[完成] 已创建桌面快捷方式：$shortcutPath" -ForegroundColor Green
exit 0
