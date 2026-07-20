$ErrorActionPreference = "Stop"
$Root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path

function Resolve-Uv {
    $command = Get-Command uv -ErrorAction SilentlyContinue
    if ($null -ne $command) { return $command.Source }
    $candidates = @(
        (Join-Path $HOME ".local\bin\uv.exe"),
        (Join-Path $env:USERPROFILE ".local\bin\uv.exe"),
        (Join-Path $env:LOCALAPPDATA "uv\bin\uv.exe")
    )
    foreach ($candidate in $candidates) {
        if ($candidate -and (Test-Path -LiteralPath $candidate)) { return $candidate }
    }
    return $null
}

Write-Host "========================================" -ForegroundColor DarkMagenta
Write-Host "  中国房地产政策数据库 · 首次安装" -ForegroundColor Magenta
Write-Host "========================================" -ForegroundColor DarkMagenta

Write-Host "[1/6] 检查 Windows PowerShell……" -ForegroundColor Cyan
if ($env:OS -ne "Windows_NT") { throw "一键安装程序仅支持 Windows。" }
if ($PSVersionTable.PSVersion.Major -lt 5) { throw "需要 Windows PowerShell 5.1 或更高版本。" }
Write-Host "[通过] PowerShell $($PSVersionTable.PSVersion)" -ForegroundColor Green

Write-Host "[2/6] 检查 uv……" -ForegroundColor Cyan
$uv = Resolve-Uv
if (-not $uv) {
    Write-Host "[信息] 未检测到 uv，将从 Astral 官方地址安装。" -ForegroundColor Yellow
    try {
        $installer = Invoke-RestMethod -Uri "https://astral.sh/uv/install.ps1"
        Invoke-Expression $installer
    }
    catch {
        throw "uv 安装失败。请检查网络后重试。官方地址：https://astral.sh/uv/install.ps1`n$($_.Exception.Message)"
    }
    $uv = Resolve-Uv
}
if (-not $uv) { throw "uv 安装完成后仍无法定位 uv.exe，请重新打开安装程序。" }
Write-Host "[通过] uv：$uv" -ForegroundColor Green

Write-Host "[3/6] 安装 Python 和项目依赖，这可能需要数分钟……" -ForegroundColor Cyan
Push-Location $Root
try {
    & $uv sync --all-extras
    if ($LASTEXITCODE -ne 0) { throw "uv sync --all-extras 返回错误代码 $LASTEXITCODE" }
}
finally {
    Pop-Location
}
if (-not (Test-Path -LiteralPath (Join-Path $Root ".venv\Scripts\python.exe"))) {
    throw "依赖安装结束，但未找到 .venv\Scripts\python.exe。"
}
Write-Host "[通过] 运行环境安装完成。" -ForegroundColor Green

Write-Host "[4/6] 检查数据库文件……" -ForegroundColor Cyan
$databaseExists = Test-Path -LiteralPath (Join-Path $Root "database\policydb.duckdb")
$curatedExists = Test-Path -LiteralPath (Join-Path $Root "data\curated")
if ($databaseExists -and $curatedExists) {
    Write-Host "[通过] 已发现数据库和 Curated 数据。" -ForegroundColor Green
}
else {
    Write-Host "[提示] 数据库或 Curated 数据尚未准备好。" -ForegroundColor Yellow
    Write-Host "       安装器不会猜测或自动导入文件；网站将打开首次设置向导。"
}

Write-Host "[5/6] 创建桌面快捷方式（可选）……" -ForegroundColor Cyan
$answer = Read-Host "是否创建桌面快捷方式？请输入 Y 或 N"
if ($answer.Trim().ToUpperInvariant() -eq "Y") {
    & (Join-Path $PSScriptRoot "create_desktop_shortcut.ps1")
    if ($LASTEXITCODE -ne 0) { throw "桌面快捷方式创建失败。" }
}
else {
    Write-Host "[跳过] 未创建桌面快捷方式。"
}

Write-Host "[6/6] 启动数据库网站……" -ForegroundColor Cyan
& (Join-Path $PSScriptRoot "launch_dashboard.ps1")
exit $LASTEXITCODE
