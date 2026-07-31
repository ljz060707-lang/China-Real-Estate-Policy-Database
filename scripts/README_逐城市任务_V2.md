# CRPD逐城市历史抓取任务包 V2

## 本次修复

旧版脚本错误地调用：

```powershell
.venv\Scripts\python.exe -m policydb.cli
```

但当前仓库的 `cli.py` 没有模块执行入口。它只在 `pyproject.toml` 中注册了控制台命令：

```toml
policydb = "policydb.cli:app"
```

因此旧版每个月实际上只导入模块后退出，未创建抓取任务，才会在约1—2秒内得到：

```text
status=unknown
candidates=0
fetched=0
documents=0
```

V2改为直接执行：

```text
.venv\Scripts\policydb.exe
```

并增加三项保护：

1. 启动时执行 `policydb --help`；
2. `crawl historical` 必须返回 `job_id`，否则立即停止；
3. `jobs status` 必须返回可解析状态，禁止把空输出记为零政策。

## 清理旧版假运行状态

旧版没有抓取新数据，但生成了错误的状态和日志。先重命名保留：

```powershell
$Stamp = Get-Date -Format "yyyyMMdd_HHmmss"

if (Test-Path "D:\Data Set\CRPD\jobs\city_full_search\CITY_320100") {
    Rename-Item `
      "D:\Data Set\CRPD\jobs\city_full_search\CITY_320100" `
      "CITY_320100_false_run_$Stamp"
}

if (Test-Path "D:\Data Set\CRPD\logs\city_full_search\CITY_320100") {
    Rename-Item `
      "D:\Data Set\CRPD\logs\city_full_search\CITY_320100" `
      "CITY_320100_false_run_$Stamp"
}
```

## 重新测试南京一个月

```powershell
.\scripts\CRPD_Run_One_City_Exhaustive_v2.ps1 `
  -CityName "南京市" `
  -StartDate "2025-01-01" `
  -EndDate "2025-01-31" `
  -SkipAI
```

正常结果必须满足：

```text
job_id 非空
status = completed 或 completed_with_warnings
source_count > 0
candidate_count > 0（该来源和月份存在候选时）
```

确认后再运行2018年至今。

## 南京2018年至今

```powershell
.\scripts\CRPD_Run_One_City_Exhaustive_v2.ps1 `
  -CityName "南京市" `
  -StartDate "2018-01-01" `
  -EndDate "2026-07-31"
```

## 全部105城市

```powershell
.\scripts\CRPD_Run_All_Cities_Sequential_v2.ps1 `
  -StartDate "2018-01-01" `
  -EndDate "2026-07-31" `
  -StartIndex 1 `
  -EndIndex 105 `
  -ContinueAfterCityFailure
```
