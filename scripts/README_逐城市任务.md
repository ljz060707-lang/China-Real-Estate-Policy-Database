# CRPD逐城市历史抓取任务包

包含三个脚本：

1. `CRPD_Run_One_City_Exhaustive.ps1`
   - 执行一个城市；
   - 2018年至今先按月份分片；
   - 分片达到10000候选或10000抓取上限时自动二分，最小到单日；
   - 每个城市独立保存日志、状态和摘要；
   - 城市完成后运行归档、AI分类、AI复核、分类映射、去重、存量池路由、覆盖和数据库校验。

2. `CRPD_Run_All_Cities_Sequential.ps1`
   - 从仓库的 `data/reference/cities_105.csv` 读取真实105城市；
   - 按城市依次运行，避免多个城市同时写DuckDB或冲击政府网站；
   - 可通过 `StartIndex` 和 `EndIndex` 分批执行；
   - 支持断点续跑。

3. `CRPD_Generate_City_Task_Manifest.ps1`
   - 生成105行城市任务清单；
   - 输出到 `E:\Data Set\CRPD\jobs\city_full_search\city_task_manifest.csv`。

## 安装

把三个 `.ps1` 文件复制到项目：

```text
<项目根目录>\scripts\
```

## 先生成任务清单

```powershell
.\scripts\CRPD_Generate_City_Task_Manifest.ps1
```

## 单独运行南京

```powershell
.\scripts\CRPD_Run_One_City_Exhaustive.ps1 `
  -CityName "南京市" `
  -StartDate "2018-01-01" `
  -EndDate "2026-07-31"
```

## 顺序运行前10个城市

```powershell
.\scripts\CRPD_Run_All_Cities_Sequential.ps1 `
  -StartDate "2018-01-01" `
  -EndDate "2026-07-31" `
  -StartIndex 1 `
  -EndIndex 10 `
  -ContinueAfterCityFailure
```

## 顺序运行全部105城市

```powershell
.\scripts\CRPD_Run_All_Cities_Sequential.ps1 `
  -StartDate "2018-01-01" `
  -EndDate "2026-07-31" `
  -StartIndex 1 `
  -EndIndex 105 `
  -ContinueAfterCityFailure
```

## 重要边界

- 当前未配置搜索API时，只扫描已经登记并启用、且能映射到该城市的来源。
- 每城市任务被拆成多个日期分片，因此不受“整库最多10000条”的限制。
- 单日仍达到10000条时，脚本会标记 `daily_cap_hit`，不能宣称完整，需要继续按 `source_id` 拆分。
- 当前代码只有在覆盖窗口记录分页耗尽、无上限命中、无待处理错误后，才能形成可审计的“全量”证明。
