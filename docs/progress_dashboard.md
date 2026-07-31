# 全量搜索进度页

Dashboard 新增“全量搜索进度”，只读取小型审计Parquet：

- 105城市与525槽位总览；
- 城市—年份门控完成度矩阵；
- 城市5类来源、逐年进度、月度分片和分页证据；
- 只读候选来源审核表。

CLI 同步提供：

```powershell
.\.venv\Scripts\policydb.exe progress status
.\.venv\Scripts\policydb.exe progress status --city "南京市"
.\.venv\Scripts\policydb.exe progress watch --city "南京市"
.\.venv\Scripts\policydb.exe progress export --format csv
```

页面和CLI共享 `city_year_progress.parquet`，刷新不会触发抓取或扫描完整正文。
