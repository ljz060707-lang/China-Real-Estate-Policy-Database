# 逐城全量扫描

最小任务单元为：

```text
city → source_role → source_id → year → month/shard
```

每个分片持久化候选、分页、抓取、错误、未知日期、跨期拒绝、归档和后处理状态。
命中候选/页面/抓取上限或分页未自然结束时不会认证完整；范围会继续拆为半月、周和日。

```powershell
.\.venv\Scripts\policydb.exe crawl exhaustive-city --city "南京市" --from "2023-02-01" --to "2023-02-28" --no-run-ai
.\.venv\Scripts\policydb.exe crawl exhaustive-all --from "2018-01-01" --to "today" --no-run-ai
.\.venv\Scripts\policydb.exe crawl exhaustive-resume --city "南京市"
.\.venv\Scripts\policydb.exe crawl exhaustive-retry --city "南京市"
```

105城市默认顺序执行，避免并发覆盖Parquet或DuckDB。抓取完成与覆盖完整是两个不同结论。
