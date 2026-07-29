# 自动更新指南

Dashboard 长任务由独立 JobManager 工作进程执行。正式自动化使用 Windows Task Scheduler，
复用任务工作区、写锁、原子 Parquet 合并、临时 DuckDB 验证和原子替换。

```powershell
uv run policydb schedule status
uv run policydb schedule install
uv run policydb schedule install --confirm
uv run policydb schedule run-daily
uv run policydb schedule run-weekly
uv run policydb schedule run-monthly
uv run policydb schedule uninstall --confirm
```

每日增量使用重叠窗口；周度重试失败并补缺；月度检查上月分页完整性。失败任务不得破坏稳定
Curated 或 DuckDB。未带 `--confirm` 的安装/删除只预览，不修改系统计划。
