# 自动更新操作指南

长期任务继续使用现有 JobManager 独立后台进程，不在 Streamlit 主线程运行。Windows 任务计划
程序仅负责按时调用现有 daily/weekly/monthly runner。

```powershell
uv run policydb schedule status
uv run policydb schedule install-windows
uv run policydb schedule install-windows --confirm
uv run policydb schedule run-daily
uv run policydb schedule run-weekly
uv run policydb schedule run-monthly
uv run policydb schedule remove-windows --confirm
```

未带 `--confirm` 的安装/删除命令只预览，不修改系统。每日任务回扫 7 天，周度任务回扫 30 天，
月度任务执行 105 城历史完整性补扫。每次抓取仍由现有 JobManager 保存 request、state、事件、
性能日志和报告。

Dashboard 的“自动更新与完整性”可查看计划状态、覆盖缺口和后台任务，并可启动三个后台任务。
公开只读部署只能查看，不能安装计划或启动更新。
