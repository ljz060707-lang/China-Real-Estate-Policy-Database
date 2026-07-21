# V2 日常运维

唯一来源登记为 `data/reference/source_registry.yaml`，唯一更新配置为 `data/reference/update_schedule.yaml`。普通更新统一进入现有 JobManager 后台进程，不在 Streamlit 主线程运行。

```powershell
uv run policydb update plan --mode daily
uv run policydb update run --mode daily
uv run policydb update status
uv run policydb update report --run-id <RUN_OR_JOB_ID>
```

四层更新策略：daily 检查最近窗口与条件请求；weekly 增加公报、站内搜索和附件补漏；monthly 审计覆盖缺口并只补失败窗口；quarterly 复核 mandatory 来源、网站改版和历史收敛。没有新增信息时不得全量重建 DuckDB。

Windows 计划任务默认只预览：

```powershell
powershell -ExecutionPolicy Bypass -File scripts/install_update_schedule.ps1
```

确认预览后使用 `-Enable`，并在交互提示中输入 `ENABLE`。四个运行脚本自动选择 `.venv` 或 `.venv-1`，日志写入 `data/logs/scheduled_updates/`，命令行不携带 API Key。

更多迁移、覆盖、置信度和页面操作见 [V2 运维与研究使用指南](v2_operator_guide.md)。
