# 525 来源补全与全量回溯门禁

完整运行的顺序固定为：候选发现 → 页面级入口验证 → `sources promote` → `sources enable` → `sources reconcile` → `sources audit-525` → 网络对比 → 南京 2023-02 试跑 → 105 城市回溯。

正文页、文章页和单篇政策 URL 只能作为证据，不能成为持续采集入口。`entry_eligible` 和历史审核状态不是绕过条件；每次权威复核都会重新执行 URL 入口判定，并撤销不合格候选的 `is_verified` 与 `is_enabled`。

```powershell
.\.venv\Scripts\policydb.exe sources verify-candidates
.\.venv\Scripts\policydb.exe sources reconcile --dry-run
.\.venv\Scripts\policydb.exe sources audit-525 --no-seed-registry
.\.venv\Scripts\policydb.exe network compare --url "https://www.nanjing.gov.cn/"
.\.venv\Scripts\policydb.exe supervisor status
```

只有 `verified_coverage_pct=100`、南京网络路线非 `blocked`，且南京单月没有 `source_incomplete`、`partial_network`、`partial_parser`、`partial_cap` 或待处理后处理计数时，才允许启动全量回溯。`CRPD_Resume_Enabled_Sources_v2_2.ps1` 默认执行这些门禁；`-PartialEnabledSourcesOnly` 只用于明确标注的局部扫描，不能用于全量验收。

监督器每次输出 `E:\Data Set\CRPD\outputs\supervisor\latest.json`。退出码 2 表示仍有来源、网络或分片问题，计划任务应记录告警而不能把该次检查标为成功。
