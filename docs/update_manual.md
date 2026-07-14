# 更新手册

1. 将文件放入 Raw 对应目录，先计算哈希；不同哈希不得覆盖同名文件。
2. Excel 用 `policydb import-excel`；CSV、URL、PDF 和目录使用 `update.adapters` 的对应适配器。
3. 外部来源须在 `source_registry.yml` 审核后显式启用。
4. 采集顺序为 discover→fetch→snapshot→parse→normalize→validate→publish。
5. 哈希无变化计入 unchanged；变化新增 `policy_versions`，不覆盖历史。
6. 运行 `policydb validate`；失败时保留上一版数据库。
7. 正式版本运行 `policydb release --version X.Y.Z`。

## 人工审核中心

1. 运行 `uv run policydb review generate` 扫描缺失标题、缺失原文、链接问题、低置信分类、T4 未关联、T2 未解释和重复记录。
2. 运行 `uv run policydb dashboard`，在左侧进入“人工审核中心”。按类型和状态筛选，查看来源工作表、单元格和证据链接。
3. 选择“确认正确”“保存修改”“拒绝”或“暂不处理”。系统自动写入修正清单和历史日志，不需手工编辑 CSV。
4. 完成本批审核后运行 `uv run policydb review apply`。该命令只更新 Curated Parquet，并重建 DuckDB；Raw 保持只读。
5. 运行 `uv run policydb validate` 和 `uv run policydb review generate` 复核剩余问题。重复执行不会产生重复任务或重复应用。

## 105 城市持续更新

1. 首次从 Excel 建立来源注册表：`uv run policydb sources bootstrap-from-excel "data/raw/seed/【中金不动产与空间服务】政策数据库 20260705.xlsx"`。
2. 在 `data/reference/source_registry.yaml` 人工检查来源后，仅将确认可抓取的来源设为 `crawl_enabled: true`。
3. 历史回溯：`uv run policydb crawl backfill --scope large-cities-105 --from 2018-01-01 --to today --official-first`。
4. 日常增量：`uv run policydb crawl update --scope large-cities-105`。
5. 配置密钥后处理待提取正文：`uv run policydb enrich glm --pending-only`。未配置密钥不会妨碍抓取和 Raw 快照保存。
6. 重建范围与面板：`uv run policydb build-city-scope`，随后运行 `uv run policydb validate`。
7. 检查覆盖：`uv run policydb crawl audit --scope large-cities-105`；报告位于 `outputs/`。

GitHub 仓库的 Settings → Secrets and variables → Actions 中新增 `GLM_API_KEY`，如需指定模型再新增 `GLM_MODEL`。不要把密钥写入 `.env.example`、工作流文件或运行日志。
