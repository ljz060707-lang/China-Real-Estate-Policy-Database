# CRPD Storage Consolidation Acceptance

审计运行：`20260810T040408Z`

## 当前结论

`STATUS = WAITING_FOR_SAFE_DATABASE_SWITCH`

Storage Resolver、候选数据库、Dashboard runtime、只读降级、E 盘 pytest temp/cache 和浏览器 smoke 已通过。正式数据库仍由活动 full-backfill writer 保护，因此 Phase B 的正式替换和 D 盘可删除资产清理均未执行。

## 已验收

- D code root：`D:\Codex\projects\Documents-Codex\2026-07-13\text-20260705-xlsx-text-data-raw\policy-database`
- E data root：`E:\Data Set\CRPD`
- candidate DB：healthy，136 VIEW，旧 D external reference 为 0。
- Dashboard：`http://127.0.0.1:8501`，runtime 位于 `E:\Data Set\CRPD\runtime\dashboard`。
- Dashboard 六页浏览器 smoke：通过。
- 政策中心 Curated fallback：关键词筛选和政策详情通过。
- 实时来源：E Curated events + shards + automation JSON，未改为 DuckDB 轮询。
- pytest 默认 temp、cache 和测试日志均位于 E 盘。
- current crawler、autonomous scheduler、source registry 和正式数据库均未被修改。

## 技术验收

- pytest：461 collected / 461 passed，0 failed，0 errors；最终运行 597.6 秒。
- Ruff：`src app tests scripts` 全部通过。
- compileall：`src app scripts` 全部通过。
- `git diff --check`：通过；仅存在 Git 行尾转换提示。
- secret scan：480 个当前 Git 可见文本文件，0 个密钥命中；扫描结果不保存匹配值。
- 生产代码旧 `D:\Data Set\CRPD` 命中：0；历史文档、测试 fixture、兼容脚本和审计记录继续保留并分类。
- 浏览器：六页导航、Curated 降级政策列表、关键词筛选和政策详情均通过，无前端异常。

## 未执行

- 未替换 `E:\Data Set\CRPD\database\policydb.duckdb`。
- 未创建正式库切换前备份；该动作必须紧邻安全切换发生。
- 未清理 `.venv-1`、repo `data`、历史 outputs/cache/test temp。
- 未 commit、未 push。

## Phase B 恢复条件

重新读取真实进程和 checkpoint 后，若五门禁全部通过：

1. 将旧正式库备份到 `E:\Data Set\CRPD\backups\database\policydb_before_interface_repair_<timestamp>.duckdb`；
2. 生成 SHA-256 manifest；
3. 在同目录原子发布已验证 candidate；
4. 重跑数据库接口、Dashboard smoke 和完整测试；
5. 对 D 盘候选资产逐项进行用途、hash/count、mtime 对比后，才允许迁移、quarantine 或删除。

任何 writer、checkpoint 或一致性异常都必须继续保持等待态。
