# CRPD Storage & Data Interface Consolidation Plan

审计运行：`20260810T040408Z`

## 目标

将项目收敛为两类明确职责：

- D 盘仓库：代码、Dashboard 平台、配置、测试、必要文档和当前生产 `.venv`。
- `E:\Data Set\CRPD`：数据库、Curated/Raw、输出、日志、自动化状态、runtime、缓存、临时文件、测试产物和备份。

所有生产路径由 `policydb.settings.Settings` 统一解析。默认只需配置
`CRPD_DATA_ROOT=E:\Data Set\CRPD`，其他路径从该根目录派生。解析优先级为：

1. CLI/显式 `Settings` 字段；
2. 环境变量；
3. `config/storage.yaml`；
4. portable/test 默认目录。

## 安全门禁

本轮采用 Sol-Luna 分工：Sol 固定安全边界与验收证据，Luna 分工作包实施，主线程独立复核。当前 full backfill writer 仍活动，因此只执行 Phase A：

- 只读生产审计；
- Storage Resolver 和接口代码修复；
- 构建私有 candidate DuckDB；
- E 盘 runtime、temp、cache、test artifacts 接线；
- Dashboard 独立重启与只读 smoke；
- 测试和文档。

正式数据库切换仅在以下五项同时满足时允许：

1. current crawler writer = 0；
2. legacy supervisor writer = 0；
3. checkpoint safe；
4. candidate validation PASS；
5. Dashboard smoke PASS。

任一项不满足时，不替换、ALTER 或 VACUUM 正式 DuckDB，也不移动活动 Parquet、checkpoint、生产 `.venv` 或 crawler 脚本。

## 实施工作包

### WP1：统一 Storage Resolver

- 扩展 `Settings` 的 database、curated、raw、research、outputs、logs、automation、control、runtime、cache、temp、test_artifacts、dashboard、backups 和 dashboard_runtime 派生属性。
- 属性访问只解析路径，不隐式创建目录。
- `config/storage.yaml` 只保存 data root，不建立第二套路径系统。

### WP2：DuckDB 接口修复

- 修复 `build_database()` 对隐藏原子临时 Parquet 的误匹配。
- 对当前 Curated 中缺失的可选城市属性使用显式 `NULL` 投影，不改写 Parquet、不伪造值。
- 使用正式 builder 生成 `policydb_interface_candidate.duckdb`。
- 验证 file、connect、代表性查询、E Curated 计数和全部 VIEW 路径。
- 提供无副作用的正式切换五门禁函数。

### WP3：Dashboard 数据与 runtime

- 保留六页中文导航、`DashboardSnapshot`、20 秒局部刷新、mtime cache 和只读降级。
- database health 区分 `NORMAL`、`DATABASE_UPDATING`、`INDEX_REFRESH_PENDING`、`QUERY_UNAVAILABLE`、`CURATED_FALLBACK`。
- 新 runtime 写入 `E:\Data Set\CRPD\runtime\dashboard`；迁移期按文件从 new path 回退读取旧 `.runtime`。

### WP4：测试产物

- pytest 默认 basetemp：`E:\Data Set\CRPD\temp\pytest\pytest-<pid>`。
- pytest cache：`E:\Data Set\CRPD\cache\pytest`。
- 日志与报告：`E:\Data Set\CRPD\test_artifacts\20260810T040408Z`。

### WP5：D 盘轻量化

活动 writer 存在时仅做 inventory。`.venv`、`.venv-1`、repo `data`、历史 outputs、cache 和旧 test temp 均不删除。Phase B 必须先完成用途、hash/count 和 E 盘副本核验；不确定资产进入 quarantine，不直接删除。

## 验收输出

大体量证据位于：

`E:\Data Set\CRPD\outputs\storage_migration\20260810T040408Z`

代码库仅保留本计划、迁移审计、数据库接口修复说明和最终验收说明。
