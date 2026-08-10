# CRPD Database Interface Repair

## 修复范围

正式库的故障不是 DuckDB 文件位置，而是外部 VIEW 中固化的旧 D 盘 Curated 路径。本轮修复建库源代码并创建私有候选库，未触碰活动 writer 使用的正式库。

## Builder 修复

`src/policydb/query/database.py` 现在：

- 只把非隐藏、合法 SQL 标识符命名的 `*.parquet` 注册为 Curated VIEW；
- 排除 `.name.*.tmp.parquet` 等原子写入中间件，避免 parser/binder 把临时文件当正式表；
- 当 `cities_105.parquet` 缺少可选的 `city_tier_existing`、`city_scale_2020` 字段时，在研究 VIEW 中投影类型明确的 `NULL`，不改写原始 Parquet，也不把缺失伪装为 0。

`src/policydb/query/database_validation.py` 现在提供：

- file exists、read-only connect、代表性查询三层健康检查；
- `duckdb_views()` 旧 D 根扫描；
- candidate 与 E Curated 的计数一致性检查；
- 必需 relation 缺失即失败；
- 同目录临时库构建、验证通过后仅原子替换 candidate；
- candidate 路径不得等于正式库；
- 正式切换五门禁的 fail-closed blocker 计算。

## Candidate 验收

候选库：

`E:\Data Set\CRPD\database\policydb_interface_candidate.duckdb`

结果：

- status：`healthy`
- representative queries：PASS
- VIEW 数：136
- VIEW 中旧 `D:/Data Set/CRPD` 引用：0
- candidate 原子发布：完成
- production database touched：false

| 对象 | Candidate | E Curated | 差异 |
|---|---:|---:|---:|
| records | 3,568 | 3,568 | 0 |
| policy_document_versions | 3,422 | 3,422 | 0 |
| crawl_items | 67,683 | 67,683 | 0 |
| source_sync_state | 22 | 22 | 0 |
| geographies | 3,033 | 3,033 | 0 |
| source slots | 525 | 525 | 0 |

政策列表、政策详情、日期范围、来源汇总、质量汇总和城市汇总查询均通过。

## 正式切换状态

当前 crawler writer 仍活动，因此没有备份/替换正式库。正式库继续被 Dashboard 标记为 `INDEX_REFRESH_PENDING`，政策中心使用 E Curated 只读降级。只有 writer=0、legacy supervisor writer=0、checkpoint safe、candidate PASS、Dashboard smoke PASS 同时成立后，才可备份旧正式库并执行原子切换。
