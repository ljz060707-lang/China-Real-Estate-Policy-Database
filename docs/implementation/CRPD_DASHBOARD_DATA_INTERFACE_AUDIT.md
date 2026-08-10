# CRPD Dashboard 数据接口核验

核验时间：2026-08-10（Asia/Shanghai）  
核验方式：只读检查代码、E 盘 Parquet/JSON、E 盘 DuckDB 元数据、运行日志与进程；未修改抓取器、来源注册表、checkpoint、计划任务或数据库。

## 1. 权威数据边界

| 数据域 | 权威位置 | 当前状态 | Dashboard 使用原则 |
|---|---|---|---|
| 正式数据库 | `E:\Data Set\CRPD\database\policydb.duckdb` | 文件存在，但视图仍引用迁移前的 `D:/Data Set/CRPD/curated/*.parquet` | 先做只读可用性检查；不可用时显示“数据库索引待刷新”，不得白屏或把旧值当实时值 |
| Curated 数据 | `E:\Data Set\CRPD\curated` | 正在由抓取进程原子更新 | 实时监控和覆盖统计的主要数据源；只读、按 mtime 缓存、限制列和行数 |
| 自动化状态 | `E:\Data Set\CRPD\automation` | `MASTER_STATE.json` 有心跳；其他状态文件较旧 | 展示自动化控制层状态，并明确文件新鲜度 |
| 全量回溯日志 | `E:\Data Set\CRPD\logs\audited_full_backfill\20260809_180844_ad754dea` | 当前运行福州市 | 仅用于诊断和历史核对，不作为首屏逐次扫描的数据源 |
| Dashboard 作业队列 | `E:\Data Set\CRPD\control\dashboard_jobs` | 业务代码已从 `Settings.data_root` 派生 | 页面不再显示旧 D 盘路径；配置移除硬编码旧路径 |

环境变量当前已指向 E 盘：

- `CRPD_DATA_ROOT=E:\Data Set\CRPD`
- `POLICYDB_DATABASE=E:\Data Set\CRPD\database\policydb.duckdb`
- `POLICYDB_CURATED_ROOT=E:\Data Set\CRPD\curated`
- `POLICYDB_OUTPUT_ROOT=E:\Data Set\CRPD\outputs`

## 2. 运行中任务与安全边界

核验时存在独立全量历史回溯：

- runner PID：`22436`
- `policydb crawl exhaustive-city` PID：`29576`
- Python worker：`49804`、`31344`
- 当前城市：福州市（`CITY_350100`）
- 当前来源角色：自然资源部门
- 当前分片：2018-10-01 至 2018-10-31
- 最新实时事件：`discovering`
- 最新事件时间：2026-08-10 10:45:44（Asia/Shanghai）

本轮 Dashboard 改造不得停止、重启、重新领取或并行写入上述任务。Curated 目录中存在活动原子写入临时文件，这是正常 writer 证据，不得删除。

## 3. 实时进度接口核验

### 3.1 `pipeline_progress_events.parquet`

- 粒度：一行一个 pipeline 事件。
- 关键字段：`event_id`、`batch_id`、`shard_id`、`stage`、`message`、`counts_json`、`created_at`。
- 核验时共 3,159 行，文件持续更新。
- 适用：最新事件、当前分片、事件时间线、心跳新鲜度。
- 限制：事件本身不直接保存城市和来源角色，需按 `shard_id` 关联 `crawl_shards.parquet`。

### 3.2 `crawl_shards.parquet`

- 粒度：一行一个城市×来源×时间分片。
- 核验时共 11,233 行，其中 4,170 个非 pending、7,063 个 pending。
- 累计 fetched/document_versions 均为 33,371，failed 为 105。
- 适用：真实已处理分片数、当前分片范围、HTTP/解析结果、城市轮转进度。
- 限制：pending 行在计划阶段已预生成，因此 `shard_count > 0` 只表示“已规划”，不能证明已执行。

### 3.3 `city_year_progress.parquet`

- 粒度：一行一个城市×年份。
- 核验时共 945 行；文件更新时间停留在上一轮 `rebuild_progress`。
- `overall_completion_pct` 是多个严格门控分项的最小值：来源验证、时段完成、分页耗尽、错误闭合、归档、AI 后处理、去重路由等任一项为 0，结果即为 0。
- 该文件仅在 `rebuild_progress()` 后刷新；当前城市逐月运行期间不会逐事件更新。
- 结论：`overall_completion_pct` 是“严格完整性门控”，不是“实时抓取进度”。页面将它用于实时进度会产生“任务运行、分片存在但百分比为 0”的语义错误。

### 3.4 `city_source_year_progress.parquet`

- 粒度：城市×来源角色×来源×年份。
- 适用：阶段性来源/年份完整性快照。
- 限制：同样由 `rebuild_progress()` 批量生成，不是逐分片实时流。

### 3.5 `source_slot_progress.parquet`

- 粒度：525 个必需来源槽位。
- 核验时：verified=398、enabled=400、enabled_unverified=2。
- 适用：来源覆盖与严格门控审计。
- 限制：不表示当前抓取进度；当前不一致必须展示为健康告警，不得由 Dashboard 自动修复。

## 4. 自动化 JSON 核验

| 文件 | 核验状态 | 解释 |
|---|---|---|
| `MASTER_STATE.json` | `WAIT_CURRENT_RUN`，心跳 2026-08-10 10:18:16 | 自动化控制器正在等待既有全量回溯结束；不能替代 crawler 实时事件 |
| `CURRENT_RUN.json` | `NOT_STARTED`，更新时间较旧 | 与当前独立 backfill runner 不同一状态域，必须标注新鲜度 |
| `COVERAGE_STATE.json` | `UNKNOWN` | 不能把 UNKNOWN 显示为 0% 或已完成 |
| `AI_QUEUE_STATE.json` | `NOT_STARTED` | 当前回溯使用 `--no-run-ai`，符合实际 |
| `PDF_ARCHIVE_STATE.json` | `NOT_STARTED` | 只代表自动化阶段未启动，不代表 PDF 表完全为空 |

## 5. DuckDB 核验

E 盘 DuckDB 可只读打开，包含 133 个表/视图；但查询 `records`、`policy_document_versions`、`crawl_items` 时均因视图仍引用 `D:/Data Set/CRPD/curated/*.parquet` 而失败。数据库文件最后更新时间为 2026-08-01，早于当前 E 盘抓取数据。

结论：

1. Dashboard 不得把“数据库文件可连接”误判为“查询可用”。
2. 必须执行代表性查询健康检查。
3. 正式库不可用时，监控页面继续使用 E 盘 Curated；政策中心显示明确降级状态。
4. 如需可查询的政策中心，只能构建引用 E 盘 Curated 的独立只读派生查询缓存；不得修改当前正式数据库或持有长事务。

## 6. 现有 Dashboard 数据流缺陷

1. `overview_metrics()` 读取旧的 `outputs/fast_bulk_ingest/current_status.json` 或 `outputs/all_cities_since_2018/current_status.json`，未读取当前 `pipeline_progress_events` 与 `MASTER_STATE`。
2. `city_year_coverage()` 根据 records 推导有文档年份，却与“严格 city-year 完整性”同名展示，定义混淆。
3. `app/exhaustive_progress.py` 把 `overall_completion_pct` 直接绘制为城市×年份进度，误导为实时完成度。
4. 页面多次独立读取同一 Parquet，单次刷新没有一致快照。
5. 多个页面存在乱码源码、原始内部枚举、原始 JSON 与内部路径直出。
6. `config/dashboard.yaml` 仍硬编码旧 D 盘 operations queue。
7. Dashboard launcher 的诊断日志仍检查仓库本地数据库/Curated，而不是 Settings 解析后的 E 盘位置。
8. `@st.cache_resource` 长期持有 `PolicyDB` 对象；虽然单次查询使用短只读连接，但正式库视图失效时会造成页面异常。

## 7. 统一指标语义

Dashboard 后续统一使用三类指标：

- 实时抓取进度：来自 events + crawl_shards，回答“现在运行到哪里、完成多少已规划分片”。
- 处理进度：来自 document versions、解析/AI/去重/PDF 状态，回答“抓到的数据处理到哪一步”。
- 覆盖与完整性：来自 525 slots、city/year progress、gaps，回答“是否达到严格可审计完整”。

每个进度指标必须同时携带：`label`、`value`、`numerator`、`denominator`、`status`、`source`、`updated_at`、`definition`。没有可信分母时显示“暂无数据”，不得填 0。

## 8. 页面处置清单

| 现有模块 | 处置 | 原因 |
|---|---|---|
| `app/dashboard.py` | REWRITE | 导航名称、统一快照、数据库降级、刷新策略均需调整 |
| `app/overview.py` | REWRITE | 需要真实实时事件、当前任务与分层指标 |
| `app/policy_center.py` | REWRITE/REUSE QUERY | 保留参数化分页查询思想，修复中文和数据库不可用状态 |
| `app/automation_center.py` | MERGE | 操作队列并入“采集与处理”，去掉原始 JSON/旧路径 |
| `app/exhaustive_progress.py` | MERGE | 矩阵能力保留，但严格门控与实时进度分开 |
| `app/quality_center.py` | REWRITE | 改为 E 盘快照优先，细分质量、覆盖和缺口 |
| `app/review_center.py` | REWRITE/KEEP WRITE GATES | 保留审核写入边界，列表和状态中文化 |
| `app/settings_page.py` | REWRITE | 改为“系统与设置”，默认隐藏敏感路径和技术细节 |
| `app/theme.py` | REWRITE | 改成专业研究工作台视觉系统，降低卡片化和紫色面积 |
| `app/ui.py` | KEEP + HARDEN | 保留 Windows-safe pandas 边界，增加统一格式化和安全结构化显示 |

## 9. 不在本轮修复的生产问题

- 当前 verified/enabled 不一致由 Dashboard 只读展示，不自动更改。
- `city_year_progress` 的严格门控定义保持不变。
- 当前 crawler 的事件写入、重建时机、状态机、并发与 checkpoint 保持不变。
- 正式 E 盘 DuckDB 视图重建不在运行中 crawler 的 Dashboard 改造里执行。

