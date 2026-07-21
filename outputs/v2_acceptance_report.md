# 中国房地产与城市政策研究数据库 V2 验收报告

- 验收日期：2026-07-21
- 工作分支：`feat/policydb-v2`
- 当前项目版本：`0.1.0`
- Schema 版本：`2`
- 结论：V2 代码、兼容迁移、质量视图和本地交互验收通过；来源范围与历史覆盖证据尚未达到正式发布门槛，因此没有把项目版本提前改为 `2.0.0`。

## 1. V1 架构审计结论

V1 已有 Raw → Staging → Curated → DuckDB、Typer CLI、Streamlit、JobManager 独立 worker、人工审核和 Windows 启动器，能够继续作为唯一主干。主要缺口是：没有“城市 × 来源 × 时间窗”的完整扫描证据，重复判断没有统一决策事实，字段置信度缺少字段级证据，来源登记中的职责与城市范围不足。详细审计见 `docs/v2_current_system_audit.md`。

## 2. 删除或合并的冗余

- `data/reference/source_registry.yaml` 成为唯一来源事实；`config/source_registry.yml` 只保留兼容说明，不双写来源。
- 更新计划迁至唯一文件 `data/reference/update_schedule.yaml`；删除重复的 `config/update_schedule.yaml`。
- 旧 crawl/update 命令保留名称，但路由到现有 JobManager 和统一更新服务。
- 没有新增覆盖中心、去重中心、置信度中心等平行顶级页面；现有“数据质量”页内部升级为“覆盖完整性”“增量与去重”“准确性与置信度”“异常与人工复核”四个标签页。
- 没有引入 Redis、PostgreSQL、Celery、Airflow 或第二套验证框架。

## 3. 保留和复用的模块

继续复用 DuckDB、Parquet、Polars、Pydantic、Typer、Streamlit、`CrawlPipeline`、`RespectfulFetcher`、`JobManager`、`GLMEnricher`、`manual_review_tasks`、现有原子工作区/写锁、Windows 一键启动器及 V1 研究视图。V2 仅扩展现有模型、抓取闸门、数据库构建和验证入口。

## 4. 主要新增文件

- Schema 与迁移：`migrations/020_v2_core_schema.sql`、`021_v2_quality_views.sql`、`022_v2_research_views.sql`、`src/policydb/migration_v2.py`。
- 三类判断器：`src/policydb/coverage.py`、`coverage_audit.py`、`confidence.py`、`source_quality.py`。
- 分层更新：`src/policydb/update/v2.py`、`data/reference/update_schedule.yaml`。
- Windows 计划任务：`scripts/install_update_schedule.ps1`、`remove_update_schedule.ps1`、四个 `run_*_update.ps1`。
- 页面：`app/quality_center.py`。
- 文档：`docs/architecture_v2.md`、`migration_v1_to_v2.md`、`due_diligence_protocol.md`、`operations_v2.md`、`dedup_and_versioning.md`、`confidence_methodology.md`、`v2_operator_guide.md`、`CHANGELOG_V2.md`。
- 测试：`tests/test_v2.py`（26 项）。

## 5. 主要修改文件

- `src/policydb/cli.py`、`query/database.py`、`validate/quality.py`、`sources.py`。
- `src/policydb/crawl/models.py`、`registry.py`、`pipeline.py`、`fetcher.py`、`checkpoint.py`、`dedup.py`、`health.py`。
- `src/policydb/enrich/glm.py`。
- `app/dashboard.py`、`README.md`、`docs/data_dictionary.md`、`docs/sources.md`。
- 旧 `scripts/install_update_tasks.ps1`、`remove_update_tasks.ps1` 变为兼容入口。

## 6. Schema 迁移结果

实际执行了 dry-run、apply、verify。最终 `outputs/v2_migration_report.json` 显示：registry version=2、来源 816、缺失字段 0、T1=3,011、日期 2003-06-05 至 2026-07-02、Raw 写入 0。创建的核心事实仅有 `crawl_source_windows`、`dedup_decisions`、`field_confidence`；扩展已有 `crawl_items`、`policy_document_versions`、`llm_extractions`、`llm_verifications`。

## 7. 旧数据保护结果

最近备份为 `outputs/v2_migration_backup/20260721T071802Z/`，包含 DuckDB、来源登记及四个受影响 Parquet 的 SHA-256 清单。备份库与迁移后库的关键计数一致：records 3,568、policy_document_versions 154、manual_review_tasks 7,457、sources 816。迁移函数测试还对 Raw 文件迁移前后 SHA-256 进行相等断言。工作区中既有 Raw 脏文件属于先前抓取结果，本次提交不会重置、覆盖或夹带这些文件。

## 8. 来源登记升级结果

816 个来源已升级到 schema 2，包含 scope、agency、required level、覆盖期、频率、有效期、替代来源、解析器与失败状态。已知 8 个中央域名安全映射为 national，推导出 840 条城市—来源映射。其余 808 个来源因证据不足保持 `scope_type=unknown`，没有根据域名文字猜城市。来源验证无重复 ID、无“非官方 mandatory”违规项。

## 9. 105 城市来源矩阵状态

`v_source_city_matrix` 可查询 840 行；`v_city_month_coverage` 为 10,815 行，即 105 城市 × 2018-01 至 2026-07。当前 10,815 行均为 `not_scanned`，因为还没有完整扫描窗口；确认零政策为 0。`v_city_month_policy_panel_research_ready` 10,815 行，`v_city_year_policy_panel_research_ready` 945 行，错误赋零计数为 0。

## 10. 覆盖完整性算法

覆盖事实按“城市 × 来源 × 时间窗”保存。只有入口成功、分页/日期边界到达、无未解决错误且 `completion_evidence.exhaustive=true` 时，才能得到 `complete_policy_found` 或 `complete_confirmed_zero`。其他状态保持 partial/failed/not_scanned，research-ready 的政策数保持 null。覆盖评分用于诊断，不替代 mandatory 来源、扫描率、例外、抽样召回等硬门槛。

## 11. 去重算法与阈值

L0—L7 依次处理 task key、规范 URL、HTTP 条件请求、二进制 SHA-256、规范正文 SHA-256、政策身份键、SimHash/文本相似版本、GLM 缓存。相同规范正文判定为 L4 duplicate；高相似但金额、比例、日期、年限或数量不同判定为 `material_change`。决定保存 level、score、threshold、rules version 和证据，不删除 Raw。

## 12. 置信度算法

权重为来源权威 0.30、证据覆盖 0.25、跨来源一致 0.20、抽取确定性 0.15、实体匹配 0.10。已生成 10,513 条字段置信度，覆盖 3,568 条记录；当前记录分层为 review 3,011、hold 557、high 0。该结果说明历史证据仍不足，系统没有把旧记录批量自动通过。原人工任务历史仍为 7,457 条，本次没有据此批量新增任务；现有质量视图中的待审核记录为 22 条。

## 13. 日、周、月、季度更新

`policydb update daily|weekly|monthly|quarterly` 以及 `plan/run/status/report` 均使用同一 JobManager。daily 为近 3 天官方增量；weekly 增加补漏；monthly 检查近 40 天缺口并默认暂存；quarterly 复核来源健康和历史缺口。Windows 脚本实际预览了四项任务，退出码 0，默认没有创建系统计划；安装必须显式使用 `-Enable` 并输入 `ENABLE`。

## 14. Streamlit 不阻塞验收

`outputs/acceptance/crawl_responsiveness.json` 的本地 mock 结果：

| 场景 | start 返回 | 总耗时 | health | 最大 health 延迟 | 稳定库未变化 |
|---|---:|---:|---|---:|---|
| 5 URL 暂存 | 0.030s | 2.80s | 全部 200 | 0.055s | 是 |
| 100 URL 暂存 | 0.026s | 4.40s | 全部 200 | 0.059s | 是 |
| 1000 候选/100 抓取 | 0.024s | 4.44s | 全部 200 | 0.042s | 是 |
| 5 URL + GLM mock 复核 | 0.028s | 2.67s | 全部 200 | 0.074s | 是 |

最高 worker RSS 171,352,064 bytes，线程峰值 16；取消任务 0.769s 收敛为 cancelled，工作区保留，健康检查持续 200。浏览器实测数据总览与“覆盖与质量”页面均可切换，控制台 error/warning 为 0；标签重命名后由 Streamlit AppTest 回归。最终复查 `http://127.0.0.1:8501/_stcore/health` 返回 200/ok。

## 15. 去重算力节省结果

真实正式数据中当前 `dedup_decisions` 为 0，因此不能声称已经产生真实 API 费用节省。5 URL mock 首轮生成 5 个文档版本和 5 条决策；相同输入第二轮仍为 5 个文档版本，新增 5 条 `duplicate_content` 决策，没有产生重复版本。GLM 缓存键已包含正文、模型、prompt 和 schema 版本；真实节省量需在下一次 V2 正式抓取后统计。

## 16. 第二次重复运行结果

`tests/test_v2.py::test_five_url_v2_pipeline_writes_auditable_decisions` 实际执行两轮相同 5 URL：版本数 5 → 5，决策数 5 → 10，第二轮 5 条全部为 `duplicate_content`。关键数值冲突的独立测试返回 `material_change`。

## 17. 人工审核回流

既有审核测试覆盖任务生成幂等、分页、状态更新、自动写 `manual_corrections.csv`、两次应用修正幂等及 Raw SHA 不变。自动复核测试验证模型不能独自决定最终状态。V2 置信度构建只生成字段事实与视图，不把 3,568 条低证据记录直接扩张为新的人工任务。

## 18. pytest 结果

最终收集 166 项测试；全量运行 166/166 通过，退出码 0，用时 94.4 秒。V2 专项 26/26 通过；覆盖来源迁移、确认零硬门槛、URL/正文去重、关键数值冲突、GLM 缓存、字段置信度、数据库视图和两轮 5 URL 管线。

## 19. ruff 结果

`uv run ruff check .` 实际退出码 0，输出 `All checks passed!`。

## 20. 尚未解决或未做真实外部验证的限制

1. 808 个来源仍需人工核实城市/省份范围；不能自动猜测。
2. 5 个 required 来源中只有 1 个启用；尚不满足尽职门槛。
3. 完整扫描窗口为 0；月度召回为 `not_evaluated_no_complete_windows`，零政策抽样为 `not_evaluated_no_confirmed_zero_windows`。
4. 尚未在真实政府网站、真实 GLM/Search API 上运行本次 V2 链路；网络抓取成功不能由 mock 代替。
5. 正式 `dedup_decisions` 尚无下一轮真实抓取数据，费用节省仍不可量化。
6. 字段证据中 high 为 0，说明历史记录尚未达到 research-ready 的高置信门槛。

## 21. 是否满足 V2 正式发布条件

不满足。工程实现、迁移、测试、浏览器和本地性能门槛已通过，但来源启用率、808 个范围未决项、完整扫描窗口、召回抽样和真实网络验证尚未通过。根据“不在验收前宣称 V2 完成”的要求，保持版本 `0.1.0`，不打 `2.0.0` 发布标签。建议下一步先人工核实 mandatory 来源与 105 城市范围，然后运行 weekly/monthly 小批量扫描，形成完整窗口后重做覆盖抽样；全部硬门槛通过后再发布 2.0.0。
