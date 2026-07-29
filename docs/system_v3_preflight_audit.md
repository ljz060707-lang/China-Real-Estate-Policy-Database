# CRPD V3 预检审计

审计日期：2026-07-23  
审计分支：`fix/unified-policy-system-v3`

## 1. 本地与远端状态

- 当前本地提交：`9ece7ea`。
- 当前工作区已有用户数据变更：`llm_extractions.parquet`、`llm_verifications.parquet`、
  `record_geographies_normalized.parquet`、`record_terms.parquet`、
  `validation_report.json` 和 `policydb.duckdb`。本轮不得覆盖、回退或提交这些文件。
- 仓库没有名为 `origin` 的远端；已有 `main` 与 `policy-database` 两个远端名称。
- 远端拉取因本机 Windows Schannel 凭据错误 `SEC_E_NO_CREDENTIALS` 失败。因此本报告中的
  远端比较基于本地缓存引用；当前提交相对缓存的 `policy-database/main` 为领先 25 个提交。
- 未执行自动推送。

## 2. 已实现且应复用的模块

- Dashboard 已收敛为六个一级入口：数据总览、政策中心、自动更新与完整性、数据质量、
  人工审核、个人设置；`app/dashboard.py` 已是轻量路由。
- 五大政策动作分类、CICC topic 映射、统一动作视图、政策中心查询层和清华紫主题已存在。
- SiliconFlow Provider、模型发现、双轮证据约束审核和旧 GLM 兼容入口已存在。
- 政策实体、文档版本、转载副本、重复簇及审计报告已存在。
- D 盘内容寻址归档、哈希审计和相对路径存储已存在。
- `crawl_source_windows`、105 城市覆盖矩阵及“未扫描不记 0”规则已存在。
- Windows 日/周/月调度、任务工作区、写锁、原子数据库替换和报告框架已存在。
- 政策强度 D1—D8 及研究就绪门槛保留，不在本轮重写。

## 3. 需要修复的真实缺口

1. `CrawlJobRequest` 中的城市、省份和主题没有完整传入 `CrawlPipeline.plan()`；后台执行范围
   可能大于用户选择范围。
2. `CrawlPipeline.plan()` 使用顺序追加后全局截断，前面的来源会优先耗尽总配额，历史扫描
   缺少来源公平性。
3. 列表页 `max_pages` 没有成为正式任务参数；分页达到上限、自然耗尽和循环终止的证据没有
   结构化暴露。
4. 统一动作视图缺少 `legacy_collection`、`source_topic` 以及规范命名的
   `pdf_available`、`full_text_available` 兼容列。
5. CICC 映射缺少验收要求中的 topic inventory 和规范命名的 unmapped 输出文件。
6. CLI 缺少 `crawl health`、`crawl historical`、`archive recover-missing` 以及
   `schedule install/uninstall` 的统一兼容入口。

## 4. 本轮不重复实现

- 不重建 Dashboard、SiliconFlow、AI 审核、归档、去重、覆盖矩阵或政策强度子系统。
- 不重新迁移 Raw/Staging/Curated，不修改 Raw，不覆盖现有 DuckDB。
- 不把媒体线索自动提升为 canonical source。
- 不把分页未耗尽、抓取失败或未扫描窗口记为“无政策”。
- 不在首次验收中启动 105 城市全历史真实抓取或大规模付费 AI。

## 5. 实施边界

本轮采用兼容性增量修复：保留现有字段和 CLI，新增范围参数、配额参数、分页诊断、视图别名
和命令别名；先用 mock/本地 fixture 验证，再决定是否进行小规模真实来源检查。
