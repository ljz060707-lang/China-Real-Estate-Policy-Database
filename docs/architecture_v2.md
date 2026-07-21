# 中国房地产与城市政策研究数据库 V2.0 增量架构

## 原则

V2 不建立平行项目。Parquet 仍是事实存储，DuckDB 仍是查询与研究视图层，Streamlit 仍是普通用户入口。所有自动判断必须可追溯到来源、规则版本和证据；未扫描不能解释为零政策。

## 数据流

```text
来源登记 source_registry.yaml
        ↓
分层更新计划与来源—时间窗任务
        ↓
发现 → 条件请求 → Raw 快照 → 解析 → 文档版本
        ↓                ↓
  crawl_source_windows   dedup_decisions
                                ↓
                     GLM/规则抽取与独立复核
                                ↓
                        field_confidence
                                ↓
                  Curated 原子合并与 DuckDB 重建
                                ↓
             覆盖、质量、研究面板与少量人工兜底
```

## 三个新增事实表

### crawl_source_windows

唯一描述某来源在某时间窗、某扫描方法下完成了什么。它是“确认零政策”的必要证据，不替代 `crawl_runs` 或 `crawl_items`。

### dedup_decisions

记录 L0 任务、L1 URL、L2 条件请求、L3 二进制哈希、L4 文本哈希、L5 政策身份、L6 相似版本、L7 GLM 缓存的输入、阈值、决策和规则版本。

### field_confidence

保存每个字段的值、证据位置、来源权威、证据覆盖、跨来源一致、抽取确定性和实体匹配分量。记录级置信度只在 DuckDB 视图中计算，避免把派生结果反写主数据。

## 兼容策略

- V1 视图原名保留；V2 新增 `*_research_ready` 视图。
- 旧 crawl CLI 保留命令名，内部转到统一更新服务。
- 旧来源字段由 Pydantic 迁移映射读取，写回时使用 schema version 2。
- 所有迁移支持 dry-run、备份、apply、verify；失败时保留原数据库和原 Parquet。
- V2 版本号只在完整验收通过后修改为 2.0.0。

## 实施阶段

1. 增加 schema version 2、三张空事实表和来源登记迁移，验证行数与哈希锚点。
2. 贯通覆盖状态、条件请求、分层去重和 GLM 缓存键。
3. 计算字段置信度并只把真正冲突/低置信问题送入审核。
4. 创建覆盖、质量和 research-ready 视图；验证 105 城市 × 2018 至今骨架。
5. 建立日/周/月/季度更新计划、分组验证和 Windows Task Scheduler 脚本。
6. 在现有“数据质量”位置升级为“覆盖与质量”四个 Tab。
7. 运行迁移、全量测试、浏览器与 Windows 操作验收，生成 `outputs/v2_acceptance_report.md`。

