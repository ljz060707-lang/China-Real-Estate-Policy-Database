# V2 变更记录

## V2 候选版本（尚未发布）

- 在 V1 流程上增加 schema version 2 兼容迁移和可恢复备份；
- 将 `data/reference/source_registry.yaml` 升级为唯一来源登记；
- 新增 `crawl_source_windows`、`dedup_decisions`、`field_confidence` 三张事实表；
- 增加 L0—L7 去重、条件请求、规范正文哈希和版本判断；
- 增加字段证据置信度及 research-ready 视图；
- 增加 105 城市自 2018 年起的覆盖骨架，严格区分确认零与未扫描；
- 将现有“数据质量”页面升级为“覆盖与质量”四个标签页；
- 增加 daily、weekly、monthly、quarterly 后台更新和 Windows 计划任务预览；
- 增加来源、覆盖、去重、置信度、研究和发布验证组。

当前不提升项目版本到 2.0.0：正式来源范围和完整扫描窗口尚未达到发布门槛。详见 `outputs/v2_acceptance_report.md`。
