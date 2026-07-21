# 政策强度系统实施前审计

审计时间：2026-07-22（Asia/Shanghai）  
分支：`feat/policydb-v2`  
审计起点提交：`7d666c5`  
包版本 / 数据版本默认值：`0.1.0` / `0.1.0`

## 结论

现有 V2 数据库可以作为强度系统的来源与追溯底座，但尚不存在动作级强度事实表，也不具备正式金标准和完整来源窗口。因此本次只能交付可复现的 **experimental/provisional** 测量管线，不能把输出声明为研究就绪或政策效果。

## 实测清单

| 项目 | 实测结果 | 判定 |
|---|---:|---|
| DuckDB 文件 | 存在，5,255,168 bytes | 可查询 |
| DuckDB 表/视图 | 85 | 已有 V2 结构 |
| records | 3,568 | 时间 2003-06-05—2026-07-02 |
| 主目录 T1 锚点 | 3,011（既有验收） | 保持不变 |
| policy_document_versions | 209 | 版本覆盖明显少于 records |
| field_confidence | 10,513 | 可复用 |
| source_registry（Curated） | 816 | 200 个 enabled，0 个 recommended |
| source_registry（YAML 管理入口） | 42 | 与 Curated 816 的角色不同，不能互相覆盖 |
| RegisteredSource 校验 | 816/816 通过 | 无需迁移或备份修复 |
| cities_105 | 105 | 基准城市范围完整 |
| policy_applicable_cities | 2,289；覆盖 105 城；546 待复核 | 可聚合但需质量标记 |
| crawl_source_windows | 0 | 不得声称完整时间窗或 verified zero |
| dedup_decisions | 0 | 正式数据未形成可审计去重决策 |
| manual_review_tasks | 7,457；其中 pending 7,449 | 不将其自动膨胀为强度任务 |
| llm_extractions | 74（complete 73，failed_validation 1） | 仅为既有结构抽取，不等于强度金标准 |
| llm_verifications | 73 | 可复用缓存框架 |
| full_text 缺失 | 123 | 不评分 |
| full_text 1–999 字符 | 2,392 | 默认 partial/provisional |
| full_text ≥1000 字符 | 1,053 | 仍需来源和结构完整性判定 |
| 现有 `v_policy_intensity_index` | 城市月度 `policy_strength × source_quality` | 与本任务概念不符，保留兼容但不作为新指标 |

## 当前 Schema 缺口

以下必需事实不存在：`policy_actions`、`policy_action_calibrations`、`policy_intensity_scores`、`policy_model_predictions`、`policy_model_decisions`。现有 records 的 `publication_date`、`effective_date`、`expiry_date` 等部分列被推断为 INTEGER，说明旧 Excel 空列的 Parquet 类型仍有漂移；新表必须使用显式 schema，不能依赖全空列推断。

## 来源注册表验证

验证程序逐行读取 `data/curated/source_registry.parquet`，只把 `RegisteredSource.model_fields` 中的字段交给 `RegisteredSource.model_validate`；816 行全部通过。模型额外字段策略为 `extra="allow"`，Curated 中的 `jurisdiction_level`、`agency_name`、`notes`、时间戳等扩展列被保留，没有发生修复写入。详细结果见 `source_registry_validation.json`。

## 测试基线异常

- 收集测试：166 个。
- 初次完整 `pytest` 在 241.6 秒后约 51% 超时。
- 定位后，后半 74 个测试全部通过，但 `tests/test_review.py` 的 module fixture 在复制完整 Curated 目录并重建数据库时单次 setup 消耗 109.85 秒；这是 Windows 文件复制/数据库构建的测试夹具成本，不是测试断言死锁。
- `ruff check .` 基线有 1 个既有错误：`scripts/repair_source_registry_enums.py` 导入未排序。
- scikit-learn、joblib、transformers、torch、sentence-transformers 均未安装。

## 实施保护线

1. Raw 不写入、不覆盖。
2. 既有 `v_policy_intensity_index` 保留兼容；新事实和视图使用明确的新名称。
3. 任何非完整官方正文只写 provisional 结果。
4. 无金标准时训练命令返回 `blocked_missing_gold`，不生成虚构指标。
5. 105 城聚合通过适用城市关系和去重政策实体计算，中央政策不会伪装成 105 条地方发布。

