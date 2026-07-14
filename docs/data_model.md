# 105城市扩展数据模型

系统继续使用 `records` 作为唯一规范政策实体，不新增平行政策主表。一条政策通过关系表连接多个来源、多个适用城市和多个分类。

| 表 | 主键 | 作用 |
|---|---|---|
| `cities_105` | `city_id` | 版本化的105个大城市抓取范围；与城市能级分离 |
| `policy_applicable_cities` | `policy_applicable_city_id` | 政策—适用城市多对多关系，含区县、匹配证据和审核状态 |
| `source_registry` | `source_id` | Excel种子提取的来源域名、等级、适配器和限速配置 |
| `policy_sources` | `policy_source_id` | 政策—来源多对多关系及canonical标记 |
| `crawl_runs` | `run_id` | 回溯或增量抓取批次 |
| `crawl_items` | `item_id` | URL级任务与断点状态 |
| `crawl_checkpoints` | `checkpoint_id` | 批次断点和处理计数 |
| `fetch_errors` | `error_id` | 可重试/不可重试错误；不删除既有数据 |
| `policy_document_versions` | `document_version_id` | 响应哈希对应的不可变网页/PDF版本 |
| `llm_extractions` | `extraction_id` | content hash对应的GLM结构化输出缓存 |
| `t4_match_candidates` | `t4_match_id` | T4按城市、日期、标题匹配T1的候选、分数、证据和审核状态 |

原Excel契约位于 `config/excel_sheet_map.yaml`。T1的两个“范围”分别映射为
`jurisdiction_raw` 与 `policy_category_raw`，不再使用语义不清的列名。

现有 `staging_excel_cells`、`v_policy_collection_long`、`v_policy_library_summary` 和
`v_city_month_policy_panel` 保持兼容。

T4先使用城市＋日期唯一精确匹配；未匹配行再按规范化标题、城市和日期窗口生成最多3个候选。
模糊候选只进入人工审核，不会自动修改 `policy_features.record_id`。
