# 数据字典

| 表 | 字段/主键 | 类型 | 可空 | 来源与用途 |
|---|---|---|---|---|
| records | record_id | string | 否 | 稳定记录主键 |
| records | record_type | enum | 否 | 政策、会议、报告、项目、融资等类型 |
| records | title / title_normalized | string | 是 | 原标题及仅用于匹配的规范化标题 |
| records | record_date / publication_date / issuance_date / effective_date / expiry_date | date | 是 | 分离的日期语义；不确定即空 |
| records | status / direction / official_status | enum | 否 | 状态、方向、官方性 |
| records | summary / full_text | string | 是 | 摘要和原文 |
| records | source_quality | int | 否 | 0–5 来源质量 |
| records | source_file / source_sheet / source_row / import_batch_id | string/int | 否 | 原始血缘 |
| policies | record_id | string | 否 | records 外键 |
| policies | mandatory_strength | int | 是 | 0–3 政策力度 |
| record_jurisdictions | record_id + jurisdiction_id | string | 否 | 地域关系，保留 geography_original |
| jurisdictions | jurisdiction_id | string | 否 | 行政区划实体；代码未知时为空 |
| record_terms | record_id + term_id | string | 否 | 分类、方法、证据、置信度、审核状态 |
| taxonomy_terms | term_id | string | 否 | 受控分类词典 |
| quantitative_measures | measure_id | string | 否 | 金额、比例、期限、数量等定量条款 |
| programme_events | record_id | string | 否 | 白名单、城改、收储、保障房等事件 |
| policy_relations | relation_id | string | 否 | 修订、废止、替代、实施、引用等关系 |
| update_runs | run_id | string | 否 | 幂等更新审计 |
| manual_review_tasks | task_id | string | 否 | 由问题类型、记录、字段和来源位置生成的稳定审核任务主键 |
| manual_review_tasks | record_id | string | 是 | 可为空；T2/T4 单元格任务通过来源位置追溯 |
| manual_review_tasks | review_type | enum | 否 | missing_title、missing_source、invalid_url、low_confidence、unmatched_t4、unexplained_t2、duplicate_record、other |
| manual_review_tasks | field_name / source_sheet / source_cell | string | 是 | 修正目标及来源定位 |
| manual_review_tasks | old_value / suggested_value | string | 是 | 原值与人工输入候选值；不自动猜测 |
| manual_review_tasks | confidence | double | 是 | 原自动分类置信度 |
| manual_review_tasks | status | enum | 否 | pending、approved、corrected、rejected、ignored |
| manual_review_tasks | review_note / evidence_url | string | 是 | 审核说明与证据链接 |
| manual_review_tasks | created_at / updated_at | timestamp | 否 | 任务创建与最近审核时间 |
| source_sheet_collections | source_sheet + collection_code + subcollection_code | string | 否/是 | 28 个工作表的七大库资料归属、映射角色、资料类型与理由 |
| staging_cell_catalog | cell_id | string | 否 | 91,793 个 Staging 单元格的稳定目录主键 |
| staging_cell_catalog | source_sheet / source_cell / source_row / source_column | string/int | 否 | 原工作表、坐标及行列血缘 |
| record_collections | record_id + collection_code + subcollection_code | string | 否/是 | 政策记录与七大库、34 个细分类的多对多关系 |
| record_collections | classification_source / confidence / evidence_excerpt / review_status | string/double | 否 | 自动或来源分类的方法、置信度、证据和审核状态 |
| cities_105 | city_id | string | 否 | `CITY_`加六位行政区代码；105城市范围主键 |
| cities_105 | city_name / province_name / city_code | string | 否 | 标准城市、省份及行政代码 |
| cities_105 | city_tier_existing | string | 是 | 与原298城市四级能级的连接；县级大城市无定义时为空 |
| cities_105 | scope_version / scope_source_name / scope_source_date | string/date | 否 | 105城市名单版本与权威来源 |
| policy_applicable_cities | policy_applicable_city_id | string | 否 | 政策—适用城市关系主键 |
| policy_applicable_cities | record_id / city_id | string | 否 | records和cities_105逻辑外键 |
| policy_applicable_cities | jurisdiction_level / district_name | string | 否/是 | city、district或province及区县原名 |
| policy_applicable_cities | match_method / confidence / needs_review / evidence | string/double/bool | 否 | 地域匹配审计信息 |
| source_registry | source_id | string | 否 | 来源域名稳定主键 |
| source_registry | domain / official_status / priority / crawl_enabled | string/int/bool | 否 | 来源等级和抓取开关 |
| policy_sources | policy_source_id | string | 否 | 一条政策连接一个来源URL的关系主键 |
| policy_sources | record_id / source_id / normalized_url | string | 否 | 政策、来源与规范化URL |
| policy_document_versions | document_version_id | string | 否 | URL任务＋内容SHA-256稳定版本主键 |
| crawl_runs / crawl_items / crawl_checkpoints | *_id | string | 否 | 抓取批次、URL任务和断点审计 |
| fetch_errors | error_id | string | 否 | 抓取失败、可重试状态和错误类型 |
| llm_extractions | extraction_id | string | 否 | content hash＋模型＋提示词/schema版本缓存键 |
| llm_extractions | output_json / confidence / needs_review | string/double/bool | 是 | Pydantic验证后的结构化输出与审核状态 |

其他专题表及全部字段以 `src/policydb/ingest/excel.py` 的显式 schema 为准。所有字段均附来源或由上述确定性规则生成。

`manual_review_tasks` 是 DuckDB 中的持久化工作流表，不属于 Raw 数据。
审核决定同时以追加方式写入 `manual_corrections.csv` 和 `review_history.csv`；应用前会在
`data/curated/history/<UTC时间>/` 保存受影响 Parquet 的历史副本。

`staging_excel_cells` 是全部单元格 Parquet 的统一只读 DuckDB 视图；
`v_information_completeness` 用于验证工作表、单元格和记录归库覆盖，
`v_policy_collection_long` 与 `v_policy_library_summary` 用于网页浏览和研究导出。
