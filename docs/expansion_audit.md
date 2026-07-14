# 105城市扩展前审计

- 主实体：`records` 3,568条；未建立平行政策表。
- 关系：`record_jurisdictions`、`record_terms`、`record_collections`、`documents`、`policy_versions`。
- 七大体系：党中央国务院、住房城乡建设部门、金融监管部门、自然资源部门、地方政府、风险支持专项、政策跟踪与统计。
- 城市标准化：地名别名＋稳定哈希ID；原“能级划分”保存298条四级属性。
- 来源：原链接保存在records/documents；此前配置仅有少量中央来源模板。
- 审核：DuckDB `manual_review_tasks`＋追加式修正和历史CSV，生成和应用均幂等。
- 发布：Parquet/CSV/Excel/manifest/验证报告，Raw不可反写。
- 可复用：Excel单元格导入、标准化、七大库分类、DuckDB视图、审核中心、Release和Streamlit主题。
- 新增：105城市范围、Excel来源种子提取、政策适用城市、抓取审计/版本、GLM缓存、105城市面板和每日滚动PR。

现有兼容视图 `staging_excel_cells`、`v_policy_collection_long`、
`v_policy_library_summary`、`v_city_month_policy_panel` 均保留。
