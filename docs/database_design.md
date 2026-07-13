# 数据库设计

系统采用“通用 records 主表 + policies 专用扩展 + 专题表 + 多对多关系”。Parquet 是事实存储，DuckDB 通过视图读取 Parquet；迁移 PostgreSQL 时可保持主键、枚举和关系结构不变。Raw→Staging→Curated→Research→Release 单向流动，派生层不得反写。

稳定 ID 由日期、地域、标题和规范化 URL 的 SHA-256 构造。内容哈希用于无变化判断，但不单独决定是否重复；版本、转载、解读和同名异地文件须分别处理。

## 七大政策库组织层

七大库是叠加在现有 `records` 和受控词典之上的研究组织层，不替换原数据模型。一条记录可同时归入多个库或细分类，因此分类关系数不能与去重后的政策记录数直接相加。

- `source_sheet_collections`：28 个原工作表的主归属、辅助归属、资料类型和映射理由。
- `staging_cell_catalog`：每个原始单元格的稳定 ID、来源位置及主政策库归属。
- `record_collections`：记录级多标签分类，保存规则来源、置信度、证据片段和审核状态。
- `staging_excel_cells`：DuckDB 对全部单元格级 Staging Parquet 的统一只读视图。

工作表归属是资料编排，不等同于内容判断；记录级关键词归类标记为 `unreviewed`，只有明确的来源表归属标记为 `approved`。派生统计、横版和简版只保留并可查询，不反向制造新的政策事实。

主要视图为 `v_policy_collection_long`、`v_policy_library_summary`、`v_source_collection_coverage` 和 `v_information_completeness`。
