# 中国房地产与城市政策研究数据库

本项目把原始 Excel、标准化实体、论文研究面板和正式发布快照严格分层。原始文件只读保存并登记 SHA-256；所有 Curated 记录都保留来源文件、工作表和原行号。

当前扩展范围为“中国105个大城市2018年至今房地产政策持续更新数据库”。105城市来自《2020中国人口普查分县资料》，是抓取范围；原四级城市能级仍是独立研究属性。

## 十分钟快速开始

```bash
uv sync --all-extras
uv run policydb init
uv run policydb import-excel "data/raw/seed/【中金不动产与空间服务】政策数据库 20260705.xlsx"
uv run policydb validate
uv run policydb dashboard
```

建立105城市范围和Excel来源注册表：

```bash
uv run policydb sources bootstrap-from-excel "data/raw/seed/【中金不动产与空间服务】政策数据库 20260705.xlsx"
uv run policydb build-city-scope
uv run policydb match-t4
uv run policydb crawl audit --scope large-cities-105
```

按七大政策体系重新生成可查询组织层（不会修改 Raw）：

```bash
uv run policydb organize-collections
uv run policydb validate
```

网页左侧“政策体系”可按七大库和细分类浏览、下载。DuckDB 用户可查询
`v_policy_collection_long`、`v_policy_library_summary`；全部 28 个工作表的原始单元格可通过
`staging_excel_cells` 追溯。

常用命令：

```bash
uv run policydb search --keyword "城市更新" --region "武汉市" --from 2020-01-01 --official-only
uv run policydb stats --group-by year,province,topic
uv run policydb export --view v_city_month_policy_panel --format parquet --output outputs/city_month_panel.parquet
uv run policydb release --version 0.1.0
uv run policydb crawl backfill --scope large-cities-105 --from 2018-01-01 --to today --official-first
uv run policydb crawl update --scope large-cities-105
uv run policydb enrich glm --pending-only
```

人工审核：

```bash
uv run policydb review generate
uv run policydb dashboard
# 在“人工审核中心”完成审核后：
uv run policydb review apply
uv run policydb validate
```

网页审核会自动追加 `data/reference/manual_corrections.csv` 和
`data/logs/review_history.csv`。只有 `review apply` 会把已确认修正应用到 Curated 层；
Raw 数据不会被修改。重复生成任务或重复应用修正都是幂等操作。

Python：

```python
from policydb import PolicyDB
db = PolicyDB.open()
results = db.search(keyword="城市更新", region="武汉市", start_date="2020-01-01")
timeline = db.timeline(region="北京市", topic="限购")
panel = db.research.city_month_panel("2015-01-01", "2026-12-31")
db.export(results, "outputs/search.xlsx")
```

数据库为 `database/policydb.duckdb`，核心表为 `data/curated/*.parquet`。DuckDB 只承担查询与视图，不是唯一存储。外部来源默认禁用，只有审核并启用后才会刷新。

## 数据分层

- `data/raw`：不可变源文件、网页/PDF 快照和哈希。
- `data/staging/excel`：每个工作表一个单元格级 Parquet，包含公式、合并区域、隐藏状态和血缘。
- `data/curated`：统一实体与关系表。
- `data/research`：城市—月份、城市—年份、事件研究数据。
- `data/releases`：不可变发布包。

105城市研究视图为 `v_policy_105_cities`、`v_city_month_policy_panel_105` 和
`v_city_year_policy_panel_105`。网页左侧“105城市”提供城市、省份和年份筛选，不会一次加载政策全文。

详见 `docs/`。运行 `uv run pytest` 和 `uv run ruff check .` 复核系统。

## GitHub 发布版

GitHub Pages 无法直接运行 Streamlit。本项目已准备为“GitHub 仓库 + Streamlit Community Cloud”部署：

- 云端使用精简的 `requirements.txt`；
- 审核任务分页加载，每页 50 条，避免一次渲染 5,000 条任务；
- `POLICYDB_READ_ONLY=1` 可将公开网站设为只读；
- Raw Excel、审核日志、人工修正和虚拟环境不会进入 GitHub；
- 推送后由 GitHub Actions 自动运行测试与数据验证。

完整步骤见 [GitHub 发布说明](docs/github_deployment.md)。
