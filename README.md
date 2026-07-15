# 中国房地产与城市政策研究数据库

本项目把原始 Excel、标准化实体、论文研究面板和正式发布快照严格分层。原始文件只读保存并登记 SHA-256；所有 Curated 记录都保留来源文件、工作表和原行号。

## 十分钟快速开始

```bash
uv sync --all-extras
```

## 初始化数据库

```bash
uv run policydb init
uv run policydb import-excel "data/raw/seed/【中金不动产与空间服务】政策数据库 20260705.xlsx"
uv run policydb validate
uv run policydb dashboard
```

## 导入政策数据

```bash
uv run policydb import-excel \
"data/raw/seed/政策数据库.xlsx"
```

## 数据验证

```bash
uv run policydb search --keyword "城市更新" --region "武汉市" --from 2020-01-01 --official-only
uv run policydb stats --group-by year,province,topic
uv run policydb export --view v_city_month_policy_panel --format parquet --output outputs/city_month_panel.parquet
uv run policydb release --version 0.1.0
```

## 启动分析平台

```bash
uv run policydb dashboard
# 如果8501端口已占用：uv run policydb dashboard --port 8502
# 在“人工审核中心”完成审核后：
uv run policydb review apply
uv run policydb validate
```

Dashboard 默认使用稳定模式：关闭文件监听与快速中断，限制原生计算线程，查询结果按数据库版本缓存。政策检索最多展示 200 条摘要记录，正文只在选择单条政策后加载。

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

详见 `docs/`。运行 `uv run pytest` 和 `uv run ruff check .` 复核系统。

## GitHub 发布版

GitHub Pages 无法直接运行 Streamlit。本项目已准备为“GitHub 仓库 + Streamlit Community Cloud”部署：

- 云端使用精简的 `requirements.txt`；
- 审核任务分页加载，每页 25 条，避免一次渲染数千条任务；
- `POLICYDB_READ_ONLY=1` 可将公开网站设为只读；
- Raw Excel、审核日志、人工修正和虚拟环境不会进入 GitHub；
- 推送后由 GitHub Actions 自动运行测试与数据验证。

完整步骤见 [GitHub 发布说明](docs/github_deployment.md)。
