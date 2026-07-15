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

## 自动诊断、恢复和复核

人工审核中心的默认流程已经改为：

```text
已有审核任务
→ 判断切割/解析/附件/动态页面/来源正文/真实缺失
→ 合并短文本与跨页句子、重新解析HTML/PDF/附件
→ 回抓已有URL或检索已启用的官方来源
→ GLM第一次结构化抽取
→ GLM第二次独立证据复核
→ 确定性评分程序决定自动通过或人工兜底
```

日常运行顺序：

```bash
uv run policydb review auto
uv run policydb review recover-sources --limit 20
uv run policydb enrich glm
uv run policydb enrich verify
uv run policydb review auto
uv run policydb validate
```

`review auto` 只处理已有任务，不继续批量制造逐单元格任务。自动处理状态包括
`auto_repaired_segmentation`、`auto_reparsed`、`auto_recovered_official`、
`auto_recovered_secondary` 和 `auto_verified`。综合置信度不足 0.70 或标题、城市、
日期发生关键冲突时才进入 `manual_review_required`；0.70–0.90 进入第二轮自动复核。
模型只能报告拼接关系、结构化字段和原文证据，最终状态由程序规则决定。

### 配置 GLM API

1. 在智谱开放平台创建 API Key，不要把 Key 提交到 GitHub。
2. 在 VS Code 的项目根目录复制 `.env.example` 为 `.env`。
3. 填入：

```dotenv
GLM_API_KEY=你的API_Key
GLM_MODEL=glm-4-flash
```

项目启动时会读取本地 `.env`；该文件已被 Git 忽略。也可以只在当前 PowerShell
窗口设置：

```powershell
$env:GLM_API_KEY="你的API_Key"
$env:GLM_MODEL="glm-4-flash"
```

先运行 `policydb enrich glm` 产生第一次抽取，再运行 `policydb enrich verify` 进行独立
复核。每次调用按正文 SHA-256、模型、提示词版本缓存，重复运行不会重复付费。没有
Key 时系统只记录 `awaiting_api_key`，不会伪造抽取结果。

### 官方来源抓取与恢复

来源注册表位于 `data/reference/source_registry.yaml`。所有来源默认关闭；审核域名、
robots.txt 和访问频率后，把需要使用的来源设为：

```yaml
crawl_enabled: true
seed_urls: ["https://example.gov.cn/policy/list"]
list_page_urls: ["https://example.gov.cn/policy/list"]
search_url_template: "https://example.gov.cn/search?q={keyword}"
priority: 0
rate_limit: 0.5
```

搜索模板可使用 `{title}`、`{keyword}`、`{document_number}`、`{region}`、`{year}`。
抓取器先遵守 robots.txt 和限速，保存不可变 Raw 快照及元数据，再解析正文和附件，
按哈希建立版本；失败不会破坏旧数据。恢复候选按标题、文号、机关、日期、地区、正文
相似度和官方域名评分。高置信官方候选才自动建立来源关系，冲突候选留给人工。

```bash
uv run policydb crawl update
uv run policydb crawl audit
uv run policydb review recover-sources --limit 50
```

### 地区标准化与地图

```bash
uv run policydb normalize-geography
uv run policydb build-database
```

标准化层同时保留原地名和省/地级市/区县层级，统一“广州、广州市、广东省广州市”，
并单独识别县级市。地区面板提供全国、省级、地级市排名和月度趋势，查询在 DuckDB
侧聚合并分页，不读取政策正文。地图使用天地图官方 JS API；在 `.env` 配置：

```dotenv
TIANDITU_TOKEN=你的天地图Key
TIANDITU_MAP_APPROVAL=GS（2024）0568号
TIANDITU_QUALIFICATION=甲测资字1100471
```

审图号应按实际使用的天地图底图版本更新；没有 Key 时地图页给出提示，排名和趋势仍可用。

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
