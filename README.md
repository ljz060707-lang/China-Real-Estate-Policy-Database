# 🏙️ 中国房地产与城市政策研究数据库

### China Real Estate & Urban Policy Research Database

面向房地产政策检索、动态监测、政策量化与因果推断研究的  
**可追溯、可持续更新、可复现的数据基础设施**

[![Python](https://img.shields.io/badge/Python-3.12+-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![DuckDB](https://img.shields.io/badge/DuckDB-Analytical_DB-FFF000?logo=duckdb&logoColor=black)](https://duckdb.org/)
[![Parquet](https://img.shields.io/badge/Storage-Parquet-50ABF1)](https://parquet.apache.org/)
[![Streamlit](https://img.shields.io/badge/Dashboard-Streamlit-FF4B4B?logo=streamlit&logoColor=white)](https://streamlit.io/)
[![License](https://img.shields.io/badge/License-Research_Use-lightgrey)](#使用与引用)
[![Status](https://img.shields.io/badge/Version-V1.0-success)](#项目进展)

</div>

---

## 项目简介

本项目将分散于 Excel 台账、政府网站、政策正文、PDF附件和新闻转载中的中国房地产政策，转化为一套可以持续更新和用于实证研究的标准化数据库。

数据库严格区分：

- 原始政策文件；
- 网页和附件快照；
- 标准化政策实体；
- 政策来源与适用地区；
- 政策分类与方向；
- 城市—月份、城市—年份研究面板；
- 正式发布版本。

所有原始数据均只读保存并登记 SHA-256。标准化记录保留来源文件、工作表、原始行号、网页链接和抓取批次，确保每条政策均可追溯、可审核、可重新生成。

---

## 核心能力

<table>
<tr>
<td width="50%">

### 📚 历史政策数据库

- 原始 Excel 保真导入
- 28个工作表完整血缘
- 七大政策体系组织
- T1、T4及专题库关联
- 政策标题、日期、地区标准化

</td>
<td width="50%">

### 🤖 AI辅助政策解析

- GLM结构化字段抽取
- 政策摘要与分类
- 政策方向与适用对象识别
- 第二轮独立证据复核
- 按正文哈希缓存，避免重复调用

</td>
</tr>

<tr>
<td width="50%">

### 🌐 持续抓取与来源恢复

- 官方来源注册表
- 历史政策回溯
- 最近7天增量更新
- HTML、PDF和附件保存
- robots.txt、限速和断点续跑

</td>
<td width="50%">

### 🧪 自动审核与质量控制

- 语义切割与真实缺失识别
- HTML/PDF重新解析
- 官方来源自动回溯
- 双模型交叉复核
- 仅保留少量人工兜底任务

</td>
</tr>

<tr>
<td width="50%">

### 🗺️ 地区与城市面板

- 105个大城市研究范围
- 省、市、区县层级标准化
- 城市别名统一
- 城市—月份政策面板
- 地区排名、趋势与地图展示

</td>
<td width="50%">

### 📊 研究与数据发布

- DuckDB查询视图
- Parquet核心存储
- Excel兼容导出
- 研究面板导出
- 不可变版本化发布

</td>
</tr>
</table>

---

# 🚀 一次运行完成：首次构建主干

## 1. 准备环境

要求：

- Python 3.12及以上
- Git
- uv

安装 uv：

```powershell
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
````

克隆项目：

```powershell
git clone https://github.com/ljz060707-lang/China-Real-Estate-Policy-Database.git
cd China-Real-Estate-Policy-Database
```

安装全部依赖：

```powershell
uv sync --all-extras
```

将原始政策 Excel 放入：

```text
data/raw/seed/
```

推荐文件名：

```text
【中金不动产与空间服务】政策数据库 20260705.xlsx
```

---

## 2. 首次完整构建

下面是一条可以从原始 Excel 一直运行到 Dashboard 的完整主干。

### PowerShell

```powershell
$Workbook = "data/raw/seed/【中金不动产与空间服务】政策数据库 20260705.xlsx"

uv run policydb init
uv run policydb import-excel $Workbook
uv run policydb organize-collections
uv run policydb build-city-scope
uv run policydb normalize-geography
uv run policydb match-t4
uv run policydb sources bootstrap-from-excel $Workbook
uv run policydb build-database
uv run policydb review generate
uv run policydb review auto
uv run policydb validate
uv run policydb dashboard
```

### Bash

```bash
WORKBOOK="data/raw/seed/【中金不动产与空间服务】政策数据库 20260705.xlsx"

uv run policydb init &&
uv run policydb import-excel "$WORKBOOK" &&
uv run policydb organize-collections &&
uv run policydb build-city-scope &&
uv run policydb normalize-geography &&
uv run policydb match-t4 &&
uv run policydb sources bootstrap-from-excel "$WORKBOOK" &&
uv run policydb build-database &&
uv run policydb review generate &&
uv run policydb review auto &&
uv run policydb validate &&
uv run policydb dashboard
```

启动成功后访问：

```text
http://127.0.0.1:8501
```

端口被占用时：

```powershell
uv run policydb dashboard --port 8502
```

---

## 主干流程说明

```text
原始 Excel
    ↓
Raw：只读保存与SHA-256登记
    ↓
Staging：工作表和单元格级解析
    ↓
Curated：政策实体、来源、地区与分类标准化
    ↓
自动诊断与初步修复
    ↓
DuckDB查询视图
    ↓
Research：城市—月份与城市—年份面板
    ↓
Streamlit Dashboard
```

首次主干运行不要求配置 GLM API，也不要求开启外部网站抓取。

未配置 API 时：

* 历史 Excel 仍可正常导入；
* 政策库仍可正常查询；
* 地区面板仍可生成；
* 待解析任务会记录为 `awaiting_api_key`；
* 系统不会伪造模型结果。

---

# 🌿 可选分支一：配置 GLM API

GLM用于：

* 判断政策相关性；
* 提取政策结构化字段；
* 生成政策摘要；
* 识别政策分类与方向；
* 判断适用地区和适用对象；
* 对第一次抽取进行第二轮独立复核。

模型不会替代官方来源、发布日期和行政区划等确定性事实。

## 1. 创建本地配置

复制示例文件：

```powershell
Copy-Item .env.example .env
```

在 `.env` 中填写：

```dotenv
GLM_API_KEY=你的智谱API_Key
GLM_MODEL=glm-4-flash
```

`.env` 已被 Git 忽略，不要把真实密钥写入：

* README；
* Python代码；
* YAML配置；
* 测试文件；
* Git提交；
* Codex提示词。

也可以仅在当前 PowerShell 窗口设置：

```powershell
$env:GLM_API_KEY="你的API_Key"
$env:GLM_MODEL="glm-4-flash"
```

## 2. 执行第一次结构化抽取

```powershell
uv run policydb enrich glm
```

## 3. 执行第二次独立复核

```powershell
uv run policydb enrich verify
```

## 4. 自动更新审核状态

```powershell
uv run policydb review auto
uv run policydb validate
```

完整 GLM 分支：

```powershell
uv run policydb enrich glm
uv run policydb enrich verify
uv run policydb review auto
uv run policydb build-database
uv run policydb validate
```

模型结果按以下组合缓存：

```text
正文 SHA-256
＋ 模型名称
＋ Prompt版本
＋ Schema版本
```

同一正文重复运行不会重复调用模型。

---

# 🌿 可选分支二：启用官方来源抓取

来源注册表位于：

```text
data/reference/source_registry.yaml
```

为了避免对政府网站造成不必要访问，所有外部来源默认关闭。

## 1. 从原始 Excel 提取历史来源

```powershell
uv run policydb sources bootstrap-from-excel `
  "data/raw/seed/【中金不动产与空间服务】政策数据库 20260705.xlsx"
```

## 2. 审核并启用来源

示例：

```yaml
source_id: example_city_housing
source_name: 示例市住房和城乡建设局
domain: example.gov.cn
source_type: official
official_status: official
crawl_enabled: true

seed_urls:
  - https://example.gov.cn/policy/list

list_page_urls:
  - https://example.gov.cn/policy/list

search_url_template: https://example.gov.cn/search?q={keyword}

priority: 0
rate_limit: 0.5
```

搜索模板支持：

```text
{title}
{keyword}
{document_number}
{region}
{year}
```

## 3. 历史回溯

默认覆盖：

```text
105个大城市 × 2018年至今
```

运行：

```powershell
uv run policydb crawl backfill `
  --scope large-cities-105 `
  --from 2018-01-01 `
  --to today `
  --official-first
```

## 4. 日常增量更新

增量任务默认检查最近7天：

```powershell
uv run policydb crawl update --scope large-cities-105
```

## 5. 抓取审计

```powershell
uv run policydb crawl audit --scope large-cities-105
```

抓取流程：

```text
来源注册表
    ↓
列表页或搜索入口
    ↓
网页、PDF和附件抓取
    ↓
不可变Raw快照
    ↓
正文解析与内容哈希
    ↓
版本识别与去重
    ↓
Curated候选记录
```

---

# 🌿 可选分支三：自动复核与缺失恢复

系统默认采用：

```text
自动诊断
→ 自动修复
→ 自动来源恢复
→ GLM第一次抽取
→ GLM第二次复核
→ 确定性程序决定状态
→ 少量人工兜底
```

## 日常自动复核主干

```powershell
uv run policydb review auto
uv run policydb review recover-sources --limit 50
uv run policydb enrich glm
uv run policydb enrich verify
uv run policydb review auto
uv run policydb validate
```

## 自动识别的问题类型

* 语义切割错误；
* HTML正文解析错误；
* PDF跨页断句；
* 附件未抓取；
* 动态网页正文缺失；
* 来源网页失效；
* 真实字段缺失；
* 标题、地区或日期冲突；
* 重复政策；
* T4与T1匹配异常。

## 自动状态

```text
auto_repaired_segmentation
auto_reparsed
auto_recovered_official
auto_recovered_secondary
auto_verified
manual_review_required
```

处理规则：

* 综合置信度不低于0.90：自动通过；
* 0.70—0.90：进入第二轮自动复核；
* 低于0.70：进入人工兜底；
* 标题、地区或日期发生关键冲突：进入人工兜底。

模型只提供结构化字段、拼接关系和证据片段，最终审核状态由确定性程序决定。

---

# 🌿 可选分支四：地区标准化、105城市与地图

## 构建105城市范围

```powershell
uv run policydb build-city-scope
```

## 标准化地区名称

```powershell
uv run policydb normalize-geography
```

系统同时保留：

* 原始地区名称；
* 省级名称；
* 地级市名称；
* 区县名称；
* 行政层级；
* 父级城市关系；
* 城市别名；
* 是否属于105个大城市。

例如：

```text
广州
广州市
广东省广州市
```

会关联至同一标准城市实体。

县级市不会被错误识别为普通地级市。

## 重建地区面板

```powershell
uv run policydb normalize-geography
uv run policydb build-city-scope
uv run policydb build-database
uv run policydb validate
uv run policydb dashboard
```

地区面板提供：

* 全国政策分布；
* 省级排名；
* 地级市排名；
* 城市月度趋势；
* 政策类型分布；
* 官方来源比例；
* 105城市研究面板。

查询在 DuckDB 侧聚合和分页，不会一次加载全部政策正文。

## 配置天地图

在 `.env` 中加入：

```dotenv
TIANDITU_TOKEN=你的天地图Key
TIANDITU_MAP_APPROVAL=你的实际审图号
TIANDITU_QUALIFICATION=你的测绘资质信息
```

未配置天地图 Key 时：

* 地区排名仍可使用；
* 月度趋势仍可使用；
* 数据表仍可使用；
* 地图区域显示配置提示。

---

# 🌿 可选分支五：政策查询与研究面板

## 命令行查询

```powershell
uv run policydb search `
  --keyword "城市更新" `
  --region "武汉市" `
  --from 2020-01-01 `
  --official-only
```

## 分类统计

```powershell
uv run policydb stats --group-by year,province,topic
```

## 导出城市—月份面板

```powershell
uv run policydb export `
  --view v_city_month_policy_panel `
  --format parquet `
  --output outputs/city_month_panel.parquet
```

## Python接口

```python
from policydb import PolicyDB

db = PolicyDB.open()

results = db.search(
    keyword="城市更新",
    region="武汉市",
    start_date="2020-01-01",
)

timeline = db.timeline(
    region="北京市",
    topic="限购",
)

panel = db.research.city_month_panel(
    "2015-01-01",
    "2026-12-31",
)

db.export(
    results,
    "outputs/search.xlsx",
)
```

---

# 🌿 可选分支六：导出兼容Excel

数据库不会直接覆盖原始 Excel。

可以基于原始模板生成新的发布工作簿：

```powershell
uv run policydb export-excel `
  --template "data/raw/seed/【中金不动产与空间服务】政策数据库 20260705.xlsx" `
  --output "data/releases/policy_database_latest.xlsx"
```

导出的工作簿用于：

* 保留原工作表结构；
* 兼容既有研究习惯；
* 更新政策目录和专题表；
* 建立旧行号与标准政策ID之间的映射。

---

# 🌿 可选分支七：版本发布

运行完整验证：

```powershell
uv run policydb validate
```

创建正式发布版本：

```powershell
uv run policydb release --version 0.1.0
```

正式版本写入：

```text
data/releases/
```

Release层用于保存：

* 数据版本说明；
* Parquet快照；
* 数据字典；
* 校验报告；
* 研究面板；
* Excel兼容发布文件。

---

# 🧱 数据架构

```text
data/
├── raw/
│   ├── seed/               原始Excel
│   ├── documents/          原始政策文件与附件
│   ├── webpages/           网页快照
│   └── snapshots/          版本化原始快照
│
├── staging/
│   └── excel/              单元格级Parquet与数据血缘
│
├── curated/                标准化政策实体与关系表
│
├── research/               城市—月份、城市—年份和事件研究数据
│
├── reference/              城市、来源、分类、人工修正等配置
│
├── logs/                   抓取、审核和运行日志
│
└── releases/               不可变正式发布包

database/
└── policydb.duckdb         查询数据库与研究视图

outputs/
└── 查询结果、报告与临时导出
```

---

# 🔎 数据处理原则

## 原始数据不可变

Raw层只允许新增，不允许覆盖。

## 每条记录均可追溯

Curated记录保留：

* 来源文件；
* 工作表；
* 原始行号；
* 原始URL；
* 抓取时间；
* 内容哈希；
* 解析版本；
* 模型版本；
* 审核状态。

## DuckDB不是唯一存储

核心数据保存为：

```text
data/curated/*.parquet
```

DuckDB主要承担：

* 查询；
* 聚合；
* 研究视图；
* Dashboard数据读取。

## AI不能替代事实字段

以下字段优先来自官方页面和确定性规则：

* 政策标题；
* 发布时间；
* 发布机构；
* 文件文号；
* 官方状态；
* 原始链接；
* 行政区划。

AI主要用于：

* 语义分类；
* 摘要；
* 方向识别；
* 适用对象识别；
* 证据复核。

---

# 🧰 常用工作流速查

## 首次完整构建

```powershell
uv run policydb init
uv run policydb import-excel "data/raw/seed/政策数据库.xlsx"
uv run policydb organize-collections
uv run policydb build-city-scope
uv run policydb normalize-geography
uv run policydb match-t4
uv run policydb build-database
uv run policydb validate
uv run policydb dashboard
```

## 每日智能更新

```powershell
uv run policydb crawl update
uv run policydb review recover-sources --limit 50
uv run policydb enrich glm
uv run policydb enrich verify
uv run policydb review auto
uv run policydb build-database
uv run policydb validate
```

## 仅重新构建地区面板

```powershell
uv run policydb normalize-geography
uv run policydb build-city-scope
uv run policydb build-database
uv run policydb validate
```

## 仅运行自动审核

```powershell
uv run policydb review auto
```

## 仅诊断，不写入修复

```powershell
uv run policydb review auto --dry-run
```

## 运行测试

```powershell
uv run pytest
uv run ruff check .
```

---

# ⚙️ GitHub Actions与云端部署

项目支持：

```text
GitHub仓库
＋ GitHub Actions
＋ Streamlit Community Cloud
```

GitHub Actions用于：

* 自动测试；
* 数据验证；
* 定时增量抓取；
* GLM待处理任务解析；
* 研究面板重建；
* 更新报告生成。

在 GitHub 中配置 API Key：

```text
Repository
→ Settings
→ Secrets and variables
→ Actions
→ New repository secret
```

Secret名称：

```text
GLM_API_KEY
```

工作流中通过：

```yaml
env:
  GLM_API_KEY: ${{ secrets.GLM_API_KEY }}
```

传入，不要把真实 Key 写进工作流文件。

Streamlit公开部署建议设置：

```text
POLICYDB_READ_ONLY=1
```

公开网站应保持只读，避免用户修改审核结果和原始数据。

---

# 📈 项目进展

## V1.0 已实现

* [x] 原始 Excel 保真导入
* [x] 单元格级数据血缘
* [x] Raw—Staging—Curated—Research—Release分层
* [x] 七大政策体系组织
* [x] T4与T1政策匹配
* [x] 105城市范围配置
* [x] 省、市、区县标准化
* [x] DuckDB研究视图
* [x] 城市—月份政策面板
* [x] Streamlit Dashboard
* [x] 自动审核与来源恢复
* [x] GLM第一次抽取与第二次复核
* [x] 官方来源注册表
* [x] 历史回溯与增量抓取接口
* [x] Excel兼容导出
* [x] 版本化发布

## 下一阶段

* [ ] 扩大已审核官方来源覆盖率
* [ ] 完成105城市历史政策全量回溯
* [ ] 提高区县级政策适用范围识别精度
* [ ] 建立可解释的政策强度指数
* [ ] 接入房地产价格、交易和土地市场结果数据
* [ ] 发布稳定的数据API
* [ ] 建立持续更新的数据版本与引用体系

---

# 🧪 数据质量

执行：

```powershell
uv run policydb validate
```

验证内容包括：

* 主键与唯一性；
* 日期合法性；
* 城市映射；
* 来源状态；
* URL有效性；
* 原始数据血缘；
* 重复政策；
* 分类完整性；
* 105城市范围；
* 研究面板一致性。

测试：

```powershell
uv run pytest
```

代码质量：

```powershell
uv run ruff check .
```

---

# 🤝 使用与引用

本项目面向：

* 房地产经济学研究；
* 城市经济学研究；
* 土地财政研究；
* 城市更新研究；
* 政策效果评估；
* DID与事件研究；
* 空间计量分析；
* 政策监测与知识库建设。

使用数据库开展研究时，请注明：

```text
China Real Estate & Urban Policy Research Database
中国房地产与城市政策研究数据库
```

正式数据版本发布后，请优先引用对应 Release 中的数据版本和引用说明。

---

<div align="center">

### 从政策台账到持续更新的研究基础设施

Raw Data · Traceable Entities · AI Enrichment · Research Panels

</div>
```

这版 README 与仓库实际命令保持一致：项目要求 Python 3.12及以上，核心依赖包括 DuckDB、Polars、PyArrow、Pydantic、PyMuPDF、Trafilatura、Streamlit 和 Typer。

其中“首次完整构建”使用的命令均已在当前 CLI 中实现，包括 `build-city-scope`、`normalize-geography`、`match-t4`、`sources bootstrap-from-excel`、`review auto`、`crawl backfill/update` 和 `enrich glm/verify`。
