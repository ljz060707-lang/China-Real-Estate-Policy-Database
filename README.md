# 中国房地产与城市政策研究数据库（CRPD）

CRPD 是面向中国房地产与城市政策研究的可追溯数据库。系统统一保存政策实体、正式版本、
不同网站发布副本、政策动作、原文证据、AI 分类、去重关系和城市—时间覆盖状态。

本项目坚持四条边界：

- Raw 原文、PDF、HTML、附件和原始 URL 永不覆盖；
- 多网站转载只计一个政策实体，但所有发布主体和 URL 均保留；
- 未扫描、部分扫描和失败不能记为零政策；
- “任务运行完成”不等于“覆盖完整”，只有证据完备的窗口才可确认有政策或零政策。

## 核心能力

- Excel、网页、PDF、附件的增量导入和内容寻址归档；
- 105 城市政府来源矩阵、健康监测、搜索 API 补漏和历史分片扫描；
- SiliconFlow 两轮结构化抽取、动作拆分、证据校验和置信度路由；
- URL、二进制哈希、正文哈希、文号、标题、机构、日期和语义联合去重；
- DuckDB 查询、Parquet 主数据、Streamlit Dashboard 和多格式研究导出；
- Windows 每日、周度、月度任务计划，支持幂等、互斥、断点续跑和运行报告。

## 数据范围与五类政策体系

默认历史扫描范围为 105 个大城市、2018-01-01 至今，并纳入相关中央、省、市政策。
最小分类单位是 `policy_action`，综合文件会拆成多个动作。

| 代码 | 一级分类 |
|---|---|
| D | 需求侧政策 |
| S | 供给侧政策 |
| F | 房地产金融与风险 |
| H | 住房保障与城市更新 |
| G | 市场监管与制度治理 |

中金 Excel 原始 topic、工作表和行号继续作为血缘字段保留，不再作为唯一主分类。

## 数据架构

```text
搜索 API / 官方来源矩阵
          ↓
候选 URL → 官方域名与来源主体校验
          ↓
HTML / PDF / 附件不可变归档
          ↓
正文解析 → 政策动作拆分 → 五类分类
          ↓
二次独立复核 → 确定性置信度路由
          ↓
正式存量池 / 自动恢复队列 / 少量人工审核
          ↓
DuckDB 研究视图与覆盖统计
```

四个对象不能混用：

- `policy_entity`：政策本身；
- `document_version`：正式修订版本；
- `publication_copy`：不同部门或网站的发布/转载副本；
- `policy_action`：文件中的具体政策动作。

## 快速安装

要求 Windows 10/11、PowerShell 和 Python 3.12+。普通用户可双击：

```text
首次安装.bat
打开房地产政策数据库.bat
关闭房地产政策数据库.bat
```

开发者命令：

```powershell
uv sync --all-extras
uv run policydb validate
uv run policydb dashboard
```

## D 盘正式存储

推荐配置：

```text
CRPD_DATA_ROOT=D:\Data Set\CRPD
CRPD_ARCHIVE_ROOT=D:\Data Set\CRPD\archive
POLICYDB_DATABASE=D:\Data Set\CRPD\database\policydb.duckdb
POLICYDB_CURATED_ROOT=D:\Data Set\CRPD\curated
POLICYDB_RESEARCH_ROOT=D:\Data Set\CRPD\research
POLICYDB_LOG_ROOT=D:\Data Set\CRPD\logs
POLICYDB_OUTPUT_ROOT=D:\Data Set\CRPD\outputs
```

目录结构：

```text
D:\Data Set\CRPD
├─ archive\pdf|html|text|attachments
├─ curated
├─ research
├─ database
├─ manifests
├─ logs
├─ outputs
├─ jobs
├─ quarantine
└─ backups
```

先预览，再确认迁移：

```powershell
uv run policydb storage plan-migration --target "D:\Data Set\CRPD"
uv run policydb storage migrate --target "D:\Data Set\CRPD" --confirm
uv run policydb storage verify --target "D:\Data Set\CRPD"
```

迁移采用“复制 → SHA-256 校验 → 写入非敏感偏好 → 验证”，不会删除源文件。D 盘不可用时
正式写入失败，不会静默回写 C 盘。

## 配置 SiliconFlow 与搜索 API

本地 Dashboard 的“个人设置”页把密钥写入 Windows Keyring；密钥不会进入 README、JSON、
DuckDB、Parquet、任务参数或 Git。也可以通过本机环境注入：

```text
AI_PROVIDER=siliconflow
SILICONFLOW_API_KEY=<仅保存在本机>
SILICONFLOW_BASE_URL=https://api.siliconflow.cn/v1
SILICONFLOW_CHAT_MODEL=<从 /v1/models 选择>
SILICONFLOW_VERIFY_MODEL=<建议与首轮不同>
SILICONFLOW_EMBEDDING_MODEL=BAAI/bge-m3
SILICONFLOW_RERANK_MODEL=BAAI/bge-reranker-v2-m3
```

测试连接和读取模型：

```powershell
uv run policydb ai test
uv run policydb ai models
```

模型名不写死，以 SiliconFlow `GET /v1/models` 的实际结果为准。

搜索服务支持 Serper、Tavily、Bing。可按顺序配置多个 Provider，主 Provider 超时或失败时自动
切换；搜索结果只作为候选，普通媒体不能直接成为 canonical source。

```text
SEARCH_PROVIDERS=Serper,Tavily,Bing
```

每个 Provider 的 Key 应保存在 Keyring；未配置搜索 API 时，官方列表页抓取和原链接回溯仍可运行。

## 105 城市来源矩阵

每个城市至少要求：市政府、政府公报、住建部门、公积金中心和自然资源部门。运行：

```powershell
uv run policydb sources complete-matrix
uv run policydb sources discover-city --city 南京市
uv run policydb sources discover-all --city-limit 5
uv run policydb sources discover-all --apply
uv run policydb sources health-all
uv run policydb sources enable-recommended --limit 20
uv run policydb sources complete-matrix
uv run policydb sources repair
uv run policydb sources validate-registry
```

自动发现只登记经过官方域名校验的候选，默认保持禁用；来源健康分达到 90、robots 允许、入口可
访问、能发现详情页且正文可解析后，才可推荐启用。

当前网络不可访问政府站点时，先保留完整的 105 城市 × 5 必需角色槽位，不得填造网址。更换网络并配置搜索 API 后，依次执行 discover-all --apply、health-all 和 complete-matrix；直到矩阵 CSV 中 525 个槽位均有已验证来源，才可称来源 URL 完整。缺口明细输出到 D:\Data Set\CRPD\outputs\coverage\city_source_requirement_matrix.csv。

## 历史全量扫描

首次运行不要直接启动 105 城市全时段付费 AI。按以下顺序验收：

```text
本地 fixture → 一个真实来源 → 南京最近30天 → 南京一年
→ 一个省份一年 → 10个城市 → 105城市分年度执行
```

常用命令：

```powershell
uv run policydb crawl historical --from 2018-01-01 --to 2018-12-31 --cities 南京市
uv run policydb crawl official-update
uv run policydb crawl web-discovery
uv run policydb crawl seed-backtrack
uv run policydb crawl recover-missing
```

历史任务按“城市 × 来源角色 × 年份”分片。达到安全上限只能记为 `partial`；只有分页与搜索结果
耗尽、详情页处理完成、错误已处理且相关性已判断，才能写入完整状态。

## AI 分类、正式池与人工池

```powershell
uv run policydb ai classify --run-id <RUN_ID>
uv run policydb ai verify --run-id <RUN_ID>
uv run policydb ai deduplicate
uv run policydb ai route-pools
```

自动进入正式存量池要求官方来源、正文完整、Schema 合法、证据可定位、分类合法、地区无冲突、
去重关系确定且综合置信度不低于 0.90。其余先进入自动抽取、来源恢复或第二轮自动复核；只有明确
冲突和多次自动恢复失败才进入人工审核。

AI 不得生成原文不存在的标题、日期、机关、文号、URL 或政策内容。

## Windows 自动更新

配置文件默认启用分层计划，但修改 YAML 不等于任务已经安装。预览并由本机管理员确认：

```powershell
uv run policydb schedule status
uv run policydb schedule install --confirm
uv run policydb schedule run-daily
uv run policydb schedule run-weekly
uv run policydb schedule run-monthly
uv run policydb schedule uninstall --confirm
```

- 每日 03:00：7 日重叠增量、搜索补漏、归档、AI、去重、重建和验证；
- 每周：回扫 30 天、重试失败、补抓 PDF 和替代来源；
- 每月：完整扫描上月核心来源，只有证据完整时确认零政策月份。

## Dashboard

```powershell
uv run policydb dashboard
```

主要页面：数据总览、政策中心、自动更新与完整性、数据质量、人工审核、个人设置。Dashboard
读取稳定 DuckDB；抓取、解析、AI 和重建均在独立后台进程运行。

## 覆盖度解释

系统分别报告：

- 来源覆盖率；
- 时间窗口覆盖率；
- 官方正文覆盖率；
- PDF 归档率；
- 分类完成率；
- 去重完成率。

覆盖状态：`not_scanned`、`partial`、`failed`、`complete_policy_found`、
`complete_confirmed_zero`。只有最后两种属于完整窗口。

## 研究数据与导出

```powershell
uv run policydb search --keyword "城市更新" --region "武汉市"
uv run policydb stats --group-by year,province,topic
uv run policydb export --view city_month_panel --format xlsx --output outputs/city_month_panel.xlsx
uv run policydb release --version 0.1.0
```

核心视图包括政策动作中心、城市月度/年度面板、省份月度/年度覆盖、来源角色覆盖、文档归档覆盖
和覆盖约束研究面板。未完整扫描的政策数保持 null，不伪造为 0。

## 质量控制

```powershell
uv run ruff check .
uv run pytest --basetemp .test-tmp-local
uv run policydb migrate-v2 verify
uv run policydb validate
uv run policydb sources validate-registry
```

## 项目状态与限制

代码、配置、任务安装、真实抓取、覆盖验证和研究就绪是六个不同状态。来源矩阵文件存在不代表
五类部门已全部找到；创建 run_id 或 validate 通过也不代表抓取成功。真实抓取成功至少应同时看到
`source_count > 0`、`item_count > 0`、`fetched > 0` 和 `document_versions > 0`。

政府网站会改版、下线、启用验证码或调整 robots；系统提供持续监测、重试、替代入口和失败报告，
但不能承诺所有来源永久可用。付费搜索和 AI 结果必须以实际 API 配置与运行日志为准。
