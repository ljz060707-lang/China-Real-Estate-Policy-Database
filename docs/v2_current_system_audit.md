# V2.0 当前系统审计

审计日期：2026-07-21  
审计分支：`feat/policydb-v2`  
审计范围：现有代码、Curated Parquet、DuckDB、来源登记、抓取任务、人工审核、Streamlit 与 Windows 启动器。

## 1. 审计结论

V1 已形成可运行的 `Raw → Staging → Curated → DuckDB → Streamlit` 主链路，不应重写。V2 应在现有 Parquet 事实层、DuckDB 视图层和后台任务框架上增量扩展，重点补齐三件事：可证明的来源覆盖、可解释的分层去重、字段级证据置信度。

本次基线验证结果为：`pytest` 140 个测试全部通过，`ruff check .` 通过。现有工作区包含用户先前产生的大量 Raw、Curated、DuckDB 和来源登记改动；这些改动保持原状，V2 代码提交必须使用路径限定，不能清理、回退或混入无关数据。

## 2. 当前数据资产

| 资产 | 当前数量 | 说明 |
|---|---:|---|
| `records.parquet` | 3,568 | 主记录；T1 精确对应 3,011 条 |
| T1 日期范围 | 2003-06-05 至 2026-07-02 | 与既有验收锚点一致 |
| `policy_document_versions.parquet` | 154 | 已抓取或解析的文档版本 |
| `policy_sources.parquet` | 3,311 | 政策与来源关系 |
| `source_seed_records.parquet` | 3,302 | 种子链接与原记录关系 |
| `source_registry.yaml` | 816 个来源 | 200 启用、616 未启用；609 个标为官方 |
| `cities_105.parquet` | 105 | 城市研究样本 |
| `policy_applicable_cities.parquet` | 2,289 | 记录—城市关系 |
| `crawl_runs.parquet` | 8 | 抓取批次 |
| `crawl_items.parquet` | 200 | 抓取项目 |
| `fetch_errors.parquet` | 159 | 抓取错误 |
| `llm_extractions.parquet` | 32 | GLM 抽取结果 |
| `llm_verifications.parquet` | 32 | GLM 二次复核结果 |
| `manual_review_tasks` | 7,457 | DuckDB 审核任务，7,449 条仍为 pending |

Raw 中的 Excel、网页、附件和哈希证据是不可变证据层。V2 迁移不得重写或删除这些文件。

## 3. 当前代码与数据流

### 3.1 Excel 与主数据

- Excel 通过现有 ingest/transform 脚本进入单元格级 Staging 和 Curated Parquet。
- `src/policydb/query/database.py` 将 Curated Parquet 注册为 DuckDB 视图，并创建研究视图。
- `src/policydb/api.py` 提供只读查询、搜索、时间线、统计与导出接口。
- 数据库重建已有临时数据库、基础查询验证、版本标记和 `os.replace` 原子替换机制。

### 3.2 抓取与后台任务

- `src/policydb/crawl/service.py` 是网页与 CLI 共用的业务入口。
- `src/policydb/jobs/manager.py` 使用独立 Python 子进程启动 worker；Windows 已限制控制台窗口、进程组和计算线程。
- 每个任务有独立日志目录和 `data/work/crawl_jobs/<job_id>/` 工作区。
- worker 在 staged 模式下不修改正式 Curated；full 模式在写锁内合并并原子重建 DuckDB。
- `state.json` 已节流写入，Streamlit 使用局部刷新读取小型状态文件。

### 3.3 当前抓取模式

现有服务已覆盖智能组合、官方更新、全网发现、种子回溯、历史回溯、缺失来源恢复和来源体检的入口。列表页发现、搜索 Provider、HTTP 抓取、解析、附件发现、GLM 抽取/验证和运行报告均已有基础实现。

### 3.4 人工审核

- 审核逻辑位于 `src/policydb/review.py` 与 `review_automation.py`。
- Streamlit 审核中心支持分页读取和状态更新。
- 当前任务主要由 T4 未关联、T2 未解释及其他历史质量问题构成，不能再以宽松规则批量增加任务。

## 4. DuckDB 与研究视图

当前 DuckDB 将 Curated Parquet 作为视图加载，已有政策主表、来源质量、主题、地域、城市时间线和各级月度/年度面板。关键现状：

- `v_city_month_policy_panel_105` 有 10,920 行，但通过 105 城市与月份骨架把未观测月份直接填为 0。
- 该做法不能区分“确认无政策”和“尚未扫描”，不满足 V2 实证研究要求。
- 现有 `v_city_month_policy_panel`、`v_city_year_policy_panel` 等 V1 视图应保留兼容；V2 新增 research-ready 视图，不能静默改变旧接口语义。
- 当前 `manual_review_tasks` 的 `review_type` CHECK 约束只允许旧枚举，V2 新质量任务需要迁移兼容。

## 5. 来源登记审计

权威来源登记是 `data/reference/source_registry.yaml`，当前为 version 1。现有字段能描述域名、种子链接、适配器、启用状态和简单健康分，但缺少：

- 明确覆盖城市和省份；
- 来源作用范围、机构分类和必需来源等级；
- 可证明的覆盖起止日期和扫描频率；
- 公报、站点首页、失效与替代关系；
- 解析器版本、最后扫描和连续失败状态。

`config/source_registry.yml` 是另一套仅含少量示例来源的旧配置，当前运行代码未使用，构成重复真相源。V2 应保留兼容提示，但停止把它当作可写登记表。

当前 `agency_type` 仍包含 `local_government`、`media_or_aggregator` 等旧自由值，需要在迁移时映射为受控枚举，同时保存原始值或迁移说明，不得猜测机构属性。

## 6. 去重与版本审计

现有去重仅有：

1. URL 基础规范化；
2. 响应二进制 SHA-256；
3. Parquet `append_unique` 主键覆盖；
4. GLM 结果的部分内容哈希缓存。

主要缺口：

- URL 规范化未完整处理移动端等价 URL 和保留参数规则；
- ETag、Last-Modified 和 304 已在 fetcher 层有基础支持，但未贯通到任务项目与历史请求；
- 文本未计算统一的 `normalized_text_hash` 和 SimHash；
- 没有政策身份键、跨来源转载判断、同一政策版本判断和关键数值冲突保护；
- GLM 缓存查找未同时约束 model、prompt 和 schema 版本；
- 去重决策没有独立事实表，无法审计“为什么跳过、合并或新建版本”。

## 7. 覆盖与质量审计

现有 source health 主要是可访问性、响应和解析成功率；尚不能回答“某城市某月份是否已完整扫描”。当前没有稳定的 `source × period × scan_method` 事实表，也没有来源矩阵、缺口状态和零政策确认状态。

V2 必须引入 `crawl_source_windows`，并把覆盖状态限制为显式状态，例如：`not_scanned`、`partial`、`failed`、`complete_policy_found`、`complete_confirmed_zero`。只有最后一种允许研究面板把政策数写为 0；其余未发现政策的月份应为 null。

## 8. 置信度与审核审计

当前分类表保存置信度和证据，但没有跨字段统一评分，也没有来源权威、证据覆盖、跨来源一致、抽取确定性、实体匹配五个分量。审核任务通常由缺失或阈值规则直接生成，容易把解析失败、覆盖不足和真实信息缺失混为一类。

V2 应新增 `field_confidence`，按字段保留证据、冲突和值，并由确定性规则汇总记录置信度。人工任务只接收冲突、低置信或无法自动证明的问题，不能因“未扫描”批量制造缺失任务。

## 9. 锁、I/O 与性能风险

已具备的保护：

- 后台独立进程与线程上限；
- 任务工作区；
- `PolicyWriteLock`；
- 临时 Parquet 和临时 DuckDB；
- Streamlit 只读查询和状态局部刷新。

仍需处理的风险：

- `append_unique` 会整表读取和重写，数据增长后成本线性上升；
- 一个任务结束时多个 Parquet 虽逐文件原子替换，但不是跨表事务；需用 merge manifest 和校验结果证明一致批次；
- 旧 CLI 抓取命令仍有直接调用 Pipeline 并立即重建数据库的路径，应路由到同一更新服务；
- review 表结构创建逻辑分散在 database、review 和 review_automation 中；
- Source bootstrap 有非原子写 YAML 的旧路径；
- GLM 和覆盖计算不得在 Streamlit 主线程扫描全量 Parquet。

## 10. 重复、旧接口与可删除项

| 项目 | 结论 | V2 处理 |
|---|---|---|
| `src/policydb/update/adapters.py` | 与 crawl Provider/Adapter 体系重复，且未发现调用者 | 保留短期兼容导入，标记弃用；新功能不得继续扩展此层 |
| `config/source_registry.yml` | 与权威 YAML 重复且未被运行代码引用 | 改为指向权威登记表的兼容说明，禁止双写 |
| crawl adapters 中仅 `pass` 的类 | 名义适配器，没有独立行为 | 合并到通用 Provider，只有真正特殊站点才保留子类 |
| review schema 三处创建/ALTER | 约束与迁移容易漂移 | 统一到 migration/schema helper |
| 旧 CLI 直接 Pipeline 路径 | 与 CrawlService/Job worker 重复 | 兼容命令保留，内部路由统一业务服务 |
| PostgreSQL backend placeholder | 当前未运行 | 保留接口边界，不作为 V2 依赖 |

## 11. V1 必须保留

- Raw 不可变、SHA-256 与来源追溯；
- 28 个工作表的单元格级 Staging；
- 3,011 条 T1 主目录迁移结果及既有专题表；
- 当前 Python API、CLI、Streamlit 页面和 Windows 一键启动；
- 现有研究视图名称与 V1 查询语义；
- 后台任务、取消、日志、Keyring/环境变量 SecretProvider；
- 人工审核历史；
- DuckDB 作为本地查询引擎、Parquet 作为核心事实存储。

## 12. V2 增量范围

仅新增三个核心事实表：

1. `crawl_source_windows`：来源—时间窗扫描证据和覆盖状态；
2. `dedup_decisions`：L0—L7 去重/版本决策审计；
3. `field_confidence`：字段证据、五分量评分、冲突和复核状态。

其余能力通过扩展现有来源登记、抓取表、文档版本、DuckDB 视图、验证模块和 Streamlit“覆盖与质量”页面完成，不再建立平行抓取系统或平行来源登记。

## 13. 基线验收记录

```text
pytest: 140 passed
ruff: All checks passed
基线测试耗时: 111.2 秒
ruff 耗时: 2.8 秒
Raw 修改: 无（本次审计未写 Raw）
```

