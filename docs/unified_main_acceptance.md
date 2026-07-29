# CRPD 统一 main 验收报告

生成时间：2026-07-30（Asia/Shanghai）

## 1. 分支审计与安全整合

真实分支及审计时 HEAD：

- `main`：`8ba98a62a151eca39187d1a6ae012c3c32557421`
- `feat/policydb-v2`：`9ece7ea9d78d80041ddd6abad29369d36fbe2ca6`
- `fix/unified-policy-system-v3`：`2904b22e8ef6feafa3d4979242cdce8e334f07e9`

已创建并推送三个不可变备份标签：`backup/main-before-v3-integration`、`backup/v2-before-v3-integration`、`backup/v3-before-main-integration`。整合分支为 `integration/crpd-main-v3`，先合并 V2，再合并 V3，保留普通 merge 历史。旧分支尚未删除。

`main` 已通过 `--ff-only` 快进到整合结果并正常推送至 GitHub；未使用强推。

## 2. 统一存储

- 正式根目录：`D:\Data Set\CRPD`
- DuckDB：`D:\Data Set\CRPD\database\policydb.duckdb`
- Curated：`D:\Data Set\CRPD\curated`
- Research：`D:\Data Set\CRPD\research`
- 归档：`D:\Data Set\CRPD\archive`
- 任务：`D:\Data Set\CRPD\jobs`
- 日志：`D:\Data Set\CRPD\logs`
- 输出：`D:\Data Set\CRPD\outputs`

迁移实际执行“复制、SHA-256 校验、配置切换、验证”，未删除仓库和旧 D 盘副本。实时抓取现在按 SHA-256 写入 `archive/pdf|html|text|attachments/<前2位>/`；D 盘不可用时不回退 C 盘。

当前归档文件计数是 PDF 11、HTML 130、TXT 0、附件 14。对既有 281 个文档版本执行归档核验时，37 个通过，144 个哈希不一致，100 个源文件缺失；因此历史归档尚不完整，不能称全部可复现。新抓取会生成 HTML/PDF 和清洗 TXT，但本次受网络限制没有新增真实文档。

## 3. 105 城市来源矩阵

代码和配置已建立 105 城市 × 5 个必需来源角色，共 525 个矩阵槽位。每个城市均有：市政府、政府公报、住建部门、公积金中心、自然资源部门。

当前真实状态：

- 注册来源：42
- 已启用：42
- 官方/官方转载：41
- 已映射城市：10
- 已登记必需矩阵槽位：30/525
- 缺失槽位：495/525
- 五类必需来源全部齐全的城市：0/105
- 健康分不低于 90 的现有来源：1（分数可能来自既有检查，不代表本次网络复核）

逐城市、逐角色缺口文件：

- `D:\Data Set\CRPD\outputs\coverage\city_source_requirement_matrix.csv`
- `D:\Data Set\CRPD\outputs\coverage\city_source_requirement_matrix.parquet`

矩阵槽位已完整，但 URL 未齐全。当前环境未配置搜索 API，且访问南京政府站点时发生 TLS 握手失败；系统没有编造剩余 495 个网址。更换网络并配置 Serper/Tavily/Bing 后，执行 `sources discover-all --apply`、`sources health-all`、`sources enable-recommended` 和 `sources complete-matrix` 才能补齐并验证。

## 4. 搜索 API 与 SiliconFlow

搜索 Provider 当前为 `None`，未配置搜索 Key，因此自动发现 495 个缺口尚未真实执行。多 Provider 回退代码和 mock 测试已完成。

SiliconFlow 实际连接测试通过：可用模型 91 个；当前模型为首轮 `zai-org/GLM-5.2`、复核 `Pro/zai-org/GLM-5.1`、Embedding `BAAI/bge-m3`、Rerank `BAAI/bge-reranker-v2-m3`。本次没有运行全量付费分类或去重。

## 5. 政策池、分类与去重

现有数据：政策实体 3,568、文档版本 281、发布副本 281、政策动作 858、动作分类 858。

确定性路由已实际运行：

- 正式存量池：87
- 增量处理池：3,481
- 其中待自动抽取：3,479
- 自动来源恢复：2
- 第二轮自动复核：0
- 明确人工冲突：0

这不等于全量 AI 已完成。当前 `dedup_decisions=0`，因此自动去重比例、多主体合并数量和修订版本数量尚不能给出可信的已完成比例。代码已经保留 entity/version/publication/action 四层模型和确定性准入条件；后续真实运行才会产生去重裁决。

## 6. 覆盖窗口与省份统计

新增 DuckDB 视图：`v_province_month_coverage`、`v_province_year_coverage`、`v_source_role_coverage`、`v_document_archive_coverage`。数据库重建后这些视图能够查询。

现有研究覆盖表有 10,815 行、105 个城市，`invalid_zero_count=0`；但新的来源抓取窗口表当前为 0 行。因此省份×时间完整扫描率尚未通过真实历史抓取验证。`not_scanned`、`partial`、`failed` 不会写成确认零政策，只有分页/搜索耗尽且错误闭环后才允许 `complete_policy_found` 或 `complete_confirmed_zero`。

## 7. 真实抓取验收

本地 fixture 和 mock 抓取测试通过。真实南京小规模任务运行两次：

1. `JOB_24FE1FD5936B9AE5DDFE`：1 来源、5 候选、0 抓取、5 失败、0 文档版本；暴露 seed 候选缺少 `city_id` 和日期过滤的问题。
2. `JOB_D29BE8DC4219EBB1CE06`：1 来源、4 候选、`city_id=CITY_320100`、0 抓取、4 失败、0 文档版本；HTTP→HTTPS恢复已执行，最终均为 TLS `UNEXPECTED_EOF_WHILE_READING`。

随后修复了嵌入 YYYYMM URL 的年份过滤，并新增回归测试。当前网络对三个南京官方 HTTPS 入口均在 TLS 握手阶段失败，所以没有满足 `fetched>0` 和 `document_versions>0`，不能声称真实抓取成功，也不能声称历史全量扫描已运行。

## 8. Windows 自动任务

三个任务已真实安装且处于 Ready：

- `CRPD-daily`：每日 03:00，下一次 2026-07-30 03:00
- `CRPD-weekly`：周日 03:00，下一次 2026-08-02 03:00
- `CRPD-monthly`：每月 1 日 03:30，下一次 2026-08-01 03:30

Task Scheduler 的 Last Result 为 `267011`，表示尚未运行；所以“任务已安装”成立，“自动任务已成功抓取”不成立。后台工作区、日志和锁已切换到 D 盘。Windows PID 存活检测已从会发送控制信号的 `os.kill(pid, 0)` 改为只读进程句柄检查。

## 9. Dashboard

Windows 启动器实际选择 `.venv` Python 3.12.13，Streamlit 和 policydb 导入检查通过。Dashboard 在 8501 端口启动，`/_stcore/health` 返回 HTTP 200 和 `ok`。浏览器控制插件本次返回 `Transport closed`，因此没有完成自动点击式 UI 截图；Streamlit AppTest 和 Dashboard 回归测试包含在全量 pytest 中。

## 10. 数据验证与测试

- `uv sync --all-extras`：通过
- `uv run ruff check .`：通过
- `uv run pytest`：243/243 通过；Arrow 边界修复后复跑用时约 148.4 秒
- `uv run policydb migrate-v2 verify`：通过；T1=3,011，2003-06-05 至 2026-07-02
- `uv run policydb validate`：通过；28 个工作表，3,568 条记录，105 城市，invalid zero=0
- `uv run policydb sources validate-registry`：通过，但 `SRC_33BD0596A208D584` 仍有 1 个 unresolved scope
- `uv run policydb ai test`：真实连接通过
- `uv run policydb schedule status`：三项已安装

验证报告还显示：主表缺标题 10、缺正文 123、缺链接 11、待审核 22、低置信分类 79、非规范链接 19、未映射单元格 2,357。

## 11. README

README 已重写为统一 main 版本，覆盖安装、D 盘迁移、SiliconFlow、搜索 API、105 城市来源矩阵、分阶段历史扫描、Windows 更新、覆盖含义、研究导出和限制。明确区分任务完成、真实抓取、覆盖完整和研究就绪。

## 12. 状态边界

| 项目 | 状态 |
|---|---|
| 三分支代码整合 | 已快进合并并推送 main |
| D 盘配置与数据复制 | 已完成并验证 |
| 105 城市需求槽位 | 525/525 已建立 |
| 105 城市真实 URL | 30/525 已登记，495 待搜索与验证 |
| 搜索 API | 代码完成，未配置 |
| SiliconFlow 连接 | 已验证 |
| 全量 AI 分类/去重 | 未运行 |
| 南京真实抓取 | 已运行但网络失败，0 文档 |
| 105 城市历史全量抓取 | 未运行 |
| 省份×时间真实完整覆盖 | 未验证 |
| Windows 任务安装 | 已安装，尚未首轮运行 |
| 测试与 lint | 通过 |
| 研究就绪 | 种子库可查询；完整历史扫描尚未就绪 |

## 13. 剩余工作

1. 在可访问中国政府网站的网络环境配置至少一个搜索 API，并补齐/验证 495 个来源槽位。
2. 依次做南京 30 天、南京一年、一个省份一年、10 城市、105 城市分年度扫描。
3. 修复历史归档中的 144 个哈希不一致和 100 个源文件缺失。
4. 对本次新增文档运行 SiliconFlow 两轮分类、来源恢复和去重；记录 API 用量。
5. 只有新的覆盖窗口出现完整证据后，再发布省份×时间覆盖率和确认零政策月份。