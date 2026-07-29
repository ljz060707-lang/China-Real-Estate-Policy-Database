# CRPD V3 验收报告

验收时间：2026-07-24（Asia/Shanghai）  
分支：`fix/unified-policy-system-v3`

## 1. 代码与数据边界

- 未修改 Raw。
- 用户原有的 Curated Parquet、验证报告和 DuckDB 工作区改动未回退、未覆盖。
- 正式数据库未因本轮代码验收而重建；新视图 SQL 在隔离临时目录中完成构建测试。
- 远端读取因本机 Schannel `SEC_E_NO_CREDENTIALS` 失败，未推送。

## 2. 实现验收

- Dashboard 普通用户导航固定为六项；Streamlit 自动多页面导航已通过
  `client.showSidebarNavigation=false` 关闭。
- `v_policy_action_center` 新增 `legacy_collection`、`source_topic`、
  `pdf_available` 和 `full_text_available`，旧分类只作为血缘。
- V3 分类版本为 3.0.0；CICC inventory、mapping report 和 unmapped 输出已生成。
- 城市、省份、主题、来源 ID/角色、总候选、每来源候选、每来源页数、批量和安全上限已从
  `CrawlJobRequest` 贯穿至 `CrawlPipeline.plan()`。
- 选定城市时，计划只保留全国来源或与所选城市/省份明确关联的来源；未知城市不回退全量。
- 候选以来源轮转方式分配，全局上限不再由首个来源独占。
- 分页记录页数、停止原因和是否耗尽；达到页数/候选上限保持 partial。
- `resume` 会跳过同日期范围、同来源和同城市的完整窗口。
- 新增统一 CLI：`crawl health/historical`、`archive recover-missing`、
  `schedule install/uninstall`，旧命令继续可用。
- Windows Streamlit AppTest 的 Pandas Arrow 原生崩溃已在 `safe_pandas` 边界消除。

## 3. 数据验证

隔离验证（读取稳定数据库，报告写入临时目录）：

- 工作表：28
- 主目录：3011
- 主目录日期：2003-06-05 至 2026-07-02
- 主目录缺标题：10
- 主目录缺正文：123
- 主目录缺链接：11
- 全库记录：3568
- 105 城：105 个唯一 city_id
- 覆盖矩阵行：10815
- 非 confirmed-zero 却写 0：0
- 验证结果：`passed=true`

标题缺失比初始基准 11 少 1 条；这是当前稳定数据库的程序复算结果，不反向修改原始 Excel。

## 4. 自动化、归档与 AI 的真实状态

- SiliconFlow：配置已存在；本轮只执行无密钥输出的配置审计，未调用真实付费模型，连接和
  模型可用性仍需用户在设置页执行“测试连接”确认。
- 档案审计：281 个文件记录中 181 个 archived，100 个失败/待恢复。不能宣称归档完成。
- Windows 计划任务：daily/weekly/monthly 均未安装。
- 抓取存量：42 个已启用来源、11 次 run、400 个 crawl item、281 个文档版本、
  235 个历史抓取错误。
- 未执行真实 105 城全历史抓取，不能宣称 2018 年以来真实全覆盖。

## 5. 本地 5 条演示

任务 `JOB_A6A5C3A61C0ED4C2E2AA` 使用本地 fixture：

- 候选 5
- 抓取 5
- 失败 0
- 文档版本 5
- GLM 0
- 正式库合并 0
- 状态 `completed_with_warnings`，原因是 staged-only 演示结果未合并正式库。

报告目录：
`outputs/crawl_reports/JOB_A6A5C3A61C0ED4C2E2AA/`

## 6. 测试与浏览器验收

- `pytest`：223 项通过。
- `ruff check .`：通过。
- Dashboard 定向回归：9 项通过。
- 浏览器：1440×900 验证六个导航入口、政策中心、自动更新四个标签，无
  `stException`。
- 截图：`outputs/ui/policy_center_v3_desktop.png`。
- 本地 Dashboard 验收地址：`http://127.0.0.1:8517`。

## 7. 尚未完成

1. 100 个缺失/失败档案需要运行恢复并逐项解释失败原因。
2. 235 个历史抓取错误尚未全部重试。
3. Windows 日/周/月任务尚未由用户确认安装。
4. SiliconFlow 真实连接、模型列表和付费分类未在本轮调用。
5. 105 城 2018 年以来真实完整窗口仍需按“5 条 → 单来源 → 单城 30 天 → 单城一年 →
   10 城 → 105 城分批”逐级执行。
6. `not_scanned/partial/failed` 继续保持 NULL，不得在论文中解释为零政策。
