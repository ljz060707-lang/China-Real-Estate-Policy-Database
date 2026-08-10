# CRPD Dashboard 数据接口与重设计验收

验收时间：2026-08-10（Asia/Shanghai）  
项目目录：`D:\Codex\projects\Documents-Codex\2026-07-13\text-20260705-xlsx-text-data-raw\policy-database`  
权威数据根目录：`E:\Data Set\CRPD`

## 1. 验收结论

本轮 Dashboard 的数据接口核验、信息架构重构和专业级 UI 重设计已完成，并在真实 E 盘数据、活动全量历史抓取和本机浏览器中通过验收。

- Dashboard 只读访问数据库、Parquet、checkpoint、流水线事件和自动化状态，不创建数据库长事务，也不修改来源注册表。
- 当前实时抓取、后处理进度与严格来源完整性使用不同指标、分母和定义展示，不再混为一个“总进度”。
- 主导航固定为六页：总览、政策中心、采集与处理、数据质量与覆盖率、人工审核、系统与设置。
- 页面每 20 秒局部刷新，默认仅监听 `127.0.0.1:8501`。
- 政策中心优先读取正式 DuckDB；正式索引不可查询时，自动进入只读 E 盘 Curated 降级模式，并明确提示数据状态。
- 未运行或不可用数据保持“未启用”“不可用”或空值，不伪装为 0。
- 本轮未停止、重启或修改活动抓取器；未提交、未推送 Git。

## 2. 真实数据接口核验

| 业务域 | 主要数据接口 | 更新语义 | Dashboard 用法 |
|---|---|---|---|
| 实时抓取 | `pipeline_progress_events.parquet`、`crawl_shards.parquet` | 活动抓取持续追加 | 当前城市、来源、时间分片、阶段、心跳和当前批次分片进度 |
| 城市阶段 | `audited_full_backfill/master.log` | runner 完成城市后更新 | 已完成城市 / 105，不提前把运行中城市算作完成 |
| 来源完整性 | `source_requirement_slots.parquet`、`source_slot_progress.parquet` | 来源审计重建后更新 | resolved、verified、enabled / 525 以及门控不一致 |
| 政策数据 | DuckDB；不可用时读取 E 盘 Curated Parquet | 数据落盘或索引刷新后更新 | 政策检索、统计、详情和导出 |
| 数据质量 | records、geographies、coverage gaps、PDF 资产 | 对应数据集更新后刷新 | 字段缺失、重复、缺口、来源和归档质量 |
| AI 与审核 | AI 抽取/核验状态、来源候选与审核队列 | 对应审计文件更新后刷新 | 仅展示真实调用与待审数量，不推断联网或费用 |

`city_year_progress.parquet` 是严格完整性最小门控结果，只在进度重建时刷新。本轮没有把它当成实时抓取进度。

## 3. 验收时真实状态

最终统一快照生成时间：2026-08-10 11:48（北京时间，近似）

| 指标 | 真实值 |
|---|---:|
| 活动抓取 | RUNNING |
| 当前城市 | 福州市 |
| 当前来源角色 | 自然资源部门 |
| 当前批次分片 | 438 / 520（84.2%） |
| runner PID | 22436 |
| worker PID | 49804 |
| 政策记录 | 3,568 |
| 文档版本 | 3,416 |
| 有文档城市 | 101 / 105（96.2%） |
| 已完成城市 | 20 / 105（19.0%） |
| resolved 槽位 | 498 / 525（94.9%） |
| verified 槽位 | 398 / 525（75.8%） |
| enabled 槽位 | 400 / 525（76.2%） |
| open gaps | 3,971 |
| critical gaps | 506 |

上述数值来自同一次只读快照；抓取持续运行时会继续变化。

## 4. 浏览器验收

在 `http://127.0.0.1:8501` 逐页实测：

| 页面 | 中文标题 | 真实数据 | 前端异常 |
|---|---|---|---|
| 总览 | 通过 | 通过 | 无 |
| 政策中心 | 通过 | 通过，DuckDB 异常时只读降级 | 无 |
| 采集与处理 | 通过 | 通过 | 无 |
| 数据质量与覆盖率 | 通过 | 通过 | 无 |
| 人工审核 | 通过 | 通过 | 无 |
| 系统与设置 | 通过 | 通过 | 无 |

额外实测：

- 1366、1920、2560 宽度下导航可见且无横向溢出。
- 政策关键词筛选可用，政策详情可打开。
- 抓取器活动时，可能形成第二写入链的操作按钮保持禁用。
- 政策强度页明确显示“尚未启用”，已测度文档为 0，未生成模拟结果。
- 页面未暴露 API key、Authorization header、任意 shell 输入或原始 JSON。

## 5. 自动化验收

| 检查 | 结果 |
|---|---|
| Dashboard 定向测试 | 15 passed |
| 响应式回归 | 1 passed |
| 完整 pytest | 442 passed，0 failed，0 errors，2 warnings，457.61 秒 |
| Ruff | `src app tests` 全部通过 |
| compileall | `src app scripts` 通过 |
| `git diff --check` | 通过，仅有既有行尾转换提示 |
| 敏感信息扫描 | 15 个本轮文本文件，0 命中 |

完整测试日志：`E:\Data Set\CRPD\outputs\dashboard_redesign\20260810_dashboard_redesign_final\pytest.log`

Ruff 的通过范围是 Dashboard 所依赖的 `src app tests`。将历史 `scripts` 目录一并纳入的扩展扫描仍报告 33 条既有 lint 债务；这些脚本不是本轮 UI 数据接口改造的代码范围，本轮没有为制造全仓通过结果而改写活动采集或自动化脚本。

## 6. 截图与交付物

- `docs/assets/dashboard/CRPD_Dashboard_Overview_20260810.png`
- `docs/assets/dashboard/CRPD_Dashboard_Collection_20260810.png`
- `docs/assets/dashboard/CRPD_Dashboard_Quality_20260810.png`
- `docs/implementation/CRPD_DASHBOARD_DATA_INTERFACE_AUDIT.md`
- `docs/implementation/CRPD_DASHBOARD_REDESIGN_PLAN.md`
- `docs/implementation/CRPD_DASHBOARD_ACCEPTANCE.md`

## 7. 已知真实警告

1. 来源槽位存在 `enabled_unverified=2`。Dashboard 在总览和质量页明确告警，但按照只读边界不自动修改来源注册表。
2. E 盘 DuckDB 的部分外部视图仍引用旧 D 盘 Parquet，因此代表性查询当前不可用。政策中心已使用可审计的 E 盘 Curated 只读降级路径；页面不会把旧值伪装成实时数据库结果。

这两项是数据治理后续事项，不构成 Dashboard 可用性失败，也未被本轮 UI 改造掩盖。
