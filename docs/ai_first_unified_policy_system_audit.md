# AI 优先统一政策系统审计

审计日期：2026-07-23。

## 结论

现有工程已经具备 Raw → Staging → Curated → Research → Release、DuckDB/Parquet、
后台 JobManager、来源注册、105 城范围、人工审核、政策动作和政策强度系统。本次升级应在这些
模块上增加统一 codebook、AI Provider、内容寻址档案和调度闭环，不应建立第二套抓取或数据库。

## 当前真实基线

| 指标 | 数量 | 说明 |
|---|---:|---|
| 政策记录 | 3,568 | 其中 T1 主目录 3,011 |
| 政策动作 | 858 | 已有原文证据位置 |
| 来源 | 816 | 启用 200，启用率 24.51% |
| 文档版本 | 281 | 不等于已归档政策数 |
| 去重决策 | 0 | 现有规则存在，但真实存量尚未形成决策簇 |
| 105 城 | 105 | 城市范围已固化 |
| 完整来源窗口 | 0 | 不得宣称 2018 年以来完整覆盖 |
| 人工审核任务 | 7,457 | 需通过自动复核逐步收敛 |
| D 盘档案文件 | 0 | `D:\Data Set\CRPD` 存在但为空 |

当前全文归档率不能用 `policy_document_versions` 简单替代；D 盘正式档案率为 0%。当前重复政策
数量无法从 0 条 `dedup_decisions` 推断，状态是“未完成存量语义去重”，不是“无重复”。

## 直接复用

- `src/policydb/crawl/`：发现、抓取、解析、确定性去重和来源窗口。
- `src/policydb/jobs/`：Streamlit 外独立工作进程、状态持久化和报告。
- `src/policydb/enrich/glm.py`：结构化抽取、证据跨度、第二轮复核和缓存。
- `src/policydb/review_automation.py`：自动诊断、来源恢复和人工托底路由。
- `src/policydb/intensity/`：政策动作、数值校准、模型路由和研究就绪门槛。
- `src/policydb/scope.py`、`coverage.py`：105 城适用关系和覆盖骨架。

## 需要重构

- 将 GLM 专用配置迁移到通用 `AIProvider`，新任务默认 SiliconFlow，旧命令兼容转发。
- 将七大研究专题保留为展示集合，正式政策动作分类改为 D/S/F/H/G 五类。
- 抓取 Raw 与 D 盘研究档案分工明确：项目 Raw 保持不变，档案库采用内容寻址追加。
- 将现有检索、105 城、趋势、专题视图整合为单一“政策中心”入口。
- Windows 更新脚本统一暴露为 `policydb schedule` 命令，并记录实际安装状态。

## 旧分类迁移原则

原始 `legacy_category`、工作表、七大研究集合和 `record_terms` 全部保留。新增
`policy_classifications.parquet` 只描述政策动作分类，不反向覆盖历史字段。复合或语义模糊的
中金 topic 保留原值并进入 AI 复核。

## 兼容命令

`policydb enrich glm`、`policydb enrich verify`、原抓取命令、七大研究集合页面和所有既有
PolicyDB 查询接口继续保留；新命令通过相同服务实现，不复制业务逻辑。

## 基线测试

第一次 `pytest`：142 条通过、49 条 setup error。错误统一来自用户 Temp 目录
`pytest-of-ljz52` 的 Windows 权限，不是业务断言失败。正式验收改用仓库内
`--basetemp=.test-tmp -p no:cacheprovider`。
