# 自动更新调度状态

检查日期：2026-07-23。

| 任务 | Windows 任务计划 |
|---|---|
| daily | 未安装 |
| weekly | 未安装 |
| monthly | 未安装 |

工程命令与 runner 已配置，但本次没有替用户修改 Windows 任务计划。运行
`uv run policydb schedule install-windows --confirm` 后才会实际安装。

105 城覆盖矩阵共有 43,260 个城市—来源角色—月份单元；完整窗口 0，缺口 43,260。该结果表明
完整性核算功能已运行，但历史覆盖任务尚未完成。
