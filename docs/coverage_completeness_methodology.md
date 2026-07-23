# 105 城覆盖完整性方法

覆盖单位是“城市 × 核心来源角色 × 月份”。核心角色包括政府/公报、住建/房管、公积金中心和
自然资源部门。状态仅允许：

- `not_scanned`
- `partial`
- `failed`
- `complete_policy_found`
- `complete_confirmed_zero`

只有完成分页、无抓取错误且保存 exhaustive 运行证据的窗口，才可进入两个 `complete_*`
状态。未扫描、部分扫描和失败不能记为零政策。

```powershell
uv run policydb coverage build
```

该命令生成 `outputs/coverage/city_source_month_coverage.csv` 和
`outputs/coverage/105_city_gap_report.md`。矩阵从 2018-01 起建立，即使没有登记来源也保留
`not_scanned` 单元，使来源缺口可见。

当前实际基线：完整窗口为 0；来源矩阵只覆盖 10 个城市、41 个城市—来源关系。因此当前工程
具备完整性核算能力，但没有形成“2018 年以来 105 城完整覆盖”的证据。
