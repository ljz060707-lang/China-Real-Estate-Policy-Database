# 覆盖完整性方法

正式覆盖单元为 `city × source_role × month`。状态只有：
`not_scanned`、`partial`、`failed`、`complete_policy_found` 和
`complete_confirmed_zero`。

未发现候选不等于零政策。只有分页自然耗尽或稳定进入起始日期之前、详情与附件处理完成且无
错误，并通过政策相关性验证，才能成为完整窗口。达到页数/候选上限、循环分页、限流、解析失败
或网络失败均为 partial/failed。研究面板对前三种状态保留 NULL。

```powershell
uv run policydb coverage build
uv run policydb crawl audit
```

分页扫描的页数、停止原因和是否耗尽保存在 `crawl_discovery_scans`；覆盖输出为
`outputs/coverage/city_source_month_coverage.csv` 和 `105_city_gap_report.md`。
