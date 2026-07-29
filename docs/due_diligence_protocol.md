# 城市政策覆盖尽职调查协议

最小审计单元是“城市 × 来源 × 时间窗”。`crawl_source_windows` 必须记录入口、分页边界、目标日期、站内搜索、公报、附件、成功/失败数量与异常证据。

覆盖状态严格区分：

- `not_scanned`：没有可审计扫描证据；
- `partial`：入口、分页、日期边界或渠道至少一项未完成；
- `failed`：扫描发生未解决错误；
- `complete_policy_found`：完整扫描并发现政策；
- `complete_confirmed_zero`：完整扫描且没有发现政策。

确认零政策必须同时满足入口成功、分页结束或达到目标日期、无未解决解析错误，并保存 `completion_evidence.exhaustive=true`。未扫描、部分扫描和失败月份在 research-ready 面板中保持 `policy_count=NULL`。

城市只有在 mandatory 来源登记完整、扫描率达标、失败来源存在正式例外或替代来源、目标月份没有 `not_scanned`、公报与官方搜索补漏完成且抽样召回达标时，才能进入尽职完成状态。覆盖评分仅用于定位薄弱环节，不能替代硬门槛。
