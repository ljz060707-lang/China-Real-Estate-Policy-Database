# 来源注册表

`policydb sources bootstrap-from-excel` 根据 `config/excel_sheet_map.yaml` 指定的URL列，从全部单元格级Staging提取来源。媒体链接不会被删除，因为它们可用于发现历史政策；但媒体和聚合平台只标为线索或辅助来源。

等级规则：

- P0：`.gov.cn`以及中国政府网、住建部、人行、金融监管总局、证监会等明确官方域名；
- P1：经人工确认的官方转载或政务平台；
- P2：可靠媒体或行业媒体；
- P3：自媒体、聚合页和一般线索。

当前自动规则只确定P0的域名属性；P1/P2必须人工确认。一个政策可以连接多个来源，但只有核验后的官方原文可设为canonical。无法找到官方原文时保留媒体来源，并设置
`official_status=secondary_only`、`needs_review=true`。

## V2 唯一登记与覆盖范围

`data/reference/source_registry.yaml` 是唯一权威登记；`config/source_registry.yml` 不再保存来源事实。V2 新增 `scope_type`、`city_ids`、`province_codes`、`agency_type`、`required_level`、覆盖日期、频率、有效性、替代来源、解析器版本和失败状态。

自动迁移只把已知中央域名映射为全国来源。其余无法证明范围的来源保持 unknown，并由 `policydb sources unresolved` 输出；不得根据域名名称猜测城市。
