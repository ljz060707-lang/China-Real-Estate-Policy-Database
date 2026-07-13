# 方法论

默认按 `record_date` 描述政策事件，同时保留 publication、issuance、effective、expiry 四类日期。分类由可审计关键词规则生成，保存证据与置信度；低于 0.65 进入审核。地理标准化保留原名，别名匹配与置信度单列。

城市面板按城市、年、月聚合唯一 `record_id`。政策强度使用 `config/quality_rules.yml` 中可修改权重；同时保留原始数量、方向数量、官方来源指标和来源质量，数据库不自动进行因果推断。
