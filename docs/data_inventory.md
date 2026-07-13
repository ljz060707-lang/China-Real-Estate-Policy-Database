# Excel 数据清单

源文件 SHA-256：`829951c7e88eebdffd729b96c1aac7ccf4c37037a11e6f8df8da9e36605039bf`。共 28 个工作表（含隐藏表）。

|序号|工作表|状态|非空行|非空列|公式|合并区域|性质/迁移目标|
|---:|---|---|---:|---:|---:|---:|---|
|1|T2 城市房地产政策现状|hidden|50|259|0|627|city_policy_rules + cell staging|
|2|T3.1 2020年楼市政策变动|hidden|89|4|0|30|专题原始/人工编码 → records 或专题表；完整 cell staging|
|3|T4 分城市政策统计表|hidden|143|8|0|2|专题原始/人工编码 → records 或专题表；完整 cell staging|
|4|T3 2021年末官方表态|hidden|60|4|0|2|专题原始/人工编码 → records 或专题表；完整 cell staging|
|5|能级划分|visible|299|2|0|0|jurisdiction_attributes|
|6|T1 房地产政策目录|visible|3012|10|0|1|records/policies/documents/relations|
|7|T4 2023年城市需求支持政策|visible|2118|39|42477|13|专题原始/人工编码 → records 或专题表；完整 cell staging|
|8|T4 2023年数量统计图表|visible|180|21|479|0|派生统计，仅 staging|
|9|T5 供给侧措施|visible|176|5|0|0|专题原始/人工编码 → records 或专题表；完整 cell staging|
|10|T6 城市限售政策汇总|visible|97|6|1|31|派生展示 → 仅 staging/关联基础事件|
|11|T7 中央经济工作会议|visible|22|4|0|13|专题原始/人工编码 → records 或专题表；完整 cell staging|
|12|T7 简版|visible|13|2|0|1|派生展示 → 仅 staging/关联基础事件|
|13|T8 中央政治局会议|visible|34|2|0|2|专题原始/人工编码 → records 或专题表；完整 cell staging|
|14|T8 横版|visible|16|4|0|2|派生展示 → 仅 staging/关联基础事件|
|15|T9 全国住建工作会议|visible|7|7|0|2|专题原始/人工编码 → records 或专题表；完整 cell staging|
|16|T10 政府工作报告|visible|18|2|0|2|专题原始/人工编码 → records 或专题表；完整 cell staging|
|17|T10 政府工作报告-视频|visible|7|2|0|2|派生展示 → 仅 staging/关联基础事件|
|18|T11 2007年以来历届全国党代会|visible|7|3|0|2|专题原始/人工编码 → records 或专题表；完整 cell staging|
|19|T12 央行、银保监会、证监会、住建部2014年至今政策梳理|visible|60|5|0|0|专题原始/人工编码 → records 或专题表；完整 cell staging|
|20|房地产项目白名单（城市情况）|visible|41|7|0|0|专题原始/人工编码 → records 或专题表；完整 cell staging|
|21|房地产项目白名单（企业情况）|visible|30|8|0|0|专题原始/人工编码 → records 或专题表；完整 cell staging|
|22|PSL专项贷款|visible|29|8|0|0|专题原始/人工编码 → records 或专题表；完整 cell staging|
|23|T11 政治局会议和中央经济工作会议汇总|hidden|8|3|0|0|派生展示 → 仅 staging/关联基础事件|
|24|T12 二手房参考价|hidden|10|7|0|0|专题原始/人工编码 → records 或专题表；完整 cell staging|
|25|T13 2021年楼市政策变动|hidden|59|6|0|34|专题原始/人工编码 → records 或专题表；完整 cell staging|
|26|T14 禁马甲土拍限制|hidden|17|6|0|0|专题原始/人工编码 → records 或专题表；完整 cell staging|
|27|T8 政治局会议|hidden|21|2|0|2|专题原始/人工编码 → records 或专题表；完整 cell staging|
|28|附录1 疫情期间政策跟踪|hidden|139|12|0|20|专题原始/人工编码 → records 或专题表；完整 cell staging|

## 无法可靠解释的问题

- T2 的多层标题、跨列城市分块和 627 个合并区域无法全部确定性拆解；完整单元格与合并元数据已保留，未猜测。
- T4 的公式、图表辅助列和能级虚拟变量属于派生内容，保留在 Staging，研究统计从 Curated 重算。
- 旧表中未明确发布日期/生效日/失效日的值不从单一‘日期’列猜测。