# 自动审核与地区面板审计（2026-07-15）

## 根因

- 审核队列把 T2 合并单元格占位、T4 同行重复特征、来源线索和真正语义缺失混在一起，导致人工任务虚高。
- 原 HTML 解析主要依赖单一正文抽取，未保存可诊断文本块，也未处理 DOM 正文节点、附件和动态页面提示；PDF 未执行跨页句子恢复和内嵌附件保存。
- GLM 只有第一次抽取，没有独立证据复核；模型结果也没有与确定性通过阈值明确分离。
- 地区主表 812 条记录全部为 `unknown` 层级，城市名称同时存在简称、带“市”后缀和“省+市”形式；旧城市月度视图把 `province` 固定为空。
- 地区页面直接依赖旧主视图，缺少视图/空数据/只读云端保护，也没有分页的 SQL 聚合入口。

## 自动审核结果

执行前遗留 `pending` 任务 7,435 条。执行 `policydb review auto` 后：

| 状态 | 数量 |
|---|---:|
| auto_recovered_secondary | 1,271 |
| auto_repaired_segmentation | 348 |
| auto_reparsed | 241 |
| auto_verified | 1,781 |
| rejected（结构/重复/过度推断） | 3,122 |
| pending_diagnosis（等待来源或第二轮） | 579 |
| manual_review_required | 93 |

自动处理 6,763 条，自动处理率 90.96%；明确人工兜底率 1.25%。T4 自动关联保存为
`auto_t4_links.parquet` 覆盖层，共 1,286 个特征单元格、306 个来源行；不覆盖原有
`policy_features.parquet` 人工编码。

本次只抽样回抓 3 个历史 URL，分别遇到远端协议中断、连接失败和 robots.txt 拒绝，
因此新增 Raw 正文快照为 0。失败未修改 Raw、记录正文或旧来源关系。来源恢复框架和
本地 Mock 官方来源测试均已通过，待管理员启用审查后的官方来源注册表再批量运行。

## 地区视图验证

```sql
DESCRIBE v_city_month_policy_panel;
SELECT COUNT(*) FROM v_city_month_policy_panel;
SELECT * FROM v_city_month_policy_panel LIMIT 10;
```

- 视图存在，含 21 个字段，计数 2,113 行。
- 地区关系：城市 2,173、区县 314、全国 312、省级 211、县级市 9、未知 14。
- 城市月度面板覆盖 31 个非空省级名称和 334 个城市名称；省份不再固定为空。
- 官方比例统一使用数值 `CASE WHEN ... THEN 1.0 ELSE 0.0 END`，不存在 `AVG(BOOLEAN)`。
- Streamlit 地区页面提供全国、省级、地级市筛选、数据库端聚合、30 条分页、空数据提示、
  缺视图修复命令、只读云端 Parquet 重建和可选天地图。

## 验证

- `ruff check .`：通过。
- `pytest`：94 项测试通过。
- Streamlit 内置浏览器：数据总览、地区比较、人工审核中心均成功打开；控制台无错误或警告。
- 种子 Excel SHA-256：`829951C7E88EEBDFFD729B96C1AAC7CCF4C37037A11E6F8DF8DA9E36605039BF`，未变化。
