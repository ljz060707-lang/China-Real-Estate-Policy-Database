# 种子来源候选运行报告（2026-07-31）

## 结论

本次仅从既有 Curated 种子 URL 与 `record_jurisdictions` 生成派生候选，未联网、
未调用 GLM、未安装计划任务、未修改原始 Excel，也未改变来源注册表的启用状态。

当前生成批次：`SRCSEEDRUN_14AD48B98E0C4D19B312`；算法版本：
`seed-record-jurisdiction-v1.1`。同批次复跑后候选数、证据数和批次 ID 不变。

## 真实统计

| 指标 | 数量 |
|---|---:|
| 105 城×5 类来源槽位 | 525 |
| 种子 URL×城市关系输入 | 1,661 |
| 因非 `gov.cn` 排除 | 573 |
| 带种子证据的候选 | 1,086 |
| 其中纯新增种子候选 | 973 |
| 与既有候选 URL/槽位重叠 | 113 |
| 种子逐记录证据 | 1,088 |
| 唯一证据 ID | 1,088 |
| 关联记录 | 1,088 |
| 唯一种子 URL | 1,086 |
| 有种子证据的城市 | 97 |
| 跨城市/多地区冲突证据 | 0 |
| 正文页（明确拒绝作为入口） | 1,084 |
| 部门/相应官方入口候选 | 2 |
| 市政府统一公开入口替代候选 | 0 |
| 新证据被核验/启用 | 0 / 0 |
| 纯新增种子候选被核验/启用 | 0 / 0 |

113 个重叠项保留任务开始前已有候选的状态，其中 113 个既有候选本来已经启用；
新增 `source_candidate_evidence` 行没有继承该状态，均为未核验、未启用。因而这 113 项
不能计作“新候选被启用”。

## 525 槽位覆盖状态

| 互斥状态 | 槽位数 |
|---|---:|
| 已核验且启用来源 | 0 |
| 已启用但入口证据待核验 | 30 |
| 有部门或对应官方入口候选 | 1 |
| 只有市政府统一公开入口替代候选 | 0 |
| 只有政策正文 URL 证据 | 143 |
| 其他候选待复核 | 0 |
| 无候选 | 351 |

候选总表含既有候选和本次种子候选共 1,239 行、1,204 个唯一 URL。该数字不是
“已核验来源覆盖率”。只有 `verified_enabled_source` 才代表真实核验启用覆盖，本次为 0。

## 角色证据分布

| 角色 | 证据数 |
|---|---:|
| municipal_government | 843 |
| housing_department | 147 |
| provident_fund_center | 96 |
| government_gazette | 1 |
| natural_resources_department | 1 |

角色只使用既有非泛化注册角色或明确官方主机模式；无法证明部门时回落到市政府正文证据。
政策标题中的主题词不会单独作为部门官网证明。

## 没有种子 `gov.cn` 证据的 8 个城市

- 辽宁省鞍山市
- 江苏省昆山市
- 陕西省咸阳市
- 黑龙江省齐齐哈尔市
- 山东省枣庄市
- 浙江省慈溪市
- 山西省长治市
- 福建省晋江市

这些城市的相关槽位保持 `no_candidate`，未填入猜测 URL。

## 产物

- `D:\Data Set\CRPD\curated\source_candidates.parquet`
- `D:\Data Set\CRPD\curated\source_candidate_evidence.parquet`
- `D:\Data Set\CRPD\curated\source_candidate_generation_runs.parquet`
- `D:\Data Set\CRPD\curated\source_requirement_slots.parquet`
- `D:\Data Set\CRPD\outputs\source_candidates\source_candidate_audit.csv`
- `D:\Data Set\CRPD\outputs\source_candidates\source_candidate_audit.parquet`
- `D:\Data Set\CRPD\outputs\source_candidates\source_candidate_audit.xlsx`

DuckDB 已注册 `v_source_candidates`、`v_source_candidate_evidence` 和
`v_source_candidate_generation_runs` 三个只读视图，迁移
`025_seed_source_candidate_audit` 已登记。

## 验证

- 定向候选、证据、冲突、幂等、导出和 525 槽位测试通过。
- Dashboard 六入口和“自动更新与完整性”页面 AppTest 通过。
- 完整 pytest：260 项通过。
- `ruff check .`：通过。
- `git diff --check`：通过。
- `policydb validate`：`passed=true`；28 张工作表、3,568 条记录、主目录 3,011 条、
  105 城唯一，`invalid_zero_count=0`。
