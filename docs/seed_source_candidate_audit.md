# 种子记录反向来源候选审计

本层只把 Curated 中已经存在的真实 `gov.cn` 页面及其 `record—jurisdiction`
关系转成可审计候选。它不联网、不猜测域名、不修改来源注册表，也不自动核验或启用来源。

## 数据表

- `source_candidates.parquet`：每个“城市×来源槽位×规范化 URL”一行的候选摘要。
- `source_candidate_evidence.parquet`：每条候选对应的逐记录、逐地区关系证据。
- `source_candidate_generation_runs.parquet`：输入哈希、算法版本和批次统计。
- `source_requirement_slots.parquet`：固定 105×5=525 个槽位及互斥覆盖状态。

证据表保留 `record_id`、原 URL、规范化 URL、记录标题与日期、原始和标准化地区、
地区关系类型、匹配方法与置信度、来源工作表/单元格、角色判定规则、批次和冲突标记。

## 保守判定

主机名只有等于 `gov.cn` 或以 `.gov.cn` 结尾时才通过官方域名门槛。
带文章扩展名、日期、`content/detail/article` 等特征的 URL 记为
`policy_content_page`，只能作为正文证据，`entry_eligible=false`。

只有 URL 本身呈现站点或栏目入口特征，且有来源注册表或官方主机模式支持角色时，
才登记入口候选。缺少独立部门入口而存在真实市政府入口时，可复制到缺失槽位并标为
`municipal_portal_substitute_candidate`；该候选仍保持未核验、未启用，且不得称为部门官网。

## 覆盖状态

覆盖状态按以下优先级互斥计算：

1. `verified_enabled_source`
2. `enabled_source_pending_verification`
3. `department_entry_candidate`
4. `municipal_portal_substitute_candidate`
5. `content_evidence_only`
6. `other_candidate_pending_review`
7. `no_candidate`

候选覆盖率、正文证据覆盖率和已核验启用覆盖率必须分别报告。

## 命令

```powershell
# 只计算统计，不写 Curated
.\.venv\Scripts\policydb.exe sources seed-record-candidates --dry-run

# 幂等生成候选、证据、槽位状态和三种审计导出
.\.venv\Scripts\policydb.exe sources seed-record-candidates

# 筛选查看
.\.venv\Scripts\policydb.exe sources candidates --city "南京市" --status pending

# 导出全部 CSV、Parquet 和 Excel
.\.venv\Scripts\policydb.exe sources export-candidate-audit

# 单独导出一个筛选结果
.\.venv\Scripts\policydb.exe sources export-candidate-audit `
  --city "南京市" --source-role housing_department `
  --coverage-status content_evidence_only `
  --output outputs/source_candidates/nanjing_housing.xlsx
```

API 使用 `PolicyDB.open().source_candidate_audit(...)`。Dashboard 的“候选来源审核”页只读
展示候选摘要、逐记录证据与冲突，并支持 CSV、Parquet、Excel 下载。
