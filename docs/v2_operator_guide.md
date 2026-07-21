# V2.0 运维与研究使用指南

## 一次性迁移

```powershell
uv run policydb migrate-v2 dry-run
uv run policydb migrate-v2 apply
uv run policydb migrate-v2 verify
uv run policydb confidence build
```

`apply` 会先备份 DuckDB、来源登记和受影响的 Parquet 到 `outputs/v2_migration_backup/<UTC时间>/`，并生成哈希清单。迁移不会读取后重写 Raw；验收锚点仍为 T1 3,011 条、2003-06-05 至 2026-07-02。

## 来源登记

唯一权威来源登记是 `data/reference/source_registry.yaml`。`config/source_registry.yml` 只保留兼容指针，禁止双写。

```powershell
uv run policydb sources validate-registry
uv run policydb sources matrix --output outputs/source_matrix.csv
uv run policydb sources unresolved
uv run policydb sources export-audit --output outputs/source_audit.parquet
```

范围无法由已知代码或结构确定的来源保持 `scope_type=unknown`。请人工补充 `city_ids` 或 `province_codes` 后再运行迁移/物化；不要根据域名文字猜城市。

## 覆盖状态

- `not_scanned`：没有扫描证据；
- `partial`：扫描或抓取不完整；
- `failed`：扫描失败；
- `complete_policy_found`：完整扫描且发现政策；
- `complete_confirmed_zero`：完整扫描、分页/范围证据齐全且未发现政策。

只有 `complete_confirmed_zero` 会在 research-ready 面板中得到政策数 0。其余覆盖不足月份的 `policy_count` 是 null；已知历史记录数量单独保存在 `observed_policy_count`。

```powershell
uv run policydb audit coverage --sample-size 30
uv run policydb validate --group coverage
uv run policydb validate --group research
```

## 分层去重

抓取链路按 L0—L7 记录到 `dedup_decisions`：任务键、规范 URL、条件请求、二进制 SHA、规范正文 SHA、政策身份、相似版本和 GLM 缓存。关键数值冲突永远判为 `material_change`，不能仅凭高文本相似度合并。

GLM 缓存键包含：`normalized_text_hash + model + prompt_version + schema_version`。更换模型、提示或 schema 会产生新的缓存项。

## 字段置信度

`field_confidence` 保存五个确定性评分分量及证据：来源权威 30%、证据覆盖 25%、跨来源一致 20%、抽取确定性 15%、实体匹配 10%。

- `>= 0.85`、官方来源且无冲突：可进入 high；
- `0.65—0.85` 或证据不足：review；
- `< 0.65`、非官方唯一来源或关键冲突：hold。

评分不会反写政策事实，也不会自动批量增加人工任务。只有低置信或冲突且确定性程序无法解决的问题才应进入审核中心。

## 分层更新

```powershell
uv run policydb update daily
uv run policydb update weekly
uv run policydb update monthly
uv run policydb update quarterly
```

参数在唯一配置 `data/reference/update_schedule.yaml`。所有计划任务默认禁用；预览 Windows 任务：

```powershell
powershell -ExecutionPolicy Bypass -File scripts/install_update_schedule.ps1
```

确认后才使用 `-Enable` 并输入 `ENABLE`。删除任务运行 `scripts/remove_update_schedule.ps1`。旧的 `*_update_tasks.ps1` 文件仅保留为兼容入口。

## 分组验证

```powershell
uv run policydb validate --group source
uv run policydb validate --group coverage
uv run policydb validate --group dedup
uv run policydb validate --group confidence
uv run policydb validate --group research
uv run policydb validate --group release
```

报告写入 `outputs/validation/`。覆盖抽样没有完整窗口时，报告必须显示 `not_evaluated_no_complete_windows`，不能把“没有样本”写成通过。

## Streamlit

左侧原“数据质量”菜单保留以兼容旧用户，页内标题升级为“覆盖与质量”，包含“覆盖完整性”“增量与去重”“准确性与置信度”“异常与人工复核”四个 Tab；来源矩阵位于覆盖页内。所有表格查询在 SQL 侧聚合并限制行数，不加载政策全文。
