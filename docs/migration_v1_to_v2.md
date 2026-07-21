# V1 到 V2 迁移说明

V2 是兼容迁移，不建立第二套数据库。迁移前会把 DuckDB、来源登记、受影响的 Parquet、人工审核任务统计和验证结果保存到 `outputs/v2_migration_backup/<UTC时间>/`，并记录 SHA-256。

```powershell
uv run policydb migrate-v2 dry-run
uv run policydb migrate-v2 apply
uv run policydb migrate-v2 verify
```

迁移只新增 `crawl_source_windows`、`dedup_decisions`、`field_confidence` 三张核心事实表，并扩展已有抓取、文档版本和模型缓存字段。Raw 永不改写；失败时不删除旧 DuckDB 或 Parquet；重复运行以 schema 和字段存在性判断，保持幂等。

验收锚点为 T1 主目录 3,011 条，日期范围 2003-06-05 至 2026-07-02。任何差异均必须在迁移报告中说明。正式发布版本只在来源范围、完整扫描窗口和全套验收均达标后提升到 2.0.0。
