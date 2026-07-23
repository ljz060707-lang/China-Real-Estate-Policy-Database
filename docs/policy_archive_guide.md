# 政策原文档案指南

默认档案根目录为 `D:\Data Set\CRPD`，可用 `POLICYDB_ARCHIVE_ROOT` 或个人设置中的非敏感路径
修改。目标盘不可用时任务失败，不会静默改存 C 盘。

```powershell
uv run policydb archive sync
uv run policydb archive audit
```

档案以 SHA-256 内容寻址，目录为 `raw/pdf`、`raw/html`、`raw/text` 和 `raw/attachments`。
项目 Raw 永不修改；同步先复制到临时文件、校验 hash，再 `os.replace`。同一 hash 只保存一份
物理文件。数据库只保存相对路径，`metadata/` 和 `manifests/` 保存来源和完整性结果。

`outputs/archive/archive_coverage_report.csv` 列出每个文档版本的 `archived`、
`missing_source_file`、`invalid_expected_hash` 或 `hash_mismatch` 状态。只有 `archived` 才能计入
正式归档率。
