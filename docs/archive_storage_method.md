# 永久档案存储方法

档案根目录由 `CRPD_ARCHIVE_ROOT`（兼容 `POLICYDB_ARCHIVE_ROOT`）或个人设置读取，默认
`D:\Data Set\CRPD`。业务代码不得硬编码其他路径；根目录不可用时明确失败，不静默回退。

PDF、HTML、TXT 和附件使用内容寻址：

```text
raw/<type>/<sha前两位>/<sha256>.<ext>
```

同 hash 只保存一份物理文件，数据库仅保存相对路径。复制先写临时文件、核验 hash，再原子
替换。Raw 只新增，不覆盖。

```powershell
uv run policydb archive sync
uv run policydb archive audit
uv run policydb archive recover-missing
```

覆盖报告位于 `outputs/archive/archive_coverage_report.csv`。
