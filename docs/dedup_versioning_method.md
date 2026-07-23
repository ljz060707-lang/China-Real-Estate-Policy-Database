# 去重与版本方法

实体层级为 `policy_entity → document_version → publication_copy`。判断顺序为文件 SHA-256、
规范正文 hash、文号、标题＋机关＋日期、语义召回、rerank、AI 关系判断和程序路由。

关系包括同政策同版本、转载、修订、本地实施、相关但独立、不重复和不确定。系统不删除重复
记录：官方原文、转载主体、适用地区、URL、版本和来源血缘均保留；相同正文但不同 URL 形成
转载关系，实质修订形成新版本。

```powershell
uv run policydb ai deduplicate
uv run policydb ai audit
```

审计输出为 `outputs/dedup/dedup_audit_report.csv`。详细字段说明见
`docs/ai_dedup_method.md` 和 `docs/dedup_and_versioning.md`。
