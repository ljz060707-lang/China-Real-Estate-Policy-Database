# 智能去重与版本识别

去重顺序：文件 SHA-256 → 规范化正文 hash → 文号/标题/机构/日期身份键 → Embedding 候选召回
→ Reranker → AI 关系判断 → 确定性路由。

完全相同的 SHA 或规范化正文可以自动形成 `same_policy_same_version` 簇；身份键相同但正文不同时
必须复核。媒体转载与官方原文是不同 publication，可关联到同一 entity，但不能覆盖彼此。
修订、地方实施和相关但不同文件均保留独立版本。系统从不因去重直接删除记录。

当前存量先生成 `policy_entities.parquet`、`policy_publications.parquet` 和
`policy_duplicate_clusters.parquet`。SiliconFlow 认证或模型未配置时，只运行确定性阶段并明确
标记语义阶段未执行。
