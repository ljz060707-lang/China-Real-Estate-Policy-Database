# AI 服务与质量状态

生成日期：2026-07-23。

- Provider：SiliconFlow
- `/v1/models` 实际连接：失败（HTTP 401，认证失败）
- 分类模型：未配置
- 独立复核模型：未配置
- Embedding 模型：`BAAI/bge-m3`
- Rerank 模型：`BAAI/bge-reranker-v2-m3`
- 真实分类调用：未执行
- 真实独立复核调用：未执行
- 自动分类率、macro-F1、自动审核通过率：未评估

当前 Keyring 中旧 GLM 兼容密钥不能通过 SiliconFlow 认证。请在“个人设置 → AI模型”保存有效
SiliconFlow Key，并从 `/v1/models` 返回列表中明确选择分类与复核模型。认证失败时系统不会静默
切换模型，也不会生成分类结果。
