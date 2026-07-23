# SiliconFlow Provider 指南

正式 Provider 为 `siliconflow`。密钥只允许来自系统 Keyring、Streamlit Secrets 或环境变量，
不得写入普通 JSON、DuckDB、Parquet、任务请求或日志。

兼容变量：

```text
AI_PROVIDER=siliconflow
SILICONFLOW_BASE_URL=https://api.siliconflow.cn/v1
SILICONFLOW_API_KEY
SILICONFLOW_CHAT_MODEL
SILICONFLOW_VERIFY_MODEL
SILICONFLOW_EMBEDDING_MODEL
SILICONFLOW_RERANK_MODEL
```

在“个人设置 → AI 服务”中保存 Key 后，先请求 `/v1/models`。分类、复核、Embedding 和
Rerank 下拉框只能保存该接口返回的可用模型；不可用模型会阻止新 AI 任务，不会静默替换。

诊断命令：

```powershell
uv run policydb ai test
uv run policydb ai models
uv run policydb ai audit
```

更详细的安全存储、响应脱敏和旧 GLM 兼容说明见
`docs/siliconflow_ai_provider_guide.md`。
