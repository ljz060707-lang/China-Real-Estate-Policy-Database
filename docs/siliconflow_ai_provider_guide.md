# SiliconFlow AI Provider 使用指南

新任务统一使用 `AI_PROVIDER=siliconflow`。旧的 `GLM_API_KEY`、`GLM_MODEL` 和
`policydb enrich glm/verify` 暂时保留兼容；没有配置 SiliconFlow 时不会静默切换模型或伪造结果。

## 网页配置

打开“个人设置 → AI模型”，填写 SiliconFlow Key、分类模型和独立复核模型。Key 只写入 Windows
Keyring，页面不会回显。模型名留空时 AI 任务会明确报错，不会永久硬编码一个可能下线的模型。
保存后点击“测试连接”；系统调用 `/v1/models`，同时检查已配置模型是否仍可用。

## 环境变量

```dotenv
AI_PROVIDER=siliconflow
SILICONFLOW_API_KEY=
SILICONFLOW_BASE_URL=https://api.siliconflow.cn/v1
SILICONFLOW_CHAT_MODEL=
SILICONFLOW_VERIFY_MODEL=
SILICONFLOW_EMBEDDING_MODEL=BAAI/bge-m3
SILICONFLOW_RERANK_MODEL=BAAI/bge-reranker-v2-m3
```

## 命令

```powershell
uv run policydb ai test
uv run policydb ai models
uv run policydb ai classify --run-id <RUN_ID>
uv run policydb ai verify --run-id <RUN_ID>
uv run policydb ai deduplicate
uv run policydb ai audit
```

`classify` 和 `verify` 默认只处理指定 run 的新增文档版本。输出必须通过 Pydantic 枚举和 JSON
校验；有值的字段必须有原文证据。429、503、504 和网络失败由 SDK 按配置重试，最终失败进入
待处理状态，不产生替代事实。

API 兼容性依据：
[SiliconFlow 模型列表](https://docs.siliconflow.cn/en/api-reference/models/get-model-list)；
[OpenAI Python 客户端](https://github.com/openai/openai-python)。
