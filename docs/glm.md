# GLM辅助提取

GLM仅辅助相关性、摘要、分类、方向、适用对象、证据片段和城市候选识别。发布时间、发布机构、官方状态、URL、行政代码和文件真实性首先来自确定性网页证据。

本地配置：

```dotenv
GLM_API_KEY=
GLM_MODEL=glm-4-flash
```

运行：

```bash
uv run policydb enrich glm --pending-only
```

输出由Pydantic严格验证并缓存到 `llm_extractions`。缓存键包括content hash、模型、提示词版本和schema版本。未配置密钥时仍保留Raw文档，并建立 `awaiting_api_key` 待处理记录；JSON多次校验失败进入人工审核。密钥不得写入源码、测试、README或日志。
