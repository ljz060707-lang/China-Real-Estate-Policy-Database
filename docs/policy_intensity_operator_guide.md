# 政策强度系统操作指南

## 推荐顺序

```powershell
uv run policydb intensity literature-audit
uv run policydb intensity prepare-annotations
uv run policydb intensity score --limit 100 --formal-only
uv run policydb intensity route
uv run policydb intensity aggregate
uv run policydb intensity validate
uv run policydb intensity benchmark
uv run policydb intensity report
```

完成人工裁决后再安装并训练传统模型：

```powershell
uv sync --extra intensity-ml
uv run policydb intensity train-baselines
```

Transformer 是可选的大型依赖，Dashboard 不需要它：

```powershell
uv sync --extra intensity-transformer
uv run policydb intensity train-transformer --model hfl/chinese-macbert-base
```

GLM Key 继续使用现有“个人设置”/系统 Keyring 或 `GLM_API_KEY` 环境变量，密钥不会进入命令参数、Parquet 或报告：

```powershell
uv run policydb intensity glm-extract --limit 50
uv run policydb intensity glm-verify --limit 50
```

网页“政策强度”包含模型比较、一致性、GLM 辅助、人工标注、强度构成和研究质量六个页签。网页只读展示结果，不在主线程训练模型。

