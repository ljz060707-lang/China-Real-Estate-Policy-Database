# 多模型架构

```text
官方/历史文本
  └─ 条款切分
      └─ 确定性规则 ───────┐
      └─ TF-IDF 基线 ──────┤
      └─ 可选 Transformer ─┤→ 任务级路由 → 确定性评分
      └─ 复杂样本 GLM ─────┤
          └─ 独立 GLM 复核 ┘
```

预测写入 `policy_model_predictions`，融合决定写入 `policy_model_decisions`。预测不会因模型升级被覆盖；缓存键包含正文哈希、模型、提示和 schema 版本。Dashboard 不导入 torch 或 transformers，训练只能由显式 CLI 或后台任务触发。

路由器按任务处理：数值规则优先；有证据的一致候选自动接受；冲突进入复核；无证据的 GLM 结果在 Pydantic 层拒绝。GLM 不负责连续总分、来源权威性、覆盖完整性、去重或 research-ready 判定。

