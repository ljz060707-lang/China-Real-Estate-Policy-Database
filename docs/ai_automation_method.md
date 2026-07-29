# AI 自动化方法

流程为：确定性相关性筛选 → 第一次动作拆分与分类 → 独立第二轮证据复核 → 程序规则路由。
AI 只能抽取原文存在的动作、分类、方向、对象、地区候选和证据，不能补写日期、文号、机关、
链接或政策正文。

自动通过必须有可定位证据，且两轮结果兼容；无证据、地区冲突、重大修订/废止、疑似误合并、
数值冲突和连续模型失败进入人工兜底。正式动作强度仍由确定性 D1–D8 规则计算。

命令：

```powershell
uv run policydb ai classify --run-id <RUN_ID>
uv run policydb ai verify --run-id <RUN_ID>
uv run policydb review auto
```

当前系统的研究就绪状态仍受人工金标准和留出集基准约束；未达到门槛的结果只能标记为
provisional，不得作为正式因果结论。
