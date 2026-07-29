# 政策强度系统验收记录

## 已实现并实测

- 文献综述、算法映射、方法决策和版本化 codebook/词典/标度。
- 816/816 个 Curated 来源通过当前 `RegisteredSource` 校验。
- 生成 500 篇文件和 3,000 条款的未标注样本；双人编码和裁决金标准均为 0。
- 本地 100 篇官方状态记录试点：858 个动作、336 个数值候选、7 个确定性旧新值配对。
- 858 个动作生成 6,864 个维度记录和 858 个综合分数；607 formal、251 provisional。
- 城市月度试点 47 行、33 城，覆盖状态为 `partial_coverage`。
- 强度针对性测试 25/25 通过；完整回归 191/191 通过（209 秒）。
- `ruff check .` 通过；`uv run policydb intensity --help`、`literature-audit` 和 `validate` 通过。
- Streamlit AppTest 验证全部页面可渲染；应用内浏览器的 localhost 导航被浏览器安全策略拒绝，未声称完成真实可视点击验收。

## 不能验收为研究就绪

- 人工金标准为 0，TF-IDF Logistic/SVM 和 Transformer 均返回 `blocked_missing_gold`。
- 强度 GLM 已完成 1 个动作的真实抽取和 1 次独立复核；另有 4 次证据偏移校验失败。该小样本只验证接口，不能冒充准确率。
- 动作/工具/方向 F1、数值准确率、Kappa、专家 Spearman 和费用均不可用；已知 Token 至少 2,533，但三次早期失败调用未被旧版计量，故总量不可用。
- 来源扫描窗口和正式去重决策均为 0，未扫描月份不能作为零政策。

最终状态：`experimental`, `research_ready=false`。
