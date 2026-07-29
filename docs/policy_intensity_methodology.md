# 房地产政策文本强度方法

## 测量对象

系统测量三个相互分离的对象：`textual_policy_design_intensity`（文本政策设计强度）、`textual_implementation_commitment_intensity`（文本实施承诺强度）和 `instrument_calibration_intensity`（工具校准幅度）。它们不是执行率、政策效果或因果效应。

层级为：Policy Entity → Version → Clause → Action → Evidence Span → Calibration → Dimension Score → City-Month Mix。动作须有逐字证据和字符偏移；新闻背景、政策解读和无新增本地措施的转载不计为新动作。

## 七层管线

1. 文本规范化和条款切分；
2. 确定性规则识别动作、工具、方向、数值与证据；
3. TF-IDF Logistic Regression / Linear SVM 基线；
4. 可选中文 Transformer；
5. GLM 处理复杂语义，输出 0/1/2/3/NA rubric 与证据；
6. 独立自动复核后，由 `HybridDecisionRouter` 按任务优先级决定接受值；
7. 确定性评分和城市月度聚合。

数值规则优先于 GLM；融合器不平均原始模型分数。完整官方正文才可产生 formal 结果，其他文本仅 provisional。

## 分数

D1–D7 映射为 `rubric/3`，只对 applicable 维度加权平均。D8 对已配对的旧值、新值计算：

`magnitude = 1 - exp(-abs((new-old)/tool_scale))`

`tool_scale` 来自版本化领域配置，不用样本最大值。默认综合文本设计强度为：有 D8 时 `0.75 × qualitative + 0.25 × D8`；无 D8 时对定性分数重归一。来源权威调整和数据质量调整另列，不能回写原始文本强度。

## 流量和存量

城市月度流量同时输出政策实体数、动作数、平均/总强度、收紧/放松及净强度、工具多样性。修订、替代和废止更新动作存量，不作为普通重复累加。中央和省级政策按适用范围关系展开用于城市分析，但模型只调用一次，且发布层级仍保留。

## 覆盖语义

`not_scanned`、`partial` 和 `failed` 的指数为 NULL；只有完整扫描且确认无政策的 `confirmed_zero` 才为 0。当前来源窗口为 0，因此试点面板只能标记 `partial_coverage`。

