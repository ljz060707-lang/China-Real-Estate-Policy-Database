# 文献—算法映射

| 文献依据 | 分析单元 | 测量维度 | 本项目算法 | 验证方式 | 采用状态 | 代码位置 |
|---|---|---|---|---|---|---|
| Schaffrin et al. (2015) | 政策工具/设计要素 | 目标、范围、整合、资源、实施、监测 | D1–D6 的 0/1/2/3/NA rubric；动作级证据 | 双人金标准、维度加权一致性 | 调整采用 | `policydb.intensity.scoring` |
| Schmidt & Sewerin (2019) | 单项工具→政策组合 | 密度和强度 | 动作分数聚合到文件、城市月度 mix | 文件族分组切分、转载去重 | 采用 | `policydb.intensity.aggregate` |
| Alam et al. (2019) | 工具动作/月 | 方向、动作计数、LTV 等水平 | 方向单独保存；旧值/新值确定性配对；gross/net 同时输出 | 数值抽取与配对准确率 | 采用 | `policydb.intensity.rules`, `calibration` |
| Xie et al. (2024) | 政策措施/文件 | 措施、标题、文种 | 文本分类器仅输出候选；权威性不由模型推断 | 模型基准 + 路由决策审计 | 原则采用，公式不复刻 | `policydb.intensity.baselines`, `router` |
| Wang et al. | 句子/条款 | 多标签政策领域 | 词/字 TF-IDF 基线；可选中文 Transformer；GLM 只处理复杂样本 | macro-F1、分城市/时间留出 | 调整采用 | `policydb.intensity.baselines`, `transformer` |
| Gentzkow et al. (2019) | 文本样本 | 监督文本标签 | Logistic Regression 与 Linear SVM 保留为不可删除基线 | 分组外样本、校准曲线、漂移报告 | 采用 | `policydb.intensity.baselines` |
| Zhen et al. (2026) | 句子→政策 | 工具/目标/强度 | 规则预标注、人工金标准、分类器比较、公开原始计数 | 分层人工复核、模型横向比较 | 调整采用 | `policydb.intensity.annotations`, `benchmark` |
| 王羲泽等（2023） | 城市—政策—时间 | 限购力度 | 限购人口/套数/户籍/区域等领域标度 | 城市与时间留出、专家复核 | 领域校准采用 | `policy_calibration_scales.yaml` |

## 八维度映射

| 维度 | 名称 | 证据要求 | 主方法 | 模型角色 |
|---|---|---|---|---|
| D1 | objective_specificity | 明确目标、对象或结果 | 规则 + 分类器 + GLM rubric | GLM 仅解释复杂目标 |
| D2 | scope | 地区、人群、住房/项目范围 | 实体规则 + 分类器 | GLM 检查过度推断 |
| D3 | integration_coordination | 联合部门、跨工具或联动机制 | 机构/连接词规则 + 分类器 | GLM 识别隐含协同 |
| D4 | resource_commitment | 金额、额度、土地、住房、人员资源 | 数值规则优先 | GLM 不得改写数值 |
| D5 | implementation_procedure | 步骤、主体、期限、申报/审批流程 | 条款规则 + 分类器 | GLM 补充复杂流程语义 |
| D6 | monitoring_accountability | 监测、考核、报告、问责 | 词典规则 + 分类器 | GLM 给证据跨度 |
| D7 | bindingness | 必须/应当/不得等约束 | 约束词典 + 文种辅助 | 权威性仍来自来源表 |
| D8 | calibration_magnitude | 旧值、新值、单位、比较方向 | 确定性解析和工具标度 | GLM 只能报告疑似配对 |

融合不是模型原始分数平均。每个任务由 `HybridDecisionRouter` 按确定性证据、模型一致性、阈值和回退规则选择接受值；所有预测继续保留。

