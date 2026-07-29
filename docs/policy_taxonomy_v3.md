# 政策动作分类 V3

正式分类单元是 `policy_action`，不是整份文件。每个动作只有一个一级类别，一级类别固定为
`D` 需求侧、`S` 供给侧、`F` 房地产金融与风险、`H` 住房保障与城市更新、`G`
市场监管与制度治理。二级代码为 D01–D11、S01–S12、F01–F11、H01–H16 和 G01–G13。

唯一机器可读 codebook 是 `data/reference/policy_taxonomy_v2.yaml`。文件名因兼容旧调用暂不
改动，但内部版本为 `3.0.0`。`instrument_type`、`direction`、`target_actor`、
`lifecycle_stage` 和适用地区均为正交属性。

中金原 topic 与旧七大库只保存在 `source_topic`、`legacy_collection` 中作为血缘，不参与
前台主分类。确定性 topic 映射不调用 AI；复合或模糊 topic 保留原值并进入证据复核。

分类自动通过须同时满足：Schema 合法、一级唯一、二级属于一级、证据可在原文唯一定位、
两轮判断兼容，且地区与确定性数值规则无冲突。模型自报置信度不能单独触发通过。

生成映射：

```powershell
uv run policydb taxonomy build
```

输出包括 `cicc_topic_inventory.csv`、`cicc_topic_mapping_report.md` 和
`cicc_unmapped_topics.csv`。
