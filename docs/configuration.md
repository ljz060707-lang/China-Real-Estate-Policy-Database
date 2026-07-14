# 配置

关键配置文件：

- `config/excel_sheet_map.yaml`：原Excel数据契约；
- `data/reference/cities_105.csv`：版本化城市范围；
- `data/reference/source_registry.yaml`：来源、适配器、优先级、限速和启用状态；
- `data/reference/crawl_keywords.yaml`：需求端、供给端、住房保障与城市更新关键词；
- `config/taxonomy.yml`：七大政策体系和细分类；
- `config/quality_rules.yml`：政策强度与质量规则。

`.env` 仅用于本地，已被Git忽略。复制 `.env.example` 后填写密钥；不要提交 `.env`。

