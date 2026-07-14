# 抓取与增量更新

来源默认 `crawl_enabled: false`。人工核验来源列表页、robots.txt和网站条款后，才在
`data/reference/source_registry.yaml` 中启用。

```bash
uv run policydb sources bootstrap-from-excel "data/raw/seed/【中金不动产与空间服务】政策数据库 20260705.xlsx"
uv run policydb crawl backfill --scope large-cities-105 --from 2018-01-01 --to today --official-first
uv run policydb crawl update --scope large-cities-105
uv run policydb crawl audit --scope large-cities-105
```

通用适配器覆盖政府列表页、站内检索、普通列表、PDF及官方微信线索。105个城市不复制105份代码；差异通过来源注册表配置。

抓取器遵守robots.txt，按域名限速，使用超时、有限重试和指数退避。响应保存至
`data/raw/webpages/YYYY/MM/`，同时保存请求URL、最终URL、HTTP状态、Content-Type、时间和SHA-256元数据。相同响应哈希不重复写入；内容变化新增文档版本，不覆盖旧版本。

GitHub Actions会将Raw网页快照包含在滚动PR中。大文件网站应改用经审核的对象存储并把不可变对象清单纳入Release；不得仅依赖临时runner磁盘。

