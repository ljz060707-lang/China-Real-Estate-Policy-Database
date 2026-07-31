# 105城市来源候选发现

唯一需求网格来自 `cities_105.csv` 和 `city_source_requirements.yaml`，固定为
105城市 × 5个既有必需角色 = 525槽位。

候选不等于官方来源。发现顺序为：既有注册表/Excel种子、城市政府门户中的机构与友情链接、
省级门户、站内搜索，最后才是可选搜索API。无搜索API时，系统只保存真实页面上出现的链接，
不会拼接或猜测子域名。

核验同时要求：

1. 官方域名证据；
2. 城市归属证据；
3. 部门角色证据；
4. 健康和网络检查。

```powershell
.\.venv\Scripts\policydb.exe sources audit-525
.\.venv\Scripts\policydb.exe sources discover-city --city "南京市"
.\.venv\Scripts\policydb.exe sources candidates --city "南京市"
.\.venv\Scripts\policydb.exe sources verify-candidates --city "南京市"
.\.venv\Scripts\policydb.exe sources export-candidates --output "D:\Data Set\CRPD\outputs\source_candidates.csv"
.\.venv\Scripts\policydb.exe sources seed-record-candidates
.\.venv\Scripts\policydb.exe sources export-candidate-audit
```

无法核验的槽位保持 `unresolved` 并附原因；不得为达到525/525填入假URL。
种子反向候选的字段、覆盖状态和审计口径见
[`seed_source_candidate_audit.md`](seed_source_candidate_audit.md)。
