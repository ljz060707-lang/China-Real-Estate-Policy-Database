# CRPD 分支整合审计

审计日期：2026-07-29  
目标仓库：`ljz060707-lang/China-Real-Estate-Policy-Database`  
本地工作树：`D:\Codex\projects\Documents-Codex\2026-07-13\text-20260705-xlsx-text-data-raw\policy-database`

## 1. 真实分支与提交

远端没有名为 `origin` 的 remote；`policy-database` 与 `main` 两个 remote 均指向同一
GitHub 仓库。为避免歧义，本次整合统一使用 `policy-database`。

| 角色 | 真实分支 | HEAD |
|---|---|---|
| 稳定主线 | `main` | `8ba98a62a151eca39187d1a6ae012c3c32557421` |
| V2 | `feat/policydb-v2` | `9ece7ea9d78d80041ddd6abad29369d36fbe2ca6` |
| V3 | `fix/unified-policy-system-v3` | `2904b22e8ef6feafa3d4979242cdce8e334f07e9` |

GitHub Compare 与本地 ancestry 检查一致：

- V2 相对 `main`：ahead 25、behind 0；
- V3 相对 V2：ahead 6、behind 0；
- 实际历史为 `main → V2 → V3`，没有相互分叉。

因此不存在需要在内容层面三方裁决的 merge conflict。仍使用临时整合分支保留审计、
验证和后续功能提交，不直接修改远端 `main`。

## 2. 不可变备份

以下标签已创建并推送：

| 标签 | 指向 |
|---|---|
| `backup/main-before-v3-integration` | 合并前 `main` |
| `backup/v2-before-v3-integration` | 合并前 V2 |
| `backup/v3-before-main-integration` | 合并前 V3 |

审计时工作树无未提交修改，因此没有创建本地修改备份分支或 stash。

## 3. 分支独有内容

### V2：25 个提交，1175 个文件变化

V2 提供主体架构：

- 五类政策动作分类、政策强度与证据约束；
- 内容寻址归档、政策实体、版本、发布副本与重复簇；
- 105 城市覆盖证据、覆盖矩阵与 `complete_confirmed_zero` 语义；
- SiliconFlow Provider、字段置信度、AI 复核；
- Windows 更新调度、统一 Dashboard、迁移脚本与兼容视图；
- 17 个测试文件、30 个文档、3 个 V2 SQL migration。

V2 还提交了大量 Raw、Curated、DuckDB 和输出文件。整合时保留其血缘与验收数据，
后续将大型运行数据迁移到可配置的 `CRPD_DATA_ROOT`，并阻止新增大文件进入 Git。

### V3：6 个提交，38 个文件变化

V3 在 V2 基础上集中修复：

- 抓取范围传递、候选分页、来源轮换和安全上限；
- 城市/省份/主题/来源筛选与覆盖窗口证据；
- Windows Streamlit/Pandas Arrow 稳定性；
- `v_policy_action_center` 的分类、归档与来源血缘；
- V3 分类映射、操作文档和 8 个专项测试。

## 4. 合并取舍

由于 V3 是 V2 的严格后继，不采用批量 `ours`/`theirs`。最终规则如下：

- 抓取、AI、归档、覆盖、调度和统一分类采用 V3 当前实现；
- 保留 V2 migration、字段置信度、覆盖状态、审计文档和兼容视图；
- 保留 `main` 已有安装器、启动器、设置页、CLI 和后台任务入口；
- Raw 永不修改或覆盖；
- 大型数据库、Parquet、PDF、HTML、附件和日志迁出仓库后，不删除历史 Git 对象；
- 未扫描、部分扫描、失败与确认零政策保持严格区分。

## 5. 已知缺口

整合前代码审计确认仍需补齐：

1. `D:\Data Set\CRPD` 目前只有 `raw`、`metadata`、`manifests`，尚未形成统一正式目录；
2. 缺少独立的 `city_source_requirements.yaml` 及五类必需部门逐城市缺口原因；
3. 搜索 Provider fallback、来源自动发现/修复 CLI 尚未覆盖本任务要求；
4. `policy_stock_pool` 与 `policy_increment_review_pool` 尚未形成完整可查询存量；
5. 省份×月份/年份、来源角色、文档归档覆盖视图不完整；
6. README 仍混杂 V1/V2/V3 历史与过时命令；
7. 真实网络全量扫描、真实付费 AI 复核和 Windows 任务安装尚未验收。

## 6. 整合门禁

远端 `main` 只在以下检查通过后更新：

```powershell
uv sync --all-extras
uv run ruff check .
uv run pytest
uv run policydb migrate-v2 verify
uv run policydb validate
uv run policydb ai test
uv run policydb sources validate-registry
```

需要真实密钥或外部网络的检查必须单独标记为“未配置”或“未验证”，不得用本地 fixture
通过代替真实抓取成功。
