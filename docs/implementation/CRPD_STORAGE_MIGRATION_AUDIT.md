# CRPD Storage Migration Audit

审计运行：`20260810T040408Z`  
阶段：`PHASE_A_ACTIVE_WRITER`

## 生产状态

- full backfill 仍在运行，执行链使用仓库 `.venv` 和 `scripts/CRPD_Audited_Full_Backfill.ps1`。
- 正式数据库：`E:\Data Set\CRPD\database\policydb.duckdb`。
- 当前 writer 存在，正式数据库安全切换门禁为 false。
- autonomous 计划任务保持原状态；本轮未启动、停止或重置。
- Dashboard 已独立迁移到 E runtime 并在本机端口 8501 恢复运行。

实时 PID、城市、来源、分片、事件和任务计划状态以
`CURRENT_PRODUCTION_STATE.json` 为准；不得把本文中的历史 PID 当作当前真值。

## 旧 DuckDB 根因

旧正式库文件虽然位于 E 盘，但 71 个外部 VIEW 固化了
`D:/Data Set/CRPD/curated/*.parquet`。根因是
`src/policydb/query/database.py` 的 `build_database()` 在建库时把当时的
`settings.curated` 绝对路径写入 VIEW SQL。

本轮没有手工 patch 71 条 VIEW，也没有修改正式库。修复的是 builder 和统一 Resolver，并用正式 pipeline 构建私有 candidate。

证据：

- `DUCKDB_EXTERNAL_VIEW_AUDIT.csv`
- `FORMAL_DATABASE_HEALTH_BEFORE.json`
- `DATABASE_INTERFACE_VALIDATION.json`

## 旧路径审计

初始仓库命中 315 条：

| 分类 | 数量 | 处置 |
|---|---:|---|
| ACTIVE_PRODUCTION | 7 | 已窄修默认路径；不影响已启动进程 |
| LEGACY_COMPAT | 239 | 保留并审计，不做机械全局替换 |
| DOC_ONLY | 62 | 保留为历史说明 |
| DEAD_CODE | 7 | 仅登记，Phase B 才可处置 |

生产 `src`、Dashboard config/launcher、当前 crawler 配置和 Storage Resolver 中的旧数据根引用已移除；历史兼容脚本和 `.bak` 仍以审计分类保留。

## D 盘 footprint

Phase A 基线为 5,353.115 MB。主要目录：

| 目录 | MB | 决定 |
|---|---:|---|
| `.venv` | 1,676.788 | 当前生产依赖，保留 |
| `.venv-1` | 987.734 | 待证明无人引用 |
| `data` | 929.579 | 有近期生产依赖，`SKIP_ACTIVE` |
| `outputs` | 565.276 | 待 hash/count 对比 |
| `.git` | 225.492 | 永久保留 |
| `.uv-cache-local` | 105.403 | Phase B 候选 |
| `.test-tmp` | 98.793 | Phase B 候选 |
| `.uv-cache` | 98.414 | Phase B 候选 |

本轮未删除、移动或 quarantine 任何 D 盘资产，节省空间为 0。完整清单见 `D_DRIVE_FOOTPRINT_BEFORE.csv` 和 `D_DRIVE_FOOTPRINT_AFTER.csv`。

## 来源治理旁路审计

刷新审计时 `enabled_unverified=0`。本轮未直接修改来源验证、启用字段或 source registry；来源治理与存储治理继续分离。

## 当前迁移决定

1. D 盘继续承担 code + application + 当前生产 `.venv`。
2. E 盘承担 data + runtime + cache + temp + test artifacts。
3. candidate 数据库验证通过，但不等于正式库已修复。
4. writer 安全退出前，状态保持 `WAITING_FOR_SAFE_DATABASE_SWITCH`。
