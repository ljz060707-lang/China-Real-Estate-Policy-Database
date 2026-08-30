# CRPD E 盘 canonical layout

本版本将 CRPD 的代码运行根目录统一为 `E:\policy-database`，生产数据根目录统一为
`E:\Data Set\CRPD`。代码和数据分离：代码仓库保存源代码、测试、配置模板、schema 和文档；
原始抓取、DuckDB、Parquet、缓存、日志、checkpoint、审计和运行输出只保存在数据根目录。

## 目录层级

```text
E:\policy-database\              # Git 工作树和固定 .venv
├─ app/                           # Dashboard/application entrypoints
├─ config/                        # non-secret runtime configuration
├─ docs/                          # operational and data-governance docs
├─ scripts/                       # checked launch/monitor/repair scripts
├─ src/                           # policydb package
├─ tests/                         # unit/integration tests
└─ .venv/                         # local runtime, ignored by Git

E:\Data Set\CRPD\               # canonical data/runtime root
├─ raw/                           # immutable raw inputs and fetched objects
├─ database/                      # DuckDB and version metadata
├─ curated/                       # curated tables (where configured)
├─ research/                      # derived research outputs
├─ archive/                       # content-addressed archives
├─ manifests/                     # manifests and checksums
├─ logs/                          # process and API audit logs
├─ outputs/                       # checkpoints, reports and snapshots
├─ control/                       # STOP and control files
└─ jobs/                          # queued operations and locks
```

The repository's `database`, `data\curated`, `data\raw`, and `data\staging` entries are junctions to
the existing E-drive data locations. The migration preserved those links and did not duplicate or
rewrite the production data.

## Runtime contract

The canonical launchers resolve the project root from their own location and set:

```text
CRPD_HOME=E:\policy-database
CRPD_DATA_ROOT=E:\Data Set\CRPD
```

No API key is copied into the repository, YAML, command line, logs, or migration artifacts. Use the
existing user-level environment/SecretStore. `scripts\check_e_drive_layout.ps1` is a read-only
installation check.

## Rollback

The pre-existing export directory was preserved as a timestamped `E:\policy-database_legacy_export_*`
directory. The source worktree was left unchanged during copy and validation. If a rollback is needed,
stop only at a safe application boundary, retain the current E canonical directory, and restore the
legacy export or recopy the source explicitly; never delete production data to roll back code.
