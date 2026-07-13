# GitHub 与 Streamlit Cloud 发布

GitHub Pages 只能托管静态网页，不能直接运行本项目的 Python、Streamlit 和 DuckDB。
推荐结构是：GitHub 保存代码及发布版 DuckDB/Parquet，Streamlit Community Cloud 负责运行网页。

## 发布范围

可以提交：

- `app/`、`src/`、`config/`、`tests/` 和文档；
- `database/policydb.duckdb`；
- `data/staging/` 与 `data/curated/` 中用于复现和测试的 Parquet。

`.gitignore` 已排除：原始 Excel、Raw 文件、正式发布包、虚拟环境、审核日志、人工修正记录和运行输出。

## 首次发布

在 GitHub 新建一个空仓库，然后在项目根目录执行：

```bash
git init
git branch -M main
git add .
git commit -m "publish policy database dashboard"
git remote add origin https://github.com/<用户名>/<仓库名>.git
git push -u origin main
```

进入 Streamlit Community Cloud，选择该仓库并填写：

```text
Branch: main
Main file path: app/dashboard.py
```

在 App settings → Secrets 中加入：

```toml
POLICYDB_ROOT = "."
POLICYDB_READ_ONLY = "1"
POLICYDB_DATA_VERSION = "0.1.0"
```

云端发布版建议只读。Streamlit Community Cloud 的本地磁盘不是永久存储，网页审核结果可能在重启或重新部署后丢失。人工审核应在本地完成，运行 `policydb review apply` 后，再提交更新后的 DuckDB/Parquet 快照。

## 更新发布数据

```bash
uv run policydb review apply
uv run policydb validate
git add database data/curated data/staging
git commit -m "update policy data snapshot"
git push
```

推送后 Streamlit Cloud 会自动重新部署。GitHub Actions 会运行 ruff、pytest 和数据验证。
