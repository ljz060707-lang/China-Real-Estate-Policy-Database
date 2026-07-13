.PHONY: sync migrate validate test lint dashboard release
sync:
	uv sync --all-extras
migrate:
	uv run policydb import-excel "data/raw/seed/【中金不动产与空间服务】政策数据库 20260705.xlsx"
validate:
	uv run policydb validate
test:
	uv run pytest
lint:
	uv run ruff check .
dashboard:
	uv run policydb dashboard
release:
	uv run policydb release --version 0.1.0
