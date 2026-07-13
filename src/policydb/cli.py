from __future__ import annotations

import json
import subprocess
from pathlib import Path

import typer

from policydb.api import PolicyDB
from policydb.export.release import create_release
from policydb.ingest.excel import import_excel, inventory_excel
from policydb.query.database import build_database
from policydb.review import apply_corrections, generate_review_tasks
from policydb.settings import Settings
from policydb.transform.collections import build_collection_layer
from policydb.validate.quality import validate as validate_db

app = typer.Typer(no_args_is_help=True, help="中国房地产与城市政策研究数据库")
review_app = typer.Typer(no_args_is_help=True, help="生成、处理和应用人工审核任务")
app.add_typer(review_app, name="review")


@app.command()
def init():
    s = Settings.discover()
    for p in (
        "data/raw/documents",
        "data/raw/webpages",
        "data/raw/snapshots",
        "data/staging",
        "data/curated",
        "data/research",
        "data/reference",
        "data/logs",
        "data/releases",
        "database",
        "outputs",
    ):
        (s.root / p).mkdir(parents=True, exist_ok=True)
    typer.echo(f"Initialized {s.root}")


@app.command()
def inventory(path: Path):
    typer.echo(json.dumps(inventory_excel(path), ensure_ascii=False, indent=2))


@app.command("import-excel")
def import_excel_cmd(path: Path):
    typer.echo(json.dumps(import_excel(path), ensure_ascii=False, indent=2, default=str))


@app.command("build-database")
def build_database_cmd():
    typer.echo(build_database())


@app.command()
def validate():
    report = validate_db()
    typer.echo(json.dumps(report, ensure_ascii=False, indent=2))
    if not report["passed"]:
        raise typer.Exit(1)


@app.command()
def search(
    keyword: str | None = None,
    region: str | None = None,
    from_: str | None = typer.Option(None, "--from"),
    to: str | None = typer.Option(None, "--to"),
    official_only: bool = False,
    limit: int = 50,
):
    typer.echo(
        PolicyDB.open().search(
            keyword=keyword,
            region=region,
            start_date=from_,
            end_date=to,
            official_only=official_only,
            limit=limit,
        )
    )


@app.command()
def stats(group_by: str = "year"):
    typer.echo(PolicyDB.open().stats(group_by.split(",")))


@app.command()
def export(
    view: str = typer.Option(..., "--view"),
    format_: str = typer.Option("xlsx", "--format"),
    output: Path = Path("outputs/export.xlsx"),
):
    if output.suffix.lower() != f".{format_}":
        output = output.with_suffix(f".{format_}")
    typer.echo(PolicyDB.open().export(view, output))


@app.command()
def dashboard():
    s = Settings.discover()
    subprocess.run(
        [
            str(Path(__import__("sys").executable).resolve()),
            "-m",
            "streamlit",
            "run",
            str(s.root / "app" / "dashboard.py"),
        ],
        check=True,
    )


@app.command()
def refresh():
    typer.echo(
        "Source registry loaded. All external sources are disabled by default; use manual adapters or enable reviewed sources."
    )


@app.command()
def release(version: str = typer.Option(..., "--version")):
    typer.echo(create_release(version))


@app.command("organize-collections")
def organize_collections():
    """按七大政策库重建工作表和记录级分类关系。"""
    result = build_collection_layer()
    build_database()
    typer.echo(json.dumps(result, ensure_ascii=False, indent=2))


@review_app.command("generate")
def review_generate():
    """扫描当前数据库并生成待审核任务；已审核任务不会被覆盖。"""
    result = generate_review_tasks()
    typer.echo("发现审核问题：")
    for review_type in (
        "missing_title",
        "missing_source",
        "invalid_url",
        "low_confidence",
        "unmatched_t4",
        "unexplained_t2",
        "duplicate_record",
    ):
        typer.echo(f"  {review_type}: {result['discovered'].get(review_type, 0)}")
    typer.echo(f"本次新增任务：{result['created_total']}")


@review_app.command("apply")
def review_apply():
    """将已确认的修正应用到 Curated 层并重建 DuckDB。"""
    result = apply_corrections()
    typer.echo(json.dumps(result, ensure_ascii=False, indent=2))
