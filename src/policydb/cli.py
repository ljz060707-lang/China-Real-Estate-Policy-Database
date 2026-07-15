from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Annotated

import typer

from policydb.api import PolicyDB
from policydb.crawl.pipeline import CrawlPipeline
from policydb.enrich.glm import GLMEnricher
from policydb.export.excel_compatible import export_excel_compatible
from policydb.export.release import create_release
from policydb.geography import materialize_geography
from policydb.ingest.excel import import_excel, inventory_excel
from policydb.query.database import build_database
from policydb.recovery import recover_review_sources
from policydb.review import apply_corrections, generate_review_tasks
from policydb.review_automation import automate_review_tasks
from policydb.scope import materialize_city_scope
from policydb.settings import Settings
from policydb.sources import bootstrap_sources_from_excel
from policydb.transform.collections import build_collection_layer
from policydb.transform.t4_matching import build_t4_match_candidates
from policydb.validate.quality import validate as validate_db

app = typer.Typer(no_args_is_help=True, help="中国房地产与城市政策研究数据库")
review_app = typer.Typer(no_args_is_help=True, help="生成、处理和应用人工审核任务")
sources_app = typer.Typer(no_args_is_help=True, help="管理政策来源注册表")
crawl_app = typer.Typer(no_args_is_help=True, help="断点续跑的政策网页抓取")
enrich_app = typer.Typer(no_args_is_help=True, help="可选的结构化模型辅助提取")
app.add_typer(review_app, name="review")
app.add_typer(sources_app, name="sources")
app.add_typer(crawl_app, name="crawl")
app.add_typer(enrich_app, name="enrich")


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
def dashboard(port: int = typer.Option(8501, "--port", min=1024, max=65535)):
    s = Settings.discover()
    env = os.environ.copy()
    env.setdefault("POLARS_MAX_THREADS", "2")
    env.setdefault("OMP_NUM_THREADS", "2")
    env.setdefault("ARROW_NUM_THREADS", "2")
    command = [
        str(Path(sys.executable).resolve()),
        "-m",
        "streamlit",
        "run",
        str(s.root / "app" / "dashboard.py"),
        "--server.address=127.0.0.1",
        f"--server.port={port}",
        "--server.fileWatcherType=none",
        "--server.runOnSave=false",
        "--runner.fastReruns=false",
        "--browser.gatherUsageStats=false",
    ]
    typer.echo(f"正在启动稳定模式：http://127.0.0.1:{port}")
    try:
        result = subprocess.run(command, check=False, env=env)
    except KeyboardInterrupt:
        return
    if result.returncode:
        typer.echo(
            f"网页进程已退出（代码 {result.returncode}）。请重新运行命令；"
            "若端口被占用，可增加 --port 8502。",
            err=True,
        )
        raise typer.Exit(1)


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


@app.command("build-city-scope")
def build_city_scope():
    """校验105城市范围并生成适用城市关系及研究视图。"""
    result = materialize_city_scope()
    build_database()
    typer.echo(json.dumps(result, ensure_ascii=False, indent=2))


@app.command("normalize-geography")
def normalize_geography():
    """统一省、市、县级市名称和层级，并重建地区研究视图。"""
    result = materialize_geography()
    build_database()
    typer.echo(json.dumps(result, ensure_ascii=False, indent=2))


@app.command("match-t4")
def match_t4():
    """生成T4到T1的精确/模糊匹配候选；模糊结果不自动应用。"""
    result = build_t4_match_candidates()
    build_database()
    typer.echo(json.dumps(result, ensure_ascii=False, indent=2))


@sources_app.command("bootstrap-from-excel")
def sources_bootstrap_from_excel(
    workbook: Annotated[Path | None, typer.Argument()] = None,
):
    """从Excel单元格级Staging提取所有有效URL并生成来源注册表。"""
    result = bootstrap_sources_from_excel(workbook)
    build_database()
    typer.echo(json.dumps(result, ensure_ascii=False, indent=2))


def _date(value: str) -> date:
    return date.today() if value == "today" else date.fromisoformat(value)


@crawl_app.command("backfill")
def crawl_backfill(
    scope: str = typer.Option("large-cities-105", "--scope"),
    from_: str = typer.Option("2018-01-01", "--from"),
    to: str = typer.Option("today", "--to"),
    official_first: bool = typer.Option(True, "--official-first/--no-official-first"),
):
    """按已审核并启用的来源规划和执行历史回溯。"""
    if scope != "large-cities-105":
        raise typer.BadParameter("Only large-cities-105 is configured")
    pipeline = CrawlPipeline()
    plan = pipeline.plan(
        run_type="backfill",
        start_date=_date(from_),
        end_date=_date(to),
        official_first=official_first,
    )
    result = pipeline.run(plan["run_id"])
    build_database()
    typer.echo(json.dumps({**plan, **result}, ensure_ascii=False, indent=2))


@crawl_app.command("update")
def crawl_update(scope: str = typer.Option("large-cities-105", "--scope")):
    """只抓取注册表中已启用来源的增量入口。"""
    if scope != "large-cities-105":
        raise typer.BadParameter("Only large-cities-105 is configured")
    pipeline = CrawlPipeline()
    plan = pipeline.plan(
        run_type="incremental",
        start_date=date.today() - timedelta(days=7),
        end_date=date.today(),
    )
    result = pipeline.run(plan["run_id"])
    build_database()
    typer.echo(json.dumps({**plan, **result}, ensure_ascii=False, indent=2))


@crawl_app.command("audit")
def crawl_audit(scope: str = typer.Option("large-cities-105", "--scope")):
    if scope != "large-cities-105":
        raise typer.BadParameter("Only large-cities-105 is configured")
    typer.echo(json.dumps(CrawlPipeline().audit(), ensure_ascii=False, indent=2))


@enrich_app.command("glm")
def enrich_glm(pending_only: bool = typer.Option(True, "--pending-only/--all")):
    """处理待提取正文；无GLM_API_KEY时仅建立待处理缓存。"""
    _ = pending_only
    result = GLMEnricher().enrich_pending()
    build_database()
    typer.echo(json.dumps(result, ensure_ascii=False, indent=2))


@enrich_app.command("verify")
def enrich_verify():
    """独立复核第一次GLM抽取；最终状态仍由确定性规则决定。"""
    result = GLMEnricher().verify_pending()
    build_database()
    typer.echo(json.dumps(result, ensure_ascii=False, indent=2))


@app.command("export-excel")
def export_excel(
    template: Annotated[Path, typer.Option("--template")],
    output: Annotated[Path, typer.Option("--output")],
):
    typer.echo(
        json.dumps(
            export_excel_compatible(template, output), ensure_ascii=False, indent=2
        )
    )


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
        "other",
    ):
        typer.echo(f"  {review_type}: {result['discovered'].get(review_type, 0)}")
    typer.echo(f"本次新增任务：{result['created_total']}")


@review_app.command("apply")
def review_apply():
    """将已确认的修正应用到 Curated 层并重建 DuckDB。"""
    result = apply_corrections()
    typer.echo(json.dumps(result, ensure_ascii=False, indent=2))


@review_app.command("auto")
def review_auto(
    dry_run: bool = typer.Option(False, "--dry-run", help="仅诊断，不写入Curated修复"),
):
    """自动诊断、修复和分流已有任务，不新增人工任务。"""
    result = automate_review_tasks(apply_repairs=not dry_run)
    typer.echo(json.dumps(result, ensure_ascii=False, indent=2))


@review_app.command("recover-sources")
def review_recover_sources(limit: int = typer.Option(20, "--limit", min=1, max=500)):
    """优先回抓已有URL，再搜索已启用的官方来源注册表。"""
    result = recover_review_sources(limit=limit)
    typer.echo(json.dumps(result, ensure_ascii=False, indent=2, default=str))
