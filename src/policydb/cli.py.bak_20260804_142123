from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Annotated

import typer

from policydb.ai import get_ai_provider
from policydb.api import PolicyDB
from policydb.archive import archive_document_versions
from policydb.confidence import materialize_field_confidence
from policydb.coverage import build_city_source_month_coverage
from policydb.coverage_audit import run_coverage_audit
from policydb.crawl.health import disable_unhealthy, enable_recommended, evaluate_sources
from policydb.crawl.pipeline import CrawlPipeline
from policydb.crawl.reconcile import reconcile_run_status
from policydb.dedup_audit import materialize_policy_identity
from policydb.enrich.glm import GLMEnricher
from policydb.exhaustive import ExhaustiveCrawler, export_progress
from policydb.exhaustive_acceptance import build_exhaustive_acceptance
from policydb.export.excel_compatible import export_excel_compatible
from policydb.export.release import create_release
from policydb.geography import materialize_geography
from policydb.ingest.excel import import_excel, inventory_excel
from policydb.intensity.annotations import prepare_annotations
from policydb.intensity.baselines import train_baselines
from policydb.intensity.benchmark import build_benchmark
from policydb.intensity.glm import glm_extract_pending, glm_verify_pending
from policydb.intensity.operations import (
    create_model_review_tasks,
    route_predictions,
    validate_intensity,
)
from policydb.intensity.service import PolicyIntensityService
from policydb.intensity.transformer import train_transformer
from policydb.jobs import CrawlJobRequest, JobManager
from policydb.jobs.worker import run_job
from policydb.migration_v2 import apply_migration, migration_plan, verify_migration
from policydb.network import (
    audit_source_routes,
    compare_routes,
    diagnose_network,
    probe_direct,
    probe_proxy,
)
from policydb.parquet_store import atomic_write_parquet, read_parquet_snapshot
from policydb.policy_pools import materialize_policy_pools
from policydb.query.database import build_database
from policydb.recovery import recover_review_sources
from policydb.review import apply_corrections, generate_review_tasks
from policydb.review_automation import automate_review_tasks
from policydb.schedule import (
    install_windows_schedule,
    remove_windows_schedule,
    schedule_status,
)
from policydb.scope import load_cities_105, materialize_city_scope
from policydb.seed_source_candidates import (
    export_source_candidate_audit,
    generate_candidates_from_seed_records,
)
from policydb.settings import Settings
from policydb.source_discovery import (
    complete_source_matrix,
    discover_all_sources,
    discover_city_sources,
    repair_sources,
)
from policydb.source_quality import export_source_audit, unresolved_sources, validate_registry
from policydb.source_slots import (
    audit_525,
    build_requirement_slots,
    enable_source_strict,
    enable_verified_sources,
    list_candidates,
    probe_candidates,
    promote_candidate,
    promote_verified_candidates,
    reconcile_registry,
    reconcile_registry_roles,
    resolve_slot,
    seed_candidates_from_registry,
    verify_candidates,
)
from policydb.sources import bootstrap_sources_from_excel
from policydb.storage import migrate_storage, storage_plan, verify_storage
from policydb.supervisor import repair_recipe, supervisor_status
from policydb.taxonomy_v2 import build_cicc_mapping, materialize_action_classifications
from policydb.transform.collections import build_collection_layer
from policydb.transform.t4_matching import build_t4_match_candidates
from policydb.update.v2 import build_update_request, start_update
from policydb.validate.quality import validate as validate_db

app = typer.Typer(no_args_is_help=True, help="中国房地产与城市政策研究数据库")
review_app = typer.Typer(no_args_is_help=True, help="生成、处理和应用人工审核任务")
sources_app = typer.Typer(no_args_is_help=True, help="管理政策来源注册表")
crawl_app = typer.Typer(no_args_is_help=True, help="断点续跑的政策网页抓取")
enrich_app = typer.Typer(no_args_is_help=True, help="可选的结构化模型辅助提取")
jobs_app = typer.Typer(no_args_is_help=True, help="后台抓取任务")
report_app = typer.Typer(no_args_is_help=True, help="运行报告")
migrate_v2_app = typer.Typer(no_args_is_help=True, help="V2 schema migration")
update_app = typer.Typer(no_args_is_help=True, help="Layered V2 updates")
confidence_app = typer.Typer(no_args_is_help=True, help="Field evidence confidence")
audit_app = typer.Typer(no_args_is_help=True, help="V2 coverage and quality audits")
intensity_app = typer.Typer(
    no_args_is_help=True,
    help="房地产政策动作识别、多模型路由和文本强度指数",
)
taxonomy_app = typer.Typer(no_args_is_help=True, help="五类政策动作分类与中金 topic 映射")
ai_app = typer.Typer(no_args_is_help=True, help="SiliconFlow AI 分类、复核与去重")
archive_app = typer.Typer(no_args_is_help=True, help="D盘政策原文与附件内容寻址档案")
schedule_app = typer.Typer(no_args_is_help=True, help="Windows每日、周度和月度自动更新")
coverage_app = typer.Typer(no_args_is_help=True, help="105城市来源—月份完整性")
storage_app = typer.Typer(no_args_is_help=True, help="CRPD外部存储规划、迁移和校验")
network_app = typer.Typer(no_args_is_help=True, help="政府直连与代理/TUN网络诊断")
progress_app = typer.Typer(no_args_is_help=True, help="105城市全量搜索持久化进度")
supervisor_app = typer.Typer(
    no_args_is_help=True, help="Full-run watchdog and repair recipes"
)
app.add_typer(review_app, name="review")
app.add_typer(sources_app, name="sources")
app.add_typer(crawl_app, name="crawl")
app.add_typer(enrich_app, name="enrich")
app.add_typer(jobs_app, name="jobs")
app.add_typer(report_app, name="report")
app.add_typer(migrate_v2_app, name="migrate-v2")
app.add_typer(update_app, name="update")
app.add_typer(confidence_app, name="confidence")
app.add_typer(audit_app, name="audit")
app.add_typer(intensity_app, name="intensity")
app.add_typer(taxonomy_app, name="taxonomy")
app.add_typer(ai_app, name="ai")
app.add_typer(archive_app, name="archive")
app.add_typer(schedule_app, name="schedule")
app.add_typer(coverage_app, name="coverage")
app.add_typer(storage_app, name="storage")
app.add_typer(network_app, name="network")
app.add_typer(progress_app, name="progress")
app.add_typer(supervisor_app, name="supervisor")


@ai_app.command("test")
def ai_test():
    result = get_ai_provider().test()
    typer.echo(json.dumps(result, ensure_ascii=False, indent=2))
    if not result["connected"]:
        raise typer.Exit(1)


@ai_app.command("models")
def ai_models():
    typer.echo("\n".join(get_ai_provider().models()))


@ai_app.command("classify")
def ai_classify(
    run_id: str | None = typer.Option(None, "--run-id"),
):
    result = GLMEnricher().enrich_pending(run_id=run_id)
    typer.echo(json.dumps(result, ensure_ascii=False, indent=2))


@ai_app.command("verify")
def ai_verify(
    run_id: str | None = typer.Option(None, "--run-id"),
):
    result = GLMEnricher().verify_pending(run_id=run_id)
    typer.echo(json.dumps(result, ensure_ascii=False, indent=2))


@ai_app.command("queue-dry-run")
def ai_queue_dry_run(
    run_id: str | None = typer.Option(None, "--run-id"),
):
    """Report hard-eligible AI documents without opening a paid API call."""
    settings = Settings.discover()
    enricher = GLMEnricher(settings, api_key=None)
    eligible = enricher._pending_versions(run_id=run_id, require_archive=True)
    checks_path = settings.curated / "archive_integrity_checks.parquet"
    checks = __import__("polars").read_parquet(checks_path) if checks_path.exists() else None
    result = {
        "run_id": run_id,
        "eligible_documents": eligible.height,
        "eligible_document_version_ids": eligible["document_version_id"].to_list() if eligible.height else [],
        "archive_integrity_available": checks is not None,
        "api_call_started": False,
        "api_key_configured": bool(settings.siliconflow_api_key),
        "gate": "archived_hash_verified_nonempty_text",
    }
    typer.echo(json.dumps(result, ensure_ascii=False, indent=2, default=str))


@ai_app.command("deduplicate")
def ai_deduplicate():
    result = materialize_policy_identity()
    result["semantic_ai_status"] = (
        "configured_unverified"
        if Settings.discover().siliconflow_api_key
        else "awaiting_api_key"
    )
    result["records_deleted"] = 0
    typer.echo(json.dumps(result, ensure_ascii=False, indent=2))


@ai_app.command("route-pools")
def ai_route_pools():
    result = materialize_policy_pools()
    build_database()
    typer.echo(json.dumps(result, ensure_ascii=False, indent=2))


@ai_app.command("audit")
def ai_audit():
    settings = Settings.discover()
    result = {
        "provider": settings.ai_provider,
        "api_key_configured": bool(settings.siliconflow_api_key),
        "chat_model": settings.siliconflow_chat_model or None,
        "verify_model": settings.siliconflow_verify_model or None,
        "embedding_model": settings.siliconflow_embedding_model,
        "rerank_model": settings.siliconflow_rerank_model,
    }
    typer.echo(json.dumps(result, ensure_ascii=False, indent=2))


@archive_app.command("sync")
def archive_sync():
    result = archive_document_versions()
    build_database()
    typer.echo(json.dumps(result, ensure_ascii=False, indent=2))


@archive_app.command("audit")
def archive_audit():
    settings = Settings.discover()
    path = settings.curated / "archive_integrity_checks.parquet"
    if not path.exists():
        typer.echo('{"status":"not_run"}')
        raise typer.Exit(1)
    import polars as pl

    frame = read_parquet_snapshot(path)
    result = {
        "checked": frame.height,
        "archived": frame.filter(pl.col("archive_status") == "archived").height,
        "failed": frame.filter(pl.col("archive_status") != "archived").height,
    }
    typer.echo(json.dumps(result, ensure_ascii=False, indent=2))


@archive_app.command("recover-missing")
def archive_recover_missing():
    """重新检查缺失归档；只新增内容寻址文件，不覆盖已有归档。"""
    result = archive_document_versions()
    build_database()
    typer.echo(json.dumps(result, ensure_ascii=False, indent=2))


@coverage_app.command("build")
def coverage_build():
    result = build_city_source_month_coverage()
    result["source_requirement_matrix"] = complete_source_matrix()
    typer.echo(json.dumps(result, ensure_ascii=False, indent=2))


@storage_app.command("plan-migration")
def storage_plan_migration(
    target: Annotated[Path, typer.Option("--target")] = Path(r"D:\Data Set\CRPD"),
):
    typer.echo(json.dumps(storage_plan(target=target), ensure_ascii=False, indent=2))


@storage_app.command("migrate")
def storage_migrate(
    target: Annotated[Path, typer.Option("--target")] = Path(r"D:\Data Set\CRPD"),
    confirm: bool = typer.Option(False, "--confirm"),
):
    typer.echo(json.dumps(migrate_storage(target=target, confirm=confirm), ensure_ascii=False, indent=2))


@storage_app.command("verify")
def storage_verify(
    target: Annotated[Path, typer.Option("--target")] = Path(r"D:\Data Set\CRPD"),
):
    result = verify_storage(target=target)
    typer.echo(json.dumps(result, ensure_ascii=False, indent=2))
    if not result["passed"]:
        raise typer.Exit(1)


@schedule_app.command("status")
def schedule_status_cmd():
    typer.echo(json.dumps(schedule_status(), ensure_ascii=False, indent=2))


@schedule_app.command("install-windows")
def schedule_install_windows(
    confirm: bool = typer.Option(False, "--confirm", help="确认写入 Windows 任务计划"),
):
    result = install_windows_schedule(confirm=confirm)
    typer.echo(json.dumps(result, ensure_ascii=False, indent=2))
    if result["confirmation_required"]:
        typer.echo("预览完成。确认后重新运行并增加 --confirm。")


@schedule_app.command("install")
def schedule_install(
    confirm: bool = typer.Option(False, "--confirm", help="确认写入 Windows 任务计划"),
):
    """`install-windows` 的统一兼容入口。"""
    schedule_install_windows(confirm)


@schedule_app.command("remove-windows")
def schedule_remove_windows(
    confirm: bool = typer.Option(False, "--confirm", help="确认删除 Windows 任务计划"),
):
    typer.echo(
        json.dumps(
            remove_windows_schedule(confirm=confirm), ensure_ascii=False, indent=2
        )
    )


@schedule_app.command("uninstall")
def schedule_uninstall(
    confirm: bool = typer.Option(False, "--confirm", help="确认删除 Windows 任务计划"),
):
    """`remove-windows` 的统一兼容入口。"""
    schedule_remove_windows(confirm)


def _schedule_run(layer: str) -> None:
    typer.echo(json.dumps(start_update(layer), ensure_ascii=False, indent=2))


@schedule_app.command("run-daily")
def schedule_run_daily():
    _schedule_run("daily")


@schedule_app.command("run-weekly")
def schedule_run_weekly():
    _schedule_run("weekly")


@schedule_app.command("run-monthly")
def schedule_run_monthly():
    _schedule_run("monthly")


@taxonomy_app.command("build")
def taxonomy_build():
    result = {
        "actions": materialize_action_classifications(),
        "cicc_topics": build_cicc_mapping(),
    }
    build_database()
    typer.echo(json.dumps(result, ensure_ascii=False, indent=2))


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
def validate(group: str = typer.Option("all", "--group")):
    report = validate_db(group=group)
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


@sources_app.command("discover-city")
def sources_discover_city(
    city: str = typer.Option(..., "--city"),
    apply: bool = typer.Option(False, "--apply", help="只登记官方候选且保持禁用"),
):
    typer.echo(json.dumps(discover_city_sources(city, apply=apply), ensure_ascii=False, indent=2))


@sources_app.command("discover-all")
def sources_discover_all(
    apply: bool = typer.Option(False, "--apply", help="只登记官方候选且保持禁用"),
    city_limit: int | None = typer.Option(None, "--city-limit", min=1, max=105),
):
    typer.echo(json.dumps(discover_all_sources(apply=apply, city_limit=city_limit), ensure_ascii=False, indent=2))


@sources_app.command("complete-matrix")
def sources_complete_matrix():
    typer.echo(json.dumps(complete_source_matrix(), ensure_ascii=False, indent=2))


@sources_app.command("health-all")
def sources_health_all():
    typer.echo(json.dumps(evaluate_sources(), ensure_ascii=False, indent=2))


@sources_app.command("repair")
def sources_repair():
    typer.echo(json.dumps(repair_sources(), ensure_ascii=False, indent=2))


@sources_app.command("candidates")
def sources_candidates(
    city: str | None = typer.Option(None, "--city"),
    status: str | None = typer.Option(None, "--status"),
):
    """查看来源候选；候选不等于已核验或已启用来源。"""
    frame = list_candidates(city=city, status=status)
    typer.echo(frame.write_csv() if frame.height else "未找到符合条件的来源候选。")


@sources_app.command("verify-candidates")
def sources_verify_candidates(
    city: str | None = typer.Option(None, "--city"),
):
    """按官方域名、城市与部门角色证据执行确定性核验，不自动启用。"""
    typer.echo(
        json.dumps(verify_candidates(city=city), ensure_ascii=False, indent=2)
    )


@sources_app.command("probe-candidates")
def sources_probe_candidates(
    city: str | None = typer.Option(None, "--city"),
    source_id: str | None = typer.Option(None, "--source-id"),
    candidate_id: str | None = typer.Option(None, "--candidate-id"),
    slot_id: str | None = typer.Option(None, "--slot-id"),
    limit: int | None = typer.Option(None, "--limit", min=1),
    rounds: int = typer.Option(2, "--rounds", min=2, max=10),
):
    """Run real network and list-parser probes, then apply verification gates."""
    typer.echo(
        json.dumps(
            probe_candidates(
                city=city,
                source_id=source_id,
                candidate_id=candidate_id,
                slot_id=slot_id,
                limit=limit,
                rounds=rounds,
            ),
            ensure_ascii=False,
            indent=2,
        )
    )


@sources_app.command("seed-registry-entries")
def sources_seed_registry_entries(
    city: str | None = typer.Option(None, "--city"),
    source_id: str | None = typer.Option(None, "--source-id"),
    slot_id: str | None = typer.Option(None, "--slot-id"),
    apply: bool = typer.Option(False, "--apply/--dry-run"),
):
    """Seed only formal registry entries; dry-run is the default."""
    typer.echo(
        json.dumps(
            seed_candidates_from_registry(
                city=city,
                source_id=source_id,
                slot_id=slot_id,
                write=apply,
            ),
            ensure_ascii=False,
            indent=2,
        )
    )


@sources_app.command("promote")
def sources_promote(
    candidate_id: str = typer.Option(..., "--candidate-id"),
):
    """Promote one fully verified reusable entry; the new source remains disabled."""
    typer.echo(
        json.dumps(promote_candidate(candidate_id), ensure_ascii=False, indent=2)
    )


@sources_app.command("promote-verified")
def sources_promote_verified(
    city: str | None = typer.Option(None, "--city"),
    slot_id: str | None = typer.Option(None, "--slot-id"),
):
    """Promote fully probed candidates into the registry, disabled by default."""
    typer.echo(
        json.dumps(
            promote_verified_candidates(city=city, slot_id=slot_id),
            ensure_ascii=False,
            indent=2,
        )
    )


@sources_app.command("enable")
def sources_enable(
    source_id: str = typer.Option(..., "--source-id"),
):
    """Enable one source only after all official, entry, health and role gates pass."""
    typer.echo(
        json.dumps(enable_source_strict(source_id), ensure_ascii=False, indent=2)
    )


@sources_app.command("enable-verified")
def sources_enable_verified(
    city: str | None = typer.Option(None, "--city"),
):
    typer.echo(
        json.dumps(enable_verified_sources(city=city), ensure_ascii=False, indent=2)
    )


@sources_app.command("reconcile")
def sources_reconcile(
    apply: bool = typer.Option(False, "--apply/--dry-run"),
):
    """Audit enabled sources against verified reusable candidates; dry-run by default."""
    typer.echo(
        json.dumps(reconcile_registry(apply=apply), ensure_ascii=False, indent=2)
    )


@sources_app.command("disable-invalid-entries")
def sources_disable_invalid_entries():
    typer.echo(
        json.dumps(reconcile_registry(apply=True), ensure_ascii=False, indent=2)
    )


@sources_app.command("reconcile-registry")
def sources_reconcile_registry(
    apply: bool = typer.Option(False, "--apply/--dry-run"),
):
    typer.echo(
        json.dumps(reconcile_registry(apply=apply), ensure_ascii=False, indent=2)
    )


@sources_app.command("reconcile-roles")
def sources_reconcile_roles(
    apply: bool = typer.Option(False, "--apply/--dry-run"),
):
    """Correct high-confidence organization role mismatches; dry-run by default."""
    typer.echo(
        json.dumps(reconcile_registry_roles(apply=apply), ensure_ascii=False, indent=2)
    )


@sources_app.command("resolve-slot")
def sources_resolve_slot(
    slot_id: str = typer.Option(..., "--slot-id"),
    candidate_id: str | None = typer.Option(None, "--candidate-id"),
    note: str | None = typer.Option(None, "--note"),
):
    typer.echo(
        json.dumps(
            resolve_slot(slot_id, candidate_id=candidate_id, note=note),
            ensure_ascii=False,
            indent=2,
        )
    )


@sources_app.command("export-candidates")
def sources_export_candidates(
    output: Annotated[Path, typer.Option("--output")],
    city: str | None = typer.Option(None, "--city"),
    status: str | None = typer.Option(None, "--status"),
):
    frame = list_candidates(city=city, status=status)
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.suffix.lower() == ".parquet":
        atomic_write_parquet(frame, output, {"job_id": "cli-export-candidates"})
    elif output.suffix.lower() == ".xlsx":
        frame.write_excel(output, autofit=True)
    else:
        frame.write_csv(output)
    typer.echo(str(output.resolve()))


@sources_app.command("seed-record-candidates")
def sources_seed_record_candidates(
    dry_run: bool = typer.Option(False, "--dry-run"),
):
    """从真实种子URL及记录—地区关系生成未核验、未启用候选。"""
    typer.echo(
        json.dumps(
            generate_candidates_from_seed_records(write=not dry_run),
            ensure_ascii=False,
            indent=2,
        )
    )


@sources_app.command("export-candidate-audit")
def sources_export_candidate_audit(
    output: Annotated[Path | None, typer.Option("--output")] = None,
    city: str | None = typer.Option(None, "--city"),
    source_role: str | None = typer.Option(None, "--source-role"),
    coverage_status: str | None = typer.Option(None, "--coverage-status"),
):
    """导出城市×槽位×候选审计；未提供路径时同时生成三种格式。"""
    typer.echo(
        json.dumps(
            export_source_candidate_audit(
                output,
                city=city,
                source_role=source_role,
                coverage_status=coverage_status,
            ),
            ensure_ascii=False,
            indent=2,
        )
    )


@sources_app.command("audit-525")
def sources_audit_525(
    seed_registry: bool = typer.Option(
        True, "--seed-registry/--no-seed-registry"
    ),
):
    """构建105×5槽位并从既有注册表提取候选，绝不填充猜测URL。"""
    build_requirement_slots()
    if seed_registry:
        seed_candidates_from_registry()
    ExhaustiveCrawler().rebuild_progress()
    typer.echo(json.dumps(audit_525(), ensure_ascii=False, indent=2))


@sources_app.command("evaluate")
def sources_evaluate(limit: int | None = typer.Option(None, "--limit")):
    """检测来源入口、解析能力与健康评分；网络访问只发生在显式运行时。"""
    typer.echo(json.dumps(evaluate_sources(limit=limit), ensure_ascii=False, indent=2))


@sources_app.command("enable-recommended")
def sources_enable_recommended(limit: int = typer.Option(20, "--limit", min=1, max=100)):
    """启用已经过体检并标记为推荐的有限来源。"""
    typer.echo(json.dumps(enable_recommended(limit=limit), ensure_ascii=False, indent=2))


@sources_app.command("disable-unhealthy")
def sources_disable_unhealthy():
    typer.echo(json.dumps(disable_unhealthy(), ensure_ascii=False, indent=2))


@sources_app.command("validate-registry")
def sources_validate_registry():
    result = validate_registry()
    typer.echo(json.dumps(result, ensure_ascii=False, indent=2))
    if not result["passed"]:
        raise typer.Exit(1)


@sources_app.command("matrix")
def sources_matrix(
    output: Annotated[Path, typer.Option("--output")] = Path("outputs/source_matrix.csv"),
):
    typer.echo(json.dumps(export_source_audit(output), ensure_ascii=False, indent=2))


@sources_app.command("coverage-matrix")
def sources_coverage_matrix(
    output: Annotated[Path, typer.Option("--output")] = Path("outputs/source_matrix.csv"),
):
    """兼容 V2 规范名称，输出由唯一来源登记推导的城市—来源矩阵。"""
    typer.echo(json.dumps(export_source_audit(output), ensure_ascii=False, indent=2))


@sources_app.command("unresolved")
def sources_unresolved():
    typer.echo(unresolved_sources().write_csv())


@sources_app.command("export-audit")
def sources_export_audit(
    output: Annotated[Path, typer.Option("--output")] = Path("outputs/source_audit.parquet"),
):
    typer.echo(json.dumps(export_source_audit(output), ensure_ascii=False, indent=2))


@migrate_v2_app.command("dry-run")
def migrate_v2_dry_run():
    typer.echo(json.dumps(migration_plan(), ensure_ascii=False, indent=2))


@migrate_v2_app.command("apply")
def migrate_v2_apply():
    result = apply_migration()
    if result["verified"]:
        build_database()
    typer.echo(json.dumps(result, ensure_ascii=False, indent=2))
    if not result["verified"]:
        raise typer.Exit(1)


@migrate_v2_app.command("verify")
def migrate_v2_verify():
    result = verify_migration()
    typer.echo(json.dumps(result, ensure_ascii=False, indent=2))
    if not result["verified"]:
        raise typer.Exit(1)


@confidence_app.command("build")
def confidence_build():
    result = materialize_field_confidence()
    build_database()
    typer.echo(json.dumps(result, ensure_ascii=False, indent=2))


@audit_app.command("coverage")
def audit_coverage(
    sample_size: Annotated[int, typer.Option("--sample-size", min=1, max=500)] = 30,
):
    typer.echo(
        json.dumps(
            run_coverage_audit(sample_size=sample_size),
            ensure_ascii=False,
            indent=2,
            default=str,
        )
    )


@audit_app.command("exhaustive")
def audit_exhaustive():
    typer.echo(
        json.dumps(
            build_exhaustive_acceptance(),
            ensure_ascii=False,
            indent=2,
            default=str,
        )
    )


def _start_layered_update(layer: str) -> None:
    typer.echo(json.dumps(start_update(layer), ensure_ascii=False, indent=2))


@update_app.command("daily")
def update_daily():
    _start_layered_update("daily")


@update_app.command("weekly")
def update_weekly():
    _start_layered_update("weekly")


@update_app.command("monthly")
def update_monthly():
    _start_layered_update("monthly")


@update_app.command("quarterly")
def update_quarterly():
    _start_layered_update("quarterly")


@update_app.command("plan")
def update_plan(mode: str = typer.Option(..., "--mode")):
    """只生成轻量计划，不访问网络或创建后台任务。"""
    request = build_update_request(mode)
    typer.echo(json.dumps(request.model_dump(mode="json"), ensure_ascii=False, indent=2))


@update_app.command("run")
def update_run(mode: str = typer.Option(..., "--mode")):
    _start_layered_update(mode)


@update_app.command("status")
def update_status(limit: int = typer.Option(10, "--limit", min=1, max=100)):
    states = [state.model_dump(mode="json") for state in JobManager().list_states(limit)]
    typer.echo(json.dumps(states, ensure_ascii=False, indent=2))


@update_app.command("report")
def update_report(run_id: str = typer.Option(..., "--run-id")):
    manager = JobManager()
    match = next(
        (
            state
            for state in manager.list_states(limit=1000)
            if state.run_id == run_id or state.job_id == run_id
        ),
        None,
    )
    if match is None:
        raise typer.BadParameter(f"找不到 run/job：{run_id}")
    report = manager.job_dir(match.job_id) / "report.json"
    if not report.exists():
        raise typer.BadParameter(f"报告尚未生成：{report}")
    typer.echo(report.read_text(encoding="utf-8"))


def _date(value: str) -> date:
    if value == "today":
        return date.today()
    if value == "overlap3":
        return date.today() - timedelta(days=3)
    return date.fromisoformat(value)


@network_app.command("diagnose")
def network_diagnose(
    city: str | None = typer.Option(None, "--city"),
    url: str | None = typer.Option(None, "--url"),
):
    """比较环境代理、完全直连与curl --noproxy；输出不含密钥。"""
    settings = Settings.discover()
    if url is None:
        if city:
            crawler = ExhaustiveCrawler(settings)
            city_row = crawler.resolve_city(city)
            from policydb.crawl.registry import load_registry

            sources = [
                source
                for source in load_registry(settings)
                if city_row["city_id"] in source.city_ids
            ]
            url = next(
                (
                    item
                    for source in sources
                    for item in [
                        source.homepage_url,
                        *source.list_page_urls,
                        *source.seed_urls,
                    ]
                    if item
                ),
                None,
            )
        if url is None:
            raise typer.BadParameter(
                "请提供 --url，或提供注册表中已有来源的 --city。"
            )
    typer.echo(
        json.dumps(
            diagnose_network(url=url, city=city, settings=settings),
            ensure_ascii=False,
            indent=2,
        )
    )


@network_app.command("probe-proxy")
def network_probe_proxy(
    url: str = typer.Option("https://github.com", "--url"),
    proxy_url: str | None = typer.Option(None, "--proxy-url", hidden=True),
):
    """Detect HTTP CONNECT versus SOCKS5H without printing proxy credentials."""
    typer.echo(json.dumps(probe_proxy(url=url, proxy_url=proxy_url), ensure_ascii=False, indent=2))


@network_app.command("probe-direct")
def network_probe_direct(url: str = typer.Option(..., "--url")):
    """Probe with Python proxy inheritance disabled and curl --noproxy."""
    typer.echo(json.dumps(probe_direct(url=url), ensure_ascii=False, indent=2))


@network_app.command("compare")
def network_compare(
    url: str = typer.Option(..., "--url"),
    proxy_url: str | None = typer.Option(None, "--proxy-url", hidden=True),
):
    typer.echo(json.dumps(compare_routes(url=url, proxy_url=proxy_url), ensure_ascii=False, indent=2))


@network_app.command("audit-sources")
def network_audit_sources(
    city: str | None = typer.Option(None, "--city"),
    enabled_only: bool = typer.Option(True, "--enabled-only/--all"),
    limit: int | None = typer.Option(None, "--limit", min=1),
):
    typer.echo(
        json.dumps(
            audit_source_routes(city=city, enabled_only=enabled_only, limit=limit),
            ensure_ascii=False,
            indent=2,
        )
    )


@supervisor_app.command("status")
def supervisor_status_command(
    stale_minutes: int = typer.Option(30, "--stale-minutes", min=1),
):
    result = supervisor_status(stale_minutes=stale_minutes)
    typer.echo(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    if not result["healthy"]:
        raise typer.Exit(2)


@supervisor_app.command("repair-recipe")
def supervisor_repair_recipe(
    status: str = typer.Option(..., "--status"),
    city: str | None = typer.Option(None, "--city"),
    run_id: str | None = typer.Option(None, "--run-id"),
):
    typer.echo(
        json.dumps(
            repair_recipe(status, city=city, run_id=run_id),
            ensure_ascii=False,
            indent=2,
        )
    )


@progress_app.command("status")
def progress_status(city: str | None = typer.Option(None, "--city")):
    crawler = ExhaustiveCrawler()
    crawler.rebuild_progress()
    typer.echo(
        json.dumps(
            crawler.city_status(city),
            ensure_ascii=False,
            indent=2,
            default=str,
        )
    )


@progress_app.command("watch")
def progress_watch(
    city: str | None = typer.Option(None, "--city"),
    interval: float = typer.Option(2.0, "--interval", min=0.5, max=60.0),
):
    """TTY中原位刷新；非TTY输出结构化快照，Ctrl+C安全退出。"""
    import time

    crawler = ExhaustiveCrawler()
    try:
        while True:
            snapshot = crawler.city_status(city)
            typer.echo(
                json.dumps(
                    {
                        "at": datetime.now().isoformat(),
                        "rows": snapshot["rows"],
                        "data": snapshot["data"][:10],
                    },
                    ensure_ascii=False,
                    default=str,
                )
            )
            time.sleep(interval)
    except KeyboardInterrupt:
        typer.echo("已停止监视；抓取任务状态未被修改。")


@progress_app.command("export")
def progress_export(
    format: str = typer.Option("csv", "--format"),
    city: str | None = typer.Option(None, "--city"),
):
    typer.echo(str(export_progress(format=format, city=city)))


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
    _ = official_first
    _job_mode("historical_105", from_, to, 10000, False)


@crawl_app.command("update")
def crawl_update(scope: str = typer.Option("large-cities-105", "--scope")):
    """只抓取注册表中已启用来源的增量入口。"""
    if scope != "large-cities-105":
        raise typer.BadParameter("Only large-cities-105 is configured")
    _job_mode("official_update", "overlap3", "today", 100, False)


@crawl_app.command("audit")
def crawl_audit(scope: str = typer.Option("large-cities-105", "--scope")):
    if scope != "large-cities-105":
        raise typer.BadParameter("Only large-cities-105 is configured")
    typer.echo(json.dumps(CrawlPipeline().audit(), ensure_ascii=False, indent=2))


@crawl_app.command("reconcile-run-status")
def crawl_reconcile_run_status(
    run_id: str | None = typer.Option(None, "--run-id"),
    apply: bool = typer.Option(False, "--apply/--dry-run"),
):
    """Reconcile only terminal-checkpoint runs; dry-run is the default."""
    typer.echo(
        json.dumps(
            reconcile_run_status(run_id=run_id, apply=apply),
            ensure_ascii=False,
            indent=2,
            default=str,
        )
    )


def _job_mode(
    mode: str,
    from_: str,
    to: str,
    max_fetches: int,
    run_glm: bool,
    **request_options,
) -> None:
    settings = Settings.discover()
    manager = JobManager(settings)
    request = CrawlJobRequest(
        mode=mode,
        start_date=_date(from_),
        end_date=_date(to),
        max_fetches=max_fetches,
        run_glm=run_glm,
        **request_options,
    )
    state = manager.create(request)
    result = run_job(state.job_id, settings)
    typer.echo(json.dumps({"job_id": state.job_id, **result}, ensure_ascii=False, indent=2, default=str))


@crawl_app.command("smart")
def crawl_smart(
    from_: str = typer.Option("overlap3", "--from"),
    to: str = typer.Option("today", "--to"),
    max_fetches: int = typer.Option(100, "--max-fetches"),
    run_glm: bool = typer.Option(False, "--glm/--no-glm"),
):
    _job_mode("smart", from_, to, max_fetches, run_glm)


@crawl_app.command("official-update")
def crawl_official_update(
    from_: str = typer.Option("overlap3", "--from"),
    to: str = typer.Option("today", "--to"),
    max_fetches: int = typer.Option(100, "--max-fetches"),
):
    _job_mode("official_update", from_, to, max_fetches, False)


@crawl_app.command("web-discovery")
def crawl_web_discovery(
    from_: str = typer.Option("today", "--from"),
    to: str = typer.Option("today", "--to"),
    max_fetches: int = typer.Option(100, "--max-fetches"),
):
    _job_mode("web_discovery", from_, to, max_fetches, False)


@crawl_app.command("seed-backtrack")
def crawl_seed_backtrack(
    from_: str = typer.Option("2018-01-01", "--from"),
    to: str = typer.Option("today", "--to"),
    max_fetches: int = typer.Option(100, "--max-fetches"),
):
    _job_mode("seed_backtrack", from_, to, max_fetches, False)


@crawl_app.command("recover-missing")
def crawl_recover_missing(max_fetches: int = typer.Option(20, "--max-fetches")):
    _job_mode("recover_missing", "2018-01-01", "today", max_fetches, False)


@crawl_app.command("health")
def crawl_health(limit: int = typer.Option(20, "--limit", min=1)):
    """检查来源健康状态，不创建抓取成功假象。"""
    typer.echo(
        json.dumps(
            evaluate_sources(limit=limit),
            ensure_ascii=False,
            indent=2,
            default=str,
        )
    )


def _split_option(value: str) -> list[str]:
    return [part.strip() for part in value.split(",") if part.strip()]


@crawl_app.command("historical")
def crawl_historical(
    from_: str = typer.Option("2018-01-01", "--from"),
    to: str = typer.Option("today", "--to"),
    cities: str = typer.Option("", "--cities"),
    provinces: str = typer.Option("", "--provinces"),
    topics: str = typer.Option("", "--topics"),
    source_ids: str = typer.Option("", "--source-ids"),
    source_roles: str = typer.Option("", "--source-roles"),
    max_pages_per_source: int = typer.Option(20, "--max-pages-per-source", min=1),
    max_candidates_per_source: int = typer.Option(
        50, "--max-candidates-per-source", min=1
    ),
    max_candidates_total: int = typer.Option(10000, "--max-candidates-total", min=1),
    max_fetches: int = typer.Option(1000, "--max-fetches", min=1),
    resume: bool = typer.Option(True, "--resume/--no-resume"),
):
    """按明确范围创建可恢复的105城市历史任务。"""
    _job_mode(
        "historical_105",
        from_,
        to,
        max_fetches,
        False,
        cities=_split_option(cities),
        provinces=_split_option(provinces),
        topics=_split_option(topics),
        source_ids=_split_option(source_ids),
        source_roles=_split_option(source_roles),
        max_pages_per_source=max_pages_per_source,
        max_candidates_per_source=max_candidates_per_source,
        max_candidates=max_candidates_total,
        max_candidates_total=max_candidates_total,
        global_safety_limit=max_candidates_total,
        resume=resume,
    )


def _exhaustive_progress(current: int, total: int, status: str, shard: dict) -> None:
    percent = (current / total * 100) if total else 100.0
    typer.echo(
        f"[{current}/{total} {percent:5.1f}%] "
        f"{shard['city_name']} {shard['source_role']} "
        f"{shard['start_date']}—{shard['end_date']}: {status}"
    )


def _execute_exhaustive_postprocess(
    crawler: ExhaustiveCrawler,
    result: dict,
    *,
    run_ai: bool,
    archive: bool,
) -> dict:
    """Execute requested work and persist residual counts; never report flags only."""
    run_ids = [str(value) for value in result.get("run_ids", [])]
    postprocess: dict[str, object] = {}
    captured_metrics = result.get("run_metrics", {})
    per_run = {
        run_id: {
            "ai_pending_count": int(
                captured_metrics.get(run_id, {}).get("ai_pending_count", 0)
            ),
            "dedup_pending_count": int(
                captured_metrics.get(run_id, {}).get("dedup_pending_count", 0)
            ),
            "archive_missing_count": int(
                captured_metrics.get(run_id, {}).get("archive_missing_count", 0)
            ),
        }
        for run_id in run_ids
    }
    if archive:
        # Archive only the document versions attached to the current crawl
        # runs.  A global archive rebuild remains an explicit archive command.
        postprocess["archive"] = {
            run_id: archive_document_versions(crawler.settings, run_id=run_id)
            for run_id in run_ids
        }
    if run_ai:
        enricher = GLMEnricher(crawler.settings)
        ai_rows: dict[str, dict] = {}
        for run_id in run_ids:
            classified = enricher.enrich_pending(run_id=run_id)
            verified = enricher.verify_pending(run_id=run_id)
            per_run[run_id]["ai_pending_count"] = int(
                classified.get("awaiting_api_key", 0)
            ) + int(classified.get("failed", 0)) + int(
                verified.get("pending", 0)
            ) + int(verified.get("awaiting_api_key", 0)) + int(
                verified.get("failed", 0)
            )
            ai_rows[run_id] = {"classify": classified, "verify": verified}
        postprocess["ai"] = ai_rows
        postprocess["taxonomy"] = materialize_action_classifications(crawler.settings)
        dedup_rows: dict[str, dict] = {}
        for run_id in run_ids:
            try:
                dedup_rows[run_id] = materialize_policy_identity(
                    crawler.settings, run_id=run_id
                )
            except TypeError as exc:
                # Preserve compatibility with injected legacy test doubles;
                # production implementation always accepts run_id.
                if "run_id" not in str(exc):
                    raise
                dedup_rows[run_id] = materialize_policy_identity(crawler.settings)
        postprocess["dedup"] = dedup_rows
        postprocess["route_pools"] = materialize_policy_pools(crawler.settings)
        postprocess["confidence"] = materialize_field_confidence(crawler.settings)
        postprocess["coverage"] = build_city_source_month_coverage(crawler.settings)
        postprocess["database"] = build_database(crawler.settings)
    postprocess["progress_update"] = crawler.apply_postprocess_metrics(per_run)
    result["postprocess"] = postprocess
    result["run_ai_executed"] = run_ai
    result["archive_executed"] = archive
    result["status"] = crawler.city_status(str(result["city_id"]))
    return result


@crawl_app.command("exhaustive-city")
def crawl_exhaustive_city(
    city: str = typer.Option(..., "--city"),
    from_: str = typer.Option("2018-01-01", "--from"),
    to: str = typer.Option("today", "--to"),
    source_roles: str = typer.Option("", "--source-roles"),
    source_ids: str = typer.Option("", "--source-ids"),
    max_pages_per_source: int = typer.Option(50, "--max-pages-per-source", min=1),
    max_candidates_per_shard: int = typer.Option(
        500, "--max-candidates-per-shard", min=1
    ),
    max_fetches_per_shard: int = typer.Option(
        500, "--max-fetches-per-shard", min=1
    ),
    retry_errors: bool = typer.Option(
        False, "--retry-errors/--no-retry-errors"
    ),
    resume: bool = typer.Option(True, "--resume/--no-resume"),
    run_ai: bool = typer.Option(False, "--run-ai/--no-run-ai"),
    archive: bool = typer.Option(True, "--archive/--no-archive"),
    sequential: bool = typer.Option(True, "--sequential/--no-sequential"),
):
    """逐城市、逐来源、逐月扫描；状态和检查点均写入D盘Curated层。"""
    if not sequential:
        raise typer.BadParameter("当前正式写入仅支持 --sequential，避免并发覆盖。")
    crawler = ExhaustiveCrawler()
    result = crawler.run_city(
        city,
        start_date=_date(from_),
        end_date=_date(to),
        source_roles=_split_option(source_roles),
        source_ids=_split_option(source_ids),
        max_pages_per_source=max_pages_per_source,
        max_candidates_per_shard=max_candidates_per_shard,
        max_fetches_per_shard=max_fetches_per_shard,
        resume=resume,
        retry_errors=retry_errors,
        progress=_exhaustive_progress,
    )
    result["run_ai_requested"] = run_ai
    result["archive_requested"] = archive
    if run_ai:
        result["ai_note"] = (
            "本命令仅在抓取闭环后按新增run处理AI；未配置模型时保持ai_pending。"
        )
    result = _execute_exhaustive_postprocess(
        crawler, result, run_ai=run_ai, archive=archive
    )
    result.pop("ai_note", None)
    result.pop("run_ai_requested", None)
    result.pop("archive_requested", None)
    typer.echo(json.dumps(result, ensure_ascii=False, indent=2, default=str))


@crawl_app.command("exhaustive-all")
def crawl_exhaustive_all(
    from_: str = typer.Option("2018-01-01", "--from"),
    to: str = typer.Option("today", "--to"),
    cities: str = typer.Option("", "--cities"),
    source_roles: str = typer.Option("", "--source-roles"),
    max_pages_per_source: int = typer.Option(50, "--max-pages-per-source", min=1),
    max_candidates_per_shard: int = typer.Option(
        500, "--max-candidates-per-shard", min=1
    ),
    max_fetches_per_shard: int = typer.Option(
        500, "--max-fetches-per-shard", min=1
    ),
    resume: bool = typer.Option(True, "--resume/--no-resume"),
    run_ai: bool = typer.Option(False, "--run-ai/--no-run-ai"),
    archive: bool = typer.Option(True, "--archive/--no-archive"),
):
    """105城市默认顺序执行；可用--cities先做小规模验收。"""
    crawler = ExhaustiveCrawler()
    city_frame = load_cities_105(crawler.settings)
    requested = _split_option(cities)
    names = (
        requested
        if requested
        else city_frame["city_name"].to_list()
    )
    results = []
    for index, city in enumerate(names, 1):
        typer.echo(f"城市 {index}/{len(names)}：{city}")
        city_result = crawler.run_city(
                city,
                start_date=_date(from_),
                end_date=_date(to),
                source_roles=_split_option(source_roles),
                max_pages_per_source=max_pages_per_source,
                max_candidates_per_shard=max_candidates_per_shard,
                max_fetches_per_shard=max_fetches_per_shard,
                resume=resume,
                progress=_exhaustive_progress,
            )
        results.append(
            _execute_exhaustive_postprocess(
                crawler,
                city_result,
                run_ai=run_ai,
                archive=archive,
            )
        )
    typer.echo(
        json.dumps(
            {
                "cities": len(results),
                "run_ai_executed": run_ai,
                "archive_executed": archive,
                "results": results,
            },
            ensure_ascii=False,
            indent=2,
            default=str,
        )
    )


@crawl_app.command("exhaustive-status")
def crawl_exhaustive_status(
    city: str | None = typer.Option(None, "--city"),
):
    crawler = ExhaustiveCrawler()
    crawler.rebuild_progress()
    typer.echo(
        json.dumps(crawler.city_status(city), ensure_ascii=False, indent=2, default=str)
    )


@crawl_app.command("exhaustive-resume")
def crawl_exhaustive_resume(
    city: str = typer.Option(..., "--city"),
    from_: str = typer.Option("2018-01-01", "--from"),
    to: str = typer.Option("today", "--to"),
):
    result = ExhaustiveCrawler().run_city(
        city,
        start_date=_date(from_),
        end_date=_date(to),
        resume=True,
        progress=_exhaustive_progress,
    )
    typer.echo(json.dumps(result, ensure_ascii=False, indent=2, default=str))


@crawl_app.command("exhaustive-retry")
def crawl_exhaustive_retry(
    city: str = typer.Option(..., "--city"),
    from_: str = typer.Option("2018-01-01", "--from"),
    to: str = typer.Option("today", "--to"),
):
    result = ExhaustiveCrawler().run_city(
        city,
        start_date=_date(from_),
        end_date=_date(to),
        resume=True,
        retry_errors=True,
        progress=_exhaustive_progress,
    )
    typer.echo(json.dumps(result, ensure_ascii=False, indent=2, default=str))


@jobs_app.command("run")
def jobs_run(job_id: str = typer.Option(..., "--job-id")):
    typer.echo(json.dumps(run_job(job_id), ensure_ascii=False, indent=2, default=str))


@jobs_app.command("status")
def jobs_status(job_id: str = typer.Option(..., "--job-id")):
    typer.echo(JobManager().load_state(job_id).model_dump_json(indent=2))


@jobs_app.command("cancel")
def jobs_cancel(job_id: str = typer.Option(..., "--job-id")):
    typer.echo(JobManager().cancel(job_id).model_dump_json(indent=2))


@report_app.command("crawl")
def report_crawl(job_id: str = typer.Option(..., "--job-id")):
    manager = JobManager()
    path = manager.job_dir(job_id) / "report.md"
    if not path.exists():
        raise typer.BadParameter("报告尚未生成")
    typer.echo(path.read_text(encoding="utf-8"))


@enrich_app.command("glm")
def enrich_glm(
    pending_only: bool = typer.Option(True, "--pending-only/--all"),
    run_id: str | None = typer.Option(None, "--run-id"),
):
    """处理待提取正文；无GLM_API_KEY时仅建立待处理缓存。"""
    _ = pending_only
    result = GLMEnricher().enrich_pending(run_id=run_id)
    build_database()
    typer.echo(json.dumps(result, ensure_ascii=False, indent=2))


@enrich_app.command("verify")
def enrich_verify(run_id: str | None = typer.Option(None, "--run-id")):
    """独立复核第一次GLM抽取；最终状态仍由确定性规则决定。"""
    result = GLMEnricher().verify_pending(run_id=run_id)
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


def _write_intensity_metric(name: str, payload: dict) -> Path:
    output = Settings.discover().root / "outputs" / "policy_intensity"
    output.mkdir(parents=True, exist_ok=True)
    path = output / name
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


@intensity_app.command("literature-audit")
def intensity_literature_audit():
    root = Settings.discover().root
    names = [
        "policy_intensity_literature_review.md",
        "policy_intensity_algorithm_mapping.md",
        "policy_intensity_method_decisions.md",
    ]
    result = {name: (root / "docs" / name).exists() for name in names}
    typer.echo(json.dumps({"passed": all(result.values()), "documents": result}, ensure_ascii=False, indent=2))


@intensity_app.command("prepare-annotations")
def intensity_prepare_annotations(
    documents: int = typer.Option(500, "--documents", min=1),
    clauses: int = typer.Option(3000, "--clauses", min=1),
):
    result = prepare_annotations(document_count=documents, clause_count=clauses)
    _write_intensity_metric("annotation_metrics.json", result)
    typer.echo(json.dumps(result, ensure_ascii=False, indent=2))


@intensity_app.command("train-baselines")
def intensity_train_baselines():
    result = train_baselines()
    _write_intensity_metric("baseline_metrics.json", result)
    typer.echo(json.dumps(result, ensure_ascii=False, indent=2))


@intensity_app.command("train-transformer")
def intensity_train_transformer(
    model: str = typer.Option("hfl/chinese-macbert-base", "--model"),
):
    result = train_transformer(model_name=model)
    _write_intensity_metric("transformer_metrics.json", result)
    typer.echo(json.dumps(result, ensure_ascii=False, indent=2))


@intensity_app.command("glm-extract")
def intensity_glm_extract(limit: int = typer.Option(50, "--limit", min=1, max=1000)):
    result = glm_extract_pending(limit=limit)
    _write_intensity_metric("glm_metrics.json", {"stage": "extract", **result, "research_ready": False})
    typer.echo(json.dumps(result, ensure_ascii=False, indent=2))


@intensity_app.command("glm-verify")
def intensity_glm_verify(limit: int = typer.Option(50, "--limit", min=1, max=1000)):
    result = glm_verify_pending(limit=limit)
    _write_intensity_metric("glm_verification_metrics.json", result)
    typer.echo(json.dumps(result, ensure_ascii=False, indent=2))


@intensity_app.command("benchmark")
def intensity_benchmark():
    result = build_benchmark()
    typer.echo(json.dumps(result, ensure_ascii=False, indent=2))


@intensity_app.command("route")
def intensity_route():
    result = route_predictions()
    _write_intensity_metric("hybrid_metrics.json", {**result, "metrics": {}, "research_ready": False})
    typer.echo(json.dumps(result, ensure_ascii=False, indent=2))


@intensity_app.command("score")
def intensity_score(
    limit: int | None = typer.Option(None, "--limit", min=1),
    formal_only: bool = typer.Option(False, "--formal-only"),
):
    service = PolicyIntensityService()
    extraction = service.extract(limit=limit, formal_only=formal_only)
    scoring = service.score()
    service.rebuild_database()
    typer.echo(json.dumps({"extraction": extraction, "scoring": scoring}, ensure_ascii=False, indent=2))


@intensity_app.command("aggregate")
def intensity_aggregate():
    service = PolicyIntensityService()
    result = service.aggregate()
    service.rebuild_database()
    typer.echo(json.dumps(result, ensure_ascii=False, indent=2))


@intensity_app.command("validate")
def intensity_validate():
    result = validate_intensity()
    _write_intensity_metric("validation_metrics.json", result)
    typer.echo(json.dumps(result, ensure_ascii=False, indent=2))
    if not result["passed_structural"]:
        raise typer.Exit(1)


@intensity_app.command("review")
def intensity_review():
    result = create_model_review_tasks()
    typer.echo(json.dumps(result, ensure_ascii=False, indent=2))


@intensity_app.command("report")
def intensity_report():
    result = {
        "validation": validate_intensity(),
        "benchmark": build_benchmark(),
        "research_ready": False,
    }
    _write_intensity_metric("acceptance_report.json", result)
    typer.echo(json.dumps(result, ensure_ascii=False, indent=2))
