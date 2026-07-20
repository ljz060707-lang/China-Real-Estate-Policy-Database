from __future__ import annotations

import argparse
import json
import os
import threading
import time
from datetime import UTC, datetime
from urllib.parse import urlsplit

import psutil

from policydb.crawl.service import CrawlService, commit_crawl_workspace
from policydb.jobs.manager import JobManager, PolicyWriteLock
from policydb.jobs.reporting import generate_crawl_report
from policydb.settings import Settings


def _redact_url(value: str | None) -> str | None:
    if not value:
        return None
    parts = urlsplit(value)
    return f"{parts.scheme}://{parts.netloc}{parts.path}"[:500]


def _workspace_size(path) -> int:
    return sum(item.stat().st_size for item in path.rglob("*") if item.is_file())


def _start_monitor(manager: JobManager, job_id: str, stop: threading.Event) -> threading.Thread:
    process = psutil.Process(os.getpid())
    output = manager.job_dir(job_id) / "performance.jsonl"
    output.touch(exist_ok=True)
    workspace = manager.workspace_dir(job_id)

    def write_sample() -> None:
        state = manager.load_state(job_id)
        sample = {
            "at": datetime.now(UTC).isoformat(),
            "stage": state.stage,
            "cpu_percent": process.cpu_percent(None),
            "rss_bytes": process.memory_info().rss,
            "thread_count": process.num_threads(),
            "read_bytes": process.io_counters().read_bytes,
            "write_bytes": process.io_counters().write_bytes,
            "processed_count": state.processed_count,
            "queued_count": state.queued_count,
            "state_bytes": (manager.job_dir(job_id) / "state.json").stat().st_size,
            "workspace_bytes": _workspace_size(workspace),
        }
        with output.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(sample, ensure_ascii=False) + "\n")
        manager.update(
            job_id,
            force=True,
            emit_event=False,
            heartbeat_at=datetime.now(UTC),
        )

    def monitor() -> None:
        process.cpu_percent(None)
        while not stop.is_set():
            try:
                write_sample()
            except Exception:
                pass
            stop.wait(5)
        try:
            write_sample()
        except Exception:
            pass

    thread = threading.Thread(target=monitor, name="policydb-job-monitor", daemon=True)
    thread.start()
    return thread


def run_job(job_id: str, settings: Settings | None = None) -> dict:
    settings = settings or Settings.discover()
    manager = JobManager(settings)
    request = manager.load_request(job_id)
    started = datetime.now(UTC)
    manager.update(
        job_id,
        force=True,
        status="preparing",
        stage="preparing",
        started_at=started,
        worker_started_at=started,
        heartbeat_at=started,
        message="正在检查配置和运行环境",
    )
    monitor_stop = threading.Event()
    monitor_thread = _start_monitor(manager, job_id, monitor_stop)

    def progress(stage: str, current: int, total: int, message: str, counters: dict) -> None:
        state = manager.load_state(job_id)
        if state.cancel_requested:
            raise InterruptedError("任务已按用户请求安全停止")
        details = dict(counters)
        current_url = _redact_url(str(details.pop("_current_url", "")))
        source_id = details.pop("_source_id", None)
        processed = int(details.get("processed", state.processed_count))
        queued = int(details.get("queued", state.queued_count))
        manager.update(
            job_id,
            status=stage,
            stage=stage,
            progress_current=current,
            progress_total=max(total, 1),
            message=message,
            counters={**state.counters, **details},
            heartbeat_at=datetime.now(UTC),
            last_progress_at=datetime.now(UTC),
            current_url_redacted=current_url or state.current_url_redacted,
            current_source_id=str(source_id) if source_id else state.current_source_id,
            processed_count=processed,
            queued_count=queued,
        )

    last_cancel_read = 0.0
    cached_cancel = False

    def cancel_check() -> bool:
        nonlocal last_cancel_read, cached_cancel
        now = time.monotonic()
        if now - last_cancel_read >= 0.25:
            cached_cancel = manager.load_state(job_id).cancel_requested
            last_cancel_read = now
        return cached_cancel

    try:
        mode_updates = {
            "staged_only": {"run_glm": False, "run_verification": False},
            "glm": {"run_glm": True, "run_verification": False},
            "glm_verify": {"run_glm": True, "run_verification": True},
        }
        effective_request = request.model_copy(
            update=mode_updates.get(request.processing_mode, {})
        )
        service = CrawlService(
            settings, workspace=manager.workspace_dir(job_id)
        )
        result = service.execute(
            effective_request, progress=progress, cancel_check=cancel_check
        )
        if cancel_check():
            raise InterruptedError("任务已按用户请求安全停止")
        if request.processing_mode == "full":
            manager.update(
                job_id,
                force=True,
                status="rebuilding",
                stage="rebuilding",
                message="正在校验并原子合并任务增量",
            )
            with PolicyWriteLock(settings, job_id):
                manifest = commit_crawl_workspace(
                    settings, manager.workspace_dir(job_id), job_id
                )
                result["merge_manifest"] = manifest
                if request.rebuild_database:
                    from policydb.query.database import (
                        DatabaseSwapDeferred,
                        build_database_atomic,
                    )

                    manager.update(
                        job_id,
                        force=True,
                        status="rebuilding",
                        stage="rebuilding",
                        message="正在构建并验证临时 DuckDB",
                    )
                    try:
                        build_database_atomic(settings, job_id)
                    except DatabaseSwapDeferred as exc:
                        result["warning"] = True
                        result.setdefault("recommendations", []).append(str(exc))
                if request.run_validation:
                    from policydb.validate.quality import validate

                    manager.update(
                        job_id,
                        force=True,
                        status="validating",
                        stage="validating",
                        message="正在验证稳定数据快照",
                    )
                    validation = validate(settings)
                    if not validation.get("passed"):
                        result["warning"] = True
        else:
            result.setdefault("recommendations", []).append(
                "抓取结果已暂存，尚未合并到正式数据库。"
            )
        state = manager.load_state(job_id)
        final_status = "completed_with_warnings" if result.get("warning") else "completed"
        state = manager.update(
            job_id,
            force=True,
            status="reporting",
            stage="reporting",
            message="正在生成抓取报告",
            run_id=result.get("run_id"),
            counters=result.get("metrics", {}),
            processed_count=int(result.get("metrics", {}).get("fetched", state.processed_count)),
            queued_count=0,
        )
        output = generate_crawl_report(settings, state.model_copy(update={"status": final_status}), result)
        manager.update(
            job_id,
            force=True,
            status=final_status,
            stage=final_status,
            progress_current=state.progress_total,
            message=f"任务完成；报告：{output}",
            finished_at=datetime.now(UTC),
            heartbeat_at=datetime.now(UTC),
            run_id=result.get("run_id"),
            counters=result.get("metrics", {}),
        )
        return result
    except InterruptedError as exc:
        manager.update(
            job_id,
            force=True,
            status="cancelled",
            stage="cancelled",
            message=str(exc),
            finished_at=datetime.now(UTC),
            heartbeat_at=datetime.now(UTC),
        )
        return {"cancelled": True}
    except Exception as exc:
        manager.update(
            job_id,
            force=True,
            status="failed",
            stage="failed",
            message="任务失败；请查看脱敏日志",
            error_type=type(exc).__name__,
            error_message=str(exc)[:500],
            finished_at=datetime.now(UTC),
            heartbeat_at=datetime.now(UTC),
        )
        raise
    finally:
        monitor_stop.set()
        monitor_thread.join(timeout=1)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--job-id", required=True)
    parser.add_argument("--root", required=True)
    args = parser.parse_args()
    run_job(args.job_id, Settings.discover(args.root))


if __name__ == "__main__":
    main()
