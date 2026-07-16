from __future__ import annotations

import argparse
from datetime import UTC, datetime

from policydb.crawl.service import CrawlService
from policydb.jobs.manager import JobManager, PolicyWriteLock
from policydb.jobs.reporting import generate_crawl_report
from policydb.settings import Settings


def run_job(job_id: str, settings: Settings | None = None) -> dict:
    settings = settings or Settings.discover()
    manager = JobManager(settings)
    request = manager.load_request(job_id)
    manager.update(job_id, status="preparing", stage="preparing", started_at=datetime.now(UTC), message="正在检查配置和运行环境")

    def progress(stage: str, current: int, total: int, message: str, counters: dict) -> None:
        state = manager.load_state(job_id)
        if state.cancel_requested:
            raise InterruptedError("任务已按用户请求安全停止")
        manager.update(job_id, status=stage, stage=stage, progress_current=current, progress_total=max(total, 1), message=message, counters={**state.counters, **counters})

    def cancel_check() -> bool:
        return manager.load_state(job_id).cancel_requested

    try:
        lock_required = request.rebuild_database or request.mode in {"seed_backtrack", "official_update", "smart", "historical_105", "recover_missing", "source_health"}
        if lock_required:
            with PolicyWriteLock(settings, job_id):
                result = CrawlService(settings).execute(
                    request, progress=progress, cancel_check=cancel_check
                )
        else:
            result = CrawlService(settings).execute(
                request, progress=progress, cancel_check=cancel_check
            )
        state = manager.load_state(job_id)
        final_status = "completed_with_warnings" if result.get("warning") else "completed"
        state = manager.update(job_id, status="reporting", stage="reporting", message="正在生成抓取报告", run_id=result.get("run_id"), counters=result.get("metrics", {}))
        output = generate_crawl_report(settings, state.model_copy(update={"status": final_status}), result)
        manager.update(job_id, status=final_status, stage=final_status, progress_current=state.progress_total, message=f"任务完成；报告：{output}", finished_at=datetime.now(UTC), run_id=result.get("run_id"), counters=result.get("metrics", {}))
        return result
    except InterruptedError as exc:
        manager.update(job_id, status="cancelled", stage="cancelled", message=str(exc), finished_at=datetime.now(UTC))
        return {"cancelled": True}
    except Exception as exc:
        manager.update(job_id, status="failed", stage="failed", message="任务失败；请查看脱敏日志", error_type=type(exc).__name__, error_message=str(exc)[:500], finished_at=datetime.now(UTC))
        raise


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--job-id", required=True)
    parser.add_argument("--root", required=True)
    args = parser.parse_args()
    run_job(args.job_id, Settings.discover(args.root))


if __name__ == "__main__":
    main()
