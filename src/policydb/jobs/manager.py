from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import time
from datetime import UTC, datetime
from pathlib import Path

from policydb.config.secret_store import default_secret_store, redact_secrets
from policydb.jobs.models import CrawlJobRequest, JobState
from policydb.settings import Settings
from policydb.transform.normalization import stable_id


def atomic_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(
        f"{path.name}.{os.getpid()}.{threading.get_ident()}.tmp"
    )
    temp.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8"
    )
    try:
        for attempt in range(5):
            try:
                os.replace(temp, path)
                return
            except PermissionError:
                if attempt == 4:
                    raise
                time.sleep(0.01 * (attempt + 1))
    finally:
        temp.unlink(missing_ok=True)


class PolicyWriteLock:
    def __init__(self, settings: Settings, job_id: str) -> None:
        self.path = settings.root / "data" / "logs" / "policydb-write.lock"
        self.job_id = job_id

    @staticmethod
    def _alive(pid: int) -> bool:
        try:
            os.kill(pid, 0)
            return True
        except OSError:
            return False

    def __enter__(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if self.path.exists():
            try:
                existing = json.loads(self.path.read_text(encoding="utf-8"))
                if self._alive(int(existing.get("pid", -1))):
                    raise RuntimeError(f"另一个写任务正在运行：{existing.get('job_id')}")
            except (ValueError, json.JSONDecodeError):
                pass
            self.path.unlink(missing_ok=True)
        flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
        handle = os.open(self.path, flags)
        os.write(handle, json.dumps({"job_id": self.job_id, "pid": os.getpid()}).encode())
        os.close(handle)
        return self

    def __exit__(self, *_: object) -> None:
        self.path.unlink(missing_ok=True)


class JobManager:
    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or Settings.discover()
        self.root = self.settings.root / "data" / "logs" / "crawl_jobs"
        self.work_root = self.settings.root / "data" / "work" / "crawl_jobs"
        self._state_lock = threading.RLock()
        self._last_write: dict[str, float] = {}
        self._last_event: dict[str, tuple[str, str, int]] = {}

    def job_dir(self, job_id: str) -> Path:
        return self.root / job_id

    def workspace_dir(self, job_id: str) -> Path:
        return self.work_root / job_id

    def record_timing(self, job_id: str, name: str, seconds: float) -> None:
        path = self.job_dir(job_id) / "timings.json"
        values = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
        values[name] = seconds
        atomic_json(path, values)

    def create(self, request: CrawlJobRequest) -> JobState:
        if self.settings.read_only:
            raise PermissionError("只读公开部署不能创建抓取任务")
        now = datetime.now(UTC)
        job_id = stable_id(request.mode, now.isoformat(), prefix="JOB")
        directory = self.job_dir(job_id)
        directory.mkdir(parents=True, exist_ok=False)
        self.workspace_dir(job_id).mkdir(parents=True, exist_ok=False)
        atomic_json(directory / "request.json", request.model_dump(mode="json"))
        state = JobState(job_id=job_id, mode=request.mode, created_at=now)
        self.save_state(state)
        (directory / "events.jsonl").touch()
        return state

    def load_request(self, job_id: str) -> CrawlJobRequest:
        return CrawlJobRequest.model_validate_json(
            (self.job_dir(job_id) / "request.json").read_text(encoding="utf-8")
        )

    def load_state(self, job_id: str) -> JobState:
        path = self.job_dir(job_id) / "state.json"
        for attempt in range(5):
            try:
                return JobState.model_validate_json(path.read_text(encoding="utf-8"))
            except (PermissionError, FileNotFoundError):
                if attempt == 4:
                    raise
                time.sleep(0.01 * (attempt + 1))
        raise RuntimeError(f"无法读取任务状态：{job_id}")

    def inspect_state(self, job_id: str) -> JobState:
        return self._recover_stale_state(self.load_state(job_id))

    @staticmethod
    def _pid_alive(pid: int | None) -> bool:
        if not pid:
            return False
        try:
            os.kill(pid, 0)
            return True
        except OSError:
            return False

    def _recover_stale_state(self, state: JobState) -> JobState:
        terminal = {"completed", "completed_with_warnings", "failed", "cancelled"}
        if state.status in terminal or not state.pid or self._pid_alive(state.pid):
            return state
        cancelled = state.cancel_requested
        recovered = state.model_copy(
            update={
                "status": "cancelled" if cancelled else "failed",
                "stage": "cancelled" if cancelled else "failed",
                "message": "后台进程已退出，任务已安全收敛",
                "finished_at": datetime.now(UTC),
                "error_type": None if cancelled else "StaleWorker",
                "error_message": None if cancelled else "后台进程已退出，未留下运行中的假状态",
            }
        )
        if not self.settings.read_only:
            self.save_state(recovered)
        return recovered

    def save_state(self, state: JobState) -> None:
        with self._state_lock:
            atomic_json(
                self.job_dir(state.job_id) / "state.json", state.model_dump(mode="json")
            )

    def update(
        self,
        job_id: str,
        *,
        force: bool = False,
        emit_event: bool | None = None,
        **changes: object,
    ) -> JobState:
        secrets = default_secret_store()
        values = [
            secrets.get_secret(name) or ""
            for name in (
                "siliconflow_api_key",
                "glm_api_key",
                "tianditu_token",
                "search_api_key",
                "http_proxy",
            )
        ]
        for key in ("message", "error_message"):
            if key in changes and changes[key] is not None:
                changes[key] = redact_secrets(changes[key], values)
        with self._state_lock:
            previous = self.load_state(job_id)
            state = previous.model_copy(update=changes)
            now = time.monotonic()
            stage_changed = state.stage != previous.stage or state.status != previous.status
            terminal = state.status in {
                "completed",
                "completed_with_warnings",
                "failed",
                "cancelled",
            }
            progress_changed = (
                state.progress_total > 0
                and state.progress_current * 100 // state.progress_total
                != previous.progress_current * 100 // max(previous.progress_total, 1)
            )
            message_changed = state.message != previous.message
            due = now - self._last_write.get(job_id, 0.0) >= 0.5
            if not (force or stage_changed or terminal or (due and (progress_changed or message_changed))):
                return previous
            self.save_state(state)
            self._last_write[job_id] = now
            processed = int(state.processed_count or state.counters.get("processed", 0))
            last_stage, last_message, last_processed = self._last_event.get(
                job_id, ("", "", -10)
            )
            should_event = (
                emit_event is True
                or stage_changed
                or state.error_type is not None
                or processed - last_processed >= 10
            )
            if emit_event is False:
                should_event = False
            if should_event and (state.stage, state.message, processed) != (
                last_stage,
                last_message,
                last_processed,
            ):
                self.event(job_id, state.stage, state.message, state.counters)
                self._last_event[job_id] = (state.stage, state.message, processed)
            return state

    def event(self, job_id: str, stage: str, message: str, counters: dict | None = None) -> None:
        path = self.job_dir(job_id) / "events.jsonl"
        secrets = default_secret_store()
        safe = redact_secrets(
            message,
            [
                secrets.get_secret(name) or ""
                for name in (
                    "siliconflow_api_key",
                    "glm_api_key",
                    "tianditu_token",
                    "search_api_key",
                    "http_proxy",
                )
            ],
        )
        with path.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps({"at": datetime.now(UTC).isoformat(), "stage": stage, "message": safe, "counters": counters or {}}, ensure_ascii=False) + "\n")

    def start(self, job_id: str) -> JobState:
        started = time.perf_counter()
        if self.settings.read_only:
            raise PermissionError("只读公开部署不能启动抓取任务")
        state = self.load_state(job_id)
        stdout = (self.job_dir(job_id) / "stdout.log").open("a", encoding="utf-8")
        stderr = (self.job_dir(job_id) / "stderr.log").open("a", encoding="utf-8")
        env = os.environ.copy()
        env["POLICYDB_ROOT"] = str(self.settings.root)
        env.update(
            {
                "POLARS_MAX_THREADS": "2",
                "OMP_NUM_THREADS": "1",
                "ARROW_NUM_THREADS": "2",
                "OPENBLAS_NUM_THREADS": "1",
                "MKL_NUM_THREADS": "1",
                "NUMEXPR_NUM_THREADS": "1",
                "POLICYDB_MAX_CONCURRENCY": str(min(self.settings.max_concurrency, 16)),
            }
        )
        command = [
            str(Path(sys.executable).resolve()),
            "-m",
            "policydb.jobs.worker",
            "--job-id",
            job_id,
            "--root",
            str(self.settings.root),
        ]
        creationflags = 0
        if os.name == "nt":
            creationflags = (
                subprocess.CREATE_NEW_PROCESS_GROUP
                | subprocess.CREATE_NO_WINDOW
                | subprocess.BELOW_NORMAL_PRIORITY_CLASS
            )
        try:
            process = subprocess.Popen(
                command,
                cwd=self.settings.root,
                env=env,
                stdout=stdout,
                stderr=stderr,
                shell=False,
                creationflags=creationflags,
                close_fds=True,
            )
        except Exception as exc:
            self.update(
                job_id,
                force=True,
                status="failed",
                stage="failed",
                message="后台进程启动失败",
                error_type=type(exc).__name__,
                error_message=str(exc)[:500],
                finished_at=datetime.now(UTC),
            )
            raise
        finally:
            stdout.close()
            stderr.close()
        current = self.load_state(job_id)
        state = current.model_copy(
            update={"pid": process.pid, "message": "后台工作进程已启动"}
        )
        self.save_state(state)
        self.record_timing(
            job_id, "job_manager_start_seconds", time.perf_counter() - started
        )
        return state

    def cancel(self, job_id: str) -> JobState:
        return self.update(
            job_id,
            force=True,
            emit_event=True,
            cancel_requested=True,
            message="已请求安全停止",
        )

    def terminate(self, job_id: str) -> JobState:
        state = self.load_state(job_id)
        if state.pid and self._pid_alive(state.pid):
            try:
                import psutil

                process = psutil.Process(state.pid)
                for child in process.children(recursive=True):
                    child.kill()
                process.kill()
            except Exception:
                os.kill(state.pid, 9)
        return self.update(
            job_id,
            force=True,
            status="cancelled",
            stage="cancelled",
            cancel_requested=True,
            message="后台进程已强制终止；暂存产物已保留",
            finished_at=datetime.now(UTC),
        )

    def list_states(self, limit: int = 50) -> list[JobState]:
        if not self.root.exists():
            return []
        states = []
        for path in sorted(self.root.glob("*/state.json"), key=lambda item: item.stat().st_mtime, reverse=True)[:limit]:
            try:
                state = JobState.model_validate_json(path.read_text(encoding="utf-8"))
                states.append(self._recover_stale_state(state))
            except Exception:
                continue
        return states
