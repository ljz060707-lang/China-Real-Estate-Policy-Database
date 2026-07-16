from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

from policydb.config.secret_store import default_secret_store, redact_secrets
from policydb.jobs.models import CrawlJobRequest, JobState
from policydb.settings import Settings
from policydb.transform.normalization import stable_id


def atomic_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    os.replace(temp, path)


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

    def job_dir(self, job_id: str) -> Path:
        return self.root / job_id

    def create(self, request: CrawlJobRequest) -> JobState:
        if self.settings.read_only:
            raise PermissionError("只读公开部署不能创建抓取任务")
        now = datetime.now(UTC)
        job_id = stable_id(request.mode, now.isoformat(), prefix="JOB")
        directory = self.job_dir(job_id)
        directory.mkdir(parents=True, exist_ok=False)
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
        return JobState.model_validate_json(
            (self.job_dir(job_id) / "state.json").read_text(encoding="utf-8")
        )

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
        atomic_json(self.job_dir(state.job_id) / "state.json", state.model_dump(mode="json"))

    def update(self, job_id: str, **changes: object) -> JobState:
        secrets = default_secret_store()
        values = [
            secrets.get_secret(name) or ""
            for name in ("glm_api_key", "tianditu_token", "search_api_key", "http_proxy")
        ]
        for key in ("message", "error_message"):
            if key in changes and changes[key] is not None:
                changes[key] = redact_secrets(changes[key], values)
        state = self.load_state(job_id).model_copy(update=changes)
        self.save_state(state)
        self.event(job_id, state.stage, state.message, state.counters)
        return state

    def event(self, job_id: str, stage: str, message: str, counters: dict | None = None) -> None:
        path = self.job_dir(job_id) / "events.jsonl"
        secrets = default_secret_store()
        safe = redact_secrets(
            message,
            [
                secrets.get_secret(name) or ""
                for name in ("glm_api_key", "tianditu_token", "search_api_key", "http_proxy")
            ],
        )
        with path.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps({"at": datetime.now(UTC).isoformat(), "stage": stage, "message": safe, "counters": counters or {}}, ensure_ascii=False) + "\n")

    def start(self, job_id: str) -> JobState:
        if self.settings.read_only:
            raise PermissionError("只读公开部署不能启动抓取任务")
        state = self.load_state(job_id)
        log = (self.job_dir(job_id) / "stdout.log").open("a", encoding="utf-8")
        env = os.environ.copy()
        env["POLICYDB_ROOT"] = str(self.settings.root)
        process = subprocess.Popen(
            [sys.executable, "-m", "policydb.jobs.worker", "--job-id", job_id, "--root", str(self.settings.root)],
            cwd=self.settings.root,
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
            shell=False,
        )
        log.close()
        state = state.model_copy(update={"pid": process.pid, "message": "后台工作进程已启动"})
        self.save_state(state)
        return state

    def cancel(self, job_id: str) -> JobState:
        return self.update(job_id, cancel_requested=True, message="已请求安全停止")

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
