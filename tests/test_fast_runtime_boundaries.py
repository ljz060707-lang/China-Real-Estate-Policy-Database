from __future__ import annotations

from pathlib import Path

from policydb.full_sync import FullSyncConfig, FullSyncController, _pipeline_run_with_retry
from policydb.settings import Settings


def test_pipeline_retry_passes_safe_stop_and_attachment_budget(tmp_path: Path) -> None:
    settings = Settings(root=tmp_path, curated_path=tmp_path / "curated")
    seen = {}

    class FakePipeline:
        def __init__(self) -> None:
            self.settings = settings

        def run(self, run_id, **kwargs):
            seen.update(kwargs)
            return {"run_id": run_id, "status": "cancelled", "cancelled": True, "fetched": 1, "failed": 0}

    result = _pipeline_run_with_retry(FakePipeline(), "RUN1", max_fetches=3, cancel_check=lambda: True, max_attachment_attempts=1)
    assert result["cancelled"] is True
    assert seen["cancel_check"]() is True
    assert seen["max_attachment_attempts"] == 1


def test_full_sync_stop_marker_is_read_without_killing_writer(tmp_path: Path) -> None:
    settings = Settings(root=tmp_path, curated_path=tmp_path / "curated")
    controller = FullSyncController(settings, config=FullSyncConfig(apply=True), output=tmp_path / "run", run_id="RUN1")
    assert controller.stop_requested() is False
    controller.run_dir.mkdir(parents=True, exist_ok=True)
    (controller.run_dir / "STOP_AUTOPILOT").write_text("test", encoding="utf-8")
    assert controller.stop_requested() is True
