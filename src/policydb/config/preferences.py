from __future__ import annotations

import json
import os
from pathlib import Path

ALLOWED_FIELDS = {
    "glm_model",
    "glm_base_url",
    "search_provider",
    "search_base_url",
    "search_max_results",
    "request_timeout",
    "connect_timeout",
    "max_retries",
    "default_rate_limit",
    "default_overlap_days",
    "default_max_fetches",
    "default_cities",
    "default_topics",
    "user_agent",
    "respect_robots",
    "max_concurrency",
    "tianditu_map_approval",
    "tianditu_qualification",
    "project_python_path",
}


class PreferencesStore:
    def __init__(self, path: Path) -> None:
        self.path = path

    def load(self) -> dict:
        if not self.path.exists():
            return {}
        data = json.loads(self.path.read_text(encoding="utf-8"))
        return {key: value for key, value in data.items() if key in ALLOWED_FIELDS}

    def save(self, values: dict) -> dict:
        forbidden = sorted(set(values) - ALLOWED_FIELDS)
        if forbidden:
            raise ValueError(f"Secret or unsupported preferences: {', '.join(forbidden)}")
        current = self.load()
        current.update(values)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temp = self.path.with_suffix(self.path.suffix + ".tmp")
        temp.write_text(
            json.dumps(current, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        os.replace(temp, self.path)
        return current
