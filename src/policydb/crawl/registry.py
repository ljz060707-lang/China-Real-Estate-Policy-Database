from __future__ import annotations

import yaml

from policydb.crawl.models import RegisteredSource
from policydb.settings import Settings


def load_registry(settings: Settings | None = None) -> list[RegisteredSource]:
    settings = settings or Settings.discover()
    path = settings.root / "data" / "reference" / "source_registry.yaml"
    if not path.exists():
        return []
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return [RegisteredSource.model_validate(item) for item in data.get("sources", [])]

