"""Run one validated Dashboard job and exit; suitable for Task Scheduler."""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
for path in (ROOT, ROOT / "src"):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from policydb.dashboard_jobs import run_next_job  # noqa: E402
from policydb.settings import Settings  # noqa: E402


def main() -> int:
    settings = Settings.discover(ROOT)
    result = run_next_job(settings)
    print(json.dumps(result or {"status": "IDLE"}, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
