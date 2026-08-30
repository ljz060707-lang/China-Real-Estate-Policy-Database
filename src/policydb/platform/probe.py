"""Import-only probe for CRPD platform seams. No network, no DB writes."""
import json
import sys

from policydb.platform.config import CRPDConfig
from policydb.platform.seams import SEAM_MAP, probe_seams
from policydb.platform.stage_graph import STAGE_GRAPH, STAGES

report = {
    "config": None,
    "seams": probe_seams(),
    "stage_count": len(STAGES),
    "stage_graph_count": len(STAGE_GRAPH),
    "python": sys.version.split()[0],
}

try:
    cfg = CRPDConfig.discover()
    report["config"] = {
        "data_root": str(cfg.data_root),
        "db": str(cfg.db_path) if hasattr(cfg, "db_path") else None,
        "dirs_ok": cfg.ensure_dirs() if hasattr(cfg, "ensure_dirs") else None,
    }
except Exception as exc:  # report, never swallow silently
    report["config_error"] = f"{type(exc).__name__}: {exc}"

resolved = sum(1 for v in report["seams"].values() if v.get("resolves"))
print(json.dumps(report, ensure_ascii=False, indent=2))
print(f"SEAMS_RESOLVED {resolved}/{len(SEAM_MAP)}")
