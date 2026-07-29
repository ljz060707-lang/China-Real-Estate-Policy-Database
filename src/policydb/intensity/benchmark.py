from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from policydb.settings import Settings


def _load(path: Path, fallback: dict) -> dict:
    if not path.exists():
        return fallback
    return json.loads(path.read_text(encoding="utf-8"))


def build_benchmark(settings: Settings | None = None) -> dict:
    settings = settings or Settings.discover()
    output = settings.root / "outputs" / "policy_intensity"
    output.mkdir(parents=True, exist_ok=True)
    unavailable = {"status": "not_run", "metrics": {}, "research_ready": False}
    payload = {
        "generated_at": datetime.now(UTC).isoformat(),
        "rules": _load(output / "rule_metrics.json", unavailable),
        "baselines": _load(output / "baseline_metrics.json", unavailable),
        "transformer": _load(output / "transformer_metrics.json", unavailable),
        "glm": _load(output / "glm_metrics.json", unavailable),
        "hybrid": _load(output / "hybrid_metrics.json", unavailable),
        "formal_thresholds": {
            "action_macro_f1": 0.85,
            "instrument_macro_f1": 0.85,
            "direction_macro_f1": 0.90,
            "numeric_accuracy": 0.95,
            "pairing_accuracy": 0.95,
            "inter_annotator_kappa": 0.75,
            "expert_spearman": 0.80,
        },
        "research_ready": False,
        "blocking_reasons": ["no adjudicated gold", "no formal held-out benchmark"],
    }
    (output / "model_benchmark.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    lines = [
        "# 模型基准",
        "",
        "当前状态：**experimental / not research-ready**。无裁决金标准，因此不报告伪造的 F1、Kappa 或相关系数。",
        "",
        "| 方法 | 状态 | 正式指标 |",
        "|---|---|---|",
    ]
    for name in ("rules", "baselines", "transformer", "glm", "hybrid"):
        result = payload[name]
        lines.append(f"| {name} | {result.get('status', 'not_run')} | 不可用 |")
    (output / "model_benchmark.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return payload

