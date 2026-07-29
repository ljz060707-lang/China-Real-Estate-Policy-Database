from __future__ import annotations

import json
from pathlib import Path

import plotly.express as px
import polars as pl
import streamlit as st

from app.theme import style_plotly_figure
from app.ui import safe_dataframe, safe_pandas


def _json(path: Path, fallback: dict) -> dict:
    if not path.exists():
        return fallback
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return fallback


def _count_parquet(path: Path) -> int:
    return pl.scan_parquet(path).select(pl.len()).collect().item() if path.exists() else 0


def render_intensity_panel(db, root: Path) -> None:
    output = root / "outputs" / "policy_intensity"
    annotations = root / "data" / "annotations" / "policy_intensity"
    benchmark = _json(output / "model_benchmark.json", {"research_ready": False})
    validation = _json(output / "validation_metrics.json", {})
    try:
        summary = db._query(
            "SELECT count(*) action_count," 
            "count(*) FILTER(WHERE formal_status='formal') formal_count," 
            "avg(textual_policy_design_intensity) mean_design," 
            "count(*) FILTER(WHERE review_required) review_count "
            "FROM v_policy_action_intensity"
        ).row(0, named=True)
    except Exception:
        summary = {"action_count": 0, "formal_count": 0, "mean_design": None, "review_count": 0}
    for column, (label, value) in zip(
        st.columns(4),
        [
            ("动作级分数", int(summary["action_count"])),
            ("完整官方文本动作", int(summary["formal_count"])),
            ("平均文本设计强度", f"{float(summary['mean_design'] or 0):.3f}"),
            ("待模型复核", int(summary["review_count"])),
        ],
        strict=True,
    ):
        column.metric(label, value)
    st.warning(
        "当前指数为 experimental：它测量政策文本设计、实施承诺和工具校准，"
        "不代表实际执行效果。金标准与样本外门槛未完成前，不可作为正式因果变量。"
    )

    comparison_tab, agreement_tab, glm_tab, annotation_tab, composition_tab, quality_tab = st.tabs(
        ["模型比较", "模型一致性", "GLM辅助识别", "人工标注", "强度构成", "研究质量"]
    )
    with comparison_tab:
        rows = []
        for name in ("rules", "baselines", "transformer", "glm", "hybrid"):
            result = benchmark.get(name, {})
            rows.append(
                {
                    "方法": name,
                    "状态": result.get("status", "not_run"),
                    "训练样本": result.get("training_rows"),
                    "正式指标可用": bool(result.get("metrics")),
                    "研究就绪": bool(result.get("research_ready", False)),
                }
            )
        safe_dataframe(pl.DataFrame(rows), height=260)
        st.caption("没有裁决金标准时，页面明确显示不可用，不用规则测试准确率冒充真实模型指标。")

    with agreement_tab:
        try:
            agreement = db._query(
                "SELECT accepted_method,decision_reason,count(*) decision_count," 
                "avg(agreement) agreement_mean,avg(decision_confidence) confidence_mean," 
                "count(*) FILTER(WHERE review_required) review_count "
                "FROM policy_model_decisions GROUP BY ALL ORDER BY decision_count DESC"
            )
            safe_dataframe(agreement, height=330)
        except Exception:
            st.info("尚未生成多模型路由决策。请先运行 `policydb intensity route`。")

    with glm_tab:
        glm_metrics = _json(output / "glm_metrics.json", {"status": "not_run"})
        st.json(
            {
                "status": glm_metrics.get("status", "not_run"),
                "processed": glm_metrics.get("processed", 0),
                "failed": glm_metrics.get("failed", 0),
                "token_usage": glm_metrics.get("token_usage"),
                "cost": glm_metrics.get("cost"),
            }
        )
        st.caption("GLM 只处理复杂语义和证据复核；数值事实、连续总分和研究就绪状态由确定性程序决定。")

    with annotation_tab:
        counts = {
            "文件样本": _count_parquet(annotations / "document_sample.parquet"),
            "条款样本": _count_parquet(annotations / "clause_sample.parquet"),
            "双人编码": _count_parquet(annotations / "double_coded.parquet"),
            "裁决金标准": _count_parquet(annotations / "adjudicated_gold.parquet"),
        }
        for column, (label, value) in zip(st.columns(4), counts.items(), strict=True):
            column.metric(label, value)
        st.info("文件和条款已经抽样，但标签必须由人工填写并裁决；系统不会把规则预标注当作金标准。")

    with composition_tab:
        try:
            dimensions = db._query(
                "SELECT dimension_code,dimension_name,count(*) FILTER(WHERE applicable) applicable_count," 
                "avg(mapped_score) FILTER(WHERE applicable) mean_score "
                "FROM policy_intensity_dimensions GROUP BY ALL ORDER BY dimension_code"
            )
            frame = safe_pandas(dimensions)
            figure = px.bar(
                frame,
                x="dimension_code",
                y="mean_score",
                hover_data=["dimension_name", "applicable_count"],
                title="八维度平均映射分数（NA不进入均值）",
                color_discrete_sequence=["#82318E"],
            )
            st.plotly_chart(style_plotly_figure(figure), width="stretch")
            safe_dataframe(dimensions, height=300)
        except Exception:
            st.info("尚未生成维度分数。请先运行 `policydb intensity score`。")

    with quality_tab:
        st.json(
            {
                "结构文件通过": validation.get("passed_structural", False),
                "正式基准通过": validation.get("formal_benchmark_passed", False),
                "research_ready": validation.get("research_ready", False),
                "阻塞原因": validation.get(
                    "blocking_reasons",
                    ["尚未运行强度验证或尚无人工金标准"],
                ),
            }
        )
        st.caption("未扫描窗口保持 NULL；只有完整扫描且确认无政策的窗口才记为 0。")

