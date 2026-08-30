"""Conservative deterministic closure for EP930 action promotion gates.

This module only derives fields from already recovered action/document evidence.
It never calls an API, searches the network, or writes curated tables.  Formal
publication remains the responsibility of :class:`Episode930Pipeline`.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

import polars as pl

_UNKNOWN = {"", "NONE", "NULL", "UNKNOWN", "UNRESOLVED", "N/A"}
_DIRECTION_ALIASES = {
    "TIGHTEN": "TIGHTENING",
    "TIGHTENING": "TIGHTENING",
    "RESTRICTIVE": "TIGHTENING",
    "LOOSENING": "SUPPORTIVE",
    "EASING": "SUPPORTIVE",
    "SUPPORT": "SUPPORTIVE",
    "SUPPORTIVE": "SUPPORTIVE",
    "NEUTRAL": "NEUTRAL",
}

# These phrases are intentionally explicit.  A generic word such as "increase"
# is not sufficient because its effect depends on the parameter being changed.
_TIGHTENING_TERMS = (
    "提高首付",
    "提高首付款",
    "提高利率",
    "限购",
    "限售",
    "暂停发放",
    "暂停贷款",
    "禁止购买",
    "严禁",
    "收紧",
    "加强监管",
    "延长缴存",
    "增加缴存年限",
    "提高门槛",
    "限制购买",
    "降低贷款额度",
    "reduce loan quota",
    "raise downpayment",
    "increase downpayment",
    "restrict purchase",
    "purchase restriction",
    "tighten",
    "higher rate",
    "longer contribution",
)
_SUPPORTIVE_TERMS = (
    "降低首付",
    "下调首付",
    "降低首付款",
    "降低利率",
    "取消限购",
    "取消限售",
    "放宽",
    "优化",
    "支持",
    "增加供应",
    "提高贷款额度",
    "恢复贷款",
    "补贴",
    "减免",
    "缩短限售",
    "relax purchase",
    "remove purchase restriction",
    "lower downpayment",
    "lower rate",
    "increase loan quota",
    "resume loans",
    "support",
    "subsidy",
)
_NEUTRAL_TERMS = (
    "转发",
    "解读",
    "执行",
    "办理",
    "forward",
    "explanation",
    "procedure",
)
_NEGATION_TERMS = ("不", "未", "无", "不得", "禁止", "not", "without", "no")
_NARROW_SCOPE_RE = re.compile(r"(?P<name>[\u4e00-\u9fff]{2,8}(?:区|县))")
_EXPLICIT_CITY_SCOPE = ("全市", "本市", "市区", "中心城区", "全市范围", "citywide", "municipal")
_NUMBER_RE = re.compile(r"(?<!\d)(\d+(?:\.\d+)?)")
_VALUE_UNIT_RE = re.compile(r"(?:%|％|万元|万|元|年|个月|套|平方米|平米)\s*$")


def _text(value: object) -> str:
    return str(value or "").strip()


def _snippet(text: str, phrase: str, limit: int = 360) -> str:
    if not text:
        return ""
    index = text.lower().find(phrase.lower())
    if index < 0:
        return text[:limit]
    start = max(0, index - 100)
    return text[start : start + limit]


def _near_negated(text: str, start: int) -> bool:
    window = text[max(0, start - 10) : start].lower()
    return any(term.lower() in window for term in _NEGATION_TERMS)


def _lexical_candidates(text: str) -> list[tuple[str, str, bool]]:
    result: list[tuple[str, str, bool]] = []
    for state, terms in (
        ("TIGHTENING", _TIGHTENING_TERMS),
        ("SUPPORTIVE", _SUPPORTIVE_TERMS),
        ("NEUTRAL", _NEUTRAL_TERMS),
    ):
        for term in terms:
            index = text.lower().find(term.lower())
            if index >= 0:
                result.append((state, term, _near_negated(text, index)))
    return result


def _numeric_pair(action: Mapping[str, Any]) -> tuple[float, float] | None:
    old = _text(action.get("old_value"))
    new = _text(action.get("new_value"))
    unit = _text(action.get("unit")).lower()
    if not old or not new or not unit:
        return None
    if any(term in unit for term in ("date", "year-month")):
        return None
    if re.search(r"20\d{2}", old) or re.search(r"20\d{2}", new):
        return None
    old_suffix = _VALUE_UNIT_RE.search(old)
    new_suffix = _VALUE_UNIT_RE.search(new)
    if old_suffix and new_suffix and old_suffix.group(0) != new_suffix.group(0):
        return None
    if new_suffix and new_suffix.group(0).strip().lower() not in unit:
        return None
    if old_suffix and old_suffix.group(0).strip().lower() not in unit:
        return None
    old_match = _NUMBER_RE.search(old)
    new_match = _NUMBER_RE.search(new)
    if old_match is None or new_match is None:
        return None
    try:
        return float(old_match.group(1)), float(new_match.group(1))
    except ValueError:
        return None


def derive_direction(action: Mapping[str, Any], document: Mapping[str, Any] | None = None) -> dict[str, Any]:
    """Derive direction only when existing or deterministic evidence is clear."""

    current = _text(action.get("action_direction") or action.get("direction")).upper()
    if current in _DIRECTION_ALIASES:
        state = _DIRECTION_ALIASES[current]
        return {
            "direction_state": state,
            "direction_source": "EXISTING_ACTION_VALUE",
            "direction_evidence": current,
            "direction_confidence": "HIGH",
        }

    action_text = _text(action.get("action_text") or action.get("official_text_excerpt"))
    doc_text = _text((document or {}).get("official_text"))
    title = _text((document or {}).get("document_title"))
    text = " ".join(value for value in (action_text, title, doc_text) if value)
    matches = _lexical_candidates(text)
    resolved: list[tuple[str, str]] = []
    for state, term, negated in matches:
        if state == "NEUTRAL":
            resolved.append((state, term))
            continue
        if negated:
            state = "SUPPORTIVE" if state == "TIGHTENING" else "TIGHTENING"
        resolved.append((state, term))
    substantive = [(state, term) for state, term in resolved if state != "NEUTRAL"]
    if substantive:
        states = {state for state, _ in substantive}
        if len(states) == 1:
            state = next(iter(states))
            return {
                "direction_state": state,
                "direction_source": "LEXICAL_RULE_NEGATION_AWARE",
                "direction_evidence": _snippet(text, substantive[0][1]),
                "direction_confidence": "HIGH",
            }
        return {
            "direction_state": "UNKNOWN",
            "direction_source": "LEXICAL_CONFLICT",
            "direction_evidence": "; ".join(term for _, term in substantive),
            "direction_confidence": "LOW",
        }
    pair = _numeric_pair(action)
    policy_type = _text(action.get("policy_type")).upper()
    if pair and pair[0] != pair[1]:
        old, new = pair
        if policy_type in {"PF_DOWNPAYMENT", "COMMERCIAL_DOWNPAYMENT"}:
            state = "TIGHTENING" if new > old else "SUPPORTIVE"
        elif policy_type == "PF_LOAN_CEILING":
            state = "SUPPORTIVE" if new > old else "TIGHTENING"
        else:
            state = "UNKNOWN"
        if state != "UNKNOWN":
            return {
                "direction_state": state,
                "direction_source": "PARAMETER_COMPARISON_SAME_UNIT",
                "direction_evidence": f"old={old:g}; new={new:g}; unit={_text(action.get('unit'))}; policy_type={policy_type}",
                "direction_confidence": "MEDIUM",
            }
    if resolved:
        return {
            "direction_state": "NEUTRAL",
            "direction_source": "NEUTRAL_PROCEDURAL_LEXICON",
            "direction_evidence": "; ".join(term for _, term in resolved),
            "direction_confidence": "MEDIUM",
        }
    return {
        "direction_state": "UNKNOWN",
        "direction_source": "INSUFFICIENT_DETERMINISTIC_EVIDENCE",
        "direction_evidence": action_text[:360],
        "direction_confidence": "LOW",
    }


def derive_geography(action: Mapping[str, Any], document: Mapping[str, Any] | None = None) -> dict[str, Any]:
    """Derive geography with explicit/narrow-scope precedence and audit fields."""

    current = _text(action.get("geographic_scope"))
    if current and current.upper() not in _UNKNOWN:
        return {
            "geography_state": current,
            "geography_source": "EXISTING_ACTION_VALUE",
            "geography_evidence": current,
            "geography_confidence": "HIGH",
        }
    document = document or {}
    action_text = _text(action.get("action_text"))
    title = _text(document.get("document_title"))
    issuer = _text(document.get("issuer"))
    official_text = _text(document.get("official_text"))
    combined = " ".join(value for value in (action_text, title, issuer, official_text) if value)
    for phrase in _EXPLICIT_CITY_SCOPE:
        if phrase.lower() in action_text.lower():
            return {
                "geography_state": "CITY_LEVEL",
                "geography_source": "EXPLICIT_ACTION",
                "geography_evidence": _snippet(action_text, phrase),
                "geography_confidence": "HIGH",
            }
    narrow = _NARROW_SCOPE_RE.search(action_text)
    if narrow:
        value = f"DISTRICT_OR_COUNTY:{narrow.group('name')}"
        return {
            "geography_state": value,
            "geography_source": "EXPLICIT_ACTION",
            "geography_evidence": _snippet(action_text, narrow.group("name")),
            "geography_confidence": "HIGH",
        }
    for phrase in _EXPLICIT_CITY_SCOPE:
        if phrase.lower() in combined.lower():
            return {
                "geography_state": "CITY_LEVEL",
                "geography_source": "EXPLICIT_ACTION_OR_DOCUMENT",
                "geography_evidence": _snippet(combined, phrase),
                "geography_confidence": "HIGH",
            }
    if _NARROW_SCOPE_RE.search(combined):
        return {
            "geography_state": "UNKNOWN",
            "geography_source": "NARROW_SCOPE_NOT_ACTION_BOUND",
            "geography_evidence": _snippet(combined, _NARROW_SCOPE_RE.search(combined).group("name")),
            "geography_confidence": "LOW",
        }
    city = _text(document.get("city"))
    city_title_match = bool(city and title and city in title)
    issuer_city_match = bool(city and issuer and city in issuer)
    url = _text(document.get("official_url")).lower()
    if city and (city_title_match or issuer_city_match) and ".gov.cn" in url:
        source = "ISSUER_JURISDICTION_DEFAULT" if issuer_city_match else "DOCUMENT_CITY_SCOPE_INHERITANCE"
        return {
            "geography_state": "CITY_LEVEL",
            "geography_source": source,
            "geography_evidence": f"city={city}; title_or_issuer_match=true; official_url={url}",
            "geography_confidence": "MEDIUM",
        }
    return {
        "geography_state": "UNKNOWN",
        "geography_source": "INSUFFICIENT_DETERMINISTIC_EVIDENCE",
        "geography_evidence": combined[:360],
        "geography_confidence": "LOW",
    }


def close_action_gates(actions: pl.DataFrame, documents: pl.DataFrame) -> pl.DataFrame:
    """Return actions enriched with conservative direction/geography evidence."""

    documents_by_id = {
        _text(row.get("document_id")): row
        for row in documents.iter_rows(named=True)
        if _text(row.get("document_id"))
    }
    rows: list[dict[str, Any]] = []
    for action in actions.iter_rows(named=True):
        document = documents_by_id.get(_text(action.get("document_id")), {})
        row = dict(action)
        direction = derive_direction(row, document)
        geography = derive_geography(row, document)
        row.update(direction)
        row.update(geography)
        if direction["direction_state"] != "UNKNOWN":
            row["action_direction"] = direction["direction_state"]
        if geography["geography_state"] != "UNKNOWN":
            row["geographic_scope"] = geography["geography_state"]
        rows.append(row)
    return pl.DataFrame(rows, infer_schema_length=None) if rows else actions


__all__ = ["close_action_gates", "derive_direction", "derive_geography"]
