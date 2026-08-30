from __future__ import annotations

import hashlib
import math
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import yaml

from policydb.intensity.models import ActionCalibration, PolicyAction, TextCompleteness

CLAUSE_BOUNDARY = re.compile(r"(?<=[。！？；;])|\n+")
PAIR_PATTERN = re.compile(
    r"(?:由|从)?\s*(?P<old>\d+(?:\.\d+)?)\s*(?P<old_unit>%|％|万元|亿元|元|年|个月|万平方米|平方米|套|户)"
    r"\s*(?P<verb>降至|降低至|下调至|缩短至|提高至|上调至|增加至|调整为|变更为)\s*"
    r"(?P<new>\d+(?:\.\d+)?)\s*(?P<new_unit>%|％|万元|亿元|元|年|个月|万平方米|平方米|套|户)"
)
NUMBER_PATTERN = re.compile(
    r"(?P<value>\d+(?:\.\d+)?)\s*(?P<unit>%|％|万元|亿元|元|年|个月|万平方米|平方米|套|户)"
)
# Numbered clause markers: 第X条 / 第(X)条 / （一） / 一、 / 1.  (deterministic structure)
NUMBERED_CLAUSE_RE = re.compile(
    r"(?:第[（(]?[一二三四五六七八九十百千0-9]+[）)]?条|"
    r"（[一二三四五六七八九十0-9]+）|"
    r"(?:^|\n)\s*[一二三四五六七八九十百]+、|"
    r"(?:^|\n)\s*[0-9]{1,2}[\.、])"
)
SENTENCE_RE = re.compile(r"[。；;！？!]+")
NEGATION_WINDOW = 8  # chars before a verb/keyword to consider negation
# Negation / reversal context terms (context feature; the AI layer decides the
# final semantic direction — the deterministic layer never flips silently).
NEGATION_TERMS = (
    "取消", "不再", "停止", "废除", "废止", "终止", "解除", "豁免", "不受",
    "暂停", "不予", "不得", "继续执行", "不再执行", "保持不变",
)
DATE_MENTION_RE = re.compile(r"(?<!\d)(20\d{2})\s*[-/.年]\s*(\d{1,2})\s*[-/.月]\s*(\d{1,2})\s*日?")
PARAM_PERCENT_RE = re.compile(r"(?<!\d)(\d{1,3}(?:\.\d+)?)\s*%|百分之([0-9一二三四五六七八九十百]+)")
PARAM_AMOUNT_RE = re.compile(r"(\d+(?:\.\d+)?)\s*(万亿元|亿元|万元|元)")
PARAM_YEAR_RE = re.compile(r"满\s*(\d+)\s*年|(\d+)\s*年(?:以[上内]|以上)")
GEO_SCOPE_RE = re.compile(r"(全市|全域|本县|本市区|市区|中心城区|全省|县区|主城区|全市范围|本市|本省)")

_GEO_PROVINCES = (
    "北京", "天津", "上海", "重庆", "河北", "山西", "辽宁", "吉林", "黑龙江", "江苏",
    "浙江", "安徽", "福建", "江西", "山东", "河南", "湖北", "湖南", "广东", "海南",
    "四川", "贵州", "云南", "陕西", "甘肃", "青海", "内蒙古", "广西", "西藏", "宁夏",
    "新疆", "香港", "澳门",
)


def _stable_id(*parts: object, prefix: str) -> str:
    digest = hashlib.sha256("|".join(str(part or "") for part in parts).encode("utf-8")).hexdigest()[:24]
    return f"{prefix}_{digest}"


def classify_text_completeness(
    text: str | None,
    *,
    official_status: str,
    title: str | None = None,
) -> TextCompleteness:
    clean = (text or "").strip()
    if not clean:
        return "missing_text"
    official = official_status in {"official", "official_reprint", "consultation_draft"}
    if not official:
        return "third_party_summary"
    if len(clean) < 180:
        return "title_abstract_only"
    truncation = any(marker in clean[-80:] for marker in ("……", "全文见", "详见附件", "点击查看"))
    structural = len(re.findall(r"[一二三四五六七八九十]+、|第[一二三四五六七八九十\d]+条", clean)) >= 2
    if len(clean) >= 1000 and structural and not truncation:
        return "full_official_text"
    if title and "摘要" in title and len(clean) < 500:
        return "title_abstract_only"
    return "partial_official_text"


@dataclass(frozen=True)
class Clause:
    clause_id: str
    text: str
    start: int
    end: int
    number: str | None = None  # numbered marker (第X条 / （一） / 一、 / 1.) when present


def split_clauses(text: str, *, record_id: str) -> list[Clause]:
    """Split into clauses, preserving absolute positions and numbered markers."""
    clauses: list[Clause] = []
    cursor = 0
    for part in CLAUSE_BOUNDARY.split(text):
        if not part:
            continue
        stripped = part.strip()
        if not stripped:
            cursor += len(part)
            continue
        local = part.find(stripped)
        start = cursor + max(local, 0)
        end = start + len(stripped)
        markers = list(NUMBERED_CLAUSE_RE.finditer(stripped))
        if not markers:
            clauses.append(
                Clause(_stable_id(record_id, start, end, prefix="CLAUSE"), stripped, start, end)
            )
            cursor += len(part)
            continue
        # Split the part at numbered markers, attaching the marker to its first
        # sub-clause only; sentence-split the body so compound numbered items
        # (e.g. one 条 with several policy sentences) yield separate candidates.
        segment_start = 0
        for index, match in enumerate(markers):
            if match.start() > segment_start:
                head = stripped[segment_start : match.start()].strip()
                if head:
                    head_start = start + segment_start
                    clauses.append(
                        Clause(
                            _stable_id(record_id, head_start, head_start + len(head), prefix="CLAUSE"),
                            head,
                            head_start,
                            head_start + len(head),
                        )
                    )
            number = stripped[match.start() : match.end()].strip()
            body_start = match.end()
            body_end = markers[index + 1].start() if index + 1 < len(markers) else len(stripped)
            body = stripped[body_start:body_end].strip()
            sub_parts = [part.strip() for part in SENTENCE_RE.split(body) if part.strip()]
            if not sub_parts:
                sub_parts = [body]
            for sub_index, sub in enumerate(sub_parts):
                prefix = f"{number} " if sub_index == 0 else ""
                text = f"{prefix}{sub}".strip()
                if sub_index == 0:
                    offset = match.start()
                else:
                    offset = match.start() + body.find(sub)
                seg_start = start + offset
                clauses.append(
                    Clause(
                        _stable_id(record_id, seg_start, seg_start + len(text), prefix="CLAUSE"),
                        text,
                        seg_start,
                        seg_start + len(text),
                        number=number if sub_index == 0 else None,
                    )
                )
            segment_start = match.start()
        cursor += len(part)
    return clauses


def _scan_negation(clause: str) -> list[str]:
    return [term for term in NEGATION_TERMS if term in clause]


def _scan_mentions(clause: str) -> list[dict[str, object]]:
    mentions: list[dict[str, object]] = []
    for match in DATE_MENTION_RE.finditer(clause):
        mentions.append(
            {"kind": "date", "text": clause[match.start() : match.end()],
             "start": match.start(), "end": match.end()}
        )
    for pattern in (PARAM_PERCENT_RE, PARAM_AMOUNT_RE, PARAM_YEAR_RE):
        for match in pattern.finditer(clause):
            mentions.append(
                {"kind": "parameter", "text": clause[match.start() : match.end()],
                 "start": match.start(), "end": match.end()}
            )
    for match in GEO_SCOPE_RE.finditer(clause):
        mentions.append(
            {"kind": "geography", "text": clause[match.start() : match.end()],
             "start": match.start(), "end": match.end()}
        )
    for province in _GEO_PROVINCES:
        position = clause.find(province)
        if position >= 0:
            mentions.append(
                {"kind": "geography", "text": province, "start": position, "end": position + len(province)}
            )
    mentions.sort(key=lambda m: (int(m["start"]), str(m["kind"])))
    return mentions


class DeterministicPolicyRules:
    version = "1.0.0"

    def __init__(self, reference_dir: Path) -> None:
        self.reference_dir = reference_dir
        self.patterns = yaml.safe_load((reference_dir / "policy_action_patterns.yaml").read_text(encoding="utf-8"))
        self.scales = yaml.safe_load((reference_dir / "policy_calibration_scales.yaml").read_text(encoding="utf-8"))
        self.binding = yaml.safe_load((reference_dir / "policy_binding_lexicon.yaml").read_text(encoding="utf-8"))

    def is_interpretation(self, title: str | None, text: str) -> bool:
        title = title or ""
        negative = self.patterns["negative_document_patterns"]
        return any(term in title for term in negative) and not any(
            marker in text for marker in ("决定自", "本通知自", "现将有关事项通知如下")
        )

    def _instrument(self, clause: str) -> str | None:
        """Position-based instrument: the family whose term occurs EARLIEST wins.

        Compound clauses (e.g. 公积金贷款额度…，公积金首付…) resolve to the
        instrument mentioned first rather than dictionary order.
        """
        best: tuple[int, str] | None = None
        for instrument, patterns in self.patterns["instrument_patterns"].items():
            for term in patterns:
                position = clause.find(term)
                if position >= 0 and (best is None or position < best[0]):
                    best = (position, instrument)
        return best[1] if best else None

    def _direction(self, clause: str, negation: list[str] | None = None) -> str:
        negation = negation or []
        matches = {
            direction: [
                word for word in words
                if word in clause and not (
                    negation
                    and any(
                        term in clause[max(0, clause.find(word) - NEGATION_WINDOW) : clause.find(word)]
                        for term in negation
                    )
                )
            ]
            for direction, words in self.patterns["action_verbs"].items()
        }
        present = {direction for direction, words in matches.items() if words}
        # 提高至/上调至/增加至 + 额度/上限 is a quota raise -> supportive, not tightening.
        if any(word in clause for word in ("提高至", "上调至", "增加至", "提高额度", "提高上限")) and any(
            word in clause for word in ("额度", "上限", "万元", "亿元")
        ):
            present.discard("tightening")
            present.add("supportive")
        if len(present) > 1:
            return "mixed"
        return next(iter(present), "unknown")

    def extract_actions(
        self,
        *,
        record_id: str,
        text: str,
        title: str | None,
        official_status: str,
        document_version_id: str | None = None,
    ) -> list[PolicyAction]:
        if self.is_interpretation(title, text):
            return []
        completeness = classify_text_completeness(text, official_status=official_status, title=title)
        now = datetime.now(UTC).isoformat()
        # Document-level date mentions (e.g. 施行日期) propagate to every action
        # whose own clause carries no date mention — deterministic linkage.
        document_dates = [
            {"kind": "date", "text": match.group(0), "start": match.start(), "end": match.end()}
            for match in DATE_MENTION_RE.finditer(text)
        ]
        actions: list[PolicyAction] = []
        seen: set[tuple[str, str]] = set()
        for clause in split_clauses(text, record_id=record_id):
            instrument = self._instrument(clause.text)
            negation = _scan_negation(clause.text)
            direction = self._direction(clause.text, negation)
            has_action = direction != "unknown" or any(
                word in clause.text for words in self.patterns["action_verbs"].values() for word in words
            ) or any(
                word in clause.text
                for words in self.binding["scores"].values()
                for word in words
            ) or bool(
                instrument and NUMBER_PATTERN.search(clause.text)
            )
            if not instrument or not has_action:
                continue
            key = (instrument, clause.text)
            if key in seen:
                continue
            seen.add(key)
            action_id = _stable_id(record_id, document_version_id, clause.start, instrument, prefix="ACTION")
            formal = completeness == "full_official_text"
            mentions = _scan_mentions(clause.text)
            if not any(m["kind"] == "date" for m in mentions):
                mentions = list(mentions) + [
                    {"kind": "date", "text": m["text"], "start": m["start"], "end": m["end"],
                     "source": "document"}
                    for m in document_dates
                ]
            actions.append(
                PolicyAction(
                    action_id=action_id,
                    record_id=record_id,
                    document_version_id=document_version_id,
                    clause_id=clause.clause_id,
                    clause_text=clause.text,
                    evidence_start=clause.start,
                    evidence_end=clause.end,
                    instrument=instrument,
                    direction=direction,
                    action_status="active" if formal else "provisional",
                    text_completeness=completeness,
                    formal_eligible=formal,
                    evidence_text=clause.text,
                    negation_terms=_scan_negation(clause.text),
                    mentions=mentions,
                    clause_number=clause.number,
                    created_at=now,
                    updated_at=now,
                )
            )
        return actions

    @staticmethod
    def _convert(value: float, unit: str) -> tuple[float, str]:
        if unit in {"%", "％"}:
            return value / 100.0, "percent"
        if unit == "万元":
            return value * 10_000, "CNY"
        if unit == "亿元":
            return value * 100_000_000, "CNY"
        if unit == "元":
            return value, "CNY"
        if unit == "个月":
            return value / 12.0, "year"
        if unit == "年":
            return value, "year"
        if unit == "万平方米":
            return value * 10_000, "square_meter"
        if unit == "平方米":
            return value, "square_meter"
        if unit in {"套", "户"}:
            return value, "unit"
        return value, unit

    def _measure_type(self, text: str, unit: str) -> str:
        if "首付" in text:
            return "mortgage_downpayment"
        if "公积金" in text and any(word in text for word in ("额度", "贷款")):
            return "provident_fund_quota"
        if any(word in text for word in ("社保", "纳税")) and unit == "year":
            return "social_security_years"
        if "限售" in text or "转让" in text:
            return "sale_restriction_years"
        if "补贴" in text or "奖励" in text:
            return "subsidy_amount"
        if unit == "unit":
            return "housing_units"
        if unit == "square_meter":
            return "floor_area"
        return "other_numeric_measure"

    def extract_calibrations(self, action: PolicyAction) -> list[ActionCalibration]:
        now = datetime.now(UTC).isoformat()
        results: list[ActionCalibration] = []
        for index, match in enumerate(PAIR_PATTERN.finditer(action.clause_text)):
            old, unit = self._convert(float(match.group("old")), match.group("old_unit"))
            new, new_unit = self._convert(float(match.group("new")), match.group("new_unit"))
            measure = self._measure_type(action.clause_text, unit)
            config = self.scales["tools"].get(measure)
            compatible = unit == new_unit and config is not None
            standardized = (new - old) / float(config["scale"]) if compatible else None
            magnitude = 1 - math.exp(-abs(standardized)) if standardized is not None else None
            verb = match.group("verb")
            direction = "loosening" if any(x in verb for x in ("降", "下调", "缩短")) else "supportive"
            start = action.evidence_start + match.start()
            end = action.evidence_start + match.end()
            results.append(
                ActionCalibration(
                    calibration_id=_stable_id(action.action_id, index, match.group(0), prefix="CAL"),
                    action_id=action.action_id,
                    record_id=action.record_id,
                    measure_type=measure,
                    old_value=old,
                    new_value=new,
                    unit=unit,
                    standardized_change=standardized,
                    magnitude=magnitude,
                    direction=direction,
                    pairing_status="paired" if compatible else "ambiguous",
                    evidence_text=match.group(0),
                    evidence_start=start,
                    evidence_end=end,
                    review_required=not compatible,
                    created_at=now,
                )
            )
        if results:
            return results
        numbers = list(NUMBER_PATTERN.finditer(action.clause_text))
        for index, match in enumerate(numbers):
            value, unit = self._convert(float(match.group("value")), match.group("unit"))
            measure = self._measure_type(action.clause_text, unit)
            start = action.evidence_start + match.start()
            end = action.evidence_start + match.end()
            results.append(
                ActionCalibration(
                    calibration_id=_stable_id(action.action_id, index, match.group(0), prefix="CAL"),
                    action_id=action.action_id,
                    record_id=action.record_id,
                    measure_type=measure,
                    new_value=value,
                    unit=unit,
                    direction=action.direction,
                    pairing_status="single_value",
                    evidence_text=match.group(0),
                    evidence_start=start,
                    evidence_end=end,
                    review_required=True,
                    created_at=now,
                )
            )
        return results
