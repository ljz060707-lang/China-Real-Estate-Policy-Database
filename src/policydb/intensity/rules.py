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


def split_clauses(text: str, *, record_id: str) -> list[Clause]:
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
        clauses.append(Clause(_stable_id(record_id, start, end, prefix="CLAUSE"), stripped, start, end))
        cursor += len(part)
    return clauses


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
        for instrument, patterns in self.patterns["instrument_patterns"].items():
            if any(term in clause for term in patterns):
                return instrument
        return None

    def _direction(self, clause: str) -> str:
        matches = {
            direction
            for direction, words in self.patterns["action_verbs"].items()
            if any(word in clause for word in words)
        }
        if len(matches) > 1:
            return "mixed"
        return next(iter(matches), "unknown")

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
        actions: list[PolicyAction] = []
        seen: set[tuple[str, str]] = set()
        for clause in split_clauses(text, record_id=record_id):
            instrument = self._instrument(clause.text)
            direction = self._direction(clause.text)
            has_action = direction != "unknown" or any(
                word in clause.text for words in self.patterns["action_verbs"].values() for word in words
            ) or any(
                word in clause.text
                for words in self.binding["scores"].values()
                for word in words
            )
            if not instrument or not has_action:
                continue
            key = (instrument, clause.text)
            if key in seen:
                continue
            seen.add(key)
            action_id = _stable_id(record_id, document_version_id, clause.start, instrument, prefix="ACTION")
            formal = completeness == "full_official_text"
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
