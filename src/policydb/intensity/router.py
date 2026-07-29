from __future__ import annotations

import hashlib
from datetime import UTC, datetime

from policydb.intensity.models import DecisionCandidate, ModelDecision


class HybridDecisionRouter:
    """Task-specific deterministic arbitration; never averages raw model scores."""

    version = "1.0.0"

    @staticmethod
    def _id(*parts: object) -> str:
        digest = hashlib.sha256("|".join(str(part) for part in parts).encode()).hexdigest()[:24]
        return f"DECISION_{digest}"

    def route(
        self,
        *,
        record_id: str,
        task_name: str,
        candidates: list[DecisionCandidate],
        action_id: str | None = None,
    ) -> ModelDecision:
        now = datetime.now(UTC).isoformat()
        evidenced = [candidate for candidate in candidates if candidate.has_evidence]
        rule = next((candidate for candidate in evidenced if candidate.method.startswith("rule")), None)
        values = [candidate.value for candidate in evidenced]
        normalized_values = [str(value) for value in values]
        agreement = max(
            (
                normalized_values.count(value) / len(normalized_values)
                for value in set(normalized_values)
            ),
            default=0.0,
        )
        accepted = None
        method = "unresolved"
        review = True
        reason = "no_evidenced_candidate"
        fallback = "manual_review"
        confidence = 0.0
        if task_name in {"numeric_value", "numeric_pair", "calibration"} and rule:
            accepted = rule
            method = "deterministic_rule"
            conflicts = any(candidate.value != rule.value for candidate in evidenced if candidate is not rule)
            review = conflicts
            reason = "numeric_rule_overrides_models" + ("_with_conflict" if conflicts else "")
            fallback = "rule_numeric_priority"
            confidence = rule.confidence * (0.8 if conflicts else 1.0)
        elif evidenced and len({str(value) for value in values}) == 1:
            accepted = max(evidenced, key=lambda candidate: candidate.confidence)
            method = "model_consensus" if len(evidenced) > 1 else "task_priority"
            review = accepted.confidence < 0.70
            reason = "all_evidenced_candidates_agree"
            fallback = "consensus"
            confidence = min(1.0, sum(candidate.confidence for candidate in evidenced) / len(evidenced))
        elif rule:
            accepted = rule
            method = "task_priority"
            review = True
            reason = "rule_selected_but_models_disagree"
            fallback = "rule_then_manual"
            confidence = rule.confidence * 0.75
        elif evidenced:
            accepted = max(evidenced, key=lambda candidate: candidate.confidence)
            method = "task_priority"
            review = True
            reason = "model_disagreement"
            fallback = "highest_confidence_then_manual"
            confidence = accepted.confidence * 0.65
        return ModelDecision(
            decision_id=self._id(record_id, action_id, task_name, self.version, [c.model_dump() for c in candidates]),
            action_id=action_id,
            record_id=record_id,
            task_name=task_name,
            accepted_value=accepted.value if accepted else None,
            accepted_method=method,
            agreement=agreement,
            decision_confidence=confidence,
            review_required=review,
            decision_reason=reason,
            router_version=self.version,
            candidate_methods=[candidate.method for candidate in candidates],
            fallback_path=fallback,
            created_at=now,
        )
