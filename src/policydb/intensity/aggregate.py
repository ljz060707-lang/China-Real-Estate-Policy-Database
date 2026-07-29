from __future__ import annotations

import hashlib


def coverage_intensity_value(
    coverage_status: str,
    *,
    action_count: int,
    gross_intensity: float | None,
) -> float | None:
    """Distinguish unobserved windows from verified zero policy actions."""

    if coverage_status in {"not_scanned", "partial", "failed", "unknown"}:
        return None
    if coverage_status == "confirmed_zero" and action_count == 0:
        return 0.0
    return gross_intensity


def apply_revision_stock(
    events: list[dict],
) -> list[dict]:
    """Apply amendment/repeal events without treating revisions as ordinary duplicates."""

    active: dict[str, dict] = {}
    snapshots: list[dict] = []
    for event in sorted(events, key=lambda row: (row["effective_date"], row["action_id"])):
        family = event.get("action_family_id") or event["action_id"]
        relation = event.get("relation_type")
        if relation in {"repeals", "replaces", "supersedes"}:
            target = event.get("target_action_family_id") or family
            active.pop(target, None)
        if event.get("status") not in {"repealed", "expired"}:
            active[family] = event
        snapshots.append(
            {
                "effective_date": event["effective_date"],
                "event_action_id": event["action_id"],
                "active_action_count": len(active),
                "stock_intensity": sum(float(row.get("intensity") or 0) for row in active.values()),
            }
        )
    return snapshots


def deduplicate_policy_actions(actions: list[dict]) -> list[dict]:
    """Keep one action per policy entity/semantic action while preserving source versions elsewhere."""

    selected: dict[tuple[str, str], dict] = {}
    for action in actions:
        entity = action.get("policy_entity_id") or action.get("record_id")
        semantic = action.get("semantic_action_key")
        if not semantic:
            text = "".join(str(action.get("clause_text") or "").split())
            semantic = hashlib.sha256(
                f"{action.get('instrument')}|{action.get('direction')}|{text}".encode()
            ).hexdigest()
        key = (str(entity), str(semantic))
        current = selected.get(key)
        if current is None or action.get("source_priority", 0) > current.get("source_priority", 0):
            selected[key] = action
    return list(selected.values())


def unique_model_input_count(actions: list[dict], city_links: list[dict]) -> int:
    """City expansion never multiplies model calls for the same action."""

    _ = city_links
    return len({action["action_id"] for action in actions})
