from datetime import UTC, datetime, timedelta

import polars as pl

from policydb.autopilot import AutopilotStateStore
from policydb.autopilot_runtime import select_manual_research_slots


def _row(slot_id: str, work_status: str) -> dict[str, object]:
    return {
        'slot_id': slot_id,
        'city_id': f'CITY_{slot_id}',
        'city_name': f'City {slot_id}',
        'province_name': 'Province',
        'source_role': 'housing_department',
        'work_status': work_status,
        'coverage_status': 'no_candidate',
        'candidate_count': 0,
        'best_candidate_id': None,
        'health_probe_success_count': 0,
        'verified_candidate_count': 0,
        'enabled_source_count': 0,
        'is_verified': False,
        'is_enabled': False,
        'manual_review_status': None,
    }


def test_recent_failed_research_cools_down_without_starving_no_candidate() -> None:
    now = datetime(2026, 8, 5, 13, 30, tzinfo=UTC)
    queue = pl.from_dicts(
        [_row('RECENT_FAILURE', 'failed_recoverable'), _row('NO_CANDIDATE', 'no_candidate_manual_research')],
        infer_schema_length=None,
    )
    checkpoints = {
        'RECENT_FAILURE': {
            'status': 'FAILED_RECOVERABLE',
            'updated_at': (now - timedelta(minutes=5)).isoformat(),
        }
    }

    selected = select_manual_research_slots(
        queue,
        max_slots=20,
        checkpoint_records=checkpoints,
        now=now,
        retry_cooldown_seconds=3600,
    )

    assert selected['slot_id'].to_list() == ['NO_CANDIDATE']


def test_expired_failed_research_becomes_claimable() -> None:
    now = datetime(2026, 8, 5, 13, 30, tzinfo=UTC)
    queue = pl.from_dicts([_row('EXPIRED_FAILURE', 'failed_recoverable')], infer_schema_length=None)
    checkpoints = {
        'EXPIRED_FAILURE': {
            'status': 'FAILED_RECOVERABLE',
            'updated_at': (now - timedelta(hours=2)).isoformat(),
        }
    }

    selected = select_manual_research_slots(
        queue,
        max_slots=20,
        checkpoint_records=checkpoints,
        now=now,
        retry_cooldown_seconds=3600,
    )

    assert selected['slot_id'].to_list() == ['EXPIRED_FAILURE']


def test_interrupted_recoverable_state_is_append_only_and_auditable(tmp_path) -> None:
    store = AutopilotStateStore(tmp_path / 'run')
    store.write({'job_id': 'JOB', 'run_id': 'RUN', 'status': 'SOURCE_COMPLETION'})

    state = store.transition(
        new_status='INTERRUPTED_RECOVERABLE',
        reason_code='worker_missing_after_boundary',
        idempotency_key='RUN:interrupted:1',
    )

    assert state['status'] == 'INTERRUPTED_RECOVERABLE'
    events = store.events_path.read_text(encoding='utf-8').splitlines()
    assert len(events) == 1
    assert 'worker_missing_after_boundary' in events[0]
