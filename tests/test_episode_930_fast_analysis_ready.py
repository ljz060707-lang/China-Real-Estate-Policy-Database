from __future__ import annotations

from pathlib import Path

import polars as pl

from policydb.ai import AIStructuredOutputError, classify_ai_failure
from policydb.episode_930_monitor import (
    action_extraction_readiness,
    gap_impact,
    no_api_success_for_15m,
    split_gap_metrics,
    timeout_fingerprint,
)
from policydb.episode_930_production import (
    analysis_ready_decision,
    api_classification_allowed,
    derive_recovery_priority,
    freeze_analysis_ready_scope,
    load_authoritative_episode_gaps,
    prioritize_recovery_queue,
    recovery_timeout_policy,
    select_next_work_source,
    select_recovery_claim_rows,
)


def test_api_2xx_parseable_invalid_schema_is_not_provider_failure() -> None:
    error = AIStructuredOutputError(
        "schema rejected",
        parse_status="validation_failed",
        raw_response_hash="abc",
        raw_fields=("result",),
        raw_payload={"result": {"unexpected": True}},
        http_status=200,
        response_bytes=31,
        schema_errors=("actions: Field required",),
    )

    diagnostics = classify_ai_failure(error, latency_ms=125.0)

    assert diagnostics["failure_class"] == "SCHEMA_VALIDATION_FAILURE"
    assert diagnostics["http_status"] == 200
    assert diagnostics["response_received"] is True
    assert diagnostics["json_parse_ok"] is True
    assert diagnostics["schema_valid"] is False
    assert diagnostics["provider_error_message_sanitized"] == "schema rejected"


def test_no_success_fifteen_minute_boundary() -> None:
    assert no_api_success_for_15m(899) is False
    assert no_api_success_for_15m(900) is True
    assert no_api_success_for_15m(901) is True
    assert no_api_success_for_15m(None) is True


def test_extended_timeout_is_only_for_suspected_single_probe() -> None:
    extended = recovery_timeout_policy(
        "BACKOFF_SINGLE_PROBE",
        client_timeout_suspected=True,
        connect_timeout=10,
    )
    normal_micro = recovery_timeout_policy(
        "MICRO_5",
        client_timeout_suspected=False,
        connect_timeout=10,
    )
    normal_single = recovery_timeout_policy(
        "SINGLE_PROBE",
        client_timeout_suspected=False,
        connect_timeout=10,
    )

    assert extended == {
        "read_timeout": 300.0,
        "connect_timeout": 10.0,
        "max_retries": 0,
        "hard_wall_timeout_seconds": 330,
        "reason_code": "CLIENT_READ_TIMEOUT_SUSPECTED_SINGLE_PROBE",
    }
    assert normal_micro["read_timeout"] is None
    assert normal_single["read_timeout"] is None


def test_extended_timeout_must_cover_micro_batches_when_client_timeout_suspected() -> None:
    """Regression: MICRO_5/MICRO_20 must inherit the extended read timeout.

    When the SDK retry-chain fingerprint has already proven that the provider
    needs longer than the default 30s read timeout (observed SINGLE latency
    69-157s), letting MICRO_5 fall back to 30s + 3 SDK retries makes every
    micro batch fail (~127s wall clock), so SINGLE keeps passing while MICRO_5
    never can: the certification loop can never advance.
    """

    micro5 = recovery_timeout_policy(
        "MICRO_5",
        client_timeout_suspected=True,
        connect_timeout=10,
    )
    micro20 = recovery_timeout_policy(
        "MICRO_20",
        client_timeout_suspected=True,
        connect_timeout=10,
    )
    single = recovery_timeout_policy(
        "SINGLE_PROBE",
        client_timeout_suspected=True,
        connect_timeout=10,
    )

    for policy in (single, micro5, micro20):
        assert policy["read_timeout"] == 300.0
        assert policy["max_retries"] == 0
        assert policy["hard_wall_timeout_seconds"] == 330
        assert policy["reason_code"] == "CLIENT_READ_TIMEOUT_SUSPECTED_SINGLE_PROBE"

    # Without the fingerprint the normal policy remains unchanged.
    normal = recovery_timeout_policy(
        "MICRO_5",
        client_timeout_suspected=False,
        connect_timeout=10,
    )
    assert normal["read_timeout"] is None
    assert normal["max_retries"] is None


def test_pass2_is_not_eligible_before_pass1_success() -> None:
    decision = analysis_ready_decision(
        {
            "core_discovery": True,
            "official_evidence": True,
            "action_extraction": True,
            "api_pass1": False,
            "api_pass2": False,
            "date_verification": True,
            "critical_dedup": True,
            "critical_gaps": True,
            "formal_promotion": True,
            "dashboard_action_export": True,
        },
        pass1_success=0,
        pass1_total=5,
        pass2_success=0,
        final_recovery_remaining=0,
    )

    assert decision["pass2_not_yet_eligible"] == 5
    assert decision["pass2_eligible"] == 0
    assert decision["pass2_waiting"] == 0


def test_analysis_ready_export_can_pass_before_final_recovery() -> None:
    decision = analysis_ready_decision(
        {
            "core_discovery": True,
            "official_evidence": True,
            "action_extraction": True,
            "api_pass1": True,
            "api_pass2": True,
            "date_verification": True,
            "critical_dedup": True,
            "critical_gaps": True,
            "formal_promotion": True,
            "dashboard_action_export": True,
        },
        pass1_success=5,
        pass1_total=5,
        pass2_success=5,
        final_recovery_remaining=99,
    )

    assert decision["analysis_ready"] is True
    assert decision["final_ready"] is False


def test_critical_gap_impact_counts_share_one_authoritative_frame() -> None:
    gaps = pl.DataFrame(
        [
            {"gap_id": "G1", "document_id": "D1", "action_id": None, "city_id": "C1", "severity": "HIGH"},
            {"gap_id": "G2", "document_id": "D1", "action_id": "A1", "city_id": "C1", "severity": "MEDIUM"},
            {"gap_id": "G3", "document_id": None, "action_id": None, "city_id": "C2", "severity": "HIGH"},
        ]
    )

    impact = gap_impact(gaps)

    assert impact["blocking_gap_count"] == 3
    assert impact["affected_document_count"] == 1
    assert impact["affected_action_count"] == 1
    assert impact["affected_city_count"] == 2
    assert impact["non_document_action_gap_count"] == 1


def test_core_gap_metrics_are_separate_from_global_final_gaps() -> None:
    gaps = pl.DataFrame(
        [
            {"gap_id": "G_CORE", "document_id": "D_CORE", "action_id": None, "city_id": "C_CORE", "severity": "HIGH"},
            {"gap_id": "G_GLOBAL", "document_id": "D_GLOBAL", "action_id": None, "city_id": "C_OTHER", "severity": "HIGH"},
            {"gap_id": "G_CITY", "document_id": None, "action_id": None, "city_id": "C_CORE", "severity": "MEDIUM"},
        ]
    )

    split = split_gap_metrics(
        gaps,
        core_document_ids={"D_CORE"},
        core_action_ids=set(),
        core_city_ids={"C_CORE"},
        core_city_names=set(),
    )

    assert split["analysis_ready_core_blocking_gaps"]["blocking_gap_count"] == 2
    assert split["global_final_blocking_gaps"]["blocking_gap_count"] == 3
    assert split["analysis_ready_core_blocking_gaps"]["critical_severity_gap_count"] == 1


def test_core_rolling_gap_loader_prefers_curated_authoritative_register(tmp_path: Path) -> None:
    curated = tmp_path / "curated_gaps.parquet"
    local_run = tmp_path / "run_gap_register.parquet"
    pl.DataFrame(
        [{"gap_id": "CURATED_CORE", "document_id": "D_CORE", "severity": "HIGH"}]
    ).write_parquet(curated)
    pl.DataFrame(
        [{"gap_id": "LOCAL_ONLY", "document_id": "D_LOCAL", "severity": "HIGH"}]
    ).write_parquet(local_run)

    loaded = load_authoritative_episode_gaps(curated, (local_run,))

    assert loaded.get_column("gap_id").to_list() == ["CURATED_CORE"]


def test_timeout_fingerprint_requires_two_configured_samples() -> None:
    one = timeout_fingerprint(
        pl.DataFrame([{"failure_class": "READ_TIMEOUT", "latency_ms": 120_100}]),
        configured_read_timeout=120,
    )
    two = timeout_fingerprint(
        pl.DataFrame(
            [
                {"failure_class": "READ_TIMEOUT", "latency_ms": 120_100, "configured_read_timeout": 120},
                {"failure_class": "READ_TIMEOUT", "latency_ms": 121_300, "configured_read_timeout": 120},
            ]
        ),
    )

    assert one["CLIENT_READ_TIMEOUT_SUSPECTED"] is None
    assert two["CLIENT_READ_TIMEOUT_SUSPECTED"] is True


def test_timeout_fingerprint_detects_configured_sdk_retry_chain() -> None:
    result = timeout_fingerprint(
        pl.DataFrame(
            [
                {
                    "failure_class": "READ_TIMEOUT",
                    "latency_ms": 125_800,
                    "configured_read_timeout": 30,
                    "max_retries": 3,
                },
                {
                    "failure_class": "READ_TIMEOUT",
                    "latency_ms": 128_500,
                    "configured_read_timeout": 30,
                    "max_retries": 3,
                },
            ]
        )
    )

    assert result["CLIENT_READ_TIMEOUT_SUSPECTED"] is True
    assert result["SDK_RETRY_CHAIN_SUSPECTED"] is True
    assert result["reason_code"] == "SDK_RETRY_CHAIN_SUSPECTED"


def test_action_extraction_distinguishes_global_and_analysis_scope() -> None:
    readiness = action_extraction_readiness(
        eligible_document_ids={"D1", "D2"},
        completed_document_ids={"D1"},
        analysis_scope_document_ids={"D1"},
        excluded_with_reason={"D2": "NO_DETERMINISTIC_POLICY_CLAUSE"},
    )

    assert readiness["global"]["percent"] == 50.0
    assert readiness["analysis_ready"]["percent"] == 100.0
    assert readiness["analysis_ready"]["gate"] == "PASS"
    assert readiness["excluded_with_reason"] == {"D2": "NO_DETERMINISTIC_POLICY_CLAUSE"}


def test_frozen_analysis_ready_scope_cannot_mutate(tmp_path: Path) -> None:
    path = tmp_path / "930_ANALYSIS_READY_SCOPE.json"
    first_queue = pl.DataFrame(
        [
            {
                "queue_item_id": "Q1",
                "city_id": "CITY_110000",
                "city": "北京市",
                "priority": 10,
                "window_start": "2016-09-25",
                "window_end": "2016-10-10",
            }
        ]
    )
    first = freeze_analysis_ready_scope(path, first_queue, created_at="2026-08-14T00:00:00+00:00")
    original = path.read_bytes()
    changed_queue = pl.concat(
        [
            first_queue,
            pl.DataFrame(
                [
                    {
                        "queue_item_id": "Q2",
                        "city_id": "CITY_120000",
                        "city": "天津市",
                        "priority": 10,
                        "window_start": "2016-09-25",
                        "window_end": "2016-10-10",
                    }
                ]
            ),
        ]
    )

    second = freeze_analysis_ready_scope(path, changed_queue, created_at="2026-08-15T00:00:00+00:00")

    assert second == first
    assert path.read_bytes() == original
    assert first["cities"] == ["北京市"]
    assert first["scope_hash"]


def test_priority_queue_keeps_every_global_recovery_item() -> None:
    recovery = pl.DataFrame(
        [
            {"recovery_id": "R1", "queue_item_id": "Q1", "status": "RECOVERY_REQUIRED"},
            {"recovery_id": "R2", "queue_item_id": "Q2", "status": "RECOVERY_REQUIRED"},
            {"recovery_id": "R3", "queue_item_id": "Q3", "status": "RECOVERY_REQUIRED"},
        ]
    )
    queue = pl.DataFrame(
        [
            {"queue_item_id": "Q1", "city_id": "C1"},
            {"queue_item_id": "Q2", "city_id": "C2"},
            {"queue_item_id": "Q3", "city_id": "C3"},
        ]
    )
    scope = {"queue_item_ids": ["Q1"]}

    prioritized = prioritize_recovery_queue(recovery, queue, scope, critical_city_ids={"C2"})

    assert prioritized.height == recovery.height
    assert prioritized.get_column("queue_item_id").to_list() == ["Q1", "Q2", "Q3"]
    assert prioritized.get_column("priority_lane").to_list() == [0, 1, 2]


def test_old_overlay_derives_exact_core_and_gap_priority() -> None:
    recovery = pl.DataFrame(
        [
            {"recovery_id": "R3", "queue_item_id": "Q3", "status": "RECOVERY_REQUIRED", "priority_lane": 1},
            {"recovery_id": "R1", "queue_item_id": "Q1", "status": "RECOVERY_REQUIRED", "priority_lane": 3},
            {"recovery_id": "R2", "queue_item_id": "Q2", "status": "RECOVERY_REQUIRED", "priority_lane": 2},
        ]
    )
    queue = pl.DataFrame(
        [
            {"queue_item_id": "Q1", "city_id": "C1", "city": "甲市", "priority": 30},
            {"queue_item_id": "Q2", "city_id": "C2", "city": "乙市", "priority": 10},
            {"queue_item_id": "Q3", "city_id": "C3", "city": "丙市", "priority": 20},
        ]
    )

    prioritized = derive_recovery_priority(
        recovery,
        queue,
        {"queue_item_ids": ["Q1"], "scope_hash": None},
        critical_city_ids={"C2"},
    )

    assert prioritized.get_column("queue_item_id").to_list() == ["Q1", "Q2", "Q3"]
    assert prioritized.get_column("normalized_priority").to_list() == [0, 1, 2]
    assert prioritized.get_column("priority_reason").to_list() == [
        "ANALYSIS_READY_CORE",
        "CRITICAL_GAP_CLOSURE",
        "GLOBAL_FINAL_RECOVERY",
    ]


def test_recovery_claim_does_not_consume_lower_priority_while_core_required() -> None:
    recovery = pl.DataFrame(
        [
            {"recovery_id": "R1", "queue_item_id": "Q1", "status": "RECOVERY_REQUIRED"},
            {"recovery_id": "R2", "queue_item_id": "Q2", "status": "RECOVERY_REQUIRED"},
            {"recovery_id": "R3", "queue_item_id": "Q3", "status": "RECOVERY_REQUIRED"},
        ]
    )
    queue = pl.DataFrame(
        [
            {"queue_item_id": "Q1", "city_id": "C1", "city": "甲市", "priority": 30},
            {"queue_item_id": "Q2", "city_id": "C2", "city": "乙市", "priority": 10},
            {"queue_item_id": "Q3", "city_id": "C3", "city": "丙市", "priority": 10},
        ]
    )

    claims = select_recovery_claim_rows(
        recovery,
        queue,
        {"queue_item_ids": ["Q1"]},
        ["C1", "C2", "C3"],
        critical_city_ids={"C2"},
    )

    assert [row["queue_item_id"] for row in claims] == ["Q1"]
    assert all(row["normalized_priority"] == 0 for row in claims)


def test_global_p0_blocks_lower_lane_when_current_city_set_has_no_p0() -> None:
    recovery = pl.DataFrame(
        [
            {"recovery_id": "R1", "queue_item_id": "Q1", "status": "RECOVERY_REQUIRED"},
            {"recovery_id": "R2", "queue_item_id": "Q2", "status": "RECOVERY_REQUIRED"},
        ]
    )
    queue = pl.DataFrame(
        [
            {"queue_item_id": "Q1", "city_id": "C1", "city": "甲市", "priority": 10},
            {"queue_item_id": "Q2", "city_id": "C2", "city": "乙市", "priority": 10},
        ]
    )

    claims = select_recovery_claim_rows(
        recovery,
        queue,
        {"queue_item_ids": ["Q1"]},
        ["C2"],
    )

    assert claims == []


def test_global_work_source_arbitration_prefers_core_then_gap_then_final() -> None:
    recovery = pl.DataFrame(
        [
            {"recovery_id": "R0", "queue_item_id": "Q0", "status": "RECOVERY_REQUIRED"},
            {"recovery_id": "R1", "queue_item_id": "Q1", "status": "RECOVERY_REQUIRED"},
            {"recovery_id": "R2", "queue_item_id": "Q2", "status": "RECOVERY_REQUIRED"},
        ]
    )
    queue = pl.DataFrame(
        [
            {"queue_item_id": "Q0", "city_id": "C0", "city": "甲市", "priority": 20},
            {"queue_item_id": "Q1", "city_id": "C1", "city": "乙市", "priority": 10},
            {"queue_item_id": "Q2", "city_id": "C2", "city": "丙市", "priority": 10},
        ]
    )
    scope = {"queue_item_ids": ["Q0"]}

    core = select_next_work_source(
        recovery, queue, scope, city_limit=2, critical_city_ids={"C1"}
    )
    assert core["work_source"] == "CORE_RECOVERY"
    assert core["normalized_priority"] == 0
    assert core["core_scope_member"] is True
    assert core["queue_item_ids"] == ["Q0"]

    recovery = recovery.with_columns(
        pl.when(pl.col("queue_item_id") == "Q0")
        .then(pl.lit("RECOVERY_COMPLETED"))
        .otherwise(pl.col("status"))
        .alias("status")
    )
    gap = select_next_work_source(
        recovery, queue, scope, city_limit=2, critical_city_ids={"C1"}
    )
    assert gap["work_source"] == "CRITICAL_GAP_RECOVERY"
    assert gap["normalized_priority"] == 1
    assert gap["queue_item_ids"] == ["Q1"]

    recovery = recovery.with_columns(
        pl.when(pl.col("queue_item_id") == "Q1")
        .then(pl.lit("RECOVERY_COMPLETED"))
        .otherwise(pl.col("status"))
        .alias("status")
    )
    final = select_next_work_source(
        recovery, queue, scope, city_limit=2, critical_city_ids={"C1"}
    )
    assert final["work_source"] == "FINAL_RECOVERY"
    assert final["normalized_priority"] == 2
    assert final["queue_item_ids"] == ["Q2"]


def test_global_work_source_does_not_fallback_while_core_has_active_lease() -> None:
    recovery = pl.DataFrame(
        [
            {
                "recovery_id": "R0",
                "queue_item_id": "Q0",
                "status": "RECOVERY_REQUIRED",
                "lease_expires_at": "2999-01-01T00:00:00+00:00",
            },
            {"recovery_id": "R1", "queue_item_id": "Q1", "status": "RECOVERY_REQUIRED"},
        ]
    )
    queue = pl.DataFrame(
        [
            {"queue_item_id": "Q0", "city_id": "C0", "city": "甲市", "priority": 20},
            {"queue_item_id": "Q1", "city_id": "C1", "city": "乙市", "priority": 10},
        ]
    )

    decision = select_next_work_source(
        recovery,
        queue,
        {"queue_item_ids": ["Q0"]},
        city_limit=2,
        critical_city_ids={"C1"},
        now="2026-08-14T09:00:00+00:00",
    )

    assert decision["work_source"] == "CORE_RECOVERY"
    assert decision["queue_item_ids"] == []
    assert decision["blocked_by_active_lease"] is True
    assert decision["required_by_priority"] == {"0": 1, "1": 1, "2": 0}


def test_recovery_claim_skips_active_running_lease_and_is_deterministic() -> None:
    recovery = pl.DataFrame(
        [
            {
                "recovery_id": "R1",
                "queue_item_id": "Q1",
                "status": "RECOVERY_REQUIRED",
                "lease_expires_at": "2999-01-01T00:00:00+00:00",
            },
            {"recovery_id": "R2", "queue_item_id": "Q2", "status": "RECOVERY_REQUIRED"},
        ]
    )
    queue = pl.DataFrame(
        [
            {"queue_item_id": "Q1", "city_id": "C1", "city": "甲市", "priority": 10},
            {"queue_item_id": "Q2", "city_id": "C2", "city": "乙市", "priority": 10},
        ]
    )

    claims = select_recovery_claim_rows(
        recovery,
        queue,
        {"queue_item_ids": []},
        ["C1", "C2"],
        now="2026-08-14T09:00:00+00:00",
    )

    assert [row["queue_item_id"] for row in claims] == ["Q2"]


def test_recovery_claim_advances_from_p1_to_p2_only_after_higher_lanes_empty() -> None:
    recovery = pl.DataFrame(
        [
            {"recovery_id": "R1", "queue_item_id": "Q1", "status": "RECOVERY_COMPLETED"},
            {"recovery_id": "R2", "queue_item_id": "Q2", "status": "RECOVERY_REQUIRED"},
            {"recovery_id": "R3", "queue_item_id": "Q3", "status": "RECOVERY_REQUIRED"},
        ]
    )
    queue = pl.DataFrame(
        [
            {"queue_item_id": "Q1", "city_id": "C1", "city": "甲市", "priority": 10},
            {"queue_item_id": "Q2", "city_id": "C2", "city": "乙市", "priority": 10},
            {"queue_item_id": "Q3", "city_id": "C3", "city": "丙市", "priority": 10},
        ]
    )
    scope = {"queue_item_ids": ["Q1"]}

    p1_claim = select_recovery_claim_rows(
        recovery,
        queue,
        scope,
        ["C1", "C2", "C3"],
        critical_city_ids={"C2"},
    )
    assert [row["queue_item_id"] for row in p1_claim] == ["Q2"]
    assert p1_claim[0]["normalized_priority"] == 1

    recovery = recovery.with_columns(
        pl.when(pl.col("queue_item_id") == "Q2")
        .then(pl.lit("RECOVERY_COMPLETED"))
        .otherwise(pl.col("status"))
        .alias("status")
    )
    p2_claim = select_recovery_claim_rows(
        recovery,
        queue,
        scope,
        ["C1", "C2", "C3"],
        critical_city_ids={"C2"},
    )
    assert [row["queue_item_id"] for row in p2_claim] == ["Q3"]
    assert p2_claim[0]["normalized_priority"] == 2


def test_analysis_ready_export_is_independent_of_final_recovery() -> None:
    gates = {
        "core_discovery": True,
        "official_evidence": True,
        "action_extraction": True,
        "api_pass1": True,
        "api_pass2": True,
        "date_verification": True,
        "critical_dedup": True,
        "critical_gaps": True,
        "formal_promotion": True,
        "dashboard_action_export": True,
    }

    decision = analysis_ready_decision(
        gates,
        pass1_success=5,
        pass1_total=5,
        pass2_success=5,
        final_recovery_remaining=850,
    )

    assert decision["analysis_ready"] is True
    assert decision["export_required"] is True
    assert decision["final_ready"] is False


def test_main_classification_cannot_bypass_recovery_gate() -> None:
    assert api_classification_allowed({"recovery_gate": "BACKOFF_SINGLE_PROBE"}, recovery_queue_rows=10) is False
    assert api_classification_allowed({"recovery_gate": "MICRO_5"}, recovery_queue_rows=10) is False
    assert api_classification_allowed({"recovery_gate": "BACKLOG_CONSUMPTION"}, recovery_queue_rows=10) is True
    assert api_classification_allowed({}, recovery_queue_rows=0) is True
