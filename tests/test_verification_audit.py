from __future__ import annotations

import json
import shutil
import zipfile
from pathlib import Path
from types import SimpleNamespace

import polars as pl

from policydb.ai import AIStructuredOutputError
from policydb.ai_audit import AIAuditStore
from policydb.autopilot_checkpoints import GlobalSlotCheckpointStore
from policydb.settings import Settings
from policydb.source_completion_ai_workflow import (
    SourceAIAssessment,
    _ai_mapping_metadata,
    _call_ai,
)
from policydb.source_slots import (
    VERIFICATION_GATE_NAMES,
    build_human_review_rows,
    build_requirement_slots,
    evaluate_candidate_gates,
    list_candidates,
    rebuild_verification_audit,
    upsert_candidates,
    verify_candidates,
)


def _settings(tmp_path: Path) -> Settings:
    root = tmp_path / "repo"
    reference = root / "data" / "reference"
    reference.mkdir(parents=True)
    source_reference = Path(__file__).resolve().parents[1] / "data" / "reference"
    for name in ("cities_105.csv", "city_source_requirements.yaml"):
        shutil.copy2(source_reference / name, reference / name)
    (reference / "source_registry.yaml").write_text(
        "version: 2\nsources: []\n", encoding="utf-8"
    )
    return Settings(root=root)


def _healthy_candidate(candidate_id: str = "CAND_GATE") -> dict:
    return {
        "candidate_id": candidate_id,
        "slot_id": "SLOT_NANJING_HOUSING",
        "city_id": "CITY_320100",
        "source_role": "housing_department",
        "city_name": "Nanjing",
        "candidate_url": "https://nanjing.gov.cn/zwgk/",
        "canonical_url": "https://nanjing.gov.cn/zwgk/",
        "final_url": "https://nanjing.gov.cn/zwgk/",
        "candidate_kind": "official_entry_candidate",
        "page_type": "site_or_column_entry",
        "entry_eligible": True,
        "health_status": "healthy",
        "network_route": "direct_ok",
        "http_status": 200,
        "health_probe_count": 2,
        "health_probe_success_count": 2,
        "parser_status": "list_detected",
        "pagination_strategy": "natural_single_page",
        "publication_date_available": True,
        "article_link_extraction_ready": True,
        "city_match_evidence": None,
        "role_match_evidence": None,
        "is_verified": False,
        "is_enabled": False,
    }


def test_healthy_parser_candidate_has_explicit_gate_reasons_when_unverified() -> None:
    result, gates = evaluate_candidate_gates(
        _healthy_candidate(),
        slot={
            "slot_id": "SLOT_NANJING_HOUSING",
            "city_id": "CITY_320100",
            "city_name": "Nanjing",
            "source_role": "housing_department",
        },
        run_id="RUN_GATE_TEST",
    )

    assert result["verified"] is False
    assert result["status"] == "UNKNOWN"
    assert result["failed_gates"]
    assert result["reason_codes"]
    assert {row["gate_name"] for row in gates} == set(VERIFICATION_GATE_NAMES)
    assert {row["gate_status"] for row in gates if row["gate_name"] in {"direct_healthy", "parser_ready"}} == {"PASS"}
    assert all(
        row["reason_code"] and row["evidence_ids"]
        for row in gates
        if row["gate_status"] != "PASS"
    )


def test_list_entry_can_delegate_article_dates_after_parser_and_pagination() -> None:
    candidate = _healthy_candidate()
    candidate.update(
        {
            "candidate_url": "https://zjw.beijing.gov.cn/",
            "canonical_url": "https://zjw.beijing.gov.cn/",
            "final_url": "https://zjw.beijing.gov.cn/",
            "city_name": "beijing",
            "site_name": "北京市住房和城乡建设委员会",
            "city_id": "CITY_110000",
            "city_match_evidence": "registry city_ids contains CITY_110000",
            "role_match_evidence": "registry role=housing_department",
            "publication_date_available": False,
            "article_link_extraction_ready": True,
            "pagination_strategy": "natural_single_page",
        }
    )
    result, gates = evaluate_candidate_gates(
        candidate,
        slot={
            "slot_id": candidate["slot_id"],
            "city_id": candidate["city_id"],
            "city_name": "beijing",
            "source_role": "housing_department",
        },
        run_id="RUN_DATE_DELEGATION_TEST",
    )

    assert result["verified"] is True
    date_gate = next(item for item in gates if item["gate_name"] == "publication_date_available")
    assert date_gate["gate_status"] == "PASS"
    assert date_gate["reason_code"] == "publication_date_delegated_to_article_parser"


def test_verify_summary_rejects_without_empty_reasons(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    build_requirement_slots(settings)
    upsert_candidates([_healthy_candidate()], settings)
    candidate = list_candidates(settings=settings).filter(
        pl.col("candidate_id") == "CAND_GATE"
    ).row(0, named=True)

    result = verify_candidates(
        slot_id=str(candidate["slot_id"]),
        candidate_ids=["CAND_GATE"],
        run_id="RUN_VERIFY_TEST",
        settings=settings,
    )

    assert result["checked_candidates"] == 1
    assert result["verified_candidates"] == 0
    assert result["rejected"]
    rejected = result["rejected"][0]
    assert rejected["failed_gates"]
    assert rejected["reason_codes"]
    assert result["strict_enabled"] == 0


def test_probe_completed_review_row_is_not_stale_pending_probe() -> None:
    source = _healthy_candidate()
    result, _ = evaluate_candidate_gates(
        source,
        slot=source,
        run_id="RUN_REVIEW_TEST",
    )
    rows = build_human_review_rows(
        [source], [result], run_id="RUN_REVIEW_TEST"
    )

    assert len(rows) == 1
    row = rows[0]
    assert row["probe_status"] == "completed"
    assert row["machine_recommendation"] != "pending_probe"
    assert row["failed_gates"]
    assert row["reason_codes"]
    assert "city" in row["exact_question_for_human"].lower()


def test_ai_parse_failure_is_not_mapped_to_default_zero(monkeypatch) -> None:
    class FailingProvider:
        def structured(self, **_kwargs):
            raise AIStructuredOutputError(
                "invalid JSON",
                parse_status="parse_failed",
                raw_response_hash="raw-hash",
            )

    monkeypatch.setattr(
        "policydb.source_completion_ai_workflow.time.sleep", lambda _seconds: None
    )
    value, trace, error = _call_ai(
        FailingProvider(),
        "model",
        "system",
        "user",
        max_attempts=2,
    )
    metadata = _ai_mapping_metadata(value, trace, error)

    assert value is None
    assert error == "parse_failed"
    assert metadata["ai_parse_status"] == "parse_failed"
    assert metadata["ai_raw_response_hash"] == "raw-hash"
    assert metadata["ai_fields_defaulted"] == []
    assert _ai_mapping_metadata(
        None, None, None, fallback=True
    )["ai_parse_status"] == "default_fallback"


def test_ai_audit_persists_explicit_zero_and_usage(tmp_path: Path) -> None:
    class Provider:
        def structured(self, **_kwargs):
            return SourceAIAssessment(confidence=0), SimpleNamespace(
                prompt_tokens=4,
                completion_tokens=2,
                raw_response_hash="raw-hash",
                raw_fields=("confidence",),
            )

    audit = AIAuditStore(tmp_path / "run")
    payload = {
        "request_id": "REQ_AUDIT_TEST",
        "run_id": "RUN_AUDIT_TEST",
        "slot_id": "SLOT_AUDIT",
        "provider": "test",
        "model": "model",
        "prompt_version": "test-v1",
        "prompt_hash": "prompt-hash",
        "request_hash": "request-hash",
        "cache_key": "request-hash",
    }
    value, _trace, error = _call_ai(
        Provider(), "model", "system", "user", audit=audit, audit_payload=payload
    )
    record = audit.records()[0]

    assert value is not None and error is None
    assert record["status"] == "response_completed"
    assert record["total_tokens"] == 6
    assert record["ai_parse_status"] == "parsed"
    assert record["ai_raw_response_hash"] == "raw-hash"
    assert "confidence" not in record["ai_fields_defaulted"]


def test_rebuild_verification_audit_is_read_only_and_idempotent(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    build_requirement_slots(settings)
    source = _healthy_candidate("CAND_HISTORICAL")
    source.update(
        {
            "health_status": "pending",
            "network_route": "unknown",
            "http_status": None,
            "health_probe_count": 0,
            "health_probe_success_count": 0,
            "parser_status": "pending",
            "pagination_strategy": "unknown",
            "publication_date_available": None,
            "article_link_extraction_ready": None,
        }
    )
    upsert_candidates([source], settings)
    slot_id = str(source["slot_id"])
    run_dir = tmp_path / "historical_run"
    run_dir.mkdir()
    pl.DataFrame(
        [
            {
                "candidate_id": source["candidate_id"],
                "proposal_id": "PROP_HISTORICAL",
                "slot_id": slot_id,
                "city_id": source["city_id"],
                "city_name": source["city_name"],
                "source_role": source["source_role"],
                "candidate_url": source["candidate_url"],
                "selection_status": "selected_top3",
            }
        ]
    ).write_parquet(run_dir / "applied_candidates.parquet")
    pl.DataFrame({"legacy": ["keep"]}).write_parquet(
        run_dir / "deterministic_verification.parquet"
    )
    pl.DataFrame({"legacy": ["keep"]}).write_excel(
        run_dir / "HUMAN_REVIEW_QUEUE.xlsx", autofit=True
    )
    (run_dir / "run_summary.json").write_text(
        json.dumps({"legacy": "keep"}) + "\n", encoding="utf-8"
    )
    original_paths = [
        run_dir / "deterministic_verification.parquet",
        run_dir / "HUMAN_REVIEW_QUEUE.xlsx",
        run_dir / "run_summary.json",
        settings.curated / "source_candidates.parquet",
    ]
    original_bytes = {path: path.read_bytes() for path in original_paths}

    first = rebuild_verification_audit(run_dir, settings=settings)
    output_dir = run_dir / "verification_audit_rebuilt"
    first_outputs = {
        path.name: path.read_bytes()
        for path in output_dir.iterdir()
        if path.is_file()
    }
    second = rebuild_verification_audit(run_dir, settings=settings)
    second_outputs = {
        path.name: path.read_bytes()
        for path in output_dir.iterdir()
        if path.is_file()
    }

    assert first["historical_read_only"] is True
    assert first["original_files_untouched"] is True
    assert first["unknown_candidates"] == 1
    assert first["strict_rejections"] == 1
    assert first["human_review_rows"] == 1
    assert second["source_fingerprint"] == first["source_fingerprint"]
    assert first_outputs == second_outputs
    assert all(path.read_bytes() == original_bytes[path] for path in original_paths)
    summary = json.loads(
        (output_dir / "verification_summary.json").read_text(encoding="utf-8")
    )
    assert summary["unknown_candidates"] == 1
    assert summary["reason_code_counts"]
    with zipfile.ZipFile(output_dir / "HUMAN_REVIEW_QUEUE_REFRESHED.xlsx") as workbook:
        workbook_text = "\n".join(
            workbook.read(member).decode("utf-8", errors="ignore")
            for member in workbook.infolist()
            if member.filename.endswith(".xml")
        )
    assert "probe_status" in workbook_text
    assert "pending_probe" not in workbook_text


def test_ordinary_candidate_upsert_cannot_downgrade_verified_enabled(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    build_requirement_slots(settings)
    source = _healthy_candidate("CAND_STICKY")
    source.update({"is_verified": True, "is_enabled": True, "manual_review_status": "approved"})
    upsert_candidates([source], settings)
    source.update({"is_verified": False, "is_enabled": False, "manual_review_status": "pending_probe"})
    upsert_candidates([source], settings)

    row = list_candidates(settings=settings).filter(
        pl.col("candidate_id") == "CAND_STICKY"
    ).row(0, named=True)
    assert row["is_verified"] is True
    assert row["is_enabled"] is True


def test_checkpoint_jsonl_is_append_only(tmp_path: Path) -> None:
    store = GlobalSlotCheckpointStore(tmp_path)
    slot = {
        "slot_id": "SLOT_APPEND",
        "city_id": "CITY_1",
        "city_name": "Test City",
        "source_role": "housing_department",
    }
    assert store.claim(slot, run_id="RUN_APPEND")[0] is True
    first_lines = (tmp_path / "slot_checkpoints.jsonl").read_text(encoding="utf-8").splitlines()
    store.terminal(
        slot,
        status="HUMAN_REVIEW",
        run_id="RUN_APPEND",
        terminal_outcome="evidence_incomplete",
        ai_call_persisted=False,
    )
    second_lines = (tmp_path / "slot_checkpoints.jsonl").read_text(encoding="utf-8").splitlines()

    assert len(second_lines) == len(first_lines) + 1
    assert second_lines[: len(first_lines)] == first_lines
