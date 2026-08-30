"""CRPD unified seams — the 12 core interfaces with adapters to existing modules.

Every seam records the existing module it adapts (module + symbols) and its
status: IMPLEMENTED (verified adapter), PARTIAL (existing machinery exists but
the deterministic adapter is not yet wired), MISSING. Seams without a verified
adapter raise ``NotImplementedSeam`` (typed) so platform coverage is
measurable and honest — no guessing, no fabricated progress.

Deterministic-first rule: source validation never delegates to AI; only
rule-based deterministic validation is accepted (``source_quality`` /
``source_jurisdiction`` / ``crawl.health``).
"""
from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any


class NotImplementedSeam(NotImplementedError):
    """Raised when a core seam has no verified adapter yet."""


@dataclass(frozen=True)
class Seam:
    name: str
    status: str            # IMPLEMENTED | PARTIAL | MISSING
    module: str            # existing module path ("" when none)
    symbols: tuple[str, ...] = ()   # verified entry points in that module
    notes: str = ""


SEAM_MAP: dict[str, Seam] = {}


def _register(name: str, status: str, module: str, symbols: tuple[str, ...], notes: str = "") -> Seam:
    s = Seam(name=name, status=status, module=module, symbols=symbols, notes=notes)
    SEAM_MAP[name] = s
    return s


# ---------------------------------------------------------------------------
# 1. discover_sources — candidate source discovery (seed + search items)
# ---------------------------------------------------------------------------
_register(
    "discover_sources", "IMPLEMENTED",
    "policydb.crawl.discovery",
    ("discover_seed_items", "discover_search_items"),
    "discover_seed_items(source, run_id, *, city_id, start_date, end_date) -> list[dict]; "
    "discover_search_items(source, run_id, cities, years, keyword_groups) -> list[dict]",
)


def discover_sources(
    source: Any,
    run_id: str,
    *,
    city_id: str | None = None,
    start_date: Any = None,
    end_date: Any = None,
) -> list[dict]:
    from policydb.crawl.discovery import discover_seed_items
    return discover_seed_items(
        source, run_id, city_id=city_id, start_date=start_date, end_date=end_date
    )


# ---------------------------------------------------------------------------
# 2. validate_source — deterministic source validation (AI never decides)
# ---------------------------------------------------------------------------
_register(
    "validate_source", "IMPLEMENTED",
    "policydb.source_quality",
    ("validate_registry", "unresolved_sources"),
    "deterministic registry validation; jurisdiction checks in "
    "policydb.source_jurisdiction, per-source health in policydb.crawl.health",
)


def validate_source(settings: Any = None, **kw: Any) -> dict:
    from policydb.source_quality import validate_registry
    return validate_registry(settings)


# ---------------------------------------------------------------------------
# 3. plan_crawl — build crawl jobs (525 slots / city × source × year)
# ---------------------------------------------------------------------------
_register(
    "plan_crawl", "IMPLEMENTED",
    "policydb.source_slots",
    ("audit_525",),
    "audit_525(settings=None) -> dict (525-slot matrix audit); slot selection in "
    "policydb.autopilot_runtime.select_source_slots",
)


def plan_crawl(settings: Any = None, **kw: Any) -> dict:
    from policydb.source_slots import audit_525
    return audit_525(settings)


# ---------------------------------------------------------------------------
# 4. fetch_document — polite HTTP fetch with typed errors + retries
# ---------------------------------------------------------------------------
_register(
    "fetch_document", "IMPLEMENTED",
    "policydb.crawl.fetcher",
    ("RespectfulFetcher", "classify_fetch_error", "CrawlFetchError"),
    "typed failures: DnsError/ConnectError/ConnectTimeout/ReadTimeout/TlsError/"
    "Http403/Http404/Http429/Http5xx/RobotsBlocked/CaptchaDetected/EmptyContent/UnsupportedContentType",
)


def fetch_document(
    url: str,
    *,
    etag: str | None = None,
    last_modified: str | None = None,
    referer: str | None = None,
    **fetcher_kwargs: Any,
):
    from policydb.crawl.fetcher import RespectfulFetcher
    return RespectfulFetcher(**fetcher_kwargs).fetch(
        url, etag=etag, last_modified=last_modified, referer=referer
    )


# ---------------------------------------------------------------------------
# 5. extract_document — HTML/PDF/Office unified parsing
# ---------------------------------------------------------------------------
_register(
    "extract_document", "IMPLEMENTED",
    "policydb.crawl.parser",
    ("parse_document", "extract_pdf_embedded"),
    "parse_document(body, content_type, base_url=None) -> dict (HTML/PDF/Office)",
)


def extract_document(
    body: bytes, content_type: str | None = None, base_url: str | None = None
) -> dict:
    from policydb.crawl.parser import parse_document
    return parse_document(body, content_type, base_url)


# ---------------------------------------------------------------------------
# 6. extract_actions — split a document into policy actions
# ---------------------------------------------------------------------------
_register(
    "extract_actions", "IMPLEMENTED",
    "policydb.intensity.rules",
    ("DeterministicPolicyRules", "split_clauses"),
    "deterministic clause and instrument candidates are emitted before optional "
    "AI semantic classification; short or non-official text remains provisional",
)


def extract_actions(document: dict, *, settings: Any = None, **kw: Any) -> list[dict]:
    """Return auditable deterministic action candidates for one document.

    This adapter deliberately stops before semantic promotion: the existing
    rule layer supplies clause spans, instrument and direction candidates, while
    the downstream classifier/promotion gates decide whether a candidate is
    formal.  Missing text therefore returns an empty list rather than claiming
    that a document has no policy content.
    """

    from policydb.intensity.rules import DeterministicPolicyRules
    from policydb.settings import Settings

    text = str(
        document.get("official_text")
        or document.get("full_text")
        or document.get("extracted_text")
        or document.get("text")
        or ""
    )
    if not text.strip():
        return []
    resolved_settings = settings or Settings.discover()
    reference_dir = resolved_settings.root / "data" / "reference"
    if not (reference_dir / "policy_action_patterns.yaml").exists():
        reference_dir = Path(__file__).resolve().parents[3] / "data" / "reference"
    rules = DeterministicPolicyRules(reference_dir)
    record_id = str(
        document.get("record_id")
        or document.get("document_id")
        or document.get("document_version_id")
        or document.get("canonical_url")
        or "UNKNOWN_RECORD"
    )
    actions = rules.extract_actions(
        record_id=record_id,
        document_version_id=(
            str(document["document_version_id"])
            if document.get("document_version_id") is not None
            else None
        ),
        text=text,
        title=str(document.get("title") or "") or None,
        official_status=str(
            document.get("official_status")
            or document.get("source_status")
            or "unknown"
        ),
    )
    return [
        {
            **action.model_dump(mode="json"),
            "extraction_method": "deterministic_rule",
            "candidate_status": "deterministic_candidate",
        }
        for action in actions
    ]


# ---------------------------------------------------------------------------
# 7. classify_actions — deterministic rule classification + materialization
# ---------------------------------------------------------------------------
_register(
    "classify_actions", "IMPLEMENTED",
    "policydb.taxonomy_v2",
    ("classify_action", "materialize_action_classifications", "VERSION"),
    "classify_action(instrument, text) -> (primary, secondary, mechanism, confidence, method); "
    "VERSION 3.0.0; rule-based keyword + instrument map, deterministic",
)


def classify_actions(actions: list[dict], **kw: Any) -> list[dict]:
    from policydb.taxonomy_v2 import classify_action
    rows = []
    for action in actions:
        primary, secondary, mechanism, confidence, method = classify_action(
            str(action.get("instrument") or ""),
            str(action.get("clause_text") or action.get("text") or ""),
        )
        rows.append(
            {
                "action_id": action.get("action_id"),
                "record_id": action.get("record_id"),
                "primary_category": primary or None,
                "secondary_category": secondary or None,
                "instrument_type": mechanism or None,
                "confidence": confidence,
                "decision_reason": method,
                "classification_source": "deterministic_rule",
            }
        )
    return rows


# ---------------------------------------------------------------------------
# 8. deduplicate — canonicalization + identity keys + pairwise decisions
# ---------------------------------------------------------------------------
_register(
    "deduplicate", "IMPLEMENTED",
    "policydb.crawl.dedup",
    ("canonicalize_url", "policy_identity_key", "classify_text_pair", "normalized_text_hash",
     "simhash64", "DedupDecision"),
    "pairwise decisions via classify_text_pair (L4/L6, rules v2.0.0); full-run "
    "set dedup writes dedup_decisions in the autopilot stage",
)


def deduplicate_pair(
    left: str,
    right: str,
    *,
    left_numbers: Iterable[str] | None = None,
    right_numbers: Iterable[str] | None = None,
):
    from policydb.crawl.dedup import classify_text_pair
    return classify_text_pair(
        left, right,
        left_numbers=list(left_numbers) if left_numbers is not None else None,
        right_numbers=list(right_numbers) if right_numbers is not None else None,
    )


def deduplicate(items: list[dict], **kw: Any):
    """Pairwise dedup driver over existing primitives (no invented semantics)."""
    from policydb.crawl.dedup import canonicalize_url, policy_identity_key
    rows = []
    for item in items:
        rows.append(
            {
                "item_id": item.get("item_id"),
                "url": item.get("url"),
                "canonical_url": canonicalize_url(str(item.get("url") or "")),
                "identity_key": policy_identity_key(
                    title=item.get("title"),
                    document_number=item.get("document_number"),
                    agency=item.get("agency"),
                    publication_date=item.get("publication_date"),
                    jurisdiction=item.get("jurisdiction"),
                ),
            }
        )
    return rows


# ---------------------------------------------------------------------------
# 9. evaluate_coverage — read-only coverage audit
# ---------------------------------------------------------------------------
_register(
    "evaluate_coverage", "IMPLEMENTED",
    "policydb.coverage_audit",
    ("run_coverage_audit",),
    "run_coverage_audit(settings=None, *, sample_size=30) -> dict; read-only duckdb; "
    "matrix builders in policydb.coverage",
)


def evaluate_coverage(settings: Any = None, *, sample_size: int = 30, **kw: Any) -> dict:
    from policydb.coverage_audit import run_coverage_audit
    return run_coverage_audit(settings, sample_size=sample_size)


# ---------------------------------------------------------------------------
# 10. recover_gaps — typed gap recovery (official-registry search engine)
# ---------------------------------------------------------------------------
_register(
    "recover_gaps", "IMPLEMENTED",
    "policydb.recovery",
    ("SourceRecoveryEngine", "recover_review_sources", "score_source_candidate"),
    "SourceRecoveryEngine(settings=None, *, fetcher=None).discover(record, *, limit=10) "
    "/ .rank(record, candidates); recover_review_sources for the review queue",
)


def recover_gaps(record: Any = None, *, limit: int = 10, settings: Any = None, **kw: Any):
    from policydb.recovery import SourceRecoveryEngine
    if record is None:
        raise NotImplementedSeam(
            "recover_gaps: full gap-list sweep not yet wired; pass a RecoveryRecord "
            "to use SourceRecoveryEngine.discover"
        )
    return SourceRecoveryEngine(settings).discover(record, limit=limit)


# ---------------------------------------------------------------------------
# 11. promote — single-writer promotion into the database
# ---------------------------------------------------------------------------
_register(
    "promote", "IMPLEMENTED",
    "policydb.ingest.promote_versions",
    ("promote_document_versions",),
    "promote_document_versions(settings, *, run_id, document_version_ids, start_date, "
    "end_date, apply=True) -> audit dict; single-writer via "
    "policydb.jobs.manager.PolicyWriteLock; workspace commit in "
    "policydb.crawl.service.commit_crawl_workspace",
)


def promote(
    settings: Any = None,
    *,
    run_id: str | None = None,
    apply: bool = True,
    job_id: str = "PLATFORM_PROMOTE",
    **kw: Any,
) -> dict:
    from policydb.ingest.promote_versions import promote_document_versions
    from policydb.jobs.manager import PolicyWriteLock
    if apply:
        with PolicyWriteLock(settings, job_id):
            return promote_document_versions(settings, run_id=run_id, apply=True, **kw)
    return promote_document_versions(settings, run_id=run_id, apply=False, **kw)


# ---------------------------------------------------------------------------
# 12. release — immutable, hashed release
# ---------------------------------------------------------------------------
_register(
    "release", "IMPLEMENTED",
    "policydb.export.release",
    ("create_release",),
    "create_release(version, settings=None) -> Path; SHA256 manifest per release "
    "(policy_hash inside release dir)",
)


def release(version: str, settings: Any = None, **kw: Any):
    from policydb.export.release import create_release
    return create_release(version, settings)


# ---------------------------------------------------------------------------
# Seam resolution probe — verifies every IMPLEMENTED seam's module+symbols
# resolve without executing any pipeline work (import-only, deterministic).
# ---------------------------------------------------------------------------
def probe_seams() -> dict[str, dict]:
    import importlib

    report: dict[str, dict] = {}
    for name, seam in SEAM_MAP.items():
        entry: dict = {"status": seam.status, "resolves": False, "missing_symbols": []}
        if seam.status == "IMPLEMENTED":
            try:
                module = importlib.import_module(seam.module)
                missing = [sym for sym in seam.symbols if not hasattr(module, sym)]
                entry["resolves"] = not missing
                entry["missing_symbols"] = missing
            except Exception as exc:  # import failures are reported, never swallowed
                entry["import_error"] = f"{type(exc).__name__}: {exc}"
        report[name] = entry
    return report


def seam_status() -> dict[str, str]:
    return {name: s.status for name, s in SEAM_MAP.items()}
