# CRPD CONTEXT — Unified Domain Language (UDL)

This file defines the canonical vocabulary for the CRPD policy-data platform.
Code, tables, classes, files and logs must use these terms; synonyms are deprecated.

## Source layer
- Source Slot: one required source role for one city (105 cities × 5 roles = 525 slots).
- Candidate Source: any discovered URL/domain not yet validated (never enabled).
- Validated Source: a candidate that passed deterministic validation.
- Official Source: validated source with government-domain characteristics (AI never decides this).
- Official Reprint: an official-domain page that reprints an underlying policy document.

## Crawl layer
- Crawl Job: one scheduled crawl unit (city × source × year window).
- Fetch Attempt: one HTTP attempt (attempt_id, job_id, attempt_number, status, retry_reason).
- Document: logical policy document (URL identity ≠ content identity).
- Document Version: one real content version (content_hash, valid_from, retrieved_at).
- Evidence Asset: raw byte snapshot (HTML/PDF/attachment) preserved as evidence, not cache.

## Policy layer
- Policy Record: structured policy document (title, issuer, doc number, dates).
- Policy Action: one policy tool inside a document (policy_type, direction, parameters, evidence_span).
- Policy Parameter: a measured parameter change of an action.
- Classification: typed, versioned output (rule + AI; never source authority).
- Geographic Scope: CITYWIDE / PROVINCE_WIDE / DISTRICT / TARGET_GROUP / PROJECT_SPECIFIC / UNCLEAR.

## Governance layer
- Coverage: measured multi-dimensional completeness (city × year-month × slot × type × evidence).
- Gap: typed gap (SOURCE_SLOT_GAP, CITY_MONTH_GAP, FETCH_FAILURE_GAP, ATTACHMENT_GAP, DATE_GAP,
  CLASSIFICATION_GAP, OFFICIAL_EVIDENCE_GAP, REVIEW_GAP, EPISODE_GAP) — root vs derived.
- Recovery: deterministic re-search/retry of a gap.
- Review: incremental human-review pool item (review_id, object_type, reason, evidence, decision).
- Episode: configurable research episode (e.g., EP_2016_930_TIGHTENING).
- Episode Membership: city×item×episode relation.
- Treatment: frozen analysis treatment (FIRST_ANY_DEMAND_EASING for the V5 research track).
- Treatment Freeze: immutable, hash-locked treatment scope.
- Release: versioned, timestamped, hash-locked output (CSV/Parquet/Excel + manifest + SHA256).

## Principles
- Deterministic first; AI never source authority; evidence always preserved;
  single DB writer; resumable stages; idempotent jobs; immutable releases;
  Treatment independent of outcomes.
