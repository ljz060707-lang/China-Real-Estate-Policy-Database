# CRPD DATA MODEL (verified against production DB 2026-08-19)

Storage layers: raw (immutable, SHA-256) → curated (parquet, atomic writes) →
database (policydb.duckdb, materialized views) → releases (immutable, hashed).

## Core tables (verified columns, production duckdb)

### crawl_items (crawl state machine)
item_id (PK), run_id, source_id, url, canonical_url, status
(pending/fetched/unchanged/failed/blocked), city_id, query_year, keyword_group,
retry_count, first_seen_at/last_seen_at/created_at/updated_at, task_key,
scan_method, requested_url, final_url, etag, last_modified,
last_checked_at, next_check_at, candidate_date (+_source/_confidence),
period_decision, resume_from_item_id, resume_previous_run_id,
resume_skipped_http

### fetch_errors (typed failures — evidence)
error_id, run_id, item_id, source_id, url, error_type, error_message,
retryable, created_at, updated_at, requested_protocol, network_route,
redirect_chain_json

### policy_document_versions (parsed document evidence)
document_version_id (PK), record_id, crawl_item_id, source_id, canonical_url,
final_url, content_sha256, local_path, content_type, http_status, title,
extracted_text, parse_status, is_material_change, first_seen_at,
last_seen_at, created_at, updated_at, normalized_text_hash, simhash64,
policy_identity_key, parser_version, network_route, redirect_chain_json,
protocol, publication_date, publication_date_source

### dedup_decisions (pairwise evidence)
decision_id, run_id, crawl_item_id, document_version_id,
candidate_document_version_id, dedup_level, decision, reason, score,
threshold, rules_version, evidence_json, created_at

### attachments
attachment_id, run_id, parent_item_id, url, local_path, content_sha256, status

### records (promoted policy records)
record_id, title, record_date, publication_date, effective_date,
official_status, official_level, source_quality, primary_source_url,
source_sheet, direction, full_text, summary, legacy_category, status,
record_type, manual_review_status … (promote_versions contract: requires
http 200 + title + body + http url)

### record_terms (deterministic topic terms)
record_id, term_id, taxonomy_name ('topic'), term_name, classification_source
('rule'), confidence, evidence_excerpt, review_status

### policy_actions (action-level layer; curated parquet is canonical)
action_id, record_id, document_version_id, clause_id, clause_text,
evidence_start, evidence_end, instrument, direction, action_status,
text_completeness, formal_eligible, extraction_method, rules_version,
evidence_text, created_at, updated_at

### coverage layer
coverage_gaps: gap_id, city_id, slot_id, source_id, gap_type, start_date,
end_date, expected_count, observed_count, affected_urls, severity, status,
repair_attempts, last_attempt_at, next_retry_at, resolution, created_at,
resolved_at
crawl_source_windows: window_id, run_id, source_id, city_id, period_start,
period_end, scan_method, coverage_status, candidate_count, fetched_count,
policy_count, error_count, page_count, is_complete, completion_evidence,
started_at, finished_at, created_at, updated_at

### source governance layer
source_registry (513 rows; YAML canonical + curated parquet materialization),
source_requirement_slots (525 = 105 cities × 5 roles), source_candidates
(5,054), source_slot_progress (525), city_source_year_progress, crawl_shards

## Key views (40 in pilot DB build, 151 in production)

v_policy_master (records + jurisdiction + topic aggregation),
v_policy_action_center (action rows + classification + geography/issuer
CTEs), v_city_month_policy_panel / _105 (monthly panel; canonical term counts:
限购/限售/商业住房贷款/限贷/公积金/购房补贴/人才住房/城市更新…),
v_city_month_coverage (migration 021; source×city×month grid),
v_policy_quantitative_measures, v_policy_relations, v_policy_features_resolved,
v_data_quality (record/title/full_text/url completeness), v_information_completeness

## Provenance rules

- Every derived value keeps the raw value + evidence excerpt + method version.
- No derived table overwrites master data; statistics come from curated.
- Releases: immutable dir + release_manifest.json (per-file SHA-256) +
  validation_report.json; version + created_at + data_cutoff.
- Incremental updates are idempotent (atomic parquet writes with key columns;
  promote_document_versions is batch-scoped, never overwrites history).
