# CRPD CRAWL ARCHITECTURE (verified against source, 2026-08-19)

## Pipeline shape (one run)

```
registry (YAML, repo data/reference/source_registry.yaml; materialized curated/source_registry.parquet)
  → crawl/discovery (ListPageDiscovery / OfficialRegistryDiscovery / SiteSearch /
      WebSearch / MissingSourceRecoveryDiscovery; discover_seed_items /
      discover_search_items)
  → CrawlPipeline.plan(run_type, dates, cities, sources, limits, resume=True)
      (crawl/pipeline.py:56)   → crawl_items parquet (curated)
  → CrawlPipeline.run(run_id, max_fetches, max_attachment_attempts, cancel_check)
      → RespectfulFetcher.fetch (crawl/fetcher.py:94) — typed failures:
        DnsError/ConnectError/ConnectTimeout/ReadTimeout/TlsError/Http403/Http404/
        Http429/Http5xx/RobotsBlocked/CaptchaDetected/EmptyContent/
        UnsupportedContentType; retry with backoff (retry-after honored);
        per-domain rate limit; robots.txt; 50 MB cap
      → parse_document (crawl/parser.py:219) — HTML/PDF/Office; charset
        detection (GBK/GB2312 fallback), wrapper stripping, attachment link
        resolution, PDF tables + embedded files
      → attachments (crawl attachments parquet; parent_item_id)
      → policy_document_versions (content_sha256, normalized_text_hash,
        simhash64, policy_identity_key, parse_status, publication_date)
      → dedup_decisions (crawl/dedup.py: classify_text_pair L4/L6, rules v2.0.0)
      → fetch_errors (typed, retryable flag)
  → CrawlService.execute (crawl/service.py:69) — business facade used by
    worker + CLI (modes: smart/official_update/web_discovery/seed_backtrack/
    historical_105/historical_episode_930/recover_missing/source_health)
  → promote_document_versions (ingest/promote_versions.py:317) — single-writer
    via PolicyWriteLock (jobs/manager.py:58); records parquet
  → build_database_atomic (query/database.py:791) — temp build + validate +
    atomic swap → policydb.duckdb
  → run_coverage_audit (coverage_audit.py:11) — read-only DB audit
  → create_release (export/release.py:47) — immutable SHA256 release
```

## Concurrency & fairness

- Single writer: `PolicyWriteLock` (pid + job_id, stale-lock recovery).
- Rate limiting: per-origin `rate_limit` (default 0.5 s), robots.txt respected.
- Budget ledger / attempt recorder in full_sync (BudgetLedger,
  HttpAttemptRecorder) for bounded runs; `global_safety_limit` in plan.
- `crawl/fairness` covered by tests (test_crawl_fairness.py).

## Source governance (deterministic-first)

- 525-slot matrix: `source_slots.audit_525` (105 cities × 5 roles);
  `source_requirement_slots` / `source_slot_progress` parquet; registry
  validation `source_quality.validate_registry`; jurisdiction mapping
  `source_jurisdiction`; health `crawl/health.evaluate_source`.
- AI never decides official status: officiality comes from the registry
  (official/official_reprint) and deterministic domain checks.

## Storage (E-drive layer)

`E:\Data Set\CRPD\` — database/, curated/ (parquet), raw/, archive/, outputs/,
logs/, automation/, control/, runtime/, cache/, temp/, test_artifacts/,
quarantine/, manifests/, jobs/, backups/, research/. Paths resolve through
`Settings` (settings.py) with env/config/preference precedence; tests isolate
via data_root_path (tests/conftest.py).

## Unified platform mapping (additive layer)

`src/policydb/platform/` — config.py (CRPDConfig), seams.py (12 core
interfaces with verified module/symbol mapping + probe), stage_graph.py
(17 resumable stages + checkpoint keys), episode_adapter.py (EP930 frozen
scope verification + seam mapping). Pilot runner `scripts/pilot_e2e.py`
proves the chain end-to-end in an isolated root (see
CRPD_PILOT_CITY_END_TO_END_REPORT.md).

## Honest gaps

- `extract_actions` seam PARTIAL: production action splitting is AI-only
  (enrich/glm.GLMEnricher); deterministic fallback not yet verified.
- `record_terms` derives only from the Excel-import path; web-only roots get
  deterministic derivation in the pilot DATABASE stage (view-required
  vocabulary).
