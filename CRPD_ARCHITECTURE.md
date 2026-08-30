# CRPD ARCHITECTURE (baseline, READ-ONLY takeover 2026-08-20T01:05:26.864566)

## Current production architecture (verified)
1. Automation state machine (automation/AUTOMATION_CONFIG.json):
   WAIT_CURRENT_RUN → CRAWL → NORMALIZE → DEDUP → AI_CLASSIFY → AI_VERIFY → ARCHIVE →
   COVERAGE_AUDIT → RECOVER_MISSING → CRAWL_AGAIN → PDF_STAGE → PDF_VERIFY → FINAL_AUDIT → COMPLETE.
2. Controller: scripts/crpd_autonomous_controller.py (82 KB) + PS1 workers/supervisors.
3. Storage: E:\Data Set\CRPD (database/policydb.duckdb, raw/<sha>/html|pdf|attachments,
   curated/<table>.parquet, outputs/, archive/, logs/, automation/ state JSON).
4. DB: DuckDB single file; tables for source governance (525 slots), crawl, documents/versions,
   records/actions, dedup, coverage gaps, manual review; 151 views.
5. EP930: src/policydb/episode_930*.py + scripts + frozen outputs under
   E:\Data Set\CRPD\outputs\special_projects\2016_930\.

## Target consolidation seams (phase 2, minimal)
- One HTTP/fetch seam (crawl/fetch), one retry policy layer, one date parser,
  one dedup engine, one DB writer (single-writer), one classifier gateway (rule+AI, versioned),
  one coverage/gap engine, one release exporter.
- Episode configs replace per-episode pipelines (EP930 → episode config + adapter).
- Paths configurable via CRPD_HOME/CRPD_DB/CRPD_DATA_ROOT/CRPD_RAW_ROOT/CRPD_EVIDENCE_ROOT/
  CRPD_OUTPUT_ROOT/CRPD_CACHE_ROOT/CRPD_LOG_ROOT.
- Official recovery (BASE CORPUS + OFFICIAL WEB RECOVERY) becomes a first-class module.
