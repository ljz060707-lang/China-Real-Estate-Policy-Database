# CRPD SOURCE GOVERNANCE (verified 2026-08-20T01:05:26.864566)

- 525 Source Slots = 105 cities × 5 roles (source_requirement_slots: 525 rows).
- Roles observed in DB: government_gazette, canonical_candidate, etc. (slot sample: 上海 government_gazette enabled).
- source_registry: 513 rows (domain, official_status, seed_urls, list_page_urls, parser_adapter, crawl_enabled…).
- source_candidates: 5,054 rows; source_slot_progress: 525 rows.
- Deterministic validation only; AI never decides official status.
- Flow: discovery → candidate registration → probe → official-domain validation →
  source-quality validation → preferred selection → enable (with fallbacks).
- 217/211 lesson: BASE CORPUS + OFFICIAL WEB RECOVERY must be a first-class path
  (OFFICIAL_RECOVERY_REGRESSION_SET = 长治/忻州/临汾/吕梁/武威/陇南).
