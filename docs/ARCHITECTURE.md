# CRPD architecture

CRPD is a source-backed, resumable policy-text system for 105 cities and five required source roles per city.

```text
Source discovery / search evidence
        ↓
Deterministic official-domain, city, role, page-type and HTTP gates
        ↓
Bronze: bounded HTML-first raw collection
        ↓
Silver: parsing, normalization, deduplication, quality and gaps
        ↓
Research snapshots (immutable summaries)
        ↓
Gold policy intensity (disabled placeholder)
```

The existing `FullSyncController` and `CrawlPipeline` remain the write path. `FastBulkIngestController` only schedules city/role work and supplies budgets. Curated Parquet is the operational store; run directories, checkpoint JSONL, HTTP audit and source state preserve lineage.

The Dashboard is Streamlit and reads lightweight curated columns and status JSON. Its operation center writes a validated JSON request; a local operations worker calls the same controllers as the CLI.

Gold is intentionally not imported by the fast runner. Its tables and Dashboard placeholder may exist, but no intensity model, prompt or API call is run.
