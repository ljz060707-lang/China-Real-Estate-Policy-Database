# CRPD continuous synchronization

`policydb.autopilot_cli full-sync` is the bounded orchestration layer for
source completion, historical backfill, incremental refresh, gap detection,
and health reporting. It reuses the existing source registry, deterministic
source gates, `CrawlPipeline`, Parquet checkpoints, and secret store.

The workflow has two independent dimensions:

* a source is eligible for backfill when its own strict evidence is complete;
* the global database status reports unresolved slots, historical gaps,
  freshness, and quality without blocking an already-ready source.

## Safe commands

Plan is read-only with respect to network/API providers:

```powershell
python -m policydb.autopilot_cli full-sync plan --scope all --discover-missing --verify-candidates --enable-ready --backfill --incremental --repair-gaps --until-current --dry-run
```

A bounded source canary should explicitly set its source and budgets:

```powershell
python -m policydb.autopilot_cli full-sync run --scope source --source-id <source_id> --max-slots 1 --max-sources 1 --max-documents 50 --max-ai-calls 1 --max-search-calls 5 --max-http-calls 100 --concurrency 1 --backfill --incremental --repair-gaps --resume --apply
```

The full `--all-remaining` run additionally requires
`--confirm-full-sync` and a passed test-evidence file. It is not started by
the development workflow.

Daily incremental refresh is separate from historical backfill:

```powershell
python -m policydb.autopilot_cli full-sync refresh --scope all --incremental --repair-gaps --lookback-days 30 --max-sources 20 --concurrency 4 --resume --apply
```

Use `full-sync status` to read the most recent `current_status.json` and
`full-sync repair --repair-gaps` to run only deterministic gap inspection.

## Evidence and safety

Each run writes stage queues, `database_sync_status.json`, source health,
budget usage, job claims, checkpoints, append-only state transitions, gap
summaries, and resume instructions under `outputs/full_sync/<run_id>`.

The LLM/search layer may propose queries or candidates only. The existing
two-probe, official-domain, city, role, parser, pagination, and reusable-entry
gates remain the only path to `is_verified`, strict enablement, or a crawl-ready
source. A missing pagination-complete checkpoint leaves a backfill partial;
an empty page does not prove historical completeness.

Watermarks advance only after the existing crawler has committed its evidence.
Document versions use a canonical identity plus content/metadata hashes, so
re-running a source updates `last_seen_at` without duplicating a version and
content changes create a new version while preserving the old one.
