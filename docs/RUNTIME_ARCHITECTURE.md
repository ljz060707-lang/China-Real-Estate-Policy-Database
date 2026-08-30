# CRPD runtime architecture

CRPD keeps one owner for each production concern:

`JobManager → CrawlService → CrawlPipeline → HostAwareFetchPool → parse/version staging → PolicyWriteLock → atomic merge`

AI enrichment consumes persisted versions after fetch. Provider failure leaves AI work waiting; it does not own or stop the fetch lane.

## Runtime inventory

| Module | Responsibility / owner | Decision | Hot path | Reason |
|---|---|---|---:|---|
| `jobs.manager` | authoritative job state, events, process start, single write lock | KEEP | yes | one lifecycle state owner and one writer invariant |
| `jobs.worker` | one job process and final atomic commit | KEEP | yes | owns parse-to-commit process boundary |
| `crawl.service` | translate a validated request into the canonical crawl workflow | KEEP | yes | small public orchestration seam |
| `crawl.pipeline` | plan, consume ordered fetch results, parse, version and checkpoint | KEEP | yes | deterministic commit owner; network reads no longer globally serial |
| `crawl.fetcher` | HTTP retry/robots/rate limit and bounded host-aware reads | MERGED | yes | concurrency reuses the existing client and retry owner |
| `full_sync` | bounded source/city adapter into the canonical crawl pipeline | KEEP / SHRINK LATER | yes | existing configuration now reaches the canonical fetch lane |
| `episode_930_production` | EP930 scope/rules and production adapter | MERGE AFTER ACTIVE RUN | no | domain rules are unique; generic runtime ownership should move to jobs/crawl |
| `episode_930_autorun` / `episode_930_v4` | compatibility controller for the active EP930 run | MERGE AFTER TURNOVER | no | active process; deletion during production would violate resume safety |
| `scripts/crpd_autonomous_controller.py` | older all-city compatibility controller | MERGE AFTER TURNOVER | no | retains live deployment compatibility but must not become a second state owner |
| tracked `*.bak_*` copies | timestamped source copies | DELETE | no | no runtime, CLI, test, CI or documentation references; Git already stores history |

## State contract

- `state.json` is the authoritative lifecycle state: `queued`, `running`, `waiting`, `completed`, `completed_with_warnings`, `failed`, or `cancelled`.
- `stage` describes current work such as `fetching` or `validating`; it is not a second lifecycle.
- historical stage-shaped statuses are normalized to `running` at the read/update seam; historical files are not rewritten.
- `events.jsonl` is append-only audit history.
- monitor/performance JSON is a derived snapshot and never controls eligibility.

## Concurrency and ownership

- Network reads: bounded global pool, maximum 16; configured by the existing job/full-sync fields.
- Per host: bounded independently, default 1 and maximum 2. A slow host cannot create global backoff.
- Queue: at most one future per worker; no unbounded submission.
- Parsing and item status application: deterministic caller order.
- Item checkpoint update: one in-memory update batch and one atomic Parquet replacement per run.
- Database: exactly one `PolicyWriteLock` owner; no concurrent DuckDB writers.
- Retry: `RespectfulFetcher` owns HTTP retry. The scheduler owns later retry/gap recovery; wrappers do not nest HTTP retries.

## Production operations

Production start/monitor commands remain the existing documented commands. Deploy changed runtime code only after the active worker reaches a clean turnover. Health inspection must verify the PID and `policydb-write.lock`, not only a JSON status value.
