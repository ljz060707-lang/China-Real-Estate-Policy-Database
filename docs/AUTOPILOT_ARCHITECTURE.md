# CRPD API-driven Autopilot

The Autopilot is a bounded orchestration layer around the existing CRPD source,
search, probe, archive, deduplication, and crawl modules. It does not create a
second database or a second AI client.

## Decision boundary

The SiliconFlow provider is used for structured planning, query generation,
organization aliases, candidate classification/ranking, pagination hypotheses,
and failure diagnosis. The current provider is not treated as a web-search
tool. URLs and network evidence come from the existing search provider and
`GovernmentDirectClient`. Deterministic code remains the only writer of final
verification and enablement fields.

The strict path is:

`AI plan -> real search evidence -> deterministic prefilter -> ranked top-three -> two direct probes -> parser/pagination evidence -> verify_candidates -> rebuild slots -> enable_source_strict`

## Durable state

Each run is under `D:\Data Set\CRPD\outputs\autopilot\<run_id>` and contains
an atomic `current_status.json`, append-only `state_transitions.jsonl`, source
and full-crawl dry-run plans, provider audit, and the GO/NO-GO gate. A stop
file, `STOP_AUTOPILOT`, is checked before and during bounded work. Resume is
idempotent at the run level; AI request idempotency and crash-safe request
records remain owned by `AIAuditStore`.

## Caps and safety

The checked-in config defaults to 20 slots, 40 AI calls, concurrency 4,
one request per domain, at least two probe rounds, and no automatic transition
to full crawl. The full 105-city crawl requires all 525 slots to pass the GO
gate and an explicit `--auto-full-crawl` opt-in.

The state machine includes source and global states for retry, human review,
network/parser/pagination blocks, crash recovery, source gate, archive,
deduplication, AI eligibility, acceptance, and terminal stop/failure.
