# Status model

## Source/crawl statuses

`SUCCESS` means the bounded source run completed without a recorded gap. `COMPLETE_WITH_GAPS` means useful run completion evidence exists but document failures or known gaps remain. `PARTIAL_BUT_USABLE` means text was persisted before a time/page/document/STOP boundary. `PARTIAL_EMPTY` means the boundary was reached without text. `PAUSED_BUDGET` is a budget boundary. `RETRY_WAIT` is backoff, `HUMAN_REVIEW` is an ambiguity or terminal repeated failure, `FAILED_TERMINAL` is an unrecoverable run error, and `SKIPPED_DEPENDENCY` means a prerequisite source or registry entry was unavailable.

## Slot states

Slot truth remains owned by the source registry and deterministic source-slot module. AI/search suggestions cannot set `verified`, `enabled`, or strict gate fields. The Dashboard derives display states from persisted slot counters and sync rows; it does not mutate them.

## Run states

Every fast run writes `current_status.json`, an append-only transition JSONL, a checkpoint JSONL, a manifest and per-source task directories. `last_heartbeat_at` and `last_progress_at` are separate. A STOP request pauses at a pipeline checkpoint and leaves all history intact.

## Gold

Gold policy intensity is `DISABLED_PLACEHOLDER`; no intensity result is inferred from missing data and no policy-intensity API call occurs.
