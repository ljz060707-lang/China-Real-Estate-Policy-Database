# Full crawl workflow

The continuous workflow is staged and resumable:

1. `ROUND_1_FAST_COVERAGE`: rotate 105 cities and try each role with strict per-source budgets.
2. `ROUND_2_ROLE_COMPLETION`: focus on missing source roles and deterministic candidate verification.
3. `ROUND_3_YEAR_COMPLETION`: target missing city-years within the configured window.
4. `ROUND_4_DEEP_BACKFILL`: use more list pages and historical checkpoints for selected sources.
5. `ROUND_5_ATTACHMENTS`: process the queue registered during HTML-first collection.
6. `ROUND_6_MANUAL_REVIEW`: handle ambiguous institutions, role conflicts, CAPTCHA and non-standard official pages.

The current deliverable formally implements the first round and its checkpoint/status contract. Later rounds reuse the same source state and pipeline; they are not silently started by the Dashboard.

Normal interruptions are `PAUSED_BUDGET`, `RETRY_WAIT`, or `PARTIAL_BUT_USABLE`. Database write errors, checkpoint conflicts and consistency errors stop the worker for repair.
