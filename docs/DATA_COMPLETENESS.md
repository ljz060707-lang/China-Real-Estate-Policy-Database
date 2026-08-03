# Data completeness and quality

The Dashboard keeps completeness dimensions separate:

- city coverage: cities with at least one policy text / registered cities;
- role coverage: resolved, verified and enabled slots / 105×5 required slots;
- temporal coverage: city-years with text inside the configured date window;
- document fields: title, date, text, URL, hash and source fields;
- historical backfill: sources with a persisted backfill state;
- attachment completion: fetched versus `PENDING_ATTACHMENT`;
- source verification: deterministic source evidence and strict state;
- freshness: most recent successful crawl or source update.

`PARTIAL_BUT_USABLE` means useful text exists but pagination/history/attachments are not proven complete. `SKIPPED_DEPENDENCY` is an operational dependency state, not a data failure. Missing and unknown values remain unavailable/null rather than being converted to zero.

Any optional overall score is a configurable presentation heuristic only. It is not an ingestion gate and does not mean legal or statistical completeness.
