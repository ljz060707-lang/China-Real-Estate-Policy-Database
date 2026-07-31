CREATE TABLE IF NOT EXISTS source_candidate_audit_schema_meta (
    schema_name VARCHAR PRIMARY KEY,
    schema_version VARCHAR NOT NULL,
    description VARCHAR NOT NULL
);

INSERT OR REPLACE INTO source_candidate_audit_schema_meta VALUES (
    'seed_record_jurisdiction_candidates',
    '1.0.0',
    'Parquet-backed disabled candidates plus record-jurisdiction-URL provenance'
);

INSERT OR IGNORE INTO schema_migrations (migration_id, description)
VALUES (
    '025_seed_source_candidate_audit',
    'Register seed-derived source candidate and evidence audit schema'
);
