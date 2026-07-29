CREATE TABLE IF NOT EXISTS schema_migrations (
    migration_id VARCHAR PRIMARY KEY,
    description VARCHAR NOT NULL,
    applied_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS schema_metadata (
    metadata_key VARCHAR PRIMARY KEY,
    metadata_value VARCHAR NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp
);

INSERT OR REPLACE INTO schema_metadata VALUES ('schema_version', '2', current_timestamp);
INSERT OR IGNORE INTO schema_migrations (migration_id, description)
VALUES ('020_v2_core_schema', 'Register V2 source coverage, dedup and field confidence facts');

