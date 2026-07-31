CREATE TABLE IF NOT EXISTS exhaustive_schema_meta (
    schema_name VARCHAR PRIMARY KEY,
    schema_version VARCHAR NOT NULL,
    description VARCHAR NOT NULL
);

INSERT OR REPLACE INTO exhaustive_schema_meta VALUES (
    'auditable_exhaustive_105',
    '1.0.0',
    'Parquet-backed source slots, candidates, shards, evidence and city-year progress'
);
