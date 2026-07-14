CREATE TABLE IF NOT EXISTS schema_migrations (
    migration_id VARCHAR PRIMARY KEY,
    description VARCHAR NOT NULL,
    applied_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp
);

INSERT OR IGNORE INTO schema_migrations (migration_id, description)
VALUES (
    '001_105_city_expansion',
    'Add versioned 105-city scope, policy-source, applicable-city, crawl and LLM relations'
);
