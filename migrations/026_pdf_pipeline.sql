-- PDF Parquet schemas are the canonical storage for the auditable PDF layer.
-- The table records the contract without copying raw bytes into DuckDB.
CREATE TABLE IF NOT EXISTS pdf_pipeline_schema_meta (
    schema_name VARCHAR PRIMARY KEY,
    schema_version VARCHAR NOT NULL,
    raw_root VARCHAR NOT NULL,
    ocr_enabled BOOLEAN NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL
);

INSERT INTO pdf_pipeline_schema_meta
    (schema_name, schema_version, raw_root, ocr_enabled, created_at)
VALUES
    ('pdf_assets/document_attachments/pdf_text_versions', '1.0', 'raw/pdf', FALSE, current_timestamp)
ON CONFLICT (schema_name) DO NOTHING;
