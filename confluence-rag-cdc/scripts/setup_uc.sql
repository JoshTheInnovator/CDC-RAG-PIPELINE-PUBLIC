-- Run once (adjust names to match databricks.yml variables)
CREATE CATALOG IF NOT EXISTS main;
CREATE SCHEMA  IF NOT EXISTS main.confluence_rag;
CREATE VOLUME  IF NOT EXISTS main.confluence_rag.checkpoints;

-- The S3 raw bucket must be registered as a UC external location, e.g.:
-- CREATE EXTERNAL LOCATION confluence_raw URL 's3://<RawBucketName>/raw/' WITH (STORAGE CREDENTIAL <cred>);
-- GRANT READ FILES ON EXTERNAL LOCATION confluence_raw TO `<job-principal>`;
