-- Athena DDL for sample SaaS telemetry tables
-- Replace ${DATABASE} and ${S3_BUCKET} before running.
--
-- Usage:
--   sed 's/${DATABASE}/my_db/g; s|${S3_BUCKET}|s3://my-bucket/data|g' setup_tables.sql

CREATE EXTERNAL TABLE IF NOT EXISTS ${DATABASE}.accounts (
    account_id   STRING,
    name         STRING,
    plan         STRING,
    monthly_request_limit BIGINT,
    status       STRING,
    created_at   STRING
)
PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION '${S3_BUCKET}/accounts/';

CREATE EXTERNAL TABLE IF NOT EXISTS ${DATABASE}.users (
    user_id      STRING,
    account_id   STRING,
    email        STRING,
    role         STRING,
    status       STRING,
    created_at   STRING
)
PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION '${S3_BUCKET}/users/';

CREATE EXTERNAL TABLE IF NOT EXISTS ${DATABASE}.sessions (
    session_id        STRING,
    user_id           STRING,
    account_id        STRING,
    ip_address        STRING,
    country           STRING,
    user_agent        STRING,
    started_at        STRING,
    duration_seconds  BIGINT
)
PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION '${S3_BUCKET}/sessions/';

CREATE EXTERNAL TABLE IF NOT EXISTS ${DATABASE}.api_requests (
    request_id       STRING,
    account_id       STRING,
    user_id          STRING,
    method           STRING,
    endpoint         STRING,
    status_code      BIGINT,
    response_time_ms BIGINT,
    ip_address       STRING,
    user_agent       STRING,
    timestamp        STRING
)
PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION '${S3_BUCKET}/api_requests/';

CREATE EXTERNAL TABLE IF NOT EXISTS ${DATABASE}.error_logs (
    error_id     STRING,
    request_id   STRING,
    account_id   STRING,
    user_id      STRING,
    error_type   STRING,
    message      STRING,
    endpoint     STRING,
    ip_address   STRING,
    timestamp    STRING
)
PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION '${S3_BUCKET}/error_logs/';

-- After uploading data, repair partitions:
-- MSCK REPAIR TABLE ${DATABASE}.accounts;
-- MSCK REPAIR TABLE ${DATABASE}.users;
-- MSCK REPAIR TABLE ${DATABASE}.sessions;
-- MSCK REPAIR TABLE ${DATABASE}.api_requests;
-- MSCK REPAIR TABLE ${DATABASE}.error_logs;
