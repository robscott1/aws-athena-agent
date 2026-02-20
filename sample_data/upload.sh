#!/usr/bin/env bash
# Upload generated parquet data to S3, create Athena database/tables, and repair partitions.
#
# Usage:
#   ./sample_data/upload.sh [s3-bucket-path] [database] [aws-profile]
#
# Defaults:
#   S3_BUCKET: s3://your-athena-results-bucket
#   DATABASE:  telemetry

set -euo pipefail

S3_BUCKET="${1:-s3://your-athena-results-bucket}"
DATABASE="${2:-telemetry}"
AWS_PROFILE="${3:-}"

DATA_DIR="$(dirname "$0")/data"
TABLES=(accounts users sessions api_requests error_logs)

if [ ! -d "$DATA_DIR" ]; then
    echo "ERROR: No data directory found at $DATA_DIR"
    echo "Run 'poetry run python sample_data/generate.py' first."
    exit 1
fi

PROFILE_FLAG=""
if [ -n "$AWS_PROFILE" ]; then
    PROFILE_FLAG="--profile $AWS_PROFILE"
fi

# --- Step 1: Upload parquet files to S3 ---
echo "Uploading data to $S3_BUCKET ..."

for table in "${TABLES[@]}"; do
    echo "  Syncing $table ..."
    aws s3 sync "$DATA_DIR/$table/" "$S3_BUCKET/$table/" $PROFILE_FLAG --quiet
done

# --- Step 2: Create database ---
echo ""
echo "Creating database '$DATABASE' ..."
aws athena start-query-execution \
    --query-string "CREATE DATABASE IF NOT EXISTS $DATABASE" \
    --result-configuration "OutputLocation=$S3_BUCKET/_setup_output/" \
    $PROFILE_FLAG \
    --output text --query 'QueryExecutionId' > /dev/null

# Give it a moment to complete
sleep 2

# --- Step 3: Create tables ---
echo "Creating tables ..."

aws athena start-query-execution \
    --query-string "
CREATE EXTERNAL TABLE IF NOT EXISTS $DATABASE.accounts (
    account_id   STRING,
    name         STRING,
    plan         STRING,
    monthly_request_limit BIGINT,
    status       STRING,
    created_at   STRING
)
PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION '$S3_BUCKET/accounts/'
" \
    --query-execution-context "Database=$DATABASE" \
    --result-configuration "OutputLocation=$S3_BUCKET/_setup_output/" \
    $PROFILE_FLAG \
    --output text --query 'QueryExecutionId' > /dev/null
echo "  accounts"

aws athena start-query-execution \
    --query-string "
CREATE EXTERNAL TABLE IF NOT EXISTS $DATABASE.users (
    user_id      STRING,
    account_id   STRING,
    email        STRING,
    role         STRING,
    status       STRING,
    created_at   STRING
)
PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION '$S3_BUCKET/users/'
" \
    --query-execution-context "Database=$DATABASE" \
    --result-configuration "OutputLocation=$S3_BUCKET/_setup_output/" \
    $PROFILE_FLAG \
    --output text --query 'QueryExecutionId' > /dev/null
echo "  users"

aws athena start-query-execution \
    --query-string "
CREATE EXTERNAL TABLE IF NOT EXISTS $DATABASE.sessions (
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
LOCATION '$S3_BUCKET/sessions/'
" \
    --query-execution-context "Database=$DATABASE" \
    --result-configuration "OutputLocation=$S3_BUCKET/_setup_output/" \
    $PROFILE_FLAG \
    --output text --query 'QueryExecutionId' > /dev/null
echo "  sessions"

aws athena start-query-execution \
    --query-string "
CREATE EXTERNAL TABLE IF NOT EXISTS $DATABASE.api_requests (
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
LOCATION '$S3_BUCKET/api_requests/'
" \
    --query-execution-context "Database=$DATABASE" \
    --result-configuration "OutputLocation=$S3_BUCKET/_setup_output/" \
    $PROFILE_FLAG \
    --output text --query 'QueryExecutionId' > /dev/null
echo "  api_requests"

aws athena start-query-execution \
    --query-string "
CREATE EXTERNAL TABLE IF NOT EXISTS $DATABASE.error_logs (
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
LOCATION '$S3_BUCKET/error_logs/'
" \
    --query-execution-context "Database=$DATABASE" \
    --result-configuration "OutputLocation=$S3_BUCKET/_setup_output/" \
    $PROFILE_FLAG \
    --output text --query 'QueryExecutionId' > /dev/null
echo "  error_logs"

# Give tables a moment to register
sleep 3

# --- Step 4: Repair partitions ---
echo ""
echo "Repairing table partitions ..."

for table in "${TABLES[@]}"; do
    echo "  MSCK REPAIR TABLE $DATABASE.$table"
    aws athena start-query-execution \
        --query-string "MSCK REPAIR TABLE $DATABASE.$table" \
        --query-execution-context "Database=$DATABASE" \
        --result-configuration "OutputLocation=$S3_BUCKET/_setup_output/" \
        $PROFILE_FLAG \
        --output text --query 'QueryExecutionId' > /dev/null
done

echo ""
echo "Done! Tables are ready to query in Athena."
echo "  Database: $DATABASE"
echo "  S3 data:  $S3_BUCKET"
echo "Note: MSCK REPAIR runs asynchronously — allow a few seconds before querying."