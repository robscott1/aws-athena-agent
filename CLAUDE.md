# Athena Query Agent

An investigative triage tool for SaaS telemetry data via AWS Athena.

## Investigation Approach

When given a triage question:

1. **Start broad** - Run an exploratory query to understand the shape of the problem (e.g., counts by day, error rates, top accounts).
2. **Analyze results** - Explain what you see, what's normal vs. anomalous, and what hypotheses it suggests.
3. **Drill down** - Run follow-up queries to confirm or eliminate hypotheses. Cross-reference across tables.
4. **Keep going** - Don't stop after one query. Iterate until you've identified a root cause or exhausted the available data.
5. **Summarize findings** - Present a clear conclusion with supporting evidence from the queries you ran.

Think out loud at each step. Explain what you're looking for, why, and what the results tell you.

## Query Execution

1. Edit the INPUT section of `query.py` with the query and any parameters. (Edits to `query.py` are auto-approved — no need to confirm.)
2. **Show the query to the user and wait for confirmation before executing.**
3. Run: `poetry run python query.py`
4. Analyze results, then repeat as needed.

## Database & Tables

**Database:** `telemetry`

| Table | Description |
|-------|-------------|
| `accounts` | Account info: plan, status, limits. Partitioned by `dt`. |
| `users` | Users per account: role, email, status. Partitioned by `dt`. |
| `sessions` | Session data: IP, country, user agent, duration. Partitioned by `dt`. |
| `api_requests` | API call logs: endpoint, method, status code, response time. Partitioned by `dt`. |
| `error_logs` | Error details: type, message, linked to request. Partitioned by `dt`. |

Full column-level schema is in `schema.txt`. This is a living document — when you encounter a table not yet documented, run `DESCRIBE table_name` and append the schema to `schema.txt`. If a query fails with a column-not-found or schema-related error, re-run `DESCRIBE` on the table, update `schema.txt`, and retry the query.

## Configuration

Defaults in `query.py` INPUT section:
- `DATABASE`: `telemetry`
- `S3_OUTPUT`: `s3://your-athena-results-bucket/query-results/`
- `AWS_REGION`: `us-east-1`
- `AWS_PROFILE`: `None` (uses default credentials)

## Query Hygiene

**Do NOT commit:**
- Query output files
- Hardcoded customer IDs, device IDs, user IDs in queries or docs
- Any customer-identifiable information

**Safe to commit:**
- Query templates with `$parameter` placeholders
- Schema definitions (column names/types only)
- Methodology docs without customer data

### Rules

- **Partition by dt**: ALWAYS filter by `dt` for efficient queries.
- **Read-only**: The script blocks write operations. This is intentional.
- **Test with LIMIT**: When trying a new query pattern, use `LIMIT 50` first.
- **Date format**: The `dt` partition uses `YYYY-MM-DD` strings (e.g., `'2026-01-15'`).

## Saved Queries

Before you begin a query, check to see if any of the saved queries match the task you are working on.

After completing an investigation, if any query you wrote is generic and reusable (not tied to a specific account/user/incident), offer to save it to `queries/`.

Save with:
- A comment header describing what it does
- Parameter placeholders using `$param_name` syntax
- Required params listed in the comment

Example:
```sql
-- Compare daily request volume for an account over a date range
-- Params: $account_id, $start_dt, $end_dt
SELECT dt, COUNT(*) as request_count
FROM telemetry.api_requests
WHERE account_id = '$account_id'
  AND dt BETWEEN '$start_dt' AND '$end_dt'
GROUP BY dt
ORDER BY dt
```