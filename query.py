#!/usr/bin/env python3
"""
Athena Query Executor - Run parameterized Athena queries and format results.

Usage:
    1. Set AWS credentials (env vars, ~/.aws/credentials, or IAM role)
    2. Edit the INPUT section below
    3. Run: poetry run python athena/query.py
"""

import json
import time
import sys
from datetime import datetime
from pathlib import Path
from string import Template

import boto3
from boto3 import Session
from botocore.exceptions import ClientError, NoCredentialsError

# =============================================================================
# INPUT - Edit these values
# =============================================================================

# AWS / Athena config
AWS_REGION = "us-east-1"
AWS_PROFILE = None
DATABASE = "telemetry"
S3_OUTPUT = "s3://your-athena-results-bucket/query-results/"
WORKGROUP = "primary"

# Query - either inline SQL or path to a .sql file in queries/
QUERY = """
SELECT
    error_type,
    message,
    COUNT(*) AS occurrences
FROM telemetry.error_logs
WHERE dt = '2026-01-15'
GROUP BY error_type, message
ORDER BY occurrences DESC
LIMIT 5
"""

# Or load from file:
# QUERY = "queries/events_by_type.sql"

# Parameters for query substitution
PARAMS = {

}

# =============================================================================

OUTPUT_DIR = Path(__file__).parent / "output"


def get_aws_session() -> Session:
    """Get boto3 session using configured profile."""
    return boto3.Session(profile_name=AWS_PROFILE, region_name=AWS_REGION)


def load_query(query_input: str) -> str:
    """Load query from file if path provided, otherwise return as-is."""
    if query_input.strip().endswith(".sql"):
        query_path = Path(__file__).parent / query_input.strip()
        if not query_path.exists():
            raise FileNotFoundError(f"Query file not found: {query_path}")
        return query_path.read_text()
    return query_input


def validate_read_only(query: str) -> None:
    """Ensure query is read-only. Raises ValueError if write operations detected."""
    lines = []
    for line in query.split("\n"):
        line = line.split("--")[0].strip()
        if line:
            lines.append(line)
    normalized = " ".join(lines).upper()

    # Dangerous keywords that indicate write operations
    write_keywords = [
        "INSERT ",
        "UPDATE ",
        "DELETE ",
        "DROP ",
        "CREATE ",
        "ALTER ",
        "TRUNCATE ",
        "MERGE ",
        "UNLOAD ",
    ]

    for keyword in write_keywords:
        if keyword in normalized:
            raise ValueError(f"Write operation detected: {keyword.strip()}. Only SELECT queries allowed.")


def substitute_params(query: str, params: dict) -> str:
    """Substitute $param placeholders in query."""
    return Template(query).safe_substitute(params)


def execute_query(athena, database: str, query: str, s3_output: str, workgroup: str) -> str:
    """Start query execution and return execution ID."""
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": s3_output},
        WorkGroup=workgroup,
    )
    return response["QueryExecutionId"]


def wait_for_query(athena, execution_id: str, poll_interval: float = 1.0) -> dict:
    """Poll until query completes or fails."""
    while True:
        response = athena.get_query_execution(QueryExecutionId=execution_id)
        state = response["QueryExecution"]["Status"]["State"]

        if state == "SUCCEEDED":
            return response["QueryExecution"]
        elif state in ("FAILED", "CANCELLED"):
            reason = response["QueryExecution"]["Status"].get("StateChangeReason", "Unknown")
            raise RuntimeError(f"Query {state}: {reason}")

        print(f"  Status: {state}...")
        time.sleep(poll_interval)


def fetch_results(athena, execution_id: str) -> tuple[list[str], list[list[str]]]:
    """Fetch all results, handling pagination."""
    columns = []
    rows = []
    next_token = None
    first_page = True

    while True:
        kwargs = {"QueryExecutionId": execution_id, "MaxResults": 1000}
        if next_token:
            kwargs["NextToken"] = next_token

        response = athena.get_query_results(**kwargs)

        result_set = response["ResultSet"]

        if first_page:
            columns = [col["Label"] for col in result_set["ResultSetMetadata"]["ColumnInfo"]]
            # First row is header in Athena results
            data_rows = result_set["Rows"][1:]
            first_page = False
        else:
            data_rows = result_set["Rows"]

        for row in data_rows:
            rows.append([col.get("VarCharValue", "") for col in row["Data"]])

        next_token = response.get("NextToken")
        if not next_token:
            break

    return columns, rows


def format_results(columns: list[str], rows: list[list[str]], query: str, execution_info: dict) -> str:
    """Format results as readable text."""
    lines = [
        "Athena Query Results",
        "=" * 70,
        f"Timestamp: {datetime.now().isoformat()}",
        f"Database: {DATABASE}",
        f"Execution ID: {execution_info['QueryExecutionId']}",
        f"Data scanned: {execution_info['Statistics'].get('DataScannedInBytes', 0) / 1024 / 1024:.2f} MB",
        f"Execution time: {execution_info['Statistics'].get('TotalExecutionTimeInMillis', 0)}ms",
        "=" * 70,
        "",
        "Query:",
        "-" * 40,
        query.strip(),
        "-" * 40,
        "",
        f"Results ({len(rows)} rows):",
        "",
    ]

    if not rows:
        lines.append("(no results)")
        return "\n".join(lines)

    # Calculate column widths
    widths = [len(col) for col in columns]
    for row in rows:
        for i, val in enumerate(row):
            widths[i] = max(widths[i], len(str(val)))

    # Cap widths at 200 chars
    widths = [min(w, 200) for w in widths]

    # Header
    header = " | ".join(col.ljust(widths[i]) for i, col in enumerate(columns))
    lines.append(header)
    lines.append("-" * len(header))

    # Rows
    for row in rows:
        formatted = " | ".join(str(val)[:widths[i]].ljust(widths[i]) for i, val in enumerate(row))
        lines.append(formatted)

    return "\n".join(lines)


def main():
    print("Athena Query Executor")
    print("=" * 40)

    # Load and prepare query
    try:
        raw_query = load_query(QUERY)
        final_query = substitute_params(raw_query, PARAMS)
        validate_read_only(final_query)
    except FileNotFoundError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
    except ValueError as e:
        print(f"BLOCKED: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"Database: {DATABASE}")
    print(f"Parameters: {json.dumps(PARAMS)}")
    print()

    # Execute
    try:
        print(f"Using profile: {AWS_PROFILE}")
        session = get_aws_session()
        athena = session.client("athena")
    except NoCredentialsError:
        print("ERROR: No AWS credentials found.", file=sys.stderr)
        print("  Run 'aws sso login' or configure ~/.aws/credentials", file=sys.stderr)
        sys.exit(1)
    except RuntimeError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        print("Starting query...")
        execution_id = execute_query(athena, DATABASE, final_query, S3_OUTPUT, WORKGROUP)
        print(f"Execution ID: {execution_id}")

        print("Waiting for results...")
        execution_info = wait_for_query(athena, execution_id)

        print("Fetching results...")
        columns, rows = fetch_results(athena, execution_id)
        print(f"Retrieved {len(rows)} rows")

    except ClientError as e:
        print(f"\nERROR: AWS API error", file=sys.stderr)
        print(f"  Code: {e.response['Error']['Code']}", file=sys.stderr)
        print(f"  Message: {e.response['Error']['Message']}", file=sys.stderr)
        sys.exit(1)
    except RuntimeError as e:
        print(f"\nERROR: {e}", file=sys.stderr)
        sys.exit(1)

    # Format and output
    output = format_results(columns, rows, final_query, execution_info)

    print()
    print(output)

    # Save to file
    OUTPUT_DIR.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = OUTPUT_DIR / f"query_{timestamp}.txt"
    output_path.write_text(output)
    print(f"\nSaved to: {output_path}")


if __name__ == "__main__":
    main()
