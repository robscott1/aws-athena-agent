#!/bin/bash
# Pre-tool hook: block query.py execution if the query has no dt partition filter.

COMMAND=$(jq -r '.tool_input.command' 2>/dev/null)

if ! echo "$COMMAND" | grep -q 'query\.py'; then
  exit 0
fi

QUERY_FILE="${CLAUDE_PROJECT_DIR}/query.py"
if [ ! -f "$QUERY_FILE" ]; then
  exit 0
fi

if grep -qiE '^\s*DESCRIBE\b' "$QUERY_FILE"; then
  exit 0
fi

if grep -qiE "WHERE.*dt\b|dt\s*(>=|<=|=|BETWEEN|IN)" "$QUERY_FILE"; then
  exit 0
fi

echo "BLOCKED: query.py has no dt partition filter. Add a WHERE clause filtering by dt to avoid expensive full-table scans." >&2
exit 2