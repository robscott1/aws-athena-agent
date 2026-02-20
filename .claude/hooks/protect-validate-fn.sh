#!/bin/bash
# Pre-tool hook: block edits that modify the validate_read_only function in query.py.
# This function is a critical safety gate — it must not be weakened or removed.

INPUT=$(cat)

FILE_PATH=$(echo "$INPUT" | jq -r '.tool_input.file_path // .tool_input.content // empty' 2>/dev/null)

if ! echo "$FILE_PATH" | grep -q 'query\.py'; then
  exit 0
fi

QUERY_FILE="${CLAUDE_PROJECT_DIR}/query.py"
if [ ! -f "$QUERY_FILE" ]; then
  exit 0
fi

OLD_STRING=$(echo "$INPUT" | jq -r '.tool_input.old_string // empty' 2>/dev/null)
NEW_STRING=$(echo "$INPUT" | jq -r '.tool_input.new_string // empty' 2>/dev/null)

for TEXT in "$OLD_STRING" "$NEW_STRING"; do
  if [ -z "$TEXT" ]; then
    continue
  fi
  if echo "$TEXT" | grep -qE 'validate_read_only|write_keywords|Write operation detected'; then
    echo "BLOCKED: Edits to the validate_read_only function in query.py are not allowed. This function is a critical safety gate that prevents write operations against Athena." >&2
    exit 2
  fi
done

# For Write tool: if the full file is being written, extract the function and compare to git
CONTENT=$(echo "$INPUT" | jq -r '.tool_input.content // empty' 2>/dev/null)
if [ -n "$CONTENT" ]; then
  GIT_FN=$(git -C "$CLAUDE_PROJECT_DIR" show HEAD:query.py 2>/dev/null | sed -n '/^def validate_read_only/,/^def /{ /^def validate_read_only/p; /^def [^v]/!p; }')
  NEW_FN=$(echo "$CONTENT" | sed -n '/^def validate_read_only/,/^def /{ /^def validate_read_only/p; /^def [^v]/!p; }')

  if [ -z "$NEW_FN" ]; then
    echo "BLOCKED: The validate_read_only function was removed from query.py. This function is a critical safety gate that prevents write operations against Athena." >&2
    exit 2
  fi

  if [ "$GIT_FN" != "$NEW_FN" ]; then
    echo "BLOCKED: The validate_read_only function in query.py was modified. This function is a critical safety gate that prevents write operations against Athena." >&2
    exit 2
  fi
fi

exit 0