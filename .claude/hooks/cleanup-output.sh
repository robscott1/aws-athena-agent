#!/bin/bash
# Post-tool hook: keep only the 25 most recent files in the output/ directory.

OUTPUT_DIR="$CLAUDE_PROJECT_DIR/output"

if [ ! -d "$OUTPUT_DIR" ]; then
  exit 0
fi

# Count files (non-recursive)
FILE_COUNT=$(find "$OUTPUT_DIR" -maxdepth 1 -type f | wc -l | tr -d ' ')

if [ "$FILE_COUNT" -le 25 ]; then
  exit 0
fi

# Delete oldest files, keeping the 25 most recent
EXCESS=$((FILE_COUNT - 25))
find "$OUTPUT_DIR" -maxdepth 1 -type f -print0 \
  | xargs -0 ls -t \
  | tail -n "$EXCESS" \
  | xargs rm --

exit 0