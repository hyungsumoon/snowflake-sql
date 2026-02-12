#!/usr/bin/env sh
# Check for Snowflake CLI (`snow`) and show version
if command -v snow >/dev/null 2>&1; then
  snow --version
  exit 0
fi
echo "No Snowflake CLI (snow) found. Install Snowflake CLI." >&2
