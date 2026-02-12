#!/usr/bin/env sh
# 간단한 실행 스크립트 (uses Snowflake CLI `snow`)
if command -v snow >/dev/null 2>&1; then
  exec snow sql -a "${SNOWFLAKE_ACCOUNT}" -u "${SNOWFLAKE_USER}" -f "$(dirname "$0")/queries/etl_sample.sql"
else
  echo "snow not found. Install Snowflake CLI (snow)." >&2
  exit 1
fi
