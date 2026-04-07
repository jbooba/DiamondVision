#!/usr/bin/env bash
set -euo pipefail

DATA_ROOT="${RAILWAY_VOLUME_MOUNT_PATH:-/data}"
export MLB_HISTORY_RAW_DATA_DIR="${MLB_HISTORY_RAW_DATA_DIR:-${DATA_ROOT}/raw}"
export MLB_HISTORY_PROCESSED_DIR="${MLB_HISTORY_PROCESSED_DIR:-${DATA_ROOT}/processed}"
export MLB_HISTORY_DATABASE_PATH="${MLB_HISTORY_DATABASE_PATH:-${MLB_HISTORY_PROCESSED_DIR}/mlb_history.sqlite3}"

mkdir -p "${MLB_HISTORY_RAW_DATA_DIR}" "${MLB_HISTORY_PROCESSED_DIR}"

if [ ! -f "${MLB_HISTORY_DATABASE_PATH}" ]; then
  echo "Warning: ${MLB_HISTORY_DATABASE_PATH} does not exist yet."
  echo "Historical queries will stay limited until the database is populated on the mounted volume."
fi

exec python -m uvicorn mlb_history_bot.api:app --host 0.0.0.0 --port "${PORT:-8000}"
