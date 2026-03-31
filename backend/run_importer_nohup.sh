#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

export HEALTH_DB_HOST="${HEALTH_DB_HOST:-127.0.0.1}"
export HEALTH_DB_PORT="${HEALTH_DB_PORT:-3306}"
export HEALTH_DB_USER="${HEALTH_DB_USER:-root}"
export HEALTH_DB_NAME="${HEALTH_DB_NAME:-apple_health}"

if [[ -z "${HEALTH_DB_PASSWORD:-}" ]]; then
  echo "HEALTH_DB_PASSWORD is required"
  exit 1
fi

mkdir -p logs

nohup python3 backend/importer.py >> logs/importer.log 2>&1 &

echo "importer started, pid=$!"
echo "log: $(pwd)/logs/importer.log"
