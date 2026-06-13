#!/usr/bin/env bash
set -euo pipefail

HOST="${TEMPORAL_HOST:-127.0.0.1}"
PORT="${TEMPORAL_PORT:-7233}"
UI_PORT="${TEMPORAL_UI_PORT:-8233}"
METRICS_PORT="${TEMPORAL_METRICS_PORT:-7234}"
DB_FILENAME="${TEMPORAL_DB_FILENAME:-.temporal/dev-server.db}"

if ! command -v temporal >/dev/null 2>&1; then
  cat >&2 <<'EOF'
Temporal CLI is required for the persistent dev server target.

Install Temporal CLI, then rerun this script:

  brew install temporal

The Docker fallback used by scripts/start-temporal.sh is intentionally not used
here because persistence needs a stable local database file.
EOF
  exit 1
fi

mkdir -p "$(dirname "$DB_FILENAME")"

echo "Starting persistent Temporal dev server"
echo "  gRPC:     $HOST:$PORT"
echo "  Web UI:   http://$HOST:$UI_PORT"
echo "  Metrics:  http://$HOST:$METRICS_PORT/metrics"
echo "  DB file:  $DB_FILENAME"

exec temporal server start-dev \
  --ip "$HOST" \
  --port "$PORT" \
  --ui-port "$UI_PORT" \
  --metrics-port "$METRICS_PORT" \
  --db-filename "$DB_FILENAME"
