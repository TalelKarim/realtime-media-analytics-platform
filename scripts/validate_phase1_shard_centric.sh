#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

python3 -m py_compile \
  "${ROOT_DIR}/services/realtime-processor/src/handler.py" \
  "${ROOT_DIR}/services/broadcast-coordinator/src/handler.py" \
  "${ROOT_DIR}/services/websocket-connect-handler/src/handler.py" \
  "${ROOT_DIR}/services/websocket-default-handler/src/handler.py" \
  "${ROOT_DIR}/services/websocket-disconnect-handler/src/handler.py"

grep -q 'name            = "connection-shard-index"' \
  "${ROOT_DIR}/terraform/modules/dynamodb/main.tf"

grep -q 'PUBLISH_SHARD_JOBS' \
  "${ROOT_DIR}/terraform/environments/dev/lambda_broadcasting_enhanced.tf"

grep -q 'broadcast_shard_jobs_enabled' \
  "${ROOT_DIR}/terraform/environments/dev/braodcasting_enhanced_event_sources.tf"

grep -q 'event_timestamp_bounds_by_topic_by_window' \
  "${ROOT_DIR}/services/realtime-processor/src/handler.py"

grep -q 'broadcast.shard.job' \
  "${ROOT_DIR}/services/broadcast-coordinator/src/handler.py"

grep -q 'broadcast_shard_jobs_publish_skipped_phase1' \
  "${ROOT_DIR}/services/broadcast-coordinator/src/handler.py"

for service in \
  realtime-processor \
  broadcast-coordinator \
  websocket-connect-handler \
  websocket-default-handler \
  websocket-disconnect-handler; do
  diff -qr \
    --exclude='__pycache__' \
    --exclude='*.pyc' \
    "${ROOT_DIR}/services/${service}/src" \
    "${ROOT_DIR}/.build/lambdas/${service}/src"
done

echo "Phase 1 validation passed."
