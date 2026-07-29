#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export PYTHONDONTWRITEBYTECODE=1

python3 -m py_compile \
  "${ROOT_DIR}/services/broadcast-coordinator/src/handler.py" \
  "${ROOT_DIR}/services/broadcast-worker/src/handler.py" \
  "${ROOT_DIR}/services/broadcast-worker/src/observability.py"

for pattern in \
  'broadcast.shard.job' \
  'aggregation_window_epoch_ms' \
  'build_shard_jobs' \
  'send_jobs(shard_jobs)'; do
  grep -q "${pattern}" \
    "${ROOT_DIR}/services/broadcast-coordinator/src/handler.py"
done

for pattern in \
  'connection-shard-index' \
  'check_latest_before_load' \
  'check_latest_before_fanout' \
  'stats.batch_update' \
  'batch_get_item' \
  'chunk_updates' \
  'connection-sender'; do
  grep -q "${pattern}" \
    "${ROOT_DIR}/services/broadcast-worker/src/handler.py"
done

grep -q 'CONNECTION_SHARD_INDEX_NAME' \
  "${ROOT_DIR}/terraform/environments/dev/lambda_broadcasting_enhanced.tf"
grep -q 'dynamodb:BatchGetItem' \
  "${ROOT_DIR}/terraform/modules/iam/main.tf"
grep -q '/index/connection-shard-index' \
  "${ROOT_DIR}/terraform/modules/iam/main.tf"
grep -Eq '^broadcast_shard_jobs_enabled[[:space:]]*=[[:space:]]*false' \
  "${ROOT_DIR}/terraform/environments/dev/terraform.tfvars"

grep -q 'stats.batch_update' \
  "${ROOT_DIR}/frontend/dashboard/src/lib/normalize.ts"
grep -q 'latestBroadcastCursorRef' \
  "${ROOT_DIR}/frontend/dashboard/src/App.tsx"

for service in broadcast-coordinator broadcast-worker; do
  diff -qr \
    --exclude='__pycache__' \
    --exclude='*.pyc' \
    "${ROOT_DIR}/services/${service}/src" \
    "${ROOT_DIR}/.build/lambdas/${service}/src"
done

AWS_ENV=(
  AWS_EC2_METADATA_DISABLED=true
  AWS_ACCESS_KEY_ID=test
  AWS_SECRET_ACCESS_KEY=test
  AWS_DEFAULT_REGION=us-east-1
  AWS_REGION=us-east-1
  OTEL_ENABLED=false
)

env "${AWS_ENV[@]}" \
  AGGREGATES_TABLE_NAME=aggregates \
  SNAPSHOTS_TABLE_NAME=snapshots \
  BROADCAST_JOBS_QUEUE_URL=https://sqs.us-east-1.amazonaws.com/123/jobs.fifo \
  CONNECTION_SHARD_COUNT=3 \
  PUBLISH_SHARD_JOBS=false \
  PYTHONPATH="${ROOT_DIR}/.build/lambdas/broadcast-coordinator" \
  python3 - <<'PY'
from src import handler

jobs = handler.build_shard_jobs(
    manifest_id="MANIFEST#105",
    sequence=105,
    aggregation_window="2026-07-29T15:00:00Z",
    broadcast_window="2026-07-29T15:00:03Z",
)
assert len(jobs) == 3
assert [job["connection_shard"] for job in jobs] == [
    "SHARD#00", "SHARD#01", "SHARD#02"
]
assert len({handler.build_message_deduplication_id(job) for job in jobs}) == 3
assert all(job["aggregation_window_epoch_ms"] > 0 for job in jobs)
print("Coordinator pure tests passed: exactly one job per connection shard.")
PY

env "${AWS_ENV[@]}" \
  SNAPSHOTS_TABLE_NAME=snapshots \
  CONNECTIONS_TABLE_NAME=connections \
  SUBSCRIPTIONS_TABLE_NAME=subscriptions \
  CONNECTION_SHARD_INDEX_NAME=connection-shard-index \
  WEBSOCKET_ENDPOINT_URL=https://example.execute-api.us-east-1.amazonaws.com/dev \
  PYTHONPATH="${ROOT_DIR}/.build/lambdas/broadcast-worker" \
  python3 - <<'PY'
from src import handler

job = {
    "schema_version": 3,
    "message_type": "broadcast.shard.job",
    "manifest_id": "MANIFEST#105",
    "sequence": 105,
    "shard_id": 1,
    "connection_shard": "SHARD#01",
    "aggregation_window": "2026-07-29T15:00:00Z",
    "aggregation_window_epoch_ms": 1785337200000,
}
handler.validate_job(job)
pointer = {
    "sequence": 105,
    "aggregation_window_epoch_ms": 1785337200000,
    "manifest_id": "MANIFEST#105",
}
assert handler.classify_job_against_latest(job, pointer) == "current"
assert handler.classify_job_against_latest(
    {**job, "sequence": 104}, pointer
) == "stale"

manifest = {
    "snapshot_id": "MANIFEST#105",
    "sequence": 105,
    "aggregation_window": "2026-07-29T15:00:00Z",
}
updates = [
    {
        "topic": f"wiki:w{index}",
        "data": {"blob": "x" * 3500},
        "latest_event_timestamp_ms": 1000 + index,
        "oldest_event_timestamp_ms": 900 + index,
    }
    for index in range(20)
]
chunks = handler.chunk_updates(manifest=manifest, updates=updates)
assert len(chunks) >= 3
assert sum(chunk["topic_count"] for chunk in chunks) == 20
assert all(
    handler.serialized_payload_size(chunk["message"])
    <= handler.MAX_WEBSOCKET_PAYLOAD_BYTES
    for chunk in chunks
)
print(
    f"Worker pure tests passed: double cursor logic and {len(chunks)} safe chunks."
)
PY

if command -v tsc >/dev/null 2>&1; then
  (
    cd "${ROOT_DIR}/frontend/dashboard"
    tsc --strict --noEmit \
      --target ES2022 \
      --module ESNext \
      --moduleResolution Bundler \
      --lib ES2022,DOM \
      src/types/realtime.ts \
      src/lib/normalize.ts
  )
  echo "Frontend contract/cursor TypeScript checks passed."
else
  echo "WARNING: tsc not found; frontend type check skipped."
fi

if [ -x "${ROOT_DIR}/frontend/dashboard/node_modules/.bin/vite" ]; then
  (
    cd "${ROOT_DIR}/frontend/dashboard"
    npm run build
  )
else
  echo "INFO: node_modules absent; run 'npm ci && npm run build' in frontend/dashboard."
fi

if command -v terraform >/dev/null 2>&1; then
  terraform fmt -check -recursive "${ROOT_DIR}/terraform"
else
  echo "INFO: terraform CLI absent; Terraform Cloud will perform the HCL validation."
fi

find "${ROOT_DIR}/services" "${ROOT_DIR}/.build/lambdas/broadcast-coordinator" \
  "${ROOT_DIR}/.build/lambdas/broadcast-worker" \
  -type d -name '__pycache__' -prune -exec rm -rf {} + 2>/dev/null || true
find "${ROOT_DIR}/services" "${ROOT_DIR}/.build/lambdas/broadcast-coordinator" \
  "${ROOT_DIR}/.build/lambdas/broadcast-worker" \
  -type f -name '*.pyc' -delete 2>/dev/null || true

echo "Phase 2 shard-centric validation passed. Activation remains OFF."
