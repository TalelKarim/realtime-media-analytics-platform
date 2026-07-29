#!/usr/bin/env bash
set -euo pipefail

SHARD_ID="${1:-0}"
if ! [[ "${SHARD_ID}" =~ ^[0-9]+$ ]]; then
  echo "Usage: ./scripts/invoke_phase2_worker_from_latest.sh <shard-id>"
  exit 1
fi

AWS_REGION="${AWS_REGION:-us-east-1}"
PREFIX="${RMAP_PREFIX:-realtime-media-analytics-dev}"
TABLE_NAME="${PREFIX}-broadcast-snapshots"
CONNECTIONS_TABLE="${PREFIX}-websocket-connections"
FUNCTION_NAME="${PREFIX}-broadcast-worker"
SHARD_NAME="$(printf 'SHARD#%02d' "${SHARD_ID}")"

for command in aws jq python3; do
  command -v "${command}" >/dev/null || {
    echo "Missing required command: ${command}"
    exit 1
  }
done

LATEST_JSON="$(aws dynamodb get-item \
  --region "${AWS_REGION}" \
  --table-name "${TABLE_NAME}" \
  --key '{"snapshot_id":{"S":"LATEST"},"topic":{"S":"MANIFEST"}}' \
  --consistent-read)"

MANIFEST_ID="$(jq -r '.Item.manifest_id.S // empty' <<<"${LATEST_JSON}")"
SEQUENCE="$(jq -r '.Item.sequence.N // empty' <<<"${LATEST_JSON}")"
WINDOW="$(jq -r '.Item.aggregation_window.S // empty' <<<"${LATEST_JSON}")"
WINDOW_EPOCH_MS="$(jq -r '.Item.aggregation_window_epoch_ms.N // empty' <<<"${LATEST_JSON}")"

if [ -z "${MANIFEST_ID}" ] || [ -z "${SEQUENCE}" ] || [ -z "${WINDOW}" ]; then
  echo "LATEST pointer is incomplete."
  jq . <<<"${LATEST_JSON}"
  exit 1
fi

CONNECTION_COUNT="$(aws dynamodb query \
  --region "${AWS_REGION}" \
  --table-name "${CONNECTIONS_TABLE}" \
  --index-name connection-shard-index \
  --key-condition-expression 'connection_shard = :shard' \
  --expression-attribute-values "{\":shard\":{\"S\":\"${SHARD_NAME}\"}}" \
  --select COUNT \
  --query Count \
  --output text)"

if [ "${CONNECTION_COUNT}" = "0" ]; then
  echo "No active connection in ${SHARD_NAME}. Choose another shard."
  exit 1
fi

CREATED_AT_MS="$(python3 - <<'PY'
import time
print(int(time.time() * 1000))
PY
)"

PAYLOAD="$(jq -nc \
  --arg manifest_id "${MANIFEST_ID}" \
  --arg sequence "${SEQUENCE}" \
  --arg shard_id "${SHARD_ID}" \
  --arg connection_shard "${SHARD_NAME}" \
  --arg aggregation_window "${WINDOW}" \
  --arg window_epoch_ms "${WINDOW_EPOCH_MS:-0}" \
  --arg created_at_ms "${CREATED_AT_MS}" \
  '{
    schema_version: 3,
    message_type: "broadcast.shard.job",
    manifest_id: $manifest_id,
    broadcast_id: $manifest_id,
    sequence: ($sequence | tonumber),
    shard_id: ($shard_id | tonumber),
    connection_shard: $connection_shard,
    aggregation_window: $aggregation_window,
    aggregation_window_epoch_ms: ($window_epoch_ms | tonumber),
    created_at_ms: ($created_at_ms | tonumber)
  }')"

OUTPUT_FILE="/tmp/phase2-worker-${SHARD_ID}.json"
aws lambda invoke \
  --region "${AWS_REGION}" \
  --function-name "${FUNCTION_NAME}" \
  --cli-binary-format raw-in-base64-out \
  --payload "${PAYLOAD}" \
  "${OUTPUT_FILE}" >/tmp/phase2-worker-invoke-metadata.json

cat /tmp/phase2-worker-invoke-metadata.json | jq .
cat "${OUTPUT_FILE}" | jq .
echo "Controlled Worker invocation completed for ${SHARD_NAME} (${CONNECTION_COUNT} connection(s))."
