#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

"${ROOT_DIR}/scripts/build_layer.sh" common-python
"${ROOT_DIR}/scripts/build_layer.sh" websocket-python

"${ROOT_DIR}/scripts/build_lambda.sh" realtime-processor

# Later:
# "${ROOT_DIR}/scripts/build_lambda.sh" broadcaster
# "${ROOT_DIR}/scripts/build_lambda.sh" alert-processor
# "${ROOT_DIR}/scripts/build_lambda.sh" websocket-connect-handler
# "${ROOT_DIR}/scripts/build_lambda.sh" websocket-disconnect-handler
# "${ROOT_DIR}/scripts/build_lambda.sh" websocket-default-handler

echo "All builds completed."