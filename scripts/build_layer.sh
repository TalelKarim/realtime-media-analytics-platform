#!/usr/bin/env bash
set -euo pipefail

LAYER_NAME="${1:?Usage: ./scripts/build_layer.sh <layer-name>}"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SOURCE_DIR="${ROOT_DIR}/services/layers/${LAYER_NAME}"
BUILD_DIR="${ROOT_DIR}/.build/layers/${LAYER_NAME}"

if [ ! -d "${SOURCE_DIR}" ]; then
  echo "Layer source directory not found: ${SOURCE_DIR}"
  exit 1
fi

rm -rf "${BUILD_DIR}"
mkdir -p "${BUILD_DIR}/python"

if [ -d "${SOURCE_DIR}/python" ]; then
  cp -R "${SOURCE_DIR}/python/." "${BUILD_DIR}/python/"
fi

if [ -f "${SOURCE_DIR}/requirements.txt" ] && [ -s "${SOURCE_DIR}/requirements.txt" ]; then
  python3 -m pip install \
    --upgrade \
    -r "${SOURCE_DIR}/requirements.txt" \
    -t "${BUILD_DIR}/python"
fi

find "${BUILD_DIR}" -type d -name "__pycache__" -prune -exec rm -rf {} +
find "${BUILD_DIR}" -type f -name "*.pyc" -delete

echo "Layer build completed: ${BUILD_DIR}"