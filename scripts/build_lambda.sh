#!/usr/bin/env bash
set -euo pipefail

LAMBDA_NAME="${1:?Usage: ./scripts/build_lambda.sh <lambda-name>}"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SOURCE_DIR="${ROOT_DIR}/services/${LAMBDA_NAME}"
BUILD_DIR="${ROOT_DIR}/.build/lambdas/${LAMBDA_NAME}"

if [ ! -d "${SOURCE_DIR}/src" ]; then
  echo "Lambda source directory not found: ${SOURCE_DIR}/src"
  exit 1
fi

rm -rf "${BUILD_DIR}"
mkdir -p "${BUILD_DIR}"

if [ -f "${SOURCE_DIR}/requirements.txt" ] && [ -s "${SOURCE_DIR}/requirements.txt" ]; then
  python3 -m pip install \
    --upgrade \
    -r "${SOURCE_DIR}/requirements.txt" \
    -t "${BUILD_DIR}"
fi

cp -R "${SOURCE_DIR}/src" "${BUILD_DIR}/src"

find "${BUILD_DIR}" -type d -name "__pycache__" -prune -exec rm -rf {} +
find "${BUILD_DIR}" -type f -name "*.pyc" -delete

echo "Lambda build completed: ${BUILD_DIR}"