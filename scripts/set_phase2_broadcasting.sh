#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-}"
if [ "${MODE}" != "true" ] && [ "${MODE}" != "false" ]; then
  echo "Usage: ./scripts/set_phase2_broadcasting.sh true|false"
  exit 1
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TFVARS="${ROOT_DIR}/terraform/environments/dev/terraform.tfvars"

python3 - "${TFVARS}" "${MODE}" <<'PY'
from pathlib import Path
import re
import sys

path = Path(sys.argv[1])
mode = sys.argv[2]
text = path.read_text()
pattern = r"(?m)^broadcast_shard_jobs_enabled\s*=\s*(true|false)\s*$"
replacement = f"broadcast_shard_jobs_enabled             = {mode}"
updated, count = re.subn(pattern, replacement, text)
if count != 1:
    raise SystemExit(
        f"Expected exactly one broadcast_shard_jobs_enabled entry, found {count}"
    )
path.write_text(updated)
PY

echo "broadcast_shard_jobs_enabled=${MODE}"
echo "Review with: git diff -- terraform/environments/dev/terraform.tfvars"
