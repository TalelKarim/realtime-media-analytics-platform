#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
ROOT="$(cd "$ROOT" && pwd)"

checks=(
  'terraform/modules/apigw_websocket/main.tf:resource "aws_acm_certificate" "websocket"'
  'terraform/modules/apigw_websocket/main.tf:resource "aws_acm_certificate_validation" "websocket"'
  'terraform/modules/apigw_websocket/main.tf:resource "aws_apigatewayv2_domain_name" "websocket"'
  'terraform/modules/apigw_websocket/main.tf:resource "aws_apigatewayv2_api_mapping" "custom_domain"'
  'terraform/modules/apigw_websocket/main.tf:resource "aws_route53_record" "websocket_alias"'
  'terraform/modules/apigw_websocket/variables.tf:variable "hosted_zone_name"'
  'terraform/environments/dev/apigw_websocket.tf:hosted_zone_name              = var.domain_name'
  'terraform/environments/dev/terraform.tfvars:websocket_custom_domain_name = "stream-websocket.talelkarimchebbi.com"'
  'frontend/dashboard/.env:VITE_WS_URL=wss://stream-websocket.talelkarimchebbi.com'
)

for item in "${checks[@]}"; do
  file="${item%%:*}"
  pattern="${item#*:}"
  if ! grep -Fq "$pattern" "$ROOT/$file"; then
    echo "Validation failed: missing '$pattern' in $file" >&2
    exit 1
  fi
done

if [[ -d "$ROOT/terraform/stable/websocket-domain" ]]; then
  echo "Validation failed: the separate stable Terraform root still exists." >&2
  exit 1
fi

python3 - "$ROOT" <<'PY'
from pathlib import Path
import sys

root = Path(sys.argv[1])
files = [
    root / "terraform/modules/apigw_websocket/main.tf",
    root / "terraform/modules/apigw_websocket/variables.tf",
    root / "terraform/modules/apigw_websocket/outputs.tf",
    root / "terraform/environments/dev/apigw_websocket.tf",
    root / "terraform/environments/dev/variables.tf",
    root / "terraform/environments/dev/outputs.tf",
]
for path in files:
    text = path.read_text()
    if text.count("{") != text.count("}"):
        raise SystemExit(f"Unbalanced braces in {path}")
print("Basic Terraform brace validation passed.")
PY

echo "Same-workspace WebSocket custom-domain validation passed."
