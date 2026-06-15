#!/bin/bash
# À exécuter depuis la racine du projet

# ADRs
mkdir -p docs/adr
touch docs/adr/ADR-001-fargate-collector.md
touch docs/adr/ADR-002-kinesis-backbone.md
touch docs/adr/ADR-003-kinesis-partition-key.md
touch docs/adr/ADR-004-dynamodb-aggregates.md
touch docs/adr/ADR-005-write-sharding.md
touch docs/adr/ADR-006-websocket-dashboard.md
touch docs/adr/ADR-007-sqs-fifo-dedup.md
touch docs/adr/ADR-008-historical-analytics.md

# Services
for svc in collector realtime-processor alert-processor broadcaster \
            websocket-connect-handler websocket-disconnect-handler \
            websocket-default-handler; do
  mkdir -p services/$svc/src
  touch services/$svc/requirements.txt
  touch services/$svc/src/handler.py
done

# Collector spécifique (a un Dockerfile)
touch services/collector/Dockerfile
mv services/collector/src/handler.py services/collector/src/main.py

# Frontend
mkdir -p frontend/dashboard
touch frontend/dashboard/.gitkeep

# Terraform modules
for mod in networking kms iam kinesis s3-datalake dynamodb sqs sns \
            ecs-collector lambda apigw-websocket firehose glue athena monitoring; do
  mkdir -p terraform/modules/$mod
  touch terraform/modules/$mod/main.tf
  touch terraform/modules/$mod/variables.tf
  touch terraform/modules/$mod/outputs.tf
done

# Terraform dev environment
mkdir -p terraform/environments/dev
touch terraform/environments/dev/main.tf
touch terraform/environments/dev/variables.tf
touch terraform/environments/dev/outputs.tf
touch terraform/environments/dev/terraform.tfvars

echo "Scaffold complete"