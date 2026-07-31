# Repository Map

## Runtime services

| Path | Responsibility | Trigger |
|---|---|---|
| `services/collector` | SSE connection, validation, sampling, normalization, Kinesis producer | ECS service process |
| `services/realtime-processor` | realtime aggregates and broadcast signals | Kinesis event source mapping |
| `services/alert-processor` | alert counters, baselines and SNS | Kinesis event source mapping |
| `services/broadcast-coordinator` | snapshots, manifests, LATEST and shard jobs | `broadcast-signal.fifo` |
| `services/broadcast-worker` | shard query, chunking and WebSocket fan-out | `broadcast-jobs.fifo` |
| `services/websocket-connect-handler` | create connection/default topic | API Gateway `$connect` |
| `services/websocket-default-handler` | subscribe/unsubscribe | API Gateway `$default` |
| `services/websocket-disconnect-handler` | remove connection state | API Gateway `$disconnect` |
| `services/broadcaster` | legacy V1 single Broadcaster | no active event source mapping; remove |

## Shared layers

| Path | Purpose |
|---|---|
| `services/layers/common-python` | shared Python dependencies/helpers |
| `services/layers/websocket-python` | topic normalization, shard calculation, DynamoDB serialization and EMF logging |

## Frontend

| Path | Purpose |
|---|---|
| `frontend/dashboard/src/hooks/useRealtimeWebSocket.ts` | connection, heartbeat, reconnect and subscriptions |
| `frontend/dashboard/src/lib/normalize.ts` | batch/single message normalization and cursor extraction |
| `frontend/dashboard/src/types/realtime.ts` | frontend contracts |
| `frontend/dashboard/src/App.tsx` | cursor guard and dashboard state |

## Terraform environment composition

| File | Purpose |
|---|---|
| `kinesis.tf` | source stream |
| `ecs_collector.tf` | Collector service, secret and task configuration |
| `lambda_realtime_processor.tf` | live Kinesis consumer |
| `lambda_alert_processor.tf` | alert Kinesis consumer |
| `lambda_broadcasting_enhanced.tf` | Coordinator and Worker Lambdas |
| `braodcasting_enhanced_event_sources.tf` | active signal/job event mappings; rename typo |
| `lambda_websocket_handlers.tf` | lifecycle Lambdas |
| `lambda_broadcaster.tf` | legacy V1 Lambda; remove |
| `dynamodb.tf` | table module composition |
| `sqs.tf` | signal and jobs queues |
| `apigw_websocket.tf` | WebSocket API, routes and custom domain |
| `firehose.tf` | Kinesis-to-S3 Bronze delivery |
| `glue_etl.tf` | Bronze/Silver/Gold jobs |
| `realtime_dashboard.tf` | static dashboard infrastructure |
| `observability.tf` | Grafana AWS integration and monitoring composition |
| `loki_promtail_subscriptions.tf` | CloudWatch Logs to Loki subscriptions |
| `k6_load_generator_ec2.tf` | temporary load-generator infrastructure |

## Terraform modules

Modules cover API Gateway, Athena, CloudWatch log groups, DynamoDB, ECS Collector, Firehose, Glue, IAM, Kinesis, KMS, Lambda, monitoring, networking, QuickSight, dashboard hosting, S3, SNS and SQS.

## Build scripts

```text
scripts/build_layer.sh
scripts/build_lambda.sh
scripts/build_all.sh
```

Rollout validation scripts with `phase1`/`phase2` names are migration artifacts and are listed in `v2-cleanup.md`.

## Documentation ownership

Every source contract should be updated in this order:

```text
code/Terraform
→ data-contracts.md
→ architecture.md and sequence-diagrams.md
→ README.md
→ operations/load-test docs
```
