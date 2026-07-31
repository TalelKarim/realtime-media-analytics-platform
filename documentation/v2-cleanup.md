# V2-only Cleanup Plan

This document identifies migration residue in the uploaded branch. Do not delete the transitional subscription table before updating all lifecycle and cleanup code that references it.

## A. Files safe to delete after confirming V2 is active

```text
services/broadcaster/
terraform/environments/dev/lambda_broadcaster.tf
.build/lambdas/broadcaster/
.patch-backups/
scripts/validate_phase1_shard_centric.sh
scripts/set_phase2_broadcasting.sh
```

Potentially delete these rollout-only/manual validation scripts after replacing any useful checks with V2-named smoke tests:

```text
scripts/invoke_phase2_worker_from_latest.sh
scripts/validate_phase2_shard_centric.sh
```

## B. V1 blocks to remove from shared files

These are not whole-file deletions.

### `terraform/modules/iam/main.tf`

Remove:

```text
aws_iam_role.broadcaster
aws_iam_role_policy.broadcaster
legacy broadcaster name/local references
```

### `terraform/modules/iam/outputs.tf`

Remove:

```text
broadcaster_role_arn
broadcaster entry from lambda_role_arns
```

### `terraform/environments/dev/cloudwatch_log_groups.tf`

Remove:

```text
/aws/lambda/${project}-${environment}-broadcaster
```

### `terraform/environments/dev/loki_promtail_subscriptions.tf`

Remove the `broadcaster` map entry.

### `scripts/build_all.sh`

Remove the commented legacy Broadcaster build lines and duplicated “Later” block.

## C. Transitional topic-shard table removal

The final V2 recipient path uses:

```text
websocket_connections
+ connection-shard-index
+ topics list on each connection
```

`websocket_subscriptions` is no longer needed for discovery, but the uploaded code still dual-writes/deletes it.

After refactoring the handlers, remove:

### DynamoDB module

From `terraform/modules/dynamodb/main.tf`:

```text
local websocket_subscriptions_table_name
aws_dynamodb_table.websocket_subscriptions
```

From `terraform/modules/dynamodb/outputs.tf`:

```text
websocket_subscriptions_table_name
websocket_subscriptions_table_arn
entries in table maps
```

From environment files:

```text
outputs.tf subscription table outputs
iam.tf websocket_subscriptions_table_name argument
lambda_websocket_handlers.tf subscription table environment variables
lambda_broadcasting_enhanced.tf SUBSCRIPTIONS_TABLE_NAME
```

### IAM module

Remove the subscription table variable/local/ARN and permissions from:

```text
broadcast_worker
websocket_connect
websocket_default
websocket_disconnect
```

### Application code

Refactor before deleting the table:

```text
websocket-connect-handler:
  Put only websocket_connections item

websocket-default-handler:
  update only connection topics/connection_shard

websocket-disconnect-handler:
  delete only websocket_connections item

broadcast-worker cleanup_gone_connection:
  delete only websocket_connections item
```

Then remove unused `build_topic_shard` paths and `WEBSOCKET_SUBSCRIPTIONS_TABLE_NAME` environment requirements.

## D. Rollout flags to remove

Final V2 should hard-enable the active path and remove:

```text
broadcast_shard_jobs_enabled
enhaned_broadcasting_enabled
PUBLISH_SHARD_JOBS rollout semantics
Phase 1 / Phase 2 comments
```

Keep only a deliberate operational kill switch if it is named and documented as such.

Rename typos:

```text
braodcasting_enhanced_event_sources.tf
→ broadcasting_v2_event_sources.tf

enhaned_broadcasting_enabled
→ broadcasting_v2_enabled (only if a kill switch remains)
```

## E. Resources that are V2 and must remain

Do not delete:

```text
broadcast-signal.fifo and its DLQ
broadcast-jobs.fifo and its DLQ
broadcast-coordinator Lambda
broadcast-worker Lambda
broadcast_snapshots table
websocket_connections table and connection-shard-index
Realtime Processor broadcast signal code
```

`broadcast-signal.fifo` existed before V2, but it remains an essential V2 queue between Processor and Coordinator.
