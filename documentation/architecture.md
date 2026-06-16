# Architecture — Realtime Media Analytics Platform

---

## High-Level Architecture

```
                       ┌──────────────────────────────┐
                       │   Wikimedia EventStreams      │
                       │   recentchange SSE ~1000/sec  │
                       └──────────────┬───────────────┘
                                      │ HTTPS / SSE
                                      ▼
                       ┌──────────────────────────────┐
                       │   ECS Fargate Collector       │
                       │   parse · normalize · batch   │
                       └──────────────┬───────────────┘
                                      │ PutRecords
                                      ▼
                       ┌──────────────────────────────┐
                       │   Kinesis Data Streams        │
                       │   central event backbone      │
                       └──────┬───────────┬────────────┘
                              │           │           │
              ┌───────────────┘           │           └──────────────┐
              ▼                           ▼                          ▼
 ┌────────────────────┐    ┌─────────────────────┐    ┌─────────────────────┐
 │ Realtime Processor │    │ Firehose Delivery   │    │ Alert Processor     │
 │ Lambda             │    │ Stream              │    │ Lambda              │
 └────────┬───────────┘    └──────────┬──────────┘    └──────────┬──────────┘
          │                           │                           │
          ▼                           ▼                           ▼
 ┌────────────────────┐    ┌─────────────────────┐    ┌─────────────────────┐
 │ DynamoDB           │    │ S3 Data Lake        │    │ SNS Topic           │
 │ realtime_aggregates│    │ bronze/silver/gold  │    │ alerts              │
 │ websocket_conns    │    └──────────┬──────────┘    └─────────────────────┘
 └────────┬───────────┘               │
          ▼                           ▼
 ┌────────────────────┐    ┌─────────────────────┐
 │ SQS FIFO           │    │ Glue Data Catalog   │
 │ broadcast signal   │    └──────────┬──────────┘
 └────────┬───────────┘               │
          ▼                           ▼
 ┌────────────────────┐    ┌─────────────────────┐
 │ Broadcaster Lambda │    │ Athena              │
 └────────┬───────────┘    └──────────┬──────────┘
          ▼                           ▼
 ┌────────────────────┐    ┌─────────────────────┐
 │ API Gateway        │    │ QuickSight          │
 │ WebSocket          │    │ historical dashboards│
 └────────┬───────────┘    └─────────────────────┘
          ▼
 ┌────────────────────┐
 │ Frontend Dashboard │
 │ live visualization │
 └────────────────────┘
```

---

## Real-Time Flow

```
Wikimedia SSE → ECS Fargate Collector → Kinesis Data Streams
→ Realtime Processor Lambda → DynamoDB realtime_aggregates
→ SQS FIFO → Broadcaster Lambda → API Gateway WebSocket → Frontend
```

### ECS Fargate Collector

Maintains a persistent SSE connection to Wikimedia. Lambda is excluded because it cannot hold an infinite HTTP connection.

Internal design:
```
SSE Reader Thread → In-memory Buffer → Kinesis Sender Thread
```

Flush strategy:
```
Flush when: 100 events accumulated  OR  2 seconds elapsed  OR  shutdown signal
```

### Kinesis Data Streams

Central fan-out backbone. Receives all normalized events from the collector and serves 3 independent consumers simultaneously.

```
Kinesis ──► Realtime Processor Lambda  (real-time aggregation)
        ──► Firehose Delivery Stream   (archival to S3)
        ──► Alert Processor Lambda     (spike detection)
```

Partition key:
```
PutRecords PartitionKey = hash(meta.id)
```
`meta.id` is the globally unique UUID from the Wikimedia Event Platform. Using it as the partition key distributes load evenly and avoids hot shards on high-volume keys like `enwiki:edit`.

### Realtime Processor Lambda

Consumes Kinesis batches. For each event:

1. Decode and validate the normalized event contract.
2. Compute the 1-minute window key.
3. Compute `shard_id = hash(event_id) % AGGREGATE_WRITE_SHARDS` (default: 10).
4. Atomic `UpdateItem ADD` on DynamoDB — no read-modify-write.
5. Write to `METRIC#GLOBAL_ACTIVITY#SHARD#{shard_id}`.
6. Write to `METRIC#WIKI_ACTIVITY#WIKI#{wiki}`.
7. If `namespace = 0`: write to `METRIC#TOP_PAGES#WIKI#{wiki}`.
8. Write to `METRIC#CHANGE_TYPE#TYPE#{change_type}`.
9. Send deduplicated signal to SQS FIFO.

> Log events (`namespace = -1`) are counted in global and wiki activity but excluded from top pages.

### Write Sharding

Global counters are distributed across 10 DynamoDB shards to prevent hot partition throttling.

```
AGGREGATE_WRITE_SHARDS = 10

Write: METRIC#GLOBAL_ACTIVITY#SHARD#{0..9}
Read:  Broadcaster queries all 10 shards and sums
```

### SQS FIFO Deduplication

Prevents the Broadcaster from being invoked more than once per time window, even if 50+ Lambda invocations process batches in the same minute.

```
MessageGroupId        = "broadcast-signal"
MessageDeduplicationId = "aggregates-window-{window_key}"
```

### Broadcaster Lambda

Triggered by SQS FIFO. For each signal:

1. Query all 10 global activity shards from DynamoDB.
2. Sum totals and compute `bot_ratio`.
3. Query `websocket_connections` for connections subscribed to the relevant topics.
4. Call `postToConnection` for each connection.
5. On `GoneException / 410`: delete the stale connection from DynamoDB.

### API Gateway WebSocket Routes

| Route | Handler | Action |
|---|---|---|
| `$connect` | Connect Lambda | Store `connection_id` + default topic `global` + TTL |
| `$disconnect` | Disconnect Lambda | Delete `connection_id` |
| `$default` | Default Lambda | Handle `subscribe` / `unsubscribe` messages |

### WebSocket topic subscriptions

The frontend sends JSON messages to subscribe or unsubscribe from topics. This bidirectional need justifies WebSocket over SSE (SSE is server-to-client only).

```
global              → global platform stats
wiki:{wiki_code}    → per-wiki stats  (e.g. wiki:enwiki, wiki:frwiki)
top_pages           → top changed pages (namespace=0 only)
```

---

## DynamoDB Tables



### Table: websocket_connections

```
PK  = connection_id   (string)

Attributes:
  connected_at   string    ISO8601
  client_type    string    "dashboard"
  topics         list      ["global", "wiki:enwiki", "top_pages"]
  ttl            number    connected_at + 7200 seconds (2 hours)
```

TTL auto-deletes ghost connections where `$disconnect` was missed.

### Table: realtime_aggregates

```
PK  = metric_key   (string)
SK  = window_key   (string)

TTL = window_start + 7200 seconds (2 hours)
```

Five item patterns:

```
METRIC#GLOBAL_ACTIVITY#SHARD#{0-9}  /  WINDOW#{minute}
METRIC#WIKI_ACTIVITY#WIKI#{wiki}    /  WINDOW#{minute}
METRIC#TOP_PAGES#WIKI#{wiki}        /  WINDOW#{minute}#TITLE#{title}
METRIC#CHANGE_TYPE#TYPE#{type}      /  WINDOW#{minute}
METRIC#NAMESPACE#NS#{namespace}     /  WINDOW#{minute}
```



### Table: alert_state

```
PK  = alert_scope   (string)   — "ALERT#GLOBAL" or "ALERT#WIKI#{wiki}"
SK  = minute_key    (string)   — "MINUTE#{yyyy-MM-ddTHH:mm}"

TTL = minute_start + 2100 seconds (35 minutes)
```

Stores per-minute event counts for the Alert Processor rolling window.
The Lambda writes the current minute count and reads the last 30 minutes
to detect activity spikes via z_score.

Design note: 35-minute TTL provides 5 minutes of margin beyond the
30-minute rolling window used for z_score computation.




---

## Alert Processor

Consumes Kinesis independently. Detects:

- Global event volume spike: `z_score > 2.0` over a 30-minute rolling window.
- Log type burst: `log_type = delete` or `block` count > 3× normal in 5 minutes.

Publishes to SNS on detection.

---

## Architecture Decision Records

### ADR-001 — ECS Fargate for SSE Collector
Lambda is excluded: it has a maximum execution duration and cannot maintain an infinite HTTP connection. ECS Fargate runs a long-lived container with an automatic restart policy managed by the ECS service.

### ADR-002 — Kinesis Data Streams as event backbone
Kinesis provides durable buffering, fan-out to multiple independent consumers, configurable retention (up to 7 days), and native Lambda and Firehose integration.

### ADR-003 — hash(meta.id) as Kinesis PartitionKey
`meta.id` is the globally unique UUID from the Wikimedia Event Platform. Hashing it distributes records evenly across shards. Using `wiki` or `change_type` as key would create hot shards: `enwiki` and `edit` dominate the stream volume.

### ADR-004 — DynamoDB for real-time aggregates
DynamoDB provides single-digit millisecond writes, atomic `ADD` counter updates without read-modify-write, and TTL for automatic cleanup. Raw events are not stored here — only aggregated counters needed by the broadcaster.

### ADR-005 — Write sharding for global counters
A single `METRIC#GLOBAL_ACTIVITY` item receiving all increments from concurrent Lambda invocations would become a hot partition. Distributing writes across 10 shards and merging at read time eliminates throttling at expected throughput.

### ADR-006 — API Gateway WebSocket for live dashboard
The dashboard requires bidirectional communication: the backend pushes `stats.update` snapshots, and the frontend sends `subscribe`/`unsubscribe` messages. SSE only supports server-to-client direction and is therefore excluded.

### ADR-007 — SQS FIFO for broadcast deduplication
Without deduplication, every Kinesis Lambda invocation would trigger a broadcaster call. At 50+ invocations per second, this would cause unnecessary DynamoDB reads and WebSocket fan-out. SQS FIFO deduplication by window key ensures one broadcast per time window.

### ADR-008 — Firehose + S3 + Glue + Athena + QuickSight for historical analytics
Firehose is the lowest-overhead archival path from Kinesis: no code, no servers, automatic batching and compression. S3 Parquet with Hive partitioning and Athena partition projection enables cost-efficient SQL without any database to manage. QuickSight connects to Athena for business dashboards.

---

## Scalability Path

### V1 — Portfolio / up to 10K users
```
Single Broadcaster Lambda
SQS FIFO deduplication
DynamoDB connection table
write sharding (10 shards)
```

### V2 — 100K+ users
```
Broadcaster Coordinator Lambda
→ Splits connection list into chunks
→ Sends chunks to SQS standard queue
→ N Broadcaster Worker Lambdas each push to a subset
```

### V3 — Millions of users
```
AWS IoT Core pub/sub
AWS AppSync subscriptions
ECS/EKS persistent WebSocket fleet
Managed: Ably, Pusher, PubNub
```

---

## Observability

### CloudWatch Log Groups

```
/ecs/realtime-media-analytics/collector
/aws/lambda/realtime-processor
/aws/lambda/broadcaster
/aws/lambda/websocket-connect
/aws/lambda/websocket-disconnect
/aws/lambda/websocket-default
/aws/lambda/alert-processor
```

### Custom Metrics

**Collector**
```
custom:collector:events_read_total       total SSE events received from Wikimedia
custom:collector:events_sent_kinesis     total events delivered to Kinesis
custom:collector:batch_size              events per PutRecords call
custom:collector:reconnect_count         SSE reconnection events
custom:collector:kinesis_throttle_count  Kinesis PutRecords throttle responses
```

**Realtime Processor**
```
custom:processor:events_processed        events processed per Lambda invocation
custom:processor:dynamodb_write_count    DynamoDB UpdateItem calls
custom:processor:sqs_signal_sent         broadcast signals sent to SQS FIFO
```

**Broadcaster**
```
custom:broadcaster:connections_read      active connections queried
custom:broadcaster:messages_sent         WebSocket messages delivered
custom:broadcaster:gone_connections      stale connections removed on GoneException
```

### CloudWatch Alarms

| Alarm | Metric | Threshold | Action |
|---|---|---|---|
| Kinesis iterator age high | `GetRecords.IteratorAgeMilliseconds` | > 60 000 ms | SNS |
| Realtime processor errors | Lambda `Errors` | > 5 in 5 min | SNS |
| Broadcaster errors | Lambda `Errors` | > 5 in 5 min | SNS |
| Collector task stopped | ECS `RunningTaskCount` | < 1 | SNS |
| DynamoDB throttles | `SystemErrors` | > 10 in 5 min | SNS |
| SQS queue depth | `ApproximateNumberOfMessagesVisible` | > 100 | SNS |

**Key metric:** `IteratorAgeMilliseconds` = age of the oldest unprocessed record in the shard. Target: < 5 000 ms.

---

## Security

### IAM — Least privilege per component

| Component | Permissions granted |
|---|---|
| ECS Fargate Collector | `kinesis:PutRecord`, `kinesis:PutRecords`, `logs:PutLogEvents` |
| Realtime Processor Lambda | `kinesis:GetRecords`, `kinesis:GetShardIterator`, `dynamodb:UpdateItem`, `sqs:SendMessage` |
| Broadcaster Lambda | `dynamodb:Query`, `dynamodb:DeleteItem`, `sqs:ReceiveMessage`, `execute-api:ManageConnections` |
| Connect / Disconnect / Default Lambdas | `dynamodb:PutItem`, `dynamodb:DeleteItem`, `dynamodb:UpdateItem` |
| Alert Processor Lambda | `kinesis:GetRecords`, `kinesis:GetShardIterator`, `dynamodb:GetItem`, `dynamodb:PutItem`, `dynamodb:UpdateItem`, `dynamodb:Query`, `sns:Publish` || Glue ETL Jobs | `s3:GetObject`, `s3:PutObject`, `glue:*` |

### Encryption

| Resource | Encryption |
|---|---|
| Kinesis Data Streams | SSE — AWS managed KMS key |
| Kinesis Firehose | SSE — AWS managed KMS key |
| DynamoDB | Encryption at rest — AWS managed KMS key |
| S3 Data Lake | SSE-S3 on all buckets |
| SQS FIFO | SSE — AWS managed KMS key |
| CloudWatch Logs | KMS |
| API Gateway | TLS enforced on all endpoints and WebSocket connections |

### S3 bucket hardening

```
Block all public access: enabled on all buckets
Bucket versioning: enabled on bronze zone (immutable raw archive)

Lifecycle rules:
  bronze        → Glacier after 90 days  → delete after 2 years
  silver        → Glacier after 60 days  → delete after 1 year
  gold          → Standard-IA after 30 days → delete after 3 years
  athena-results → delete after 7 days
```

---

## Runbooks

### Runbook 1 — Collector crash

```
Symptom : ECS RunningTaskCount alarm fires. No new records in Kinesis.

1. Check ECS service events in the AWS console.
2. Check CloudWatch Logs /ecs/realtime-media-analytics/collector.
3. Verify Wikimedia SSE endpoint reachable:
   curl -N https://stream.wikimedia.org/v2/stream/recentchange
4. ECS service restart policy restarts the task automatically.
5. If restarts loop → check environment variables and IAM task role.
6. Confirm Kinesis resumes receiving records.

Note: SSE is a live stream. Events missed during downtime are not recoverable.
      S3 archive is unaffected (Firehose buffers independently).
```

### Runbook 2 — Kinesis high iterator age

```
Symptom : IteratorAgeMilliseconds > 60 000 ms. Dashboard lagging.

1. Check Lambda realtime-processor concurrency in CloudWatch.
2. Check ConcurrentExecutions against reserved concurrency limit.
3. Check DynamoDB SystemErrors (throttling).
4. If Lambda throttled → increase reserved concurrency or request quota increase.
5. If DynamoDB throttled → switch to on-demand:
   aws dynamodb update-table \
     --table-name realtime_aggregates \
     --billing-mode PAY_PER_REQUEST
6. If shard count insufficient:
   aws kinesis update-shard-count \
     --stream-name wikimedia-events \
     --target-shard-count 4 \
     --scaling-type UNIFORM_SCALING
   Note: resharding takes ~30 seconds and briefly disrupts consumers.
7. Monitor until IteratorAge < 5 000 ms.
```

### Runbook 3 — Broadcaster errors

```
Symptom : Broadcaster Lambda error alarm. Dashboard stops updating.

1. Check CloudWatch Logs /aws/lambda/broadcaster.
2. Check API Gateway WebSocket endpoint health.
3. Check DynamoDB websocket_connections table accessibility.
4. If GoneException rate is high → stale connections piling up.
   Run a manual scan + delete of items with expired TTL.
5. If postToConnection consistently fails → check API Gateway execution logs.
6. Clients keep their WebSocket connection open but receive no updates.
```

### Runbook 4 — Historical data replay

```
Use case: Glue ETL job failed for a time window. Silver or gold data missing.

1. Identify missing partition: year=2026/month=06/day=11/hour=14
2. Verify bronze data exists in S3 for that partition.
3. Re-run bronze-to-silver job with the specific input partition.
4. Verify silver Parquet output written correctly.
5. Re-run silver-to-gold job for the same window.
6. Partition projection makes Athena aware automatically (no MSCK needed).
7. Trigger QuickSight SPICE incremental refresh.
```

### Runbook 5 — DynamoDB throttling

```
Symptom : DynamoDB SystemErrors alarm. Real-time aggregates lagging.

1. Check consumed write capacity in CloudWatch for both DynamoDB tables.
2. Identify throttled table: realtime_aggregates or websocket_connections.
3. Switch to on-demand if provisioned throughput is exceeded:
   aws dynamodb update-table \
     --table-name realtime_aggregates \
     --billing-mode PAY_PER_REQUEST
4. Monitor consumed capacity.
5. If global counter throttles persist despite write sharding:
   → Increase AGGREGATE_WRITE_SHARDS from 10 to 20.
   → Update Realtime Processor and Broadcaster Lambda configuration.
```