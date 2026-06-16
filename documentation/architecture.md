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
                       │ parse · normalize · raw_event │
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
 │ alert_state        │               │
 └────────┬───────────┘               ▼
          ▼                ┌─────────────────────┐
 ┌────────────────────┐    │ Glue Data Catalog   │
 │ SQS FIFO           │    └──────────┬──────────┘
 │ broadcast signal   │               │
 └────────┬───────────┘               ▼
          ▼                ┌─────────────────────┐
 ┌────────────────────┐    │ Athena              │
 │ Broadcaster Lambda │    └──────────┬──────────┘
 └────────┬───────────┘               │
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
Wikimedia SSE
→ ECS Fargate Collector
→ Kinesis Data Streams
→ Realtime Processor Lambda
→ DynamoDB realtime_aggregates
→ SQS FIFO broadcast signal
→ Broadcaster Lambda
→ API Gateway WebSocket
→ Frontend Dashboard
```

### ECS Fargate Collector

Maintains a persistent SSE connection to Wikimedia. Lambda is excluded because it cannot hold an infinite HTTP connection.

The Collector receives the raw Wikimedia JSON object from each SSE `data:` line, filters invalid or canary events, creates a normalized envelope, embeds the original raw event under `raw_event`, batches records, and writes them to Kinesis.

Internal design:
```
SSE Reader Thread → Normalizer / Validator → In-memory Buffer → Kinesis Sender Thread
```

Flush strategy:
```
Flush when: 100 events accumulated  OR  2 seconds elapsed  OR  shutdown signal
```

Filtering rules:
```
DROP if meta.id is missing
DROP if meta.dt is missing or unparseable
DROP if meta.domain == "canary"
KEEP all five change types: edit, new, categorize, log, external
```

Kinesis record shape:
```
{
  "event_id": "...",
  "occurred_at": "...",
  "payload": { normalized fields },
  "raw_event": { original Wikimedia JSON exactly as received }
}
```

### Kinesis Data Streams

Central fan-out backbone. Receives normalized envelopes from the Collector and serves 3 independent consumers simultaneously.

```
Kinesis ──► Realtime Processor Lambda  (real-time aggregation)
        ──► Firehose Delivery Stream   (archival to S3 Bronze)
        ──► Alert Processor Lambda     (spike detection)
```

Partition key:
```
PutRecords PartitionKey = hash(meta.id)
```

`meta.id` is the globally unique UUID from the Wikimedia Event Platform. Using it as the partition key distributes load evenly and avoids hot shards on high-volume keys like `enwiki:edit`.

### Realtime Processor Lambda

Consumes Kinesis batches. For each normalized envelope:

1. Decode and validate the normalized envelope contract.
2. Read only the stable normalized `payload` for real-time computation.
3. Compute the 1-minute `aggregation_window`.
4. Compute `shard_id = hash(event_id) % AGGREGATE_WRITE_SHARDS` (default: 10).
5. Atomic `UpdateItem ADD` on DynamoDB — no read-modify-write.
6. Write to `METRIC#GLOBAL_ACTIVITY#SHARD#{shard_id}`.
7. Write to `METRIC#WIKI_ACTIVITY#WIKI#{wiki}`.
8. If `namespace = 0`: write to `METRIC#TOP_PAGES#WIKI#{wiki}`.
9. Write to `METRIC#CHANGE_TYPE#TYPE#{change_type}`.
10. Write to `METRIC#NAMESPACE#NS#{namespace}`.
11. Send a deduplicated broadcast signal to SQS FIFO every 5-second broadcast window.

> Log events (`namespace = -1`) are counted in global and wiki activity but excluded from top pages.

### Write Sharding

Global counters are distributed across 10 DynamoDB shards to prevent hot partition throttling.

```
AGGREGATE_WRITE_SHARDS = 10

Write: METRIC#GLOBAL_ACTIVITY#SHARD#{0..9}
Read:  Broadcaster reads all 10 shards and sums them
```

### SQS FIFO Deduplication

SQS FIFO prevents the Broadcaster from being invoked once per Kinesis Lambda invocation.

Aggregation and broadcast use two different time concepts:

```
aggregation_window = 1 minute   # DynamoDB counter window
broadcast_window   = 5 seconds  # dashboard refresh trigger
```

Example:
```
aggregation_window = 2026-06-11T16:44:00Z
broadcast_window   = 2026-06-11T16:44:10Z
```

Message settings:
```
MessageGroupId         = "broadcast-signal"
MessageDeduplicationId = "broadcast-window-{broadcast_window}"
```

This means the Realtime Processor can update the same minute counter continuously, while the Broadcaster pushes a live snapshot of the in-progress minute every 5 seconds.

### Broadcaster Lambda

Triggered by SQS FIFO. For each broadcast signal:

1. Read all 10 global activity shards for the signal's `aggregation_window`.
2. Sum totals and compute `bot_ratio`.
3. Read current minute partial counters and recent completed windows.
4. Scan `websocket_connections`.
5. Filter subscribed topics inside Lambda.
6. Build `stats.update` messages for each requested topic.
7. Call `postToConnection` for each connection.
8. On `GoneException / 410`: delete the stale connection from DynamoDB.

V1 intentionally uses a DynamoDB Scan on `websocket_connections` because subscriptions are stored as a list on the connection item. This is acceptable for portfolio scale. V2 introduces a `websocket_subscriptions` table keyed by topic.

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

Access pattern in V1:
```
Broadcaster → Scan websocket_connections
Broadcaster → filter topics in Lambda
```

V2 scaling option:
```
websocket_subscriptions table
PK = TOPIC#{topic}
SK = CONNECTION#{connection_id}
```

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
PK  = alert_scope   (string)   — "ALERT#GLOBAL", "ALERT#WIKI#{wiki}", "ALERT#LOG_TYPE#{log_type}"
SK  = minute_key    (string)   — "MINUTE#{yyyy-MM-ddTHH:mm}"

TTL = minute_start + 2100 seconds (35 minutes)
```

Stores per-minute event counts for the Alert Processor rolling window.

The Lambda updates the current minute using atomic `UpdateItem ADD`, then reads the last 30 minutes to detect activity spikes via `z_score`.

Tracked counters:
```
event_count
log_count
delete_count
block_count
```

Design note: 35-minute TTL provides 5 minutes of margin beyond the 30-minute rolling window.

---

## Alert Processor

Consumes Kinesis independently. It reads the normalized `payload`, updates DynamoDB `alert_state`, then queries recent state to detect anomalies.

Detections:
- Global event volume spike: `z_score > 2.0` over a 30-minute rolling window.
- Per-wiki event volume spike: `z_score > 2.0` over a 30-minute rolling window.
- Moderation burst: `delete` or `block` count > 3× normal over a 5-minute window.

Write model:
```
UpdateItem ADD event_count, log_count, delete_count, block_count
```

Publishes to SNS on detection.

---

## Architecture Decision Records

### ADR-001 — ECS Fargate for SSE Collector
Lambda is excluded: it has a maximum execution duration and cannot maintain an infinite HTTP connection. ECS Fargate runs a long-lived container with an automatic restart policy managed by the ECS service.

### ADR-002 — Kinesis Data Streams as event backbone
Kinesis provides durable buffering, fan-out to multiple independent consumers, configurable retention, and native Lambda and Firehose integration.

### ADR-003 — hash(meta.id) as Kinesis PartitionKey
`meta.id` is the globally unique UUID from the Wikimedia Event Platform. Hashing it distributes records evenly across shards. Using `wiki` or `change_type` as key would create hot shards: `enwiki` and `edit` dominate the stream volume.

### ADR-004 — DynamoDB for real-time aggregates
DynamoDB provides single-digit millisecond writes, atomic `ADD` counter updates without read-modify-write, and TTL for automatic cleanup. Raw events are not stored in DynamoDB — only aggregated counters needed by the broadcaster and alert state.

### ADR-005 — Write sharding for global counters
A single `METRIC#GLOBAL_ACTIVITY` item receiving all increments from concurrent Lambda invocations would become a hot partition. Distributing writes across 10 shards and merging at read time eliminates throttling at expected throughput.

### ADR-006 — API Gateway WebSocket for live dashboard
The dashboard requires bidirectional communication: the backend pushes `stats.update` snapshots, and the frontend sends `subscribe`/`unsubscribe` messages. SSE only supports server-to-client direction and is therefore excluded.

### ADR-007 — SQS FIFO for 5-second broadcast deduplication
Without deduplication, every Kinesis Lambda invocation would trigger a broadcaster call. SQS FIFO deduplicates broadcast triggers by 5-second `broadcast_window`, while DynamoDB counters remain aggregated by 1-minute `aggregation_window`. This provides a live dashboard feel without excessive fan-out.

### ADR-008 — Normalized envelope + embedded raw_event
The Collector sends a normalized envelope to Kinesis and embeds the original Wikimedia JSON under `raw_event`. Real-time consumers use the stable `payload`; S3 Bronze preserves source fidelity for audit, replay, and schema recovery.

### ADR-009 — Firehose + S3 + Glue + Athena + QuickSight for historical analytics
Firehose is the lowest-overhead archival path from Kinesis: no servers, automatic batching and compression. S3 Parquet with Hive partitioning and Athena partition projection enables cost-efficient SQL without any database to manage. QuickSight connects to Athena for business dashboards.

### ADR-010 — WebSocket subscriptions V1 use Scan
V1 stores topics as a list on each `websocket_connections` item. The Broadcaster scans active connections and filters in Lambda. This is intentionally simple for portfolio scale. V2 introduces a `websocket_subscriptions` table for topic-based Query access.

---

## Scalability Path

### V1 — Portfolio / up to low thousands of concurrent users
```
Single Broadcaster Lambda
SQS FIFO deduplication every 5 seconds
DynamoDB websocket_connections table
Broadcaster Scan + Lambda-side topic filtering
write sharding for global counters (10 shards)
```

### V2 — 100K+ users
```
Add websocket_subscriptions table:
  PK = TOPIC#{topic}
  SK = CONNECTION#{connection_id}

Broadcaster Coordinator Lambda
→ Query subscriptions by topic
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
custom:collector:events_dropped_canary   canary events filtered out
custom:collector:events_sampled_out      events dropped by dev sampling
custom:collector:events_sent_kinesis     total envelopes delivered to Kinesis
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
custom:broadcaster:connections_scanned   active connections scanned
custom:broadcaster:connections_matched   connections matched after topic filtering
custom:broadcaster:messages_sent         WebSocket messages delivered
custom:broadcaster:gone_connections      stale connections removed on GoneException
```

**Alert Processor**
```
custom:alert:state_updates               DynamoDB alert_state updates
custom:alert:z_score                     computed z_score
custom:alert:alerts_published            SNS alerts published
```

### CloudWatch Alarms

| Alarm | Metric | Threshold | Action |

|---|---|---|---|
|Realtime Processor lag | Lambda IteratorAge | > 60 000 ms | SNS
|Alert Processor lag    | Lambda IteratorAge | > 60 000 ms | SNS
|Firehose freshness lag | DeliveryToS3.DataFreshness | > 300 000 ms | SNS
|Kinesis read throttles | ReadProvisionedThroughputExceeded | > 0 | SNS
|Kinesis write throttles| WriteProvisionedThroughputExceeded | > 0 | SNS
| Kinesis iterator age high | `GetRecords.IteratorAgeMilliseconds` | > 60 000 ms | SNS |
| Realtime processor errors | Lambda `Errors` | > 5 in 5 min | SNS |
| Broadcaster errors | Lambda `Errors` | > 5 in 5 min | SNS |
| Collector task stopped | ECS `RunningTaskCount` | < 1 | SNS |
| DynamoDB throttles | `ThrottledRequests` | > 10 in 5 min | SNS |
| SQS queue depth | `ApproximateNumberOfMessagesVisible` | > 100 | SNS |

**Key metric:** `IteratorAgeMilliseconds` = age of the oldest unprocessed record in the shard. Target: < 5 000 ms.
For Lambda consumers, IteratorAge measures the age of the last Kinesis record included in the batch delivered to that specific Lambda event source mapping. It must be monitored per consumer, not only at stream level.
---

## Security

### IAM — Least privilege per component

| Component | Permissions granted |
|---|---|
| ECS Fargate Collector | `kinesis:PutRecord`, `kinesis:PutRecords`, `logs:PutLogEvents`, KMS use for Kinesis |
| Realtime Processor Lambda | `kinesis:GetRecords`, `kinesis:GetShardIterator`, `dynamodb:UpdateItem`, `sqs:SendMessage`, KMS use for Kinesis/DynamoDB/SQS |
| Broadcaster Lambda | `dynamodb:GetItem`, `dynamodb:Scan`, `dynamodb:DeleteItem`, `sqs:ReceiveMessage`, `sqs:DeleteMessage`, `execute-api:ManageConnections`, KMS use for DynamoDB/SQS |
| Connect / Disconnect / Default Lambdas | `dynamodb:PutItem`, `dynamodb:DeleteItem`, `dynamodb:UpdateItem`, `execute-api:ManageConnections` for acknowledgements |
| Alert Processor Lambda | `kinesis:GetRecords`, `kinesis:GetShardIterator`, `dynamodb:GetItem`, `dynamodb:UpdateItem`, `dynamodb:Query`, `sns:Publish`, KMS use for Kinesis/DynamoDB |
| Firehose Delivery Stream | `kinesis:GetRecords`, `s3:PutObject`, KMS use for Kinesis/S3 |
| Glue ETL Jobs | `s3:GetObject`, `s3:PutObject`, Glue Data Catalog access, KMS use for S3 |

### Encryption

| Resource | Encryption |
|---|---|
| Kinesis Data Streams | SSE-KMS with customer-managed key `realtime-media-analytics-{env}-kinesis` |
| Kinesis Firehose | SSE-KMS where supported, using customer-managed key for delivery and S3 writes |
| DynamoDB | Encryption at rest with customer-managed key `realtime-media-analytics-{env}-dynamodb` |
| S3 Data Lake | SSE-KMS with customer-managed key `realtime-media-analytics-{env}-s3` |
| SQS FIFO | SSE-KMS with customer-managed key `realtime-media-analytics-{env}-sqs` |
| CloudWatch Logs | KMS with customer-managed key `realtime-media-analytics-{env}-logs` |
| API Gateway | TLS enforced on all endpoints and WebSocket connections |

### S3 bucket hardening

```
Block all public access: enabled on all buckets
Bucket versioning: enabled on bronze zone

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

Note: Firehose is independent from the Realtime Processor, but not from collector ingestion.
If the Collector is down, both real-time and historical paths miss events during the downtime.
```

### Runbook 2 — Kinesis high iterator age

```
Symptom : IteratorAgeMilliseconds > 60 000 ms. Dashboard lagging.

1. Check Lambda realtime-processor concurrency in CloudWatch.
2. Check ConcurrentExecutions against reserved concurrency limit.
3. Check DynamoDB throttles.
4. If Lambda throttled → increase reserved concurrency or request quota increase.
5. If DynamoDB throttled → switch to on-demand:
   aws dynamodb update-table \
     --table-name realtime_aggregates \
     --billing-mode PAY_PER_REQUEST
6. If shard count insufficient:
   aws kinesis update-shard-count \
     --stream-name realtime-media-analytics-dev-wikimedia-events \
     --target-shard-count 4 \
     --scaling-type UNIFORM_SCALING
7. Monitor until IteratorAge < 5 000 ms.
```

### Runbook 3 — Broadcaster errors

```
Symptom : Broadcaster Lambda error alarm. Dashboard stops updating.

1. Check CloudWatch Logs /aws/lambda/broadcaster.
2. Check API Gateway WebSocket endpoint health.
3. Check DynamoDB websocket_connections table accessibility.
4. If GoneException rate is high → stale connections are piling up.
   Scan and delete expired or stale connection items.
5. If postToConnection consistently fails → check API Gateway execution logs and ManageConnections IAM permissions.
6. Clients should auto-reconnect transparently on frontend side.
```

### Runbook 4 — Historical data replay

```
Use case: Glue ETL job failed for a time window. Silver or gold data missing.

1. Identify missing partition: year=2026/month=06/day=11/hour=14.
2. Verify Bronze envelope data exists in S3 for that partition.
3. Re-run bronze-to-silver job with the specific input partition.
4. Verify silver Parquet output written correctly.
5. Re-run silver-to-gold job for the same window.
6. Partition projection makes Athena aware automatically (no MSCK needed).
7. Trigger QuickSight SPICE incremental refresh.
```

### Runbook 5 — DynamoDB throttling

```
Symptom : DynamoDB throttling alarm. Real-time aggregates lagging.

1. Check consumed write capacity in CloudWatch for realtime_aggregates, websocket_connections, and alert_state.
2. Identify throttled table.
3. Switch to on-demand if provisioned throughput is exceeded:
   aws dynamodb update-table \
     --table-name realtime_aggregates \
     --billing-mode PAY_PER_REQUEST
4. Monitor consumed capacity.
5. If global counter throttles persist despite write sharding:
   → Increase AGGREGATE_WRITE_SHARDS from 10 to 20.
   → Update Realtime Processor and Broadcaster Lambda configuration.
```
