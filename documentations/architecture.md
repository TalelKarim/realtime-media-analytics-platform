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

Maintains a persistent SSE connection to Wikimedia. Lambda is excluded because it cannot hold an indefinite HTTP connection.

The Collector receives the raw Wikimedia JSON object from each SSE `data:` line, validates source metadata, drops canary events, applies deterministic sampling, builds a normalized envelope, embeds the original raw event under `raw_event`, buffers normalized envelopes, and writes batches to Kinesis.

Internal design:

```text
Single long-running event loop
→ read SSE line
→ parse and validate
→ deterministic sampling
→ normalize
→ append to in-memory buffer
→ synchronous PutRecords flush when required
```

Flush strategy:

```text
Flush when: 100 events accumulated OR 2 seconds elapsed OR shutdown signal
```

Filtering rules:

```text
DROP if meta.id is missing
DROP if meta.dt is missing
DROP if meta.domain == "canary"
DO NOT filter by change type; the current source types are edit, new, categorize, log, external
```

Sampling:

```text
SAMPLE_RATE default in development = 0.01
sampling key = normalized event_id
algorithm = deterministic SHA-256 score
```

Kinesis record shape:

```text
{
  "event_id": "wikimedia-{meta.id}",
  "event_type": "wiki.recentchange",
  "occurred_at": "{meta.dt}",
  "payload": { stable normalized fields },
  "raw_event": { original Wikimedia JSON exactly as received },
  "trace_context": { optional W3C trace context injected at flush time }
}
```

### Kinesis Data Streams

Central fan-out backbone. Receives normalized envelopes from the Collector and serves three independent consumers simultaneously.

```text
Kinesis ──► Realtime Processor Lambda  (real-time aggregation)
        ──► Firehose Delivery Stream   (archival to S3 Bronze)
        ──► Alert Processor Lambda     (spike detection)
```

Partition key:

```text
PutRecords PartitionKey = normalized event_id
Example: wikimedia-{meta.id}
```

`meta.id` is a globally unique Wikimedia event UUID. Prefixing it as `wikimedia-{meta.id}` preserves high-cardinality distribution and avoids hot shards on dominant values such as `enwiki` or `edit`.

The Collector creates one OpenTelemetry producer span per `PutRecords` flush. Every record built inside that flush receives the same W3C producer context in the optional `trace_context` envelope field.

### Realtime Processor Lambda

Consumes Kinesis batches. For each batch:

1. Decode base64 JSON records.
2. Accept envelopes where `event_type = wiki.recentchange`, `payload` is an object, and `event_id` is present.
3. Normalize null or malformed optional values defensively.
4. Compute the 1-minute `aggregation_window` from event time.
5. Aggregate records in memory by `(metric_key, window_key)`.
6. Submit atomic DynamoDB `UpdateItem ADD` operations through a bounded thread pool.
7. Use a reusable low-level DynamoDB client and HTTP connection pool.
8. Wait for all submitted writes before sending the broadcast signal.
9. Send one deduplicated SQS FIFO signal per configured 3-second broadcast window after successful DynamoDB writes.

Current bounded-parallel write configuration:

```text
DYNAMODB_WRITE_WORKERS         = 12
DYNAMODB_MAX_POOL_CONNECTIONS = 20
```

Metric families written by the Processor:

```text
METRIC#GLOBAL_ACTIVITY#SHARD#{shard_id}
METRIC#WIKI_ACTIVITY#WIKI#{wiki}
METRIC#TOP_WIKIS#SHARD#{shard_id}
METRIC#CHANGE_TYPE#TYPE#{change_type}
METRIC#WIKI_CHANGE_TYPE#WIKI#{wiki}#TYPE#{change_type}
METRIC#BOT_ACTIVITY#BOT#{true|false}
METRIC#WIKI_BOT_ACTIVITY#WIKI#{wiki}#BOT#{true|false}
METRIC#NAMESPACE#NS#{namespace}
METRIC#WIKI_NAMESPACE#WIKI#{wiki}#NS#{namespace}
METRIC#TOP_PAGES#SHARD#{shard_id}  # namespace 0 only
```

The Processor propagates OpenTelemetry context across worker threads. A Kinesis batch can contain records from multiple Collector flushes: the first unique producer context becomes the direct parent of the Processor span and additional producer contexts are represented as span links.

> Log events (`namespace = -1`) are counted in global, wiki, top-wikis, change-type, bot, and namespace activity but excluded from top pages because top pages only includes `namespace = 0`.

### Write Sharding

Global counters and top read models are distributed across DynamoDB partitions to reduce hot-key pressure and support parallel reads.

```text
GLOBAL_ACTIVITY_SHARD_COUNT = 10
TOP_METRIC_SHARD_COUNT      = 10

Global activity write: METRIC#GLOBAL_ACTIVITY#SHARD#{0..9}
Global activity read : Broadcaster BatchGetItem for all shards and sums event_count

Top wikis write      : METRIC#TOP_WIKIS#SHARD#{0..9}
Top wikis read       : parallel Query across all shards, merge, sort, top N

Top pages write      : METRIC#TOP_PAGES#SHARD#{0..9}
Top pages read       : parallel Query across all shards, merge, sort, top N
```

Per-wiki activity and per-wiki distributions use the wiki code in the partition key because they are read directly for a requested `wiki:{wiki}` topic.

### SQS FIFO Deduplication

SQS FIFO prevents the Broadcaster from being invoked once per Kinesis Lambda invocation.

Aggregation and broadcast use two different time concepts:

```text
aggregation_window = 1 minute   # DynamoDB counter window
broadcast_window   = 3 seconds  # configurable dashboard refresh trigger
```

Example:

```text
aggregation_window = 2026-07-27T16:44:00Z
broadcast_window   = 2026-07-27T16:44:51Z
```

Message settings:

```text
MessageGroupId         = "realtime-broadcast"
MessageDeduplicationId = "BROADCAST#{broadcast_window}"
```

The message body includes the affected aggregation windows and oldest/latest source event timestamps per window. W3C `traceparent`, `tracestate`, and `baggage` are propagated through SQS message attributes when available.

The Realtime Processor can update the same minute counter continuously, while the Broadcaster pushes a current snapshot of the in-progress minute at most once per 3-second window.

### Broadcaster Lambda

Triggered by SQS FIFO with `batch_size = 1`. For each broadcast signal:

1. Extract the W3C trace context from SQS message attributes.
2. Scan `websocket_connections` with a projection containing only `connection_id`, `topics`, and `ttl`.
3. Skip expired connection items and group active connections by topic inside Lambda.
4. Read exact counters with DynamoDB `BatchGetItem`.
5. Query TOP_WIKIS and TOP_PAGES shards in parallel through a bounded DynamoDB read pool.
6. Build `stats.update` messages for `global`, `wiki:{wiki}`, and `top_pages`.
7. Send messages through a bounded `PostToConnection` thread pool.
8. Record freshness after each successful `PostToConnection` using the latest source event timestamp carried by the SQS signal.
9. Delete stale connections after `GoneException / HTTP 410` using bounded-parallel DynamoDB deletes.

Current concurrency defaults:

```text
MAX_POST_WORKERS          = 40
DYNAMODB_READ_WORKERS     = 24
APIGW_MAX_POOL_CONNECTIONS >= 48
DYNAMODB_MAX_POOL_CONNECTIONS >= 32
TRACE_POST_TO_CONNECTION_CALLS = false
```

Per-connection API Gateway SDK spans are disabled by default to avoid high trace volume. The high-level `broadcaster.fanout` span and detailed metrics remain enabled.

V1 intentionally uses a DynamoDB Scan on `websocket_connections` because subscriptions are stored as a list on each connection item. The validated capacity and degradation boundary are established through load testing. V2 replaces the Scan and single broadcaster with topic/shard queries and horizontally scaled fan-out workers.

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

```text
PK  = metric_key   (string)
SK  = window_key   (string)
TTL = now + AGGREGATE_TTL_DAYS
```

Current default:

```text
AGGREGATE_TTL_DAYS = 2
```

Item patterns:

```text
METRIC#GLOBAL_ACTIVITY#SHARD#{0-9}                        / WINDOW#{minute}
METRIC#WIKI_ACTIVITY#WIKI#{wiki}                          / WINDOW#{minute}
METRIC#TOP_WIKIS#SHARD#{0-9}                              / WINDOW#{minute}#WIKI#{wiki}
METRIC#CHANGE_TYPE#TYPE#{type}                            / WINDOW#{minute}
METRIC#WIKI_CHANGE_TYPE#WIKI#{wiki}#TYPE#{type}           / WINDOW#{minute}
METRIC#BOT_ACTIVITY#BOT#{true|false}                      / WINDOW#{minute}
METRIC#WIKI_BOT_ACTIVITY#WIKI#{wiki}#BOT#{true|false}     / WINDOW#{minute}
METRIC#NAMESPACE#NS#{namespace}                           / WINDOW#{minute}
METRIC#WIKI_NAMESPACE#WIKI#{wiki}#NS#{namespace}          / WINDOW#{minute}
METRIC#TOP_PAGES#SHARD#{0-9}                              / WINDOW#{minute}#WIKI#{wiki}#TITLE#{page_hash}
```

Top pages are written only when `namespace = 0`. The real display fields (`wiki`, `title`, `title_url`) are stored as item attributes; the `TITLE#{page_hash}` suffix keeps the key compact and stable.

### Table: alert_state

```
PK  = alert_key   (string)   — "ALERT#GLOBAL", "ALERT#WIKI#{wiki}", "ALERT#LOG_TYPE#delete", "ALERT#LOG_TYPE#block"
SK  = window_key  (string)   — "WINDOW#{yyyy-MM-ddTHH:mm:00Z}"

TTL = window_start + 2100 seconds (35 minutes)
```

Stores short-lived per-minute counters for the Alert Processor rolling window.

The Lambda updates alert counters using atomic `UpdateItem ADD`, then evaluates a completed window against recent historical windows to detect activity spikes via `z_score` or moderation bursts via `burst_ratio`.

Tracked counters:
```
event_count
log_count
delete_count
block_count
```

Alert state fields added only when an alert is published:
```
alert_status       PUBLISHING or SENT
alert_reserved_at  ISO8601 timestamp
alert_sent_at      ISO8601 timestamp
alert_type         GLOBAL_ACTIVITY_SPIKE, WIKI_ACTIVITY_SPIKE, or MODERATION_BURST
current_count      count used for the decision
baseline_avg       historical baseline average
baseline_stddev    historical baseline standard deviation
z_score            computed z-score for global/wiki spikes
threshold          z-score threshold used for the decision
burst_ratio        computed ratio for moderation burst alerts
```

Design note: 35-minute TTL provides 5 minutes of margin beyond the 30-minute rolling window.

---

## Alert Processor

Consumes Kinesis independently. It reads the normalized `payload`, builds alert counters in memory, writes short-lived counters to DynamoDB `alert_state`, evaluates completed windows, deduplicates alert publication, and publishes SNS alerts.

Processing order:

1. Decode and validate Kinesis normalized envelopes.
2. Extract `wiki`, `change_type`, `log_type`, `log_action`, `occurred_at`, and `user_is_bot`.
3. Aggregate counters in memory by `(alert_key, window_key)`.
4. Write counters to `alert_state` using `UpdateItem ADD`.
5. Select the latest completed eligible window using `EVALUATION_DELAY_SECONDS` so the still-open minute is not evaluated too early.
6. Query historical windows from `alert_state`.
7. Skip alert evaluation when there are fewer than `MIN_BASELINE_POINTS` historical points.
8. Detect global and per-wiki spikes using `z_score`.
9. Detect delete/block moderation bursts using a 5-minute rolling window and `burst_ratio`.
10. Before SNS, reserve the alert with `SET alert_status = "PUBLISHING"` only if `attribute_not_exists(alert_status)`.
11. Publish SNS only if reservation succeeds.
12. Mark the item `alert_status = "SENT"` after SNS publish succeeds.

Detections:
- Global event volume spike: `z_score > GLOBAL_Z_THRESHOLD` over a 30-minute rolling window and `current_count >= GLOBAL_MIN_COUNT`.
- Per-wiki event volume spike: `z_score > WIKI_Z_THRESHOLD` over a 30-minute rolling window and `current_count >= WIKI_MIN_COUNT`.
- Moderation burst: `delete` or `block` count > `MODERATION_BURST_RATIO_THRESHOLD × normal` over a 5-minute window and current count above its configured minimum.

Write model:
```
UpdateItem ADD event_count, log_count, delete_count, block_count
SET window_start, last_updated_at, ttl
```

Deduplication model:
```
SET alert_status = "PUBLISHING"
ONLY IF attribute_not_exists(alert_status)

After successful SNS publish:
SET alert_status = "SENT"
SET alert_sent_at = now
```

This guarantees at most one SNS publication per `(alert_key, window_key)`, while counters can continue to increase during the same window.

---

## Architecture Decision Records

### ADR-001 — ECS Fargate for SSE Collector
Lambda is excluded: it has a maximum execution duration and cannot maintain an infinite HTTP connection. ECS Fargate runs a long-lived container with an automatic restart policy managed by the ECS service.

### ADR-002 — Kinesis Data Streams as event backbone
Kinesis provides durable buffering, fan-out to multiple independent consumers, configurable retention, and native Lambda and Firehose integration.

### ADR-003 — normalized event_id as Kinesis PartitionKey
The Collector uses `event_id = "wikimedia-{meta.id}"` directly as the Kinesis partition key. `meta.id` is globally unique and high-cardinality, so the prefixed value distributes records evenly without concentrating traffic on dominant dimensions such as `wiki` or `change_type`.

### ADR-004 — DynamoDB for real-time aggregates
DynamoDB provides single-digit millisecond writes, atomic `ADD` counter updates without read-modify-write, and TTL for automatic cleanup. Raw events are not stored in DynamoDB — only aggregated counters needed by the broadcaster and alert state.

### ADR-005 — Write sharding for global counters
A single `METRIC#GLOBAL_ACTIVITY` item receiving all increments from concurrent Lambda invocations would become a hot partition. Distributing writes across 10 shards and merging at read time eliminates throttling at expected throughput.

### ADR-006 — API Gateway WebSocket for live dashboard
The dashboard requires bidirectional communication: the backend pushes `stats.update` snapshots, and the frontend sends `subscribe`/`unsubscribe` messages. SSE only supports server-to-client direction and is therefore excluded.

### ADR-007 — SQS FIFO for short-window broadcast deduplication
Without deduplication, every Kinesis Lambda invocation would trigger a broadcaster call. SQS FIFO deduplicates triggers by a configurable `broadcast_window`, currently 3 seconds, while DynamoDB counters remain aggregated by a 1-minute `aggregation_window`.

### ADR-008 — Normalized envelope + embedded raw_event
The Collector sends a normalized envelope to Kinesis and embeds the original Wikimedia JSON under `raw_event`. Real-time consumers use the stable `payload`; S3 Bronze preserves source fidelity for audit, replay, and schema recovery.

### ADR-009 — Firehose + S3 + Glue + Athena + QuickSight for historical analytics
Firehose is the lowest-overhead archival path from Kinesis: no servers, automatic batching and compression. S3 Parquet with Hive partitioning and Athena partition projection enables cost-efficient SQL without any database to manage. QuickSight connects to Athena for business dashboards.

### ADR-010 — WebSocket subscriptions V1 use Scan
V1 stores topics as a list on each `websocket_connections` item. The Broadcaster scans active connections and filters in Lambda. This is intentionally simple for portfolio scale. V2 introduces a `websocket_subscriptions` table for topic-based Query access.

---

## Scalability Path

### V1 — Current implementation

```text
Single Broadcaster Lambda
SQS FIFO deduplication every 3 seconds
Single MessageGroupId: realtime-broadcast
DynamoDB websocket_connections table
Broadcaster Scan + Lambda-side topic filtering
Bounded-parallel DynamoDB reads and PostToConnection calls
Write sharding for global and top read models
```

Validated baseline:

```text
500 concurrent WebSocket connections subscribed to global
500 successful PostToConnection calls in the measured broadcast
No durable SQS backlog during the validated run
```

This is a validated baseline, not the final capacity limit. Additional tests increase connection count progressively and document the highest sustainable level while maintaining freshness and avoiding durable backlog.

### V2 — Sharded fan-out

```text
Add websocket_subscriptions table:
  PK = TOPIC#{topic}#SHARD#{shard_id}
  SK = CONNECTION#{connection_id}

Snapshot Coordinator Lambda
→ builds one current payload per topic
→ creates one job per topic/shard

SQS fan-out queue
→ multiple MessageGroupIds
→ parallel worker consumption

Broadcast Worker Lambda
→ Query subscriptions by topic/shard
→ bounded-parallel PostToConnection
→ latest-state-wins for stale jobs
```

### V3 — Managed or persistent realtime fleet

```text
AWS IoT Core pub/sub
AWS AppSync subscriptions
ECS/EKS persistent WebSocket fleet
Managed realtime platform where appropriate
```

## Observability

Grafana Cloud is the central operational interface:

```text
Grafana Cloud
├── Mimir  : metrics
├── Loki   : logs
├── Tempo  : traces
└── Grafana dashboards and alerting
```

### Telemetry paths

Collector:

```text
ECS Collector OTel SDK
→ local Grafana Alloy sidecar
→ Grafana Cloud OTLP endpoint
```

Realtime Processor and Broadcaster:

```text
Lambda OTel SDK
→ local OpenTelemetry Collector Lambda Extension
→ Grafana Cloud OTLP endpoint
```

Structured application logs continue through CloudWatch Logs and are forwarded to Loki. AWS managed-service metrics remain native in CloudWatch and are queried from Grafana through the AWS integration.

### Distributed tracing

```text
Collector flush producer span
→ trace_context embedded in each Kinesis record
→ Realtime Processor parent context + span links for additional producers
→ traceparent/tracestate/baggage in SQS MessageAttributes
→ Broadcaster Lambda
→ API Gateway Management API
```

The trace represents technical causality and processing latency. It does not provide exact event-to-snapshot lineage because the Broadcaster reads shared DynamoDB aggregates that can contain contributions from multiple Processor invocations.

### Realtime Processor metrics

```text
realtime_processor_batches_total
realtime_processor_records_received_total
realtime_processor_records_decoded_total
realtime_processor_records_valid_total
realtime_processor_records_skipped_total
realtime_processor_records_failed_total
dynamodb_aggregate_updates_total
dynamodb_aggregate_update_failure_total
dynamodb_update_batch_duration_ms
broadcast_signals_sent_total
broadcast_signal_failure_total
broadcast_signal_skipped_total
broadcast_signal_duration_ms
processor_batch_duration_ms
```

### Broadcaster metrics

```text
websocket_post_success_total
websocket_post_failure_total
websocket_connection_gone_total
websocket_messages_sent_total
broadcast_completed_total
broadcast_failed_total
broadcast_duration_ms
websocket_post_duration_ms
event_to_dashboard_latency_ms
oldest_event_to_dashboard_latency_ms
active_connections_scanned
connections_scan_duration_ms
aggregate_reads_duration_ms
payload_build_duration_ms
fanout_duration_ms
fanout_batch_size
gone_cleanup_duration_ms
```

### Primary operational signals

```text
Realtime freshness p50/p95/p99
Freshness compliance ratio below 10 seconds
WebSocket delivery success ratio
Kinesis IteratorAge
SQS ApproximateAgeOfOldestMessage
SQS visible message count
Lambda duration, errors, throttles, and concurrency
DynamoDB throttled requests
API Gateway callback errors
OTel export and collector health
Historical Silver/Gold freshness
```

### CloudWatch Log Groups

```text
/ecs/realtime-media-analytics/collector
/aws/lambda/realtime-processor
/aws/lambda/broadcaster
/aws/lambda/websocket-connect
/aws/lambda/websocket-disconnect
/aws/lambda/websocket-default
/aws/lambda/alert-processor
```

Native CloudWatch alarms remain a minimal independent safety layer for critical AWS health signals.

## Security

### IAM — Least privilege per component

| Component | Permissions granted |
|---|---|
| ECS Fargate Collector | `kinesis:PutRecord`, `kinesis:PutRecords`, `logs:PutLogEvents`, KMS use for Kinesis |
| Realtime Processor Lambda | `kinesis:GetRecords`, `kinesis:GetShardIterator`, `dynamodb:UpdateItem`, `sqs:SendMessage`, KMS use for Kinesis/DynamoDB/SQS |
| Broadcaster Lambda | `dynamodb:GetItem`, `dynamodb:BatchGetItem`, `dynamodb:Query`, `dynamodb:Scan`, `dynamodb:DeleteItem`, `sqs:ReceiveMessage`, `sqs:DeleteMessage`, `execute-api:ManageConnections`, KMS use for DynamoDB/SQS |
| Connect / Disconnect / Default Lambdas | `dynamodb:PutItem`, `dynamodb:DeleteItem`, `dynamodb:UpdateItem`, `execute-api:ManageConnections` for acknowledgements |
| Alert Processor Lambda | `kinesis:GetRecords`, `kinesis:GetShardIterator`, `dynamodb:GetItem`, `dynamodb:UpdateItem`, `dynamodb:Query`, `sns:Publish`, KMS use for Kinesis/DynamoDB/SNS |
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
   → Increase `GLOBAL_ACTIVITY_SHARD_COUNT` if the global counter family is hot.
   → Increase `TOP_METRIC_SHARD_COUNT` if TOP_WIKIS or TOP_PAGES read models are hot.
   → Update Realtime Processor and Broadcaster configuration together.
```
