# Data Contracts — Realtime Media Analytics Platform V2

This document is the contract source of truth for the active code in the uploaded V2 branch. All timestamps are UTC. Epoch values are milliseconds unless explicitly stated.

## 1. Contract conventions

| Convention | Meaning |
|---|---|
| `schema_version` | Integer version of an internal persisted/message contract |
| `event_id` | Stable normalized Wikimedia identity, prefixed `wikimedia-` |
| `occurred_at` | Source event time from Wikimedia `meta.dt` |
| `ingested_at` | Collector processing time |
| `sequence` | 3-second broadcast cursor, not a source event timestamp |
| `aggregation_window` | 60-second event-time window |
| `connection_shard` | Logical fan-out partition such as `SHARD#03` |
| `ttl` | DynamoDB expiry epoch in seconds |

## 2. Wikimedia SSE input

Required fields used by the Collector:

```json
{
  "meta": {
    "id": "unique-source-id",
    "dt": "2026-07-31T16:00:00.500Z",
    "domain": "fr.wikipedia.org",
    "stream": "mediawiki.recentchange"
  },
  "wiki": "frwiki",
  "type": "edit",
  "namespace": 0,
  "title": "Example",
  "user": "ExampleUser",
  "bot": false,
  "timestamp": 1785513600
}
```

Validation:

- `meta` must be an object;
- `meta.id` and `meta.dt` must be present;
- `meta.domain == canary` is dropped;
- deterministic sampling is calculated from `wikimedia-{meta.id}`.

## 3. Normalized Kinesis envelope

Producer: Collector. Consumers: Realtime Processor, Alert Processor and Firehose.

```json
{
  "event_id": "wikimedia-<meta.id>",
  "event_type": "wiki.recentchange",
  "event_version": "1.0",
  "source": "wikimedia.eventstreams",
  "occurred_at": "2026-07-31T16:00:00.500Z",
  "ingested_at": "2026-07-31T16:00:00.720Z",
  "correlation_id": "uuid-v4",
  "payload": {
    "wiki": "frwiki",
    "domain": "fr.wikipedia.org",
    "stream": "mediawiki.recentchange",
    "request_id": null,
    "topic": null,
    "partition": null,
    "offset": null,
    "change_type": "edit",
    "namespace": 0,
    "title": "Example",
    "title_url": "https://...",
    "user": "ExampleUser",
    "bot": false,
    "minor": false,
    "patrolled": true,
    "comment": "...",
    "parsedcomment": "...",
    "source_timestamp": 1785513600,
    "revision_old": 1,
    "revision_new": 2,
    "length_old": 100,
    "length_new": 130,
    "length_delta": 30,
    "change_url": "https://...",
    "server_url": "https://fr.wikipedia.org",
    "server_name": "fr.wikipedia.org",
    "server_script_path": "/w"
  },
  "raw_event": {},
  "trace_context": {
    "traceparent": "00-...-...-01",
    "tracestate": "optional",
    "baggage": "optional"
  }
}
```

Kinesis `PartitionKey` is `event_id`.

## 4. `realtime_aggregates` table

Key schema:

```text
PK metric_key : String
SK window_key : String
```

Common attributes:

```json
{
  "metric_key": "...",
  "window_key": "WINDOW#2026-07-31T16:00:00Z...",
  "event_count": 123,
  "window_start": "2026-07-31T16:00:00Z",
  "last_updated_at": "2026-07-31T16:00:03Z",
  "ttl": 1785686400
}
```

`event_count` is modified with atomic DynamoDB `ADD`.

### 4.1 Global activity

```text
PK METRIC#GLOBAL_ACTIVITY#SHARD#<0..9>
SK WINDOW#<minute-iso>
```

### 4.2 Wiki activity

```text
PK METRIC#WIKI_ACTIVITY#WIKI#<wiki>
SK WINDOW#<minute-iso>
```

Additional attribute: `wiki`.

### 4.3 Top wikis

```text
PK METRIC#TOP_WIKIS#SHARD#<0..9>
SK WINDOW#<minute-iso>#WIKI#<wiki>
```

### 4.4 Change types

```text
PK METRIC#CHANGE_TYPE#TYPE#<type>
SK WINDOW#<minute-iso>

PK METRIC#WIKI_CHANGE_TYPE#WIKI#<wiki>#TYPE#<type>
SK WINDOW#<minute-iso>
```

Supported configured types: `edit`, `new`, `categorize`, `log`, `external`.

### 4.5 Bot/human

```text
PK METRIC#BOT_ACTIVITY#BOT#true|false
SK WINDOW#<minute-iso>

PK METRIC#WIKI_BOT_ACTIVITY#WIKI#<wiki>#BOT#true|false
SK WINDOW#<minute-iso>
```

### 4.6 Namespaces

```text
PK METRIC#NAMESPACE#NS#<namespace>
SK WINDOW#<minute-iso>

PK METRIC#WIKI_NAMESPACE#WIKI#<wiki>#NS#<namespace>
SK WINDOW#<minute-iso>
```

Configured read list: `-1,0,1,2,4,6,10,14`.

### 4.7 Top pages

Only namespace `0` is included.

```text
PK METRIC#TOP_PAGES#SHARD#<0..9>
SK WINDOW#<minute-iso>#WIKI#<wiki>#TITLE#<hash>
```

Attributes include:

```json
{
  "wiki": "frwiki",
  "title": "Example",
  "title_url": "https://...",
  "namespace": "0",
  "last_change_type": "edit",
  "last_seen_at": "2026-07-31T16:00:00Z"
}
```

## 5. Broadcast signal: `aggregates.updated`

Queue: `broadcast-signal.fifo`.

```json
{
  "schema_version": 3,
  "message_type": "aggregates.updated",
  "signal_id": "BROADCAST#1785513600000#<32-char-hash>",
  "source": "realtime-processor",
  "created_at": "2026-07-31T16:00:02Z",
  "sequence": 1785513600000,
  "broadcast_window": "2026-07-31T16:00:00Z",
  "aggregation_windows": [
    "2026-07-31T16:00:00Z"
  ],
  "updated_topics": [
    "global",
    "top_pages",
    "wiki:frwiki"
  ],
  "event_timestamp_bounds_by_window": {
    "2026-07-31T16:00:00Z": {
      "oldest_event_timestamp_ms": 1785513600100,
      "latest_event_timestamp_ms": 1785513601800
    }
  },
  "event_timestamp_bounds_by_topic_by_window": {
    "2026-07-31T16:00:00Z": {
      "global": {
        "oldest_event_timestamp_ms": 1785513600100,
        "latest_event_timestamp_ms": 1785513601800
      },
      "wiki:frwiki": {
        "oldest_event_timestamp_ms": 1785513600200,
        "latest_event_timestamp_ms": 1785513601500
      },
      "top_pages": {
        "oldest_event_timestamp_ms": 1785513600300,
        "latest_event_timestamp_ms": 1785513601400
      }
    }
  },
  "oldest_event_timestamp_ms": 1785513600100,
  "latest_event_timestamp_ms": 1785513601800
}
```

SQS attributes:

```text
MessageGroupId = realtime-broadcast
MessageDeduplicationId = signal_id
```

Deduplication material:

```text
sequence | sorted aggregation_windows | sorted updated_topics
```

W3C context can be included in SQS MessageAttributes.

## 6. Coordinator idempotency item

Table: `broadcast_snapshots`.

```json
{
  "snapshot_id": "IDEMPOTENCY#<sha256>",
  "topic": "COORDINATOR",
  "item_type": "IDEMPOTENCY",
  "schema_version": 3,
  "source_signal_id": "BROADCAST#...",
  "sequence": 1785513600000,
  "status": "PROCESSING|COMPLETED|FAILED",
  "lease_owner": "lambda-request-id",
  "lease_expires_at": 1785513660,
  "manifest_ids": ["MANIFEST#..."],
  "created_at": "...",
  "updated_at": "...",
  "ttl": 1785514500
}
```

A conditional put/update prevents concurrent duplicate processing and allows takeover after an expired lease.

## 7. Topic snapshot item

Table: `broadcast_snapshots`.

```json
{
  "snapshot_id": "SNAPSHOT#<sequence>#WINDOW#<epoch>#SIGNAL#<token>",
  "topic": "global",
  "item_type": "SNAPSHOT",
  "schema_version": 3,
  "sequence": 1785513600000,
  "aggregation_window": "2026-07-31T16:00:00Z",
  "aggregation_window_epoch_ms": 1785513600000,
  "broadcast_window": "2026-07-31T16:00:00Z",
  "source_signal_id": "BROADCAST#...",
  "source_signal_created_at": "2026-07-31T16:00:02Z",
  "created_at": "2026-07-31T16:00:03Z",
  "latest_event_timestamp_ms": 1785513601800,
  "oldest_event_timestamp_ms": 1785513600100,
  "payload": {
    "current_minute_events_so_far": 500,
    "bot_count": 100,
    "human_count": 400,
    "top_wikis": [],
    "change_types": [],
    "namespaces": [],
    "top_pages": []
  },
  "ttl": 1785514503
}
```

Topic-specific payloads:

- `global`: total, bot/human, top wikis, change types, namespaces and top pages;
- `wiki:<wiki>`: wiki-specific total/distributions/pages and empty `top_wikis`;
- `top_pages`: total count derived from returned pages and `top_pages` list.

## 8. Manifest item

```json
{
  "snapshot_id": "MANIFEST#<sequence>#WINDOW#<epoch>#SIGNAL#<token>",
  "topic": "MANIFEST",
  "item_type": "MANIFEST",
  "schema_version": 3,
  "sequence": 1785513600000,
  "aggregation_window": "2026-07-31T16:00:00Z",
  "aggregation_window_epoch_ms": 1785513600000,
  "broadcast_window": "2026-07-31T16:00:00Z",
  "source_signal_id": "BROADCAST#...",
  "updated_topics": ["global", "wiki:frwiki"],
  "snapshots": {
    "global": {
      "snapshot_id": "SNAPSHOT#...",
      "topic": "global",
      "oldest_event_timestamp_ms": 1785513600100,
      "latest_event_timestamp_ms": 1785513601800
    },
    "wiki:frwiki": {
      "snapshot_id": "SNAPSHOT#...",
      "topic": "wiki:frwiki",
      "oldest_event_timestamp_ms": 1785513600200,
      "latest_event_timestamp_ms": 1785513601500
    }
  },
  "created_at": "2026-07-31T16:00:03Z",
  "ttl": 1785514503
}
```

## 9. LATEST pointer

```json
{
  "snapshot_id": "LATEST",
  "topic": "MANIFEST",
  "item_type": "POINTER",
  "schema_version": 3,
  "sequence": 1785513600000,
  "manifest_id": "MANIFEST#...",
  "aggregation_window": "2026-07-31T16:00:00Z",
  "aggregation_window_epoch_ms": 1785513600000,
  "source_signal_id": "BROADCAST#...",
  "updated_at": "2026-07-31T16:00:03Z"
}
```

Update rule:

```text
accept a greater sequence
or the same sequence with an equal/greater aggregation_window_epoch_ms
```

Workers read this item strongly consistently.

## 10. Broadcast shard job

Queue: `broadcast-jobs.fifo`.

```json
{
  "schema_version": 3,
  "message_type": "broadcast.shard.job",
  "broadcast_id": "MANIFEST#...",
  "manifest_id": "MANIFEST#...",
  "sequence": 1785513600000,
  "shard_id": 3,
  "connection_shard": "SHARD#03",
  "aggregation_window": "2026-07-31T16:00:00Z",
  "aggregation_window_epoch_ms": 1785513600000,
  "broadcast_window": "2026-07-31T16:00:00Z",
  "created_at_ms": 1785513603200,
  "created_at": "2026-07-31T16:00:03Z"
}
```

SQS attributes:

```text
MessageGroupId = connection_shard
MessageDeduplicationId = SHA256(manifest_id | connection_shard)
```

## 11. WebSocket connection item

Table: `websocket_connections`.

```json
{
  "connection_id": "api-gateway-connection-id",
  "connected_at": "2026-07-31T16:00:00Z",
  "client_type": "dashboard",
  "topics": ["global", "wiki:frwiki"],
  "subscription_shard": 3,
  "connection_shard": "SHARD#03",
  "ttl": 1785520800
}
```

Shard calculation is deterministic from `connection_id` and `CONNECTION_SHARD_COUNT`.

GSI:

```text
connection-shard-index
PK connection_shard
SK connection_id
projection ALL
```

## 12. Transitional subscription item

The uploaded branch still writes this compatibility item. It is not required for V2 recipient discovery.

```json
{
  "topic_shard": "TOPIC#wiki:frwiki#SHARD#03",
  "connection_id": "...",
  "topic": "wiki:frwiki",
  "shard_id": 3,
  "connected_at": "...",
  "ttl": 1785520800
}
```

Final V2 cleanup should remove this table and dual-write contract.

## 13. Client subscribe/unsubscribe commands

```json
{
  "action": "subscribe",
  "topic": "wiki:frwiki"
}
```

```json
{
  "action": "unsubscribe",
  "topic": "wiki:frwiki"
}
```

Allowed topics:

```text
global
top_pages
wiki:<2 to 80 lowercase alphanumeric, underscore or hyphen characters>
```

`global` is always retained. Maximum configured topics per connection: 50.

## 14. Subscription acknowledgement

```json
{
  "type": "subscription.ack",
  "topic": "wiki:frwiki",
  "status": "subscribed|unsubscribed|already_subscribed|not_subscribed"
}
```

Invalid inputs return a WebSocket message such as:

```json
{
  "type": "error",
  "message": "Invalid action|Unsupported topic|Invalid JSON message"
}
```

## 15. Worker topic update

A topic update inside a batch:

```json
{
  "topic": "global",
  "data": {
    "current_minute_events_so_far": 500,
    "bot_count": 100,
    "human_count": 400,
    "top_wikis": [],
    "change_types": [],
    "namespaces": [],
    "top_pages": []
  },
  "latest_event_timestamp_ms": 1785513601800,
  "oldest_event_timestamp_ms": 1785513600100
}
```

## 16. WebSocket batch envelope

```json
{
  "type": "stats.batch_update",
  "schema_version": 3,
  "sequence": 1785513600000,
  "manifest_id": "MANIFEST#...",
  "aggregation_window": "2026-07-31T16:00:00Z",
  "aggregation_window_epoch_ms": 1785513600000,
  "chunk_index": 0,
  "chunk_count": 1,
  "updates": [
    {
      "topic": "global",
      "data": {},
      "latest_event_timestamp_ms": 1785513601800,
      "oldest_event_timestamp_ms": 1785513600100
    }
  ],
  "server_send_attempt_at_ms": 1785513606800
}
```

`server_send_attempt_at_ms` is inserted immediately before each `postToConnection` attempt. It is not the success time used by the backend freshness metric.

Payload limit behavior:

```text
MAX_WEBSOCKET_PAYLOAD_BYTES = 30000
CHUNK_SIZE_SAFETY_BYTES = 512
```

A single update too large to fit by itself is a structural payload build failure.

## 17. Frontend broadcast cursor

```typescript
interface BroadcastCursor {
  sequence: number;
  aggregationWindowEpochMs: number;
  manifestId?: string;
}
```

Comparison order:

```text
sequence
then aggregationWindowEpochMs
```

The frontend rejects only a strictly older cursor. Equal cursors are accepted so all chunks of one manifest can be processed.

## 18. Alert state item

Key schema:

```text
PK alert_key
SK window_key
```

Examples:

```text
ALERT#GLOBAL
ALERT#WIKI#frwiki
ALERT#LOG_TYPE#delete
ALERT#LOG_TYPE#block
```

```json
{
  "alert_key": "ALERT#GLOBAL",
  "window_key": "WINDOW#2026-07-31T16:00:00Z",
  "event_count": 100,
  "delete_count": 0,
  "block_count": 0,
  "last_updated_at": "...",
  "ttl": 1785515700,
  "alert_status": "PUBLISHING|SENT|FAILED",
  "alert_sent_at": "..."
}
```

Not every attribute is present on every item.

## 19. Bronze data contract

Bronze stores the normalized Kinesis envelope as JSON Lines GZIP without removing `raw_event`.

Prefix:

```text
bronze/wikimedia/recentchange/year=YYYY/month=MM/day=DD/hour=HH/
```

## 20. Silver contract

Silver is typed Parquet/SNAPPY with selected normalized columns. It excludes the complete nested `raw_event` from the analytical table while Bronze remains the source-fidelity archive.

Partition:

```text
ingestion_date=YYYY-MM-DD
```

## 21. Gold contracts

The Glue job produces business datasets including:

```text
top_wikis_by_hour
bot_vs_human_by_hour
change_type_distribution
top_pages_by_day
activity_spikes
```

Exact columns are defined by `terraform/modules/glue_etl/scripts/silver_to_gold.py` and the Glue Catalog resources.

## 22. Trace propagation contract

```text
Collector envelope.trace_context
→ Kinesis batch extraction/links
→ Realtime Processor active span
→ SQS MessageAttributes
→ Coordinator span
→ broadcast job MessageAttributes
→ Worker process_shard span
```

A Kinesis Lambda batch may contain records from multiple producer traces. The first unique valid context is used as parent and additional contexts are attached as span links.
