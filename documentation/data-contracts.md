# Data Contracts — Realtime Media Analytics Platform

This document is the source of truth for the platform data contracts.


Schema reference:

```text
https://github.com/wikimedia/mediawiki-event-schemas/blob/master/jsonschema/mediawiki/recentchange/current.yaml
```

Important source-schema rule:

> The official Wikimedia schema uses `additionalProperties: true` and only guarantees the `meta` object. All other fields are optional. Every component must handle absent fields gracefully.

---

## Contract 1 — Wikimedia Raw SSE Event

**Producer:** Wikimedia EventStreams  
**Consumer:** ECS Fargate Collector

```text
Protocol : HTTPS / Server-Sent Events
Encoding : text/event-stream, UTF-8
Format   : one JSON object per "data:" line
Schema   : /mediawiki/recentchange/1.0.0
```

The raw event is the JSON object contained in the SSE `data:` line.

The Collector receives it directly from:

```text
https://stream.wikimedia.org/v2/stream/recentchange
```

The Collector does not fetch raw events from another API.

### Event types

Wikimedia recentchange emits five main change types:

| Type | Approx. frequency | Notes |
|---|---:|---|
| `edit` | ~60-65% | Article, file, or page modification |
| `categorize` | ~25-30% | Category membership change, usually namespace 14 |
| `new` | ~5-8% | New page creation |
| `log` | ~5-8% | Administrative action, usually namespace -1 |
| `external` | <1% | External system change |

### Important raw fields

```text
meta.id        globally unique event UUID
meta.dt        event timestamp, ISO8601
meta.domain    Wikimedia domain
id             recentchange id, not globally unique across wikis
type           edit/new/categorize/log/external
wiki           wiki code, for example enwiki, frwiki, commonswiki
namespace      namespace id
title          page or object title
user           user name
bot            boolean
log_type       only for log events, optional
log_action     only for log events, optional
log_params     only for log events, optional
```

Known `log_type` values include:

```text
delete
restore
block
unblock
protect
upload
move
import
patrol
rights
newusers
merge
suppress
tag
```

### Field presence matrix

```text
Field                edit   new    categorize  log    external
────────────────────────────────────────────────────────────────
meta.id (UUID)        ✅     ✅      ✅          ✅      ✅
meta.dt (ISO8601)     ✅     ✅      ✅          ✅      ✅
meta.domain           ✅     ✅      ✅          ✅      ✅
meta.topic            ✅     ✅      ✅          ✅      ✅
meta.partition        ✅     ✅      ✅          ✅      ✅
meta.offset           ✅     ✅      ✅          ✅      ✅
id (rcid)             ✅     ✅      ✅          ✅      ✅
type                  ✅     ✅      ✅          ✅      ✅
namespace             ✅     ✅      ✅          ✅      ✅
title                 ✅     ✅      ✅          ✅      ✅
comment               ✅     ✅      ✅          ✅      ✅
parsedcomment         ⚠️     ⚠️      ⚠️          ⚠️      ⚠️
timestamp (Unix)      ✅     ✅      ✅          ✅      ✅
user                  ✅     ✅      ✅          ✅      ✅
bot                   ✅     ✅      ✅          ✅      ✅
server_url            ✅     ✅      ✅          ✅      ✅
server_name           ✅     ✅      ✅          ✅      ✅
server_script_path    ✅     ✅      ✅          ✅      ✅
wiki                  ✅     ✅      ✅          ✅      ✅
title_url             ⚠️     ⚠️      ⚠️          ⚠️      ⚠️
minor                 ⚠️     ⚠️      ❌          ❌      ❌
patrolled             ⚠️     ⚠️      ❌          ❌      ❌
length.old            ⚠️     ⚠️      ❌          ❌      ❌
length.new            ⚠️     ⚠️      ❌          ❌      ❌
revision.old          ⚠️     ⚠️      ❌          ❌      ❌
revision.new          ⚠️     ⚠️      ❌          ❌      ❌
log_id                ❌     ❌      ❌          ⚠️      ❌
log_type              ❌     ❌      ❌          ⚠️      ❌
log_action            ❌     ❌      ❌          ⚠️      ❌
log_params            ❌     ❌      ❌          ⚠️      ❌
log_action_comment    ❌     ❌      ❌          ⚠️      ❌
```

Legend:

```text
✅ usually present
⚠️ optional / config-dependent
❌ absent for this type
```

### Collector filtering rules

```text
DROP if meta.id is missing
DROP if meta.dt is missing
DROP if meta.domain == "canary"
DO NOT filter by change type; the current source types are edit, new, categorize, log, external
```

---

## Contract 2 — Kinesis Normalized Envelope

**Producer:** ECS Fargate Collector  
**Consumers:** Realtime Processor Lambda, Alert Processor Lambda, Firehose Delivery Stream

The Collector transforms every valid and sampled Wikimedia raw event into a normalized envelope.

The envelope contains:

```text
payload        normalized fields used by internal consumers
raw_event      original Wikimedia JSON exactly as received
trace_context  optional W3C propagation metadata added when the Kinesis record is built
```

### Kinesis partition key

```text
PartitionKey = normalized event_id
Example: wikimedia-{meta.id}
```

The partition key is a `PutRecords` request parameter and is not duplicated as a separate JSON field.

### Envelope schema

```json
{
  "event_id": "wikimedia-{meta.id}",
  "event_type": "wiki.recentchange",
  "event_version": "1.0",
  "source": "wikimedia.eventstreams",
  "occurred_at": "{meta.dt}",
  "ingested_at": "{collector ingestion time}",
  "correlation_id": "{collector generated UUID}",
  "payload": {
    "wiki": "{wiki or null}",
    "domain": "{meta.domain or null}",
    "stream": "{meta.stream or null}",
    "request_id": "{meta.request_id or null}",
    "topic": "{meta.topic or null}",
    "partition": "{meta.partition or null}",
    "offset": "{meta.offset or null}",
    "change_type": "{type or null}",
    "namespace": "{namespace or null}",
    "title": "{title or null}",
    "title_url": "{title_url or null}",
    "user": "{user or null}",
    "bot": "{bot or null}",
    "minor": "{minor or null}",
    "patrolled": "{patrolled or null}",
    "comment": "{comment or null}",
    "parsedcomment": "{parsedcomment or null}",
    "source_timestamp": "{timestamp or null}",
    "revision_old": "{revision.old or null}",
    "revision_new": "{revision.new or null}",
    "length_old": "{length.old or null}",
    "length_new": "{length.new or null}",
    "length_delta": "{length.new - length.old or null}",
    "change_url": "{notify_url or title_url or meta.uri or null}",
    "server_url": "{server_url or null}",
    "server_name": "{server_name or null}",
    "server_script_path": "{server_script_path or null}"
  },
  "raw_event": {
    "...": "original Wikimedia event"
  },
  "trace_context": {
    "traceparent": "00-...-...-01",
    "tracestate": "optional",
    "baggage": "optional"
  }
}
```

`trace_context` is optional. It is infrastructure metadata, not business data. All Kinesis records created inside the same Collector flush producer span receive the same W3C producer context.

### Field mapping

| Envelope field | Source / rule |
|---|---|
| `event_id` | `"wikimedia-" + meta.id` |
| `event_type` | constant `wiki.recentchange` |
| `event_version` | constant `1.0` |
| `source` | constant `wikimedia.eventstreams` |
| `occurred_at` | `meta.dt` |
| `ingested_at` | Collector UTC ingestion timestamp |
| `correlation_id` | Collector-generated UUID per normalized event |
| `payload.wiki` | `wiki` |
| `payload.domain` | `meta.domain` |
| `payload.stream` | `meta.stream` |
| `payload.request_id` | `meta.request_id` |
| `payload.topic` | `meta.topic` |
| `payload.partition` | `meta.partition` |
| `payload.offset` | `meta.offset` |
| `payload.change_type` | `type` |
| `payload.namespace` | `namespace` |
| `payload.title` | `title` |
| `payload.title_url` | `title_url` |
| `payload.user` | `user` |
| `payload.bot` | `bot` |
| `payload.minor` | `minor` |
| `payload.patrolled` | `patrolled` |
| `payload.comment` | `comment` |
| `payload.parsedcomment` | `parsedcomment` |
| `payload.source_timestamp` | `timestamp` |
| `payload.revision_old` | `revision.old` |
| `payload.revision_new` | `revision.new` |
| `payload.length_old` | `length.old` |
| `payload.length_new` | `length.new` |
| `payload.length_delta` | `length.new - length.old`, null unless both are integers |
| `payload.change_url` | `notify_url` OR `title_url` OR `meta.uri` |
| `raw_event` | full source JSON object |
| `trace_context` | optional W3C carrier injected at Kinesis flush time |

Fields such as `log_type`, `log_action`, `log_params`, `id`, and other source-only values remain available under `raw_event` even when they are not duplicated into `payload`.

### Realtime Processor acceptance rules

The Realtime Processor accepts a record when:

```text
event_type == wiki.recentchange
payload is a JSON object
event_id is present
```

Optional values are normalized defensively:

```text
occurred_at malformed or absent → processing time fallback
wiki absent                    → "unknown"
change_type absent             → "unknown"
title absent                   → "unknown"
bot absent                     → false
```

The Processor accepts both `payload.user_is_bot` and `payload.bot` for backward compatibility.

### Alert Processor fields

Alert-specific values that are not present in the normalized payload remain available in `raw_event`, including:

```text
log_type
log_action
log_params
```

The Alert Processor contract must remain null-safe because all non-`meta` source fields are optional.

## Contract 3 — DynamoDB `realtime_aggregates`

**Producer:** Realtime Processor Lambda  
**Consumer:** Broadcaster Lambda

This table is the source of truth for the live dashboard read model. It stores short-lived 1-minute counters.

The Realtime Processor aggregates Kinesis records in memory and then executes atomic `UpdateItem ADD` operations through a bounded thread pool.

### Table keys

```text
PK = metric_key
SK = window_key
```

### Common attributes

```json
{
  "metric_key": "METRIC#...",
  "window_key": "WINDOW#2026-07-27T16:44:00Z",
  "event_count": 1,
  "window_start": "2026-07-27T16:44:00Z",
  "last_updated_at": "2026-07-27T16:44:53.747000Z",
  "ttl": 1785343493
}
```

Naming rule:

```text
event_count is singular
```

Window rule:

```text
window_key always starts with WINDOW#{yyyy-MM-ddTHH:mm:00Z}
```

TTL rule:

```text
ttl = processing time + AGGREGATE_TTL_DAYS
AGGREGATE_TTL_DAYS default = 2
```

### 3a — Global activity

```text
metric_key = METRIC#GLOBAL_ACTIVITY#SHARD#{0..9}
window_key = WINDOW#{minute}
shard_id   = hash(event_id) % GLOBAL_ACTIVITY_SHARD_COUNT
```

The Broadcaster reads all configured shards with `BatchGetItem` and sums `event_count`.

### 3b — Wiki activity

```text
metric_key = METRIC#WIKI_ACTIVITY#WIKI#{wiki}
window_key = WINDOW#{minute}
```

Used for the `wiki:{wiki}` topic activity counter.

### 3c — Top wikis read model

```text
metric_key = METRIC#TOP_WIKIS#SHARD#{0..9}
window_key = WINDOW#{minute}#WIKI#{wiki}
shard_id   = hash(wiki) % TOP_METRIC_SHARD_COUNT
```

The Broadcaster queries all shards in parallel, merges counts by wiki, sorts descending, and returns the configured top N.

### 3d — Global change-type distribution

```text
metric_key = METRIC#CHANGE_TYPE#TYPE#{change_type}
window_key = WINDOW#{minute}
```

### 3e — Per-wiki change-type distribution

```text
metric_key = METRIC#WIKI_CHANGE_TYPE#WIKI#{wiki}#TYPE#{change_type}
window_key = WINDOW#{minute}
```

Used by `wiki:{wiki}` messages.

### 3f — Global bot/human distribution

```text
METRIC#BOT_ACTIVITY#BOT#true  / WINDOW#{minute}
METRIC#BOT_ACTIVITY#BOT#false / WINDOW#{minute}
```

```text
bot_ratio = bot_count / (bot_count + human_count)
```

### 3g — Per-wiki bot/human distribution

```text
METRIC#WIKI_BOT_ACTIVITY#WIKI#{wiki}#BOT#true  / WINDOW#{minute}
METRIC#WIKI_BOT_ACTIVITY#WIKI#{wiki}#BOT#false / WINDOW#{minute}
```

### 3h — Global namespace distribution

```text
metric_key = METRIC#NAMESPACE#NS#{namespace}
window_key = WINDOW#{minute}
```

Common namespace values:

```text
-1  Special/log
0   Article
1   Talk
2   User
4   Project
6   File
10  Template
14  Category
```

### 3i — Per-wiki namespace distribution

```text
metric_key = METRIC#WIKI_NAMESPACE#WIKI#{wiki}#NS#{namespace}
window_key = WINDOW#{minute}
```

### 3j — Top pages read model

Only namespace `0` events are counted.

```text
metric_key    = METRIC#TOP_PAGES#SHARD#{0..9}
window_key    = WINDOW#{minute}#WIKI#{wiki}#TITLE#{page_hash}
page_identity = wiki + "#" + sanitized_title
page_hash     = sha256(page_identity)[:12]
shard_id      = hash(page_identity) % TOP_METRIC_SHARD_COUNT
```

Stored attributes include:

```text
wiki
title
title_url
namespace
last_change_type
last_seen_at
window_start
last_updated_at
ttl
```

The Broadcaster queries all TOP_PAGES shards in parallel, merges results, sorts by `event_count`, and returns the configured top N.

## Contract 4 — SQS FIFO Broadcast Signal

**Producer:** Realtime Processor Lambda  
**Consumer:** Broadcaster Lambda

The Realtime Processor sends a signal after successful DynamoDB counter updates.

### Message body

```json
{
  "message_type": "aggregates.updated",
  "source": "realtime-processor",
  "created_at": "2026-07-27T16:44:53.814000Z",
  "broadcast_window": "2026-07-27T16:44:51Z",
  "aggregation_windows": [
    "2026-07-27T16:44:00Z"
  ],
  "event_timestamp_bounds_by_window": {
    "2026-07-27T16:44:00Z": {
      "oldest_event_timestamp_ms": 1785170692652,
      "latest_event_timestamp_ms": 1785170693422
    }
  },
  "oldest_event_timestamp_ms": 1785170692652,
  "latest_event_timestamp_ms": 1785170693422
}
```

`aggregation_windows` is a list because one Kinesis Lambda batch can contain records from more than one minute.

`event_timestamp_bounds_by_window` is the preferred source for freshness calculations. The top-level oldest/latest values are retained as a fallback and logging shortcut.

### Message attributes

When a valid OpenTelemetry context is active, the Processor injects:

```text
traceparent
tracestate
baggage
```

The Broadcaster extracts these attributes and continues the distributed trace.

### FIFO settings

```text
MessageGroupId         = realtime-broadcast
MessageDeduplicationId = BROADCAST#{broadcast_window}
```

Example:

```text
MessageGroupId         = realtime-broadcast
MessageDeduplicationId = BROADCAST#2026-07-27T16:44:51Z
```

### Time concepts

```text
aggregation_window = 1-minute DynamoDB counter window
broadcast_window   = configurable dashboard trigger, currently 3 seconds
```

Several Processor invocations can update DynamoDB during the same broadcast window. SQS FIFO accepts at most one signal with the same deduplication ID during its deduplication interval.

## Contract 5 — WebSocket `stats.update`

**Producer:** Broadcaster Lambda  
**Consumer:** Frontend Dashboard

The Broadcaster reads current DynamoDB aggregates, builds a topic payload, and sends it to matching WebSocket connections.

Freshness metadata is carried in each payload:

```text
latest_event_timestamp_ms
oldest_event_timestamp_ms
server_send_attempt_at_ms
```

`server_send_attempt_at_ms` is added immediately before each individual `PostToConnection` call, so it can differ between clients receiving the same logical update.

### Global topic message

```json
{
  "type": "stats.update",
  "topic": "global",
  "timestamp": "2026-07-27T16:44:53Z",
  "aggregation_window": "2026-07-27T16:44:00Z",
  "broadcast_window": "2026-07-27T16:44:51Z",
  "is_partial_window": true,
  "latest_event_timestamp_ms": 1785170693422,
  "oldest_event_timestamp_ms": 1785170692652,
  "server_send_attempt_at_ms": 1785170693812,
  "data": {
    "current_minute_events_so_far": 220,
    "bot_count": 80,
    "human_count": 140,
    "bot_ratio": 0.3636,
    "top_wikis": [
      { "wiki": "commonswiki", "count": 90 },
      { "wiki": "enwiki", "count": 65 },
      { "wiki": "frwiki", "count": 18 }
    ],
    "change_types": {
      "edit": 120,
      "new": 10,
      "categorize": 70,
      "log": 18,
      "external": 2
    },
    "namespace_distribution": {
      "0": 90,
      "6": 30,
      "14": 70,
      "-1": 18
    },
    "top_pages": [
      {
        "wiki": "enwiki",
        "title": "Scale AI",
        "count": 6,
        "url": "https://en.wikipedia.org/wiki/Scale_AI",
        "title_url": "https://en.wikipedia.org/wiki/Scale_AI",
        "namespace": "0",
        "last_change_type": "edit",
        "last_seen_at": "2026-07-27T16:44:52Z"
      }
    ]
  }
}
```

### Wiki topic message

```json
{
  "type": "stats.update",
  "topic": "wiki:frwiki",
  "timestamp": "2026-07-27T16:44:53Z",
  "aggregation_window": "2026-07-27T16:44:00Z",
  "broadcast_window": "2026-07-27T16:44:51Z",
  "is_partial_window": true,
  "latest_event_timestamp_ms": 1785170693422,
  "oldest_event_timestamp_ms": 1785170692652,
  "server_send_attempt_at_ms": 1785170693812,
  "data": {
    "wiki": "frwiki",
    "current_minute_events_so_far": 18,
    "bot_count": 4,
    "human_count": 14,
    "bot_ratio": 0.2222,
    "top_wikis": [],
    "change_types": {
      "edit": 12,
      "new": 1,
      "categorize": 3,
      "log": 2
    },
    "namespace_distribution": {
      "0": 10,
      "14": 3,
      "-1": 2
    },
    "top_pages": []
  }
}
```

### Top-pages topic message

```json
{
  "type": "stats.update",
  "topic": "top_pages",
  "timestamp": "2026-07-27T16:44:53Z",
  "aggregation_window": "2026-07-27T16:44:00Z",
  "broadcast_window": "2026-07-27T16:44:51Z",
  "is_partial_window": true,
  "latest_event_timestamp_ms": 1785170693422,
  "oldest_event_timestamp_ms": 1785170692652,
  "server_send_attempt_at_ms": 1785170693812,
  "data": {
    "current_minute_events_so_far": 15,
    "top_pages": []
  }
}
```

Dashboard interpretation:

```text
current_minute_events_so_far  increases while the minute is open
is_partial_window             true while the aggregation minute is open
broadcast_window              dashboard trigger bucket, not an aggregation bucket
latest_event_timestamp_ms     source event time used for the primary freshness SLI
server_send_attempt_at_ms     server timestamp immediately before postToConnection
```

---

## Contract 6 — WebSocket client messages

**Producer:** Frontend Dashboard  
**Consumer:** WebSocket Default Handler Lambda

Supported messages:

```json
{ "action": "subscribe", "topic": "global" }
{ "action": "subscribe", "topic": "wiki:frwiki" }
{ "action": "subscribe", "topic": "top_pages" }
{ "action": "unsubscribe", "topic": "wiki:frwiki" }
```

Valid topics:

```text
global
wiki:{wiki_code}
top_pages
```

---

## Contract 7 — WebSocket acknowledgements

**Producer:** WebSocket Default Handler Lambda  
**Consumer:** Frontend Dashboard

```json
{ "type": "subscription.ack", "topic": "wiki:frwiki", "status": "subscribed" }
{ "type": "subscription.ack", "topic": "wiki:enwiki", "status": "unsubscribed" }
{ "type": "error", "message": "Unsupported topic" }
{ "type": "error", "message": "Invalid action" }
```

---

## Contract 8 — DynamoDB `websocket_connections`

**Producers:** WebSocket Connect and Default Handler Lambdas  
**Consumers:** Broadcaster and Disconnect Handler Lambdas

### Table key

```text
PK = connection_id
```

### Item shape

```json
{
  "connection_id": "Mn2Pc9dfPHcCEug=",
  "connected_at": "2026-07-27T16:44:00Z",
  "client_type": "dashboard",
  "topics": [
    "global",
    "wiki:frwiki"
  ],
  "ttl": 1785177840
}
```

TTL:

```text
ttl = connected_at + 2 hours
```

### V1 access pattern

```text
Broadcaster Scan with ProjectionExpression: connection_id, topics, ttl
Broadcaster skips expired items
Broadcaster groups connections by topic inside Lambda
```

### V2 scaling option

```text
websocket_subscriptions table
PK = TOPIC#{topic}#SHARD#{shard_id}
SK = CONNECTION#{connection_id}
```

V2 replaces the table Scan with topic/shard Query operations and horizontally scaled fan-out workers.

## Contract 9 — DynamoDB `alert_state`

**Producer:** Alert Processor Lambda  
**Consumer:** Alert Processor Lambda

The Alert Processor has its own table and must not depend on `realtime_aggregates`.

It consumes Kinesis directly, builds short-lived alert counters, and detects anomalies.

### Table keys

```text
PK = alert_key
SK = window_key
```

### Common item shape

```json
{
  "alert_key": "ALERT#GLOBAL",
  "window_key": "WINDOW#2026-06-24T13:30:00Z",
  "window_start": "2026-06-24T13:30:00Z",
  "event_count": 1200,
  "log_count": 30,
  "delete_count": 8,
  "block_count": 2,
  "last_updated_at": "2026-06-24T13:30:42Z",
  "ttl": 1782471042
}
```

TTL:

```text
ttl = window_start + 35 minutes
```

35 minutes gives 30 minutes of rolling baseline plus 5 minutes of safety margin.

DynamoDB TTL is asynchronous. Consumers must filter by `window_key` range and must not rely on exact deletion timing.

### Alert scopes

Global activity:

```text
ALERT#GLOBAL
```

Per-wiki activity:

```text
ALERT#WIKI#{wiki}
```

Moderation actions:

```text
ALERT#LOG_TYPE#delete
ALERT#LOG_TYPE#block
```

### Write model

The Alert Processor aggregates Kinesis records in memory, then writes atomic updates:

```text
UpdateItem ADD event_count, log_count, delete_count, block_count
SET window_start, last_updated_at, ttl
```

It writes one item per `(alert_key, window_key)` touched by the batch.

### Global spike detection

```text
Query previous 30 completed 1-minute windows for ALERT#GLOBAL.
Compute average, standard deviation, and z_score.
Trigger if z_score > 2.0 and current_count >= configured minimum.
```

### Per-wiki spike detection

```text
Query previous 30 completed 1-minute windows for ALERT#WIKI#{wiki}.
Compute average, standard deviation, and z_score.
Trigger if z_score > 2.0 and current_count >= configured minimum.
```

### Moderation burst detection

```text
Query current 5-minute delete/block activity.
Compare to normal baseline.
Trigger if current_5m_count > 3 × normal and current_5m_count >= configured minimum.
```

### Alert deduplication

When an alert is detected, the Lambda must reserve the item before publishing SNS:

```text
SET alert_status = "PUBLISHING"
ONLY IF attribute_not_exists(alert_status)
```

After SNS publish succeeds:

```text
SET alert_status = "SENT"
SET alert_sent_at = now
SET z_score = computed value
SET baseline_avg = computed value
SET baseline_stddev = computed value
```

Example after alert publication:

```json
{
  "alert_key": "ALERT#GLOBAL",
  "window_key": "WINDOW#2026-06-24T13:30:00Z",
  "window_start": "2026-06-24T13:30:00Z",
  "event_count": 1500,
  "baseline_avg": 900,
  "baseline_stddev": 200,
  "z_score": 3.0,
  "alert_status": "SENT",
  "alert_sent_at": "2026-06-24T13:30:46Z",
  "last_updated_at": "2026-06-24T13:30:46Z",
  "ttl": 1782471042
}
```

The item remains in DynamoDB until TTL deletes it.

Counters may continue to increase during the same minute, but SNS must be published only once per `(alert_key, window_key)`.

---

## Contract 10 — SNS alert message

**Producer:** Alert Processor Lambda  
**Consumer:** Platform Engineer / Email / SMS

### Global activity spike

```json
{
  "message_type": "realtime.alert.triggered",
  "alert_type": "GLOBAL_ACTIVITY_SPIKE",
  "severity": "warning",
  "alert_key": "ALERT#GLOBAL",
  "window_key": "WINDOW#2026-06-24T13:30:00Z",
  "window_start": "2026-06-24T13:30:00Z",
  "current_count": 1500,
  "baseline_avg": 900,
  "baseline_stddev": 200,
  "z_score": 3.0,
  "threshold": 2.0,
  "created_at": "2026-06-24T13:30:46Z"
}
```

### Per-wiki activity spike

```json
{
  "message_type": "realtime.alert.triggered",
  "alert_type": "WIKI_ACTIVITY_SPIKE",
  "severity": "warning",
  "wiki": "commonswiki",
  "alert_key": "ALERT#WIKI#commonswiki",
  "window_key": "WINDOW#2026-06-24T13:30:00Z",
  "window_start": "2026-06-24T13:30:00Z",
  "current_count": 720,
  "baseline_avg": 300,
  "baseline_stddev": 100,
  "z_score": 4.2,
  "threshold": 2.0,
  "created_at": "2026-06-24T13:30:46Z"
}
```

### Moderation burst

```json
{
  "message_type": "realtime.alert.triggered",
  "alert_type": "MODERATION_BURST",
  "severity": "warning",
  "log_type": "delete",
  "alert_key": "ALERT#LOG_TYPE#delete",
  "window_key": "WINDOW#2026-06-24T13:30:00Z",
  "window_start": "2026-06-24T13:30:00Z",
  "current_5m_count": 25,
  "baseline_5m_avg": 6,
  "burst_ratio": 4.17,
  "threshold_ratio": 3.0,
  "created_at": "2026-06-24T13:30:46Z"
}
```

---

## Contract 11 — S3 Bronze

**Producer:** Kinesis Firehose  
**Consumer:** Glue Bronze-to-Silver ETL

Bronze stores the normalized Kinesis envelope, including `payload` and `raw_event`.

```text
Format      : JSON Lines
Compression : GZIP
Source      : Kinesis normalized envelope
Purpose     : source-fidelity archive and replay base
```

Path:

```text
s3://{bucket}/bronze/wikimedia/recentchange/
  year=YYYY/month=MM/day=DD/hour=HH/
```

Bronze is not raw-only. It stores the envelope produced by the Collector.

The original Wikimedia event remains available inside `raw_event`.

---

## Contract 12 — S3 Silver

**Producer:** Glue Bronze-to-Silver ETL  
**Consumers:** Athena, Glue Silver-to-Gold ETL

Silver is cleaned, typed, columnar data derived from the current Bronze envelope.

```text
Format      : Parquet
Compression : SNAPPY
Partition   : ingestion_date
```

Output fields:

```text
event_id
occurred_at
ingestion_date
wiki
domain
change_type
namespace
title
title_url
user
user_is_bot
is_minor
is_patrolled
old_length
new_length
delta_bytes
revision_old
revision_new
change_url
raw_notify_url
log_type
log_action
log_params
wikimedia_rcid
```

Current Bronze-to-Silver source mapping:

```text
user_is_bot     ← payload.bot
is_minor        ← payload.minor
is_patrolled    ← payload.patrolled
old_length      ← payload.length_old
new_length      ← payload.length_new
delta_bytes     ← payload.length_delta
revision_old    ← payload.revision_old
revision_new    ← payload.revision_new
change_url      ← payload.change_url
raw_notify_url  ← raw_event.notify_url
log_type        ← raw_event.log_type
log_action      ← raw_event.log_action
log_params      ← raw_event.log_params serialized as JSON
wikimedia_rcid  ← raw_event.id
```

Silver does not preserve the complete `raw_event`; Bronze remains the source-fidelity archive.

## Contract 13 — S3 Gold

**Producer:** Glue Silver-to-Gold ETL  
**Consumers:** Athena, QuickSight

Gold contains pre-aggregated analytical datasets.

Planned datasets:

```text
top_wikis_by_hour
top_pages_by_day
bot_vs_human_by_hour
change_type_distribution
activity_spikes
```

### top_wikis_by_hour

```text
wiki
hour_window
event_count
bot_count
human_count
edit_count
new_count
categorize_count
log_count
external_count
```

### top_pages_by_day

```text
wiki
title
day
event_count
last_change_type
last_seen_at
```

Filter:

```text
namespace = 0
```

### bot_vs_human_by_hour

```text
hour_window
bot_count
human_count
total_count
bot_ratio
```

### change_type_distribution

```text
change_type
hour_window
event_count
```

### activity_spikes

```text
hour_window
wiki
event_count
z_score
is_spike
```

Gold `activity_spikes` is historical analytics. It is independent from the real-time Alert Processor SNS flow.

---

## Critical filtering and processing rules

### Collector

```text
DROP    if meta.id is missing
DROP    if meta.dt is missing
DROP    if meta.domain == "canary"
SAMPLE  deterministically with SHA-256(event_id) and SAMPLE_RATE
KEEP    all five change types
EMBED   raw_event exactly as received
BUFFER  normalized envelopes
INJECT  optional W3C trace_context when building records for a flush
WRITE   Kinesis PartitionKey = normalized event_id
```

### Realtime Processor

```text
READ      Kinesis normalized envelope
ACCEPT    event_type == wiki.recentchange, payload object, event_id present
FALLBACK  malformed occurred_at to processing time
NORMALIZE absent optional fields defensively
AGGREGATE records in memory by metric_key/window_key
WRITE     DynamoDB UpdateItem ADD with bounded parallelism
WRITE     global and per-wiki activity/distributions
WRITE     top_pages only when namespace == 0
WAIT      for all DynamoDB writes before sending the signal
SEND      SQS FIFO broadcast signal after successful DynamoDB writes
PROPAGATE W3C trace context into SQS message attributes
```

### Broadcaster

```text
READ     SQS broadcast signal and W3C message attributes
SCAN     websocket_connections with projected attributes in V1
FILTER   expired items and group subscriptions inside Lambda
READ     exact counters with BatchGetItem
QUERY    TOP_WIKIS and TOP_PAGES shards concurrently
BUILD    global, wiki, and top_pages payloads
PUSH     with bounded-parallel PostToConnection calls
MEASURE  freshness after every successful server-side post
DELETE   stale connections on GoneException / 410
```

### Alert Processor

```text
READ   Kinesis normalized envelope directly
READ   source-only log fields from raw_event when required
WRITE  alert_state with UpdateItem ADD
TRACK  event_count, log_count, delete_count, block_count
QUERY  30-minute rolling window for global/wiki spikes
QUERY  5-minute rolling window for delete/block bursts
PUBLISH SNS only after conditional dedup reservation
```

### Glue Silver ETL

```text
READ      Bronze Contract 2 envelope
DROP      if event_id is null
DROP      if occurred_at is null or unparseable
KEEP      all five change types
MAP       current payload field names to stable Silver column names
READ      source-only fields from raw_event
CAST      bot/minor/patrolled values to booleans
USE       payload.length_delta for delta_bytes
SERIALIZE raw_event.log_params as JSON string
SELECT    known output columns only
PRESERVE  complete raw_event in Bronze only
```

