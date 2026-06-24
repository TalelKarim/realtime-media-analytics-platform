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
DROP if meta.dt is missing or unparseable
DROP if meta.domain == "canary"
KEEP all five change types: edit, new, categorize, log, external
```

---

## Contract 2 — Kinesis Normalized Envelope

**Producer:** ECS Fargate Collector  
**Consumers:** Realtime Processor Lambda, Alert Processor Lambda, Firehose Delivery Stream

The Collector transforms every valid Wikimedia raw event into a normalized envelope.

The envelope intentionally contains both:

```text
payload    stable normalized fields used by real-time consumers
raw_event  original Wikimedia JSON exactly as received from the SSE data line
```

This gives the project a stable real-time contract while preserving source fidelity in S3 Bronze.

### Kinesis partition key

```text
PartitionKey = hash(meta.id)
```

The Kinesis partition key is a PutRecords call parameter. It is not stored as a JSON field.

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
    "wikimedia_recentchange_id": "{id}",
    "wiki": "{wiki}",
    "domain": "{meta.domain}",
    "change_type": "{type}",
    "namespace": "{namespace}",
    "title": "{title}",
    "title_url": "{title_url or null}",
    "user": "{user}",
    "user_is_bot": "{bot boolean}",
    "is_minor": "{minor or null}",
    "is_patrolled": "{patrolled or null}",
    "old_length": "{length.old or null}",
    "new_length": "{length.new or null}",
    "delta_bytes": "{new_length - old_length or null}",
    "revision_old": "{revision.old or null}",
    "revision_new": "{revision.new or null}",
    "change_url": "{best effort URL or null}",
    "raw_notify_url": "{notify_url or null}",
    "log_type": "{log_type or null}",
    "log_action": "{log_action or null}",
    "log_params": "{log_params or null}"
  },
  "raw_event": {
    "...": "original Wikimedia event"
  }
}
```

### Field mapping

| Normalized field | Source field | Rule |
|---|---|---|
| `event_id` | `meta.id` | `"wikimedia-" + meta.id` |
| `occurred_at` | `meta.dt` | ISO8601 |
| `wikimedia_recentchange_id` | `id` | integer, per-wiki only |
| `wiki` | `wiki` | lower-case when possible |
| `domain` | `meta.domain` | null-safe |
| `change_type` | `type` | `edit`, `new`, `categorize`, `log`, `external` |
| `namespace` | `namespace` | may be -1, 0, 14, etc. |
| `title` | `title` | null-safe |
| `title_url` | `title_url` | null if absent |
| `user` | `user` | null-safe |
| `user_is_bot` | `bot` | cast boolean |
| `is_minor` | `minor` | null for non-edit/non-new types |
| `is_patrolled` | `patrolled` | null if absent |
| `old_length` | `length.old` | null if absent |
| `new_length` | `length.new` | null if absent |
| `delta_bytes` | computed | `new_length - old_length`, null if either absent |
| `revision_old` | `revision.old` | null if absent |
| `revision_new` | `revision.new` | null if absent |
| `change_url` | computed | `notify_url` OR diff URL OR `title_url` OR null |
| `raw_notify_url` | `notify_url` | null if absent |
| `log_type` | `log_type` | null for non-log types |
| `log_action` | `log_action` | null for non-log types |
| `log_params` | `log_params` | null for non-log; may be array, object, or string |
| `raw_event` | full raw event | original Wikimedia JSON object |

### Required fields for Realtime Processor

Realtime Processor requires:

```text
event_id
event_type = wiki.recentchange
occurred_at
payload.wiki
payload.change_type
payload.namespace
payload.title
payload.user_is_bot or payload.bot
```

Events missing required fields are skipped.

### Required fields for Alert Processor

Alert Processor requires:

```text
event_id
event_type = wiki.recentchange
occurred_at
payload.wiki
payload.change_type
payload.log_type
payload.log_action
payload.user_is_bot
```

Optional / used only for log detection 
payload.log_type
payload.log_action
---

## Contract 3 — DynamoDB `realtime_aggregates`

**Producer:** Realtime Processor Lambda  
**Consumer:** Broadcaster Lambda

This contract is the source of truth for the live dashboard read model.

The table stores short-lived, 1-minute real-time counters.

All writes are atomic `UpdateItem ADD` operations.

The Realtime Processor must aggregate records in memory first, then update one item per touched `(metric_key, window_key)` pair.

### Table keys

```text
PK = metric_key
SK = window_key
```

### Common attributes

All current metric items use this common shape:

```json
{
  "metric_key": "METRIC#...",
  "window_key": "WINDOW#2026-06-24T13:30:00Z",
  "event_count": 1,
  "window_start": "2026-06-24T13:30:00Z",
  "last_updated_at": "2026-06-24T13:30:42.123456Z",
  "ttl": 1782480642
}
```

Naming rule:

```text
event_count is singular.
Do not use events_count.
```

Window key rule:

```text
window_key always starts with WINDOW#{yyyy-MM-ddTHH:mm:00Z}
```

TTL rule:

```text
ttl = now + AGGREGATE_TTL_DAYS
```

Current default:

```text
AGGREGATE_TTL_DAYS = 2
```

---

### 3a — Global activity

Metric family:

```text
METRIC#GLOBAL_ACTIVITY#SHARD#{0..9}
```

Example:

```json
{
  "metric_key": "METRIC#GLOBAL_ACTIVITY#SHARD#2",
  "window_key": "WINDOW#2026-06-24T13:30:00Z",
  "event_count": 120,
  "window_start": "2026-06-24T13:30:00Z",
  "last_updated_at": "2026-06-24T13:30:42.123456Z",
  "ttl": 1782480642
}
```

Write rule:

```text
shard_id = hash(event_id) % GLOBAL_ACTIVITY_SHARD_COUNT
```

Read rule:

```text
Read all GLOBAL_ACTIVITY shards for the requested window.
Sum event_count across shards.
```

Purpose:

```text
Global platform activity count for the live dashboard.
```

---

### 3b — Wiki activity by known wiki

Metric family:

```text
METRIC#WIKI_ACTIVITY#WIKI#{wiki}
```

Example:

```json
{
  "metric_key": "METRIC#WIKI_ACTIVITY#WIKI#frwiki",
  "window_key": "WINDOW#2026-06-24T13:30:00Z",
  "event_count": 18,
  "wiki": "frwiki",
  "window_start": "2026-06-24T13:30:00Z",
  "last_updated_at": "2026-06-24T13:30:42.123456Z",
  "ttl": 1782480642
}
```

Purpose:

```text
Read activity for a specific wiki topic, for example wiki:frwiki.
```

Important distinction:

```text
This metric is not used to compute top N wikis globally.
Top N wikis are served by the TOP_WIKIS read model.
```

---

### 3c — Top wikis read model

Metric family:

```text
METRIC#TOP_WIKIS#SHARD#{0..9}
```

Example:

```json
{
  "metric_key": "METRIC#TOP_WIKIS#SHARD#4",
  "window_key": "WINDOW#2026-06-24T13:30:00Z#WIKI#frwiki",
  "event_count": 18,
  "wiki": "frwiki",
  "window_start": "2026-06-24T13:30:00Z",
  "last_updated_at": "2026-06-24T13:30:42.123456Z",
  "ttl": 1782480642
}
```

Write rule:

```text
shard_id = hash(wiki) % TOP_METRIC_SHARD_COUNT
```

Read rule:

```text
For each TOP_WIKIS shard:
  Query metric_key = METRIC#TOP_WIKIS#SHARD#{n}
  KeyCondition: begins_with(window_key, "WINDOW#{minute}#WIKI#")

Then merge all shard results, sort by event_count descending, and return top N.
```

Purpose:

```text
Efficient live dashboard top_wikis without scanning WIKI_ACTIVITY partitions.
```

---

### 3d — Change type distribution

Metric family:

```text
METRIC#CHANGE_TYPE#TYPE#{change_type}
```

Possible values:

```text
edit
new
categorize
log
external
unknown
```

Example:

```json
{
  "metric_key": "METRIC#CHANGE_TYPE#TYPE#categorize",
  "window_key": "WINDOW#2026-06-24T13:30:00Z",
  "event_count": 35,
  "change_type": "categorize",
  "window_start": "2026-06-24T13:30:00Z",
  "last_updated_at": "2026-06-24T13:30:42.123456Z",
  "ttl": 1782480642
}
```

Purpose:

```text
Count events by Wikimedia change type per 1-minute window.
```

---

### 3e — Bot activity

Metric families:

```text
METRIC#BOT_ACTIVITY#BOT#true
METRIC#BOT_ACTIVITY#BOT#false
```

Example:

```json
{
  "metric_key": "METRIC#BOT_ACTIVITY#BOT#false",
  "window_key": "WINDOW#2026-06-24T13:30:00Z",
  "event_count": 72,
  "is_bot": false,
  "window_start": "2026-06-24T13:30:00Z",
  "last_updated_at": "2026-06-24T13:30:42.123456Z",
  "ttl": 1782480642
}
```

Read rule:

```text
bot_count   = event_count for METRIC#BOT_ACTIVITY#BOT#true
human_count = event_count for METRIC#BOT_ACTIVITY#BOT#false
bot_ratio   = bot_count / (bot_count + human_count)
```

---

### 3f — Namespace distribution

Metric family:

```text
METRIC#NAMESPACE#NS#{namespace}
```

Example:

```json
{
  "metric_key": "METRIC#NAMESPACE#NS#0",
  "window_key": "WINDOW#2026-06-24T13:30:00Z",
  "event_count": 42,
  "namespace": "0",
  "window_start": "2026-06-24T13:30:00Z",
  "last_updated_at": "2026-06-24T13:30:42.123456Z",
  "ttl": 1782480642
}
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

Purpose:

```text
Namespace distribution for the live dashboard.
```

---

### 3g — Top pages read model

Metric family:

```text
METRIC#TOP_PAGES#SHARD#{0..9}
```

Only namespace `0` events are counted.

Example:

```json
{
  "metric_key": "METRIC#TOP_PAGES#SHARD#7",
  "window_key": "WINDOW#2026-06-24T13:30:00Z#WIKI#enwiki#TITLE#b7e4a91c8d75",
  "event_count": 6,
  "wiki": "enwiki",
  "title": "Scale AI",
  "title_url": "https://en.wikipedia.org/wiki/Scale_AI",
  "namespace": "0",
  "last_change_type": "edit",
  "last_seen_at": "2026-06-24T13:30:42.123456Z",
  "window_start": "2026-06-24T13:30:00Z",
  "last_updated_at": "2026-06-24T13:30:42.123456Z",
  "ttl": 1782480642
}
```

Write rule:

```text
Only write when namespace == 0.
page_identity = wiki + "#" + sanitized_title
page_hash = sha256(page_identity)[:12]
shard_id = hash(page_identity) % TOP_METRIC_SHARD_COUNT
```

Read rule:

```text
For each TOP_PAGES shard:
  Query metric_key = METRIC#TOP_PAGES#SHARD#{n}
  KeyCondition: begins_with(window_key, "WINDOW#{minute}#")

Then merge all shard results, sort by event_count descending, and return top N.
```

Purpose:

```text
Live dashboard top_pages without relying on S3/Athena historical analytics.
```

---

## Contract 4 — SQS FIFO Broadcast Signal

**Producer:** Realtime Processor Lambda  
**Consumer:** Broadcaster Lambda

The Realtime Processor sends a broadcast signal after successful DynamoDB counter updates.

### Message body

```json
{
  "message_type": "aggregates.updated",
  "source": "realtime-processor",
  "created_at": "2026-06-24T13:30:42Z",
  "broadcast_window": "2026-06-24T13:30:40Z",
  "aggregation_windows": [
    "2026-06-24T13:30:00Z"
  ]
}
```

`aggregation_windows` is a list because one Lambda batch can contain events from more than one 1-minute window.

### Current FIFO settings

```text
MessageGroupId         = realtime-broadcast
MessageDeduplicationId = BROADCAST#{broadcast_window}
```

Example:

```text
MessageGroupId         = realtime-broadcast
MessageDeduplicationId = BROADCAST#2026-06-24T13:30:40Z
```

### Time concepts

```text
aggregation_window = 1-minute DynamoDB counter window
broadcast_window   = 5-second dashboard refresh trigger
```

The Realtime Processor updates minute counters continuously.

The Broadcaster should push a live snapshot every 5 seconds.

---

## Contract 5 — WebSocket `stats.update`

**Producer:** Broadcaster Lambda  
**Consumer:** Frontend Dashboard

The Broadcaster reads `realtime_aggregates`, builds a snapshot, and sends it to matching WebSocket connections.

### Global topic message

```json
{
  "type": "stats.update",
  "topic": "global",
  "timestamp": "2026-06-24T13:30:40Z",
  "aggregation_window": "2026-06-24T13:30:00Z",
  "broadcast_window": "2026-06-24T13:30:40Z",
  "is_partial_window": true,
  "data": {
    "current_minute_events_so_far": 220,
    "bot_count": 80,
    "human_count": 140,
    "bot_ratio": 0.36,
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
        "url": "https://en.wikipedia.org/wiki/Scale_AI"
      }
    ]
  }
}
```

Dashboard interpretation:

```text
current_minute_events_so_far  monotonically increases during the current minute
is_partial_window             true when the current minute is still in progress
broadcast_window              dashboard refresh trigger, not an aggregation window
```

### Wiki topic message

```json
{
  "type": "stats.update",
  "topic": "wiki:frwiki",
  "timestamp": "2026-06-24T13:30:40Z",
  "aggregation_window": "2026-06-24T13:30:00Z",
  "broadcast_window": "2026-06-24T13:30:40Z",
  "is_partial_window": true,
  "data": {
    "wiki": "frwiki",
    "current_minute_events_so_far": 18
  }
}
```

---

## Contract 6 — WebSocket client messages

**Producer:** Frontend Dashboard  
**Consumer:** WebSocket Default Handler Lambda

Supported messages:

```json
{ "action": "subscribe", "topic": "global" }
{ "action": "subscribe", "topic": "wiki:frwiki" }
{ "action": "unsubscribe", "topic": "wiki:frwiki" }
```

Valid topics:

```text
global
wiki:{wiki_code}
```

Planned optional topic:

```text
top_pages
```

If `top_pages` is exposed as a separate topic, it must be served from `METRIC#TOP_PAGES#SHARD#{n}`.

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

**Producer:** WebSocket Connect Handler Lambda, WebSocket Default Handler Lambda  
**Consumers:** Broadcaster Lambda, WebSocket Disconnect Handler Lambda

### Table key

```text
PK = connection_id
```

### Item shape

```json
{
  "connection_id": "Mn2Pc9dfPHcCEug=",
  "connected_at": "2026-06-24T13:30:00Z",
  "client_type": "dashboard",
  "topics": [
    "global",
    "wiki:frwiki"
  ],
  "ttl": 1782487800
}
```

TTL:

```text
ttl = connected_at + 2 hours
```

### V1 access pattern

```text
Broadcaster scans websocket_connections.
Broadcaster filters topics inside Lambda.
```

This is acceptable for portfolio-scale V1.

### V2 scaling option

Add a `websocket_subscriptions` table:

```text
PK = TOPIC#{topic}
SK = CONNECTION#{connection_id}
```

This allows the Broadcaster to Query subscriptions by topic instead of scanning all connections.

---

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

Silver is cleaned, typed, columnar data derived from Bronze envelope fields and `payload`.

```text
Format      : Parquet
Compression : SNAPPY
Partition   : ingestion_date
```

Fields:

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

Silver does not need to preserve full `raw_event` because Bronze is the source-fidelity archive.

---

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
DROP   if meta.id is missing
DROP   if meta.dt is missing or unparseable
DROP   if meta.domain == "canary"
KEEP   all five change types
EMBED  raw_event exactly as received
```

### Realtime Processor

```text
READ   Kinesis normalized envelope
DROP   if event_type != wiki.recentchange
DROP   if payload is missing or invalid
DROP   if event_id is missing
FALLBACK occurred_at to processing time only if malformed
WRITE  realtime_aggregates with UpdateItem ADD
WRITE  global, wiki, top_wikis, change_type, bot_activity, namespace
WRITE  top_pages only when namespace == 0
SEND   SQS FIFO broadcast signal after successful DynamoDB writes
```

### Broadcaster

```text
READ   SQS broadcast signal
READ   realtime_aggregates for the requested aggregation window
READ   GLOBAL_ACTIVITY shards and sum them
READ   BOT_ACTIVITY true/false and compute bot_ratio
READ   CHANGE_TYPE known values
READ   NAMESPACE known/common values
READ   TOP_WIKIS shards and sort top N
READ   TOP_PAGES shards and sort top N
SCAN   websocket_connections in V1
FILTER topics inside Lambda
PUSH   stats.update to matching WebSocket connections
DELETE stale connections on GoneException / 410
```

### Alert Processor

```text
READ   Kinesis normalized envelope directly
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
KEEP      all 5 change_types
CAST      user_is_bot to boolean
CAST      is_minor to boolean, null if absent
COMPUTE   delta_bytes only when both lengths are non-null
SERIALIZE log_params as JSON string
SELECT    known payload columns only
PRESERVE  raw_event in Bronze; Silver does not need full raw_event by default
```
