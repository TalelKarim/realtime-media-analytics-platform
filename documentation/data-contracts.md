# Data Contracts — Realtime Media Analytics Platform

Schema reference (official, verified):
```
https://github.com/wikimedia/mediawiki-event-schemas/blob/master/jsonschema/mediawiki/recentchange/current.yaml
```

> The official schema uses `additionalProperties: true` and only guarantees the `meta` object.
> All other fields are optional. Every component must handle absent fields gracefully.

---

## Contract 1 — Wikimedia Raw SSE Event

**Producer:** Wikimedia EventStreams  
**Consumer:** ECS Fargate Collector

```
Protocol : HTTPS / Server-Sent Events
Encoding : text/event-stream, UTF-8
Format   : one JSON object per "data:" line
Schema   : /mediawiki/recentchange/1.0.0
```

Five event types emitted by the stream:

| Type | Approx. frequency | Notes |
|---|---|---|
| `edit` | ~60-65% | Article or file modification |
| `categorize` | ~25-30% | Category membership change — always namespace 14 |
| `new` | ~5-8% | New page creation |
| `log` | ~5-8% | Admin action — always namespace -1 |
| `external` | <1% | External system change (e.g. Wikidata link) |

Field presence matrix (verified against official schema + observed stream):

```
Field                edit   new    categorize  log    external
────────────────────────────────────────────────────────────────
meta.id (UUID)        ✅     ✅      ✅          ✅      ✅
meta.dt (ISO8601)     ✅     ✅      ✅          ✅      ✅
meta.domain           ✅     ✅      ✅          ✅      ✅
meta.topic            ✅     ✅      ✅          ✅      ✅
meta.partition        ✅     ✅      ✅          ✅      ✅
meta.offset           ✅     ✅      ✅          ✅      ✅
id (rcid, not unique) ✅     ✅      ✅          ✅      ✅
type                  ✅     ✅      ✅          ✅      ✅
namespace             ✅     ✅      ✅(=14)     ✅(=-1) ✅(=0)
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
title_url             ⚠️*    ⚠️*     ⚠️*         ⚠️*     ⚠️*
minor                 ✅     ✅      ❌          ❌      ❌
patrolled             ⚠️     ⚠️      ❌          ❌      ❌
length.old            ✅     ✅(=0)  ❌          ❌      ❌
length.new            ✅     ✅      ❌          ❌      ❌
revision.old          ✅     ✅(null)❌          ❌      ❌
revision.new          ✅     ✅      ❌          ❌      ❌
log_id                ❌     ❌      ❌          ✅      ❌
log_type              ❌     ❌      ❌          ✅      ❌
log_action            ❌     ❌      ❌          ✅      ❌
log_params            ❌     ❌      ❌          ⚠️      ❌
log_action_comment    ❌     ❌      ❌          ⚠️      ❌

✅ always present   ⚠️ optional / config-dependent   ❌ absent for this type
* title_url: observed in practice but absent from the official schema — treat as optional
```

### Real event examples

**type = edit**
```json
{
  "$schema": "/mediawiki/recentchange/1.0.0",
  "meta": {
    "id": "e195ebcf-e9ff-4179-9591-0d2384b96117",
    "dt": "2026-06-11T16:41:05Z",
    "domain": "commons.wikimedia.org",
    "stream": "mediawiki.recentchange",
    "topic": "eqiad.mediawiki.recentchange",
    "partition": 0,
    "offset": 6232533545
  },
  "id": 1754327016,
  "type": "edit",
  "namespace": 6,
  "title": "File:DESERT SCIMITAR 130430-M-OC922-009.jpg",
  "comment": "add location United States inside Taken On on template",
  "parsedcomment": "add location United States inside Taken On on template",
  "timestamp": 1781196065,
  "user": "RudolphousBot",
  "bot": true,
  "minor": true,
  "patrolled": true,
  "length": { "old": 1132, "new": 1155 },
  "revision": { "old": 585514253, "new": 586543834 },
  "server_url": "https://commons.wikimedia.org",
  "server_name": "commons.wikimedia.org",
  "server_script_path": "/w",
  "wiki": "commonswiki"
}
```

**type = categorize**
```json
{
  "$schema": "/mediawiki/recentchange/1.0.0",
  "meta": {
    "id": "a9b8c7d6-aaaa-bbbb-cccc-ddddeeeeffff",
    "dt": "2026-06-11T16:43:00Z",
    "domain": "commons.wikimedia.org",
    "stream": "mediawiki.recentchange",
    "topic": "eqiad.mediawiki.recentchange",
    "partition": 0,
    "offset": 6232534000
  },
  "id": 998877665,
  "type": "categorize",
  "namespace": 14,
  "title": "Category:Photos by RudolphousBot",
  "comment": "File:DESERT_SCIMITAR.jpg added to category",
  "timestamp": 1781196180,
  "user": "RudolphousBot",
  "bot": true,
  "server_url": "https://commons.wikimedia.org",
  "server_name": "commons.wikimedia.org",
  "server_script_path": "/w",
  "wiki": "commonswiki"
}
```

**type = log (delete)**
```json
{
  "$schema": "/mediawiki/recentchange/1.0.0",
  "meta": {
    "id": "b1c2d3e4-f5a6-b7c8-d9e0-f1a2b3c4d5e6",
    "dt": "2026-06-11T16:44:00Z",
    "domain": "en.wikipedia.org",
    "stream": "mediawiki.recentchange",
    "topic": "eqiad.mediawiki.recentchange",
    "partition": 0,
    "offset": 6232534200
  },
  "id": 112233445,
  "type": "log",
  "namespace": -1,
  "title": "Fake News Article 2026",
  "comment": "Spam: blatant advertising",
  "timestamp": 1781196240,
  "user": "Fastily",
  "bot": false,
  "server_url": "https://en.wikipedia.org",
  "server_name": "en.wikipedia.org",
  "server_script_path": "/w",
  "wiki": "enwiki",
  "log_id": 98765432,
  "log_type": "delete",
  "log_action": "delete",
  "log_params": { "suppressedrevs": false },
  "log_action_comment": "Deleted [[Fake News Article 2026]]: Spam: blatant advertising"
}
```

**type = log (block)**
```json
{
  "$schema": "/mediawiki/recentchange/1.0.0",
  "meta": {
    "id": "c2d3e4f5-a6b7-c8d9-e0f1-a2b3c4d5e6f7",
    "dt": "2026-06-11T16:45:00Z",
    "domain": "en.wikipedia.org",
    "stream": "mediawiki.recentchange",
    "topic": "eqiad.mediawiki.recentchange",
    "partition": 0,
    "offset": 6232534400
  },
  "id": 112233446,
  "type": "log",
  "namespace": -1,
  "title": "User:Vandal1234",
  "comment": "Persistent vandalism",
  "timestamp": 1781196300,
  "user": "ClueBot NG",
  "bot": true,
  "server_url": "https://en.wikipedia.org",
  "server_name": "en.wikipedia.org",
  "server_script_path": "/w",
  "wiki": "enwiki",
  "log_id": 98765433,
  "log_type": "block",
  "log_action": "block",
  "log_params": { "5::duration": "31 hours", "6::flags": "nocreate,noemail" },
  "log_action_comment": "Blocked [[User:Vandal1234]] with expiry 31 hours (nocreate, noemail)"
}
```

Known `log_type` values: `delete` · `restore` · `block` · `unblock` · `protect` · `upload` · `move` · `import` · `patrol` · `rights` · `newusers` · `merge` · `suppress` · `tag`

---

## Contract 2 — Kinesis Normalized Event

**Producer:** ECS Fargate Collector  
**Consumer:** Realtime Processor Lambda, Alert Processor Lambda

The Collector transforms every raw Wikimedia event into this unified, null-safe contract before sending to Kinesis.

> `event_id` is built from `meta.id` (UUID), not from `id` (rcid).  
> The rcid is per-wiki only — two wikis can emit the same rcid.  
> `meta.id` is globally unique across the entire stream.

> The Kinesis `PartitionKey` is `hash(meta.id)`. It is a PutRecords **call parameter**, never a JSON field.

### Schema

```json
{
  "event_id":       "wikimedia-{meta.id}",
  "event_type":     "wiki.recentchange",
  "event_version":  "1.0",
  "source":         "wikimedia.eventstreams",
  "occurred_at":    "{meta.dt}",
  "ingested_at":    "{ISO8601 — collector ingestion time}",
  "correlation_id": "{UUID generated by collector}",
  "payload": {
    "wikimedia_recentchange_id": "{id — integer, not unique cross-wiki}",
    "wiki":          "{wiki}",
    "domain":        "{meta.domain}",
    "change_type":   "{type}",
    "namespace":     "{namespace}",
    "title":         "{title}",
    "title_url":     "{title_url if present, else null}",
    "user":          "{user}",
    "user_is_bot":   "{bot — boolean}",
    "is_minor":      "{minor if present, else null}",
    "is_patrolled":  "{patrolled if present, else null}",
    "old_length":    "{length.old if present, else null}",
    "new_length":    "{length.new if present, else null}",
    "delta_bytes":   "{new_length - old_length if both present, else null}",
    "revision_old":  "{revision.old if present, else null}",
    "revision_new":  "{revision.new if present, else null}",
    "notify_url":    "{notify_url if present, else null}",
    "log_type":      "{log_type if present, else null}",
    "log_action":    "{log_action if present, else null}",
    "log_params":    "{log_params if present, else null}"
  }
}
```

### Field mapping

| Normalized field | Source field | Rule |
|---|---|---|
| `event_id` | `meta.id` | `"wikimedia-" + meta.id` — globally unique |
| `occurred_at` | `meta.dt` | ISO8601 |
| `wikimedia_recentchange_id` | `id` | integer, per-wiki only |
| `wiki` | `wiki` | lowercase |
| `domain` | `meta.domain` | |
| `change_type` | `type` | one of: edit · new · categorize · log · external |
| `namespace` | `namespace` | -1 for log, 14 for categorize, 0 for articles |
| `title` | `title` | |
| `title_url` | `title_url` | null if absent |
| `user` | `user` | |
| `user_is_bot` | `bot` | cast boolean |
| `is_minor` | `minor` | null for non-edit types |
| `is_patrolled` | `patrolled` | null if field absent |
| `old_length` | `length.old` | null for categorize / log / external |
| `new_length` | `length.new` | null for categorize / log / external |
| `delta_bytes` | computed | `new_length - old_length`; null if either is null |
| `revision_old` | `revision.old` | null for non-edit types |
| `revision_new` | `revision.new` | null for non-edit types |
| `notify_url` | `notify_url` | null if absent |
| `log_type` | `log_type` | null for non-log types |
| `log_action` | `log_action` | null for non-log types |
| `log_params` | `log_params` | null for non-log; may be array, object, or string |

### Concrete examples

**edit event:**
```json
{
  "event_id": "wikimedia-e195ebcf-e9ff-4179-9591-0d2384b96117",
  "event_type": "wiki.recentchange",
  "event_version": "1.0",
  "source": "wikimedia.eventstreams",
  "occurred_at": "2026-06-11T16:41:05Z",
  "ingested_at": "2026-06-11T16:41:05.312Z",
  "correlation_id": "7f3e2a1b-9c8d-4e5f-a6b7-c8d9e0f1a2b3",
  "payload": {
    "wikimedia_recentchange_id": 1754327016,
    "wiki": "commonswiki",
    "domain": "commons.wikimedia.org",
    "change_type": "edit",
    "namespace": 6,
    "title": "File:DESERT SCIMITAR 130430-M-OC922-009.jpg",
    "title_url": null,
    "user": "RudolphousBot",
    "user_is_bot": true,
    "is_minor": true,
    "is_patrolled": true,
    "old_length": 1132,
    "new_length": 1155,
    "delta_bytes": 23,
    "revision_old": 585514253,
    "revision_new": 586543834,
    "notify_url": "https://commons.wikimedia.org/w/index.php?diff=586543834&oldid=585514253",
    "log_type": null,
    "log_action": null,
    "log_params": null
  }
}
```

**categorize event:**
```json
{
  "event_id": "wikimedia-a9b8c7d6-aaaa-bbbb-cccc-ddddeeeeffff",
  "event_type": "wiki.recentchange",
  "event_version": "1.0",
  "source": "wikimedia.eventstreams",
  "occurred_at": "2026-06-11T16:43:00Z",
  "ingested_at": "2026-06-11T16:43:00.201Z",
  "correlation_id": "8g4f3b2c-0d9e-5f6a-b7c8-d9e0f1a2b3c4",
  "payload": {
    "wikimedia_recentchange_id": 998877665,
    "wiki": "commonswiki",
    "domain": "commons.wikimedia.org",
    "change_type": "categorize",
    "namespace": 14,
    "title": "Category:Photos by RudolphousBot",
    "title_url": null,
    "user": "RudolphousBot",
    "user_is_bot": true,
    "is_minor": null,
    "is_patrolled": null,
    "old_length": null,
    "new_length": null,
    "delta_bytes": null,
    "revision_old": null,
    "revision_new": null,
    "notify_url": null,
    "log_type": null,
    "log_action": null,
    "log_params": null
  }
}
```

**log event (delete):**
```json
{
  "event_id": "wikimedia-b1c2d3e4-f5a6-b7c8-d9e0-f1a2b3c4d5e6",
  "event_type": "wiki.recentchange",
  "event_version": "1.0",
  "source": "wikimedia.eventstreams",
  "occurred_at": "2026-06-11T16:44:00Z",
  "ingested_at": "2026-06-11T16:44:00.089Z",
  "correlation_id": "9h5g4c3d-1e0f-6a7b-c8d9-e0f1a2b3c4d5",
  "payload": {
    "wikimedia_recentchange_id": 112233445,
    "wiki": "enwiki",
    "domain": "en.wikipedia.org",
    "change_type": "log",
    "namespace": -1,
    "title": "Fake News Article 2026",
    "title_url": null,
    "user": "Fastily",
    "user_is_bot": false,
    "is_minor": null,
    "is_patrolled": null,
    "old_length": null,
    "new_length": null,
    "delta_bytes": null,
    "revision_old": null,
    "revision_new": null,
    "notify_url": null,
    "log_type": "delete",
    "log_action": "delete",
    "log_params": { "suppressedrevs": false }
  }
}
```

---

## Contract 3 — DynamoDB realtime_aggregates

**Producer:** Realtime Processor Lambda  
**Consumer:** Broadcaster Lambda

All writes use atomic `UpdateItem ADD` — no read-modify-write, race-condition safe.  
TTL = `window_start + 7200` (2 hours). Items auto-expire after the historical analytics path takes over.

### 3a — Global Activity (write-sharded)

```json
{
  "PK": "METRIC#GLOBAL_ACTIVITY#SHARD#3",
  "SK": "WINDOW#2026-06-11T16:44",
  "window_start": "2026-06-11T16:44:00Z",
  "events_count": 120,
  "bot_events": 48,
  "human_events": 72,
  "edit_events": 70,
  "new_events": 10,
  "categorize_events": 35,
  "log_events": 5,
  "external_events": 0,
  "ttl": 1781203440
}
```

Broadcaster reads `SHARD#0` through `SHARD#9` and sums all counters.

### 3b — Wiki Activity

```json
{
  "PK": "METRIC#WIKI_ACTIVITY#WIKI#enwiki",
  "SK": "WINDOW#2026-06-11T16:44",
  "wiki": "enwiki",
  "window_start": "2026-06-11T16:44:00Z",
  "events_count": 240,
  "bot_events": 80,
  "human_events": 160,
  "edit_events": 180,
  "new_events": 12,
  "categorize_events": 40,
  "log_events": 8,
  "external_events": 0,
  "ttl": 1781214240
}
```

### 3c — Top Pages (namespace = 0 only)

Log events (namespace = -1) and categorize events (namespace = 14) are excluded.

```json
{
  "PK": "METRIC#TOP_PAGES#WIKI#enwiki",
  "SK": "WINDOW#2026-06-11T16:44#TITLE#Scale AI",
  "wiki": "enwiki",
  "title": "Scale AI",
  "title_url": "https://en.wikipedia.org/wiki/Scale_AI",
  "events_count": 6,
  "last_change_type": "edit",
  "last_seen_at": "2026-06-11T16:44:09Z",
  "ttl": 1781214240
}
```

### 3d — Change Type Distribution

All five types tracked: `edit` · `new` · `categorize` · `log` · `external`

```json
{
  "PK": "METRIC#CHANGE_TYPE#TYPE#edit",
  "SK": "WINDOW#2026-06-11T16:44",
  "change_type": "edit",
  "events_count": 850,
  "ttl": 1781214240
}
```

### 3e — Namespace Distribution

Namespace -1 (log events) is a valid value and is tracked.

```json
{
  "PK": "METRIC#NAMESPACE#NS#0",
  "SK": "WINDOW#2026-06-11T16:44",
  "namespace": 0,
  "events_count": 620,
  "ttl": 1781214240
}
```

Common namespace values: `-1` (Special/log) · `0` (Article) · `1` (Talk) · `2` (User) · `4` (Project) · `6` (File) · `10` (Template) · `14` (Category)

---

## Contract 4 — DynamoDB websocket_connections

**Producer:** Connect Handler Lambda  
**Consumer:** Broadcaster Lambda, Disconnect Handler Lambda

```json
{
  "connection_id": "Mn2Pc9dfPHcCEug=",
  "connected_at": "2026-06-11T16:44:08Z",
  "client_type": "dashboard",
  "topics": ["global", "wiki:enwiki", "top_pages"],
  "ttl": 1781203448
}
```

TTL = `connected_at + 7200` (2 hours). Protects against ghost connections when `$disconnect` is never fired.

---

## Contract 5 — SQS FIFO Broadcast Signal

**Producer:** Realtime Processor Lambda  
**Consumer:** Broadcaster Lambda

```json
{
  "message_type": "aggregates.updated",
  "window": "2026-06-11T16:44:00Z",
  "topics": ["global", "top_pages", "wiki:enwiki", "wiki:commonswiki"],
  "created_at": "2026-06-11T16:44:05.123Z"
}
```

```
MessageGroupId        = "broadcast-signal"
MessageDeduplicationId = "aggregates-window-2026-06-11T16:44"
```

Deduplication ensures only one broadcast per time window, regardless of how many Lambda invocations write to the same window concurrently.

---

## Contract 6 — WebSocket stats.update (server → client)

**Producer:** Broadcaster Lambda  
**Consumer:** Frontend Dashboard

```json
{
  "type": "stats.update",
  "topic": "global",
  "timestamp": "2026-06-11T16:44:10Z",
  "window": "2026-06-11T16:44:00Z",
  "data": {
    "events_per_minute": 1240,
    "bot_ratio": 0.42,
    "human_ratio": 0.58,
    "top_wikis": [
      { "wiki": "commonswiki", "count": 320 },
      { "wiki": "enwiki",      "count": 240 },
      { "wiki": "wikidatawiki","count": 210 }
    ],
    "change_types": {
      "edit": 760, "categorize": 390, "new": 70, "log": 20, "external": 0
    },
    "top_pages": [
      { "wiki": "enwiki", "title": "Scale AI", "count": 6, "url": "https://en.wikipedia.org/wiki/Scale_AI" }
    ],
    "namespace_distribution": {
      "0": 620, "6": 210, "14": 390, "-1": 20
    }
  }
}
```

---

## Contract 7 — WebSocket messages (client → server)

**Producer:** Frontend Dashboard  
**Consumer:** Default Handler Lambda

```json
{ "action": "subscribe",   "topic": "wiki:frwiki" }
{ "action": "unsubscribe", "topic": "wiki:enwiki"  }
{ "action": "subscribe",   "topic": "global"       }
{ "action": "subscribe",   "topic": "top_pages"    }
```

Valid topics: `global` · `top_pages` · `wiki:{any_valid_wiki_code}`

---

## Contract 8 — WebSocket acknowledgements (server → client)

```json
{ "type": "subscription.ack", "topic": "wiki:frwiki", "status": "subscribed"   }
{ "type": "subscription.ack", "topic": "wiki:enwiki",  "status": "unsubscribed" }
{ "type": "error", "message": "Unsupported topic"  }
{ "type": "error", "message": "Invalid action"     }
```

---

## Contract 9 — S3 Bronze (raw archive)

**Producer:** Kinesis Firehose  
**Consumer:** Glue ETL bronze→silver

```
Path    : s3://bucket/bronze/wikimedia/recentchange/
          year=2026/month=06/day=11/hour=16/
          wikimedia-recentchange-2026-06-11-16-05-00-{uuid}.json.gz

Format  : JSON Lines, one raw Wikimedia event per line, GZIP compressed
Schema  : raw Wikimedia — no transformation, all fields preserved as-is
Latency : available in S3 within 5 minutes of ingestion (Firehose buffer: 64MB or 300s)
Purpose : immutable archive, schema recovery, event replay
```

---

## Contract 10 — S3 Silver (cleaned Parquet)

**Producer:** Glue ETL bronze→silver  
**Consumer:** Glue ETL silver→gold, Athena

```
Path    : s3://bucket/silver/wikimedia/recentchange/
          ingestion_date=2026-06-11/part-00000.parquet

Format  : Apache Parquet, SNAPPY compression
Latency : available ~1h after ingestion (hourly Glue job)
```

Schema:
```
event_id              STRING    NOT NULL  -- "wikimedia-{meta.id}"
occurred_at           TIMESTAMP NOT NULL  -- parsed from meta.dt
ingestion_date        DATE      NOT NULL  -- partition key
wiki                  STRING    NOT NULL
domain                STRING
change_type           STRING              -- edit|new|categorize|log|external
namespace             INT                 -- -1 for log, 14 for categorize
title                 STRING
title_url             STRING              -- null if absent
user                  STRING
user_is_bot           BOOLEAN
is_minor              BOOLEAN             -- null for non-edit/new
is_patrolled          BOOLEAN             -- null if field absent
old_length            INT                 -- null for categorize/log/external
new_length            INT                 -- null for categorize/log/external
delta_bytes           INT                 -- null if either length is null
revision_old          BIGINT              -- null for non-edit/new
revision_new          BIGINT              -- null for non-edit/new
log_type              STRING              -- null for non-log
log_action            STRING              -- null for non-log
log_params            STRING              -- JSON string; null if absent
wikimedia_rcid        BIGINT              -- original id field (not unique cross-wiki)
```

---

## Contract 11 — S3 Gold (pre-aggregated Parquet)

**Producer:** Glue ETL silver→gold  
**Consumer:** Athena, QuickSight

### top_wikis_by_hour
```
Path   : s3://bucket/gold/top_wikis_by_hour/year=2026/month=06/day=11/hour=16/
Schema : wiki STRING, hour_window TIMESTAMP, event_count BIGINT,
         bot_count BIGINT, human_count BIGINT,
         edit_count BIGINT, new_count BIGINT, categorize_count BIGINT,
         log_count BIGINT, external_count BIGINT
```

### top_pages_by_day
```
Path   : s3://bucket/gold/top_pages_by_day/year=2026/month=06/day=11/
Schema : wiki STRING, title STRING, day DATE,
         event_count BIGINT, last_change_type STRING, last_seen_at TIMESTAMP
Filter : namespace = 0 only
```

### bot_vs_human_by_hour
```
Path   : s3://bucket/gold/bot_vs_human_by_hour/year=2026/month=06/day=11/hour=16/
Schema : hour_window TIMESTAMP, bot_count BIGINT, human_count BIGINT,
         total_count BIGINT, bot_ratio DOUBLE
```

### change_type_distribution
```
Path   : s3://bucket/gold/change_type_distribution/year=2026/month=06/day=11/hour=16/
Schema : change_type STRING, hour_window TIMESTAMP, event_count BIGINT
Values : edit | new | categorize | log | external
```

### activity_spikes
```
Path   : s3://bucket/gold/activity_spikes/year=2026/month=06/day=11/
Schema : hour_window TIMESTAMP, wiki STRING (null=global),
         event_count BIGINT, z_score DOUBLE, is_spike BOOLEAN
Spike  : is_spike = true when z_score > 2.0
```

---

## Critical filtering rules

These rules follow directly from the real Wikimedia event structure.

**Realtime Processor**
```
top_pages     → namespace = 0 only (excludes log=-1, categorize=14)
wiki_activity → all types and namespaces included
change_type   → all 5 types counted: edit, new, categorize, log, external
bot_ratio     → user_is_bot applied across all types
```

**Glue Silver ETL**
```
DROP   if meta.id is null
DROP   if meta.dt is null or unparseable
KEEP   all 5 change_types
CAST   bot → boolean (never null in the stream)
CAST   minor → boolean, null if field absent
COMPUTE delta_bytes only when both lengths are non-null
SERIALIZE log_params as JSON string (type varies: array | object | string)
```