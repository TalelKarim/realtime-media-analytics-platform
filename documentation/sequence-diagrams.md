# Sequence Diagrams — Realtime Media Analytics Platform

Render at: https://mermaid.live  
GitHub and GitLab render Mermaid natively inside Markdown files.

---

## Diagram 1 — Full Ingestion to Live Dashboard

```mermaid
sequenceDiagram
  autonumber
  actor Wiki as Wikimedia EventStreams
  participant Col as ECS Fargate Collector
  participant KDS as Kinesis Data Streams
  participant RTP as Realtime Processor Lambda
  participant DDB as DynamoDB realtime_aggregates
  participant SQS as SQS FIFO
  participant BRD as Broadcaster Lambda
  participant DBC as DynamoDB websocket_connections
  participant AGW as API Gateway WebSocket
  participant DSH as Frontend Dashboard

  Wiki->>Col: SSE event (edit / new / categorize / log / external)
  Note over Col: Parse JSON · validate meta.id + meta.dt<br/>Normalize to null-safe contract<br/>Buffer in memory

  Col->>Col: 100 events buffered OR 2 seconds elapsed
  Col->>KDS: PutRecords — PartitionKey = hash(meta.id)
  Note over KDS: Fan-out to 3 independent consumers

  KDS->>RTP: Trigger Lambda with event batch
  loop For each event in batch
    RTP->>RTP: Compute window key (1-min bucket)<br/>Compute shard_id = hash(event_id) % 10
    RTP->>DDB: UpdateItem ADD counters<br/>METRIC#GLOBAL_ACTIVITY#SHARD#{shard_id} / WINDOW#{minute}
    RTP->>DDB: UpdateItem ADD counters<br/>METRIC#WIKI_ACTIVITY#WIKI#{wiki} / WINDOW#{minute}
    alt change_type != log AND namespace == 0
      RTP->>DDB: UpdateItem ADD events_count<br/>METRIC#TOP_PAGES#WIKI#{wiki} / WINDOW#{minute}#TITLE#{title}
    end
    RTP->>DDB: UpdateItem ADD events_count<br/>METRIC#CHANGE_TYPE#TYPE#{type} / WINDOW#{minute}
  end

  RTP->>SQS: SendMessage<br/>MessageDeduplicationId = aggregates-window-{minute}
  Note over SQS: Dedup: 1 message per window<br/>regardless of Lambda invocation count

  SQS->>BRD: Trigger Broadcaster Lambda
  loop For each shard 0..9
    BRD->>DDB: GetItem METRIC#GLOBAL_ACTIVITY#SHARD#{n}
  end
  BRD->>BRD: Sum all shards → compute totals and bot_ratio

  BRD->>DBC: Query connections subscribed to relevant topics
  DBC-->>BRD: List of active connectionIds

  loop For each connectionId
    BRD->>AGW: postToConnection(connectionId, stats.update)
    alt GoneException — client disconnected silently
      BRD->>DBC: DeleteItem connectionId
    end
  end

  AGW->>DSH: JSON stats.update message
  DSH->>DSH: Update charts and KPIs
```

---

## Diagram 2 — WebSocket Lifecycle

```mermaid
sequenceDiagram
  autonumber
  actor User as Browser / Analyst
  participant AGW as API Gateway WebSocket
  participant CNX as Connect Handler Lambda
  participant DEF as Default Handler Lambda
  participant DCN as Disconnect Handler Lambda
  participant DBC as DynamoDB websocket_connections
  participant BRD as Broadcaster Lambda

  User->>AGW: WebSocket connect (wss://...)
  AGW->>CNX: Invoke $connect route (connectionId)
  CNX->>DBC: PutItem — connection_id, connected_at, topics=["global"], ttl=now+2h
  DBC-->>CNX: OK
  CNX-->>AGW: HTTP 200
  AGW-->>User: Connection established

  Note over User,DBC: Client receives global stats by default

  User->>AGW: {"action":"subscribe","topic":"wiki:frwiki"}
  AGW->>DEF: Invoke $default (connectionId, body)
  DEF->>DBC: UpdateItem — append "wiki:frwiki" to topics list
  DBC-->>DEF: OK
  DEF->>AGW: postToConnection → subscription.ack
  AGW-->>User: {"type":"subscription.ack","topic":"wiki:frwiki","status":"subscribed"}

  Note over BRD,User: Next broadcast cycle triggers

  BRD->>DBC: Query connections where topics contains "wiki:frwiki"
  DBC-->>BRD: [connectionId, ...]
  BRD->>AGW: postToConnection(connectionId, stats.update for wiki:frwiki)
  AGW-->>User: {"type":"stats.update","topic":"wiki:frwiki","data":{...}}

  User->>AGW: Close tab or network drop
  AGW->>DCN: Invoke $disconnect (connectionId)
  DCN->>DBC: DeleteItem connectionId
  DBC-->>DCN: OK
  Note over DBC: Connection removed — no more pushes for this client<br/>TTL covers cases where $disconnect is never fired
```

---

## Diagram 3 — Historical Archive Pipeline

```mermaid
sequenceDiagram
  autonumber
  participant KDS as Kinesis Data Streams
  participant FH  as Kinesis Firehose
  participant S3B as S3 Bronze
  participant GL1 as Glue bronze→silver
  participant S3S as S3 Silver
  participant GL2 as Glue silver→gold
  participant S3G as S3 Gold
  participant CAT as Glue Data Catalog
  participant ATH as Athena
  participant QS  as QuickSight

  KDS->>FH: Stream events (parallel Kinesis consumer)
  Note over FH: Buffer: 64 MB OR 300 seconds — whichever first

  FH->>S3B: JSON Lines batch (GZIP)<br/>bronze/wikimedia/recentchange/year=Y/month=M/day=D/hour=H/
  Note over S3B: Raw · immutable · full fidelity<br/>All 5 event types · all fields preserved

  Note over GL1: Hourly Glue trigger — previous hour partition
  GL1->>S3B: Read bronze partition
  GL1->>GL1: Drop null meta.id or meta.dt<br/>Cast bot→boolean, minor→boolean<br/>Compute delta_bytes (null-safe)<br/>Serialize log_params as JSON string<br/>Select known columns only — ignore unknowns
  GL1->>S3S: Write Parquet (SNAPPY)<br/>silver/wikimedia/recentchange/ingestion_date=D/
  GL1->>CAT: Update partition metadata

  Note over GL2: Hourly Glue trigger — after bronze→silver
  GL2->>S3S: Read silver partition (same hour)
  GL2->>GL2: Aggregate top_wikis_by_hour<br/>Aggregate bot_vs_human_by_hour<br/>Aggregate change_type_distribution<br/>Aggregate top_pages_by_day (namespace=0 only)<br/>Compute z_score for activity_spikes
  GL2->>S3G: Write 5 gold Parquet datasets
  GL2->>CAT: Update gold partition metadata

  Note over ATH: Partition projection active — no MSCK REPAIR needed
  ATH->>S3G: SQL scan (partition pruned, column pruned)
  S3G-->>ATH: Parquet column data
  ATH-->>QS: Query result set

  QS->>QS: SPICE incremental refresh (hourly)<br/>Rebuild historical dashboards
```

---

## Diagram 4 — Alert Processor

```mermaid
sequenceDiagram
  autonumber
  participant KDS as Kinesis Data Streams
  participant ALP as Alert Processor Lambda
  participant SNS as SNS Topic
  participant OPS as Platform Engineer

  KDS->>ALP: Trigger Lambda — 1-minute batch
  ALP->>ALP: Count events in batch<br/>Load rolling 30-min window from state
  ALP->>ALP: z_score = (current - rolling_avg) / rolling_stddev

  alt z_score > 2.0 — global spike detected
    ALP->>SNS: Publish alert<br/>{"wiki":"global","events":1840,"avg":1240,"z_score":2.7}
    SNS->>OPS: Email / SMS<br/>"Activity spike: 1840 events/min vs avg 1240 (z=2.7)"
  else log_type burst — moderation spike
    Note over ALP: delete|block events > 3× normal in 5 min
    ALP->>SNS: Publish moderation alert<br/>"45 deletions in 5 min (normal: 12)"
    SNS->>OPS: Email / SMS
  else normal
    ALP->>ALP: Update rolling window state — no alert
  end
```

---

## Diagram 5 — Collector Crash Recovery

```mermaid
sequenceDiagram
  autonumber
  participant Wiki as Wikimedia EventStreams
  participant ECS  as ECS Fargate Service
  participant Col  as SSE Collector Task
  participant KDS  as Kinesis Data Streams
  participant CW   as CloudWatch Alarm

  Col->>Wiki: SSE connection open
  Wiki-->>Col: Live stream of events
  Col->>KDS: PutRecords (ongoing)

  Note over Col: Crash — OOM / network error / unhandled exception

  Col-xECS: Task exits (non-zero exit code)
  ECS->>CW: RunningTaskCount = 0
  CW->>CW: ALARM — ECS task count < 1
  Note over ECS: Restart policy triggers immediately

  ECS->>Col: Launch new Fargate task
  Col->>Wiki: Reconnect SSE
  Wiki-->>Col: Resume live stream

  Note over KDS: Gap during downtime is not recoverable<br/>SSE is a live stream — no replay<br/>S3 bronze archive is unaffected (Firehose is independent)

  Col->>KDS: Resume PutRecords
  CW->>CW: OK — RunningTaskCount = 1
```

---

## Diagram 6 — Kinesis High Iterator Age Recovery

```mermaid
sequenceDiagram
  autonumber
  participant KDS as Kinesis Data Streams
  participant RTP as Realtime Processor Lambda
  participant DDB as DynamoDB
  participant CW  as CloudWatch Alarm
  participant OPS as Platform Engineer

  Note over KDS,RTP: Normal — IteratorAge < 5 000 ms

  KDS->>RTP: Traffic spike — high event volume
  RTP->>DDB: High write rate → throttling
  DDB-->>RTP: ProvisionedThroughputExceededException

  RTP->>KDS: Processing slows — records pile up
  KDS->>CW: IteratorAgeMilliseconds > 60 000 ms
  CW->>OPS: ALARM — Kinesis iterator age high

  OPS->>DDB: Switch to on-demand billing<br/>aws dynamodb update-table --billing-mode PAY_PER_REQUEST
  DDB-->>OPS: Throttling resolved

  alt Still lagging after DynamoDB fix
    OPS->>KDS: Increase shard count<br/>aws kinesis update-shard-count --target-shard-count 4
    Note over KDS: Resharding: ~30 seconds · brief consumer disruption
    KDS->>RTP: Higher throughput capacity available
  end

  KDS->>CW: IteratorAgeMilliseconds < 5 000 ms
  CW->>OPS: OK — alarm resolved
```
```
