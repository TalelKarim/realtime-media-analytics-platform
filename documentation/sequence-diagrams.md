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

  Wiki->>Col: SSE event data JSON
  Note over Col: Parse JSON · validate meta.id + meta.dt<br/>Drop meta.domain == canary<br/>Build normalized envelope<br/>Embed original raw_event

  Col->>Col: 100 events buffered OR 2 seconds elapsed
  Col->>KDS: PutRecords envelope<br/>PartitionKey = hash(meta.id)
  Note over KDS: Fan-out to 3 independent consumers

  KDS->>RTP: Trigger Lambda with event batch
  loop For each envelope in batch
    RTP->>RTP: Read payload<br/>Compute aggregation_window = 1-min bucket<br/>Compute shard_id = hash(event_id) % 10
    RTP->>DDB: UpdateItem ADD counters<br/>METRIC#GLOBAL_ACTIVITY#SHARD#{shard_id} / WINDOW#{minute}
    RTP->>DDB: UpdateItem ADD counters<br/>METRIC#WIKI_ACTIVITY#WIKI#{wiki} / WINDOW#{minute}
    alt namespace == 0
      RTP->>DDB: UpdateItem ADD events_count<br/>METRIC#TOP_PAGES#WIKI#{wiki} / WINDOW#{minute}#TITLE#{title}
    end
    RTP->>DDB: UpdateItem ADD events_count<br/>METRIC#CHANGE_TYPE#TYPE#{type} / WINDOW#{minute}
    RTP->>DDB: UpdateItem ADD events_count<br/>METRIC#NAMESPACE#NS#{namespace} / WINDOW#{minute}
  end

  RTP->>SQS: SendMessage<br/>aggregation_window=minute<br/>broadcast_window=5-second bucket<br/>DedupId=broadcast-window-{broadcast_window}
  Note over SQS: Dedup: 1 broadcast trigger per 5 seconds

  SQS->>BRD: Trigger Broadcaster Lambda
  loop For each shard 0..9
    BRD->>DDB: GetItem METRIC#GLOBAL_ACTIVITY#SHARD#{n}
  end
  BRD->>BRD: Sum all shards<br/>Compute current_minute_events_so_far<br/>Compute events_last_5s and estimated_events_per_minute

  BRD->>DBC: Scan active websocket_connections
  BRD->>BRD: Filter subscribed topics in Lambda
  DBC-->>BRD: Active connection items

  loop For each matched connectionId
    BRD->>AGW: postToConnection(connectionId, stats.update)
    alt GoneException — client disconnected silently
      BRD->>DBC: DeleteItem connectionId
    end
  end

  AGW->>DSH: JSON stats.update message
  DSH->>DSH: Update KPIs and live charts
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
  CNX->>DBC: PutItem connection_id, connected_at, topics=["global"], ttl=now+2h
  DBC-->>CNX: OK
  CNX-->>AGW: HTTP 200
  AGW-->>User: Connection established

  User->>AGW: {"action":"subscribe","topic":"wiki:frwiki"}
  AGW->>DEF: Invoke $default (connectionId, body)
  DEF->>DBC: UpdateItem append "wiki:frwiki" to topics list
  DBC-->>DEF: OK
  DEF->>AGW: postToConnection subscription.ack
  AGW-->>User: subscription.ack

  Note over BRD,User: Next 5-second broadcast cycle

  BRD->>DBC: Scan active websocket_connections
  BRD->>BRD: Filter connections whose topics contain "wiki:frwiki"
  BRD->>AGW: postToConnection(connectionId, stats.update for wiki:frwiki)
  AGW-->>User: stats.update

  User->>AGW: Close tab or network drop
  AGW->>DCN: Invoke $disconnect (connectionId)
  DCN->>DBC: DeleteItem connectionId
  DBC-->>DCN: OK
  Note over DBC: TTL covers cases where $disconnect is never fired
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

  KDS->>FH: Stream normalized envelopes with embedded raw_event
  Note over FH: Buffer: 64 MB OR 300 seconds

  FH->>S3B: JSON Lines batch (GZIP)<br/>bronze/wikimedia/recentchange/year=Y/month=M/day=D/hour=H/
  Note over S3B: Immutable Contract 2 envelope<br/>payload + raw_event preserved

  GL1->>S3B: Read bronze partition
  GL1->>GL1: Read envelope + payload<br/>Drop invalid event_id/occurred_at<br/>Cast types<br/>Serialize log_params<br/>Select known fields for Silver
  GL1->>S3S: Write Parquet (SNAPPY)<br/>silver/wikimedia/recentchange/ingestion_date=D/
  GL1->>CAT: Update partition metadata

  GL2->>S3S: Read silver partition
  GL2->>GL2: Aggregate top_wikis_by_hour<br/>bot_vs_human_by_hour<br/>change_type_distribution<br/>top_pages_by_day<br/>activity_spikes
  GL2->>S3G: Write 5 gold Parquet datasets
  GL2->>CAT: Update gold partition metadata

  ATH->>S3G: SQL scan with partition pruning
  S3G-->>ATH: Parquet column data
  ATH-->>QS: Query result set
  QS->>QS: SPICE incremental refresh
```

---

## Diagram 4 — Alert Processor

```mermaid
sequenceDiagram
  autonumber
  participant KDS as Kinesis Data Streams
  participant ALP as Alert Processor Lambda
  participant DDB as DynamoDB alert_state
  participant SNS as SNS Topic
  participant OPS as Platform Engineer

  KDS->>ALP: Trigger Lambda with event batch
  ALP->>ALP: Count event_count, log_count, delete_count, block_count

  ALP->>DDB: UpdateItem ADD counters<br/>PK=ALERT#GLOBAL SK=MINUTE#{current_minute}
  ALP->>DDB: UpdateItem ADD counters<br/>PK=ALERT#WIKI#{wiki} SK=MINUTE#{current_minute}
  ALP->>DDB: UpdateItem ADD counters<br/>PK=ALERT#LOG_TYPE#delete/block SK=MINUTE#{current_minute}

  ALP->>DDB: Query ALERT#GLOBAL last 30 minutes
  DDB-->>ALP: Rolling event_count history
  ALP->>ALP: Compute z_score

  alt z_score > 2.0
    ALP->>SNS: Publish global/wiki activity spike alert
    SNS->>OPS: Email / SMS notification
  else delete/block burst > 3x normal over 5 min
    ALP->>SNS: Publish moderation burst alert
    SNS->>OPS: Email / SMS notification
  else normal activity
    ALP->>ALP: No alert
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
  Col->>KDS: PutRecords ongoing

  Note over Col: Crash — OOM / network error / unhandled exception

  Col-xECS: Task exits (non-zero exit code)
  ECS->>CW: RunningTaskCount = 0
  CW->>CW: ALARM — ECS task count < 1

  ECS->>Col: Launch new Fargate task
  Col->>Wiki: Reconnect SSE
  Wiki-->>Col: Resume live stream from current position

  Note over KDS: Gap during downtime is not recoverable<br/>SSE is a live stream — no replay<br/>Firehose is independent from Realtime Processor<br/>but not from Collector ingestion

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
    Note over KDS: Resharding takes time and can briefly disrupt consumers
    KDS->>RTP: Higher throughput capacity available
  end

  KDS->>CW: IteratorAgeMilliseconds < 5 000 ms
  CW->>OPS: OK — alarm resolved
```
