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
  participant SQS as SQS FIFO Broadcast Queue
  participant BRD as Broadcaster Lambda
  participant DBC as DynamoDB websocket_connections
  participant AGW as API Gateway WebSocket
  participant DSH as Frontend Dashboard

  Wiki->>Col: SSE event data JSON
  Note over Col: Parse JSON<br/>Validate meta.id and meta.dt<br/>Drop meta.domain == canary<br/>Build normalized envelope<br/>Embed original raw_event

  Col->>Col: Buffer event
  Note over Col: Flush when 100 events buffered<br/>OR 2 seconds elapsed<br/>OR shutdown signal received

  Col->>KDS: PutRecords envelope<br/>PartitionKey = hash(meta.id)
  Note over KDS: Kinesis is the central fan-out backbone<br/>Consumers: Realtime Processor, Firehose, Alert Processor

  KDS->>RTP: Trigger Lambda with event batch

  loop For each normalized envelope in batch
    RTP->>RTP: Read stable payload<br/>Compute aggregation_window = 1-minute bucket<br/>Compute shard_id = hash(event_id) % 10

    RTP->>DDB: UpdateItem ADD counters<br/>PK=METRIC#GLOBAL_ACTIVITY#SHARD#{shard_id}<br/>SK=WINDOW#{aggregation_window}

    RTP->>DDB: UpdateItem ADD counters<br/>PK=METRIC#WIKI_ACTIVITY#WIKI#{wiki}<br/>SK=WINDOW#{aggregation_window}

    alt namespace == 0
      RTP->>DDB: UpdateItem ADD events_count<br/>PK=METRIC#TOP_PAGES#WIKI#{wiki}<br/>SK=WINDOW#{aggregation_window}#TITLE#{title}
    end

    RTP->>DDB: UpdateItem ADD events_count<br/>PK=METRIC#CHANGE_TYPE#TYPE#{change_type}<br/>SK=WINDOW#{aggregation_window}

    RTP->>DDB: UpdateItem ADD events_count<br/>PK=METRIC#NAMESPACE#NS#{namespace}<br/>SK=WINDOW#{aggregation_window}
  end

  RTP->>SQS: SendMessage<br/>aggregation_window=1-minute bucket<br/>broadcast_window=5-second bucket<br/>DedupId=broadcast-window-{broadcast_window}
  Note over SQS: Deduplicates broadcast triggers<br/>One broadcaster invocation per 5-second window<br/>No separate 5-second aggregate is stored

  SQS->>BRD: Trigger Broadcaster Lambda

  loop For each global shard 0..9
    BRD->>DDB: GetItem<br/>PK=METRIC#GLOBAL_ACTIVITY#SHARD#{n}<br/>SK=WINDOW#{aggregation_window}
  end

  BRD->>BRD: Sum global shards<br/>Compute current_minute_events_so_far<br/>Compute bot_ratio and human_ratio

  BRD->>DDB: Read last completed minute aggregates
  DDB-->>BRD: Stable completed-minute counters

  BRD->>DBC: Scan active websocket_connections
  DBC-->>BRD: Active connection items
  BRD->>BRD: Filter subscribed topics in Lambda

  loop For each matched connectionId
    BRD->>AGW: postToConnection(connectionId, stats.update)
    alt GoneException / HTTP 410
      BRD->>DBC: DeleteItem stale connectionId
    end
  end

  AGW->>DSH: stats.update JSON message
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

  User->>AGW: WebSocket connect wss://...
  AGW->>CNX: Invoke $connect route<br/>connectionId
  CNX->>DBC: PutItem<br/>connection_id, connected_at, topics=["global"], ttl=now+2h
  DBC-->>CNX: OK
  CNX-->>AGW: HTTP 200
  AGW-->>User: Connection established

  Note over User,DBC: Client is subscribed to global stats by default

  User->>AGW: {"action":"subscribe","topic":"wiki:frwiki"}
  AGW->>DEF: Invoke $default route<br/>connectionId + message body
  DEF->>DBC: UpdateItem<br/>append "wiki:frwiki" to topics list
  DBC-->>DEF: OK
  DEF->>AGW: postToConnection subscription.ack
  AGW-->>User: {"type":"subscription.ack","topic":"wiki:frwiki","status":"subscribed"}

  Note over BRD,User: Next 5-second broadcast cycle

  BRD->>DBC: Scan active websocket_connections
  DBC-->>BRD: Active connection items
  BRD->>BRD: Filter connections whose topics contain "wiki:frwiki"

  BRD->>AGW: postToConnection(connectionId, stats.update for wiki:frwiki)
  AGW-->>User: stats.update

  User->>AGW: Close tab or network drop
  AGW->>DCN: Invoke $disconnect route<br/>connectionId
  DCN->>DBC: DeleteItem connectionId
  DBC-->>DCN: OK

  Note over DBC: TTL covers ghost connections<br/>when $disconnect is not delivered
```

---

## Diagram 3 — Historical Archive Pipeline

```mermaid
sequenceDiagram
  autonumber
  participant KDS as Kinesis Data Streams
  participant FH as Kinesis Firehose
  participant S3B as S3 Bronze
  participant GL1 as Glue bronze-to-silver
  participant S3S as S3 Silver
  participant GL2 as Glue silver-to-gold
  participant S3G as S3 Gold
  participant CAT as Glue Data Catalog
  participant ATH as Athena
  participant QS as QuickSight

  KDS->>FH: Stream normalized envelopes<br/>with embedded raw_event
  Note over FH: Buffer 64 MB OR 300 seconds<br/>whichever comes first

  FH->>S3B: JSON Lines batch GZIP<br/>bronze/wikimedia/recentchange/year=Y/month=M/day=D/hour=H/
  Note over S3B: Immutable Contract 2 envelope<br/>payload + raw_event preserved<br/>Bronze is the source-fidelity archive

  GL1->>S3B: Read Bronze partition
  GL1->>GL1: Read envelope-level fields and payload<br/>Drop invalid event_id or occurred_at<br/>Cast null-safe types<br/>Serialize log_params<br/>Select known fields for Silver

  GL1->>S3S: Write Parquet SNAPPY<br/>silver/wikimedia/recentchange/ingestion_date=D/
  GL1->>CAT: Update Silver table partition metadata

  GL2->>S3S: Read Silver partition
  GL2->>GL2: Aggregate top_wikis_by_hour<br/>Aggregate bot_vs_human_by_hour<br/>Aggregate change_type_distribution<br/>Aggregate top_pages_by_day<br/>Compute activity_spikes

  GL2->>S3G: Write Gold Parquet datasets
  GL2->>CAT: Update Gold table partition metadata

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
  ALP->>ALP: Read payload from each envelope
  ALP->>ALP: Count event_count, log_count, delete_count, block_count

  ALP->>DDB: UpdateItem ADD counters<br/>PK=ALERT#GLOBAL<br/>SK=MINUTE#{current_minute}

  loop For each wiki in batch
    ALP->>DDB: UpdateItem ADD counters<br/>PK=ALERT#WIKI#{wiki}<br/>SK=MINUTE#{current_minute}
  end

  alt delete or block log events exist
    ALP->>DDB: UpdateItem ADD counters<br/>PK=ALERT#LOG_TYPE#delete/block<br/>SK=MINUTE#{current_minute}
  end

  ALP->>DDB: Query ALERT#GLOBAL<br/>last 30 minutes
  DDB-->>ALP: Rolling global event_count history
  ALP->>ALP: Compute rolling average, stddev, z_score

  ALP->>DDB: Query ALERT#LOG_TYPE#delete/block<br/>last 5 minutes
  DDB-->>ALP: Rolling moderation action history
  ALP->>ALP: Detect delete/block burst

  alt z_score > 2.0
    ALP->>SNS: Publish global or wiki activity spike alert
    SNS->>OPS: Email / SMS notification
  else delete/block burst > 3x normal over 5 minutes
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
  participant ECS as ECS Fargate Service
  participant Col as SSE Collector Task
  participant KDS as Kinesis Data Streams
  participant CW as CloudWatch Alarm

  Col->>Wiki: SSE connection open
  Wiki-->>Col: Live stream of events
  Col->>KDS: PutRecords ongoing

  Note over Col: Crash<br/>OOM, network error, or unhandled exception

  Col-xECS: Task exits with non-zero exit code
  ECS->>CW: RunningTaskCount = 0
  CW->>CW: ALARM — ECS task count < 1

  Note over ECS: ECS service scheduler replaces failed task

  ECS->>Col: Launch new Fargate task
  Col->>Wiki: Reconnect SSE
  Wiki-->>Col: Resume live stream from current position

  Note over KDS: Events missed during downtime are not recoverable<br/>SSE is a live stream, not a replayable source<br/>Firehose is independent from Realtime Processor<br/>but not from Collector ingestion

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
  participant CW as CloudWatch Alarm
  participant OPS as Platform Engineer

  Note over KDS,RTP: Normal operation<br/>IteratorAge < 5 000 ms

  KDS->>RTP: Traffic spike<br/>high event volume
  RTP->>DDB: High write rate
  DDB-->>RTP: ProvisionedThroughputExceededException or throttling

  RTP->>KDS: Processing slows down<br/>records accumulate in stream
  KDS->>CW: IteratorAgeMilliseconds > 60 000 ms
  CW->>OPS: ALARM — Kinesis iterator age high

  OPS->>DDB: Switch affected table to on-demand billing<br/>aws dynamodb update-table --billing-mode PAY_PER_REQUEST
  DDB-->>OPS: Throttling reduced

  alt Still lagging after DynamoDB fix
    OPS->>KDS: Increase shard count<br/>aws kinesis update-shard-count --target-shard-count 4
    Note over KDS: Resharding takes time<br/>and can briefly disrupt consumers
    KDS->>RTP: Higher throughput capacity available
  end

  KDS->>CW: IteratorAgeMilliseconds < 5 000 ms
  CW->>OPS: OK — alarm resolved
```
