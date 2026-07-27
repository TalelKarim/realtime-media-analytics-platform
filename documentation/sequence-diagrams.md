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
  Note over Col: Validate meta.id/meta.dt<br/>Drop canary<br/>Deterministic SAMPLE_RATE<br/>Build normalized envelope<br/>Embed raw_event

  Col->>Col: Append normalized event to buffer
  Note over Col: Flush at 100 records<br/>OR 2 seconds<br/>OR shutdown

  Col->>Col: Start collector.flush_to_kinesis producer span<br/>Inject same W3C context into records of this flush
  Col->>KDS: PutRecords envelopes<br/>PartitionKey = normalized event_id

  KDS->>RTP: Trigger Lambda with event batch
  Note over RTP: Batch can contain records from multiple Collector flushes<br/>First unique producer context = parent<br/>Additional contexts = span links

  loop For each envelope
    RTP->>RTP: Decode and normalize<br/>Compute 1-minute window<br/>Build counters in memory
  end

  par Activity counters
    RTP->>DDB: Global / wiki activity counters
  and Change-type counters
    RTP->>DDB: Global / wiki change-type counters
  and Bot counters
    RTP->>DDB: Global / wiki bot counters
  and Namespace counters
    RTP->>DDB: Global / wiki namespace counters
  and Top read models
    RTP->>DDB: TOP_WIKIS and namespace-0 TOP_PAGES counters
  end

  RTP->>SQS: SendMessage FIFO<br/>broadcast_window = 3-second bucket<br/>timestamp bounds by aggregation window<br/>W3C context in MessageAttributes<br/>GroupId=realtime-broadcast<br/>DedupId=BROADCAST#{broadcast_window}
  Note over SQS: At most one signal per deduplication key<br/>No separate 3-second aggregate is stored

  SQS->>BRD: Trigger Broadcaster Lambda batch_size=1
  BRD->>DBC: Scan connection_id, topics, ttl
  DBC-->>BRD: Active connection items
  BRD->>BRD: Skip expired items<br/>Group subscriptions by topic

  par Exact counters
    BRD->>DDB: BatchGetItem exact global counters
  and Top wikis
    BRD->>DDB: Parallel Query TOP_WIKIS shards
  and Top pages
    BRD->>DDB: Parallel Query TOP_PAGES shards
  and Active wiki topics
    BRD->>DDB: BatchGetItem + parallel TOP_PAGES reads
  end

  BRD->>BRD: Build global/wiki/top_pages payloads

  par Connection A
    BRD->>AGW: postToConnection(connectionIdA, stats.update)
  and Connection B
    BRD->>AGW: postToConnection(connectionIdB, stats.update)
  and Connection N
    BRD->>AGW: postToConnection(connectionIdN, stats.update)
  end

  alt GoneException / HTTP 410
    BRD->>DBC: DeleteItem stale connectionId
  end

  AGW->>DSH: stats.update JSON message
  DSH->>DSH: Update KPIs and live charts
```

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

  Note over BRD,User: Next configured broadcast cycle

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
  Note over GL1,CAT: Silver table uses projected ingestion_date partitions

  GL2->>S3S: Read Silver partition
  GL2->>GL2: Aggregate top_wikis_by_hour<br/>Aggregate bot_vs_human_by_hour<br/>Aggregate change_type_distribution<br/>Aggregate top_pages_by_day<br/>Compute activity_spikes

  GL2->>S3G: Write Gold Parquet datasets
  Note over GL2,CAT: Gold tables use projected time partitions

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
  ALP->>ALP: Decode Kinesis records<br/>Validate event_type=wiki.recentchange<br/>Extract wiki, change_type, log_type, occurred_at
  ALP->>ALP: Aggregate counters in memory<br/>by alert_key/window_key

  ALP->>DDB: UpdateItem ADD counters<br/>PK=ALERT#GLOBAL<br/>SK=WINDOW#{event_minute}

  loop For each wiki touched by batch
    ALP->>DDB: UpdateItem ADD counters<br/>PK=ALERT#WIKI#{wiki}<br/>SK=WINDOW#{event_minute}
  end

  alt delete log events exist
    ALP->>DDB: UpdateItem ADD counters<br/>PK=ALERT#LOG_TYPE#delete<br/>SK=WINDOW#{event_minute}
  end

  alt block log events exist
    ALP->>DDB: UpdateItem ADD counters<br/>PK=ALERT#LOG_TYPE#block<br/>SK=WINDOW#{event_minute}
  end

  ALP->>ALP: Select latest completed eligible window<br/>using EVALUATION_DELAY_SECONDS

  loop For ALERT#GLOBAL and touched ALERT#WIKI#{wiki}
    ALP->>DDB: GetItem current completed window<br/>PK=alert_key<br/>SK=WINDOW#{evaluation_minute}
    DDB-->>ALP: Current completed-window counters

    ALP->>DDB: Query previous 30 completed windows<br/>PK=alert_key
    DDB-->>ALP: Baseline event_count points

    ALP->>ALP: If baseline_points >= MIN_BASELINE_POINTS<br/>compute average, stddev, z_score

    alt z_score > threshold and current_count >= minimum
      ALP->>DDB: Reserve alert<br/>SET alert_status=PUBLISHING<br/>IF attribute_not_exists(alert_status)
      alt reservation succeeds
        ALP->>SNS: Publish activity spike alert
        SNS->>OPS: Email / SMS notification
        ALP->>DDB: Mark alert SENT<br/>SET alert_status=SENT<br/>SET alert_sent_at=now
      else already reserved or sent
        ALP->>ALP: Deduplicate<br/>Do not publish SNS
      end
    else normal or insufficient baseline
      ALP->>ALP: No SNS
    end
  end

  loop For ALERT#LOG_TYPE#delete and ALERT#LOG_TYPE#block when touched
    ALP->>DDB: Query current moderation window<br/>last 5 completed minutes
    DDB-->>ALP: Current 5-minute delete/block count

    ALP->>DDB: Query historical moderation windows
    DDB-->>ALP: Baseline 5-minute group counts

    ALP->>ALP: Compute burst_ratio

    alt burst_ratio > threshold and current_5m_count >= minimum
      ALP->>DDB: Reserve alert<br/>SET alert_status=PUBLISHING<br/>IF attribute_not_exists(alert_status)
      alt reservation succeeds
        ALP->>SNS: Publish moderation burst alert
        SNS->>OPS: Email / SMS notification
        ALP->>DDB: Mark alert SENT<br/>SET alert_status=SENT<br/>SET alert_sent_at=now
      else already reserved or sent
        ALP->>ALP: Deduplicate<br/>Do not publish SNS
      end
    else normal or insufficient baseline
      ALP->>ALP: No SNS
    end
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


---

## Diagram 7 — OpenTelemetry Trace Propagation

```mermaid
sequenceDiagram
  autonumber
  participant Col as ECS Collector
  participant Alloy as Grafana Alloy Sidecar
  participant KDS as Kinesis
  participant RTP as Realtime Processor
  participant Ext1 as OTel Collector Extension
  participant SQS as SQS FIFO
  participant BRD as Broadcaster
  participant Ext2 as OTel Collector Extension
  participant GC as Grafana Cloud

  Col->>Col: Start collector.flush_to_kinesis producer span
  Col->>KDS: PutRecords with trace_context in each envelope
  Col->>Alloy: Export Collector metrics and spans over OTLP
  Alloy->>GC: Export to Mimir and Tempo

  KDS->>RTP: Deliver Kinesis batch
  RTP->>RTP: Extract producer contexts
  Note over RTP: One direct parent<br/>Additional unique producer contexts become span links
  RTP->>RTP: Process and update DynamoDB
  RTP->>SQS: Send signal with traceparent/tracestate/baggage
  RTP->>Ext1: Flush metrics and traces to localhost
  Ext1->>GC: Export to Mimir and Tempo

  SQS->>BRD: Deliver signal
  BRD->>BRD: Extract W3C parent context
  BRD->>BRD: Read aggregates and fan out
  BRD->>Ext2: Flush metrics and traces to localhost
  Ext2->>GC: Export to Mimir and Tempo

  Note over Col,BRD: Trace continuity represents technical causality.<br/>Shared DynamoDB snapshots are not exact per-event lineage.
```
