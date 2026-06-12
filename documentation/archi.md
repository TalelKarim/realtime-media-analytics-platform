# Architecture Diagrams — Realtime Media Analytics Platform
# Format: Mermaid — renderable in GitHub, GitLab, Notion, VSCode

---

## DIAGRAM 1 — C4 Level 1 : System Context

```mermaid
C4Context
  title System Context — Realtime Media Analytics Platform

  Person(analyst, "Media Analyst", "Monitors live Wikipedia activity via the dashboard")
  Person(ops, "Platform Engineer", "Operates and monitors the platform infrastructure")

  System(platform, "Realtime Media Analytics Platform", "Ingests Wikimedia EventStreams, processes events in real time, serves live dashboards, and archives data for historical analysis")

  System_Ext(wikimedia, "Wikimedia EventStreams", "Public SSE stream of all changes across Wikipedia, Wikidata, Wikimedia Commons — ~1000 events/sec")
  System_Ext(quicksight, "Amazon QuickSight", "Business intelligence dashboards for historical analytics")
  System_Ext(sns_email, "Email / SMS", "Alert notifications for anomaly detection")

  Rel(wikimedia, platform, "Streams recentchange events", "SSE / HTTPS")
  Rel(platform, analyst, "Pushes real-time stats", "WebSocket")
  Rel(platform, quicksight, "Exposes historical data", "Athena / S3")
  Rel(platform, sns_email, "Sends spike alerts", "SNS")
  Rel(ops, platform, "Monitors and operates", "CloudWatch / Terraform")
  Rel(analyst, quicksight, "Views historical dashboards", "HTTPS")
```

---

## DIAGRAM 2 — C4 Level 2 : Container Diagram

```mermaid
C4Container
  title Container Diagram — Realtime Media Analytics Platform

  System_Ext(wikimedia, "Wikimedia EventStreams", "SSE stream ~1000 events/sec")
  Person(analyst, "Media Analyst", "")

  Container_Boundary(ingestion, "Ingestion Layer") {
    Container(collector, "SSE Collector", "ECS Fargate / Python", "Maintains SSE connection, parses, normalizes and batches Wikimedia events")
  }

  Container_Boundary(streaming, "Streaming Backbone") {
    Container(kinesis, "Kinesis Data Streams", "AWS Kinesis", "Central event backbone — fan-out point for all consumers")
  }

  Container_Boundary(realtime, "Real-Time Processing") {
    Container(rt_processor, "Realtime Processor", "AWS Lambda", "Computes windowed aggregates, write-sharded DynamoDB updates")
    Container(dynamodb, "DynamoDB", "AWS DynamoDB", "Stores real-time aggregates and WebSocket connection registry")
    Container(sqs, "Broadcast Signal Queue", "AWS SQS FIFO", "Deduplicates broadcast triggers by time window")
    Container(broadcaster, "Broadcaster", "AWS Lambda", "Reads aggregates and pushes snapshots to all connected clients")
    Container(apigw, "API Gateway WebSocket", "AWS API Gateway", "Manages persistent WebSocket connections with dashboard clients")
    Container(dashboard, "Live Dashboard", "React / WebSocket", "Real-time visualization of Wikimedia activity")
  }

  Container_Boundary(historical, "Historical Analytics") {
    Container(firehose, "Firehose Delivery Stream", "AWS Kinesis Firehose", "Buffers and delivers raw events to S3")
    Container(s3, "S3 Data Lake", "AWS S3", "Bronze / Silver / Gold zones — raw, cleaned, aggregated")
    Container(glue, "Glue ETL Jobs", "AWS Glue", "Transforms bronze→silver→gold on hourly schedule")
    Container(athena, "Athena", "AWS Athena", "SQL query engine on S3 Parquet data")
  }

  Container_Boundary(alerting, "Alerting") {
    Container(alert_proc, "Alert Processor", "AWS Lambda", "Detects spikes and anomalies in event volume")
    Container(sns, "SNS Topic", "AWS SNS", "Delivers alerts via email or SMS")
  }

  Rel(wikimedia, collector, "SSE events", "HTTPS / SSE")
  Rel(collector, kinesis, "Normalized events", "PutRecords")
  Rel(kinesis, rt_processor, "Event batches", "Kinesis trigger")
  Rel(kinesis, firehose, "Raw events", "Kinesis consumer")
  Rel(kinesis, alert_proc, "Event batches", "Kinesis trigger")
  Rel(rt_processor, dynamodb, "Atomic counter updates", "UpdateItem ADD")
  Rel(rt_processor, sqs, "Broadcast signal", "SendMessage FIFO")
  Rel(sqs, broadcaster, "Deduplicated signal", "SQS trigger")
  Rel(broadcaster, dynamodb, "Read aggregates + connections", "Query + DeleteItem")
  Rel(broadcaster, apigw, "Push snapshots", "postToConnection")
  Rel(apigw, dashboard, "stats.update messages", "WebSocket")
  Rel(analyst, dashboard, "Views live metrics", "Browser")
  Rel(firehose, s3, "Raw JSON events", "S3 delivery")
  Rel(glue, s3, "Read bronze, write silver/gold", "S3 read/write")
  Rel(athena, s3, "SQL scans", "S3 read")
  Rel(alert_proc, sns, "Spike alerts", "Publish")
```

---

## DIAGRAM 3 — Sequence : Full Ingestion → Realtime Dashboard Flow

```mermaid
sequenceDiagram
  autonumber
  actor Wiki as Wikimedia EventStreams
  participant Col as ECS Fargate<br/>Collector
  participant KDS as Kinesis<br/>Data Streams
  participant RTP as Realtime Processor<br/>Lambda
  participant DDB as DynamoDB<br/>realtime_aggregates
  participant SQS as SQS FIFO<br/>Broadcast Queue
  participant BRD as Broadcaster<br/>Lambda
  participant DBC as DynamoDB<br/>websocket_connections
  participant AGW as API Gateway<br/>WebSocket
  participant DSH as Frontend<br/>Dashboard

  Wiki->>Col: SSE event (edit/new/categorize/log/external)
  Note over Col: Parse JSON<br/>Validate meta.id + meta.dt<br/>Normalize all fields to null-safe contract<br/>Buffer in-memory

  Col->>Col: Buffer reaches 100 events OR 2 sec elapsed
  Col->>KDS: PutRecords (batch, PartitionKey=hash(meta.id))
  Note over KDS: Fan-out to 3 consumers:<br/>Realtime Processor<br/>Firehose<br/>Alert Processor

  KDS->>RTP: Trigger Lambda with batch
  loop For each event in batch
    RTP->>RTP: Compute window key (minute bucket)<br/>Compute shard_id = hash(event_id) % 10
    RTP->>DDB: UpdateItem ADD counters<br/>PK=METRIC#GLOBAL_ACTIVITY#SHARD#{shard_id}<br/>SK=WINDOW#{minute}
    RTP->>DDB: UpdateItem ADD counters<br/>PK=METRIC#WIKI_ACTIVITY#WIKI#{wiki}<br/>SK=WINDOW#{minute}
    alt namespace == 0
      RTP->>DDB: UpdateItem ADD events_count<br/>PK=METRIC#TOP_PAGES#WIKI#{wiki}<br/>SK=WINDOW#{minute}#TITLE#{title}
    end
    RTP->>DDB: UpdateItem ADD events_count<br/>PK=METRIC#CHANGE_TYPE#TYPE#{type}<br/>SK=WINDOW#{minute}
  end

  RTP->>SQS: SendMessage (MessageDeduplicationId=window-key)<br/>{"message_type":"aggregates.updated","window":"...","topics":[...]}
  Note over SQS: FIFO dedup: only 1 message per window<br/>Even if 50 Lambda invocations send the same window key

  SQS->>BRD: Trigger Broadcaster Lambda
  loop For each shard 0..9
    BRD->>DDB: GetItem METRIC#GLOBAL_ACTIVITY#SHARD#{n}
  end
  BRD->>BRD: Merge shards → compute totals + bot_ratio

  BRD->>DBC: Query connections by topic
  DBC-->>BRD: List of active connectionIds

  loop For each connectionId
    BRD->>AGW: postToConnection(connectionId, stats.update)
    alt GoneException (client disconnected silently)
      BRD->>DBC: DeleteItem connectionId
    end
  end

  AGW->>DSH: {"type":"stats.update","data":{...}}
  DSH->>DSH: Update charts and KPIs
```

---

## DIAGRAM 4 — Sequence : WebSocket Lifecycle (Connect → Subscribe → Receive → Disconnect)

```mermaid
sequenceDiagram
  autonumber
  actor User as Browser / Analyst
  participant AGW as API Gateway<br/>WebSocket
  participant CNX as Connect Handler<br/>Lambda
  participant DEF as Default Handler<br/>Lambda
  participant DCN as Disconnect Handler<br/>Lambda
  participant DBC as DynamoDB<br/>websocket_connections
  participant BRD as Broadcaster<br/>Lambda

  User->>AGW: WebSocket connect (wss://...)
  AGW->>CNX: Invoke $connect (connectionId)
  CNX->>DBC: PutItem {connection_id, connected_at, topics:["global"], ttl: now+2h}
  DBC-->>CNX: OK
  CNX-->>AGW: HTTP 200
  AGW-->>User: Connection established

  Note over User,DBC: Client is now receiving global stats by default

  User->>AGW: Send {"action":"subscribe","topic":"wiki:frwiki"}
  AGW->>DEF: Invoke $default (connectionId, body)
  DEF->>DBC: UpdateItem — append "wiki:frwiki" to topics list
  DBC-->>DEF: OK
  DEF->>AGW: postToConnection → {"type":"subscription.ack","topic":"wiki:frwiki","status":"subscribed"}
  AGW-->>User: subscription.ack received

  Note over User,BRD: Next broadcast cycle

  BRD->>DBC: Query connections where topics contains "wiki:frwiki"
  DBC-->>BRD: [connectionId, ...]
  BRD->>AGW: postToConnection(connectionId, stats.update for wiki:frwiki)
  AGW-->>User: {"type":"stats.update","topic":"wiki:frwiki","data":{...}}

  User->>AGW: Close tab / disconnect
  AGW->>DCN: Invoke $disconnect (connectionId)
  DCN->>DBC: DeleteItem connectionId
  DBC-->>DCN: OK
  Note over DBC: Connection removed — no more pushes for this client
```

---

## DIAGRAM 5 — Sequence : Historical Archive Pipeline (Kinesis → S3 → Glue → Athena)

```mermaid
sequenceDiagram
  autonumber
  participant KDS as Kinesis<br/>Data Streams
  participant FH as Kinesis<br/>Firehose
  participant S3B as S3<br/>Bronze Zone
  participant GLU as Glue ETL<br/>bronze→silver
  participant S3S as S3<br/>Silver Zone
  participant GL2 as Glue ETL<br/>silver→gold
  participant S3G as S3<br/>Gold Zone
  participant CAT as Glue<br/>Data Catalog
  participant ATH as Athena
  participant QS as QuickSight

  KDS->>FH: Stream events (Kinesis source connector)
  Note over FH: Buffer: 64MB OR 5 minutes<br/>whichever comes first

  FH->>S3B: Deliver compressed JSON batch<br/>bronze/wikimedia/recentchange/<br/>year=2026/month=06/day=11/hour=16/<br/>filename.json.gz
  Note over S3B: Raw event — full fidelity<br/>Immutable archive<br/>All 5 event types preserved

  Note over GLU: Triggered hourly by Glue Scheduler<br/>Processes previous hour's bronze partition

  GLU->>S3B: Read bronze partition (previous hour)
  GLU->>GLU: Drop events with null meta.id or meta.dt<br/>Cast bot→boolean, minor→boolean<br/>Compute delta_bytes = new_length - old_length<br/>Normalize wiki to lowercase<br/>Serialize log_params as JSON string<br/>Select known columns only
  GLU->>S3S: Write Parquet (SNAPPY compression)<br/>silver/wikimedia/recentchange/<br/>ingestion_date=2026-06-11/<br/>part-00000.parquet
  GLU->>CAT: Update partition metadata

  Note over GL2: Triggered hourly (after bronze→silver)<br/>Processes same time window

  GL2->>S3S: Read silver partition (same hour)
  GL2->>GL2: Aggregate top_wikis_by_hour<br/>Aggregate bot_vs_human_by_hour<br/>Aggregate change_type_distribution<br/>Aggregate top_pages_by_day (namespace=0 only)<br/>Compute z_score for activity_spikes
  GL2->>S3G: Write 5 gold Parquet datasets<br/>(partitioned by time)
  GL2->>CAT: Update gold partition metadata

  Note over ATH: Partition projection — no MSCK REPAIR needed

  ATH->>S3G: SQL scan (partition pruned)<br/>SELECT wiki, COUNT(*) FROM top_wikis_by_hour<br/>WHERE hour_window >= now - 24h
  S3G-->>ATH: Parquet columns returned
  ATH-->>QS: Query results

  QS->>QS: SPICE incremental refresh (hourly)<br/>Rebuild historical dashboards
```

---

## DIAGRAM 6 — Sequence : Alert Processor (Spike Detection)

```mermaid
sequenceDiagram
  autonumber
  participant KDS as Kinesis<br/>Data Streams
  participant ALP as Alert Processor<br/>Lambda
  participant SNS as SNS Topic
  participant OPS as Platform Engineer<br/>Email / SMS

  KDS->>ALP: Trigger Lambda with 1-minute batch
  ALP->>ALP: Count events in batch<br/>Load rolling 30-min window from state<br/>(in-memory V1 — DynamoDB persistence in V2)

  ALP->>ALP: Compute z_score<br/>= (current_count - rolling_avg) / rolling_stddev

  alt z_score > 2.0 — global spike detected
    ALP->>ALP: Build alert payload<br/>{"wiki":"global","events":1840,"avg":1240,"z_score":2.7,"window":"..."}
    ALP->>SNS: Publish alert message
    SNS->>OPS: Email / SMS notification<br/>"Activity spike: 1840 events/min vs avg 1240 (z=2.7)"
  else log_type burst — moderation spike
    Note over ALP: delete|block events > 3x normal in 5 min<br/>→ potential coordinated vandalism
    ALP->>SNS: Publish moderation alert<br/>"45 deletions in 5 min (normal: 12)"
    SNS->>OPS: Email / SMS notification
  else normal activity
    ALP->>ALP: Update rolling window state
    Note over ALP: No alert — continue monitoring
  end
```

---

## DIAGRAM 7 — Sequence : Collector Crash Recovery

```mermaid
sequenceDiagram
  autonumber
  participant Wiki as Wikimedia EventStreams
  participant ECS as ECS Fargate<br/>Service
  participant Col as SSE Collector<br/>Task
  participant KDS as Kinesis<br/>Data Streams
  participant CW as CloudWatch<br/>Alarm

  Col->>Wiki: SSE connection open
  Wiki-->>Col: Stream events continuously

  Note over Col: Network error / OOM / crash

  Col-xECS: Task exits (non-zero exit code)
  ECS->>CW: RunningTaskCount = 0
  CW->>CW: Alarm: ECS task count < 1

  Note over ECS: ECS service restart policy triggers

  ECS->>Col: Launch new Fargate task
  Col->>Wiki: Reconnect SSE (Last-Event-ID not guaranteed after crash)
  Wiki-->>Col: Resume live stream from current position

  Note over KDS: Gap during downtime is not recoverable<br/>SSE is a live stream — no replay from last offset<br/>Last-Event-ID persistence is a V2 improvement<br/>Firehose archive unaffected (independent consumer)

  Col->>KDS: Resume PutRecords
  CW->>CW: Alarm resolves: RunningTaskCount = 1
```

---

## DIAGRAM 8 — Sequence : Kinesis High Iterator Age Recovery

```mermaid
sequenceDiagram
  autonumber
  participant KDS as Kinesis<br/>Data Streams
  participant RTP as Realtime Processor<br/>Lambda
  participant DDB as DynamoDB
  participant CW as CloudWatch<br/>Alarms
  participant OPS as Platform Engineer

  Note over KDS,RTP: Normal operation<br/>IteratorAge < 5 seconds

  KDS->>RTP: High volume of events (traffic spike)
  RTP->>DDB: UpdateItem (high write rate)
  DDB->>DDB: Throttling — write capacity exceeded

  RTP->>KDS: Processing slows down
  KDS->>CW: IteratorAgeMilliseconds > 60000ms
  CW->>OPS: Alarm: Kinesis iterator age high

  OPS->>DDB: Switch billing mode to on-demand<br/>aws dynamodb update-table --billing-mode PAY_PER_REQUEST
  DDB-->>OPS: Throttling resolved

  alt Still lagging
    OPS->>KDS: Increase shard count<br/>aws kinesis update-shard-count --target-shard-count 4
    Note over KDS: Resharding takes ~30 seconds<br/>Brief disruption to consumers
    KDS->>RTP: Higher throughput available
  end

  KDS->>CW: IteratorAgeMilliseconds < 5000ms
  CW->>OPS: Alarm resolves
```

---

## DIAGRAM 9 — Data Flow : All Contracts in One View

```mermaid
flowchart TD
  subgraph SOURCE["🌐 External Source"]
    W["Wikimedia EventStreams\nSSE stream — 5 event types\n~1000 events/sec"]
  end

  subgraph INGESTION["📥 Ingestion — ECS Fargate"]
    C["SSE Collector\n• reads SSE\n• normalizes to Contract 2\n• batches 100 events / 2s"]
  end

  subgraph BACKBONE["⚡ Streaming Backbone"]
    K["Kinesis Data Streams\nPartitionKey = hash(meta.id)\nContract 2 — Normalized Event"]
  end

  subgraph REALTIME["🔴 Real-Time Processing"]
    RP["Realtime Processor Lambda\nContract 3a-3e → DynamoDB"]
    DBA["DynamoDB\nrealtime_aggregates\nContract 3a Global Activity\nContract 3b Wiki Activity\nContract 3c Top Pages\nContract 3d Change Types\nContract 3e Namespaces"]
    SQ["SQS FIFO\nContract 5 — Broadcast Signal\nDedup by window key"]
    BR["Broadcaster Lambda\nReads Contract 3 + 4\nBuilds Contract 6"]
    DBC["DynamoDB\nwebsocket_connections\nContract 4"]
    AGW["API Gateway WebSocket\nContract 6 — stats.update\nContract 7 — subscribe\nContract 8 — ack/error"]
    DSH["Frontend Dashboard\nLive visualization"]
  end

  subgraph HISTORICAL["📦 Historical Analytics"]
    FH["Kinesis Firehose\nBuffer 64MB / 5min"]
    S3B["S3 Bronze\nContract 9 — Raw JSON\nImmutable archive"]
    GLU["Glue ETL bronze→silver\nHourly"]
    S3S["S3 Silver\nContract 10 — Parquet typed\nNull-safe schema"]
    GL2["Glue ETL silver→gold\nHourly"]
    S3G["S3 Gold\nContract 11 — Pre-aggregated\n5 datasets"]
    ATH["Athena\nSQL on S3\nPartition projection"]
    QS["QuickSight\nHistorical dashboards\nSPICE refresh hourly"]
  end

  subgraph ALERTING["🚨 Alerting"]
    AL["Alert Processor Lambda\nz-score spike detection"]
    SNS["SNS Topic\nEmail / SMS alerts"]
  end

  W -->|"SSE / HTTPS\nContract 1 — Raw Event"| C
  C -->|"PutRecords\nContract 2 — Normalized"| K
  K --> RP
  K --> FH
  K --> AL
  RP --> DBA
  RP --> SQ
  SQ --> BR
  BR --> DBA
  BR --> DBC
  BR --> AGW
  AGW --> DSH
  FH --> S3B
  S3B --> GLU
  GLU --> S3S
  S3S --> GL2
  GL2 --> S3G
  S3G --> ATH
  ATH --> QS
  AL --> SNS

  style SOURCE fill:#1a1a2e,stroke:#4a9eff,color:#fff
  style INGESTION fill:#16213e,stroke:#4a9eff,color:#fff
  style BACKBONE fill:#0f3460,stroke:#4a9eff,color:#fff
  style REALTIME fill:#1a1a2e,stroke:#e94560,color:#fff
  style HISTORICAL fill:#16213e,stroke:#f5a623,color:#fff
  style ALERTING fill:#1a1a2e,stroke:#ff6b6b,color:#fff
```

---

## DIAGRAM 10 — DynamoDB Access Patterns

```mermaid
erDiagram
  REALTIME_AGGREGATES {
    string PK "METRIC#GLOBAL_ACTIVITY#SHARD#n"
    string SK "WINDOW#2026-06-11T16:44"
    number events_count
    number bot_events
    number human_events
    number edit_events
    number new_events
    number categorize_events
    number log_events
    number external_events
    number ttl "window_start + 2h"
  }

  WIKI_AGGREGATES {
    string PK "METRIC#WIKI_ACTIVITY#WIKI#enwiki"
    string SK "WINDOW#2026-06-11T16:44"
    number events_count
    number bot_events
    number human_events
    number edit_events
    number new_events
    number categorize_events
    number log_events
    number external_events
    number ttl "window_start + 2h"
  }

  TOP_PAGES {
    string PK "METRIC#TOP_PAGES#WIKI#enwiki"
    string SK "WINDOW#2026-06-11T16:44#TITLE#Scale AI"
    number events_count
    string last_change_type
    string last_seen_at
    number ttl "window_start + 2h"
  }

  CHANGE_TYPE {
    string PK "METRIC#CHANGE_TYPE#TYPE#edit"
    string SK "WINDOW#2026-06-11T16:44"
    number events_count
    number ttl "window_start + 2h"
  }

  WEBSOCKET_CONNECTIONS {
    string PK "connection_id"
    string connected_at
    string client_type
    list topics "global, wiki:enwiki, top_pages"
    number ttl "connected_at + 2h"
  }
```