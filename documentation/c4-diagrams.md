# Architecture Diagrams — Realtime Media Analytics Platform
# Format: Mermaid — renderable in GitHub, GitLab, Notion, VSCode

---

## DIAGRAM 1 — C4 Level 1 : System Context

```mermaid
C4Context
  title System Context — Realtime Media Analytics Platform

  Person(analyst, "Media Analyst", "Monitors live Wikipedia activity via the dashboard")
  Person(ops, "Platform Engineer", "Operates and monitors the platform infrastructure")

  System(platform, "Realtime Media Analytics Platform", "Ingests Wikimedia EventStreams, processes events in real time, serves live dashboards, and archives source-fidelity envelopes for historical analysis")

  System_Ext(wikimedia, "Wikimedia EventStreams", "Public SSE stream of changes across Wikipedia, Wikidata, Wikimedia Commons — ~1000 events/sec")
  System_Ext(quicksight, "Amazon QuickSight", "Business intelligence dashboards for historical analytics")
  System_Ext(sns_email, "Email / SMS", "Alert notifications for anomaly detection")

  Rel(wikimedia, platform, "Streams recentchange events", "SSE / HTTPS")
  Rel(platform, analyst, "Pushes real-time stats every ~5s", "WebSocket")
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
    Container(collector, "SSE Collector", "ECS Fargate / Python", "Maintains SSE connection, filters canary events, normalizes records, embeds raw_event, batches to Kinesis")
  }

  Container_Boundary(streaming, "Streaming Backbone") {
    Container(kinesis, "Kinesis Data Streams", "AWS Kinesis", "Central event backbone — fan-out point for all consumers")
  }

  Container_Boundary(realtime, "Real-Time Processing") {
    Container(rt_processor, "Realtime Processor", "AWS Lambda", "Computes 1-minute aggregates and sends 5-second broadcast signals")
    Container(dynamodb, "DynamoDB", "AWS DynamoDB", "Stores real-time aggregates, WebSocket connections, and alert state")
    Container(sqs, "Broadcast Signal Queue", "AWS SQS FIFO", "Deduplicates broadcast triggers by 5-second broadcast window")
    Container(broadcaster, "Broadcaster", "AWS Lambda", "Reads aggregates, scans connections, filters topics in Lambda, pushes snapshots")
    Container(apigw, "API Gateway WebSocket", "AWS API Gateway", "Manages persistent WebSocket connections with dashboard clients")
    Container(dashboard, "Live Dashboard", "React / WebSocket", "Real-time visualization of Wikimedia activity")
  }

  Container_Boundary(historical, "Historical Analytics") {
    Container(firehose, "Firehose Delivery Stream", "AWS Kinesis Firehose", "Buffers and delivers normalized envelopes to S3 Bronze")
    Container(s3, "S3 Data Lake", "AWS S3", "Bronze / Silver / Gold zones — envelope archive, cleaned Parquet, aggregated datasets")
    Container(glue, "Glue ETL Jobs", "AWS Glue", "Transforms bronze→silver→gold on hourly schedule")
    Container(athena, "Athena", "AWS Athena", "SQL query engine on S3 Parquet data")
  }

  Container_Boundary(alerting, "Alerting") {
    Container(alert_proc, "Alert Processor", "AWS Lambda", "Persists rolling alert state in DynamoDB and detects spikes")
    Container(sns, "SNS Topic", "AWS SNS", "Delivers alerts via email or SMS")
  }

  Rel(wikimedia, collector, "SSE raw events", "HTTPS / SSE")
  Rel(collector, kinesis, "Normalized envelope + raw_event", "PutRecords")
  Rel(kinesis, rt_processor, "Event batches", "Kinesis trigger")
  Rel(kinesis, firehose, "Normalized envelopes", "Kinesis consumer")
  Rel(kinesis, alert_proc, "Event batches", "Kinesis trigger")
  Rel(rt_processor, dynamodb, "Atomic counter updates", "UpdateItem ADD")
  Rel(rt_processor, sqs, "5-second broadcast signal", "SendMessage FIFO")
  Rel(sqs, broadcaster, "Deduplicated signal", "SQS trigger")
  Rel(broadcaster, dynamodb, "Read aggregates + Scan connections", "GetItem + Scan + DeleteItem")
  Rel(broadcaster, apigw, "Push snapshots", "postToConnection")
  Rel(apigw, dashboard, "stats.update messages", "WebSocket")
  Rel(analyst, dashboard, "Views live metrics", "Browser")
  Rel(firehose, s3, "Envelope JSON Lines", "S3 delivery")
  Rel(glue, s3, "Read bronze, write silver/gold", "S3 read/write")
  Rel(athena, s3, "SQL scans", "S3 read")
  Rel(alert_proc, dynamodb, "Persist rolling state", "UpdateItem ADD + Query")
  Rel(alert_proc, sns, "Spike alerts", "Publish")
```

