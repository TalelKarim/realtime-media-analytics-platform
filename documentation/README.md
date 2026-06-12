# Realtime Media Analytics Platform on AWS

AWS-native streaming platform that ingests live Wikimedia activity, processes it in real time, pushes aggregated metrics to a live WebSocket dashboard, and archives everything in a Medallion Data Lake for historical analysis.


---

## What it does

| Capability | Technology |
|---|---|
| Ingest live Wikimedia SSE stream | ECS Fargate Collector |
| Fan-out to 3 consumers | Kinesis Data Streams |
| Real-time aggregation | Lambda + DynamoDB (write-sharded) |
| Live dashboard push | SQS FIFO + Lambda Broadcaster + API Gateway WebSocket |
| Spike alerting | Lambda + SNS |
| Historical archive | Firehose + S3 Bronze/Silver/Gold |
| SQL analytics | Glue + Athena + QuickSight |
| Infrastructure as Code | Terraform + Terraform Cloud |

---

## Architecture patterns demonstrated

- Real-time event ingestion over SSE
- Event-driven fan-out with Kinesis
- Write sharding for DynamoDB hot partition mitigation
- Serverless broadcasting via WebSocket
- Medallion Data Lake (Bronze / Silver / Gold)
- Partition projection on Athena
- Observability with CloudWatch custom metrics and alarms
- IAM least-privilege per component
- Encryption at rest and in transit (KMS)

---

## Data source

```
https://stream.wikimedia.org/v2/stream/recentchange
```

Public SSE stream of all changes across Wikipedia, Wikidata, and Wikimedia Commons.
Approximately **1000 events/sec** at peak, covering 5 event types:
`edit` · `new` · `categorize` · `log` · `external`

Official schema:
```
https://github.com/wikimedia/mediawiki-event-schemas/blob/master/jsonschema/mediawiki/recentchange/current.yaml
```

---

## Documentation index

| File | Content |
|---|---|
| `README.md` | This file — project overview and index |
| `architecture.md` | High-level and detailed architecture, ADRs, scalability path, security, observability, runbooks |
| `data-contracts.md` | All data contracts across the pipeline (source → Kinesis → DynamoDB → WebSocket → S3 → Gold) |
| `sequence-diagrams.md` | All sequence diagrams in Mermaid format |
| `historical-analytics.md` | Data Lake architecture, Glue ETL, Athena queries, QuickSight dashboards |

---

## Repository structure

```
realtime-media-analytics-platform/
├── README.md
├── docs/
│   ├── architecture.md
│   ├── data-contracts.md
│   ├── sequence-diagrams.md
│   ├── historical-analytics.md
│   └── adr/
│       ├── ADR-001-fargate-collector.md
│       ├── ADR-002-kinesis-backbone.md
│       ├── ADR-003-kinesis-partition-key.md
│       ├── ADR-004-dynamodb-aggregates.md
│       ├── ADR-005-write-sharding.md
│       ├── ADR-006-websocket-dashboard.md
│       ├── ADR-007-sqs-fifo-dedup.md
│       └── ADR-008-historical-analytics.md
├── services/
│   ├── collector/
│   ├── realtime-processor/
│   ├── websocket-connect-handler/
│   ├── websocket-disconnect-handler/
│   ├── websocket-default-handler/
│   └── broadcaster/
├── frontend/
│   └── dashboard/
└── terraform/
    ├── environments/
    │   └── dev/
    └── modules/
        ├── networking/
        ├── ecs-collector/
        ├── kinesis/
        ├── lambda/
        ├── dynamodb/
        ├── sqs/
        ├── apigw-websocket/
        ├── firehose/
        ├── s3-datalake/
        ├── glue/
        ├── athena/
        └── monitoring/
```

---

## AWS services

**Real-time path**
ECS Fargate · Kinesis Data Streams · Lambda · DynamoDB · SQS FIFO · API Gateway WebSocket · SNS · CloudWatch · IAM · KMS

**Historical path**
Kinesis Firehose · S3 · Glue · Athena · QuickSight

**Infrastructure**
Terraform · Terraform Cloud · GitHub Actions