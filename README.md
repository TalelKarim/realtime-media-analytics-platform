# Realtime Media Analytics Platform on AWS

AWS-native streaming platform that ingests live Wikimedia activity, processes it in real time, pushes aggregated metrics to a live WebSocket dashboard, and archives everything in a Medallion Data Lake for historical analysis.

---

## High-Level Architecture

```
                       ┌──────────────────────────────┐
                       │   Wikimedia EventStreams      │
                       │   recentchange SSE ~1000/sec  │
                       └──────────────┬───────────────┘
                                      │ HTTPS / SSE
                                      ▼
                       ┌──────────────────────────────┐
                       │   ECS Fargate Collector       │
                       │   parse · normalize · batch   │
                       └──────────────┬───────────────┘
                                      │ PutRecords
                                      ▼
                       ┌──────────────────────────────┐
                       │   Kinesis Data Streams        │
                       │   central event backbone      │
                       └──────┬───────────┬────────────┘
                              │           │           │
              ┌───────────────┘           │           └──────────────┐
              ▼                           ▼                          ▼
 ┌────────────────────┐    ┌─────────────────────┐    ┌─────────────────────┐
 │ Realtime Processor │    │ Firehose Delivery   │    │ Alert Processor     │
 │ Lambda             │    │ Stream              │    │ Lambda              │
 └────────┬───────────┘    └──────────┬──────────┘    └──────────┬──────────┘
          │                           │                           │
          ▼                           ▼                           ▼
 ┌────────────────────┐    ┌─────────────────────┐    ┌─────────────────────┐
 │ DynamoDB           │    │ S3 Data Lake        │    │ SNS Topic           │
 │ realtime_aggregates│    │ bronze/silver/gold  │    │ alerts              │
 │ websocket_conns    │    └──────────┬──────────┘    └─────────────────────┘
 └────────┬───────────┘               │
          ▼                           ▼
 ┌────────────────────┐    ┌─────────────────────┐
 │ SQS FIFO           │    │ Glue Data Catalog   │
 │ broadcast signal   │    └──────────┬──────────┘
 └────────┬───────────┘               │
          ▼                           ▼
 ┌────────────────────┐    ┌─────────────────────┐
 │ Broadcaster Lambda │    │ Athena              │
 └────────┬───────────┘    └──────────┬──────────┘
          ▼                           ▼
 ┌────────────────────┐    ┌─────────────────────┐
 │ API Gateway        │    │ QuickSight          │
 │ WebSocket          │    │ historical dashboards│
 └────────┬───────────┘    └─────────────────────┘
          ▼
 ┌────────────────────┐
 │ Frontend Dashboard │
 │ live visualization │
 └────────────────────┘
```

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

## Dev Environment — Cost Control & Sampling

Running this platform at full throughput (1000 events/sec) for 12 hours costs approximately **$200**, driven almost entirely by DynamoDB write volume (~157M WRUs).

To make development and testing affordable, the Collector supports a configurable **sampling rate** via environment variable:

```
COLLECTOR_SAMPLE_RATE=0.10   # dev  → 10% of the stream (~100 events/sec)
COLLECTOR_SAMPLE_RATE=1.0    # prod → full throughput (1000 events/sec)
```

### How sampling works

```python
import os, random

SAMPLE_RATE = float(os.environ.get('COLLECTOR_SAMPLE_RATE', '1.0'))

def process_event(event):
    if random.random() > SAMPLE_RATE:
        return  # drop this event
    send_to_kinesis(normalize(event))
```

### Cost comparison

| Mode | Events/sec | Volume (12h) | Estimated cost |
|---|---|---|---|
| Production | 1 000 | ~43M events / 26 GB | ~$200 |
| Dev (10% sampling) | 100 | ~4.3M events / 2.6 GB | ~$20 |
| Dev (1% sampling) | 10 | ~430K events / 260 MB | ~$2 |

### Important note on sampled metrics

When running with `COLLECTOR_SAMPLE_RATE=0.10`, all real-time aggregates
(events/min, bot ratio, top wikis, top pages) reflect **10% of actual activity**.
Multiply displayed values by `1 / SAMPLE_RATE` to extrapolate production-equivalent figures.

The architecture, data contracts, and pipeline behavior are **identical** at any sampling rate.
Sampling only affects ingestion volume — not the system design.

---

## Documentation index

| File | Content |
|---|---|
| `README.md` | This file — project overview and index |
| `documentation/architecture.md` | High-level and detailed architecture, ADRs, scalability path, security, observability, runbooks |
| `documentation/data-contracts.md` | All data contracts across the pipeline (source → Kinesis → DynamoDB → WebSocket → S3 → Gold) |
| `documentation/sequence-diagrams.md` | All sequence diagrams in Mermaid format |
| `documentation/historical-analytics.md` | Data Lake architecture, Glue ETL, Athena queries, QuickSight dashboards |

---

## Repository structure

```
realtime-media-analytics-platform/
├── README.md
├── documentation/
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
│   ├── alert-processor/
│   ├── broadcaster/
│   ├── websocket-connect-handler/
│   ├── websocket-disconnect-handler/
│   └── websocket-default-handler/
├── frontend/
│   └── dashboard/
└── terraform/
    ├── environments/
    │   └── dev/
    │       ├── main.tf
    │       ├── variables.tf
    │       ├── outputs.tf
    │       └── terraform.tfvars
    └── modules/
        ├── networking/
        ├── kms/
        ├── iam/
        ├── kinesis/
        ├── s3-datalake/
        ├── dynamodb/
        ├── sqs/
        ├── sns/
        ├── ecs-collector/
        ├── lambda/
        ├── apigw-websocket/
        ├── firehose/
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
