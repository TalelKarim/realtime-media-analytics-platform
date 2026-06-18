# Realtime Media Analytics Platform on AWS

AWS-native streaming platform that ingests live Wikimedia activity, processes it in real time, pushes aggregated metrics to a live WebSocket dashboard, and archives a source-fidelity event envelope in a Medallion Data Lake for historical analysis.

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
                       │ parse · normalize · raw_event │
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
 │ alert_state        │               │
 └────────┬───────────┘               ▼
          ▼                ┌─────────────────────┐
 ┌────────────────────┐    │ Glue Data Catalog   │
 │ SQS FIFO           │    └──────────┬──────────┘
 │ broadcast signal   │               │
 └────────┬───────────┘               ▼
          ▼                ┌─────────────────────┐
 ┌────────────────────┐    │ Athena              │
 │ Broadcaster Lambda │    └──────────┬──────────┘
 └────────┬───────────┘               │
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
| Normalize events while preserving source fidelity | Normalized envelope + embedded `raw_event` |
| Fan-out to 3 consumers | Kinesis Data Streams |
| Real-time aggregation | Lambda + DynamoDB atomic counters and write sharding |
| Live dashboard push | SQS FIFO 5-second broadcast signal + Lambda Broadcaster + API Gateway WebSocket |
| WebSocket subscription tracking | DynamoDB `websocket_connections` with V1 Scan + Lambda-side topic filtering |
| Spike alerting | Lambda + DynamoDB `alert_state` + SNS |
| Historical archive | Firehose + S3 Bronze/Silver/Gold |
| SQL analytics | Glue + Athena + QuickSight |
| Infrastructure as Code | Terraform + Terraform Cloud |

---

## Architecture patterns demonstrated

- Real-time event ingestion over SSE
- Event-driven fan-out with Kinesis
- Stable normalized event contract with embedded raw source event
- Write sharding for DynamoDB hot partition mitigation
- Atomic counters with `UpdateItem ADD`
- Serverless broadcasting via WebSocket
- SQS FIFO deduplication by short broadcast windows
- Medallion Data Lake (Bronze / Silver / Gold)
- Partition projection on Athena
- Observability with CloudWatch custom metrics and alarms
- IAM least-privilege per component
- Encryption at rest and in transit with customer-managed KMS keys

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

The raw Wikimedia event is the JSON object received in the SSE `data:` line.
The Collector preserves that object under `raw_event` while also building a stable normalized `payload`.

---

## Dev Environment — Cost Control & Sampling

Running this platform at full throughput (1000 events/sec) for 12 hours can be expensive, driven mostly by DynamoDB write volume and downstream processing.

To make development and testing affordable, the Collector supports a configurable **sampling rate** via environment variable:

```
COLLECTOR_SAMPLE_RATE=0.10   # dev  → 10% of the stream (~100 events/sec)
COLLECTOR_SAMPLE_RATE=1.0    # prod → full throughput (~1000 events/sec)
```

### How sampling works

```python
import os, random

SAMPLE_RATE = float(os.environ.get("COLLECTOR_SAMPLE_RATE", "1.0"))

def process_event(raw_event):
    if raw_event.get("meta", {}).get("domain") == "canary":
        return  # drop Wikimedia canary events

    if random.random() > SAMPLE_RATE:
        return  # drop this event in dev/test mode

    envelope = build_normalized_envelope(raw_event)  # payload + raw_event
    send_to_kinesis(envelope)
```

### Cost comparison

| Mode | Events/sec | Volume (12h) | Estimated cost |
|---|---:|---:|---:|
| Production | 1 000 | ~43M events | high |
| Dev (10% sampling) | 100 | ~4.3M events | moderate |
| Dev (1% sampling) | 10 | ~430K events | low |

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
| `documentation/c4-diagrams.md` | C4 system context and container diagrams|

---


## AWS services

**Real-time path**  
ECS Fargate · Kinesis Data Streams · Lambda · DynamoDB · SQS FIFO · API Gateway WebSocket · SNS · CloudWatch · IAM · KMS

**Historical path**  
Kinesis Firehose · S3 · Glue · Athena · QuickSight

**Infrastructure**  
Terraform · Terraform Cloud · GitHub Actions
