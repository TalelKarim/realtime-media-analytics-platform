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
| Real-time aggregation | Lambda + in-memory aggregation + bounded-parallel DynamoDB atomic updates + write sharding |
| Live dashboard push | SQS FIFO 3-second broadcast signal + bounded-parallel Lambda Broadcaster + API Gateway WebSocket |
| WebSocket subscription tracking | DynamoDB `websocket_connections` with V1 Scan + Lambda-side topic filtering |
| Spike alerting | Lambda + DynamoDB `alert_state` + SNS |
| Historical archive | Firehose + S3 Bronze/Silver/Gold |
| SQL analytics | Glue + Athena + QuickSight |
| Observability | OpenTelemetry + Grafana Alloy / Lambda Collector Extension + Grafana Cloud Mimir, Loki, and Tempo |
| Infrastructure as Code | Terraform + Terraform Cloud |

## Architecture patterns demonstrated

- Real-time event ingestion over SSE
- Event-driven fan-out with Kinesis
- Stable normalized event contract with embedded raw source event
- Deterministic sampling for development cost control
- Write sharding for DynamoDB hot partition mitigation
- In-memory aggregation before persistence
- Atomic counters with `UpdateItem ADD`
- Bounded parallelism for DynamoDB writes, aggregate reads, and WebSocket fan-out
- Serverless broadcasting via WebSocket
- SQS FIFO deduplication by short broadcast windows
- Medallion Data Lake (Bronze / Silver / Gold)
- Partition projection on Athena
- OpenTelemetry metrics and distributed tracing
- Centralized observability in Grafana Cloud with Mimir, Loki, and Tempo
- IAM least privilege per component
- Encryption at rest and in transit with customer-managed KMS keys

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

Running this platform at full throughput for long periods can be expensive, driven mostly by DynamoDB write volume and downstream processing.

The Collector supports a configurable sampling rate through the `SAMPLE_RATE` environment variable:

```text
SAMPLE_RATE=0.01   # dev default → keep 1% of valid source events
SAMPLE_RATE=0.10   # load test / richer demo → keep 10%
SAMPLE_RATE=1.0    # full stream
```

### How sampling works

Sampling is deterministic and based on the normalized `event_id`:

```python
import hashlib


def sampling_score(event_id: str) -> float:
    digest = hashlib.sha256(event_id.encode("utf-8")).hexdigest()
    return int(digest[:8], 16) / 0xFFFFFFFF


def should_sample(event_id: str, sample_rate: float) -> bool:
    if sample_rate >= 1.0:
        return True
    if sample_rate <= 0.0:
        return False
    return sampling_score(event_id) < sample_rate
```

The same `event_id` always produces the same sampling decision for a given rate.

### Cost comparison

| Mode | Sampling rate | Approximate retained volume |
|---|---:|---:|
| Full stream | 100% | 100% of valid events |
| Rich dev / load test | 10% | ~10% of valid events |
| Default dev | 1% | ~1% of valid events |

### Important note on sampled metrics

All real-time aggregates reflect the sampled input volume. Ratios such as `bot_ratio` remain representative when the sample is sufficiently large, while absolute counters represent only the retained events.

The architecture, contracts, and processing behavior are unchanged by the sampling rate.

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

**Observability**  
OpenTelemetry · Grafana Alloy · OpenTelemetry Collector Lambda Extension · Grafana Cloud Mimir/Loki/Tempo · CloudWatch AWS integration

**Infrastructure**  
Terraform · Terraform Cloud · GitHub Actions
