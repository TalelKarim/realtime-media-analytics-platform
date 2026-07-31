# ADR-008 — Use a Medallion Data Lake for Historical Analytics

- Status: Accepted

## Decision

Archive normalized envelopes through Firehose to S3 Bronze, transform to Silver Parquet and produce Gold analytical datasets with Glue.

## Consequences

Positive: durable source fidelity, reprocessing, cheap columnar SQL and BI separation from the live path.

Negative: minute-level delivery/ETL latency, catalog/schema maintenance and no automatic reconciliation with realtime DynamoDB views.
