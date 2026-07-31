# Historical Analytics — Bronze, Silver and Gold

## 1. Purpose

The historical path preserves normalized source events independently from the low-latency dashboard. It supports replay, SQL exploration and BI without reading DynamoDB live aggregates.

```text
Kinesis
→ Firehose
→ S3 Bronze
→ Glue Bronze-to-Silver
→ S3 Silver Parquet
→ Glue Silver-to-Gold
→ S3 Gold Parquet
→ Athena / QuickSight
```

## 2. Firehose delivery

Terraform source: `terraform/environments/dev/firehose.tf`.

| Setting | Value |
|---|---:|
| Source | Kinesis Data Streams |
| Destination | encrypted S3 data lake |
| Buffer size | 64 MiB |
| Buffer interval | 300 seconds |
| Log retention | 14 days |

Firehose delivers whichever threshold is reached first. At the current sampled dev volume, time is often the trigger.

`DeliveryToS3.DataFreshness` is measured in seconds and represents the age of the oldest record still buffered/not delivered to S3. It is not the WebSocket freshness SLI.

## 3. Bronze layer

Prefix contract:

```text
bronze/wikimedia/recentchange/
  year=YYYY/
  month=MM/
  day=DD/
  hour=HH/
```

Format:

```text
JSON Lines
GZIP
normalized envelope retained
raw_event retained
```

Bronze is immutable source-fidelity storage. Corrections happen downstream; Bronze should not be destructively rewritten during normal operation.

## 4. Silver layer

Glue script: `terraform/modules/glue_etl/scripts/bronze_to_silver.py`.

Responsibilities:

- read Bronze envelopes;
- reject records missing required identity/time fields;
- flatten stable envelope/payload fields;
- cast fields null-safely;
- serialize complex log parameters when needed;
- write columnar Parquet with SNAPPY compression;
- partition by `ingestion_date`.

Prefix:

```text
silver/wikimedia/recentchange/ingestion_date=YYYY-MM-DD/
```

Silver is the cleaned, typed analytical fact layer. It is not the source of truth for source fidelity; Bronze is.

## 5. Gold layer

Glue script: `terraform/modules/glue_etl/scripts/silver_to_gold.py`.

Current analytical outputs include:

```text
top_wikis_by_hour
bot_vs_human_by_hour
change_type_distribution
top_pages_by_day
activity_spikes
```

Gold datasets are optimized for direct analytical consumption and dashboarding.

## 6. Glue Data Catalog and partition projection

Catalog definitions allow Athena to discover Bronze/Silver/Gold schemas. Partition projection reduces the need to register every time partition manually and enables partition pruning when queries include date/hour filters.

## 7. Athena query discipline

Always filter partitions.

Good:

```sql
SELECT wiki, SUM(event_count)
FROM gold_top_wikis_by_hour
WHERE event_date BETWEEN DATE '2026-07-01' AND DATE '2026-07-31'
GROUP BY wiki
ORDER BY 2 DESC
LIMIT 20;
```

Avoid unrestricted scans of Bronze or all Silver history.

Operational checks:

```sql
SELECT COUNT(*)
FROM silver_wikimedia_recentchange
WHERE ingestion_date = DATE '2026-07-31';
```

```sql
SELECT wiki, COUNT(*) AS events
FROM silver_wikimedia_recentchange
WHERE ingestion_date = DATE '2026-07-31'
GROUP BY wiki
ORDER BY events DESC
LIMIT 10;
```

## 8. QuickSight

QuickSight uses Athena-backed datasets and can import results into SPICE. The Terraform module receives an explicit QuickSight principal ARN as owner/manager.

Recommended dashboard pages:

- hourly activity trend;
- top wikis;
- bot versus human ratio;
- change type distribution;
- top pages;
- detected activity spikes.

## 9. Data consistency with the speed layer

The live and historical paths consume the same Kinesis envelopes but have different goals:

```text
DynamoDB/WebSocket
→ fast, sampled, mutable current-window view

S3/Glue/Athena
→ durable, reproducible historical analysis
```

The project is Lambda-Architecture-inspired, but there is no automated reconciliation process that overwrites live DynamoDB views from Gold results. The two paths should therefore be compared during validation, not assumed exactly identical under retries or late events.

## 10. Failure handling

- Firehose delivery failures are exposed through CloudWatch metrics/logs.
- S3 is encrypted with a customer-managed KMS key.
- Glue failures are visible in Glue job runs and CloudWatch Logs.
- ETL jobs must be rerunnable for a selected partition.
- Bronze retention must be long enough to support reprocessing.

## 11. Cost controls

- deterministic Collector sampling reduces all downstream volume;
- Firehose batches small events into larger S3 objects;
- Parquet/SNAPPY reduces Athena scanned bytes;
- partition pruning is mandatory;
- Gold datasets prevent repeated expensive raw scans;
- QuickSight SPICE avoids repeated interactive Athena queries where appropriate.
