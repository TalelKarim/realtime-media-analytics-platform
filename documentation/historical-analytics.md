# Historical Analytics — Realtime Media Analytics Platform

---

## Flow

```
Kinesis Data Streams
  → Kinesis Firehose       (buffer 64MB / 5min → S3)
  → S3 Bronze              (normalized envelope + raw_event, immutable)
  → Glue ETL bronze→silver (hourly, Parquet)
  → S3 Silver              (cleaned, typed, null-safe)
  → Glue ETL silver→gold   (hourly, pre-aggregated)
  → S3 Gold                (5 datasets)
  → Glue Data Catalog      (partition projection)
  → Athena                 (SQL on S3)
  → QuickSight             (SPICE dashboards)
```

---

## Kinesis Firehose

Dedicated Kinesis consumer for archival. No code, no servers.

```
Source         : Kinesis Data Streams (parallel consumer alongside Realtime Processor)
Destination    : S3 bronze zone
Buffer size    : 64 MB
Buffer time    : 300 seconds (5 minutes)
Trigger        : whichever condition is met first
Compression    : GZIP on delivery
Latency        : Bronze envelope files available in S3 within 5 minutes
```

Firehose reads the same Kinesis record consumed by the real-time path: the normalized envelope with an embedded `raw_event`.

Dynamic partitioning uses the envelope-level `occurred_at` field, not the Firehose delivery timestamp:

```
bronze/wikimedia/recentchange/
  year=!{partitionKeyFromQuery:year}/
  month=!{partitionKeyFromQuery:month}/
  day=!{partitionKeyFromQuery:day}/
  hour=!{partitionKeyFromQuery:hour}/
```

This prevents late-arriving events from landing in the wrong S3 partition.

---

## S3 Data Lake — Medallion Architecture

```
s3://realtime-media-analytics-datalake/
├── bronze/
│   └── wikimedia/recentchange/
│       └── year=2026/month=06/day=11/hour=16/
│           └── wikimedia-recentchange-2026-06-11-16-05-00-uuid.json.gz
│
├── silver/
│   └── wikimedia/recentchange/
│       └── ingestion_date=2026-06-11/
│           └── part-00000.parquet
│
└── gold/
    ├── top_wikis_by_hour/year=2026/month=06/day=11/hour=16/
    ├── top_pages_by_day/year=2026/month=06/day=11/
    ├── bot_vs_human_by_hour/year=2026/month=06/day=11/hour=16/
    ├── change_type_distribution/year=2026/month=06/day=11/hour=16/
    └── activity_spikes/year=2026/month=06/day=11/
```

### Bronze — Normalized envelope + raw_event

```
Format    : JSON Lines (.json.gz)
Schema    : Contract 2 — normalized envelope with embedded raw_event
Purpose   : immutable source-fidelity archive, event replay, schema recovery
Retention : 2 years → Glacier after 90 days
```

Bronze is not raw-only. It stores the exact Kinesis envelope produced by the Collector.  
The original Wikimedia JSON remains available inside `raw_event`.

### Silver — Cleaned

```
Format    : Apache Parquet, SNAPPY compression
Schema    : typed, null-safe normalized schema (see data-contracts.md Contract 10)
Purpose   : ad-hoc Athena queries across the full event history
Retention : 1 year → Glacier after 60 days
```

Silver is built from the envelope-level fields and `payload`. It does not need to include the full `raw_event` by default because Bronze remains the source-fidelity archive.

### Gold — Pre-aggregated

```
Format    : Apache Parquet, SNAPPY compression
Schema    : 5 metric-specific datasets (see data-contracts.md Contract 11)
Purpose   : fast QuickSight dashboards — minimal Athena scan surface
Retention : 3 years → Standard-IA after 30 days
```

---

## Glue ETL Jobs

### Job 1 — Bronze to Silver

```
Name     : wikimedia-bronze-to-silver
Trigger  : hourly scheduler — processes previous hour's partition
Runtime  : AWS Glue 4.0, Python 3
Workers  : 2 × G.1X (scalable)
```

Transformations applied:

```python
from pyspark.sql.functions import col, to_date, to_json, when

# Bronze rows are Contract 2 envelopes:
# event_id, occurred_at, ingested_at, payload.*, raw_event

df = df.filter(col("event_id").isNotNull())
df = df.filter(col("occurred_at").isNotNull())

df = df.withColumn("ingestion_date", to_date(col("occurred_at")))
df = df.withColumn("wiki", lower(col("payload.wiki")))
df = df.withColumn("user_is_bot", col("payload.user_is_bot").cast("boolean"))
df = df.withColumn("is_minor", col("payload.is_minor").cast("boolean"))
df = df.withColumn("is_patrolled", col("payload.is_patrolled").cast("boolean"))
df = df.withColumn("delta_bytes", col("payload.delta_bytes"))

df = df.withColumn(
    "log_params",
    when(col("payload.log_params").isNotNull(), to_json(col("payload.log_params")))
    .otherwise(None)
)

df = df.select(
    "event_id",
    "occurred_at",
    "ingestion_date",
    "wiki",
    col("payload.domain").alias("domain"),
    col("payload.change_type").alias("change_type"),
    col("payload.namespace").alias("namespace"),
    col("payload.title").alias("title"),
    col("payload.title_url").alias("title_url"),
    col("payload.user").alias("user"),
    "user_is_bot",
    "is_minor",
    "is_patrolled",
    col("payload.old_length").alias("old_length"),
    col("payload.new_length").alias("new_length"),
    "delta_bytes",
    col("payload.revision_old").alias("revision_old"),
    col("payload.revision_new").alias("revision_new"),
    col("payload.change_url").alias("change_url"),
    col("payload.raw_notify_url").alias("raw_notify_url"),
    col("payload.log_type").alias("log_type"),
    col("payload.log_action").alias("log_action"),
    "log_params",
    col("payload.wikimedia_recentchange_id").alias("wikimedia_rcid")
)

df.write.partitionBy("ingestion_date").mode("append").parquet("s3://bucket/silver/...")
```

### Job 2 — Silver to Gold

```
Name     : wikimedia-silver-to-gold
Trigger  : hourly scheduler — after bronze-to-silver completes
Runtime  : AWS Glue 4.0, Python 3
Workers  : 2 × G.1X (scalable)
```

Five aggregations produced:

```python
# top_wikis_by_hour
top_wikis = df.groupBy("wiki", window("occurred_at", "1 hour").alias("hour_window")) \
    .agg(
        count("*").alias("event_count"),
        sum(when(col("user_is_bot"), 1).otherwise(0)).alias("bot_count"),
        sum(when(~col("user_is_bot"), 1).otherwise(0)).alias("human_count"),
        sum(when(col("change_type") == "edit", 1).otherwise(0)).alias("edit_count"),
        sum(when(col("change_type") == "new", 1).otherwise(0)).alias("new_count"),
        sum(when(col("change_type") == "categorize", 1).otherwise(0)).alias("categorize_count"),
        sum(when(col("change_type") == "log", 1).otherwise(0)).alias("log_count"),
        sum(when(col("change_type") == "external", 1).otherwise(0)).alias("external_count"),
    )

# bot_vs_human_by_hour
bot_human = df.groupBy(window("occurred_at", "1 hour").alias("hour_window")) \
    .agg(
        sum(when(col("user_is_bot"), 1).otherwise(0)).alias("bot_count"),
        sum(when(~col("user_is_bot"), 1).otherwise(0)).alias("human_count"),
        count("*").alias("total_count"),
    ) \
    .withColumn("bot_ratio", col("bot_count") / col("total_count"))

# top_pages_by_day — namespace = 0 only
top_pages = df.filter(col("namespace") == 0) \
    .groupBy("wiki", "title", to_date("occurred_at").alias("day")) \
    .agg(
        count("*").alias("event_count"),
        last("change_type").alias("last_change_type"),
        max("occurred_at").alias("last_seen_at"),
    )

# change_type_distribution
change_dist = df.groupBy("change_type", window("occurred_at", "1 hour").alias("hour_window")) \
    .agg(count("*").alias("event_count"))

# activity_spikes
# z_score computed over a rolling historical baseline per wiki/global scope.
```

---

## Glue Data Catalog

```
Database : realtime_media_analytics

Tables:
  wikimedia_bronze_recentchange  → s3://bucket/bronze/
  wikimedia_silver_recentchange  → s3://bucket/silver/
  top_wikis_by_hour              → s3://bucket/gold/top_wikis_by_hour/
  top_pages_by_day               → s3://bucket/gold/top_pages_by_day/
  bot_vs_human_by_hour           → s3://bucket/gold/bot_vs_human_by_hour/
  change_type_distribution       → s3://bucket/gold/change_type_distribution/
  activity_spikes                → s3://bucket/gold/activity_spikes/
```

Partition projection enabled on all tables:

```
year  : integer, range 2026–2030
month : integer, range 1–12
day   : integer, range 1–31
hour  : integer, range 0–23
```

Partition projection eliminates `MSCK REPAIR TABLE`.

---

## Athena

```
Workgroup       : realtime-media-analytics
Output location : s3://bucket/athena-results/
Encryption      : SSE-KMS with the S3 customer-managed key
Scan limit      : 1 GB per query (cost control)
```

### Example queries

**Top 10 most edited articles in the last 7 days**
```sql
SELECT wiki, title, COUNT(*) AS edit_count
FROM wikimedia_silver_recentchange
WHERE ingestion_date >= DATE_ADD('day', -7, CURRENT_DATE)
  AND change_type = 'edit'
  AND namespace = 0
GROUP BY wiki, title
ORDER BY edit_count DESC
LIMIT 10;
```

**Bot vs human ratio by hour — last 24 hours**
```sql
SELECT hour_window, bot_count, human_count,
       ROUND(bot_ratio * 100, 2) AS bot_pct
FROM bot_vs_human_by_hour
WHERE hour_window >= DATE_ADD('hour', -24, NOW())
ORDER BY hour_window ASC;
```

**Activity spikes — last 30 days, z_score > 2**
```sql
SELECT hour_window, wiki, event_count, ROUND(z_score, 2) AS z_score
FROM activity_spikes
WHERE z_score > 2.0
  AND hour_window >= DATE_ADD('day', -30, CURRENT_DATE)
ORDER BY z_score DESC
LIMIT 20;
```

**Most active wikis in the last hour**
```sql
SELECT wiki, event_count, bot_count, human_count,
       ROUND(CAST(bot_count AS DOUBLE) / event_count * 100, 1) AS bot_pct
FROM top_wikis_by_hour
WHERE hour_window = DATE_TRUNC('hour', NOW() - INTERVAL '1' HOUR)
ORDER BY event_count DESC
LIMIT 10;
```

---

## QuickSight

```
Data source : Athena workgroup realtime-media-analytics
Refresh     : SPICE incremental refresh every 1 hour
```

### Dashboard 1 — Global Activity Overview
```
KPIs   : total events 24h / 7d / 30d · average events/hour · peak hour · bot ratio trend
Charts : events over time (line, hourly) · bot vs human (stacked bar, daily)
```

### Dashboard 2 — Top Wikis
```
KPIs   : most active wiki last 24h · wiki with highest bot ratio
Charts : top 10 wikis by count (bar) · wiki activity heatmap (day × hour)
         · wiki activity trend 30 days (line)
```

### Dashboard 3 — Top Pages
```
KPIs   : most edited page last 24h · most created pages last 7d
Charts : top 20 pages by edit count (horizontal bar) · page activity over time (line)
```

### Dashboard 4 — Change Type Distribution
```
Charts : edit/new/categorize/log/external distribution (pie, daily)
         · change type trend 30 days (stacked area)
         · namespace distribution (bar)
```

### Dashboard 5 — Activity Spikes
```
Charts : spike timeline (event_count + z_score overlay)
         · annotated spike events table (wiki · hour · count · z_score)
```

---

## Challenges and mitigations

**Firehose dynamic partitioning**  
By default Firehose uses its own delivery timestamp, not the event's `occurred_at`. Late-arriving events would land in the wrong S3 partition. Mitigation: enable dynamic partitioning with a Firehose Lambda transformer or metadata extraction that derives year/month/day/hour from the envelope-level `occurred_at`.

**Small files**  
Five-minute Firehose buffers produce many small files. Small files degrade Athena scan performance and increase S3 API costs. Mitigation: run a Glue compaction job every 6 hours targeting 128 MB per Parquet file.

**Schema evolution**  
Wikimedia may add or rename fields. Mitigation: Bronze stores the normalized envelope and preserves the original source object in `raw_event`. Silver ETL explicitly selects known payload columns and discards unknowns. `event_version` enables downstream version detection.

**Athena cost**  
Athena charges per TB scanned. Mitigations: year/month/day/hour partitioning for pruning · Parquet columnar format for column pruning · SNAPPY compression · 1 GB scan limit per query · gold pre-aggregated tables used by QuickSight instead of Silver.
