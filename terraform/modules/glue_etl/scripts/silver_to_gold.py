import sys
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import (
    asc,
    coalesce,
    col,
    count,
    countDistinct,
    desc,
    lit,
    max as spark_max,
    min as spark_min,
    row_number,
    sum as spark_sum,
    to_date,
    to_timestamp,
    when,
)
from pyspark.sql.types import BooleanType, IntegerType, StringType
from pyspark.sql.window import Window


def get_arg(name: str, default: str | None = None) -> str | None:
    key = f"--{name}"

    if key not in sys.argv:
        return default

    index = sys.argv.index(key)

    if index + 1 >= len(sys.argv):
        return default

    return sys.argv[index + 1]


def require_arg(name: str) -> str:
    value = get_arg(name)

    if not value:
        raise ValueError(f"Missing required Glue argument --{name}")

    return value


def z2(value: str | int) -> str:
    return f"{int(value):02d}"


def resolve_processing_hour():
    year = get_arg("PROCESS_YEAR")
    month = get_arg("PROCESS_MONTH")
    day = get_arg("PROCESS_DAY")
    hour = get_arg("PROCESS_HOUR")

    if year and month and day and hour:
        return str(year), z2(month), z2(day), z2(hour)

    previous_hour = (
        datetime.now(timezone.utc)
        .replace(minute=0, second=0, microsecond=0)
        - timedelta(hours=1)
    )

    return (
        str(previous_hour.year),
        z2(previous_hour.month),
        z2(previous_hour.day),
        z2(previous_hour.hour),
    )


def parse_s3_uri(s3_uri: str):
    parsed = urlparse(s3_uri)

    if parsed.scheme != "s3":
        raise ValueError(f"Invalid S3 URI: {s3_uri}")

    bucket = parsed.netloc
    prefix = parsed.path.lstrip("/")

    if prefix and not prefix.endswith("/"):
        prefix = f"{prefix}/"

    return bucket, prefix


def s3_prefix_has_parquet_objects(s3_uri: str) -> bool:
    bucket, prefix = parse_s3_uri(s3_uri)

    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                return True

    return False


def delete_s3_prefix(s3_uri: str) -> None:
    bucket, prefix = parse_s3_uri(s3_uri)

    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")

    total_deleted = 0

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        objects = page.get("Contents", [])

        if not objects:
            continue

        response = s3.delete_objects(
            Bucket=bucket,
            Delete={
                "Objects": [{"Key": obj["Key"]} for obj in objects]
            },
        )

        total_deleted += len(response.get("Deleted", []))

    print(f"Deleted {total_deleted} existing objects under {s3_uri}")


def hourly_path(base_path: str, year: str, month: str, day: str, hour: str) -> str:
    return (
        f"{base_path.rstrip('/')}/"
        f"year={year}/"
        f"month={month}/"
        f"day={day}/"
        f"hour={hour}/"
    )


def add_metric_columns(df, metric_date: str, metric_hour: str):
    return (
        df
        .withColumn("metric_date", to_date(lit(metric_date)))
        .withColumn("metric_hour", to_timestamp(lit(metric_hour)))
    )


def bot_count_expr():
    return spark_sum(
        when(col("user_is_bot") == lit(True), lit(1)).otherwise(lit(0))
    ).cast("bigint")


def human_count_expr():
    return spark_sum(
        when(col("user_is_bot") == lit(False), lit(1)).otherwise(lit(0))
    ).cast("bigint")


def write_gold_dataset(df, output_path: str, dataset_name: str) -> None:
    row_count = df.count()
    print(f"Gold dataset {dataset_name} rows: {row_count}")
    print(f"Gold dataset {dataset_name} output path: {output_path}")

    delete_s3_prefix(output_path)

    (
        df.write
        .mode("append")
        .option("compression", "snappy")
        .parquet(output_path)
    )


sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

spark.conf.set("spark.sql.session.timeZone", "UTC")

job_name = get_arg("JOB_NAME", "silver_to_gold")
job = Job(glue_context)
job.init(job_name, {"JOB_NAME": job_name})

silver_base_path = require_arg("SILVER_BASE_PATH").rstrip("/")
gold_base_path = require_arg("GOLD_BASE_PATH").rstrip("/")
top_pages_limit = int(get_arg("TOP_PAGES_LIMIT", "100"))

process_year, process_month, process_day, process_hour = resolve_processing_hour()

processing_date = f"{process_year}-{process_month}-{process_day}"
processing_hour_start = f"{processing_date} {process_hour}:00:00"

silver_input_path = hourly_path(
    silver_base_path,
    process_year,
    process_month,
    process_day,
    process_hour,
)

print("Starting silver_to_gold job")
print(f"Silver input path       : {silver_input_path}")
print(f"Gold base path          : {gold_base_path}")
print(f"Processing date         : {processing_date}")
print(f"Processing hour start   : {processing_hour_start}")
print(f"Top pages limit         : {top_pages_limit}")

if not s3_prefix_has_parquet_objects(silver_input_path):
    raise ValueError(
        f"No Silver Parquet files found under {silver_input_path}. "
        "Refusing to overwrite Gold outputs."
    )

silver_df = spark.read.parquet(silver_input_path).cache()

input_count = silver_df.count()
print(f"Silver input records: {input_count}")

if input_count == 0:
    raise ValueError(
        f"Silver input path {silver_input_path} contains zero rows. "
        "Refusing to overwrite Gold outputs."
    )

base_df = (
    silver_df
    .filter(col("event_id").isNotNull())
    .filter(col("occurred_at").isNotNull())
    .filter(col("wiki").isNotNull())
    .filter(col("change_type").isNotNull())
    .filter(col("namespace").isNotNull())
    .select(
        col("event_id").cast(StringType()).alias("event_id"),
        col("occurred_at").alias("occurred_at"),
        col("wiki").cast(StringType()).alias("wiki"),
        col("domain").cast(StringType()).alias("domain"),
        col("change_type").cast(StringType()).alias("change_type"),
        col("namespace").cast(IntegerType()).alias("namespace"),
        col("title").cast(StringType()).alias("title"),
        coalesce(col("user_is_bot"), lit(False)).cast(BooleanType()).alias("user_is_bot"),
    )
    .cache()
)

valid_count = base_df.count()
print(f"Valid Silver records for Gold: {valid_count}")

if valid_count == 0:
    raise ValueError(
        "No valid Silver records after filtering required fields. "
        "Refusing to overwrite Gold outputs."
    )

wiki_activity_df = (
    base_df
    .groupBy("wiki")
    .agg(
        count(lit(1)).cast("bigint").alias("event_count"),
        bot_count_expr().alias("bot_event_count"),
        human_count_expr().alias("human_event_count"),
        countDistinct("title").cast("bigint").alias("distinct_titles"),
        spark_min("occurred_at").alias("first_event_at"),
        spark_max("occurred_at").alias("last_event_at"),
    )
)

wiki_activity_df = (
    add_metric_columns(wiki_activity_df, processing_date, processing_hour_start)
    .select(
        "metric_date",
        "metric_hour",
        "wiki",
        "event_count",
        "bot_event_count",
        "human_event_count",
        "distinct_titles",
        "first_event_at",
        "last_event_at",
    )
)

change_type_df = (
    base_df
    .groupBy("change_type")
    .agg(
        count(lit(1)).cast("bigint").alias("event_count"),
        spark_min("occurred_at").alias("first_event_at"),
        spark_max("occurred_at").alias("last_event_at"),
    )
)

change_type_df = (
    add_metric_columns(change_type_df, processing_date, processing_hour_start)
    .select(
        "metric_date",
        "metric_hour",
        "change_type",
        "event_count",
        "first_event_at",
        "last_event_at",
    )
)

bot_activity_df = (
    base_df
    .groupBy("user_is_bot")
    .agg(
        count(lit(1)).cast("bigint").alias("event_count"),
        spark_min("occurred_at").alias("first_event_at"),
        spark_max("occurred_at").alias("last_event_at"),
    )
)

bot_activity_df = (
    add_metric_columns(bot_activity_df, processing_date, processing_hour_start)
    .select(
        "metric_date",
        "metric_hour",
        "user_is_bot",
        "event_count",
        "first_event_at",
        "last_event_at",
    )
)

namespace_activity_df = (
    base_df
    .groupBy("namespace")
    .agg(
        count(lit(1)).cast("bigint").alias("event_count"),
        spark_min("occurred_at").alias("first_event_at"),
        spark_max("occurred_at").alias("last_event_at"),
    )
)

namespace_activity_df = (
    add_metric_columns(namespace_activity_df, processing_date, processing_hour_start)
    .select(
        "metric_date",
        "metric_hour",
        "namespace",
        "event_count",
        "first_event_at",
        "last_event_at",
    )
)

top_pages_agg_df = (
    base_df
    .filter(col("title").isNotNull())
    .groupBy("wiki", "namespace", "title")
    .agg(
        count(lit(1)).cast("bigint").alias("event_count"),
        bot_count_expr().alias("bot_event_count"),
        human_count_expr().alias("human_event_count"),
        spark_min("occurred_at").alias("first_event_at"),
        spark_max("occurred_at").alias("last_event_at"),
    )
)

ranking_window = Window.orderBy(
    desc("event_count"),
    asc("wiki"),
    asc("namespace"),
    asc("title"),
)

top_pages_df = (
    top_pages_agg_df
    .withColumn("page_rank", row_number().over(ranking_window).cast("int"))
    .filter(col("page_rank") <= lit(top_pages_limit))
)

top_pages_df = (
    add_metric_columns(top_pages_df, processing_date, processing_hour_start)
    .select(
        "metric_date",
        "metric_hour",
        "page_rank",
        "wiki",
        "namespace",
        "title",
        "event_count",
        "bot_event_count",
        "human_event_count",
        "first_event_at",
        "last_event_at",
    )
)

gold_datasets = {
    "wiki_activity_by_hour": wiki_activity_df,
    "change_type_by_hour": change_type_df,
    "bot_activity_by_hour": bot_activity_df,
    "namespace_activity_by_hour": namespace_activity_df,
    "top_pages_by_hour": top_pages_df,
}

for dataset_name, dataset_df in gold_datasets.items():
    output_path = hourly_path(
        f"{gold_base_path}/{dataset_name}",
        process_year,
        process_month,
        process_day,
        process_hour,
    )

    write_gold_dataset(dataset_df, output_path, dataset_name)

base_df.unpersist()
silver_df.unpersist()

print("silver_to_gold job completed successfully")

job.commit()