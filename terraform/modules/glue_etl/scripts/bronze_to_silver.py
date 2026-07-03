import sys
from datetime import datetime, timedelta, timezone

from awsglue.context import GlueContext
from awsglue.job import Job
import boto3
from urllib.parse import urlparse

from pyspark.context import SparkContext
from pyspark.sql.functions import (
    col,
    coalesce,
    lit,
    lower,
    to_date,
    to_json,
    to_timestamp,
)
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructType,
)


def get_arg(name: str, default: str | None = None) -> str | None:
    """
    Read optional Glue job argument from sys.argv.

    Example:
      --PROCESS_YEAR 2026
    """
    key = f"--{name}"

    if key not in sys.argv:
        return default

    index = sys.argv.index(key)

    if index + 1 >= len(sys.argv):
        return default

    return sys.argv[index + 1]


def delete_s3_prefix(s3_uri: str) -> None:
    """
    Delete all objects under an S3 prefix.

    This makes the job idempotent:
    rerunning the same hour replaces the previous Silver output.
    """
    parsed = urlparse(s3_uri)

    if parsed.scheme != "s3":
        raise ValueError(f"Invalid S3 URI: {s3_uri}")

    bucket = parsed.netloc
    prefix = parsed.path.lstrip("/")

    if not prefix.endswith("/"):
        prefix = f"{prefix}/"

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



def require_arg(name: str) -> str:
    value = get_arg(name)

    if not value:
        raise ValueError(f"Missing required Glue argument --{name}")

    return value


def z2(value: str | int) -> str:
    return f"{int(value):02d}"


def get_nested_type(schema, path: str):
    """
    Returns the Spark data type of a nested path like payload.wiki.
    Returns None if the field does not exist.

    This keeps the ETL null-safe when Wikimedia/Collector schema evolves.
    """
    current_type = schema

    for part in path.split("."):
        if not isinstance(current_type, StructType):
            return None

        field = next((f for f in current_type.fields if f.name == part), None)

        if field is None:
            return None

        current_type = field.dataType

    return current_type


def field_exists(df, path: str) -> bool:
    return get_nested_type(df.schema, path) is not None


def safe_col(df, path: str, data_type=StringType()):
    """
    Returns col(path).cast(data_type) if the nested field exists.
    Otherwise returns NULL casted to data_type.
    """
    if field_exists(df, path):
        return col(path).cast(data_type)

    return lit(None).cast(data_type)


def safe_json_col(df, path: str):
    """
    log_params may be object, array, map, string, or absent.
    Silver stores it as string JSON for analytics safety.
    """
    data_type = get_nested_type(df.schema, path)

    if data_type is None:
        return lit(None).cast(StringType())

    if isinstance(data_type, (StructType, ArrayType, MapType)):
        return to_json(col(path))

    return col(path).cast(StringType())


def resolve_processing_hour():
    """
    If explicit PROCESS_YEAR/MONTH/DAY/HOUR are provided, process that hour.
    Otherwise process the previous complete UTC hour.
    """
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


sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

spark.conf.set("spark.sql.session.timeZone", "UTC")

job_name = get_arg("JOB_NAME", "bronze_to_silver")
job = Job(glue_context)
job.init(job_name, {"JOB_NAME": job_name})

bronze_base_path = require_arg("BRONZE_BASE_PATH").rstrip("/")
silver_base_path = require_arg("SILVER_BASE_PATH").rstrip("/")
write_mode = get_arg("WRITE_MODE", "append")

process_year, process_month, process_day, process_hour = resolve_processing_hour()

bronze_input_path = (
    f"{bronze_base_path}/"
    f"year={process_year}/"
    f"month={process_month}/"
    f"day={process_day}/"
    f"hour={process_hour}/"
)

print(f"Starting bronze_to_silver job")
print(f"Bronze input path : {bronze_input_path}")
print(f"Silver output path: {silver_base_path}")
print(f"Write mode        : {write_mode}")

bronze_df = (
    spark.read
    .option("mode", "PERMISSIVE")
    .json(bronze_input_path)
)

input_count = bronze_df.count()
print(f"Bronze input records: {input_count}")

valid_df = (
    bronze_df
    .filter(col("event_id").isNotNull())
    .filter(col("occurred_at").isNotNull())
)

valid_count = valid_df.count()
print(f"Valid records after required-field filtering: {valid_count}")

old_length = coalesce(
    safe_col(valid_df, "payload.old_length", LongType()),
    safe_col(valid_df, "payload.length_old", LongType()),
)

new_length = coalesce(
    safe_col(valid_df, "payload.new_length", LongType()),
    safe_col(valid_df, "payload.length_new", LongType()),
)

delta_bytes = coalesce(
    safe_col(valid_df, "payload.delta_bytes", LongType()),
    safe_col(valid_df, "payload.length_delta", LongType()),
    new_length - old_length,
)

occurred_at_ts = to_timestamp(col("occurred_at"))

silver_df = valid_df.select(
    col("event_id").cast(StringType()).alias("event_id"),
    occurred_at_ts.alias("occurred_at"),
    to_date(occurred_at_ts).alias("ingestion_date"),

    lower(safe_col(valid_df, "payload.wiki", StringType())).alias("wiki"),
    safe_col(valid_df, "payload.domain", StringType()).alias("domain"),
    safe_col(valid_df, "payload.change_type", StringType()).alias("change_type"),
    safe_col(valid_df, "payload.namespace", IntegerType()).alias("namespace"),
    safe_col(valid_df, "payload.title", StringType()).alias("title"),
    safe_col(valid_df, "payload.title_url", StringType()).alias("title_url"),
    safe_col(valid_df, "payload.user", StringType()).alias("user"),

    coalesce(
        safe_col(valid_df, "payload.user_is_bot", BooleanType()),
        safe_col(valid_df, "payload.bot", BooleanType()),
    ).alias("user_is_bot"),

    coalesce(
        safe_col(valid_df, "payload.is_minor", BooleanType()),
        safe_col(valid_df, "payload.minor", BooleanType()),
    ).alias("is_minor"),

    coalesce(
        safe_col(valid_df, "payload.is_patrolled", BooleanType()),
        safe_col(valid_df, "payload.patrolled", BooleanType()),
    ).alias("is_patrolled"),

    old_length.alias("old_length"),
    new_length.alias("new_length"),
    delta_bytes.alias("delta_bytes"),

    safe_col(valid_df, "payload.revision_old", LongType()).alias("revision_old"),
    safe_col(valid_df, "payload.revision_new", LongType()).alias("revision_new"),
    safe_col(valid_df, "payload.change_url", StringType()).alias("change_url"),
    safe_col(valid_df, "payload.raw_notify_url", StringType()).alias("raw_notify_url"),
    safe_col(valid_df, "payload.log_type", StringType()).alias("log_type"),
    safe_col(valid_df, "payload.log_action", StringType()).alias("log_action"),
    safe_json_col(valid_df, "payload.log_params").alias("log_params"),

    coalesce(
        safe_col(valid_df, "payload.wikimedia_recentchange_id", LongType()),
        safe_col(valid_df, "payload.id", LongType()),
    ).alias("wikimedia_rcid"),
)

silver_df = (
    silver_df
    .filter(col("occurred_at").isNotNull())
    .filter(col("wiki").isNotNull())
    .filter(col("change_type").isNotNull())
    .filter(col("namespace").isNotNull())
)

silver_count = silver_df.count()
print(f"Silver output records: {silver_count}")


silver_output_path = (
    f"{silver_base_path}/"
    f"year={process_year}/"
    f"month={process_month}/"
    f"day={process_day}/"
    f"hour={process_hour}/"
)

print(f"Silver partition output path: {silver_output_path}")

delete_s3_prefix(silver_output_path)



(
    silver_df.write
    .mode("append")
    .option("compression", "snappy")
    .parquet(silver_output_path)
)



print("bronze_to_silver job completed successfully")

job.commit()