from __future__ import annotations

import hashlib
import json
import logging
import os
import random
import time
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from contextvars import copy_context
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Iterator


import boto3
from boto3.dynamodb.types import TypeDeserializer
from botocore.config import Config
from opentelemetry import propagate, trace
from opentelemetry.trace import Status, StatusCode

from .observability import flush_otel, meter, tracer


LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
ENVIRONMENT = os.getenv("ENVIRONMENT", "dev")
logger = logging.getLogger()
logger.setLevel(LOG_LEVEL)


def _env_int(
    name: str,
    default: int,
    minimum: int = 1,
    maximum: int | None = None,
) -> int:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default

    try:
        value = int(raw_value)
    except ValueError:
        logger.warning("invalid_integer_environment_variable: %s=%r", name, raw_value)
        return default

    if value < minimum:
        return default

    if maximum is not None and value > maximum:
        return maximum

    return value


def _env_bool(name: str, default: bool = False) -> bool:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    return raw_value.strip().lower() in {"1", "true", "yes", "on"}


AGGREGATES_TABLE_NAME = os.environ["AGGREGATES_TABLE_NAME"]
SNAPSHOTS_TABLE_NAME = os.environ["SNAPSHOTS_TABLE_NAME"]
BROADCAST_JOBS_QUEUE_URL = os.environ["BROADCAST_JOBS_QUEUE_URL"]

CONNECTION_SHARD_COUNT = _env_int(
    "CONNECTION_SHARD_COUNT",
    _env_int("SUBSCRIPTION_SHARD_COUNT", 20, 1, 1000),
    1,
    1000,
)
PUBLISH_SHARD_JOBS = _env_bool("PUBLISH_SHARD_JOBS", True)
IDEMPOTENCY_LEASE_SECONDS = _env_int(
    "IDEMPOTENCY_LEASE_SECONDS",
    60,
    10,
    900,
)
SNAPSHOT_TTL_SECONDS = _env_int("SNAPSHOT_TTL_SECONDS", 900, 60, 86400)
GLOBAL_ACTIVITY_SHARD_COUNT = _env_int("GLOBAL_ACTIVITY_SHARD_COUNT", 10, 1, 100)
TOP_METRIC_SHARD_COUNT = _env_int("TOP_METRIC_SHARD_COUNT", 10, 1, 100)
TOP_WIKIS_LIMIT = _env_int("TOP_WIKIS_LIMIT", 10, 1, 100)
TOP_PAGES_LIMIT = _env_int("TOP_PAGES_LIMIT", 10, 1, 100)
DYNAMODB_READ_WORKERS = _env_int("DYNAMODB_READ_WORKERS", 24, 1, 64)
DYNAMODB_MAX_POOL_CONNECTIONS = _env_int(
    "DYNAMODB_MAX_POOL_CONNECTIONS",
    max(DYNAMODB_READ_WORKERS + 8, 32),
    DYNAMODB_READ_WORKERS,
    128,
)
DYNAMODB_BATCH_GET_MAX_RETRIES = _env_int(
    "DYNAMODB_BATCH_GET_MAX_RETRIES",
    5,
    1,
    10,
)

CHANGE_TYPES = [
    value.strip()
    for value in os.getenv(
        "CHANGE_TYPES",
        "edit,new,categorize,log,external",
    ).split(",")
    if value.strip()
]

NAMESPACES = [
    value.strip()
    for value in os.getenv(
        "NAMESPACES",
        "-1,0,1,2,4,6,10,14",
    ).split(",")
    if value.strip()
]

ENABLE_OTEL_FLUSH = os.getenv("ENABLE_OTEL_FLUSH", "true").lower() == "true"


dynamodb_client = boto3.client(
    "dynamodb",
    config=Config(
        max_pool_connections=DYNAMODB_MAX_POOL_CONNECTIONS,
        connect_timeout=2,
        read_timeout=5,
        retries={"mode": "standard", "max_attempts": 4},
    ),
)
dynamodb_resource = boto3.resource("dynamodb")
snapshots_table = dynamodb_resource.Table(SNAPSHOTS_TABLE_NAME)
sqs_client = boto3.client("sqs")

DYNAMODB_READ_EXECUTOR = ThreadPoolExecutor(
    max_workers=DYNAMODB_READ_WORKERS,
    thread_name_prefix="coordinator-ddb-read",
)
DYNAMODB_DESERIALIZER = TypeDeserializer()


coordinator_signals_total = meter.create_counter(
    name="broadcast_coordinator_signals_total",
    unit="1",
    description="Broadcast signals processed by the Coordinator.",
)
coordinator_signals_failed_total = meter.create_counter(
    name="broadcast_coordinator_signals_failed_total",
    unit="1",
    description="Broadcast signals that failed in the Coordinator.",
)
broadcast_snapshots_created_total = meter.create_counter(
    name="broadcast_snapshots_created_total",
    unit="1",
    description="Real snapshots created by the Coordinator.",
)
broadcast_jobs_created_total = meter.create_counter(
    name="broadcast_jobs_created_total",
    unit="1",
    description="Fan-out jobs created by the Coordinator.",
)
broadcast_jobs_planned_total = meter.create_counter(
    name="broadcast_jobs_planned_total",
    unit="1",
    description="Shard-centric jobs planned by the Coordinator.",
)
broadcast_manifests_created_total = meter.create_counter(
    name="broadcast_manifests_created_total",
    unit="1",
    description="Broadcast manifests created by the Coordinator.",
)
broadcast_coordinator_duplicates_total = meter.create_counter(
    name="broadcast_coordinator_duplicates_total",
    unit="1",
    description="Duplicate signals skipped by the Coordinator.",
)
broadcast_coordinator_duration_ms = meter.create_histogram(
    name="broadcast_coordinator_duration_ms",
    unit="ms",
    description="Coordinator signal processing duration.",
)
broadcast_coordinator_aggregate_read_duration_ms = meter.create_histogram(
    name="broadcast_coordinator_aggregate_read_duration_ms",
    unit="ms",
    description="Aggregate read duration for one topic snapshot.",
)
broadcast_coordinator_payload_build_duration_ms = meter.create_histogram(
    name="broadcast_coordinator_payload_build_duration_ms",
    unit="ms",
    description="Payload build and snapshot persistence duration.",
)


def log_json(level: str, message: str, **fields: Any) -> None:
    span_context = trace.get_current_span().get_span_context()

    trace_id = None
    span_id = None

    if span_context.is_valid:
        trace_id = format(span_context.trace_id, "032x")
        span_id = format(span_context.span_id, "016x")

    log_method = getattr(logger, level.lower(), logger.info)
    log_method(
        json.dumps(
            {
                "message": message,
                "trace_id": trace_id,
                "span_id": span_id,
                **fields,
            },
            default=str,
            separators=(",", ":"),
        )
    )


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace(
        "+00:00",
        "Z",
    )


def parse_iso_datetime(value: str) -> datetime | None:
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (AttributeError, ValueError):
        return None


def iso_to_epoch_ms(value: str) -> int:
    timestamp = parse_iso_datetime(value)
    if timestamp is None:
        raise ValueError(f"Invalid ISO-8601 timestamp: {value}")
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    return int(timestamp.timestamp() * 1000)


def normalize_window_key(aggregation_window: str) -> str:
    if aggregation_window.startswith("WINDOW#"):
        return aggregation_window
    return f"WINDOW#{aggregation_window}"


def strip_window_prefix(value: str) -> str:
    return value.removeprefix("WINDOW#")


def is_partial_window(aggregation_window: str) -> bool:
    window_start = parse_iso_datetime(strip_window_prefix(aggregation_window))
    if window_start is None:
        return True
    return datetime.now(timezone.utc) < window_start + timedelta(minutes=1)


def to_int(value: Any, default: int = 0) -> int:
    if value is None:
        return default
    if isinstance(value, Decimal):
        return int(value)
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return default
    return default


def to_dynamodb_safe(value: Any) -> Any:
    """DynamoDB Resource rejects Python float values inside Map/List values."""
    if isinstance(value, float):
        return Decimal(str(value))
    if isinstance(value, dict):
        return {key: to_dynamodb_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [to_dynamodb_safe(item) for item in value]
    if isinstance(value, tuple):
        return [to_dynamodb_safe(item) for item in value]
    return value


def _submit_with_context(
    executor: ThreadPoolExecutor,
    function: Any,
    *args: Any,
    **kwargs: Any,
) -> Future:
    context = copy_context()
    return executor.submit(context.run, function, *args, **kwargs)


def _deserialize_dynamodb_item(raw_item: dict[str, Any]) -> dict[str, Any]:
    return {
        key: DYNAMODB_DESERIALIZER.deserialize(value)
        for key, value in raw_item.items()
    }


def _chunks(values: list[str], size: int) -> list[list[str]]:
    return [values[index : index + size] for index in range(0, len(values), size)]


def get_event_count(item: dict[str, Any] | None) -> int:
    return to_int(item.get("event_count"), 0) if item else 0


def _batch_get_metric_counts(
    metric_keys: list[str],
    window_key: str,
) -> dict[str, int]:
    counts: dict[str, int] = {}

    for metric_key_chunk in _chunks(list(dict.fromkeys(metric_keys)), 100):
        request_items: dict[str, Any] = {
            AGGREGATES_TABLE_NAME: {
                "Keys": [
                    {
                        "metric_key": {"S": metric_key},
                        "window_key": {"S": window_key},
                    }
                    for metric_key in metric_key_chunk
                ],
                "ProjectionExpression": "#mk, #wk, #ec",
                "ExpressionAttributeNames": {
                    "#mk": "metric_key",
                    "#wk": "window_key",
                    "#ec": "event_count",
                },
                "ConsistentRead": False,
            }
        }
        attempt = 0

        while request_items:
            response = dynamodb_client.batch_get_item(RequestItems=request_items)

            for raw_item in response.get("Responses", {}).get(
                AGGREGATES_TABLE_NAME,
                [],
            ):
                item = _deserialize_dynamodb_item(raw_item)
                metric_key = item.get("metric_key")
                if metric_key:
                    counts[str(metric_key)] = get_event_count(item)

            unprocessed = response.get("UnprocessedKeys") or {}
            table_unprocessed = unprocessed.get(AGGREGATES_TABLE_NAME) or {}
            if not table_unprocessed.get("Keys"):
                break

            attempt += 1
            if attempt > DYNAMODB_BATCH_GET_MAX_RETRIES:
                raise RuntimeError(
                    "DynamoDB BatchGetItem still has unprocessed keys after "
                    f"{DYNAMODB_BATCH_GET_MAX_RETRIES} retries"
                )

            time.sleep(
                min(
                    0.05 * (2 ** (attempt - 1)) + random.uniform(0.0, 0.025),
                    1.0,
                )
            )
            request_items = {AGGREGATES_TABLE_NAME: table_unprocessed}

    return counts


def _query_metric_items(
    metric_key: str,
    window_key_prefix: str,
    projection_expression: str,
    projection_attribute_names: dict[str, str],
) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    exclusive_start_key: dict[str, Any] | None = None

    while True:
        kwargs: dict[str, Any] = {
            "TableName": AGGREGATES_TABLE_NAME,
            "KeyConditionExpression": (
                "#mk = :metric_key AND begins_with(#wk, :window_prefix)"
            ),
            "ExpressionAttributeNames": {
                "#mk": "metric_key",
                "#wk": "window_key",
                **projection_attribute_names,
            },
            "ExpressionAttributeValues": {
                ":metric_key": {"S": metric_key},
                ":window_prefix": {"S": window_key_prefix},
            },
            "ProjectionExpression": projection_expression,
            "ConsistentRead": False,
        }
        if exclusive_start_key:
            kwargs["ExclusiveStartKey"] = exclusive_start_key

        response = dynamodb_client.query(**kwargs)
        items.extend(
            _deserialize_dynamodb_item(raw_item)
            for raw_item in response.get("Items", [])
        )
        exclusive_start_key = response.get("LastEvaluatedKey")
        if not exclusive_start_key:
            return items


def _query_top_wikis_shard(
    shard_id: int,
    window_key_prefix: str,
) -> list[dict[str, Any]]:
    return _query_metric_items(
        metric_key=f"METRIC#TOP_WIKIS#SHARD#{shard_id}",
        window_key_prefix=window_key_prefix,
        projection_expression="#wk, #wiki, #ec",
        projection_attribute_names={"#wiki": "wiki", "#ec": "event_count"},
    )


def _query_top_pages_shard(
    shard_id: int,
    window_key_prefix: str,
) -> list[dict[str, Any]]:
    return _query_metric_items(
        metric_key=f"METRIC#TOP_PAGES#SHARD#{shard_id}",
        window_key_prefix=window_key_prefix,
        projection_expression=(
            "#wiki, #title, #ec, #title_url, #namespace, "
            "#last_change_type, #last_seen_at"
        ),
        projection_attribute_names={
            "#wiki": "wiki",
            "#title": "title",
            "#ec": "event_count",
            "#title_url": "title_url",
            "#namespace": "namespace",
            "#last_change_type": "last_change_type",
            "#last_seen_at": "last_seen_at",
        },
    )


def _collect_top_wikis(futures: list[Future]) -> list[dict[str, Any]]:
    candidates: dict[str, int] = {}
    for future in as_completed(futures):
        for item in future.result():
            wiki = item.get("wiki")
            if not wiki:
                window_item_key = str(item.get("window_key", ""))
                if "#WIKI#" in window_item_key:
                    wiki = window_item_key.split("#WIKI#", 1)[1]
            if wiki:
                candidates[str(wiki)] = candidates.get(str(wiki), 0) + get_event_count(item)

    return [
        {"wiki": wiki, "count": count}
        for wiki, count in sorted(
            candidates.items(),
            key=lambda candidate: candidate[1],
            reverse=True,
        )[:TOP_WIKIS_LIMIT]
    ]


def _collect_top_pages(futures: list[Future]) -> list[dict[str, Any]]:
    pages: list[dict[str, Any]] = []
    for future in as_completed(futures):
        for item in future.result():
            pages.append(
                {
                    "wiki": item.get("wiki"),
                    "title": item.get("title"),
                    "count": get_event_count(item),
                    "url": item.get("title_url"),
                    "title_url": item.get("title_url"),
                    "namespace": item.get("namespace"),
                    "last_change_type": item.get("last_change_type"),
                    "last_seen_at": item.get("last_seen_at"),
                }
            )

    pages = [
        page
        for page in pages
        if page.get("wiki") and page.get("title") and page.get("count", 0) > 0
    ]
    pages.sort(key=lambda page: page["count"], reverse=True)
    return pages[:TOP_PAGES_LIMIT]


def read_global_snapshot(window_key: str) -> tuple[dict[str, Any], float]:
    started_at = time.perf_counter()
    global_activity_keys = [
        f"METRIC#GLOBAL_ACTIVITY#SHARD#{shard_id}"
        for shard_id in range(GLOBAL_ACTIVITY_SHARD_COUNT)
    ]
    bot_keys = [
        "METRIC#BOT_ACTIVITY#BOT#true",
        "METRIC#BOT_ACTIVITY#BOT#false",
    ]
    change_type_keys = [
        f"METRIC#CHANGE_TYPE#TYPE#{change_type}" for change_type in CHANGE_TYPES
    ]
    namespace_keys = [
        f"METRIC#NAMESPACE#NS#{namespace}" for namespace in NAMESPACES
    ]

    counts_future = _submit_with_context(
        DYNAMODB_READ_EXECUTOR,
        _batch_get_metric_counts,
        global_activity_keys + bot_keys + change_type_keys + namespace_keys,
        window_key,
    )
    top_wikis_futures = [
        _submit_with_context(
            DYNAMODB_READ_EXECUTOR,
            _query_top_wikis_shard,
            shard_id,
            f"{window_key}#WIKI#",
        )
        for shard_id in range(TOP_METRIC_SHARD_COUNT)
    ]
    top_pages_futures = [
        _submit_with_context(
            DYNAMODB_READ_EXECUTOR,
            _query_top_pages_shard,
            shard_id,
            f"{window_key}#",
        )
        for shard_id in range(TOP_METRIC_SHARD_COUNT)
    ]

    counts = counts_future.result()
    top_wikis = _collect_top_wikis(top_wikis_futures)
    top_pages = _collect_top_pages(top_pages_futures)
    bot_count = counts.get("METRIC#BOT_ACTIVITY#BOT#true", 0)
    human_count = counts.get("METRIC#BOT_ACTIVITY#BOT#false", 0)
    bot_total = bot_count + human_count

    return (
        {
            "current_minute_events_so_far": sum(
                counts.get(metric_key, 0) for metric_key in global_activity_keys
            ),
            "bot_count": bot_count,
            "human_count": human_count,
            "bot_ratio": round(bot_count / bot_total, 4) if bot_total else 0.0,
            "top_wikis": top_wikis,
            "change_types": {
                change_type: counts.get(
                    f"METRIC#CHANGE_TYPE#TYPE#{change_type}",
                    0,
                )
                for change_type in CHANGE_TYPES
            },
            "namespace_distribution": {
                str(namespace): count
                for namespace in NAMESPACES
                if (
                    count := counts.get(
                        f"METRIC#NAMESPACE#NS#{namespace}",
                        0,
                    )
                ) > 0
            },
            "top_pages": top_pages,
        },
        round((time.perf_counter() - started_at) * 1000, 2),
    )


def read_top_pages_snapshot(window_key: str) -> tuple[list[dict[str, Any]], float]:
    started_at = time.perf_counter()
    futures = [
        _submit_with_context(
            DYNAMODB_READ_EXECUTOR,
            _query_top_pages_shard,
            shard_id,
            f"{window_key}#",
        )
        for shard_id in range(TOP_METRIC_SHARD_COUNT)
    ]
    return (
        _collect_top_pages(futures),
        round((time.perf_counter() - started_at) * 1000, 2),
    )


def read_wiki_snapshot(wiki: str, window_key: str) -> tuple[dict[str, Any], float]:
    started_at = time.perf_counter()
    wiki_activity_key = f"METRIC#WIKI_ACTIVITY#WIKI#{wiki}"
    bot_true_key = f"METRIC#WIKI_BOT_ACTIVITY#WIKI#{wiki}#BOT#true"
    bot_false_key = f"METRIC#WIKI_BOT_ACTIVITY#WIKI#{wiki}#BOT#false"
    change_type_keys = [
        f"METRIC#WIKI_CHANGE_TYPE#WIKI#{wiki}#TYPE#{change_type}"
        for change_type in CHANGE_TYPES
    ]
    namespace_keys = [
        f"METRIC#WIKI_NAMESPACE#WIKI#{wiki}#NS#{namespace}"
        for namespace in NAMESPACES
    ]

    counts_future = _submit_with_context(
        DYNAMODB_READ_EXECUTOR,
        _batch_get_metric_counts,
        [
            wiki_activity_key,
            bot_true_key,
            bot_false_key,
            *change_type_keys,
            *namespace_keys,
        ],
        window_key,
    )
    top_pages_futures = [
        _submit_with_context(
            DYNAMODB_READ_EXECUTOR,
            _query_top_pages_shard,
            shard_id,
            f"{window_key}#WIKI#{wiki}#",
        )
        for shard_id in range(TOP_METRIC_SHARD_COUNT)
    ]

    counts = counts_future.result()
    top_pages = _collect_top_pages(top_pages_futures)
    bot_count = counts.get(bot_true_key, 0)
    human_count = counts.get(bot_false_key, 0)
    total = bot_count + human_count

    return (
        {
            "current_minute_events_so_far": counts.get(wiki_activity_key, 0),
            "bot_count": bot_count,
            "human_count": human_count,
            "bot_ratio": round(bot_count / total, 4) if total else 0.0,
            "change_types": {
                change_type: count
                for change_type in CHANGE_TYPES
                if (
                    count := counts.get(
                        f"METRIC#WIKI_CHANGE_TYPE#WIKI#{wiki}#TYPE#{change_type}",
                        0,
                    )
                ) > 0
            },
            "namespace_distribution": {
                str(namespace): count
                for namespace in NAMESPACES
                if (
                    count := counts.get(
                        f"METRIC#WIKI_NAMESPACE#WIKI#{wiki}#NS#{namespace}",
                        0,
                    )
                ) > 0
            },
            "top_pages": top_pages,
        },
        round((time.perf_counter() - started_at) * 1000, 2),
    )


def add_freshness_fields(
    message: dict[str, Any],
    latest_event_timestamp_ms: int | None,
    oldest_event_timestamp_ms: int | None,
) -> dict[str, Any]:
    if latest_event_timestamp_ms is not None:
        message["latest_event_timestamp_ms"] = latest_event_timestamp_ms
    if oldest_event_timestamp_ms is not None:
        message["oldest_event_timestamp_ms"] = oldest_event_timestamp_ms
    return message


def get_sqs_trace_carrier(record: dict[str, Any]) -> dict[str, str]:
    carrier: dict[str, str] = {}
    message_attributes = record.get("messageAttributes", {})
    for key in ("traceparent", "tracestate", "baggage"):
        attribute = message_attributes.get(key)
        if not isinstance(attribute, dict):
            continue
        value = attribute.get("stringValue") or attribute.get("StringValue")
        if value:
            carrier[key] = str(value)
    return carrier


def build_trace_message_attributes() -> dict[str, dict[str, str]]:
    carrier: dict[str, str] = {}
    propagate.inject(carrier)
    return {
        key: {"DataType": "String", "StringValue": value}
        for key in ("traceparent", "tracestate", "baggage")
        if (value := carrier.get(key))
    }


def iter_input_records(
    event: dict[str, Any],
) -> Iterator[tuple[str | None, dict[str, Any], dict[str, Any] | None]]:
    records = event.get("Records")
    if isinstance(records, list):
        for record in records:
            body = record.get("body")
            if not isinstance(body, str):
                raise ValueError("SQS record body must be a JSON string")
            yield record.get("messageId"), json.loads(body), record
        return
    yield None, event, None


def normalize_topics(raw_topics: Any) -> list[str]:
    topics: set[str] = set()
    if isinstance(raw_topics, list):
        for raw_topic in raw_topics:
            if not isinstance(raw_topic, str):
                continue
            topic = raw_topic.strip().lower()
            if topic in {"global", "top_pages"} or (
                topic.startswith("wiki:")
                and topic.removeprefix("wiki:").strip()
            ):
                topics.add(topic)
    if not topics:
        topics.add("global")
    return sorted(
        topics,
        key=lambda topic: (
            topic != "global",
            topic != "top_pages",
            topic,
        ),
    )


def get_window_timestamp_bounds(
    signal: dict[str, Any],
    aggregation_window: str,
) -> tuple[int | None, int | None]:
    bounds_by_window = signal.get("event_timestamp_bounds_by_window")
    if isinstance(bounds_by_window, dict):
        bounds = bounds_by_window.get(strip_window_prefix(aggregation_window))
        if isinstance(bounds, dict):
            return (
                to_int(bounds.get("oldest_event_timestamp_ms"), 0) or None,
                to_int(bounds.get("latest_event_timestamp_ms"), 0) or None,
            )
    return (
        to_int(signal.get("oldest_event_timestamp_ms"), 0) or None,
        to_int(signal.get("latest_event_timestamp_ms"), 0) or None,
    )


def get_topic_window_timestamp_bounds(
    signal: dict[str, Any],
    aggregation_window: str,
    topic: str,
) -> tuple[int | None, int | None]:
    by_topic_by_window = signal.get(
        "event_timestamp_bounds_by_topic_by_window"
    )
    if isinstance(by_topic_by_window, dict):
        window_bounds = by_topic_by_window.get(
            strip_window_prefix(aggregation_window)
        )
        if isinstance(window_bounds, dict):
            topic_bounds = window_bounds.get(topic)
            if isinstance(topic_bounds, dict):
                return (
                    to_int(
                        topic_bounds.get("oldest_event_timestamp_ms"),
                        0,
                    )
                    or None,
                    to_int(
                        topic_bounds.get("latest_event_timestamp_ms"),
                        0,
                    )
                    or None,
                )

    return get_window_timestamp_bounds(signal, aggregation_window)


def canonical_signal_id(signal: dict[str, Any]) -> str:
    explicit_signal_id = signal.get("signal_id")
    if isinstance(explicit_signal_id, str) and explicit_signal_id.strip():
        return explicit_signal_id.strip()

    material = json.dumps(
        signal,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    digest = hashlib.sha256(material.encode("utf-8")).hexdigest()
    return f"SIGNAL#{digest}"


def signal_token(signal_id: str) -> str:
    return hashlib.sha256(signal_id.encode("utf-8")).hexdigest()[:16]


def build_snapshot_id(
    sequence: int,
    aggregation_window: str,
    source_signal_id: str,
) -> str:
    window_epoch_ms = iso_to_epoch_ms(
        strip_window_prefix(aggregation_window)
    )
    return (
        f"SNAPSHOT#{sequence}#WINDOW#{window_epoch_ms}"
        f"#SIGNAL#{signal_token(source_signal_id)}"
    )


def build_manifest_id(
    sequence: int,
    aggregation_window: str,
    source_signal_id: str,
) -> str:
    window_epoch_ms = iso_to_epoch_ms(
        strip_window_prefix(aggregation_window)
    )
    return (
        f"MANIFEST#{sequence}#WINDOW#{window_epoch_ms}"
        f"#SIGNAL#{signal_token(source_signal_id)}"
    )


def build_idempotency_key(source_signal_id: str) -> str:
    digest = hashlib.sha256(source_signal_id.encode("utf-8")).hexdigest()
    return f"IDEMPOTENCY#{digest}"


def acquire_idempotency_lease(
    *,
    source_signal_id: str,
    sequence: int,
    aws_request_id: str | None,
) -> tuple[bool, dict[str, Any] | None]:
    now_ms = int(time.time() * 1000)
    lease_until_ms = now_ms + (IDEMPOTENCY_LEASE_SECONDS * 1000)
    key = {
        "snapshot_id": build_idempotency_key(source_signal_id),
        "topic": "COORDINATOR",
    }
    owner = aws_request_id or "manual"
    item = {
        **key,
        "item_type": "IDEMPOTENCY",
        "schema_version": 3,
        "source_signal_id": source_signal_id,
        "sequence": sequence,
        "status": "IN_PROGRESS",
        "owner_request_id": owner,
        "lease_until_ms": lease_until_ms,
        "created_at": now_iso(),
        "updated_at": now_iso(),
        "ttl": int(time.time()) + SNAPSHOT_TTL_SECONDS,
    }

    try:
        snapshots_table.put_item(
            Item=item,
            ConditionExpression=(
                "attribute_not_exists(snapshot_id) "
                "AND attribute_not_exists(topic)"
            ),
        )
        return True, None
    except snapshots_table.meta.client.exceptions.ConditionalCheckFailedException:
        existing_response = snapshots_table.get_item(
            Key=key,
            ConsistentRead=True,
        )
        existing = existing_response.get("Item")
        if not existing:
            raise RuntimeError(
                "Idempotency item disappeared after conditional failure"
            )

        if existing.get("status") == "COMPLETED":
            return False, existing

        try:
            takeover_response = snapshots_table.update_item(
                Key=key,
                UpdateExpression=(
                    "SET #status = :in_progress, "
                    "owner_request_id = :owner, "
                    "lease_until_ms = :lease_until, "
                    "updated_at = :updated_at, "
                    "#ttl = :ttl"
                ),
                ConditionExpression=(
                    "#status = :failed OR lease_until_ms < :now_ms"
                ),
                ExpressionAttributeNames={
                    "#status": "status",
                    "#ttl": "ttl",
                },
                ExpressionAttributeValues={
                    ":in_progress": "IN_PROGRESS",
                    ":failed": "FAILED",
                    ":owner": owner,
                    ":lease_until": lease_until_ms,
                    ":updated_at": now_iso(),
                    ":ttl": int(time.time()) + SNAPSHOT_TTL_SECONDS,
                    ":now_ms": now_ms,
                },
                ReturnValues="ALL_NEW",
            )
            return True, takeover_response.get("Attributes")
        except snapshots_table.meta.client.exceptions.ConditionalCheckFailedException as error:
            raise RuntimeError(
                "The same broadcast signal is already being processed"
            ) from error


def complete_idempotency(
    *,
    source_signal_id: str,
    aws_request_id: str | None,
    manifests: list[str],
    snapshots_created: int,
    jobs_planned: int,
    jobs_published: int,
) -> None:
    owner = aws_request_id or "manual"
    snapshots_table.update_item(
        Key={
            "snapshot_id": build_idempotency_key(source_signal_id),
            "topic": "COORDINATOR",
        },
        UpdateExpression=(
            "SET #status = :completed, "
            "manifest_ids = :manifest_ids, "
            "snapshots_created = :snapshots_created, "
            "jobs_planned = :jobs_planned, "
            "jobs_published = :jobs_published, "
            "completed_at = :completed_at, "
            "updated_at = :updated_at REMOVE lease_until_ms"
        ),
        ConditionExpression=(
            "owner_request_id = :owner AND #status = :in_progress"
        ),
        ExpressionAttributeNames={"#status": "status"},
        ExpressionAttributeValues={
            ":completed": "COMPLETED",
            ":in_progress": "IN_PROGRESS",
            ":owner": owner,
            ":manifest_ids": manifests,
            ":snapshots_created": snapshots_created,
            ":jobs_planned": jobs_planned,
            ":jobs_published": jobs_published,
            ":completed_at": now_iso(),
            ":updated_at": now_iso(),
        },
    )


def fail_idempotency(
    *,
    source_signal_id: str,
    aws_request_id: str | None,
    error: Exception,
) -> None:
    owner = aws_request_id or "manual"
    try:
        snapshots_table.update_item(
            Key={
                "snapshot_id": build_idempotency_key(source_signal_id),
                "topic": "COORDINATOR",
            },
            UpdateExpression=(
                "SET #status = :failed, "
                "last_error_type = :error_type, "
                "last_error_message = :error_message, "
                "lease_until_ms = :expired, "
                "updated_at = :updated_at"
            ),
            ConditionExpression="owner_request_id = :owner",
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues={
                ":failed": "FAILED",
                ":owner": owner,
                ":error_type": type(error).__name__,
                ":error_message": str(error)[:1000],
                ":expired": 0,
                ":updated_at": now_iso(),
            },
        )
    except Exception as lease_error:
        log_json(
            "WARNING",
            "broadcast_coordinator_idempotency_failure_not_recorded",
            source_signal_id=source_signal_id,
            error_type=type(lease_error).__name__,
            error_message=str(lease_error),
        )


def read_topic_snapshot_data(
    topic: str,
    window_key: str,
    top_pages_cache: list[dict[str, Any]] | None,
) -> tuple[dict[str, Any], list[dict[str, Any]] | None, float]:
    started_at = time.perf_counter()

    if topic == "global":
        global_data, _ = read_global_snapshot(window_key)
        return (
            global_data,
            global_data.get("top_pages", []),
            round((time.perf_counter() - started_at) * 1000, 2),
        )

    if topic == "top_pages":
        pages = top_pages_cache
        if pages is None:
            pages, _ = read_top_pages_snapshot(window_key)
        return (
            {
                "current_minute_events_so_far": sum(
                    to_int(page.get("count"), 0) for page in pages
                ),
                "top_pages": pages,
            },
            pages,
            round((time.perf_counter() - started_at) * 1000, 2),
        )

    wiki = topic.removeprefix("wiki:")
    wiki_data, _ = read_wiki_snapshot(wiki, window_key)
    return (
        {
            "wiki": wiki,
            **wiki_data,
            "top_wikis": [],
        },
        top_pages_cache,
        round((time.perf_counter() - started_at) * 1000, 2),
    )


def persist_snapshot(
    *,
    snapshot_id: str,
    topic: str,
    sequence: int,
    aggregation_window: str,
    broadcast_window: str,
    data: dict[str, Any],
    source_signal_id: str,
    source_signal_created_at: str | None,
    latest_event_timestamp_ms: int | None,
    oldest_event_timestamp_ms: int | None,
) -> dict[str, Any]:
    item: dict[str, Any] = {
        "snapshot_id": snapshot_id,
        "topic": topic,
        "item_type": "SNAPSHOT",
        "schema_version": 3,
        "sequence": sequence,
        "aggregation_window": strip_window_prefix(aggregation_window),
        "aggregation_window_epoch_ms": iso_to_epoch_ms(
            strip_window_prefix(aggregation_window)
        ),
        "broadcast_window": broadcast_window,
        "source_signal_id": source_signal_id,
        "created_at": now_iso(),
        "source_signal_created_at": source_signal_created_at or "unknown",
        "payload": to_dynamodb_safe(data),
        "ttl": int(time.time()) + SNAPSHOT_TTL_SECONDS,
    }
    add_freshness_fields(
        item,
        latest_event_timestamp_ms,
        oldest_event_timestamp_ms,
    )
    snapshots_table.put_item(Item=item)
    return item


def persist_manifest(
    *,
    manifest_id: str,
    sequence: int,
    aggregation_window: str,
    broadcast_window: str,
    source_signal_id: str,
    snapshot_references: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    item = {
        "snapshot_id": manifest_id,
        "topic": "MANIFEST",
        "item_type": "MANIFEST",
        "schema_version": 3,
        "sequence": sequence,
        "aggregation_window": strip_window_prefix(aggregation_window),
        "aggregation_window_epoch_ms": iso_to_epoch_ms(
            strip_window_prefix(aggregation_window)
        ),
        "broadcast_window": broadcast_window,
        "source_signal_id": source_signal_id,
        "updated_topics": sorted(snapshot_references),
        "snapshots": to_dynamodb_safe(snapshot_references),
        "created_at": now_iso(),
        "ttl": int(time.time()) + SNAPSHOT_TTL_SECONDS,
    }
    snapshots_table.put_item(Item=item)
    return item


def update_latest_pointer(
    *,
    sequence: int,
    manifest_id: str,
    aggregation_window: str,
    source_signal_id: str,
) -> bool:
    aggregation_window_epoch_ms = iso_to_epoch_ms(
        strip_window_prefix(aggregation_window)
    )
    try:
        snapshots_table.update_item(
            Key={
                "snapshot_id": "LATEST",
                "topic": "MANIFEST",
            },
            UpdateExpression=(
                "SET item_type = :item_type, "
                "schema_version = :schema_version, "
                "#sequence = :sequence, "
                "manifest_id = :manifest_id, "
                "aggregation_window = :aggregation_window, "
                "aggregation_window_epoch_ms = :window_epoch_ms, "
                "source_signal_id = :source_signal_id, "
                "updated_at = :updated_at"
            ),
            ConditionExpression=(
                "attribute_not_exists(#sequence) "
                "OR #sequence < :sequence "
                "OR (#sequence = :sequence "
                "AND aggregation_window_epoch_ms <= :window_epoch_ms)"
            ),
            ExpressionAttributeNames={"#sequence": "sequence"},
            ExpressionAttributeValues={
                ":item_type": "POINTER",
                ":schema_version": 3,
                ":sequence": sequence,
                ":manifest_id": manifest_id,
                ":aggregation_window": strip_window_prefix(
                    aggregation_window
                ),
                ":window_epoch_ms": aggregation_window_epoch_ms,
                ":source_signal_id": source_signal_id,
                ":updated_at": now_iso(),
            },
        )
        return True
    except snapshots_table.meta.client.exceptions.ConditionalCheckFailedException:
        return False


def build_shard_jobs(
    *,
    manifest_id: str,
    sequence: int,
    aggregation_window: str,
    broadcast_window: str,
) -> list[dict[str, Any]]:
    created_at_ms = int(time.time() * 1000)
    return [
        {
            "schema_version": 3,
            "message_type": "broadcast.shard.job",
            "broadcast_id": manifest_id,
            "manifest_id": manifest_id,
            "sequence": sequence,
            "shard_id": shard_id,
            "connection_shard": f"SHARD#{shard_id:02d}",
            "aggregation_window": strip_window_prefix(
                aggregation_window
            ),
            "aggregation_window_epoch_ms": iso_to_epoch_ms(
                strip_window_prefix(aggregation_window)
            ),
            "broadcast_window": broadcast_window,
            "created_at_ms": created_at_ms,
            "created_at": now_iso(),
        }
        for shard_id in range(CONNECTION_SHARD_COUNT)
    ]


def build_message_deduplication_id(job: dict[str, Any]) -> str:
    material = (
        f"{job['manifest_id']}|{job['connection_shard']}"
    )
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def send_jobs(jobs: list[dict[str, Any]]) -> int:
    sent_count = 0
    for batch_start in range(0, len(jobs), 10):
        entries: list[dict[str, Any]] = []
        for index, job in enumerate(
            jobs[batch_start : batch_start + 10],
            batch_start,
        ):
            entry: dict[str, Any] = {
                "Id": f"job-{index:03d}",
                "MessageBody": json.dumps(
                    job,
                    separators=(",", ":"),
                ),
                "MessageGroupId": job["connection_shard"],
                "MessageDeduplicationId": (
                    build_message_deduplication_id(job)
                ),
            }
            message_attributes = build_trace_message_attributes()
            if message_attributes:
                entry["MessageAttributes"] = message_attributes
            entries.append(entry)

        response = sqs_client.send_message_batch(
            QueueUrl=BROADCAST_JOBS_QUEUE_URL,
            Entries=entries,
        )
        failed = response.get("Failed", [])
        if failed:
            raise RuntimeError(
                "Some shard-centric broadcast jobs failed to publish: "
                + json.dumps(failed, default=str)
            )
        sent_count += len(response.get("Successful", []))
    return sent_count


def process_signal(
    signal: dict[str, Any],
    aws_request_id: str | None,
) -> dict[str, Any]:
    message_type = str(signal.get("message_type", ""))
    if not message_type.startswith("aggregates.updated"):
        raise ValueError(f"Unsupported message_type: {message_type}")

    broadcast_window = str(signal.get("broadcast_window") or now_iso())
    sequence = to_int(signal.get("sequence"), 0) or iso_to_epoch_ms(
        broadcast_window
    )
    source_signal_id = canonical_signal_id(signal)
    aggregation_windows = signal.get("aggregation_windows") or [
        broadcast_window
    ]
    if not isinstance(aggregation_windows, list):
        raise ValueError("aggregation_windows must be a list")
    topics = normalize_topics(signal.get("updated_topics"))

    acquired, existing = acquire_idempotency_lease(
        source_signal_id=source_signal_id,
        sequence=sequence,
        aws_request_id=aws_request_id,
    )
    if not acquired:
        broadcast_coordinator_duplicates_total.add(
            1,
            {"environment": ENVIRONMENT},
        )
        log_json(
            "INFO",
            "broadcast_coordinator_duplicate_signal_skipped",
            aws_request_id=aws_request_id,
            source_signal_id=source_signal_id,
            sequence=sequence,
            existing_status=existing.get("status") if existing else None,
            manifest_ids=existing.get("manifest_ids") if existing else None,
        )
        return {
            "status": "duplicate",
            "sequence": sequence,
            "source_signal_id": source_signal_id,
            "topics": topics,
            "aggregation_windows": [
                strip_window_prefix(str(value))
                for value in aggregation_windows
            ],
            "snapshots_created": 0,
            "manifests_created": 0,
            "jobs_planned": 0,
            "jobs_published": 0,
        }

    created_snapshots = 0
    manifest_ids: list[str] = []
    latest_manifest_candidate: tuple[int, str, str] | None = None

    try:
        for raw_window in aggregation_windows:
            aggregation_window = strip_window_prefix(str(raw_window))
            window_key = normalize_window_key(aggregation_window)
            snapshot_id = build_snapshot_id(
                sequence,
                aggregation_window,
                source_signal_id,
            )
            snapshot_references: dict[str, dict[str, Any]] = {}
            top_pages_cache: list[dict[str, Any]] | None = None

            for topic in topics:
                with tracer.start_as_current_span(
                    "broadcast_coordinator.read_aggregates"
                ) as read_span:
                    read_span.set_attribute("broadcast.topic", topic)
                    read_span.set_attribute(
                        "broadcast.aggregation_window",
                        aggregation_window,
                    )
                    data, top_pages_cache, read_duration_ms = (
                        read_topic_snapshot_data(
                            topic,
                            window_key,
                            top_pages_cache,
                        )
                    )
                    read_span.set_attribute(
                        "broadcast.aggregate_read_duration_ms",
                        read_duration_ms,
                    )
                    read_span.set_status(Status(StatusCode.OK))
                    broadcast_coordinator_aggregate_read_duration_ms.record(
                        read_duration_ms,
                        {
                            "environment": ENVIRONMENT,
                            "topic_type": topic.split(":", 1)[0],
                        },
                    )

                oldest_ms, latest_ms = get_topic_window_timestamp_bounds(
                    signal,
                    aggregation_window,
                    topic,
                )
                build_started_at = time.perf_counter()
                persist_snapshot(
                    snapshot_id=snapshot_id,
                    topic=topic,
                    sequence=sequence,
                    aggregation_window=aggregation_window,
                    broadcast_window=broadcast_window,
                    data=data,
                    source_signal_id=source_signal_id,
                    source_signal_created_at=signal.get("created_at"),
                    latest_event_timestamp_ms=latest_ms,
                    oldest_event_timestamp_ms=oldest_ms,
                )
                build_duration_ms = round(
                    (time.perf_counter() - build_started_at) * 1000,
                    2,
                )
                broadcast_coordinator_payload_build_duration_ms.record(
                    build_duration_ms,
                    {
                        "environment": ENVIRONMENT,
                        "topic_type": topic.split(":", 1)[0],
                    },
                )
                broadcast_snapshots_created_total.add(
                    1,
                    {
                        "environment": ENVIRONMENT,
                        "topic_type": topic.split(":", 1)[0],
                    },
                )
                created_snapshots += 1
                snapshot_references[topic] = {
                    "snapshot_id": snapshot_id,
                    "topic": topic,
                    "oldest_event_timestamp_ms": oldest_ms,
                    "latest_event_timestamp_ms": latest_ms,
                }

                log_json(
                    "INFO",
                    "broadcast_snapshot_created",
                    aws_request_id=aws_request_id,
                    source_signal_id=source_signal_id,
                    snapshot_id=snapshot_id,
                    sequence=sequence,
                    topic=topic,
                    aggregation_window=aggregation_window,
                    aggregate_read_duration_ms=read_duration_ms,
                    payload_build_duration_ms=build_duration_ms,
                )

            manifest_id = build_manifest_id(
                sequence,
                aggregation_window,
                source_signal_id,
            )
            persist_manifest(
                manifest_id=manifest_id,
                sequence=sequence,
                aggregation_window=aggregation_window,
                broadcast_window=broadcast_window,
                source_signal_id=source_signal_id,
                snapshot_references=snapshot_references,
            )
            manifest_ids.append(manifest_id)
            broadcast_manifests_created_total.add(
                1,
                {"environment": ENVIRONMENT},
            )

            window_epoch_ms = iso_to_epoch_ms(aggregation_window)
            candidate = (
                window_epoch_ms,
                manifest_id,
                aggregation_window,
            )
            if (
                latest_manifest_candidate is None
                or candidate[0] > latest_manifest_candidate[0]
            ):
                latest_manifest_candidate = candidate

            log_json(
                "INFO",
                "broadcast_manifest_created",
                aws_request_id=aws_request_id,
                source_signal_id=source_signal_id,
                manifest_id=manifest_id,
                sequence=sequence,
                aggregation_window=aggregation_window,
                topic_count=len(snapshot_references),
                realtime_delivery_candidate=True,
                connection_shard_count=CONNECTION_SHARD_COUNT,
            )

        if latest_manifest_candidate is None:
            raise RuntimeError("No manifest was created")

        _, latest_manifest_id, latest_aggregation_window = (
            latest_manifest_candidate
        )
        pointer_updated = update_latest_pointer(
            sequence=sequence,
            manifest_id=latest_manifest_id,
            aggregation_window=latest_aggregation_window,
            source_signal_id=source_signal_id,
        )

        # Real-time delivery is latest-state-only. Even when one signal carries
        # several aggregation windows, publish exactly one job per connection
        # shard for the newest manifest, never windows × shards jobs. If a newer
        # pointer already won the conditional update, this signal publishes no
        # stale work at all.
        shard_jobs = (
            build_shard_jobs(
                manifest_id=latest_manifest_id,
                sequence=sequence,
                aggregation_window=latest_aggregation_window,
                broadcast_window=broadcast_window,
            )
            if pointer_updated
            else []
        )
        jobs_planned = len(shard_jobs)
        jobs_published = 0
        broadcast_jobs_planned_total.add(
            jobs_planned,
            {"environment": ENVIRONMENT},
        )

        if PUBLISH_SHARD_JOBS:
            jobs_published = send_jobs(shard_jobs)
            broadcast_jobs_created_total.add(
                jobs_published,
                {"environment": ENVIRONMENT},
            )
        else:
            log_json(
                "INFO",
                "broadcast_shard_jobs_publish_skipped",
                aws_request_id=aws_request_id,
                sequence=sequence,
                jobs_planned=jobs_planned,
                reason="PUBLISH_SHARD_JOBS=false",
            )

        complete_idempotency(
            source_signal_id=source_signal_id,
            aws_request_id=aws_request_id,
            manifests=manifest_ids,
            snapshots_created=created_snapshots,
            jobs_planned=jobs_planned,
            jobs_published=jobs_published,
        )

        coordinator_signals_total.add(
            1,
            {"environment": ENVIRONMENT, "result": "success"},
        )
        return {
            "status": "success",
            "sequence": sequence,
            "source_signal_id": source_signal_id,
            "topics": topics,
            "aggregation_windows": [
                strip_window_prefix(str(value))
                for value in aggregation_windows
            ],
            "snapshots_created": created_snapshots,
            "manifests_created": len(manifest_ids),
            "manifest_ids": manifest_ids,
            "latest_pointer_updated": pointer_updated,
            "jobs_planned": jobs_planned,
            "jobs_published": jobs_published,
            "publish_shard_jobs": PUBLISH_SHARD_JOBS,
        }

    except Exception as error:
        fail_idempotency(
            source_signal_id=source_signal_id,
            aws_request_id=aws_request_id,
            error=error,
        )
        raise


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    started_at = time.perf_counter()
    aws_request_id = getattr(context, "aws_request_id", None)
    batch_item_failures: list[dict[str, str]] = []
    direct_results: list[dict[str, Any]] = []

    try:
        for message_id, signal, sqs_record in iter_input_records(event):
            parent_context = None
            if sqs_record is not None:
                parent_context = propagate.extract(
                    get_sqs_trace_carrier(sqs_record)
                )

            try:
                with tracer.start_as_current_span(
                    "broadcast_coordinator.process_signal",
                    context=parent_context,
                ) as span:
                    result = process_signal(signal, aws_request_id)
                    span.set_attribute(
                        "faas.trigger",
                        "sqs" if sqs_record else "manual",
                    )
                    span.set_attribute(
                        "broadcast.sequence",
                        result["sequence"],
                    )
                    span.set_attribute(
                        "broadcast.topic_count",
                        len(result["topics"]),
                    )
                    span.set_attribute(
                        "broadcast.snapshots_created",
                        result["snapshots_created"],
                    )
                    span.set_attribute(
                        "broadcast.manifests_created",
                        result["manifests_created"],
                    )
                    span.set_attribute(
                        "broadcast.jobs_planned",
                        result["jobs_planned"],
                    )
                    span.set_attribute(
                        "broadcast.jobs_published",
                        result["jobs_published"],
                    )
                    span.set_status(Status(StatusCode.OK))
                    direct_results.append(result)

            except Exception as error:
                coordinator_signals_failed_total.add(
                    1,
                    {
                        "environment": ENVIRONMENT,
                        "error_type": type(error).__name__,
                    },
                )
                log_json(
                    "ERROR",
                    "broadcast_coordinator_record_failed",
                    aws_request_id=aws_request_id,
                    message_id=message_id,
                    error_type=type(error).__name__,
                    error_message=str(error),
                )
                if message_id:
                    batch_item_failures.append(
                        {"itemIdentifier": message_id}
                    )
                else:
                    raise

        duration_ms = round(
            (time.perf_counter() - started_at) * 1000,
            2,
        )
        broadcast_coordinator_duration_ms.record(
            duration_ms,
            {"environment": ENVIRONMENT},
        )
        log_json(
            "INFO",
            "broadcast_coordinator_completed",
            aws_request_id=aws_request_id,
            failed_records=len(batch_item_failures),
            duration_ms=duration_ms,
            publish_shard_jobs=PUBLISH_SHARD_JOBS,
        )

        if event.get("Records") is not None:
            return {"batchItemFailures": batch_item_failures}
        return {"status": "success", "results": direct_results}

    finally:
        if ENABLE_OTEL_FLUSH:
            flush_result = flush_otel()
            log_json(
                "INFO",
                "broadcast_coordinator_otel_flush_completed",
                aws_request_id=aws_request_id,
                flush_result=flush_result,
            )
