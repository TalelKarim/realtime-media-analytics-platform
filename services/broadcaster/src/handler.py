import json
import logging
import os
import random
import time
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from contextlib import nullcontext
from contextvars import copy_context
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional, Set, Tuple

import boto3
from boto3.dynamodb.types import TypeDeserializer
from botocore.config import Config
from botocore.exceptions import ClientError

from opentelemetry import propagate, trace as otel_trace
from opentelemetry.context import Context
from opentelemetry.propagators.textmap import Getter
from opentelemetry.trace import Status, StatusCode

try:
    from opentelemetry.instrumentation.utils import suppress_instrumentation
except ImportError:
    # Compatibility fallback for older opentelemetry-instrumentation packages.
    def suppress_instrumentation():
        return nullcontext()

from .observability import (
    active_connections_scanned,
    aggregate_reads_duration_ms,
    broadcast_completed_total,
    broadcast_duration_ms,
    broadcast_failed_total,
    connections_scan_duration_ms,
    event_to_dashboard_latency_ms,
    fanout_batch_size,
    fanout_duration_ms,
    flush_otel,
    gone_cleanup_duration_ms,
    oldest_event_to_dashboard_latency_ms,
    payload_build_duration_ms,
    tracer,
    websocket_connection_gone_total,
    websocket_messages_sent_total,
    websocket_post_duration_ms,
    websocket_post_failure_total,
    websocket_post_success_total,
)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
ENVIRONMENT = os.getenv("ENVIRONMENT", "dev")

logger = logging.getLogger()
logger.setLevel(LOG_LEVEL)


def _env_int(
    name: str,
    default: int,
    minimum: int = 1,
    maximum: Optional[int] = None,
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
        logger.warning(
            "environment_variable_below_minimum: %s=%r minimum=%s",
            name,
            raw_value,
            minimum,
        )
        return default

    if maximum is not None and value > maximum:
        logger.warning(
            "environment_variable_above_maximum: %s=%r maximum=%s",
            name,
            raw_value,
            maximum,
        )
        return maximum

    return value


def _env_float(
    name: str,
    default: float,
    minimum: float = 0.1,
) -> float:
    raw_value = os.getenv(name)

    if raw_value is None:
        return default

    try:
        value = float(raw_value)
    except ValueError:
        logger.warning("invalid_float_environment_variable: %s=%r", name, raw_value)
        return default

    if value < minimum:
        logger.warning(
            "environment_variable_below_minimum: %s=%r minimum=%s",
            name,
            raw_value,
            minimum,
        )
        return default

    return value


ENABLE_OTEL_FLUSH = os.getenv("ENABLE_OTEL_FLUSH", "true").lower() == "true"

# Per-PostToConnection botocore spans are expensive at high fan-out. The
# high-level broadcaster.fanout span plus metrics remain enabled. Set this to
# true temporarily when detailed per-connection trace debugging is required.
TRACE_POST_TO_CONNECTION_CALLS = (
    os.getenv("TRACE_POST_TO_CONNECTION_CALLS", "false").lower() == "true"
)

AGGREGATES_TABLE_NAME = os.environ["AGGREGATES_TABLE_NAME"]
CONNECTIONS_TABLE_NAME = os.environ["CONNECTIONS_TABLE_NAME"]
WEBSOCKET_ENDPOINT_URL = os.environ["WEBSOCKET_ENDPOINT_URL"]
# Example:
# https://abc123.execute-api.eu-west-1.amazonaws.com/dev

GLOBAL_ACTIVITY_SHARD_COUNT = _env_int("GLOBAL_ACTIVITY_SHARD_COUNT", 10)
TOP_METRIC_SHARD_COUNT = _env_int("TOP_METRIC_SHARD_COUNT", 10)

TOP_WIKIS_LIMIT = _env_int("TOP_WIKIS_LIMIT", 10)
TOP_PAGES_LIMIT = _env_int("TOP_PAGES_LIMIT", 10)

# One bounded pool per warm Lambda execution environment.
# 40 is intentionally conservative enough for API Gateway while removing the
# sequential fan-out bottleneck for hundreds of connections.
MAX_POST_WORKERS = _env_int(
    "MAX_POST_WORKERS",
    40,
    minimum=1,
    maximum=200,
)
DYNAMODB_READ_WORKERS = _env_int(
    "DYNAMODB_READ_WORKERS",
    24,
    minimum=1,
    maximum=64,
)

APIGW_MAX_POOL_CONNECTIONS = _env_int(
    "APIGW_MAX_POOL_CONNECTIONS",
    max(MAX_POST_WORKERS + 8, 48),
    minimum=MAX_POST_WORKERS,
    maximum=256,
)
DYNAMODB_MAX_POOL_CONNECTIONS = _env_int(
    "DYNAMODB_MAX_POOL_CONNECTIONS",
    max(DYNAMODB_READ_WORKERS + 8, 32),
    minimum=DYNAMODB_READ_WORKERS,
    maximum=128,
)

APIGW_CONNECT_TIMEOUT_SECONDS = _env_float(
    "APIGW_CONNECT_TIMEOUT_SECONDS",
    2.0,
)
APIGW_READ_TIMEOUT_SECONDS = _env_float(
    "APIGW_READ_TIMEOUT_SECONDS",
    5.0,
)
APIGW_RETRY_MAX_ATTEMPTS = _env_int(
    "APIGW_RETRY_MAX_ATTEMPTS",
    2,
    minimum=1,
    maximum=5,
)

DYNAMODB_CONNECT_TIMEOUT_SECONDS = _env_float(
    "DYNAMODB_CONNECT_TIMEOUT_SECONDS",
    2.0,
)
DYNAMODB_READ_TIMEOUT_SECONDS = _env_float(
    "DYNAMODB_READ_TIMEOUT_SECONDS",
    5.0,
)
DYNAMODB_RETRY_MAX_ATTEMPTS = _env_int(
    "DYNAMODB_RETRY_MAX_ATTEMPTS",
    4,
    minimum=1,
    maximum=8,
)
DYNAMODB_BATCH_GET_MAX_RETRIES = _env_int(
    "DYNAMODB_BATCH_GET_MAX_RETRIES",
    5,
    minimum=1,
    maximum=10,
)

CHANGE_TYPES = [
    value.strip()
    for value in os.getenv("CHANGE_TYPES", "edit,new,categorize,log,external").split(",")
    if value.strip()
]

NAMESPACES = [
    value.strip()
    for value in os.getenv("NAMESPACES", "-1,0,1,2,4,6,10,14").split(",")
    if value.strip()
]

ENABLE_TOP_PAGES_TOPIC = os.getenv("ENABLE_TOP_PAGES_TOPIC", "true").lower() == "true"


# ---------------------------------------------------------------------------
# AWS clients and reusable thread pools
# ---------------------------------------------------------------------------

dynamodb_resource = boto3.resource("dynamodb")
connections_table = dynamodb_resource.Table(CONNECTIONS_TABLE_NAME)

# Low-level clients are used inside worker threads. Boto3 clients can be shared
# between threads; DynamoDB Resource objects remain on the main thread.
dynamodb_client = boto3.client(
    "dynamodb",
    config=Config(
        max_pool_connections=DYNAMODB_MAX_POOL_CONNECTIONS,
        connect_timeout=DYNAMODB_CONNECT_TIMEOUT_SECONDS,
        read_timeout=DYNAMODB_READ_TIMEOUT_SECONDS,
        retries={
            "mode": "standard",
            "max_attempts": DYNAMODB_RETRY_MAX_ATTEMPTS,
        },
    ),
)

apigw_management = boto3.client(
    "apigatewaymanagementapi",
    endpoint_url=WEBSOCKET_ENDPOINT_URL,
    config=Config(
        max_pool_connections=APIGW_MAX_POOL_CONNECTIONS,
        connect_timeout=APIGW_CONNECT_TIMEOUT_SECONDS,
        read_timeout=APIGW_READ_TIMEOUT_SECONDS,
        retries={
            "mode": "standard",
            "max_attempts": APIGW_RETRY_MAX_ATTEMPTS,
        },
    ),
)

POST_EXECUTOR = ThreadPoolExecutor(
    max_workers=MAX_POST_WORKERS,
    thread_name_prefix="ws-post",
)
DYNAMODB_READ_EXECUTOR = ThreadPoolExecutor(
    max_workers=DYNAMODB_READ_WORKERS,
    thread_name_prefix="ddb-read",
)

DYNAMODB_DESERIALIZER = TypeDeserializer()

# ---------------------------------------------------------------------------
# Generic helpers
# ---------------------------------------------------------------------------


class SQSMessageAttributesGetter(Getter[Dict[str, Any]]):
    """
    OpenTelemetry textmap getter for AWS Lambda SQS event records.

    The realtime-processor injects W3C trace context into SQS MessageAttributes:
      - traceparent
      - tracestate
      - baggage

    Lambda receives those attributes with the shape:
      record["messageAttributes"]["traceparent"]["stringValue"]

    This getter lets OpenTelemetry extract that context so the broadcaster spans
    become children of the realtime-processor trace.
    """

    def get(self, carrier: Dict[str, Any], key: str) -> List[str]:
        if not carrier:
            return []

        message_attributes = carrier.get("messageAttributes") or {}
        if not isinstance(message_attributes, dict):
            return []

        attribute = message_attributes.get(key)
        if not isinstance(attribute, dict):
            return []

        value = (
            attribute.get("stringValue")
            or attribute.get("StringValue")
            or attribute.get("value")
        )

        if not value:
            return []

        return [str(value)]

    def keys(self, carrier: Dict[str, Any]) -> List[str]:
        if not carrier:
            return []

        message_attributes = carrier.get("messageAttributes") or {}
        if not isinstance(message_attributes, dict):
            return []

        return list(message_attributes.keys())


SQS_MESSAGE_ATTRIBUTES_GETTER = SQSMessageAttributesGetter()


def get_sqs_message_attribute(record: Dict[str, Any], name: str) -> Optional[str]:
    message_attributes = record.get("messageAttributes") or {}
    if not isinstance(message_attributes, dict):
        return None

    attribute = message_attributes.get(name)
    if not isinstance(attribute, dict):
        return None

    value = (
        attribute.get("stringValue")
        or attribute.get("StringValue")
        or attribute.get("value")
    )

    return str(value) if value else None


def has_trace_context(record: Dict[str, Any]) -> bool:
    return bool(get_sqs_message_attribute(record, "traceparent"))


def extract_trace_context_from_sqs_record(record: Dict[str, Any]) -> Optional[Context]:
    """
    Extract W3C trace context from SQS MessageAttributes.

    Returns None when the record has no traceparent or extraction fails.
    This keeps backward compatibility with old messages created before trace
    propagation was added.
    """
    if not has_trace_context(record):
        return None

    try:
        return propagate.extract(
            carrier=record,
            getter=SQS_MESSAGE_ATTRIBUTES_GETTER,
        )
    except Exception as error:
        logger.warning("sqs_trace_context_extract_failed: %s", error)
        return None


def first_trace_context(records: List[Dict[str, Any]]) -> Tuple[Optional[Context], bool]:
    """
    The SQS event source mapping is configured with batch_size=1 in this project.
    For that normal case, the Lambda root span can safely use the first record's
    trace context as parent.

    If no propagated trace context exists, the broadcaster starts a new trace.
    """
    if not records:
        return None, False

    parent_context = extract_trace_context_from_sqs_record(records[0])
    return parent_context, parent_context is not None

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def parse_iso_datetime(value: str) -> Optional[datetime]:
    if not value:
        return None

    try:
        normalized = value.replace("Z", "+00:00")
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def normalize_window_key(aggregation_window: str) -> str:
    """
    SQS message sends:
      aggregation_windows = ["2026-06-24T13:30:00Z"]

    DynamoDB stores:
      window_key = "WINDOW#2026-06-24T13:30:00Z"
    """
    if aggregation_window.startswith("WINDOW#"):
        return aggregation_window

    return f"WINDOW#{aggregation_window}"


def strip_window_prefix(window_key_or_iso: str) -> str:
    if window_key_or_iso.startswith("WINDOW#"):
        return window_key_or_iso.removeprefix("WINDOW#")

    return window_key_or_iso


def is_partial_window(aggregation_window: str) -> bool:
    """
    True when the 1-minute aggregation window is still open.
    Useful for the dashboard to understand that current_minute_events_so_far
    is still increasing.
    """
    window_iso = strip_window_prefix(aggregation_window)
    window_start = parse_iso_datetime(window_iso)

    if window_start is None:
        return True

    return datetime.now(timezone.utc) < window_start + timedelta(minutes=1)


def to_int(value: Any, default: int = 0) -> int:
    if value is None:
        return default

    if isinstance(value, Decimal):
        return int(value)

    if isinstance(value, int):
        return value

    if isinstance(value, float):
        return int(value)

    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return default

    return default


def epoch_ms_now() -> int:
    return int(time.time() * 1000)


def to_epoch_ms(value: Any) -> Optional[int]:
    """
    Convert an epoch-milliseconds-like value to int.

    SQS JSON can contain int, float, string or Decimal-like values depending on
    the producer and JSON parsing path. Invalid or non-positive values are ignored.
    """
    if value is None:
        return None

    if isinstance(value, Decimal):
        timestamp_ms = int(value)
    elif isinstance(value, bool):
        return None
    elif isinstance(value, int):
        timestamp_ms = value
    elif isinstance(value, float):
        timestamp_ms = int(value)
    elif isinstance(value, str):
        try:
            timestamp_ms = int(float(value.strip()))
        except ValueError:
            return None
    else:
        return None

    if timestamp_ms <= 0:
        return None

    return timestamp_ms


def topic_type_from_topic(topic: str) -> str:
    """
    Keep the freshness metric low-cardinality.

    The existing WebSocket counters already use the raw topic label. For the
    freshness histogram we intentionally avoid labels like wiki:frwiki because
    the number of wiki topics can grow.
    """
    if topic == "global":
        return "global"

    if topic == "top_pages":
        return "top_pages"

    if topic.startswith("wiki:"):
        return "wiki"

    return "unknown"


def get_event_timestamp_bounds_for_window(
    message: Dict[str, Any],
    aggregation_window: str,
) -> Dict[str, Optional[int]]:
    """
    Return oldest/latest source event timestamps for one aggregation window.

    Preferred contract from realtime-processor:
      event_timestamp_bounds_by_window[aggregation_window]

    Backward-compatible fallback:
      oldest_event_timestamp_ms / latest_event_timestamp_ms at message top-level
    """
    bounds_by_window = message.get("event_timestamp_bounds_by_window")

    if isinstance(bounds_by_window, dict):
        candidate_keys = [
            aggregation_window,
            strip_window_prefix(aggregation_window),
            normalize_window_key(aggregation_window),
        ]

        for key in candidate_keys:
            bounds = bounds_by_window.get(key)
            if isinstance(bounds, dict):
                return {
                    "oldest_event_timestamp_ms": to_epoch_ms(
                        bounds.get("oldest_event_timestamp_ms")
                    ),
                    "latest_event_timestamp_ms": to_epoch_ms(
                        bounds.get("latest_event_timestamp_ms")
                    ),
                }

    return {
        "oldest_event_timestamp_ms": to_epoch_ms(
            message.get("oldest_event_timestamp_ms")
        ),
        "latest_event_timestamp_ms": to_epoch_ms(
            message.get("latest_event_timestamp_ms")
        ),
    }


def json_safe(value: Any) -> Any:
    """
    DynamoDB returns Decimal objects.
    json.dumps cannot serialize Decimal directly.
    """
    if isinstance(value, Decimal):
        if value % 1 == 0:
            return int(value)
        return float(value)

    if isinstance(value, list):
        return [json_safe(item) for item in value]

    if isinstance(value, dict):
        return {key: json_safe(item) for key, item in value.items()}

    return value


def log_json(level: str, message: str, **fields: Any) -> None:
    """
    Emit one structured JSON log line.

    The log is intentionally written as JSON in the message body so CloudWatch
    Logs Insights, Grafana Loki, and OpenTelemetry-aware pipelines can extract
    fields consistently.

    If an OpenTelemetry span is active, trace_id and span_id are added to the log
    line. Logs still go through CloudWatch -> Promtail -> Loki; they are simply
    enriched with trace context.
    """
    payload = {
        "message": message,
        "service": "broadcaster",
        "component": "websocket-broadcast",
        "environment": ENVIRONMENT,
        **fields,
    }

    span_context = otel_trace.get_current_span().get_span_context()
    if span_context and span_context.is_valid:
        payload["trace_id"] = format(span_context.trace_id, "032x")
        payload["span_id"] = format(span_context.span_id, "016x")

    log_line = json.dumps(json_safe(payload), default=str)

    if level.upper() == "ERROR":
        logger.error(log_line)
    elif level.upper() == "WARNING":
        logger.warning(log_line)
    else:
        logger.info(log_line)


def log_exception(message: str, **fields: Any) -> None:
    """
    Emit a structured JSON error log and keep the Python stack trace.

    If an OpenTelemetry span is active, trace_id and span_id are added to the log
    line so Grafana can correlate Loki logs with Tempo traces.
    """
    payload = {
        "message": message,
        "service": "broadcaster",
        "component": "websocket-broadcast",
        "environment": ENVIRONMENT,
        **fields,
    }

    span_context = otel_trace.get_current_span().get_span_context()
    if span_context and span_context.is_valid:
        payload["trace_id"] = format(span_context.trace_id, "032x")
        payload["span_id"] = format(span_context.span_id, "016x")

    logger.exception(json.dumps(json_safe(payload), default=str))


def get_event_count(item: Optional[Dict[str, Any]]) -> int:
    if not item:
        return 0

    return to_int(item.get("event_count"), 0)


# ---------------------------------------------------------------------------
# DynamoDB read helpers — realtime_aggregates
# ---------------------------------------------------------------------------


def _submit_with_context(
    executor: ThreadPoolExecutor,
    function: Any,
    *args: Any,
    **kwargs: Any,
) -> Future:
    """
    Submit one task while copying the current OpenTelemetry/contextvars context.

    Python does not automatically propagate contextvars into worker threads.
    Giving every task its own copied context keeps botocore spans attached to
    the current broadcaster trace.
    """
    context = copy_context()

    def runner() -> Any:
        return function(*args, **kwargs)

    return executor.submit(context.run, runner)


def _deserialize_dynamodb_item(raw_item: Dict[str, Any]) -> Dict[str, Any]:
    return {
        key: DYNAMODB_DESERIALIZER.deserialize(value)
        for key, value in raw_item.items()
    }


def _chunks(values: List[str], size: int) -> List[List[str]]:
    return [
        values[index : index + size]
        for index in range(0, len(values), size)
    ]


def _batch_get_metric_counts(
    metric_keys: List[str],
    window_key: str,
) -> Dict[str, int]:
    """
    Retrieve exact-key aggregate counters with BatchGetItem.

    A batch can hold up to 100 keys. UnprocessedKeys are retried with bounded
    exponential backoff and jitter.
    """
    unique_metric_keys = list(dict.fromkeys(metric_keys))
    counts: Dict[str, int] = {}

    for metric_key_chunk in _chunks(unique_metric_keys, 100):
        request_items: Dict[str, Any] = {
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
            response = dynamodb_client.batch_get_item(
                RequestItems=request_items,
            )

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
                remaining_key_count = len(table_unprocessed.get("Keys", []))
                raise RuntimeError(
                    "DynamoDB BatchGetItem still has "
                    f"{remaining_key_count} unprocessed keys after "
                    f"{DYNAMODB_BATCH_GET_MAX_RETRIES} retries"
                )

            sleep_seconds = min(
                0.05 * (2 ** (attempt - 1)) + random.uniform(0.0, 0.025),
                1.0,
            )
            time.sleep(sleep_seconds)
            request_items = {
                AGGREGATES_TABLE_NAME: table_unprocessed,
            }

    return counts


def _query_metric_items(
    metric_key: str,
    window_key_prefix: str,
    projection_expression: str,
    projection_attribute_names: Dict[str, str],
) -> List[Dict[str, Any]]:
    """
    Query one aggregate partition and follow LastEvaluatedKey pagination.

    The low-level DynamoDB client is shared safely between read worker threads.
    """
    items: List[Dict[str, Any]] = []
    exclusive_start_key: Optional[Dict[str, Any]] = None

    expression_attribute_names = {
        "#mk": "metric_key",
        "#wk": "window_key",
        **projection_attribute_names,
    }

    while True:
        kwargs: Dict[str, Any] = {
            "TableName": AGGREGATES_TABLE_NAME,
            "KeyConditionExpression": (
                "#mk = :metric_key AND begins_with(#wk, :window_prefix)"
            ),
            "ExpressionAttributeNames": expression_attribute_names,
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
            break

    return items


def _query_top_wikis_shard(
    shard_id: int,
    window_key_prefix: str,
) -> List[Dict[str, Any]]:
    return _query_metric_items(
        metric_key=f"METRIC#TOP_WIKIS#SHARD#{shard_id}",
        window_key_prefix=window_key_prefix,
        projection_expression="#wk, #wiki, #ec",
        projection_attribute_names={
            "#wiki": "wiki",
            "#ec": "event_count",
        },
    )


def _query_top_pages_shard(
    shard_id: int,
    window_key_prefix: str,
) -> List[Dict[str, Any]]:
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


def _collect_top_wikis(
    futures: List[Future],
) -> List[Dict[str, Any]]:
    candidates: Dict[str, int] = {}

    for future in as_completed(futures):
        for item in future.result():
            wiki = item.get("wiki")

            if not wiki:
                window_item_key = str(item.get("window_key", ""))

                if "#WIKI#" in window_item_key:
                    wiki = window_item_key.split("#WIKI#", 1)[1]

            if not wiki:
                continue

            wiki_text = str(wiki)
            candidates[wiki_text] = (
                candidates.get(wiki_text, 0) + get_event_count(item)
            )

    sorted_wikis = sorted(
        candidates.items(),
        key=lambda candidate: candidate[1],
        reverse=True,
    )

    return [
        {
            "wiki": wiki,
            "count": count,
        }
        for wiki, count in sorted_wikis[:TOP_WIKIS_LIMIT]
    ]


def _collect_top_pages(
    futures: List[Future],
) -> List[Dict[str, Any]]:
    pages: List[Dict[str, Any]] = []

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
        if page.get("wiki")
        and page.get("title")
        and page.get("count", 0) > 0
    ]
    pages.sort(
        key=lambda page: page["count"],
        reverse=True,
    )

    return pages[:TOP_PAGES_LIMIT]


def read_global_snapshot(window_key: str) -> Tuple[Dict[str, Any], float]:
    """
    Read everything needed by the global payload.

    Exact-key counters use one BatchGetItem request. TOP_WIKIS and TOP_PAGES
    shard queries run concurrently in the DynamoDB read pool.
    """
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
        f"METRIC#CHANGE_TYPE#TYPE#{change_type}"
        for change_type in CHANGE_TYPES
    ]
    namespace_keys = [
        f"METRIC#NAMESPACE#NS#{namespace}"
        for namespace in NAMESPACES
    ]
    exact_metric_keys = (
        global_activity_keys
        + bot_keys
        + change_type_keys
        + namespace_keys
    )

    exact_counts_future = _submit_with_context(
        DYNAMODB_READ_EXECUTOR,
        _batch_get_metric_counts,
        exact_metric_keys,
        window_key,
    )

    top_wikis_prefix = f"{window_key}#WIKI#"
    top_wikis_futures = [
        _submit_with_context(
            DYNAMODB_READ_EXECUTOR,
            _query_top_wikis_shard,
            shard_id,
            top_wikis_prefix,
        )
        for shard_id in range(TOP_METRIC_SHARD_COUNT)
    ]

    top_pages_prefix = f"{window_key}#"
    top_pages_futures = [
        _submit_with_context(
            DYNAMODB_READ_EXECUTOR,
            _query_top_pages_shard,
            shard_id,
            top_pages_prefix,
        )
        for shard_id in range(TOP_METRIC_SHARD_COUNT)
    ]

    counts = exact_counts_future.result()
    top_wikis = _collect_top_wikis(top_wikis_futures)
    top_pages = _collect_top_pages(top_pages_futures)

    current_minute_events_so_far = sum(
        counts.get(metric_key, 0)
        for metric_key in global_activity_keys
    )

    bot_count = counts.get("METRIC#BOT_ACTIVITY#BOT#true", 0)
    human_count = counts.get("METRIC#BOT_ACTIVITY#BOT#false", 0)
    bot_total = bot_count + human_count
    bot_ratio = round(bot_count / bot_total, 4) if bot_total > 0 else 0.0

    change_types = {
        change_type: counts.get(
            f"METRIC#CHANGE_TYPE#TYPE#{change_type}",
            0,
        )
        for change_type in CHANGE_TYPES
    }

    namespace_distribution = {
        str(namespace): count
        for namespace in NAMESPACES
        if (
            count := counts.get(
                f"METRIC#NAMESPACE#NS#{namespace}",
                0,
            )
        ) > 0
    }

    duration_ms = round(
        (time.perf_counter() - started_at) * 1000,
        2,
    )

    return (
        {
            "current_minute_events_so_far": current_minute_events_so_far,
            "bot_count": bot_count,
            "human_count": human_count,
            "bot_ratio": bot_ratio,
            "change_types": change_types,
            "namespace_distribution": namespace_distribution,
            "top_wikis": top_wikis,
            "top_pages": top_pages,
        },
        duration_ms,
    )


def read_top_pages_snapshot(
    window_key_prefix: str,
) -> Tuple[List[Dict[str, Any]], float]:
    started_at = time.perf_counter()

    futures = [
        _submit_with_context(
            DYNAMODB_READ_EXECUTOR,
            _query_top_pages_shard,
            shard_id,
            window_key_prefix,
        )
        for shard_id in range(TOP_METRIC_SHARD_COUNT)
    ]

    pages = _collect_top_pages(futures)
    duration_ms = round(
        (time.perf_counter() - started_at) * 1000,
        2,
    )

    return pages, duration_ms


def read_wiki_snapshot(
    wiki: str,
    window_key: str,
) -> Tuple[Dict[str, Any], float]:
    """
    Read one wiki payload efficiently.

    Exact counters are batched. TOP_PAGES shard queries run concurrently.
    Different wiki topics are still handled one after another deliberately;
    topic-level fan-out sharding is the next architecture phase.
    """
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

    exact_metric_keys = [
        wiki_activity_key,
        bot_true_key,
        bot_false_key,
        *change_type_keys,
        *namespace_keys,
    ]

    exact_counts_future = _submit_with_context(
        DYNAMODB_READ_EXECUTOR,
        _batch_get_metric_counts,
        exact_metric_keys,
        window_key,
    )

    top_pages_prefix = f"{window_key}#WIKI#{wiki}#"
    top_pages_futures = [
        _submit_with_context(
            DYNAMODB_READ_EXECUTOR,
            _query_top_pages_shard,
            shard_id,
            top_pages_prefix,
        )
        for shard_id in range(TOP_METRIC_SHARD_COUNT)
    ]

    counts = exact_counts_future.result()
    top_pages = _collect_top_pages(top_pages_futures)

    bot_count = counts.get(bot_true_key, 0)
    human_count = counts.get(bot_false_key, 0)
    total = bot_count + human_count
    bot_ratio = round(bot_count / total, 4) if total > 0 else 0.0

    change_types = {
        change_type: count
        for change_type in CHANGE_TYPES
        if (
            count := counts.get(
                f"METRIC#WIKI_CHANGE_TYPE#WIKI#{wiki}#TYPE#{change_type}",
                0,
            )
        ) > 0
    }

    namespace_distribution = {
        str(namespace): count
        for namespace in NAMESPACES
        if (
            count := counts.get(
                f"METRIC#WIKI_NAMESPACE#WIKI#{wiki}#NS#{namespace}",
                0,
            )
        ) > 0
    }

    duration_ms = round(
        (time.perf_counter() - started_at) * 1000,
        2,
    )

    return (
        {
            "current_minute_events_so_far": counts.get(
                wiki_activity_key,
                0,
            ),
            "bot_count": bot_count,
            "human_count": human_count,
            "bot_ratio": bot_ratio,
            "change_types": change_types,
            "namespace_distribution": namespace_distribution,
            "top_pages": top_pages,
        },
        duration_ms,
    )


# ---------------------------------------------------------------------------
# DynamoDB read helpers — websocket_connections
# ---------------------------------------------------------------------------


def scan_connections() -> List[Dict[str, Any]]:
    """
    Scan active WebSocket connections.

    This keeps the current table design intact, but transfers only the three
    attributes required by the broadcaster. The later 1,000+ connection design
    should replace Scan with topic/shard Query operations.
    """
    items: List[Dict[str, Any]] = []
    exclusive_start_key = None
    now_epoch = int(datetime.now(timezone.utc).timestamp())

    while True:
        kwargs: Dict[str, Any] = {
            "ProjectionExpression": "#cid, #topics, #ttl",
            "ExpressionAttributeNames": {
                "#cid": "connection_id",
                "#topics": "topics",
                "#ttl": "ttl",
            },
        }

        if exclusive_start_key:
            kwargs["ExclusiveStartKey"] = exclusive_start_key

        response = connections_table.scan(**kwargs)

        for item in response.get("Items", []):
            ttl = item.get("ttl")

            if ttl is not None and to_int(ttl) < now_epoch:
                log_json(
                    "INFO",
                    "expired_connection_skipped",
                    connection_id=item.get("connection_id"),
                )
                continue

            if item.get("connection_id"):
                items.append(item)

        exclusive_start_key = response.get("LastEvaluatedKey")

        if not exclusive_start_key:
            break

    return items


def get_connection_topics(connection: Dict[str, Any]) -> List[str]:
    topics = connection.get("topics")

    if isinstance(topics, list) and topics:
        # Preserve order while removing duplicate subscriptions.
        return list(
            dict.fromkeys(
                str(topic)
                for topic in topics
                if topic
            )
        )

    return ["global"]


def index_connections_by_topic(
    connections: List[Dict[str, Any]],
) -> Tuple[
    List[Dict[str, Any]],
    Dict[str, List[Dict[str, Any]]],
    List[Dict[str, Any]],
]:
    global_connections: List[Dict[str, Any]] = []
    wiki_connections: Dict[str, List[Dict[str, Any]]] = {}
    top_pages_connections: List[Dict[str, Any]] = []

    for connection in connections:
        for topic in get_connection_topics(connection):
            if topic == "global":
                global_connections.append(connection)

            elif topic.startswith("wiki:"):
                wiki = topic.removeprefix("wiki:").strip().lower()

                if wiki:
                    normalized_topic = f"wiki:{wiki}"
                    wiki_connections.setdefault(
                        normalized_topic,
                        [],
                    ).append(connection)

            elif topic == "top_pages" and ENABLE_TOP_PAGES_TOPIC:
                top_pages_connections.append(connection)

    return (
        global_connections,
        wiki_connections,
        top_pages_connections,
    )


# ---------------------------------------------------------------------------
# WebSocket message builders
# ---------------------------------------------------------------------------

def add_freshness_fields(
    message: Dict[str, Any],
    latest_event_timestamp_ms: Optional[int],
    oldest_event_timestamp_ms: Optional[int],
) -> Dict[str, Any]:
    """
    Add event-time metadata to the WebSocket payload.

    These fields are useful for the backend freshness metric and for a future
    WebSocket canary/client-side freshness check.
    """
    if latest_event_timestamp_ms is not None:
        message["latest_event_timestamp_ms"] = latest_event_timestamp_ms

    if oldest_event_timestamp_ms is not None:
        message["oldest_event_timestamp_ms"] = oldest_event_timestamp_ms

    return message


def build_global_message(
    aggregation_window: str,
    broadcast_window: str,
    current_minute_events_so_far: int,
    bot_count: int,
    human_count: int,
    bot_ratio: float,
    top_wikis: List[Dict[str, Any]],
    change_types: Dict[str, int],
    namespace_distribution: Dict[str, int],
    top_pages: List[Dict[str, Any]],
    latest_event_timestamp_ms: Optional[int] = None,
    oldest_event_timestamp_ms: Optional[int] = None,
) -> Dict[str, Any]:
    message = {
        "type": "stats.update",
        "topic": "global",
        "timestamp": now_iso(),
        "aggregation_window": strip_window_prefix(aggregation_window),
        "broadcast_window": broadcast_window,
        "is_partial_window": is_partial_window(aggregation_window),
        "data": {
            "current_minute_events_so_far": current_minute_events_so_far,
            "bot_count": bot_count,
            "human_count": human_count,
            "bot_ratio": bot_ratio,
            "top_wikis": top_wikis,
            "change_types": change_types,
            "namespace_distribution": namespace_distribution,
            "top_pages": top_pages,
        },
    }

    return add_freshness_fields(
        message=message,
        latest_event_timestamp_ms=latest_event_timestamp_ms,
        oldest_event_timestamp_ms=oldest_event_timestamp_ms,
    )


def build_wiki_message(
    wiki: str,
    aggregation_window: str,
    broadcast_window: str,
    current_minute_events_so_far: int,
    bot_count: int,
    human_count: int,
    bot_ratio: float,
    change_types: Dict[str, int],
    namespace_distribution: Dict[str, int],
    top_pages: List[Dict[str, Any]],
    latest_event_timestamp_ms: Optional[int] = None,
    oldest_event_timestamp_ms: Optional[int] = None,
) -> Dict[str, Any]:
    message = {
        "type": "stats.update",
        "topic": f"wiki:{wiki}",
        "timestamp": now_iso(),
        "aggregation_window": strip_window_prefix(aggregation_window),
        "broadcast_window": broadcast_window,
        "is_partial_window": is_partial_window(aggregation_window),
        "data": {
            "wiki": wiki,
            "current_minute_events_so_far": current_minute_events_so_far,
            "bot_count": bot_count,
            "human_count": human_count,
            "bot_ratio": bot_ratio,
            "top_wikis": [],
            "change_types": change_types,
            "namespace_distribution": namespace_distribution,
            "top_pages": top_pages,
        },
    }

    return add_freshness_fields(
        message=message,
        latest_event_timestamp_ms=latest_event_timestamp_ms,
        oldest_event_timestamp_ms=oldest_event_timestamp_ms,
    )


def build_top_pages_message(
    aggregation_window: str,
    broadcast_window: str,
    top_pages: List[Dict[str, Any]],
    latest_event_timestamp_ms: Optional[int] = None,
    oldest_event_timestamp_ms: Optional[int] = None,
) -> Dict[str, Any]:
    current_minute_events_so_far = sum(to_int(page.get("count"), 0) for page in top_pages)

    message = {
        "type": "stats.update",
        "topic": "top_pages",
        "timestamp": now_iso(),
        "aggregation_window": strip_window_prefix(aggregation_window),
        "broadcast_window": broadcast_window,
        "is_partial_window": is_partial_window(aggregation_window),
        "data": {
            "current_minute_events_so_far": current_minute_events_so_far,
            "top_pages": top_pages,
        },
    }

    return add_freshness_fields(
        message=message,
        latest_event_timestamp_ms=latest_event_timestamp_ms,
        oldest_event_timestamp_ms=oldest_event_timestamp_ms,
    )


# ---------------------------------------------------------------------------
# WebSocket push
# ---------------------------------------------------------------------------


def is_gone_exception(error: ClientError) -> bool:
    error_code = error.response.get("Error", {}).get("Code")
    status_code = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode")

    return error_code in {"GoneException", "Gone"} or status_code == 410


def _delete_connection_worker(connection_id: str) -> None:
    dynamodb_client.delete_item(
        TableName=CONNECTIONS_TABLE_NAME,
        Key={
            "connection_id": {"S": connection_id},
        },
    )


def delete_connections_batch(connection_ids: List[str]) -> float:
    """
    Delete stale connections concurrently with the low-level DynamoDB client.

    This keeps the existing dynamodb:DeleteItem IAM permission; it does not
    require dynamodb:BatchWriteItem.
    """
    unique_connection_ids = list(dict.fromkeys(connection_ids))

    if not unique_connection_ids:
        return 0.0

    started_at = time.perf_counter()
    futures = [
        _submit_with_context(
            DYNAMODB_READ_EXECUTOR,
            _delete_connection_worker,
            connection_id,
        )
        for connection_id in unique_connection_ids
    ]

    first_error: Optional[Exception] = None

    for future in as_completed(futures):
        try:
            future.result()
        except Exception as error:
            if first_error is None:
                first_error = error

    duration_ms = round(
        (time.perf_counter() - started_at) * 1000,
        2,
    )

    gone_cleanup_duration_ms.record(
        duration_ms,
        {
            "environment": ENVIRONMENT,
        },
    )

    if first_error is not None:
        raise first_error

    return duration_ms


def _post_to_connection_worker(
    connection_id: str,
    prepared_message: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Execute exactly one API Gateway network call.

    This function intentionally does not update shared counters, OpenTelemetry
    metrics, logs, or DynamoDB. It returns an immutable result for the main
    thread to aggregate safely.
    """
    started_at = time.perf_counter()
    server_send_attempt_at_ms = epoch_ms_now()

    outbound_message = dict(prepared_message)
    outbound_message["server_send_attempt_at_ms"] = server_send_attempt_at_ms

    payload = json.dumps(
        outbound_message,
        separators=(",", ":"),
    ).encode("utf-8")

    try:
        instrumentation_context = (
            nullcontext()
            if TRACE_POST_TO_CONNECTION_CALLS
            else suppress_instrumentation()
        )

        with instrumentation_context:
            apigw_management.post_to_connection(
                ConnectionId=connection_id,
                Data=payload,
            )

        return {
            "connection_id": connection_id,
            "status": "sent",
            "duration_ms": round(
                (time.perf_counter() - started_at) * 1000,
                2,
            ),
            "server_send_attempt_at_ms": server_send_attempt_at_ms,
            "server_post_success_at_ms": epoch_ms_now(),
            "error": None,
            "error_type": None,
            "error_code": None,
            "http_status": 200,
        }

    except ClientError as error:
        response = error.response or {}
        error_code = response.get("Error", {}).get("Code")
        http_status = response.get(
            "ResponseMetadata",
            {},
        ).get("HTTPStatusCode")

        return {
            "connection_id": connection_id,
            "status": "gone" if is_gone_exception(error) else "error",
            "duration_ms": round(
                (time.perf_counter() - started_at) * 1000,
                2,
            ),
            "server_send_attempt_at_ms": server_send_attempt_at_ms,
            "server_post_success_at_ms": None,
            "error": error,
            "error_type": type(error).__name__,
            "error_code": error_code,
            "http_status": http_status,
        }

    except Exception as error:
        return {
            "connection_id": connection_id,
            "status": "exception",
            "duration_ms": round(
                (time.perf_counter() - started_at) * 1000,
                2,
            ),
            "server_send_attempt_at_ms": server_send_attempt_at_ms,
            "server_post_success_at_ms": None,
            "error": error,
            "error_type": type(error).__name__,
            "error_code": None,
            "http_status": None,
        }


def _record_post_result(
    result: Dict[str, Any],
    normalized_topic: str,
    latest_event_timestamp_ms: Optional[int],
    oldest_event_timestamp_ms: Optional[int],
    aws_request_id: Optional[str],
) -> None:
    metric_attrs = {
        "topic": normalized_topic,
    }
    freshness_metric_attrs = {
        "environment": ENVIRONMENT,
        "topic_type": topic_type_from_topic(normalized_topic),
    }

    duration_ms = float(result.get("duration_ms") or 0.0)
    websocket_post_duration_ms.record(
        duration_ms,
        metric_attrs,
    )

    status = result.get("status")

    if status == "sent":
        websocket_post_success_total.add(
            1,
            metric_attrs,
        )
        websocket_messages_sent_total.add(
            1,
            metric_attrs,
        )

        server_post_success_at_ms = to_epoch_ms(
            result.get("server_post_success_at_ms")
        )

        if server_post_success_at_ms is None:
            return

        if latest_event_timestamp_ms is not None:
            latency_ms = (
                server_post_success_at_ms
                - latest_event_timestamp_ms
            )

            if latency_ms >= 0:
                event_to_dashboard_latency_ms.record(
                    latency_ms,
                    freshness_metric_attrs,
                )
            else:
                log_json(
                    "WARNING",
                    "freshness_negative_latency_skipped",
                    aws_request_id=aws_request_id,
                    topic=normalized_topic,
                    latest_event_timestamp_ms=latest_event_timestamp_ms,
                    server_post_success_at_ms=server_post_success_at_ms,
                    latency_ms=latency_ms,
                )

        if oldest_event_timestamp_ms is not None:
            oldest_latency_ms = (
                server_post_success_at_ms
                - oldest_event_timestamp_ms
            )

            if oldest_latency_ms >= 0:
                oldest_event_to_dashboard_latency_ms.record(
                    oldest_latency_ms,
                    freshness_metric_attrs,
                )

        return

    if status == "gone":
        websocket_connection_gone_total.add(
            1,
            metric_attrs,
        )

        log_json(
            "INFO",
            "gone_connection_detected",
            aws_request_id=aws_request_id,
            connection_id=result.get("connection_id"),
            topic=normalized_topic,
            error_code=result.get("error_code"),
            http_status=result.get("http_status"),
        )
        return

    failure_attrs = {
        **metric_attrs,
        "error_type": str(
            result.get("error_type")
            or "UnknownError"
        ),
    }
    websocket_post_failure_total.add(
        1,
        failure_attrs,
    )

    log_json(
        "ERROR",
        "websocket_post_failed",
        aws_request_id=aws_request_id,
        connection_id=result.get("connection_id"),
        topic=normalized_topic,
        error_type=result.get("error_type"),
        error_code=result.get("error_code"),
        http_status=result.get("http_status"),
        error_message=str(result.get("error")),
    )


def send_message_to_connections(
    connections: List[Dict[str, Any]],
    message: Dict[str, Any],
    aws_request_id: Optional[str] = None,
    topic: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Fan out one payload with bounded parallelism.

    All API Gateway calls run in POST_EXECUTOR. The main thread aggregates
    metrics and performs stale-connection cleanup only after the network calls
    finish.
    """
    normalized_topic = topic or str(
        message.get("topic", "unknown")
    )
    prepared_message = json_safe(message)

    connection_ids = list(
        dict.fromkeys(
            str(connection.get("connection_id"))
            for connection in connections
            if connection.get("connection_id")
        )
    )

    attempted = len(connection_ids)

    if attempted == 0:
        return {
            "sent": 0,
            "gone": 0,
            "errors": 0,
            "gone_connection_ids": [],
            "fanout_duration_ms": 0.0,
            "gone_cleanup_duration_ms": 0.0,
        }

    fanout_batch_size.record(
        attempted,
        {
            "environment": ENVIRONMENT,
            "topic_type": topic_type_from_topic(normalized_topic),
        },
    )

    latest_event_timestamp_ms = to_epoch_ms(
        message.get("latest_event_timestamp_ms")
    )
    oldest_event_timestamp_ms = to_epoch_ms(
        message.get("oldest_event_timestamp_ms")
    )

    sent = 0
    errors = 0
    gone_connection_ids: List[str] = []
    fatal_exceptions: List[BaseException] = []
    started_at = time.perf_counter()

    with tracer.start_as_current_span(
        "broadcaster.fanout",
    ) as span:
        span.set_attribute(
            "messaging.destination.name",
            normalized_topic,
        )
        span.set_attribute(
            "fanout.connections_attempted",
            attempted,
        )
        span.set_attribute(
            "fanout.max_workers",
            MAX_POST_WORKERS,
        )

        futures = [
            _submit_with_context(
                POST_EXECUTOR,
                _post_to_connection_worker,
                connection_id,
                prepared_message,
            )
            for connection_id in connection_ids
        ]

        for future in as_completed(futures):
            try:
                result = future.result()
            except BaseException as error:
                result = {
                    "connection_id": None,
                    "status": "exception",
                    "duration_ms": 0.0,
                    "server_post_success_at_ms": None,
                    "error": error,
                    "error_type": type(error).__name__,
                    "error_code": None,
                    "http_status": None,
                }

            _record_post_result(
                result=result,
                normalized_topic=normalized_topic,
                latest_event_timestamp_ms=latest_event_timestamp_ms,
                oldest_event_timestamp_ms=oldest_event_timestamp_ms,
                aws_request_id=aws_request_id,
            )

            status = result.get("status")

            if status == "sent":
                sent += 1
            elif status == "gone":
                gone_connection_ids.append(
                    str(result["connection_id"])
                )
            else:
                errors += 1

                if status == "exception":
                    error = result.get("error")

                    if isinstance(error, BaseException):
                        fatal_exceptions.append(error)
                    else:
                        fatal_exceptions.append(
                            RuntimeError(str(error))
                        )

        fanout_only_duration_ms = round(
            (time.perf_counter() - started_at) * 1000,
            2,
        )

        fanout_duration_ms.record(
            fanout_only_duration_ms,
            {
                "environment": ENVIRONMENT,
                "topic_type": topic_type_from_topic(normalized_topic),
            },
        )

        cleanup_duration_ms = delete_connections_batch(
            gone_connection_ids,
        )

        span.set_attribute(
            "fanout.connections_sent",
            sent,
        )
        span.set_attribute(
            "fanout.connections_gone",
            len(gone_connection_ids),
        )
        span.set_attribute(
            "fanout.connections_failed",
            errors,
        )
        span.set_attribute(
            "fanout.duration_ms",
            fanout_only_duration_ms,
        )
        span.set_attribute(
            "fanout.gone_cleanup_duration_ms",
            cleanup_duration_ms,
        )

        if fatal_exceptions:
            span.record_exception(fatal_exceptions[0])
            span.set_status(
                Status(
                    StatusCode.ERROR,
                    str(fatal_exceptions[0]),
                )
            )
        else:
            span.set_status(Status(StatusCode.OK))

    log_json(
        "INFO",
        "fanout_completed",
        aws_request_id=aws_request_id,
        topic=normalized_topic,
        attempted=attempted,
        sent=sent,
        gone=len(gone_connection_ids),
        errors=errors,
        max_post_workers=MAX_POST_WORKERS,
        fanout_duration_ms=fanout_only_duration_ms,
        gone_cleanup_duration_ms=cleanup_duration_ms,
    )

    if fatal_exceptions:
        raise fatal_exceptions[0]

    return {
        "sent": sent,
        "gone": len(gone_connection_ids),
        "errors": errors,
        "gone_connection_ids": gone_connection_ids,
        "fanout_duration_ms": fanout_only_duration_ms,
        "gone_cleanup_duration_ms": cleanup_duration_ms,
    }


# ---------------------------------------------------------------------------
# Main processing
# ---------------------------------------------------------------------------

def parse_sqs_body(
    record: Dict[str, Any],
    aws_request_id: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    body = record.get("body")

    if not body:
        log_json(
            "WARNING",
            "sqs_record_skipped",
            aws_request_id=aws_request_id,
            reason="missing_body",
        )
        return None

    try:
        message = json.loads(body)
    except json.JSONDecodeError as error:
        log_exception(
            "sqs_record_invalid_json",
            aws_request_id=aws_request_id,
            error_type=type(error).__name__,
            error_message=str(error),
        )
        return None

    if message.get("message_type") != "aggregates.updated":
        log_json(
            "WARNING",
            "sqs_record_skipped",
            aws_request_id=aws_request_id,
            reason="unsupported_message_type",
            message_type=message.get("message_type"),
        )
        return None

    aggregation_windows = message.get("aggregation_windows")
    broadcast_window = message.get("broadcast_window")

    if not isinstance(aggregation_windows, list) or not aggregation_windows:
        log_json(
            "WARNING",
            "sqs_record_skipped",
            aws_request_id=aws_request_id,
            reason="missing_aggregation_windows",
        )
        return None

    if not broadcast_window:
        log_json(
            "WARNING",
            "sqs_record_skipped",
            aws_request_id=aws_request_id,
            reason="missing_broadcast_window",
        )
        return None

    return message


def collect_required_topics(
    connections: List[Dict[str, Any]]
) -> Tuple[
    List[Dict[str, Any]],
    Dict[str, List[Dict[str, Any]]],
    List[Dict[str, Any]],
    Set[str],
]:
    global_connections, wiki_connections, top_pages_connections = index_connections_by_topic(
        connections
    )

    required_wikis = {
        topic.removeprefix("wiki:")
        for topic in wiki_connections.keys()
        if topic.startswith("wiki:")
    }

    return global_connections, wiki_connections, top_pages_connections, required_wikis


def process_aggregation_window(
    aggregation_window: str,
    broadcast_window: str,
    connections: List[Dict[str, Any]],
    event_timestamp_bounds: Optional[Dict[str, Optional[int]]] = None,
    aws_request_id: Optional[str] = None,
) -> Dict[str, Any]:
    start_time = time.perf_counter()
    window_key = normalize_window_key(aggregation_window)

    metric_base_attrs = {
        "environment": ENVIRONMENT,
    }

    event_timestamp_bounds = event_timestamp_bounds or {}
    oldest_event_timestamp_ms = to_epoch_ms(
        event_timestamp_bounds.get("oldest_event_timestamp_ms")
    )
    latest_event_timestamp_ms = to_epoch_ms(
        event_timestamp_bounds.get("latest_event_timestamp_ms")
    )

    with tracer.start_as_current_span(
        "broadcaster.process_aggregation_window",
    ) as span:
        span.set_attribute(
            "broadcast.aggregation_window",
            aggregation_window,
        )
        span.set_attribute(
            "broadcast.broadcast_window",
            broadcast_window,
        )
        span.set_attribute(
            "broadcast.connections_scanned",
            len(connections),
        )
        span.set_attribute(
            "fanout.max_workers",
            MAX_POST_WORKERS,
        )
        span.set_attribute(
            "dynamodb.read_workers",
            DYNAMODB_READ_WORKERS,
        )

        if latest_event_timestamp_ms is not None:
            span.set_attribute(
                "events.latest_event_timestamp_ms",
                latest_event_timestamp_ms,
            )

        if oldest_event_timestamp_ms is not None:
            span.set_attribute(
                "events.oldest_event_timestamp_ms",
                oldest_event_timestamp_ms,
            )

        try:
            (
                global_connections,
                wiki_connections,
                top_pages_connections,
                required_wikis,
            ) = collect_required_topics(connections)

            span.set_attribute(
                "broadcast.global_subscribers",
                len(global_connections),
            )
            span.set_attribute(
                "broadcast.top_pages_subscribers",
                len(top_pages_connections),
            )
            span.set_attribute(
                "broadcast.wiki_topic_count",
                len(required_wikis),
            )

            metrics: Dict[str, Any] = {
                "connections_scanned": len(connections),
                "messages_sent": 0,
                "gone_connections": 0,
                "post_errors": 0,
                "aggregate_reads_duration_ms": 0.0,
                "payload_build_duration_ms": 0.0,
                "fanout_duration_ms": 0.0,
                "gone_cleanup_duration_ms": 0.0,
            }
            gone_connection_ids: Set[str] = set()

            if (
                not global_connections
                and not wiki_connections
                and not top_pages_connections
            ):
                duration_ms = round(
                    (time.perf_counter() - start_time) * 1000,
                    2,
                )

                broadcast_duration_ms.record(
                    duration_ms,
                    {
                        **metric_base_attrs,
                        "result": "skipped",
                    },
                )

                span.set_attribute(
                    "broadcast.result",
                    "skipped",
                )
                span.set_attribute(
                    "broadcast.reason",
                    "no_matching_subscriptions",
                )
                span.set_attribute(
                    "broadcast.duration_ms",
                    duration_ms,
                )
                span.set_status(Status(StatusCode.OK))

                log_json(
                    "INFO",
                    "broadcast_skipped",
                    aws_request_id=aws_request_id,
                    reason="no_matching_subscriptions",
                    aggregation_window=aggregation_window,
                    broadcast_window=broadcast_window,
                    latest_event_timestamp_ms=latest_event_timestamp_ms,
                    oldest_event_timestamp_ms=oldest_event_timestamp_ms,
                    duration_ms=duration_ms,
                    **metrics,
                )

                return {
                    **metrics,
                    "gone_connection_ids": [],
                }

            global_snapshot: Optional[Dict[str, Any]] = None
            top_pages: List[Dict[str, Any]] = []

            # Global aggregate reads: BatchGet exact counters and run shard
            # queries concurrently.
            if global_connections:
                global_snapshot, reads_duration_ms = read_global_snapshot(
                    window_key,
                )
                metrics["aggregate_reads_duration_ms"] += reads_duration_ms

                aggregate_reads_duration_ms.record(
                    reads_duration_ms,
                    {
                        "environment": ENVIRONMENT,
                        "topic_type": "global",
                    },
                )

                top_pages = global_snapshot["top_pages"]

                payload_started_at = time.perf_counter()
                global_message = build_global_message(
                    aggregation_window=aggregation_window,
                    broadcast_window=broadcast_window,
                    current_minute_events_so_far=global_snapshot[
                        "current_minute_events_so_far"
                    ],
                    bot_count=global_snapshot["bot_count"],
                    human_count=global_snapshot["human_count"],
                    bot_ratio=global_snapshot["bot_ratio"],
                    top_wikis=global_snapshot["top_wikis"],
                    change_types=global_snapshot["change_types"],
                    namespace_distribution=global_snapshot[
                        "namespace_distribution"
                    ],
                    top_pages=top_pages,
                    latest_event_timestamp_ms=latest_event_timestamp_ms,
                    oldest_event_timestamp_ms=oldest_event_timestamp_ms,
                )
                payload_duration_ms = round(
                    (time.perf_counter() - payload_started_at) * 1000,
                    2,
                )
                metrics["payload_build_duration_ms"] += payload_duration_ms

                payload_build_duration_ms.record(
                    payload_duration_ms,
                    {
                        "environment": ENVIRONMENT,
                        "topic_type": "global",
                    },
                )

                result = send_message_to_connections(
                    connections=[
                        connection
                        for connection in global_connections
                        if str(connection.get("connection_id"))
                        not in gone_connection_ids
                    ],
                    message=global_message,
                    aws_request_id=aws_request_id,
                    topic="global",
                )

                metrics["messages_sent"] += result["sent"]
                metrics["gone_connections"] += result["gone"]
                metrics["post_errors"] += result["errors"]
                metrics["fanout_duration_ms"] += result[
                    "fanout_duration_ms"
                ]
                metrics["gone_cleanup_duration_ms"] += result[
                    "gone_cleanup_duration_ms"
                ]
                gone_connection_ids.update(
                    result["gone_connection_ids"]
                )

            # Wiki topics still execute topic by topic, but every topic now uses
            # BatchGet and parallel TOP_PAGES shard reads internally.
            for wiki in sorted(required_wikis):
                topic = f"wiki:{wiki}"
                subscribers = [
                    connection
                    for connection in wiki_connections.get(topic, [])
                    if str(connection.get("connection_id"))
                    not in gone_connection_ids
                ]

                if not subscribers:
                    continue

                wiki_snapshot, reads_duration_ms = read_wiki_snapshot(
                    wiki,
                    window_key,
                )
                metrics["aggregate_reads_duration_ms"] += reads_duration_ms

                aggregate_reads_duration_ms.record(
                    reads_duration_ms,
                    {
                        "environment": ENVIRONMENT,
                        "topic_type": "wiki",
                    },
                )

                payload_started_at = time.perf_counter()
                wiki_message = build_wiki_message(
                    wiki=wiki,
                    aggregation_window=aggregation_window,
                    broadcast_window=broadcast_window,
                    current_minute_events_so_far=wiki_snapshot[
                        "current_minute_events_so_far"
                    ],
                    bot_count=wiki_snapshot["bot_count"],
                    human_count=wiki_snapshot["human_count"],
                    bot_ratio=wiki_snapshot["bot_ratio"],
                    change_types=wiki_snapshot["change_types"],
                    namespace_distribution=wiki_snapshot[
                        "namespace_distribution"
                    ],
                    top_pages=wiki_snapshot["top_pages"],
                    latest_event_timestamp_ms=latest_event_timestamp_ms,
                    oldest_event_timestamp_ms=oldest_event_timestamp_ms,
                )
                payload_duration_ms = round(
                    (time.perf_counter() - payload_started_at) * 1000,
                    2,
                )
                metrics["payload_build_duration_ms"] += payload_duration_ms

                payload_build_duration_ms.record(
                    payload_duration_ms,
                    {
                        "environment": ENVIRONMENT,
                        "topic_type": "wiki",
                    },
                )

                result = send_message_to_connections(
                    connections=subscribers,
                    message=wiki_message,
                    aws_request_id=aws_request_id,
                    topic=topic,
                )

                metrics["messages_sent"] += result["sent"]
                metrics["gone_connections"] += result["gone"]
                metrics["post_errors"] += result["errors"]
                metrics["fanout_duration_ms"] += result[
                    "fanout_duration_ms"
                ]
                metrics["gone_cleanup_duration_ms"] += result[
                    "gone_cleanup_duration_ms"
                ]
                gone_connection_ids.update(
                    result["gone_connection_ids"]
                )

            # Standalone top_pages reuses global TOP_PAGES reads when global is
            # subscribed. Otherwise it runs only the TOP_PAGES shard queries.
            remaining_top_pages_connections = [
                connection
                for connection in top_pages_connections
                if str(connection.get("connection_id"))
                not in gone_connection_ids
            ]

            if remaining_top_pages_connections:
                if global_snapshot is None:
                    top_pages, reads_duration_ms = read_top_pages_snapshot(
                        f"{window_key}#",
                    )
                    metrics[
                        "aggregate_reads_duration_ms"
                    ] += reads_duration_ms

                    aggregate_reads_duration_ms.record(
                        reads_duration_ms,
                        {
                            "environment": ENVIRONMENT,
                            "topic_type": "top_pages",
                        },
                    )

                payload_started_at = time.perf_counter()
                top_pages_message = build_top_pages_message(
                    aggregation_window=aggregation_window,
                    broadcast_window=broadcast_window,
                    top_pages=top_pages,
                    latest_event_timestamp_ms=latest_event_timestamp_ms,
                    oldest_event_timestamp_ms=oldest_event_timestamp_ms,
                )
                payload_duration_ms = round(
                    (time.perf_counter() - payload_started_at) * 1000,
                    2,
                )
                metrics["payload_build_duration_ms"] += payload_duration_ms

                payload_build_duration_ms.record(
                    payload_duration_ms,
                    {
                        "environment": ENVIRONMENT,
                        "topic_type": "top_pages",
                    },
                )

                result = send_message_to_connections(
                    connections=remaining_top_pages_connections,
                    message=top_pages_message,
                    aws_request_id=aws_request_id,
                    topic="top_pages",
                )

                metrics["messages_sent"] += result["sent"]
                metrics["gone_connections"] += result["gone"]
                metrics["post_errors"] += result["errors"]
                metrics["fanout_duration_ms"] += result[
                    "fanout_duration_ms"
                ]
                metrics["gone_cleanup_duration_ms"] += result[
                    "gone_cleanup_duration_ms"
                ]
                gone_connection_ids.update(
                    result["gone_connection_ids"]
                )

            duration_ms = round(
                (time.perf_counter() - start_time) * 1000,
                2,
            )

            broadcast_completed_total.add(
                1,
                {
                    **metric_base_attrs,
                    "result": "completed",
                },
            )
            broadcast_duration_ms.record(
                duration_ms,
                {
                    **metric_base_attrs,
                    "result": "completed",
                },
            )

            span.set_attribute(
                "broadcast.result",
                "completed",
            )
            span.set_attribute(
                "broadcast.duration_ms",
                duration_ms,
            )
            span.set_attribute(
                "broadcast.messages_sent",
                metrics["messages_sent"],
            )
            span.set_attribute(
                "broadcast.gone_connections",
                metrics["gone_connections"],
            )
            span.set_attribute(
                "broadcast.post_errors",
                metrics["post_errors"],
            )
            span.set_attribute(
                "broadcast.aggregate_reads_duration_ms",
                metrics["aggregate_reads_duration_ms"],
            )
            span.set_attribute(
                "broadcast.payload_build_duration_ms",
                metrics["payload_build_duration_ms"],
            )
            span.set_attribute(
                "broadcast.fanout_duration_ms",
                metrics["fanout_duration_ms"],
            )
            span.set_attribute(
                "broadcast.gone_cleanup_duration_ms",
                metrics["gone_cleanup_duration_ms"],
            )
            span.set_status(Status(StatusCode.OK))

            log_json(
                "INFO",
                "broadcast_completed",
                aws_request_id=aws_request_id,
                aggregation_window=aggregation_window,
                broadcast_window=broadcast_window,
                duration_ms=duration_ms,
                max_post_workers=MAX_POST_WORKERS,
                dynamodb_read_workers=DYNAMODB_READ_WORKERS,
                **metrics,
            )

            return {
                **metrics,
                "gone_connection_ids": list(
                    gone_connection_ids
                ),
            }

        except Exception as error:
            duration_ms = round(
                (time.perf_counter() - start_time) * 1000,
                2,
            )

            failure_attrs = {
                **metric_base_attrs,
                "error_type": type(error).__name__,
            }

            broadcast_failed_total.add(
                1,
                failure_attrs,
            )
            broadcast_duration_ms.record(
                duration_ms,
                {
                    **failure_attrs,
                    "result": "failed",
                },
            )

            span.record_exception(error)
            span.set_attribute(
                "broadcast.result",
                "failed",
            )
            span.set_attribute(
                "broadcast.duration_ms",
                duration_ms,
            )
            span.set_attribute(
                "error.type",
                type(error).__name__,
            )
            span.set_status(
                Status(
                    StatusCode.ERROR,
                    str(error),
                )
            )

            raise


def process_sqs_record(
    record: Dict[str, Any],
    aws_request_id: Optional[str] = None,
) -> None:
    with tracer.start_as_current_span(
        "broadcaster.process_sqs_record",
    ) as span:
        message_id = record.get("messageId")

        if message_id:
            span.set_attribute(
                "messaging.message.id",
                str(message_id),
            )

        traceparent = get_sqs_message_attribute(
            record,
            "traceparent",
        )
        span.set_attribute(
            "otel.traceparent.received",
            bool(traceparent),
        )

        message = parse_sqs_body(
            record,
            aws_request_id=aws_request_id,
        )

        if message is None:
            span.set_attribute(
                "sqs.record.result",
                "skipped",
            )
            span.set_status(Status(StatusCode.OK))
            return

        broadcast_window = message["broadcast_window"]
        aggregation_windows = message["aggregation_windows"]
        bounds_by_window = message.get(
            "event_timestamp_bounds_by_window"
        )
        timestamp_bounds_window_count = (
            len(bounds_by_window)
            if isinstance(bounds_by_window, dict)
            else 0
        )

        span.set_attribute(
            "broadcast.broadcast_window",
            str(broadcast_window),
        )
        span.set_attribute(
            "broadcast.aggregation_window_count",
            len(aggregation_windows),
        )
        span.set_attribute(
            "events.timestamp_bounds_window_count",
            timestamp_bounds_window_count,
        )

        scan_started_at = time.perf_counter()
        connections = scan_connections()
        scan_duration_ms = round(
            (time.perf_counter() - scan_started_at) * 1000,
            2,
        )

        active_connections_scanned.record(
            len(connections),
            {
                "environment": ENVIRONMENT,
                "source": "websocket_connections",
            },
        )
        connections_scan_duration_ms.record(
            scan_duration_ms,
            {
                "environment": ENVIRONMENT,
                "source": "websocket_connections",
            },
        )

        span.set_attribute(
            "connections.scan_duration_ms",
            scan_duration_ms,
        )

        log_json(
            "INFO",
            "broadcast_signal_received",
            aws_request_id=aws_request_id,
            broadcast_window=broadcast_window,
            aggregation_windows=aggregation_windows,
            aggregation_window_count=len(aggregation_windows),
            timestamp_bounds_window_count=timestamp_bounds_window_count,
            connections_scanned=len(connections),
            connections_scan_duration_ms=scan_duration_ms,
            max_post_workers=MAX_POST_WORKERS,
            dynamodb_read_workers=DYNAMODB_READ_WORKERS,
        )

        for aggregation_window in aggregation_windows:
            aggregation_window_text = str(
                aggregation_window
            )
            event_timestamp_bounds = (
                get_event_timestamp_bounds_for_window(
                    message=message,
                    aggregation_window=aggregation_window_text,
                )
            )

            result = process_aggregation_window(
                aggregation_window=aggregation_window_text,
                broadcast_window=str(broadcast_window),
                connections=connections,
                event_timestamp_bounds=event_timestamp_bounds,
                aws_request_id=aws_request_id,
            )

            gone_ids = set(
                result.get("gone_connection_ids", [])
            )

            if gone_ids:
                connections = [
                    connection
                    for connection in connections
                    if str(connection.get("connection_id"))
                    not in gone_ids
                ]

        span.set_attribute(
            "sqs.record.result",
            "processed",
        )
        span.set_status(Status(StatusCode.OK))


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    SQS event source mapping compatible.

    Recommended event source mapping:
      batch_size = 1
      function_response_types = ["ReportBatchItemFailures"]

    We still support multiple records defensively.
    """
    start_time = time.perf_counter()
    aws_request_id = getattr(context, "aws_request_id", None)
    batch_item_failures = []

    records = event.get("Records", [])
    parent_context, trace_context_extracted = first_trace_context(records)
    span_kwargs = {"context": parent_context} if parent_context is not None else {}


    try:
        with tracer.start_as_current_span("broadcaster.lambda_handler", **span_kwargs) as span:
            span.set_attribute("faas.trigger", "sqs")
            span.set_attribute("faas.execution", aws_request_id or "unknown")
            span.set_attribute("sqs.record_count", len(records))
            span.set_attribute("otel.trace_context.extracted", trace_context_extracted)

            log_json(
                "INFO",
                "broadcaster_invoked",
                aws_request_id=aws_request_id,
                sqs_record_count=len(records),
                trace_context_extracted=trace_context_extracted,
            )

            for record in records:
                message_id = record.get("messageId")

                try:
                    process_sqs_record(record, aws_request_id=aws_request_id)

                except Exception as error:
                    span.record_exception(error)

                    log_exception(
                        "sqs_record_processing_failed",
                        aws_request_id=aws_request_id,
                        message_id=message_id,
                        error_type=type(error).__name__,
                        error_message=str(error),
                    )

                    if message_id:
                        batch_item_failures.append(
                            {
                                "itemIdentifier": message_id,
                            }
                        )

            duration_ms = round((time.perf_counter() - start_time) * 1000, 2)
            successful_records = len(records) - len(batch_item_failures)

            log_json(
                "INFO",
                "broadcaster_batch_processed",
                aws_request_id=aws_request_id,
                input_records=len(records),
                failed_records=len(batch_item_failures),
                successful_records=successful_records,
                duration_ms=duration_ms,
            )

            span.set_attribute("sqs.failed_records", len(batch_item_failures))
            span.set_attribute("sqs.successful_records", successful_records)
            span.set_attribute("lambda.duration_ms", duration_ms)

            if batch_item_failures:
                span.set_attribute("lambda.result", "partial_failure")
                span.set_status(Status(StatusCode.ERROR, "Some SQS records failed"))
            else:
                span.set_attribute("lambda.result", "success")
                span.set_status(Status(StatusCode.OK))

            return {
                "batchItemFailures": batch_item_failures,
            }
            
    finally:
        flush_result = {
            "total_duration_ms": 0.0,
            "metric": {
                "attempted": False,
                "succeeded": False,
                "duration_ms": 0.0,
                "timeout_ms": 0,
                "error": None,
            },
            "trace": {
                "attempted": False,
                "succeeded": False,
                "duration_ms": 0.0,
                "timeout_ms": 0,
                "error": None,
            },
        }

        if ENABLE_OTEL_FLUSH:
            flush_result = flush_otel()

        total_duration_ms = round(
            (time.perf_counter() - start_time) * 1000,
            2,
        )

        logger.info(
            json.dumps(
                {
                    "message": (
                        "otel_local_flush_completed"
                        if ENABLE_OTEL_FLUSH
                        else "otel_flush_skipped"
                    ),
                    "aws_request_id": aws_request_id,
                    "otel_flush_enabled": ENABLE_OTEL_FLUSH,
                    "flush_duration_ms": flush_result["total_duration_ms"],
                    "metric_flush_attempted": flush_result["metric"]["attempted"],
                    "metric_flush_succeeded": flush_result["metric"]["succeeded"],
                    "metric_flush_duration_ms": flush_result["metric"]["duration_ms"],
                    "metric_flush_timeout_ms": flush_result["metric"]["timeout_ms"],
                    "metric_flush_error": flush_result["metric"]["error"],
                    "trace_flush_attempted": flush_result["trace"]["attempted"],
                    "trace_flush_succeeded": flush_result["trace"]["succeeded"],
                    "trace_flush_duration_ms": flush_result["trace"]["duration_ms"],
                    "trace_flush_timeout_ms": flush_result["trace"]["timeout_ms"],
                    "trace_flush_error": flush_result["trace"]["error"],
                    "total_duration_ms": total_duration_ms,
                }
            )
        )
