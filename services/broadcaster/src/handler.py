import json
import logging
import os
import time
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from typing import Any, Dict, List, Optional, Set, Tuple

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError



from opentelemetry import propagate, trace as otel_trace
from opentelemetry.context import Context
from opentelemetry.propagators.textmap import Getter
from opentelemetry.trace import Status, StatusCode

from .observability import (
    active_connections_scanned,
    broadcast_completed_total,
    broadcast_duration_ms,
    broadcast_failed_total,
    event_to_dashboard_latency_ms,
    flush_otel,
    oldest_event_to_dashboard_latency_ms,
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



ENABLE_OTEL_FLUSH = (
    os.getenv("ENABLE_OTEL_FLUSH", "true").lower() == "true"
)


AGGREGATES_TABLE_NAME = os.environ["AGGREGATES_TABLE_NAME"]
CONNECTIONS_TABLE_NAME = os.environ["CONNECTIONS_TABLE_NAME"]
WEBSOCKET_ENDPOINT_URL = os.environ["WEBSOCKET_ENDPOINT_URL"]
# Example:
# https://abc123.execute-api.eu-west-1.amazonaws.com/dev

GLOBAL_ACTIVITY_SHARD_COUNT = int(os.getenv("GLOBAL_ACTIVITY_SHARD_COUNT", "10"))
TOP_METRIC_SHARD_COUNT = int(os.getenv("TOP_METRIC_SHARD_COUNT", "10"))

TOP_WIKIS_LIMIT = int(os.getenv("TOP_WIKIS_LIMIT", "10"))
TOP_PAGES_LIMIT = int(os.getenv("TOP_PAGES_LIMIT", "10"))

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
# AWS clients
# ---------------------------------------------------------------------------

logger = logging.getLogger()
logger.setLevel(LOG_LEVEL)

dynamodb = boto3.resource("dynamodb")
aggregates_table = dynamodb.Table(AGGREGATES_TABLE_NAME)
connections_table = dynamodb.Table(CONNECTIONS_TABLE_NAME)

apigw_management = boto3.client(
    "apigatewaymanagementapi",
    endpoint_url=WEBSOCKET_ENDPOINT_URL,
)


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

def get_metric_item(metric_key: str, window_key: str) -> Optional[Dict[str, Any]]:
    response = aggregates_table.get_item(
        Key={
            "metric_key": metric_key,
            "window_key": window_key,
        }
    )

    return response.get("Item")


def get_metric_count(metric_key: str, window_key: str) -> int:
    return get_event_count(get_metric_item(metric_key, window_key))


def query_metric_items(metric_key: str, window_key_prefix: str) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    exclusive_start_key = None

    while True:
        kwargs = {
            "KeyConditionExpression": (
                Key("metric_key").eq(metric_key)
                & Key("window_key").begins_with(window_key_prefix)
            )
        }

        if exclusive_start_key:
            kwargs["ExclusiveStartKey"] = exclusive_start_key

        response = aggregates_table.query(**kwargs)
        items.extend(response.get("Items", []))

        exclusive_start_key = response.get("LastEvaluatedKey")
        if not exclusive_start_key:
            break

    return items


def read_global_activity(window_key: str) -> int:
    total = 0

    for shard_id in range(GLOBAL_ACTIVITY_SHARD_COUNT):
        metric_key = f"METRIC#GLOBAL_ACTIVITY#SHARD#{shard_id}"
        total += get_metric_count(metric_key, window_key)

    return total


def read_bot_activity(window_key: str) -> Tuple[int, int, float]:
    bot_count = get_metric_count("METRIC#BOT_ACTIVITY#BOT#true", window_key)
    human_count = get_metric_count("METRIC#BOT_ACTIVITY#BOT#false", window_key)

    total = bot_count + human_count
    bot_ratio = round(bot_count / total, 4) if total > 0 else 0.0

    return bot_count, human_count, bot_ratio


def read_change_types(window_key: str) -> Dict[str, int]:
    result: Dict[str, int] = {}

    for change_type in CHANGE_TYPES:
        metric_key = f"METRIC#CHANGE_TYPE#TYPE#{change_type}"
        result[change_type] = get_metric_count(metric_key, window_key)

    return result


def read_namespace_distribution(window_key: str) -> Dict[str, int]:
    result: Dict[str, int] = {}

    for namespace in NAMESPACES:
        metric_key = f"METRIC#NAMESPACE#NS#{namespace}"
        count = get_metric_count(metric_key, window_key)

        if count > 0:
            result[str(namespace)] = count

    return result


def read_top_wikis(window_key: str) -> List[Dict[str, Any]]:
    prefix = f"{window_key}#WIKI#"
    candidates: Dict[str, int] = {}

    for shard_id in range(TOP_METRIC_SHARD_COUNT):
        metric_key = f"METRIC#TOP_WIKIS#SHARD#{shard_id}"
        items = query_metric_items(metric_key, prefix)

        for item in items:
            wiki = item.get("wiki")

            if not wiki:
                # Fallback from SK:
                # WINDOW#2026-...#WIKI#frwiki
                window_item_key = item.get("window_key", "")
                if "#WIKI#" in window_item_key:
                    wiki = window_item_key.split("#WIKI#", 1)[1]

            if not wiki:
                continue

            candidates[wiki] = candidates.get(wiki, 0) + get_event_count(item)

    sorted_wikis = sorted(
        candidates.items(),
        key=lambda item: item[1],
        reverse=True,
    )

    return [
        {
            "wiki": wiki,
            "count": count,
        }
        for wiki, count in sorted_wikis[:TOP_WIKIS_LIMIT]
    ]


def _read_top_pages_by_prefix(window_key_prefix: str) -> List[Dict[str, Any]]:
    pages: List[Dict[str, Any]] = []

    for shard_id in range(TOP_METRIC_SHARD_COUNT):
        metric_key = f"METRIC#TOP_PAGES#SHARD#{shard_id}"
        items = query_metric_items(metric_key, window_key_prefix)

        for item in items:
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


def read_top_pages(window_key: str) -> List[Dict[str, Any]]:
    # Global top pages for the whole live aggregation window.
    return _read_top_pages_by_prefix(f"{window_key}#")


def read_top_pages_for_wiki(wiki: str, window_key: str) -> List[Dict[str, Any]]:
    # Top pages restricted to a single wiki topic.
    # The realtime processor already stores TOP_PAGES with this SK shape:
    # WINDOW#{minute}#WIKI#{wiki}#TITLE#{hash}
    return _read_top_pages_by_prefix(f"{window_key}#WIKI#{wiki}#")


def read_wiki_bot_activity(wiki: str, window_key: str) -> Tuple[int, int, float]:
    bot_count = get_metric_count(
        f"METRIC#WIKI_BOT_ACTIVITY#WIKI#{wiki}#BOT#true",
        window_key,
    )
    human_count = get_metric_count(
        f"METRIC#WIKI_BOT_ACTIVITY#WIKI#{wiki}#BOT#false",
        window_key,
    )

    total = bot_count + human_count
    bot_ratio = round(bot_count / total, 4) if total > 0 else 0.0

    return bot_count, human_count, bot_ratio


def read_wiki_change_types(wiki: str, window_key: str) -> Dict[str, int]:
    result: Dict[str, int] = {}

    for change_type in CHANGE_TYPES:
        metric_key = f"METRIC#WIKI_CHANGE_TYPE#WIKI#{wiki}#TYPE#{change_type}"
        count = get_metric_count(metric_key, window_key)

        if count > 0:
            result[change_type] = count

    return result


def read_wiki_namespace_distribution(wiki: str, window_key: str) -> Dict[str, int]:
    result: Dict[str, int] = {}

    for namespace in NAMESPACES:
        metric_key = f"METRIC#WIKI_NAMESPACE#WIKI#{wiki}#NS#{namespace}"
        count = get_metric_count(metric_key, window_key)

        if count > 0:
            result[str(namespace)] = count

    return result


def read_wiki_activity(wiki: str, window_key: str) -> int:
    metric_key = f"METRIC#WIKI_ACTIVITY#WIKI#{wiki}"
    return get_metric_count(metric_key, window_key)


# ---------------------------------------------------------------------------
# DynamoDB read helpers — websocket_connections
# ---------------------------------------------------------------------------

def scan_connections() -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    exclusive_start_key = None
    now_epoch = int(datetime.now(timezone.utc).timestamp())

    while True:
        kwargs = {}

        if exclusive_start_key:
            kwargs["ExclusiveStartKey"] = exclusive_start_key

        response = connections_table.scan(**kwargs)
        batch_items = response.get("Items", [])

        for item in batch_items:
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
        return [str(topic) for topic in topics if topic]

    # Connect handler stores ["global"] by default.
    # If the attribute is unexpectedly missing, keep the connection useful
    # instead of silently starving it.
    return ["global"]


def index_connections_by_topic(
    connections: List[Dict[str, Any]]
) -> Tuple[List[Dict[str, Any]], Dict[str, List[Dict[str, Any]]], List[Dict[str, Any]]]:
    global_connections: List[Dict[str, Any]] = []
    wiki_connections: Dict[str, List[Dict[str, Any]]] = {}
    top_pages_connections: List[Dict[str, Any]] = []

    for connection in connections:
        topics = get_connection_topics(connection)

        for topic in topics:
            if topic == "global":
                global_connections.append(connection)

            elif topic.startswith("wiki:"):
                wiki = topic.removeprefix("wiki:").strip().lower()

                if wiki:
                    normalized_topic = f"wiki:{wiki}"
                    wiki_connections.setdefault(normalized_topic, []).append(connection)

            elif topic == "top_pages" and ENABLE_TOP_PAGES_TOPIC:
                top_pages_connections.append(connection)

    return global_connections, wiki_connections, top_pages_connections


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


def delete_connection(connection_id: str) -> None:
    connections_table.delete_item(
        Key={
            "connection_id": connection_id,
        }
    )

    

def post_to_connection(
    connection_id: str,
    message: Dict[str, Any],
    aws_request_id: Optional[str] = None,
    topic: Optional[str] = None,
) -> str:
    normalized_topic = topic or str(message.get("topic", "unknown"))
    start_time = time.perf_counter()

    # Existing metrics keep their current raw topic label to avoid breaking
    # existing dashboards. The new freshness histogram uses a lower-cardinality
    # topic_type label.
    metric_attrs = {
        "topic": normalized_topic,
    }

    freshness_metric_attrs = {
        "environment": ENVIRONMENT,
        "topic_type": topic_type_from_topic(normalized_topic),
    }

    latest_event_timestamp_ms = to_epoch_ms(message.get("latest_event_timestamp_ms"))
    oldest_event_timestamp_ms = to_epoch_ms(message.get("oldest_event_timestamp_ms"))

    server_send_attempt_at_ms = epoch_ms_now()
    outbound_message = dict(message)
    outbound_message["server_send_attempt_at_ms"] = server_send_attempt_at_ms

    payload = json.dumps(
        json_safe(outbound_message),
        separators=(",", ":"),
    ).encode("utf-8")

    with tracer.start_as_current_span("broadcaster.post_to_connection") as span:
        span.set_attribute("messaging.destination.name", normalized_topic)
        span.set_attribute("aws.service", "apigatewaymanagementapi")
        span.set_attribute("rpc.method", "post_to_connection")
        span.set_attribute("websocket.topic_type", freshness_metric_attrs["topic_type"])
        span.set_attribute("websocket.server_send_attempt_at_ms", server_send_attempt_at_ms)

        if latest_event_timestamp_ms is not None:
            span.set_attribute("events.latest_event_timestamp_ms", latest_event_timestamp_ms)

        if oldest_event_timestamp_ms is not None:
            span.set_attribute("events.oldest_event_timestamp_ms", oldest_event_timestamp_ms)

        try:
            apigw_management.post_to_connection(
                ConnectionId=connection_id,
                Data=payload,
            )

            server_post_success_at_ms = epoch_ms_now()
            duration_ms = round((time.perf_counter() - start_time) * 1000, 2)

            websocket_post_success_total.add(1, metric_attrs)
            websocket_messages_sent_total.add(1, metric_attrs)
            websocket_post_duration_ms.record(duration_ms, metric_attrs)

            if latest_event_timestamp_ms is not None:
                latency_ms = server_post_success_at_ms - latest_event_timestamp_ms

                if latency_ms >= 0:
                    event_to_dashboard_latency_ms.record(
                        latency_ms,
                        freshness_metric_attrs,
                    )
                    span.set_attribute("freshness.event_to_dashboard_latency_ms", latency_ms)
                else:
                    span.set_attribute("freshness.negative_latency_detected", True)
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
                oldest_latency_ms = server_post_success_at_ms - oldest_event_timestamp_ms

                if oldest_latency_ms >= 0:
                    oldest_event_to_dashboard_latency_ms.record(
                        oldest_latency_ms,
                        freshness_metric_attrs,
                    )
                    span.set_attribute(
                        "freshness.oldest_event_to_dashboard_latency_ms",
                        oldest_latency_ms,
                    )

            span.set_attribute("websocket.post.result", "sent")
            span.set_attribute("websocket.server_post_success_at_ms", server_post_success_at_ms)
            span.set_status(Status(StatusCode.OK))

            return "sent"

        except ClientError as error:
            duration_ms = round((time.perf_counter() - start_time) * 1000, 2)
            websocket_post_duration_ms.record(duration_ms, metric_attrs)

            if is_gone_exception(error):
                websocket_connection_gone_total.add(1, metric_attrs)

                span.set_attribute("websocket.post.result", "gone")
                span.set_status(Status(StatusCode.OK))

                log_json(
                    "INFO",
                    "gone_connection_cleaned",
                    aws_request_id=aws_request_id,
                    connection_id=connection_id,
                    topic=normalized_topic,
                )
                delete_connection(connection_id)
                return "gone"

            failure_attrs = {
                **metric_attrs,
                "error_type": type(error).__name__,
            }

            websocket_post_failure_total.add(1, failure_attrs)

            span.record_exception(error)
            span.set_attribute("websocket.post.result", "error")
            span.set_attribute("error.type", type(error).__name__)
            span.set_status(Status(StatusCode.ERROR, str(error)))

            log_exception(
                "websocket_post_failed",
                aws_request_id=aws_request_id,
                connection_id=connection_id,
                topic=normalized_topic,
                error_type=type(error).__name__,
                error_message=str(error),
            )
            return "error"

        except Exception as error:
            duration_ms = round((time.perf_counter() - start_time) * 1000, 2)
            websocket_post_duration_ms.record(duration_ms, metric_attrs)

            failure_attrs = {
                **metric_attrs,
                "error_type": type(error).__name__,
            }

            websocket_post_failure_total.add(1, failure_attrs)

            span.record_exception(error)
            span.set_attribute("websocket.post.result", "exception")
            span.set_attribute("error.type", type(error).__name__)
            span.set_status(Status(StatusCode.ERROR, str(error)))

            log_exception(
                "websocket_post_failed",
                aws_request_id=aws_request_id,
                connection_id=connection_id,
                topic=normalized_topic,
                error_type=type(error).__name__,
                error_message=str(error),
            )
            raise


def send_message_to_connections(
    connections: List[Dict[str, Any]],
    message: Dict[str, Any],
    aws_request_id: Optional[str] = None,
    topic: Optional[str] = None,
) -> Dict[str, int]:
    sent = 0
    gone = 0
    errors = 0

    for connection in connections:
        connection_id = connection.get("connection_id")

        if not connection_id:
            continue

        result = post_to_connection(
            connection_id=connection_id,
            message=message,
            aws_request_id=aws_request_id,
            topic=topic or str(message.get("topic", "unknown")),
        )

        if result == "sent":
            sent += 1
        elif result == "gone":
            gone += 1
        else:
            errors += 1

    return {
        "sent": sent,
        "gone": gone,
        "errors": errors,
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
) -> Dict[str, int]:
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

    with tracer.start_as_current_span("broadcaster.process_aggregation_window") as span:
        span.set_attribute("broadcast.aggregation_window", aggregation_window)
        span.set_attribute("broadcast.broadcast_window", broadcast_window)
        span.set_attribute("broadcast.connections_scanned", len(connections))

        if latest_event_timestamp_ms is not None:
            span.set_attribute("events.latest_event_timestamp_ms", latest_event_timestamp_ms)

        if oldest_event_timestamp_ms is not None:
            span.set_attribute("events.oldest_event_timestamp_ms", oldest_event_timestamp_ms)

        try:
            (
                global_connections,
                wiki_connections,
                top_pages_connections,
                required_wikis,
            ) = collect_required_topics(connections)

            span.set_attribute("broadcast.global_subscribers", len(global_connections))
            span.set_attribute("broadcast.top_pages_subscribers", len(top_pages_connections))
            span.set_attribute("broadcast.wiki_topic_count", len(required_wikis))

            metrics = {
                "connections_scanned": len(connections),
                "messages_sent": 0,
                "gone_connections": 0,
                "post_errors": 0,
            }

            # Nothing to send.
            if not global_connections and not wiki_connections and not top_pages_connections:
                duration_ms = round((time.perf_counter() - start_time) * 1000, 2)

                broadcast_duration_ms.record(
                    duration_ms,
                    {
                        **metric_base_attrs,
                        "result": "skipped",
                    },
                )

                span.set_attribute("broadcast.result", "skipped")
                span.set_attribute("broadcast.reason", "no_matching_subscriptions")
                span.set_attribute("broadcast.duration_ms", duration_ms)
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
                return metrics

            # Read top_pages once if it is needed by global message or by top_pages topic.
            top_pages: List[Dict[str, Any]] = []
            top_pages_needed = bool(global_connections) or bool(top_pages_connections)

            if top_pages_needed:
                top_pages = read_top_pages(window_key)

            # Global message.
            # The global payload intentionally includes top_pages, as defined in Contract 5.
            if global_connections:
                current_minute_events_so_far = read_global_activity(window_key)
                bot_count, human_count, bot_ratio = read_bot_activity(window_key)
                change_types = read_change_types(window_key)
                namespace_distribution = read_namespace_distribution(window_key)
                top_wikis = read_top_wikis(window_key)

                global_message = build_global_message(
                    aggregation_window=aggregation_window,
                    broadcast_window=broadcast_window,
                    current_minute_events_so_far=current_minute_events_so_far,
                    bot_count=bot_count,
                    human_count=human_count,
                    bot_ratio=bot_ratio,
                    top_wikis=top_wikis,
                    change_types=change_types,
                    namespace_distribution=namespace_distribution,
                    top_pages=top_pages,
                    latest_event_timestamp_ms=latest_event_timestamp_ms,
                    oldest_event_timestamp_ms=oldest_event_timestamp_ms,
                )

                result = send_message_to_connections(
                    global_connections,
                    global_message,
                    aws_request_id=aws_request_id,
                    topic="global",
                )
                metrics["messages_sent"] += result["sent"]
                metrics["gone_connections"] += result["gone"]
                metrics["post_errors"] += result["errors"]

            # Wiki-specific messages.
            for wiki in sorted(required_wikis):
                topic = f"wiki:{wiki}"
                subscribers = wiki_connections.get(topic, [])

                if not subscribers:
                    continue

                wiki_count = read_wiki_activity(wiki, window_key)
                wiki_bot_count, wiki_human_count, wiki_bot_ratio = read_wiki_bot_activity(
                    wiki,
                    window_key,
                )
                wiki_change_types = read_wiki_change_types(wiki, window_key)
                wiki_namespace_distribution = read_wiki_namespace_distribution(wiki, window_key)
                wiki_top_pages = read_top_pages_for_wiki(wiki, window_key)

                wiki_message = build_wiki_message(
                    wiki=wiki,
                    aggregation_window=aggregation_window,
                    broadcast_window=broadcast_window,
                    current_minute_events_so_far=wiki_count,
                    bot_count=wiki_bot_count,
                    human_count=wiki_human_count,
                    bot_ratio=wiki_bot_ratio,
                    change_types=wiki_change_types,
                    namespace_distribution=wiki_namespace_distribution,
                    top_pages=wiki_top_pages,
                    latest_event_timestamp_ms=latest_event_timestamp_ms,
                    oldest_event_timestamp_ms=oldest_event_timestamp_ms,
                )

                result = send_message_to_connections(
                    subscribers,
                    wiki_message,
                    aws_request_id=aws_request_id,
                    topic=topic,
                )
                metrics["messages_sent"] += result["sent"]
                metrics["gone_connections"] += result["gone"]
                metrics["post_errors"] += result["errors"]

            # Optional top_pages standalone topic.
            if top_pages_connections:
                top_pages_message = build_top_pages_message(
                    aggregation_window=aggregation_window,
                    broadcast_window=broadcast_window,
                    top_pages=top_pages,
                    latest_event_timestamp_ms=latest_event_timestamp_ms,
                    oldest_event_timestamp_ms=oldest_event_timestamp_ms,
                )

                result = send_message_to_connections(
                    top_pages_connections,
                    top_pages_message,
                    aws_request_id=aws_request_id,
                    topic="top_pages",
                )
                metrics["messages_sent"] += result["sent"]
                metrics["gone_connections"] += result["gone"]
                metrics["post_errors"] += result["errors"]

            duration_ms = round((time.perf_counter() - start_time) * 1000, 2)

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

            span.set_attribute("broadcast.result", "completed")
            span.set_attribute("broadcast.duration_ms", duration_ms)
            span.set_attribute("broadcast.messages_sent", metrics["messages_sent"])
            span.set_attribute("broadcast.gone_connections", metrics["gone_connections"])
            span.set_attribute("broadcast.post_errors", metrics["post_errors"])
            span.set_status(Status(StatusCode.OK))

            log_json(
                "INFO",
                "broadcast_completed",
                aws_request_id=aws_request_id,
                aggregation_window=aggregation_window,
                broadcast_window=broadcast_window,
                duration_ms=duration_ms,
                **metrics,
            )

            return metrics

        except Exception as error:
            duration_ms = round((time.perf_counter() - start_time) * 1000, 2)

            failure_attrs = {
                **metric_base_attrs,
                "error_type": type(error).__name__,
            }

            broadcast_failed_total.add(1, failure_attrs)
            broadcast_duration_ms.record(
                duration_ms,
                {
                    **failure_attrs,
                    "result": "failed",
                },
            )

            span.record_exception(error)
            span.set_attribute("broadcast.result", "failed")
            span.set_attribute("broadcast.duration_ms", duration_ms)
            span.set_attribute("error.type", type(error).__name__)
            span.set_status(Status(StatusCode.ERROR, str(error)))

            raise


def process_sqs_record(record: Dict[str, Any], aws_request_id: Optional[str] = None) -> None:
    with tracer.start_as_current_span("broadcaster.process_sqs_record") as span:
        message_id = record.get("messageId")
        if message_id:
            span.set_attribute("messaging.message.id", str(message_id))

        traceparent = get_sqs_message_attribute(record, "traceparent")
        if traceparent:
            span.set_attribute("otel.traceparent.received", True)
        else:
            span.set_attribute("otel.traceparent.received", False)

        message = parse_sqs_body(record, aws_request_id=aws_request_id)

        # Malformed messages are skipped, not retried forever.
        if message is None:
            span.set_attribute("sqs.record.result", "skipped")
            span.set_status(Status(StatusCode.OK))
            return

        broadcast_window = message["broadcast_window"]
        aggregation_windows = message["aggregation_windows"]
        bounds_by_window = message.get("event_timestamp_bounds_by_window")
        timestamp_bounds_window_count = (
            len(bounds_by_window)
            if isinstance(bounds_by_window, dict)
            else 0
        )

        span.set_attribute("broadcast.broadcast_window", str(broadcast_window))
        span.set_attribute("broadcast.aggregation_window_count", len(aggregation_windows))
        span.set_attribute(
            "events.timestamp_bounds_window_count",
            timestamp_bounds_window_count,
        )

        connections = scan_connections()

        active_connections_scanned.record(
            len(connections),
            {
                "environment": ENVIRONMENT,
                "source": "websocket_connections",
            },
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
        )

        for aggregation_window in aggregation_windows:
            aggregation_window_text = str(aggregation_window)
            event_timestamp_bounds = get_event_timestamp_bounds_for_window(
                message=message,
                aggregation_window=aggregation_window_text,
            )

            process_aggregation_window(
                aggregation_window=aggregation_window_text,
                broadcast_window=str(broadcast_window),
                connections=connections,
                event_timestamp_bounds=event_timestamp_bounds,
                aws_request_id=aws_request_id,
            )

        span.set_attribute("sqs.record.result", "processed")
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
