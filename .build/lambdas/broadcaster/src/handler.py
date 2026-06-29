import json
import logging
import os
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from typing import Any, Dict, List, Optional, Set, Tuple

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

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


def read_top_pages(window_key: str) -> List[Dict[str, Any]]:
    prefix = f"{window_key}#"
    pages: List[Dict[str, Any]] = []

    for shard_id in range(TOP_METRIC_SHARD_COUNT):
        metric_key = f"METRIC#TOP_PAGES#SHARD#{shard_id}"
        items = query_metric_items(metric_key, prefix)

        for item in items:
            pages.append(
                {
                    "wiki": item.get("wiki"),
                    "title": item.get("title"),
                    "count": get_event_count(item),
                    "url": item.get("title_url"),
                }
            )

    pages = [
        page
        for page in pages
        if page.get("wiki") and page.get("title") and page.get("count", 0) > 0
    ]

    pages.sort(key=lambda page: page["count"], reverse=True)

    return pages[:TOP_PAGES_LIMIT]


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
                logger.info(
                    "Skipping expired connection",
                    extra={"connection_id": item.get("connection_id")},
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
) -> Dict[str, Any]:
    return {
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


def build_wiki_message(
    wiki: str,
    aggregation_window: str,
    broadcast_window: str,
    current_minute_events_so_far: int,
) -> Dict[str, Any]:
    return {
        "type": "stats.update",
        "topic": f"wiki:{wiki}",
        "timestamp": now_iso(),
        "aggregation_window": strip_window_prefix(aggregation_window),
        "broadcast_window": broadcast_window,
        "is_partial_window": is_partial_window(aggregation_window),
        "data": {
            "wiki": wiki,
            "current_minute_events_so_far": current_minute_events_so_far,
        },
    }


def build_top_pages_message(
    aggregation_window: str,
    broadcast_window: str,
    top_pages: List[Dict[str, Any]],
) -> Dict[str, Any]:
    return {
        "type": "stats.update",
        "topic": "top_pages",
        "timestamp": now_iso(),
        "aggregation_window": strip_window_prefix(aggregation_window),
        "broadcast_window": broadcast_window,
        "is_partial_window": is_partial_window(aggregation_window),
        "data": {
            "top_pages": top_pages,
        },
    }


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


def post_to_connection(connection_id: str, message: Dict[str, Any]) -> str:
    payload = json.dumps(json_safe(message), separators=(",", ":")).encode("utf-8")

    try:
        apigw_management.post_to_connection(
            ConnectionId=connection_id,
            Data=payload,
        )
        return "sent"

    except ClientError as error:
        if is_gone_exception(error):
            logger.info(
                "Deleting stale WebSocket connection",
                extra={"connection_id": connection_id},
            )
            delete_connection(connection_id)
            return "gone"

        logger.exception(
            "Failed to post WebSocket message",
            extra={
                "connection_id": connection_id,
                "error": str(error),
            },
        )
        return "error"


def send_message_to_connections(
    connections: List[Dict[str, Any]],
    message: Dict[str, Any],
) -> Dict[str, int]:
    sent = 0
    gone = 0
    errors = 0

    for connection in connections:
        connection_id = connection.get("connection_id")

        if not connection_id:
            continue

        result = post_to_connection(connection_id, message)

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

def parse_sqs_body(record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    body = record.get("body")

    if not body:
        logger.warning("Skipping SQS record without body")
        return None

    try:
        message = json.loads(body)
    except json.JSONDecodeError:
        logger.exception("Skipping SQS record with invalid JSON body")
        return None

    if message.get("message_type") != "aggregates.updated":
        logger.warning(
            "Skipping unsupported SQS message_type",
            extra={"message_type": message.get("message_type")},
        )
        return None

    aggregation_windows = message.get("aggregation_windows")
    broadcast_window = message.get("broadcast_window")

    if not isinstance(aggregation_windows, list) or not aggregation_windows:
        logger.warning("Skipping SQS message without aggregation_windows")
        return None

    if not broadcast_window:
        logger.warning("Skipping SQS message without broadcast_window")
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
) -> Dict[str, int]:
    window_key = normalize_window_key(aggregation_window)

    (
        global_connections,
        wiki_connections,
        top_pages_connections,
        required_wikis,
    ) = collect_required_topics(connections)

    metrics = {
        "connections_scanned": len(connections),
        "messages_sent": 0,
        "gone_connections": 0,
        "post_errors": 0,
    }

    # Nothing to send.
    if not global_connections and not wiki_connections and not top_pages_connections:
        logger.info(
            "No matching WebSocket subscriptions found",
            extra={
                "aggregation_window": aggregation_window,
                "broadcast_window": broadcast_window,
            },
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
        )

        result = send_message_to_connections(global_connections, global_message)
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

        wiki_message = build_wiki_message(
            wiki=wiki,
            aggregation_window=aggregation_window,
            broadcast_window=broadcast_window,
            current_minute_events_so_far=wiki_count,
        )

        result = send_message_to_connections(subscribers, wiki_message)
        metrics["messages_sent"] += result["sent"]
        metrics["gone_connections"] += result["gone"]
        metrics["post_errors"] += result["errors"]

    # Optional top_pages standalone topic.
    if top_pages_connections:
        top_pages_message = build_top_pages_message(
            aggregation_window=aggregation_window,
            broadcast_window=broadcast_window,
            top_pages=top_pages,
        )

        result = send_message_to_connections(top_pages_connections, top_pages_message)
        metrics["messages_sent"] += result["sent"]
        metrics["gone_connections"] += result["gone"]
        metrics["post_errors"] += result["errors"]

    logger.info(
        "Broadcast completed",
        extra={
            "aggregation_window": aggregation_window,
            "broadcast_window": broadcast_window,
            **metrics,
        },
    )

    return metrics


def process_sqs_record(record: Dict[str, Any]) -> None:
    message = parse_sqs_body(record)

    # Malformed messages are skipped, not retried forever.
    if message is None:
        return

    broadcast_window = message["broadcast_window"]
    aggregation_windows = message["aggregation_windows"]

    connections = scan_connections()

    logger.info(
        "Processing broadcast signal",
        extra={
            "broadcast_window": broadcast_window,
            "aggregation_windows": aggregation_windows,
            "connections_scanned": len(connections),
        },
    )

    for aggregation_window in aggregation_windows:
        process_aggregation_window(
            aggregation_window=str(aggregation_window),
            broadcast_window=str(broadcast_window),
            connections=connections,
        )


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    SQS event source mapping compatible.

    Recommended event source mapping:
      batch_size = 1
      function_response_types = ["ReportBatchItemFailures"]

    We still support multiple records defensively.
    """
    batch_item_failures = []

    records = event.get("Records", [])

    for record in records:
        message_id = record.get("messageId")

        try:
            process_sqs_record(record)

        except Exception:
            logger.exception(
                "Failed to process SQS record",
                extra={"message_id": message_id},
            )

            if message_id:
                batch_item_failures.append(
                    {
                        "itemIdentifier": message_id,
                    }
                )

    return {
        "batchItemFailures": batch_item_failures,
    }