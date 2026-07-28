import json
import os
import re
import time
from typing import Any, Optional

import boto3
from botocore.exceptions import ClientError
from websocket_common import (
    build_topic_shard,
    calculate_subscription_shard,
    emit_metrics,
    log_event,
    normalize_topic,
    now_iso,
    serialize_item,
    serialize_value,
)


dynamodb = boto3.resource("dynamodb")
dynamodb_client = boto3.client("dynamodb")

CONNECTIONS_TABLE_NAME = os.environ[
    "WEBSOCKET_CONNECTIONS_TABLE_NAME"
]

SUBSCRIPTIONS_TABLE_NAME = os.environ[
    "WEBSOCKET_SUBSCRIPTIONS_TABLE_NAME"
]

SUBSCRIPTION_SHARD_COUNT = int(
    os.getenv(
        "SUBSCRIPTION_SHARD_COUNT",
        "20",
    )
)

CONNECTION_TTL_SECONDS = int(
    os.getenv(
        "CONNECTION_TTL_SECONDS",
        "7200",
    )
)

MAX_TOPICS_PER_CONNECTION = int(
    os.getenv(
        "MAX_TOPICS_PER_CONNECTION",
        "50",
    )
)

ENVIRONMENT = os.getenv(
    "ENVIRONMENT",
    "dev",
)

METRIC_NAMESPACE = (
    "RealtimeMediaAnalytics/WebSocket"
)

connections_table = dynamodb.Table(
    CONNECTIONS_TABLE_NAME
)

WIKI_TOPIC_PATTERN = re.compile(
    r"^wiki:[a-z0-9_-]{2,80}$"
)

ALLOWED_STATIC_TOPICS = {
    "global",
    "top_pages",
}


def _response(
    status_code: int,
    body: dict[str, Any],
) -> dict[str, Any]:
    return {
        "statusCode": status_code,
        "body": json.dumps(body),
    }


def _parse_body(
    event: dict[str, Any],
) -> Optional[dict[str, Any]]:
    body = event.get("body")

    if body is None:
        return None

    if isinstance(body, dict):
        return body

    try:
        parsed = json.loads(body)

        if isinstance(parsed, dict):
            return parsed

        return None

    except (
        json.JSONDecodeError,
        TypeError,
    ):
        return None


def _is_valid_topic(
    topic: str,
) -> bool:
    if topic in ALLOWED_STATIC_TOPICS:
        return True

    return bool(
        WIKI_TOPIC_PATTERN.fullmatch(
            topic
        )
    )


def _management_client(
    event: dict[str, Any],
):
    request_context = event.get(
        "requestContext",
        {},
    )

    domain_name = request_context.get(
        "domainName"
    )

    stage = request_context.get(
        "stage"
    )

    if not domain_name or not stage:
        return None

    return boto3.client(
        "apigatewaymanagementapi",
        endpoint_url=(
            f"https://{domain_name}/{stage}"
        ),
    )


def _get_connection(
    connection_id: str,
) -> Optional[dict[str, Any]]:
    response = connections_table.get_item(
        Key={
            "connection_id": connection_id,
        },
        ConsistentRead=True,
    )

    return response.get("Item")


def _connection_state(
    item: dict[str, Any],
    connection_id: str,
) -> tuple[
    list[str],
    int,
    int,
    str,
]:
    raw_topics = item.get(
        "topics",
        ["global"],
    )

    topics = sorted(
        {
            normalize_topic(topic)
            for topic in raw_topics
            if (
                isinstance(topic, str)
                and topic.strip()
            )
        }
    )

    if "global" not in topics:
        topics.insert(
            0,
            "global",
        )

    shard_id = int(
        item.get(
            "subscription_shard",
            calculate_subscription_shard(
                connection_id,
                SUBSCRIPTION_SHARD_COUNT,
            ),
        )
    )

    ttl = int(
        item.get(
            "ttl",
            (
                int(time.time())
                + CONNECTION_TTL_SECONDS
            ),
        )
    )

    connected_at = str(
        item.get(
            "connected_at",
            now_iso(),
        )
    )

    return (
        topics,
        shard_id,
        ttl,
        connected_at,
    )


def _write_subscription_change(
    *,
    connection_id: str,
    topic: str,
    updated_topics: list[str],
    shard_id: int,
    ttl: int,
    connected_at: str,
    action: str,
) -> None:
    topic_shard = build_topic_shard(
        topic,
        shard_id,
    )

    update_connection = {
        "Update": {
            "TableName": (
                CONNECTIONS_TABLE_NAME
            ),
            "Key": serialize_item(
                {
                    "connection_id": (
                        connection_id
                    )
                }
            ),
            "UpdateExpression": (
                "SET #topics = :topics, "
                "subscription_shard = "
                "if_not_exists("
                "subscription_shard, :shard)"
            ),
            "ConditionExpression": (
                "attribute_exists("
                "connection_id)"
            ),
            "ExpressionAttributeNames": {
                "#topics": "topics"
            },
            "ExpressionAttributeValues": {
                ":topics": serialize_value(
                    updated_topics
                ),
                ":shard": serialize_value(
                    shard_id
                ),
            },
        }
    }

    if action == "subscribe":
        subscription_change = {
            "Put": {
                "TableName": (
                    SUBSCRIPTIONS_TABLE_NAME
                ),
                "Item": serialize_item(
                    {
                        "topic_shard": (
                            topic_shard
                        ),
                        "connection_id": (
                            connection_id
                        ),
                        "topic": topic,
                        "shard_id": shard_id,
                        "connected_at": (
                            connected_at
                        ),
                        "ttl": ttl,
                    }
                ),
            }
        }

    else:
        subscription_change = {
            "Delete": {
                "TableName": (
                    SUBSCRIPTIONS_TABLE_NAME
                ),
                "Key": serialize_item(
                    {
                        "topic_shard": (
                            topic_shard
                        ),
                        "connection_id": (
                            connection_id
                        ),
                    }
                ),
            }
        }

    dynamodb_client.transact_write_items(
        TransactItems=[
            update_connection,
            subscription_change,
        ]
    )


def _delete_connection_state(
    connection_id: str,
) -> int:
    item = _get_connection(
        connection_id
    )

    if not item:
        return 0

    (
        topics,
        shard_id,
        _,
        _,
    ) = _connection_state(
        item,
        connection_id,
    )

    transaction_items = [
        {
            "Delete": {
                "TableName": (
                    SUBSCRIPTIONS_TABLE_NAME
                ),
                "Key": serialize_item(
                    {
                        "topic_shard": (
                            build_topic_shard(
                                topic,
                                shard_id,
                            )
                        ),
                        "connection_id": (
                            connection_id
                        ),
                    }
                ),
            }
        }
        for topic in topics
    ]

    transaction_items.append(
        {
            "Delete": {
                "TableName": (
                    CONNECTIONS_TABLE_NAME
                ),
                "Key": serialize_item(
                    {
                        "connection_id": (
                            connection_id
                        )
                    }
                ),
            }
        }
    )

    dynamodb_client.transact_write_items(
        TransactItems=transaction_items
    )

    return len(topics)


def _send_message(
    event: dict[str, Any],
    connection_id: str,
    payload: dict[str, Any],
) -> None:
    client = _management_client(event)

    if client is None:
        log_event(
            "warning",
            "management_client_unavailable",
            connection_id=connection_id,
        )

        return

    try:
        client.post_to_connection(
            ConnectionId=connection_id,
            Data=json.dumps(
                payload
            ).encode("utf-8"),
        )

    except ClientError as exc:
        error_code = (
            exc.response
            .get("Error", {})
            .get("Code")
        )

        log_event(
            "warning",
            "post_to_connection_failed",
            connection_id=connection_id,
            error_code=error_code,
            error=str(exc),
        )

        if error_code == "GoneException":
            try:
                deleted_count = (
                    _delete_connection_state(
                        connection_id
                    )
                )

                log_event(
                    "info",
                    (
                        "gone_connection_"
                        "state_deleted"
                    ),
                    connection_id=(
                        connection_id
                    ),
                    subscriptions_deleted=(
                        deleted_count
                    ),
                )

            except ClientError as cleanup_exc:
                log_event(
                    "error",
                    (
                        "gone_connection_"
                        "cleanup_failed"
                    ),
                    connection_id=(
                        connection_id
                    ),
                    error=str(
                        cleanup_exc
                    ),
                )


def lambda_handler(
    event: dict[str, Any],
    context: Any,
) -> dict[str, Any]:
    started_at = time.perf_counter()

    request_context = event.get(
        "requestContext",
        {},
    )

    connection_id = request_context.get(
        "connectionId"
    )

    if not connection_id:
        log_event(
            "error",
            "missing_connection_id",
            request_context=request_context,
        )

        return _response(
            400,
            {
                "message": (
                    "Missing connectionId"
                )
            },
        )

    message = _parse_body(event)

    if message is None:
        payload = {
            "type": "error",
            "message": (
                "Invalid JSON message"
            ),
        }

        _send_message(
            event,
            connection_id,
            payload,
        )

        return _response(
            200,
            payload,
        )

    action = message.get("action")
    raw_topic = message.get("topic")

    if action not in {
        "subscribe",
        "unsubscribe",
    }:
        payload = {
            "type": "error",
            "message": "Invalid action",
        }

        _send_message(
            event,
            connection_id,
            payload,
        )

        return _response(
            200,
            payload,
        )

    if not isinstance(
        raw_topic,
        str,
    ):
        payload = {
            "type": "error",
            "message": "Unsupported topic",
        }

        _send_message(
            event,
            connection_id,
            payload,
        )

        return _response(
            200,
            payload,
        )

    topic = normalize_topic(
        raw_topic
    )

    if not _is_valid_topic(topic):
        payload = {
            "type": "error",
            "message": "Unsupported topic",
        }

        _send_message(
            event,
            connection_id,
            payload,
        )

        return _response(
            200,
            payload,
        )

    if (
        action == "unsubscribe"
        and topic == "global"
    ):
        payload = {
            "type": "error",
            "topic": "global",
            "message": (
                "The global topic is mandatory"
            ),
        }

        _send_message(
            event,
            connection_id,
            payload,
        )

        return _response(
            200,
            payload,
        )

    try:
        connection = _get_connection(
            connection_id
        )

        if not connection:
            payload = {
                "type": "error",
                "message": (
                    "Connection state not found"
                ),
            }

            _send_message(
                event,
                connection_id,
                payload,
            )

            return _response(
                404,
                payload,
            )

        (
            current_topics,
            shard_id,
            ttl,
            connected_at,
        ) = _connection_state(
            connection,
            connection_id,
        )

        if action == "subscribe":
            if (
                topic not in current_topics
                and len(current_topics)
                >= MAX_TOPICS_PER_CONNECTION
            ):
                payload = {
                    "type": "error",
                    "message": (
                        "Maximum number "
                        "of topics reached"
                    ),
                }

                _send_message(
                    event,
                    connection_id,
                    payload,
                )

                return _response(
                    200,
                    payload,
                )

            updated_topics = sorted(
                set(
                    current_topics
                    + [topic]
                )
            )

            status = "subscribed"

            metric_name = (
                "WebsocketSubscriptionsCreated"
            )

        else:
            updated_topics = sorted(
                topic_name
                for topic_name
                in current_topics
                if topic_name != topic
            )

            if (
                "global"
                not in updated_topics
            ):
                updated_topics.insert(
                    0,
                    "global",
                )

            status = "unsubscribed"

            metric_name = (
                "WebsocketSubscriptionsDeleted"
            )

        _write_subscription_change(
            connection_id=connection_id,
            topic=topic,
            updated_topics=updated_topics,
            shard_id=shard_id,
            ttl=ttl,
            connected_at=connected_at,
            action=action,
        )

        ack_payload = {
            "type": "subscription.ack",
            "topic": topic,
            "status": status,
        }

        _send_message(
            event,
            connection_id,
            ack_payload,
        )

        duration_ms = round(
            (
                time.perf_counter()
                - started_at
            )
            * 1000,
            2,
        )

        topic_shard = build_topic_shard(
            topic,
            shard_id,
        )

        log_event(
            "info",
            "websocket_subscription_updated",
            connection_id=connection_id,
            action=action,
            topic=topic,
            shard_id=shard_id,
            topic_shard=topic_shard,
            topics=updated_topics,
            duration_ms=duration_ms,
        )

        emit_metrics(
            namespace=METRIC_NAMESPACE,
            dimensions={
                "Environment": ENVIRONMENT,
                "Handler": "default",
            },
            metrics={
                metric_name: (
                    1,
                    "Count",
                ),
                "HandlerDuration": (
                    duration_ms,
                    "Milliseconds",
                ),
            },
            action=action,
            topic=topic,
            shard_id=shard_id,
        )

        return _response(
            200,
            ack_payload,
        )

    except ClientError as exc:
        duration_ms = round(
            (
                time.perf_counter()
                - started_at
            )
            * 1000,
            2,
        )

        error_code = (
            exc.response
            .get("Error", {})
            .get("Code", "Unknown")
        )

        log_event(
            "error",
            (
                "websocket_subscription_"
                "transaction_failed"
            ),
            connection_id=connection_id,
            action=action,
            topic=topic,
            error_code=error_code,
            error=str(exc),
            duration_ms=duration_ms,
        )

        emit_metrics(
            namespace=METRIC_NAMESPACE,
            dimensions={
                "Environment": ENVIRONMENT,
                "Handler": "default",
            },
            metrics={
                "WebsocketSubscriptionErrors": (
                    1,
                    "Count",
                ),
                "HandlerDuration": (
                    duration_ms,
                    "Milliseconds",
                ),
            },
            action=action,
            error_code=error_code,
        )

        payload = {
            "type": "error",
            "message": (
                "Failed to update "
                "subscription"
            ),
        }

        _send_message(
            event,
            connection_id,
            payload,
        )

        return _response(
            500,
            payload,
        )
