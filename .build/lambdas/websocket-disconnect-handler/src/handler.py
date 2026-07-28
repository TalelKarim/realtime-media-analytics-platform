import json
import os
import time
from typing import Any

import boto3
from botocore.exceptions import ClientError
from websocket_common import (
    build_topic_shard,
    calculate_subscription_shard,
    emit_metrics,
    log_event,
    normalize_topic,
    serialize_item,
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


def _response(
    status_code: int,
    body: dict[str, Any],
) -> dict[str, Any]:
    return {
        "statusCode": status_code,
        "body": json.dumps(body),
    }


def _delete_connection_and_subscriptions(
    connection_id: str,
    topics: list[str],
    shard_id: int,
) -> int:
    unique_topics = sorted(
        {
            normalize_topic(topic)
            for topic in topics
            if topic
        }
    )

    # DynamoDB TransactWriteItems supports
    # at most 100 actions. Normal V2 limit is
    # 50 topics, but chunking also supports
    # older or malformed items safely.
    topic_chunks = [
        unique_topics[
            index:index + 99
        ]
        for index in range(
            0,
            len(unique_topics),
            99,
        )
    ]

    if not topic_chunks:
        topic_chunks = [[]]

    for (
        chunk_index,
        topic_chunk,
    ) in enumerate(topic_chunks):
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
            for topic in topic_chunk
        ]

        if (
            chunk_index
            == len(topic_chunks) - 1
        ):
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

    return len(unique_topics)


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

    try:
        response = connections_table.get_item(
            Key={
                "connection_id": connection_id,
            },
            ConsistentRead=True,
        )

        item = response.get("Item")

        if not item:
            log_event(
                "info",
                (
                    "websocket_connection_"
                    "already_absent"
                ),
                connection_id=connection_id,
            )

            return _response(
                200,
                {
                    "message": "Disconnected"
                },
            )

        raw_topics = item.get(
            "topics",
            ["global"],
        )

        topics = [
            normalize_topic(topic)
            for topic in raw_topics
            if (
                isinstance(topic, str)
                and topic.strip()
            )
        ]

        if "global" not in topics:
            topics.append("global")

        shard_id = int(
            item.get(
                "subscription_shard",
                calculate_subscription_shard(
                    connection_id,
                    SUBSCRIPTION_SHARD_COUNT,
                ),
            )
        )

        deleted_count = (
            _delete_connection_and_subscriptions(
                connection_id=connection_id,
                topics=topics,
                shard_id=shard_id,
            )
        )

        duration_ms = round(
            (
                time.perf_counter()
                - started_at
            )
            * 1000,
            2,
        )

        log_event(
            "info",
            (
                "websocket_connection_and_"
                "subscriptions_deleted"
            ),
            connection_id=connection_id,
            shard_id=shard_id,
            subscriptions_deleted=(
                deleted_count
            ),
            duration_ms=duration_ms,
        )

        emit_metrics(
            namespace=METRIC_NAMESPACE,
            dimensions={
                "Environment": ENVIRONMENT,
                "Handler": "disconnect",
            },
            metrics={
                "WebsocketConnectionsDeleted": (
                    1,
                    "Count",
                ),
                "WebsocketSubscriptionsDeleted": (
                    deleted_count,
                    "Count",
                ),
                "HandlerDuration": (
                    duration_ms,
                    "Milliseconds",
                ),
            },
            shard_id=shard_id,
        )

        return _response(
            200,
            {
                "message": "Disconnected"
            },
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
                "websocket_disconnect_"
                "transaction_failed"
            ),
            connection_id=connection_id,
            error_code=error_code,
            error=str(exc),
            duration_ms=duration_ms,
        )

        emit_metrics(
            namespace=METRIC_NAMESPACE,
            dimensions={
                "Environment": ENVIRONMENT,
                "Handler": "disconnect",
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
            error_code=error_code,
        )

        return _response(
            500,
            {
                "message": (
                    "Failed to disconnect"
                )
            },
        )
