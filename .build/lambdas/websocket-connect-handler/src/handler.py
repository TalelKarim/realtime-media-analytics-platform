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
    now_iso,
    serialize_item,
)


dynamodb_client = boto3.client("dynamodb")

CONNECTIONS_TABLE_NAME = os.environ[
    "WEBSOCKET_CONNECTIONS_TABLE_NAME"
]

SUBSCRIPTIONS_TABLE_NAME = os.environ[
    "WEBSOCKET_SUBSCRIPTIONS_TABLE_NAME"
]

CONNECTION_TTL_SECONDS = int(
    os.getenv(
        "CONNECTION_TTL_SECONDS",
        "7200",
    )
)

SUBSCRIPTION_SHARD_COUNT = int(
    os.getenv(
        "SUBSCRIPTION_SHARD_COUNT",
        "20",
    )
)

DEFAULT_TOPIC = normalize_topic(
    os.getenv(
        "DEFAULT_TOPIC",
        "global",
    )
)

ENVIRONMENT = os.getenv(
    "ENVIRONMENT",
    "dev",
)

METRIC_NAMESPACE = (
    "RealtimeMediaAnalytics/WebSocket"
)


def _response(
    status_code: int,
    body: dict[str, Any],
) -> dict[str, Any]:
    return {
        "statusCode": status_code,
        "body": json.dumps(body),
    }


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

    connected_at = now_iso()

    ttl = (
        int(time.time())
        + CONNECTION_TTL_SECONDS
    )

    shard_id = calculate_subscription_shard(
        connection_id,
        SUBSCRIPTION_SHARD_COUNT,
    )

    topic_shard = build_topic_shard(
        DEFAULT_TOPIC,
        shard_id,
    )

    connection_item = {
        "connection_id": connection_id,
        "connected_at": connected_at,
        "client_type": "dashboard",
        "topics": [
            DEFAULT_TOPIC
        ],
        "subscription_shard": shard_id,
        "ttl": ttl,
    }

    subscription_item = {
        "topic_shard": topic_shard,
        "connection_id": connection_id,
        "topic": DEFAULT_TOPIC,
        "shard_id": shard_id,
        "connected_at": connected_at,
        "ttl": ttl,
    }

    try:
        dynamodb_client.transact_write_items(
            TransactItems=[
                {
                    "Put": {
                        "TableName": (
                            CONNECTIONS_TABLE_NAME
                        ),
                        "Item": serialize_item(
                            connection_item
                        ),
                    }
                },
                {
                    "Put": {
                        "TableName": (
                            SUBSCRIPTIONS_TABLE_NAME
                        ),
                        "Item": serialize_item(
                            subscription_item
                        ),
                    }
                },
            ]
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
                "subscription_stored"
            ),
            connection_id=connection_id,
            topic=DEFAULT_TOPIC,
            shard_id=shard_id,
            topic_shard=topic_shard,
            ttl=ttl,
            duration_ms=duration_ms,
        )

        emit_metrics(
            namespace=METRIC_NAMESPACE,
            dimensions={
                "Environment": ENVIRONMENT,
                "Handler": "connect",
            },
            metrics={
                "WebsocketConnectionsCreated": (
                    1,
                    "Count",
                ),
                "WebsocketSubscriptionsCreated": (
                    1,
                    "Count",
                ),
                "HandlerDuration": (
                    duration_ms,
                    "Milliseconds",
                ),
            },
            topic=DEFAULT_TOPIC,
            shard_id=shard_id,
        )

        return _response(
            200,
            {
                "message": "Connected"
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
                "websocket_connect_"
                "transaction_failed"
            ),
            connection_id=connection_id,
            topic=DEFAULT_TOPIC,
            shard_id=shard_id,
            error_code=error_code,
            error=str(exc),
            duration_ms=duration_ms,
        )

        emit_metrics(
            namespace=METRIC_NAMESPACE,
            dimensions={
                "Environment": ENVIRONMENT,
                "Handler": "connect",
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
                    "Failed to connect"
                )
            },
        )
