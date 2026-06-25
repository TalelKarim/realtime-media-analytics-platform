# services/websocket-default-handler/src/handler.py

import json
import os
import re
from typing import Any, Dict, Optional

import boto3
from botocore.exceptions import ClientError


dynamodb = boto3.resource("dynamodb")

TABLE_NAME = os.environ["WEBSOCKET_CONNECTIONS_TABLE_NAME"]

table = dynamodb.Table(TABLE_NAME)

WIKI_TOPIC_PATTERN = re.compile(r"^wiki:[a-z0-9_-]{2,80}$")
ALLOWED_STATIC_TOPICS = {"global", "top_pages"}


def _response(status_code: int, body: dict) -> dict:
    return {
        "statusCode": status_code,
        "body": json.dumps(body),
    }


def _parse_body(event: dict) -> Optional[Dict[str, Any]]:
    body = event.get("body")

    if body is None:
        return None

    if isinstance(body, dict):
        return body

    try:
        return json.loads(body)
    except json.JSONDecodeError:
        return None


def _is_valid_topic(topic: str) -> bool:
    if topic in ALLOWED_STATIC_TOPICS:
        return True

    if WIKI_TOPIC_PATTERN.match(topic):
        return True

    return False


def _management_client(event: dict):
    request_context = event.get("requestContext", {})
    domain_name = request_context.get("domainName")
    stage = request_context.get("stage")

    if not domain_name or not stage:
        return None

    endpoint_url = f"https://{domain_name}/{stage}"

    return boto3.client(
        "apigatewaymanagementapi",
        endpoint_url=endpoint_url,
    )


def _send_message(event: dict, connection_id: str, payload: dict) -> None:
    client = _management_client(event)

    if client is None:
        print(json.dumps({
            "level": "warning",
            "message": "management_client_unavailable",
            "connection_id": connection_id,
        }))
        return

    try:
        client.post_to_connection(
            ConnectionId=connection_id,
            Data=json.dumps(payload).encode("utf-8"),
        )
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code")

        print(json.dumps({
            "level": "warning",
            "message": "post_to_connection_failed",
            "connection_id": connection_id,
            "error_code": error_code,
            "error": str(exc),
        }))

        if error_code == "GoneException":
            table.delete_item(
                Key={
                    "connection_id": connection_id,
                }
            )


def _get_current_topics(connection_id: str) -> list:
    response = table.get_item(
        Key={
            "connection_id": connection_id,
        }
    )

    item = response.get("Item")

    if not item:
        return ["global"]

    topics = item.get("topics", ["global"])

    if not isinstance(topics, list):
        return ["global"]

    return topics


def _update_topics(connection_id: str, topics: list) -> None:
    table.update_item(
        Key={
            "connection_id": connection_id,
        },
        UpdateExpression="SET topics = :topics",
        ExpressionAttributeValues={
            ":topics": topics,
        },
    )


def lambda_handler(event, context):
    request_context = event.get("requestContext", {})
    connection_id = request_context.get("connectionId")

    if not connection_id:
        print(json.dumps({
            "level": "error",
            "message": "missing_connection_id",
            "request_context": request_context,
        }))
        return _response(400, {"message": "Missing connectionId"})

    message = _parse_body(event)

    if message is None:
        error_payload = {
            "type": "error",
            "message": "Invalid JSON message",
        }
        _send_message(event, connection_id, error_payload)
        return _response(200, error_payload)

    action = message.get("action")
    topic = message.get("topic")

    if action not in {"subscribe", "unsubscribe"}:
        error_payload = {
            "type": "error",
            "message": "Invalid action",
        }
        _send_message(event, connection_id, error_payload)
        return _response(200, error_payload)

    if not isinstance(topic, str) or not _is_valid_topic(topic):
        error_payload = {
            "type": "error",
            "message": "Unsupported topic",
        }
        _send_message(event, connection_id, error_payload)
        return _response(200, error_payload)

    try:
        current_topics = _get_current_topics(connection_id)

        if action == "subscribe":
            updated_topics = sorted(set(current_topics + [topic]))
            status = "subscribed"
        else:
            updated_topics = [current_topic for current_topic in current_topics if current_topic != topic]

            if "global" not in updated_topics:
                updated_topics.insert(0, "global")

            updated_topics = sorted(set(updated_topics))
            status = "unsubscribed"

        _update_topics(connection_id, updated_topics)

        ack_payload = {
            "type": "subscription.ack",
            "topic": topic,
            "status": status,
        }

        _send_message(event, connection_id, ack_payload)

        print(json.dumps({
            "level": "info",
            "message": "websocket_subscription_updated",
            "connection_id": connection_id,
            "action": action,
            "topic": topic,
            "topics": updated_topics,
        }))

        return _response(200, ack_payload)

    except ClientError as exc:
        print(json.dumps({
            "level": "error",
            "message": "websocket_subscription_update_failed",
            "connection_id": connection_id,
            "action": action,
            "topic": topic,
            "error": str(exc),
        }))

        error_payload = {
            "type": "error",
            "message": "Failed to update subscription",
        }

        _send_message(event, connection_id, error_payload)

        return _response(500, error_payload)