# services/websocket-connect-handler/src/handler.py

import json
import os
import time
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError


dynamodb = boto3.resource("dynamodb")

TABLE_NAME = os.environ["WEBSOCKET_CONNECTIONS_TABLE_NAME"]
CONNECTION_TTL_SECONDS = int(os.environ.get("CONNECTION_TTL_SECONDS", "7200"))
DEFAULT_TOPIC = os.environ.get("DEFAULT_TOPIC", "global")

table = dynamodb.Table(TABLE_NAME)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _response(status_code: int, body: dict) -> dict:
    return {
        "statusCode": status_code,
        "body": json.dumps(body),
    }


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

    now = _now_iso()
    ttl = int(time.time()) + CONNECTION_TTL_SECONDS

    item = {
        "connection_id": connection_id,
        "connected_at": now,
        "client_type": "dashboard",
        "topics": [DEFAULT_TOPIC],
        "ttl": ttl,
    }

    try:
        table.put_item(Item=item)

        print(json.dumps({
            "level": "info",
            "message": "websocket_connection_stored",
            "connection_id": connection_id,
            "topics": item["topics"],
            "ttl": ttl,
        }))

        return _response(200, {"message": "Connected"})

    except ClientError as exc:
        print(json.dumps({
            "level": "error",
            "message": "websocket_connection_store_failed",
            "connection_id": connection_id,
            "error": str(exc),
        }))
        return _response(500, {"message": "Failed to connect"})