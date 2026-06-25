# services/websocket-disconnect-handler/src/handler.py

import json
import os

import boto3
from botocore.exceptions import ClientError


dynamodb = boto3.resource("dynamodb")

TABLE_NAME = os.environ["WEBSOCKET_CONNECTIONS_TABLE_NAME"]

table = dynamodb.Table(TABLE_NAME)


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

    try:
        table.delete_item(
            Key={
                "connection_id": connection_id,
            }
        )

        print(json.dumps({
            "level": "info",
            "message": "websocket_connection_deleted",
            "connection_id": connection_id,
        }))

        return _response(200, {"message": "Disconnected"})

    except ClientError as exc:
        print(json.dumps({
            "level": "error",
            "message": "websocket_connection_delete_failed",
            "connection_id": connection_id,
            "error": str(exc),
        }))
        return _response(500, {"message": "Failed to disconnect"})