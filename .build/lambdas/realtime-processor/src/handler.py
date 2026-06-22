import base64
import json
import logging
import os
import time
from datetime import datetime, timezone

import boto3


LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
AGGREGATES_TABLE_NAME = os.getenv("AGGREGATES_TABLE_NAME")
BROADCAST_QUEUE_URL = os.getenv("BROADCAST_QUEUE_URL")  # optional for later

logger = logging.getLogger()
logger.setLevel(LOG_LEVEL)

dynamodb = boto3.resource("dynamodb")
sqs = boto3.client("sqs")

table = dynamodb.Table(AGGREGATES_TABLE_NAME) if AGGREGATES_TABLE_NAME else None


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _decode_kinesis_record(record: dict) -> dict:
    encoded_data = record["kinesis"]["data"]
    decoded_data = base64.b64decode(encoded_data).decode("utf-8")
    return json.loads(decoded_data)


def lambda_handler(event, context):
    if not AGGREGATES_TABLE_NAME:
        raise RuntimeError("Missing required env var: AGGREGATES_TABLE_NAME")

    records = event.get("Records", [])

    logger.info(json.dumps({
        "message": "realtime_processor_invoked",
        "record_count": len(records),
        "table": AGGREGATES_TABLE_NAME,
        "aws_request_id": getattr(context, "aws_request_id", None),
    }))

    decoded_count = 0
    skipped_count = 0
    first_event_id = None
    first_event_type = None
    first_wiki = None
    first_change_type = None

    for record in records:
        try:
            envelope = _decode_kinesis_record(record)

            event_id = envelope.get("event_id")
            event_type = envelope.get("event_type")
            payload = envelope.get("payload", {})

            if event_type != "wiki.recentchange":
                skipped_count += 1
                logger.warning(json.dumps({
                    "message": "unsupported_event_type",
                    "event_type": event_type,
                    "event_id": event_id,
                }))
                continue

            decoded_count += 1

            if first_event_id is None:
                first_event_id = event_id
                first_event_type = event_type
                first_wiki = payload.get("wiki")
                first_change_type = payload.get("change_type")

        except Exception as exc:
            skipped_count += 1
            logger.exception(json.dumps({
                "message": "record_decode_failed",
                "error": str(exc),
            }))

    now_iso = _utc_now_iso()
    ttl = int(time.time()) + 86400  # keep test item 24h

    # Minimal DynamoDB write to validate permissions and connectivity.
    table.update_item(
        Key={
            "PK": "TEST#REALTIME_PROCESSOR",
            "SK": "KINESIS_CONNECTIVITY"
        },
        UpdateExpression="""
            ADD invocation_count :one, decoded_record_count :decoded
            SET last_seen_at = :now,
                last_event_id = :event_id,
                last_event_type = :event_type,
                last_wiki = :wiki,
                last_change_type = :change_type,
                ttl = :ttl
        """,
        ExpressionAttributeValues={
            ":one": 1,
            ":decoded": decoded_count,
            ":now": now_iso,
            ":event_id": first_event_id or "none",
            ":event_type": first_event_type or "none",
            ":wiki": first_wiki or "none",
            ":change_type": first_change_type or "none",
            ":ttl": ttl,
        },
    )

    logger.info(json.dumps({
        "message": "dynamodb_test_update_success",
        "decoded_count": decoded_count,
        "skipped_count": skipped_count,
        "first_event_id": first_event_id,
        "first_wiki": first_wiki,
        "first_change_type": first_change_type,
    }))

    # Optional: later, if BROADCAST_QUEUE_URL is configured, this also tests SQS SendMessage.
    if BROADCAST_QUEUE_URL and decoded_count > 0:
        sqs.send_message(
            QueueUrl=BROADCAST_QUEUE_URL,
            MessageBody=json.dumps({
                "type": "realtime_processor.test",
                "created_at": now_iso,
                "decoded_count": decoded_count,
                "first_event_id": first_event_id,
            }),
            MessageGroupId="realtime-processor-test",
            MessageDeduplicationId=f"test-{int(time.time())}",
        )

        logger.info(json.dumps({
            "message": "sqs_test_signal_sent",
            "queue_url_configured": True,
        }))

    return {
        "statusCode": 200,
        "decoded_count": decoded_count,
        "skipped_count": skipped_count,
    }