import base64
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any


LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
ENVIRONMENT = os.getenv("ENVIRONMENT", "dev")
ALERT_STATE_TABLE_NAME = os.getenv("ALERT_STATE_TABLE_NAME")
SNS_TOPIC_ARN = os.getenv("SNS_TOPIC_ARN")

logger = logging.getLogger()
logger.setLevel(LOG_LEVEL)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _to_iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _decode_kinesis_record(record: dict[str, Any]) -> dict[str, Any]:
    encoded_data = record["kinesis"]["data"]
    decoded_data = base64.b64decode(encoded_data).decode("utf-8")
    return json.loads(decoded_data)


def _extract_alert_candidate(envelope: dict[str, Any]) -> dict[str, Any] | None:
    if envelope.get("event_type") != "wiki.recentchange":
        return None

    payload = envelope.get("payload")
    if not isinstance(payload, dict):
        return None

    event_id = envelope.get("event_id")
    if not event_id:
        return None

    return {
        "event_id": event_id,
        "occurred_at": envelope.get("occurred_at"),
        "wiki": payload.get("wiki", "unknown"),
        "change_type": payload.get("change_type", "unknown"),
        "namespace": payload.get("namespace"),
        "title": payload.get("title"),
        "is_bot": bool(payload.get("user_is_bot", payload.get("bot", False))),
    }


def lambda_handler(event, context):
    records = event.get("Records", [])
    now_iso = _to_iso_z(_utc_now())

    decoded_count = 0
    valid_count = 0
    skipped_count = 0
    samples: list[dict[str, Any]] = []

    logger.info(json.dumps({
        "message": "alert_processor_invoked",
        "environment": ENVIRONMENT,
        "record_count": len(records),
        "alert_state_table": ALERT_STATE_TABLE_NAME,
        "sns_topic_configured": bool(SNS_TOPIC_ARN),
        "aws_request_id": getattr(context, "aws_request_id", None),
        "processed_at": now_iso,
    }))

    for record in records:
        try:
            envelope = _decode_kinesis_record(record)
            decoded_count += 1

            candidate = _extract_alert_candidate(envelope)
            if candidate is None:
                skipped_count += 1
                continue

            valid_count += 1

            if len(samples) < 3:
                samples.append(candidate)

        except Exception as exc:
            skipped_count += 1
            logger.exception(json.dumps({
                "message": "alert_processor_record_failed",
                "error": str(exc),
            }))

    logger.info(json.dumps({
        "message": "alert_processor_batch_processed",
        "input_records": len(records),
        "decoded_count": decoded_count,
        "valid_count": valid_count,
        "skipped_count": skipped_count,
        "sample_candidates": samples,
    }))

    return {
        "statusCode": 200,
        "input_records": len(records),
        "decoded_count": decoded_count,
        "valid_count": valid_count,
        "skipped_count": skipped_count,
    }