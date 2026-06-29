import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List


LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _log(level: str, message: str, **kwargs: Any) -> None:
    log_event = {
        "timestamp": _now_iso(),
        "level": level,
        "message": message,
        **kwargs,
    }

    print(json.dumps(log_event, default=str))


def _parse_sqs_body(record: Dict[str, Any]) -> Dict[str, Any]:
    body = record.get("body")

    if not body:
        raise ValueError("SQS record body is empty")

    try:
        return json.loads(body)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid SQS JSON body: {exc}") from exc


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, List[Dict[str, str]]]:
    records = event.get("Records", [])
    batch_item_failures: List[Dict[str, str]] = []

    _log(
        "INFO",
        "broadcaster_batch_received",
        record_count=len(records),
        aws_request_id=getattr(context, "aws_request_id", None),
    )

    for record in records:
        message_id = record.get("messageId", "unknown")

        try:
            message = _parse_sqs_body(record)

            _log(
                "INFO",
                "broadcast_signal_received",
                message_id=message_id,
                message_type=message.get("message_type"),
                source=message.get("source"),
                created_at=message.get("created_at"),
                broadcast_window=message.get("broadcast_window"),
                aggregation_windows=message.get("aggregation_windows", []),
            )

        except Exception as exc:
            _log(
                "ERROR",
                "broadcast_signal_processing_failed",
                message_id=message_id,
                error_type=type(exc).__name__,
                error_message=str(exc),
            )

            batch_item_failures.append({
                "itemIdentifier": message_id
            })

    _log(
        "INFO",
        "broadcaster_batch_completed",
        failed_count=len(batch_item_failures),
        success_count=len(records) - len(batch_item_failures),
    )

    return {
        "batchItemFailures": batch_item_failures
    }