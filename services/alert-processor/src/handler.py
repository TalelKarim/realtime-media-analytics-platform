import base64
import json
import logging
import os
import time
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import boto3
from botocore.exceptions import ClientError


LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
ENVIRONMENT = os.getenv("ENVIRONMENT", "dev")

ALERT_STATE_TABLE_NAME = os.getenv("ALERT_STATE_TABLE_NAME")
SNS_TOPIC_ARN = os.getenv("SNS_TOPIC_ARN")

ALERT_ENABLED = os.getenv("ALERT_ENABLED", "true").lower() == "true"
ALERT_WINDOW_SECONDS = int(os.getenv("ALERT_WINDOW_SECONDS", "60"))
ALERT_TTL_DAYS = int(os.getenv("ALERT_TTL_DAYS", "2"))
TTL_ATTRIBUTE_NAME = os.getenv("TTL_ATTRIBUTE_NAME", "ttl")

GLOBAL_ACTIVITY_THRESHOLD = int(os.getenv("GLOBAL_ACTIVITY_THRESHOLD", "20"))
BOT_ACTIVITY_THRESHOLD = int(os.getenv("BOT_ACTIVITY_THRESHOLD", "10"))
CATEGORIZE_ACTIVITY_THRESHOLD = int(os.getenv("CATEGORIZE_ACTIVITY_THRESHOLD", "20"))

logger = logging.getLogger()
logger.setLevel(LOG_LEVEL)

dynamodb = boto3.resource("dynamodb")
sns = boto3.client("sns")

table = dynamodb.Table(ALERT_STATE_TABLE_NAME) if ALERT_STATE_TABLE_NAME else None


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _to_iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_iso_datetime(value: str | None) -> datetime:
    if not value:
        return _utc_now()

    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        logger.warning(json.dumps({
            "message": "invalid_occurred_at_fallback_to_now",
            "occurred_at": value,
        }))
        return _utc_now()


def _floor_time(dt: datetime, window_seconds: int) -> datetime:
    epoch_seconds = int(dt.timestamp())
    floored = epoch_seconds - (epoch_seconds % window_seconds)
    return datetime.fromtimestamp(floored, tz=timezone.utc)


def _safe_str(value: Any, default: str = "unknown") -> str:
    if value is None:
        return default

    text = str(value).strip()
    if not text:
        return default

    return text.replace("#", "_")


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

    occurred_at = envelope.get("occurred_at") or payload.get("occurred_at")
    occurred_dt = _parse_iso_datetime(occurred_at)

    bot = payload.get("user_is_bot", payload.get("bot", False))

    return {
        "event_id": event_id,
        "occurred_dt": occurred_dt,
        "occurred_at": occurred_at,
        "wiki": _safe_str(payload.get("wiki")),
        "change_type": _safe_str(payload.get("change_type")),
        "namespace": payload.get("namespace"),
        "title": payload.get("title"),
        "is_bot": bool(bot),
    }


def _add_counter(
    counters: dict[tuple[str, str], int],
    alert_key: str,
    window_key: str,
    amount: int = 1,
) -> None:
    counters[(alert_key, window_key)] += amount


def _build_alert_counters(
    candidates: list[dict[str, Any]],
) -> tuple[dict[tuple[str, str], int], set[str]]:
    counters: dict[tuple[str, str], int] = defaultdict(int)
    windows: set[str] = set()

    for candidate in candidates:
        window_start = _floor_time(candidate["occurred_dt"], ALERT_WINDOW_SECONDS)
        window_start_iso = _to_iso_z(window_start)
        window_key = f"WINDOW#{window_start_iso}"

        windows.add(window_start_iso)

        # 1. Global activity alert counter.
        _add_counter(
            counters,
            alert_key="ALERT#GLOBAL_ACTIVITY",
            window_key=window_key,
        )

        # 2. Bot activity alert counter.
        if candidate["is_bot"]:
            _add_counter(
                counters,
                alert_key="ALERT#BOT_ACTIVITY#BOT#true",
                window_key=window_key,
            )

        # 3. Categorize activity alert counter.
        if candidate["change_type"] == "categorize":
            _add_counter(
                counters,
                alert_key="ALERT#CHANGE_TYPE#TYPE#categorize",
                window_key=window_key,
            )

    return counters, windows


def _threshold_for_alert(alert_key: str) -> int | None:
    if alert_key == "ALERT#GLOBAL_ACTIVITY":
        return GLOBAL_ACTIVITY_THRESHOLD

    if alert_key == "ALERT#BOT_ACTIVITY#BOT#true":
        return BOT_ACTIVITY_THRESHOLD

    if alert_key == "ALERT#CHANGE_TYPE#TYPE#categorize":
        return CATEGORIZE_ACTIVITY_THRESHOLD

    return None


def _update_alert_counter(
    alert_key: str,
    window_key: str,
    count: int,
    now_iso: str,
    ttl: int,
) -> int:
    response = table.update_item(
        Key={
            "alert_key": alert_key,
            "window_key": window_key,
        },
        UpdateExpression="""
            ADD #event_count :count
            SET #last_updated_at = :now,
                #ttl = :ttl
        """,
        ExpressionAttributeNames={
            "#event_count": "event_count",
            "#last_updated_at": "last_updated_at",
            "#ttl": TTL_ATTRIBUTE_NAME,
        },
        ExpressionAttributeValues={
            ":count": count,
            ":now": now_iso,
            ":ttl": ttl,
        },
        ReturnValues="UPDATED_NEW",
    )

    updated_count = response["Attributes"].get("event_count", 0)

    if isinstance(updated_count, Decimal):
        return int(updated_count)

    return int(updated_count)


def _reserve_alert_once(
    alert_key: str,
    window_key: str,
    now_iso: str,
) -> bool:
    try:
        table.update_item(
            Key={
                "alert_key": alert_key,
                "window_key": window_key,
            },
            UpdateExpression="""
                SET #alert_status = :publishing,
                    #alert_reserved_at = :now
            """,
            ConditionExpression="""
                attribute_not_exists(#alert_status)
            """,
            ExpressionAttributeNames={
                "#alert_status": "alert_status",
                "#alert_reserved_at": "alert_reserved_at",
            },
            ExpressionAttributeValues={
                ":publishing": "PUBLISHING",
                ":now": now_iso,
            },
        )
        return True

    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
            return False
        raise


def _mark_alert_status(
    alert_key: str,
    window_key: str,
    status: str,
    now_iso: str,
    error: str | None = None,
) -> None:
    expression_names = {
        "#alert_status": "alert_status",
        "#alert_status_updated_at": "alert_status_updated_at",
    }

    expression_values = {
        ":status": status,
        ":now": now_iso,
    }

    update_expression = """
        SET #alert_status = :status,
            #alert_status_updated_at = :now
    """

    if status == "SENT":
        expression_names["#alert_sent_at"] = "alert_sent_at"
        expression_values[":sent_at"] = now_iso
        update_expression += ", #alert_sent_at = :sent_at"

    if error is not None:
        expression_names["#alert_error"] = "alert_error"
        expression_values[":error"] = error[:500]
        update_expression += ", #alert_error = :error"

    table.update_item(
        Key={
            "alert_key": alert_key,
            "window_key": window_key,
        },
        UpdateExpression=update_expression,
        ExpressionAttributeNames=expression_names,
        ExpressionAttributeValues=expression_values,
    )


def _publish_alert(
    alert_key: str,
    window_key: str,
    current_count: int,
    threshold: int,
    now_iso: str,
) -> None:
    if not SNS_TOPIC_ARN:
        raise RuntimeError("Missing required env var: SNS_TOPIC_ARN")

    message = {
        "message_type": "realtime.alert.triggered",
        "source": "alert-processor",
        "environment": ENVIRONMENT,
        "alert_key": alert_key,
        "window_key": window_key,
        "current_count": current_count,
        "threshold": threshold,
        "created_at": now_iso,
    }

    subject = f"[{ENVIRONMENT}] Realtime alert {alert_key}"
    subject = subject[:100]

    sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=subject,
        Message=json.dumps(message, indent=2),
    )


def lambda_handler(event, context):
    if not ALERT_STATE_TABLE_NAME:
        raise RuntimeError("Missing required env var: ALERT_STATE_TABLE_NAME")

    if table is None:
        raise RuntimeError("DynamoDB table client is not initialized")

    records = event.get("Records", [])

    now = _utc_now()
    now_iso = _to_iso_z(now)
    ttl = int(time.time()) + (ALERT_TTL_DAYS * 86400)

    decoded_count = 0
    valid_count = 0
    skipped_count = 0

    candidates: list[dict[str, Any]] = []

    logger.info(json.dumps({
        "message": "alert_processor_invoked",
        "environment": ENVIRONMENT,
        "record_count": len(records),
        "alert_state_table": ALERT_STATE_TABLE_NAME,
        "sns_topic_configured": bool(SNS_TOPIC_ARN),
        "alert_enabled": ALERT_ENABLED,
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
            candidates.append(candidate)

        except Exception as exc:
            skipped_count += 1
            logger.exception(json.dumps({
                "message": "alert_processor_record_failed",
                "error": str(exc),
            }))

    counters, alert_windows = _build_alert_counters(candidates)

    dynamodb_update_count = 0
    threshold_breaches = 0
    alerts_published = 0
    alerts_suppressed = 0
    alerts_failed = 0

    for (alert_key, window_key), count in counters.items():
        current_count = _update_alert_counter(
            alert_key=alert_key,
            window_key=window_key,
            count=count,
            now_iso=now_iso,
            ttl=ttl,
        )
        dynamodb_update_count += 1

        threshold = _threshold_for_alert(alert_key)
        if threshold is None:
            continue

        if current_count < threshold:
            continue

        threshold_breaches += 1

        if not ALERT_ENABLED:
            logger.info(json.dumps({
                "message": "alert_threshold_breached_but_disabled",
                "alert_key": alert_key,
                "window_key": window_key,
                "current_count": current_count,
                "threshold": threshold,
            }))
            continue

        reserved = _reserve_alert_once(
            alert_key=alert_key,
            window_key=window_key,
            now_iso=now_iso,
        )

        if not reserved:
            alerts_suppressed += 1
            continue

        try:
            _publish_alert(
                alert_key=alert_key,
                window_key=window_key,
                current_count=current_count,
                threshold=threshold,
                now_iso=now_iso,
            )
            _mark_alert_status(
                alert_key=alert_key,
                window_key=window_key,
                status="SENT",
                now_iso=now_iso,
            )
            alerts_published += 1

            logger.warning(json.dumps({
                "message": "alert_published",
                "alert_key": alert_key,
                "window_key": window_key,
                "current_count": current_count,
                "threshold": threshold,
            }))

        except Exception as exc:
            alerts_failed += 1

            _mark_alert_status(
                alert_key=alert_key,
                window_key=window_key,
                status="FAILED",
                now_iso=now_iso,
                error=str(exc),
            )

            logger.exception(json.dumps({
                "message": "alert_publish_failed",
                "alert_key": alert_key,
                "window_key": window_key,
                "current_count": current_count,
                "threshold": threshold,
                "error": str(exc),
            }))

    sample_candidates = [
        {
            "event_id": c["event_id"],
            "occurred_at": c["occurred_at"],
            "wiki": c["wiki"],
            "change_type": c["change_type"],
            "namespace": c["namespace"],
            "title": c["title"],
            "is_bot": c["is_bot"],
        }
        for c in candidates[:3]
    ]

    logger.info(json.dumps({
        "message": "alert_processor_batch_processed",
        "input_records": len(records),
        "decoded_count": decoded_count,
        "valid_count": valid_count,
        "skipped_count": skipped_count,
        "alert_windows": sorted(alert_windows),
        "dynamodb_update_count": dynamodb_update_count,
        "threshold_breaches": threshold_breaches,
        "alerts_published": alerts_published,
        "alerts_suppressed": alerts_suppressed,
        "alerts_failed": alerts_failed,
        "sample_candidates": sample_candidates,
    }))

    return {
        "statusCode": 200,
        "input_records": len(records),
        "decoded_count": decoded_count,
        "valid_count": valid_count,
        "skipped_count": skipped_count,
        "dynamodb_update_count": dynamodb_update_count,
        "threshold_breaches": threshold_breaches,
        "alerts_published": alerts_published,
        "alerts_suppressed": alerts_suppressed,
        "alerts_failed": alerts_failed,
    }