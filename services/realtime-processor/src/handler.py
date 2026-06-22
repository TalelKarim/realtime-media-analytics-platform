import base64
import hashlib
import json
import logging
import os
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

import boto3


LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

AGGREGATES_TABLE_NAME = os.getenv("AGGREGATES_TABLE_NAME")
BROADCAST_QUEUE_URL = os.getenv("BROADCAST_QUEUE_URL")

AGGREGATION_WINDOW_SECONDS = int(os.getenv("AGGREGATION_WINDOW_SECONDS", "60"))
BROADCAST_WINDOW_SECONDS = int(os.getenv("BROADCAST_WINDOW_SECONDS", "5"))
GLOBAL_ACTIVITY_SHARD_COUNT = int(os.getenv("GLOBAL_ACTIVITY_SHARD_COUNT", "10"))
AGGREGATE_TTL_DAYS = int(os.getenv("AGGREGATE_TTL_DAYS", "2"))

logger = logging.getLogger()
logger.setLevel(LOG_LEVEL)

dynamodb = boto3.resource("dynamodb")
sqs = boto3.client("sqs")

table = dynamodb.Table(AGGREGATES_TABLE_NAME) if AGGREGATES_TABLE_NAME else None


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


def _decode_kinesis_record(record: dict[str, Any]) -> dict[str, Any]:
    encoded_data = record["kinesis"]["data"]
    decoded_data = base64.b64decode(encoded_data).decode("utf-8")
    return json.loads(decoded_data)


def _compute_shard_id(event_id: str, shard_count: int) -> int:
    digest = hashlib.sha256(event_id.encode("utf-8")).hexdigest()
    return int(digest[:8], 16) % shard_count


def _safe_str(value: Any, default: str = "unknown") -> str:
    if value is None:
        return default

    text = str(value).strip()
    if not text:
        return default

    return text.replace("#", "_")


def _add_counter(
    counters: dict[tuple[str, str], int],
    metric_key: str,
    window_key: str,
    amount: int = 1,
) -> None:
    counters[(metric_key, window_key)] += amount


def _extract_normalized_event(envelope: dict[str, Any]) -> dict[str, Any] | None:
    event_type = envelope.get("event_type")
    if event_type != "wiki.recentchange":
        return None

    payload = envelope.get("payload")
    if not isinstance(payload, dict):
        return None

    event_id = envelope.get("event_id")
    if not event_id:
        return None

    occurred_at = envelope.get("occurred_at") or payload.get("occurred_at")
    occurred_dt = _parse_iso_datetime(occurred_at)

    wiki = _safe_str(payload.get("wiki"))
    change_type = _safe_str(payload.get("change_type"))
    namespace = payload.get("namespace")
    namespace_key = _safe_str(namespace)
    title = _safe_str(payload.get("title"))

    # Support both possible contract names:
    # current collector uses "bot"; target docs may use "user_is_bot".
    bot = payload.get("user_is_bot", payload.get("bot", False))
    is_bot = bool(bot)

    return {
        "event_id": event_id,
        "occurred_dt": occurred_dt,
        "wiki": wiki,
        "change_type": change_type,
        "namespace": namespace,
        "namespace_key": namespace_key,
        "title": title,
        "is_bot": is_bot,
    }


def _build_counters(events: list[dict[str, Any]]) -> tuple[dict[tuple[str, str], int], set[str]]:
    counters: dict[tuple[str, str], int] = defaultdict(int)
    aggregation_windows: set[str] = set()

    for event in events:
        window_start = _floor_time(event["occurred_dt"], AGGREGATION_WINDOW_SECONDS)
        window_start_iso = _to_iso_z(window_start)
        window_key = f"WINDOW#{window_start_iso}"

        aggregation_windows.add(window_start_iso)

        shard_id = _compute_shard_id(
            event_id=event["event_id"],
            shard_count=GLOBAL_ACTIVITY_SHARD_COUNT,
        )

        # 1. Global activity, write-sharded.
        _add_counter(
            counters,
            metric_key=f"METRIC#GLOBAL_ACTIVITY#SHARD#{shard_id}",
            window_key=window_key,
        )

        # 2. Activity by wiki.
        _add_counter(
            counters,
            metric_key=f"METRIC#WIKI_ACTIVITY#WIKI#{event['wiki']}",
            window_key=window_key,
        )

        # 3. Distribution by change type.
        _add_counter(
            counters,
            metric_key=f"METRIC#CHANGE_TYPE#TYPE#{event['change_type']}",
            window_key=window_key,
        )

        # 4. Bot vs human.
        _add_counter(
            counters,
            metric_key=f"METRIC#BOT_ACTIVITY#BOT#{str(event['is_bot']).lower()}",
            window_key=window_key,
        )

        # 5. Namespace distribution.
        _add_counter(
            counters,
            metric_key=f"METRIC#NAMESPACE#NS#{event['namespace_key']}",
            window_key=window_key,
        )

    return counters, aggregation_windows


def _update_counter(metric_key: str, window_key: str, count: int, now_iso: str, ttl: int) -> None:
    table.update_item(
        Key={
            "metric_key": metric_key,
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
            "#ttl": "ttl",
        },
        ExpressionAttributeValues={
            ":count": count,
            ":now": now_iso,
            ":ttl": ttl,
        },
    )


def _send_broadcast_signal(aggregation_windows: set[str], now: datetime) -> bool:
    if not BROADCAST_QUEUE_URL:
        logger.info(json.dumps({
            "message": "broadcast_signal_skipped",
            "reason": "missing_broadcast_queue_url",
        }))
        return False

    if not aggregation_windows:
        logger.info(json.dumps({
            "message": "broadcast_signal_skipped",
            "reason": "no_aggregation_windows",
        }))
        return False

    broadcast_window_start = _floor_time(now, BROADCAST_WINDOW_SECONDS)
    broadcast_window_iso = _to_iso_z(broadcast_window_start)

    message_body = {
        "message_type": "aggregates.updated",
        "source": "realtime-processor",
        "created_at": _to_iso_z(now),
        "broadcast_window": broadcast_window_iso,
        "aggregation_windows": sorted(aggregation_windows),
    }

    deduplication_id = f"BROADCAST#{broadcast_window_iso}"

    try:
        sqs.send_message(
            QueueUrl=BROADCAST_QUEUE_URL,
            MessageBody=json.dumps(message_body),
            MessageGroupId="realtime-broadcast",
            MessageDeduplicationId=deduplication_id,
        )

        logger.info(json.dumps({
            "message": "broadcast_signal_sent",
            "broadcast_window": broadcast_window_iso,
            "aggregation_windows": sorted(aggregation_windows),
            "deduplication_id": deduplication_id,
        }))

        return True

    except Exception as exc:
        # Important:
        # We do NOT fail the Lambda after DynamoDB writes succeeded.
        # Otherwise Kinesis would retry the batch and could double-count DynamoDB counters.
        logger.exception(json.dumps({
            "message": "broadcast_signal_failed",
            "error": str(exc),
        }))
        return False


def lambda_handler(event, context):
    if not AGGREGATES_TABLE_NAME:
        raise RuntimeError("Missing required env var: AGGREGATES_TABLE_NAME")

    if table is None:
        raise RuntimeError("DynamoDB table client is not initialized")

    records = event.get("Records", [])
    now = _utc_now()
    now_iso = _to_iso_z(now)
    ttl = int(time.time()) + (AGGREGATE_TTL_DAYS * 86400)

    logger.info(json.dumps({
        "message": "realtime_processor_invoked",
        "record_count": len(records),
        "table": AGGREGATES_TABLE_NAME,
        "aws_request_id": getattr(context, "aws_request_id", None),
    }))

    decoded_count = 0
    valid_count = 0
    skipped_count = 0

    normalized_events: list[dict[str, Any]] = []

    for record in records:
        try:
            envelope = _decode_kinesis_record(record)
            decoded_count += 1

            normalized_event = _extract_normalized_event(envelope)
            if normalized_event is None:
                skipped_count += 1
                continue

            valid_count += 1
            normalized_events.append(normalized_event)

        except Exception as exc:
            skipped_count += 1
            logger.exception(json.dumps({
                "message": "record_processing_failed",
                "error": str(exc),
            }))

    counters, aggregation_windows = _build_counters(normalized_events)

    dynamodb_update_count = 0

    for (metric_key, window_key), count in counters.items():
        _update_counter(
            metric_key=metric_key,
            window_key=window_key,
            count=count,
            now_iso=now_iso,
            ttl=ttl,
        )
        dynamodb_update_count += 1

    broadcast_signal_sent = False
    if valid_count > 0 and dynamodb_update_count > 0:
        broadcast_signal_sent = _send_broadcast_signal(aggregation_windows, now)

    logger.info(json.dumps({
        "message": "realtime_processor_batch_processed",
        "input_records": len(records),
        "decoded_count": decoded_count,
        "valid_count": valid_count,
        "skipped_count": skipped_count,
        "aggregation_windows": sorted(aggregation_windows),
        "dynamodb_update_count": dynamodb_update_count,
        "broadcast_signal_sent": broadcast_signal_sent,
        "finops_write_reduction": {
            "events": valid_count,
            "dynamodb_updates": dynamodb_update_count,
        },
    }))

    return {
        "statusCode": 200,
        "input_records": len(records),
        "decoded_count": decoded_count,
        "valid_count": valid_count,
        "skipped_count": skipped_count,
        "dynamodb_update_count": dynamodb_update_count,
        "broadcast_signal_sent": broadcast_signal_sent,
    }