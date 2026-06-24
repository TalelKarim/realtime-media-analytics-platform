import base64
import hashlib
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any

import boto3


LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

AGGREGATES_TABLE_NAME = os.getenv("AGGREGATES_TABLE_NAME")
BROADCAST_QUEUE_URL = os.getenv("BROADCAST_QUEUE_URL")

AGGREGATION_WINDOW_SECONDS = int(os.getenv("AGGREGATION_WINDOW_SECONDS", "60"))
BROADCAST_WINDOW_SECONDS = int(os.getenv("BROADCAST_WINDOW_SECONDS", "5"))
GLOBAL_ACTIVITY_SHARD_COUNT = int(os.getenv("GLOBAL_ACTIVITY_SHARD_COUNT", "10"))
TOP_METRIC_SHARD_COUNT = int(os.getenv("TOP_METRIC_SHARD_COUNT", "10"))
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


def _hash_int(value: str) -> int:
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()
    return int(digest[:8], 16)


def _hash_to_shard(value: str, shard_count: int) -> int:
    return _hash_int(value) % shard_count


def _hash_token(value: str, length: int = 12) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:length]


def _compute_shard_id(event_id: str, shard_count: int) -> int:
    return _hash_to_shard(event_id, shard_count)


def _safe_str(value: Any, default: str = "unknown") -> str:
    if value is None:
        return default

    text = str(value).strip()
    if not text:
        return default

    return text.replace("#", "_")


def _raw_str(value: Any, default: str = "unknown") -> str:
    if value is None:
        return default

    text = str(value).strip()
    if not text:
        return default

    return text


def _is_namespace_zero(namespace: Any) -> bool:
    try:
        return int(namespace) == 0
    except Exception:
        return False


def _add_counter(
    counters: dict[tuple[str, str], dict[str, Any]],
    metric_key: str,
    window_key: str,
    amount: int = 1,
    attrs: dict[str, Any] | None = None,
) -> None:
    key = (metric_key, window_key)

    if key not in counters:
        counters[key] = {
            "count": 0,
            "attrs": {},
        }

    counters[key]["count"] += amount

    if attrs:
        counters[key]["attrs"].update(attrs)


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

    title_raw = _raw_str(payload.get("title"))
    title_key = _safe_str(title_raw)

    title_url = payload.get("title_url")
    title_url = _raw_str(title_url, default="") if title_url else None

    # Support both possible contract names:
    # target contract uses "user_is_bot"; older collector versions may use "bot".
    bot = payload.get("user_is_bot", payload.get("bot", False))
    is_bot = bool(bot)

    return {
        "event_id": event_id,
        "occurred_dt": occurred_dt,
        "wiki": wiki,
        "change_type": change_type,
        "namespace": namespace,
        "namespace_key": namespace_key,
        "title": title_raw,
        "title_key": title_key,
        "title_url": title_url,
        "is_bot": is_bot,
    }


def _build_counters(
    events: list[dict[str, Any]],
) -> tuple[dict[tuple[str, str], dict[str, Any]], set[str]]:
    counters: dict[tuple[str, str], dict[str, Any]] = {}
    aggregation_windows: set[str] = set()

    for event in events:
        window_start = _floor_time(event["occurred_dt"], AGGREGATION_WINDOW_SECONDS)
        window_start_iso = _to_iso_z(window_start)
        window_key = f"WINDOW#{window_start_iso}"

        aggregation_windows.add(window_start_iso)

        common_attrs = {
            "window_start": window_start_iso,
        }

        # 1. Global activity, write-sharded by event_id.
        global_shard_id = _compute_shard_id(
            event_id=event["event_id"],
            shard_count=GLOBAL_ACTIVITY_SHARD_COUNT,
        )

        _add_counter(
            counters,
            metric_key=f"METRIC#GLOBAL_ACTIVITY#SHARD#{global_shard_id}",
            window_key=window_key,
            attrs=common_attrs,
        )

        # 2. Activity by known wiki, useful for topic wiki:{wiki}.
        _add_counter(
            counters,
            metric_key=f"METRIC#WIKI_ACTIVITY#WIKI#{event['wiki']}",
            window_key=window_key,
            attrs={
                **common_attrs,
                "wiki": event["wiki"],
            },
        )

        # 3. Top wikis read model, sharded by wiki.
        top_wiki_shard_id = _hash_to_shard(
            value=event["wiki"],
            shard_count=TOP_METRIC_SHARD_COUNT,
        )

        _add_counter(
            counters,
            metric_key=f"METRIC#TOP_WIKIS#SHARD#{top_wiki_shard_id}",
            window_key=f"{window_key}#WIKI#{event['wiki']}",
            attrs={
                **common_attrs,
                "wiki": event["wiki"],
            },
        )

        # 4. Distribution by change type.
        _add_counter(
            counters,
            metric_key=f"METRIC#CHANGE_TYPE#TYPE#{event['change_type']}",
            window_key=window_key,
            attrs={
                **common_attrs,
                "change_type": event["change_type"],
            },
        )

        # 5. Bot vs human.
        is_bot_text = str(event["is_bot"]).lower()

        _add_counter(
            counters,
            metric_key=f"METRIC#BOT_ACTIVITY#BOT#{is_bot_text}",
            window_key=window_key,
            attrs={
                **common_attrs,
                "is_bot": event["is_bot"],
            },
        )

        # 6. Namespace distribution.
        _add_counter(
            counters,
            metric_key=f"METRIC#NAMESPACE#NS#{event['namespace_key']}",
            window_key=window_key,
            attrs={
                **common_attrs,
                "namespace": event["namespace_key"],
            },
        )

        # 7. Top pages read model.
        # Only namespace 0 represents article/content pages.
        if _is_namespace_zero(event["namespace"]):
            page_identity = f"{event['wiki']}#{event['title_key']}"
            page_hash = _hash_token(page_identity)
            top_page_shard_id = _hash_to_shard(
                value=page_identity,
                shard_count=TOP_METRIC_SHARD_COUNT,
            )

            _add_counter(
                counters,
                metric_key=f"METRIC#TOP_PAGES#SHARD#{top_page_shard_id}",
                window_key=f"{window_key}#WIKI#{event['wiki']}#TITLE#{page_hash}",
                attrs={
                    **common_attrs,
                    "wiki": event["wiki"],
                    "title": event["title"],
                    "title_url": event["title_url"],
                    "namespace": event["namespace_key"],
                    "last_change_type": event["change_type"],
                    "last_seen_at": _to_iso_z(event["occurred_dt"]),
                },
            )

    return counters, aggregation_windows


def _update_counter(
    metric_key: str,
    window_key: str,
    count: int,
    now_iso: str,
    ttl: int,
    attrs: dict[str, Any] | None = None,
) -> None:
    expression_attribute_names = {
        "#event_count": "event_count",
        "#last_updated_at": "last_updated_at",
        "#ttl": "ttl",
    }

    expression_attribute_values = {
        ":count": count,
        ":now": now_iso,
        ":ttl": ttl,
    }

    set_expressions = [
        "#last_updated_at = :now",
        "#ttl = :ttl",
    ]

    if attrs:
        for index, (attr_name, attr_value) in enumerate(attrs.items()):
            if attr_value is None:
                continue

            name_token = f"#attr_{index}"
            value_token = f":attr_{index}"

            expression_attribute_names[name_token] = attr_name
            expression_attribute_values[value_token] = attr_value
            set_expressions.append(f"{name_token} = {value_token}")

    update_expression = f"""
        ADD #event_count :count
        SET {", ".join(set_expressions)}
    """

    table.update_item(
        Key={
            "metric_key": metric_key,
            "window_key": window_key,
        },
        UpdateExpression=update_expression,
        ExpressionAttributeNames=expression_attribute_names,
        ExpressionAttributeValues=expression_attribute_values,
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

    for (metric_key, window_key), counter_data in counters.items():
        _update_counter(
            metric_key=metric_key,
            window_key=window_key,
            count=counter_data["count"],
            now_iso=now_iso,
            ttl=ttl,
            attrs=counter_data.get("attrs", {}),
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
