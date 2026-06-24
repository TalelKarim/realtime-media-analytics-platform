from __future__ import annotations

import base64
import json
import logging
import math
import os
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from statistics import mean, pstdev
from typing import Any, DefaultDict, Dict, Iterable, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError


# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------

UTC = timezone.utc

ALERT_STATE_TABLE_NAME = os.environ.get("ALERT_STATE_TABLE_NAME", "")
ALERT_TOPIC_ARN = os.environ.get("ALERT_TOPIC_ARN", "")

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
ALERT_TTL_MINUTES = int(os.environ.get("ALERT_TTL_MINUTES", "35"))
EVALUATION_DELAY_SECONDS = int(os.environ.get("EVALUATION_DELAY_SECONDS", "10"))

BASELINE_WINDOW_MINUTES = int(os.environ.get("BASELINE_WINDOW_MINUTES", "30"))
MODERATION_WINDOW_MINUTES = int(os.environ.get("MODERATION_WINDOW_MINUTES", "5"))
MIN_BASELINE_POINTS = int(os.environ.get("MIN_BASELINE_POINTS", "10"))
MIN_MODERATION_BASELINE_POINTS = int(os.environ.get("MIN_MODERATION_BASELINE_POINTS", "3"))

GLOBAL_Z_THRESHOLD = float(os.environ.get("GLOBAL_Z_THRESHOLD", "2.0"))
WIKI_Z_THRESHOLD = float(os.environ.get("WIKI_Z_THRESHOLD", "2.0"))
MODERATION_BURST_RATIO_THRESHOLD = float(os.environ.get("MODERATION_BURST_RATIO_THRESHOLD", "3.0"))

GLOBAL_MIN_COUNT = int(os.environ.get("GLOBAL_MIN_COUNT", "50"))
WIKI_MIN_COUNT = int(os.environ.get("WIKI_MIN_COUNT", "30"))
DELETE_MIN_COUNT = int(os.environ.get("DELETE_MIN_COUNT", "5"))
BLOCK_MIN_COUNT = int(os.environ.get("BLOCK_MIN_COUNT", "3"))

MAX_WIKI_ALERT_KEYS_PER_INVOCATION = int(os.environ.get("MAX_WIKI_ALERT_KEYS_PER_INVOCATION", "20"))

logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO))
logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))

_dynamodb = boto3.resource("dynamodb")
_sns = boto3.client("sns")
_alert_table = _dynamodb.Table(ALERT_STATE_TABLE_NAME) if ALERT_STATE_TABLE_NAME else None


# -----------------------------------------------------------------------------
# Types
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class NormalizedEvent:
    event_id: str
    occurred_at: datetime
    window_start: datetime
    wiki: str
    change_type: str
    log_type: Optional[str]
    log_action: Optional[str]
    user_is_bot: bool


CounterKey = Tuple[str, str]
CounterValue = Dict[str, Any]
Counters = DefaultDict[CounterKey, CounterValue]


# -----------------------------------------------------------------------------
# Generic helpers
# -----------------------------------------------------------------------------

def _now() -> datetime:
    return datetime.now(UTC)


def _to_iso_z(dt: datetime) -> str:
    return dt.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_iso_datetime(value: Any) -> Optional[datetime]:
    if not isinstance(value, str) or not value.strip():
        return None

    raw = value.strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"

    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)

    return parsed.astimezone(UTC)


def _floor_minute(dt: datetime) -> datetime:
    return dt.astimezone(UTC).replace(second=0, microsecond=0)


def _window_key(dt: datetime) -> str:
    return f"WINDOW#{_to_iso_z(_floor_minute(dt))}"


def _parse_window_key(window_key: str) -> Optional[datetime]:
    if not window_key.startswith("WINDOW#"):
        return None
    return _parse_iso_datetime(window_key[len("WINDOW#"):])


def _ttl_epoch(window_start: datetime) -> int:
    return int((window_start + timedelta(minutes=ALERT_TTL_MINUTES)).timestamp())


def _as_decimal(value: Any) -> Decimal:
    if isinstance(value, Decimal):
        return value
    if isinstance(value, float):
        return Decimal(str(round(value, 6)))
    return Decimal(str(value))


def _as_int(value: Any, default: int = 0) -> int:
    if value is None:
        return default
    if isinstance(value, Decimal):
        return int(value)
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _as_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    if isinstance(value, Decimal):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_str(value: Any, default: str = "unknown") -> str:
    if value is None:
        return default
    text = str(value).strip()
    return text if text else default


def _normalize_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"true", "1", "yes", "y"}
    return bool(value)


def _json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        if value % 1 == 0:
            return int(value)
        return float(value)
    if isinstance(value, datetime):
        return _to_iso_z(value)
    return str(value)


def _log(message: str, **fields: Any) -> None:
    payload = {"message": message, **fields}
    logger.info(json.dumps(payload, default=_json_default, separators=(",", ":")))


def _log_warning(message: str, **fields: Any) -> None:
    payload = {"message": message, **fields}
    logger.warning(json.dumps(payload, default=_json_default, separators=(",", ":")))


def _log_error(message: str, **fields: Any) -> None:
    payload = {"message": message, **fields}
    logger.error(json.dumps(payload, default=_json_default, separators=(",", ":")))


# -----------------------------------------------------------------------------
# Kinesis decoding and event extraction
# -----------------------------------------------------------------------------

def _decode_kinesis_record(record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        encoded = record["kinesis"]["data"]
        raw_bytes = base64.b64decode(encoded)
        return json.loads(raw_bytes.decode("utf-8"))
    except Exception as exc:  # noqa: BLE001 - log and skip malformed records
        _log_warning("kinesis_record_decode_failed", error=str(exc))
        return None


def _extract_normalized_event(envelope: Dict[str, Any]) -> Optional[NormalizedEvent]:
    if envelope.get("event_type") != "wiki.recentchange":
        return None

    payload = envelope.get("payload")
    if not isinstance(payload, dict):
        return None

    event_id = _safe_str(envelope.get("event_id"), default="")
    if not event_id:
        return None

    occurred_at = _parse_iso_datetime(envelope.get("occurred_at"))
    if occurred_at is None:
        return None

    wiki = _safe_str(payload.get("wiki"), default="").lower()
    if not wiki:
        return None

    change_type = _safe_str(payload.get("change_type"), default="unknown").lower()

    raw_log_type = payload.get("log_type")
    log_type = _safe_str(raw_log_type, default="").lower() if raw_log_type is not None else None
    if log_type == "":
        log_type = None

    raw_log_action = payload.get("log_action")
    log_action = _safe_str(raw_log_action, default="").lower() if raw_log_action is not None else None
    if log_action == "":
        log_action = None

    user_is_bot = _normalize_bool(payload.get("user_is_bot", payload.get("bot", False)))

    return NormalizedEvent(
        event_id=event_id,
        occurred_at=occurred_at,
        window_start=_floor_minute(occurred_at),
        wiki=wiki,
        change_type=change_type,
        log_type=log_type,
        log_action=log_action,
        user_is_bot=user_is_bot,
    )


# -----------------------------------------------------------------------------
# Counter building
# -----------------------------------------------------------------------------

def _new_counter(window_start: datetime, attrs: Optional[Dict[str, Any]] = None) -> CounterValue:
    return {
        "event_count": 0,
        "log_count": 0,
        "delete_count": 0,
        "block_count": 0,
        "window_start": window_start,
        "attrs": attrs or {},
    }


def _add_counter(
    counters: Counters,
    alert_key: str,
    window_start: datetime,
    *,
    event_count: int = 0,
    log_count: int = 0,
    delete_count: int = 0,
    block_count: int = 0,
    attrs: Optional[Dict[str, Any]] = None,
) -> None:
    key = (alert_key, _window_key(window_start))
    if key not in counters:
        counters[key] = _new_counter(window_start, attrs)

    counters[key]["event_count"] += event_count
    counters[key]["log_count"] += log_count
    counters[key]["delete_count"] += delete_count
    counters[key]["block_count"] += block_count

    if attrs:
        counters[key]["attrs"].update({k: v for k, v in attrs.items() if v is not None})


def _build_counters(events: Iterable[NormalizedEvent]) -> Counters:
    counters: Counters = defaultdict(dict)

    for event in events:
        is_log = event.change_type == "log"
        is_delete = is_log and event.log_type == "delete"
        is_block = is_log and event.log_type == "block"

        log_count = 1 if is_log else 0
        delete_count = 1 if is_delete else 0
        block_count = 1 if is_block else 0

        # Global time series.
        _add_counter(
            counters,
            "ALERT#GLOBAL",
            event.window_start,
            event_count=1,
            log_count=log_count,
            delete_count=delete_count,
            block_count=block_count,
        )

        # Per-wiki time series.
        _add_counter(
            counters,
            f"ALERT#WIKI#{event.wiki}",
            event.window_start,
            event_count=1,
            log_count=log_count,
            delete_count=delete_count,
            block_count=block_count,
            attrs={"wiki": event.wiki},
        )

        # Moderation burst time series.
        if is_delete:
            _add_counter(
                counters,
                "ALERT#LOG_TYPE#delete",
                event.window_start,
                event_count=1,
                log_count=1,
                delete_count=1,
                block_count=0,
                attrs={"log_type": "delete"},
            )

        if is_block:
            _add_counter(
                counters,
                "ALERT#LOG_TYPE#block",
                event.window_start,
                event_count=1,
                log_count=1,
                delete_count=0,
                block_count=1,
                attrs={"log_type": "block"},
            )

    return counters


# -----------------------------------------------------------------------------
# DynamoDB writes and reads
# -----------------------------------------------------------------------------

def _require_table() -> Any:
    if _alert_table is None:
        raise RuntimeError("ALERT_STATE_TABLE_NAME is required")
    return _alert_table


def _write_counter(alert_key: str, window_key: str, counter: CounterValue, now: datetime) -> Dict[str, Any]:
    table = _require_table()
    window_start = counter["window_start"]
    now_iso = _to_iso_z(now)

    expr_names = {
        "#window_start": "window_start",
        "#last_updated_at": "last_updated_at",
        "#ttl": "ttl",
        "#event_count": "event_count",
        "#log_count": "log_count",
        "#delete_count": "delete_count",
        "#block_count": "block_count",
    }
    expr_values = {
        ":window_start": _to_iso_z(window_start),
        ":last_updated_at": now_iso,
        ":ttl": Decimal(_ttl_epoch(window_start)),
        ":event_count": Decimal(counter.get("event_count", 0)),
        ":log_count": Decimal(counter.get("log_count", 0)),
        ":delete_count": Decimal(counter.get("delete_count", 0)),
        ":block_count": Decimal(counter.get("block_count", 0)),
    }

    set_parts = [
        "#window_start = :window_start",
        "#last_updated_at = :last_updated_at",
        "#ttl = :ttl",
    ]

    attrs = counter.get("attrs", {}) or {}
    for idx, (attr_name, attr_value) in enumerate(attrs.items()):
        placeholder_name = f"#attr_{idx}"
        placeholder_value = f":attr_{idx}"
        expr_names[placeholder_name] = attr_name
        expr_values[placeholder_value] = attr_value
        set_parts.append(f"{placeholder_name} = {placeholder_value}")

    update_expression = (
        "SET "
        + ", ".join(set_parts)
        + " ADD #event_count :event_count, #log_count :log_count, #delete_count :delete_count, #block_count :block_count"
    )

    response = table.update_item(
        Key={"alert_key": alert_key, "window_key": window_key},
        UpdateExpression=update_expression,
        ExpressionAttributeNames=expr_names,
        ExpressionAttributeValues=expr_values,
        ReturnValues="UPDATED_NEW",
    )

    return response.get("Attributes", {})


def _get_alert_item(alert_key: str, window_start: datetime) -> Optional[Dict[str, Any]]:
    table = _require_table()
    response = table.get_item(Key={"alert_key": alert_key, "window_key": _window_key(window_start)})
    return response.get("Item")


def _query_alert_items(alert_key: str, start_window: datetime, end_window: datetime) -> List[Dict[str, Any]]:
    """Query alert_state for one alert_key between two inclusive minute windows."""
    table = _require_table()

    response = table.query(
        KeyConditionExpression="alert_key = :ak AND window_key BETWEEN :start_wk AND :end_wk",
        ExpressionAttributeValues={
            ":ak": alert_key,
            ":start_wk": _window_key(start_window),
            ":end_wk": _window_key(end_window),
        },
    )

    items = response.get("Items", [])
    while "LastEvaluatedKey" in response:
        response = table.query(
            KeyConditionExpression="alert_key = :ak AND window_key BETWEEN :start_wk AND :end_wk",
            ExpressionAttributeValues={
                ":ak": alert_key,
                ":start_wk": _window_key(start_window),
                ":end_wk": _window_key(end_window),
            },
            ExclusiveStartKey=response["LastEvaluatedKey"],
        )
        items.extend(response.get("Items", []))

    return items


def _items_to_count_by_window(items: Iterable[Dict[str, Any]], count_field: str = "event_count") -> Dict[str, int]:
    return {str(item["window_key"]): _as_int(item.get(count_field), 0) for item in items if "window_key" in item}


# -----------------------------------------------------------------------------
# Baseline and anomaly math
# -----------------------------------------------------------------------------

def _latest_eligible_window(now: datetime) -> datetime:
    """
    Return the latest complete minute window safe to evaluate.

    A window is eligible only when:
      window_start + 60 seconds + EVALUATION_DELAY_SECONDS <= now
    """
    return _floor_minute(now - timedelta(seconds=EVALUATION_DELAY_SECONDS)) - timedelta(minutes=1)


def _baseline_points(alert_key: str, target_window: datetime, baseline_minutes: int) -> List[int]:
    baseline_end = target_window - timedelta(minutes=1)
    baseline_start = target_window - timedelta(minutes=baseline_minutes)

    items = _query_alert_items(alert_key, baseline_start, baseline_end)
    return [_as_int(item.get("event_count"), 0) for item in items]


def _compute_z_score(current_count: int, points: List[int]) -> Tuple[float, float, float]:
    avg = float(mean(points))
    raw_stddev = float(pstdev(points)) if len(points) > 1 else 0.0

    # Guardrail: when historical counts are very flat, a raw stddev of 0 would make
    # the z-score undefined. A small floor prevents division-by-zero while staying
    # conservative for low-volume series.
    stddev_for_score = raw_stddev if raw_stddev >= 1.0 else max(1.0, avg * 0.10)
    z_score = (float(current_count) - avg) / stddev_for_score

    return avg, raw_stddev, z_score


def _round_float(value: float, digits: int = 4) -> float:
    if math.isnan(value) or math.isinf(value):
        return 0.0
    return round(value, digits)


# -----------------------------------------------------------------------------
# Alert deduplication and SNS
# -----------------------------------------------------------------------------

def _reserve_alert(
    *,
    alert_key: str,
    window_key: str,
    alert_type: str,
    now: datetime,
) -> bool:
    table = _require_table()

    try:
        table.update_item(
            Key={"alert_key": alert_key, "window_key": window_key},
            UpdateExpression=(
                "SET #alert_status = :publishing, "
                "#alert_type = :alert_type, "
                "#alert_reserved_at = :reserved_at"
            ),
            ConditionExpression="attribute_not_exists(#alert_status)",
            ExpressionAttributeNames={
                "#alert_status": "alert_status",
                "#alert_type": "alert_type",
                "#alert_reserved_at": "alert_reserved_at",
            },
            ExpressionAttributeValues={
                ":publishing": "PUBLISHING",
                ":alert_type": alert_type,
                ":reserved_at": _to_iso_z(now),
            },
        )
        return True
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
            return False
        raise


def _mark_alert_sent(
    *,
    alert_key: str,
    window_key: str,
    now: datetime,
    details: Dict[str, Any],
) -> None:
    table = _require_table()

    expr_names = {
        "#alert_status": "alert_status",
        "#alert_sent_at": "alert_sent_at",
        "#last_updated_at": "last_updated_at",
    }
    expr_values = {
        ":sent": "SENT",
        ":alert_sent_at": _to_iso_z(now),
        ":last_updated_at": _to_iso_z(now),
    }

    set_parts = [
        "#alert_status = :sent",
        "#alert_sent_at = :alert_sent_at",
        "#last_updated_at = :last_updated_at",
    ]

    for idx, (key, value) in enumerate(details.items()):
        if value is None:
            continue
        name_ph = f"#detail_{idx}"
        value_ph = f":detail_{idx}"
        expr_names[name_ph] = key
        expr_values[value_ph] = _as_decimal(value) if isinstance(value, (int, float, Decimal)) else value
        set_parts.append(f"{name_ph} = {value_ph}")

    table.update_item(
        Key={"alert_key": alert_key, "window_key": window_key},
        UpdateExpression="SET " + ", ".join(set_parts),
        ExpressionAttributeNames=expr_names,
        ExpressionAttributeValues=expr_values,
    )


def _mark_alert_failed(
    *,
    alert_key: str,
    window_key: str,
    now: datetime,
    error: str,
) -> None:
    table = _require_table()
    table.update_item(
        Key={"alert_key": alert_key, "window_key": window_key},
        UpdateExpression=(
            "SET #alert_status = :failed, "
            "#alert_failed_at = :failed_at, "
            "#alert_error = :error, "
            "#last_updated_at = :last_updated_at"
        ),
        ExpressionAttributeNames={
            "#alert_status": "alert_status",
            "#alert_failed_at": "alert_failed_at",
            "#alert_error": "alert_error",
            "#last_updated_at": "last_updated_at",
        },
        ExpressionAttributeValues={
            ":failed": "FAILED",
            ":failed_at": _to_iso_z(now),
            ":error": error[:500],
            ":last_updated_at": _to_iso_z(now),
        },
    )


def _publish_sns(message: Dict[str, Any], subject: str) -> None:
    if not ALERT_TOPIC_ARN:
        raise RuntimeError("ALERT_TOPIC_ARN is required to publish SNS alerts")

    _sns.publish(
        TopicArn=ALERT_TOPIC_ARN,
        Subject=subject[:100],
        Message=json.dumps(message, default=_json_default, separators=(",", ":")),
    )


def _dedup_and_publish(
    *,
    alert_key: str,
    window_key: str,
    alert_type: str,
    sns_message: Dict[str, Any],
    ddb_details: Dict[str, Any],
    now: datetime,
) -> bool:
    reserved = _reserve_alert(alert_key=alert_key, window_key=window_key, alert_type=alert_type, now=now)
    if not reserved:
        _log("alert_deduplicated", alert_key=alert_key, window_key=window_key, alert_type=alert_type)
        return False

    try:
        _publish_sns(sns_message, subject=f"{alert_type} {window_key}")
        _mark_alert_sent(alert_key=alert_key, window_key=window_key, now=now, details=ddb_details)
        _log("alert_published", alert_key=alert_key, window_key=window_key, alert_type=alert_type)
        return True
    except Exception as exc:  # noqa: BLE001 - mark failure and surface the error
        _mark_alert_failed(alert_key=alert_key, window_key=window_key, now=now, error=str(exc))
        _log_error("alert_publish_failed", alert_key=alert_key, window_key=window_key, alert_type=alert_type, error=str(exc))
        raise


# -----------------------------------------------------------------------------
# Alert evaluation
# -----------------------------------------------------------------------------

def _evaluate_z_score_alert(alert_key: str, target_window: datetime, now: datetime) -> Tuple[int, int]:
    """Evaluate global/wiki spike alert for one completed minute.

    Returns: (alerts_evaluated, alerts_published)
    """
    item = _get_alert_item(alert_key, target_window)
    if not item:
        _log("alert_evaluation_skipped", reason="missing_current_window", alert_key=alert_key, window_key=_window_key(target_window))
        return 0, 0

    current_count = _as_int(item.get("event_count"), 0)
    is_global = alert_key == "ALERT#GLOBAL"
    is_wiki = alert_key.startswith("ALERT#WIKI#")

    if not (is_global or is_wiki):
        return 0, 0

    threshold = GLOBAL_Z_THRESHOLD if is_global else WIKI_Z_THRESHOLD
    minimum_count = GLOBAL_MIN_COUNT if is_global else WIKI_MIN_COUNT
    alert_type = "GLOBAL_ACTIVITY_SPIKE" if is_global else "WIKI_ACTIVITY_SPIKE"

    if current_count < minimum_count:
        _log(
            "alert_evaluation_skipped",
            reason="below_minimum_count",
            alert_key=alert_key,
            window_key=_window_key(target_window),
            current_count=current_count,
            minimum_count=minimum_count,
        )
        return 1, 0

    points = _baseline_points(alert_key, target_window, BASELINE_WINDOW_MINUTES)
    if len(points) < MIN_BASELINE_POINTS:
        _log(
            "alert_evaluation_skipped",
            reason="insufficient_baseline",
            alert_key=alert_key,
            window_key=_window_key(target_window),
            baseline_points=len(points),
            minimum_required=MIN_BASELINE_POINTS,
        )
        return 1, 0

    baseline_avg, baseline_stddev, z_score = _compute_z_score(current_count, points)

    _log(
        "alert_evaluation_completed",
        alert_key=alert_key,
        window_key=_window_key(target_window),
        current_count=current_count,
        baseline_points=len(points),
        baseline_avg=_round_float(baseline_avg),
        baseline_stddev=_round_float(baseline_stddev),
        z_score=_round_float(z_score),
        threshold=threshold,
    )

    if z_score <= threshold:
        return 1, 0

    window_start_iso = _to_iso_z(target_window)
    window_key = _window_key(target_window)
    created_at = _to_iso_z(now)

    sns_message: Dict[str, Any] = {
        "message_type": "realtime.alert.triggered",
        "alert_type": alert_type,
        "severity": "warning",
        "alert_key": alert_key,
        "window_key": window_key,
        "window_start": window_start_iso,
        "current_count": current_count,
        "baseline_avg": _round_float(baseline_avg),
        "baseline_stddev": _round_float(baseline_stddev),
        "z_score": _round_float(z_score),
        "threshold": threshold,
        "created_at": created_at,
    }

    if is_wiki:
        sns_message["wiki"] = alert_key.replace("ALERT#WIKI#", "", 1)

    ddb_details = {
        "current_count": current_count,
        "baseline_avg": _round_float(baseline_avg),
        "baseline_stddev": _round_float(baseline_stddev),
        "z_score": _round_float(z_score),
        "threshold": threshold,
    }

    published = _dedup_and_publish(
        alert_key=alert_key,
        window_key=window_key,
        alert_type=alert_type,
        sns_message=sns_message,
        ddb_details=ddb_details,
        now=now,
    )
    return 1, 1 if published else 0


def _sum_count_for_range(alert_key: str, start_window: datetime, end_window: datetime, count_field: str = "event_count") -> Tuple[int, int]:
    items = _query_alert_items(alert_key, start_window, end_window)
    return sum(_as_int(item.get(count_field), 0) for item in items), len(items)


def _moderation_baseline_group_sums(alert_key: str, target_window: datetime) -> Tuple[List[int], int]:
    """Return previous 30 minutes grouped into 5-minute sums before current 5-minute window."""
    current_window_start = target_window - timedelta(minutes=MODERATION_WINDOW_MINUTES - 1)
    baseline_end = current_window_start - timedelta(minutes=1)
    baseline_start = baseline_end - timedelta(minutes=BASELINE_WINDOW_MINUTES - 1)

    items = _query_alert_items(alert_key, baseline_start, baseline_end)
    count_by_window = _items_to_count_by_window(items, count_field="event_count")

    group_sums: List[int] = []
    cursor = baseline_start
    while cursor <= baseline_end:
        group_total = 0
        for offset in range(MODERATION_WINDOW_MINUTES):
            minute = cursor + timedelta(minutes=offset)
            if minute > baseline_end:
                break
            group_total += count_by_window.get(_window_key(minute), 0)
        group_sums.append(group_total)
        cursor += timedelta(minutes=MODERATION_WINDOW_MINUTES)

    return group_sums, len(items)


def _evaluate_moderation_burst(alert_key: str, target_window: datetime, now: datetime) -> Tuple[int, int]:
    if alert_key not in {"ALERT#LOG_TYPE#delete", "ALERT#LOG_TYPE#block"}:
        return 0, 0

    log_type = alert_key.replace("ALERT#LOG_TYPE#", "", 1)
    minimum_count = DELETE_MIN_COUNT if log_type == "delete" else BLOCK_MIN_COUNT

    current_start = target_window - timedelta(minutes=MODERATION_WINDOW_MINUTES - 1)
    current_5m_count, current_items = _sum_count_for_range(alert_key, current_start, target_window, count_field="event_count")

    if current_items == 0:
        _log("alert_evaluation_skipped", reason="missing_current_moderation_window", alert_key=alert_key, window_key=_window_key(target_window))
        return 0, 0

    if current_5m_count < minimum_count:
        _log(
            "alert_evaluation_skipped",
            reason="below_minimum_count",
            alert_key=alert_key,
            window_key=_window_key(target_window),
            current_5m_count=current_5m_count,
            minimum_count=minimum_count,
        )
        return 1, 0

    baseline_groups, baseline_items_available = _moderation_baseline_group_sums(alert_key, target_window)
    non_empty_groups = [value for value in baseline_groups if value > 0]

    if len(non_empty_groups) < MIN_MODERATION_BASELINE_POINTS:
        _log(
            "alert_evaluation_skipped",
            reason="insufficient_moderation_baseline",
            alert_key=alert_key,
            window_key=_window_key(target_window),
            baseline_non_empty_groups=len(non_empty_groups),
            baseline_items_available=baseline_items_available,
            minimum_required=MIN_MODERATION_BASELINE_POINTS,
        )
        return 1, 0

    baseline_5m_avg = float(mean(non_empty_groups))
    burst_ratio = float(current_5m_count) / max(baseline_5m_avg, 1.0)

    _log(
        "alert_evaluation_completed",
        alert_key=alert_key,
        window_key=_window_key(target_window),
        current_5m_count=current_5m_count,
        baseline_5m_avg=_round_float(baseline_5m_avg),
        burst_ratio=_round_float(burst_ratio),
        threshold_ratio=MODERATION_BURST_RATIO_THRESHOLD,
    )

    if burst_ratio <= MODERATION_BURST_RATIO_THRESHOLD:
        return 1, 0

    window_key = _window_key(target_window)
    now_iso = _to_iso_z(now)

    sns_message = {
        "message_type": "realtime.alert.triggered",
        "alert_type": "MODERATION_BURST",
        "severity": "warning",
        "log_type": log_type,
        "alert_key": alert_key,
        "window_key": window_key,
        "window_start": _to_iso_z(target_window),
        "current_5m_count": current_5m_count,
        "baseline_5m_avg": _round_float(baseline_5m_avg),
        "burst_ratio": _round_float(burst_ratio),
        "threshold_ratio": MODERATION_BURST_RATIO_THRESHOLD,
        "created_at": now_iso,
    }

    ddb_details = {
        "current_5m_count": current_5m_count,
        "baseline_5m_avg": _round_float(baseline_5m_avg),
        "burst_ratio": _round_float(burst_ratio),
        "threshold_ratio": MODERATION_BURST_RATIO_THRESHOLD,
    }

    published = _dedup_and_publish(
        alert_key=alert_key,
        window_key=window_key,
        alert_type="MODERATION_BURST",
        sns_message=sns_message,
        ddb_details=ddb_details,
        now=now,
    )
    return 1, 1 if published else 0


def _build_evaluation_alert_keys(counters: Counters) -> List[str]:
    touched_keys = {alert_key for (alert_key, _window_key_value) in counters.keys()}

    evaluation_keys: List[str] = []

    if "ALERT#GLOBAL" in touched_keys:
        evaluation_keys.append("ALERT#GLOBAL")

    for key in ("ALERT#LOG_TYPE#delete", "ALERT#LOG_TYPE#block"):
        if key in touched_keys:
            evaluation_keys.append(key)

    wiki_keys = sorted(key for key in touched_keys if key.startswith("ALERT#WIKI#"))
    evaluation_keys.extend(wiki_keys[:MAX_WIKI_ALERT_KEYS_PER_INVOCATION])

    return evaluation_keys


def _evaluate_alerts(counters: Counters, now: datetime) -> Tuple[int, int, str]:
    target_window = _latest_eligible_window(now)
    target_window_key = _window_key(target_window)
    evaluation_keys = _build_evaluation_alert_keys(counters)

    if not evaluation_keys:
        _log("alert_evaluation_skipped", reason="no_evaluation_keys", target_window=target_window_key)
        return 0, 0, target_window_key

    alerts_evaluated = 0
    alerts_published = 0

    for alert_key in evaluation_keys:
        if alert_key in {"ALERT#GLOBAL"} or alert_key.startswith("ALERT#WIKI#"):
            evaluated, published = _evaluate_z_score_alert(alert_key, target_window, now)
        elif alert_key in {"ALERT#LOG_TYPE#delete", "ALERT#LOG_TYPE#block"}:
            evaluated, published = _evaluate_moderation_burst(alert_key, target_window, now)
        else:
            evaluated, published = 0, 0

        alerts_evaluated += evaluated
        alerts_published += published

    return alerts_evaluated, alerts_published, target_window_key


# -----------------------------------------------------------------------------
# Lambda entrypoint
# -----------------------------------------------------------------------------

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    now = _now()
    records = event.get("Records", [])

    _log("alert_processor_invoked", input_records=len(records), evaluation_delay_seconds=EVALUATION_DELAY_SECONDS)

    decoded_count = 0
    valid_count = 0
    skipped_count = 0
    normalized_events: List[NormalizedEvent] = []

    for record in records:
        envelope = _decode_kinesis_record(record)
        if envelope is None:
            skipped_count += 1
            continue

        decoded_count += 1
        normalized_event = _extract_normalized_event(envelope)
        if normalized_event is None:
            skipped_count += 1
            continue

        valid_count += 1
        normalized_events.append(normalized_event)

    counters = _build_counters(normalized_events)

    dynamodb_update_count = 0
    for (alert_key, window_key), counter in counters.items():
        _write_counter(alert_key, window_key, counter, now)
        dynamodb_update_count += 1

    _log(
        "alert_state_update_completed",
        counter_items=len(counters),
        dynamodb_update_count=dynamodb_update_count,
        alert_keys=sorted({key for key, _ in counters.keys()}),
    )

    alerts_evaluated = 0
    alerts_published = 0
    evaluation_window = None

    if counters:
        alerts_evaluated, alerts_published, evaluation_window = _evaluate_alerts(counters, now)

    result = {
        "status": "ok",
        "input_records": len(records),
        "decoded_count": decoded_count,
        "valid_count": valid_count,
        "skipped_count": skipped_count,
        "dynamodb_update_count": dynamodb_update_count,
        "alerts_evaluated": alerts_evaluated,
        "alerts_published": alerts_published,
        "evaluation_window": evaluation_window,
    }

    _log("alert_processor_batch_processed", **result)
    return result
