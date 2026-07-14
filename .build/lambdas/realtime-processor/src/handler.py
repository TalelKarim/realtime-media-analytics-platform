import base64
import hashlib
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any

import boto3
from opentelemetry import trace as otel_trace
from opentelemetry.propagate import inject
from opentelemetry.trace import Status, StatusCode

from .observability import (
    broadcast_signal_duration_ms,
    broadcast_signal_failure_total,
    broadcast_signal_skipped_total,
    broadcast_signals_sent_total,
    dynamodb_aggregate_update_failure_total,
    dynamodb_aggregate_updates_total,
    dynamodb_update_batch_duration_ms,
    flush_otel,
    processor_batch_duration_ms,
    realtime_processor_batches_total,
    realtime_processor_records_decoded_total,
    realtime_processor_records_failed_total,
    realtime_processor_records_received_total,
    realtime_processor_records_skipped_total,
    realtime_processor_records_valid_total,
    tracer,
)


LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
ENVIRONMENT = os.getenv("ENVIRONMENT", "dev")

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


# ---------------------------------------------------------------------------
# Logging helpers
# ---------------------------------------------------------------------------

def log_json(level: str, message: str, **fields: Any) -> None:
    """
    Emit one structured JSON log line.

    Logs still go through CloudWatch Logs -> Promtail -> Loki.
    If an OpenTelemetry span is active, trace_id and span_id are added so Loki
    logs can be correlated manually with Tempo traces.
    """
    payload = {
        "message": message,
        "service": "realtime-processor",
        "component": "realtime-processing",
        "environment": ENVIRONMENT,
        **fields,
    }

    span_context = otel_trace.get_current_span().get_span_context()
    if span_context and span_context.is_valid:
        payload["trace_id"] = format(span_context.trace_id, "032x")
        payload["span_id"] = format(span_context.span_id, "016x")

    log_line = json.dumps(payload, default=str)

    if level.upper() == "ERROR":
        logger.error(log_line)
    elif level.upper() == "WARNING":
        logger.warning(log_line)
    else:
        logger.info(log_line)


def log_exception(message: str, **fields: Any) -> None:
    """
    Emit a structured JSON error log and keep the Python stack trace.

    If an OpenTelemetry span is active, trace_id and span_id are added so Grafana
    can correlate Loki logs with Tempo traces.
    """
    payload = {
        "message": message,
        "service": "realtime-processor",
        "component": "realtime-processing",
        "environment": ENVIRONMENT,
        **fields,
    }

    span_context = otel_trace.get_current_span().get_span_context()
    if span_context and span_context.is_valid:
        payload["trace_id"] = format(span_context.trace_id, "032x")
        payload["span_id"] = format(span_context.span_id, "016x")

    logger.exception(json.dumps(payload, default=str))


# ---------------------------------------------------------------------------
# Generic helpers
# ---------------------------------------------------------------------------

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
        log_json(
            "WARNING",
            "invalid_occurred_at_fallback_to_now",
            occurred_at=value,
        )
        return _utc_now()


def _datetime_to_epoch_ms(dt: datetime) -> int:
    return int(dt.astimezone(timezone.utc).timestamp() * 1000)


def _parse_epoch_ms(value: Any) -> int | None:
    if value is None:
        return None

    try:
        timestamp_ms = int(float(value))
    except (TypeError, ValueError):
        return None

    if timestamp_ms <= 0:
        return None

    return timestamp_ms


def _extract_source_event_timestamp_ms(
    envelope: dict[str, Any],
    payload: dict[str, Any],
    occurred_dt: datetime,
) -> int:
    """
    Return the event-time timestamp in epoch milliseconds.

    The final freshness SLO needs the source event timestamp, not the time at
    which the Lambda processes the record. The future/target collector contract
    can provide source_event_timestamp_ms directly. The current contract already
    provides occurred_at, so we fall back to occurred_dt when the epoch-ms field
    is absent.
    """
    for candidate in (
        envelope.get("source_event_timestamp_ms"),
        payload.get("source_event_timestamp_ms"),
        envelope.get("event_timestamp_ms"),
        payload.get("event_timestamp_ms"),
    ):
        timestamp_ms = _parse_epoch_ms(candidate)
        if timestamp_ms is not None:
            return timestamp_ms

    return _datetime_to_epoch_ms(occurred_dt)


def _compute_event_timestamp_bounds(
    events: list[dict[str, Any]],
) -> tuple[int | None, int | None]:
    timestamps = [
        event["source_event_timestamp_ms"]
        for event in events
        if event.get("source_event_timestamp_ms") is not None
    ]

    if not timestamps:
        return None, None

    return min(timestamps), max(timestamps)


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


def _safe_bool(value: Any) -> bool:
    """
    Normalize boolean values coming from Wikimedia / collector payloads.

    The current collector sends a real boolean, but this keeps the processor
    safe if a future source sends "true" / "false" as strings.
    """
    if isinstance(value, bool):
        return value

    if isinstance(value, (int, float)):
        return bool(value)

    if isinstance(value, str):
        return value.strip().lower() in {"true", "1", "yes", "y"}

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


# ---------------------------------------------------------------------------
# Event normalization and aggregation
# ---------------------------------------------------------------------------

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
    source_event_timestamp_ms = _extract_source_event_timestamp_ms(
        envelope=envelope,
        payload=payload,
        occurred_dt=occurred_dt,
    )

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
    is_bot = _safe_bool(bot)

    return {
        "event_id": event_id,
        "occurred_dt": occurred_dt,
        "source_event_timestamp_ms": source_event_timestamp_ms,
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

        is_bot_text = str(event["is_bot"]).lower()

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

        # 4b. Per-wiki change type distribution.
        # Used by topic subscriptions such as wiki:frwiki.
        _add_counter(
            counters,
            metric_key=f"METRIC#WIKI_CHANGE_TYPE#WIKI#{event['wiki']}#TYPE#{event['change_type']}",
            window_key=window_key,
            attrs={
                **common_attrs,
                "wiki": event["wiki"],
                "change_type": event["change_type"],
            },
        )

        # 5. Bot vs human.
        _add_counter(
            counters,
            metric_key=f"METRIC#BOT_ACTIVITY#BOT#{is_bot_text}",
            window_key=window_key,
            attrs={
                **common_attrs,
                "is_bot": event["is_bot"],
            },
        )

        # 5b. Per-wiki bot vs human distribution.
        _add_counter(
            counters,
            metric_key=f"METRIC#WIKI_BOT_ACTIVITY#WIKI#{event['wiki']}#BOT#{is_bot_text}",
            window_key=window_key,
            attrs={
                **common_attrs,
                "wiki": event["wiki"],
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

        # 6b. Per-wiki namespace distribution.
        _add_counter(
            counters,
            metric_key=f"METRIC#WIKI_NAMESPACE#WIKI#{event['wiki']}#NS#{event['namespace_key']}",
            window_key=window_key,
            attrs={
                **common_attrs,
                "wiki": event["wiki"],
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


# ---------------------------------------------------------------------------
# DynamoDB update helpers
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# SQS broadcast signal
# ---------------------------------------------------------------------------

def _otel_message_attributes_from_current_context() -> dict[str, dict[str, str]]:
    """
    Inject the current OpenTelemetry trace context into SQS message attributes.

    The broadcaster does not consume this context yet. This is safe and prepares
    the next step: end-to-end trace propagation realtime-processor -> SQS -> broadcaster.
    """
    carrier: dict[str, str] = {}
    inject(carrier)

    message_attributes: dict[str, dict[str, str]] = {}

    for key in ("traceparent", "tracestate", "baggage"):
        value = carrier.get(key)
        if value:
            message_attributes[key] = {
                "DataType": "String",
                "StringValue": value,
            }

    return message_attributes


def _send_broadcast_signal(
    aggregation_windows: set[str],
    now: datetime,
    latest_event_timestamp_ms: int | None,
    oldest_event_timestamp_ms: int | None,
    aws_request_id: str | None = None,
) -> bool:
    start_time = time.perf_counter()
    metric_attrs = {
        "environment": ENVIRONMENT,
    }

    with tracer.start_as_current_span("realtime_processor.send_broadcast_signal") as span:
        span.set_attribute("messaging.system", "aws.sqs")
        span.set_attribute("messaging.destination.name", "broadcast-signal")
        span.set_attribute("broadcast.aggregation_window_count", len(aggregation_windows))

        if latest_event_timestamp_ms is not None:
            span.set_attribute("events.latest_event_timestamp_ms", latest_event_timestamp_ms)

        if oldest_event_timestamp_ms is not None:
            span.set_attribute("events.oldest_event_timestamp_ms", oldest_event_timestamp_ms)

        if not BROADCAST_QUEUE_URL:
            broadcast_signal_skipped_total.add(
                1,
                {
                    **metric_attrs,
                    "reason": "missing_broadcast_queue_url",
                },
            )

            span.set_attribute("broadcast.signal.result", "skipped")
            span.set_attribute("broadcast.signal.skip_reason", "missing_broadcast_queue_url")
            span.set_status(Status(StatusCode.OK))

            log_json(
                "WARNING",
                "broadcast_signal_skipped",
                aws_request_id=aws_request_id,
                reason="missing_broadcast_queue_url",
            )
            return False

        if not aggregation_windows:
            broadcast_signal_skipped_total.add(
                1,
                {
                    **metric_attrs,
                    "reason": "no_aggregation_windows",
                },
            )

            span.set_attribute("broadcast.signal.result", "skipped")
            span.set_attribute("broadcast.signal.skip_reason", "no_aggregation_windows")
            span.set_status(Status(StatusCode.OK))

            log_json(
                "INFO",
                "broadcast_signal_skipped",
                aws_request_id=aws_request_id,
                reason="no_aggregation_windows",
            )
            return False

        broadcast_window_start = _floor_time(now, BROADCAST_WINDOW_SECONDS)
        broadcast_window_iso = _to_iso_z(broadcast_window_start)

        message_body = {
            "message_type": "aggregates.updated",
            "source": "realtime-processor",
            "created_at": _to_iso_z(now),
            "broadcast_window": broadcast_window_iso,
            "aggregation_windows": sorted(aggregation_windows),
            "latest_event_timestamp_ms": latest_event_timestamp_ms,
            "oldest_event_timestamp_ms": oldest_event_timestamp_ms,
        }

        deduplication_id = f"BROADCAST#{broadcast_window_iso}"
        message_attributes = _otel_message_attributes_from_current_context()

        span.set_attribute("broadcast.broadcast_window", broadcast_window_iso)
        span.set_attribute("messaging.message.conversation_id", deduplication_id)

        try:
            sqs.send_message(
                QueueUrl=BROADCAST_QUEUE_URL,
                MessageBody=json.dumps(message_body),
                MessageGroupId="realtime-broadcast",
                MessageDeduplicationId=deduplication_id,
                MessageAttributes=message_attributes,
            )

            duration_ms = round((time.perf_counter() - start_time) * 1000, 2)

            broadcast_signals_sent_total.add(
                1,
                {
                    **metric_attrs,
                    "result": "sent",
                },
            )
            broadcast_signal_duration_ms.record(
                duration_ms,
                {
                    **metric_attrs,
                    "result": "sent",
                },
            )

            span.set_attribute("broadcast.signal.result", "sent")
            span.set_attribute("broadcast.signal.duration_ms", duration_ms)
            span.set_status(Status(StatusCode.OK))

            log_json(
                "INFO",
                "broadcast_signal_sent",
                aws_request_id=aws_request_id,
                broadcast_window=broadcast_window_iso,
                aggregation_windows=sorted(aggregation_windows),
                aggregation_window_count=len(aggregation_windows),
                latest_event_timestamp_ms=latest_event_timestamp_ms,
                oldest_event_timestamp_ms=oldest_event_timestamp_ms,
                deduplication_id=deduplication_id,
                trace_context_injected=bool(message_attributes.get("traceparent")),
            )

            return True

        except Exception as exc:
            # Important:
            # We do NOT fail the Lambda after DynamoDB writes succeeded.
            # Otherwise Kinesis would retry the batch and could double-count DynamoDB counters.
            duration_ms = round((time.perf_counter() - start_time) * 1000, 2)
            failure_attrs = {
                **metric_attrs,
                "error_type": type(exc).__name__,
            }

            broadcast_signal_failure_total.add(1, failure_attrs)
            broadcast_signal_duration_ms.record(
                duration_ms,
                {
                    **failure_attrs,
                    "result": "failed",
                },
            )

            span.record_exception(exc)
            span.set_attribute("broadcast.signal.result", "failed")
            span.set_attribute("broadcast.signal.duration_ms", duration_ms)
            span.set_attribute("error.type", type(exc).__name__)
            span.set_status(Status(StatusCode.ERROR, str(exc)))

            log_exception(
                "broadcast_signal_failed",
                aws_request_id=aws_request_id,
                error_type=type(exc).__name__,
                error_message=str(exc),
            )
            return False


# ---------------------------------------------------------------------------
# Lambda entrypoint
# ---------------------------------------------------------------------------

def lambda_handler(event, context):
    start_time = time.perf_counter()
    aws_request_id = getattr(context, "aws_request_id", None)

    metric_base_attrs = {
        "environment": ENVIRONMENT,
    }

    try:
        with tracer.start_as_current_span("realtime_processor.lambda_handler") as span:
            span.set_attribute("faas.trigger", "kinesis")
            span.set_attribute("faas.execution", aws_request_id or "unknown")

            if not AGGREGATES_TABLE_NAME:
                raise RuntimeError("Missing required env var: AGGREGATES_TABLE_NAME")

            if table is None:
                raise RuntimeError("DynamoDB table client is not initialized")

            records = event.get("Records", [])
            now = _utc_now()
            now_iso = _to_iso_z(now)
            ttl = int(time.time()) + (AGGREGATE_TTL_DAYS * 86400)

            span.set_attribute("kinesis.record_count", len(records))
            realtime_processor_records_received_total.add(len(records), metric_base_attrs)

            log_json(
                "INFO",
                "realtime_processor_invoked",
                aws_request_id=aws_request_id,
                record_count=len(records),
                table=AGGREGATES_TABLE_NAME,
            )

            decoded_count = 0
            valid_count = 0
            skipped_count = 0
            failed_record_count = 0

            normalized_events: list[dict[str, Any]] = []

            with tracer.start_as_current_span("realtime_processor.decode_kinesis_batch") as decode_span:
                decode_span.set_attribute("kinesis.record_count", len(records))

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
                        failed_record_count += 1

                        failure_attrs = {
                            **metric_base_attrs,
                            "error_type": type(exc).__name__,
                        }
                        realtime_processor_records_failed_total.add(1, failure_attrs)

                        decode_span.record_exception(exc)

                        log_exception(
                            "record_processing_failed",
                            aws_request_id=aws_request_id,
                            error_type=type(exc).__name__,
                            error_message=str(exc),
                        )

                realtime_processor_records_decoded_total.add(decoded_count, metric_base_attrs)
                realtime_processor_records_valid_total.add(valid_count, metric_base_attrs)
                realtime_processor_records_skipped_total.add(skipped_count, metric_base_attrs)

                decode_span.set_attribute("records.decoded_count", decoded_count)
                decode_span.set_attribute("records.valid_count", valid_count)
                decode_span.set_attribute("records.skipped_count", skipped_count)
                decode_span.set_attribute("records.failed_count", failed_record_count)
                decode_span.set_status(Status(StatusCode.OK))

            with tracer.start_as_current_span("realtime_processor.build_counters") as counters_span:
                counters, aggregation_windows = _build_counters(normalized_events)
                oldest_event_timestamp_ms, latest_event_timestamp_ms = _compute_event_timestamp_bounds(
                    normalized_events
                )

                counters_span.set_attribute("events.normalized_count", len(normalized_events))
                counters_span.set_attribute("aggregation.window_count", len(aggregation_windows))
                counters_span.set_attribute("dynamodb.counter_count", len(counters))

                if latest_event_timestamp_ms is not None:
                    counters_span.set_attribute(
                        "events.latest_event_timestamp_ms",
                        latest_event_timestamp_ms,
                    )

                if oldest_event_timestamp_ms is not None:
                    counters_span.set_attribute(
                        "events.oldest_event_timestamp_ms",
                        oldest_event_timestamp_ms,
                    )

                counters_span.set_status(Status(StatusCode.OK))

            dynamodb_update_count = 0
            update_start_time = time.perf_counter()

            with tracer.start_as_current_span("realtime_processor.update_dynamodb_batch") as ddb_span:
                ddb_span.set_attribute("dynamodb.table", AGGREGATES_TABLE_NAME)
                ddb_span.set_attribute("dynamodb.counter_count", len(counters))

                try:
                    for (metric_key, window_key), counter_data in counters.items():
                        try:
                            _update_counter(
                                metric_key=metric_key,
                                window_key=window_key,
                                count=counter_data["count"],
                                now_iso=now_iso,
                                ttl=ttl,
                                attrs=counter_data.get("attrs", {}),
                            )

                            dynamodb_update_count += 1
                            dynamodb_aggregate_updates_total.add(1, metric_base_attrs)

                        except Exception as exc:
                            failure_attrs = {
                                **metric_base_attrs,
                                "error_type": type(exc).__name__,
                            }
                            dynamodb_aggregate_update_failure_total.add(1, failure_attrs)

                            ddb_span.record_exception(exc)
                            ddb_span.set_attribute("dynamodb.failed_metric_key", metric_key)
                            ddb_span.set_attribute("dynamodb.failed_window_key", window_key)
                            ddb_span.set_attribute("error.type", type(exc).__name__)
                            ddb_span.set_status(Status(StatusCode.ERROR, str(exc)))

                            log_exception(
                                "dynamodb_update_failed",
                                aws_request_id=aws_request_id,
                                metric_key=metric_key,
                                window_key=window_key,
                                error_type=type(exc).__name__,
                                error_message=str(exc),
                            )
                            raise

                    update_duration_ms = round((time.perf_counter() - update_start_time) * 1000, 2)

                    dynamodb_update_batch_duration_ms.record(
                        update_duration_ms,
                        {
                            **metric_base_attrs,
                            "result": "completed",
                        },
                    )

                    ddb_span.set_attribute("dynamodb.update_count", dynamodb_update_count)
                    ddb_span.set_attribute("dynamodb.update_duration_ms", update_duration_ms)
                    ddb_span.set_status(Status(StatusCode.OK))

                except Exception:
                    update_duration_ms = round((time.perf_counter() - update_start_time) * 1000, 2)
                    dynamodb_update_batch_duration_ms.record(
                        update_duration_ms,
                        {
                            **metric_base_attrs,
                            "result": "failed",
                        },
                    )
                    raise

            broadcast_signal_sent = False
            if valid_count > 0 and dynamodb_update_count > 0:
                broadcast_signal_sent = _send_broadcast_signal(
                    aggregation_windows=aggregation_windows,
                    now=now,
                    latest_event_timestamp_ms=latest_event_timestamp_ms,
                    oldest_event_timestamp_ms=oldest_event_timestamp_ms,
                    aws_request_id=aws_request_id,
                )

            duration_ms = round((time.perf_counter() - start_time) * 1000, 2)

            realtime_processor_batches_total.add(
                1,
                {
                    **metric_base_attrs,
                    "result": "success",
                    "broadcast_signal_sent": str(broadcast_signal_sent).lower(),
                },
            )
            processor_batch_duration_ms.record(
                duration_ms,
                {
                    **metric_base_attrs,
                    "result": "success",
                },
            )

            span.set_attribute("records.input_count", len(records))
            span.set_attribute("records.decoded_count", decoded_count)
            span.set_attribute("records.valid_count", valid_count)
            span.set_attribute("records.skipped_count", skipped_count)
            span.set_attribute("dynamodb.update_count", dynamodb_update_count)
            span.set_attribute("broadcast.signal_sent", broadcast_signal_sent)

            if latest_event_timestamp_ms is not None:
                span.set_attribute("events.latest_event_timestamp_ms", latest_event_timestamp_ms)

            if oldest_event_timestamp_ms is not None:
                span.set_attribute("events.oldest_event_timestamp_ms", oldest_event_timestamp_ms)

            span.set_attribute("lambda.duration_ms", duration_ms)
            span.set_attribute("lambda.result", "success")
            span.set_status(Status(StatusCode.OK))

            log_json(
                "INFO",
                "realtime_processor_batch_processed",
                aws_request_id=aws_request_id,
                input_records=len(records),
                decoded_count=decoded_count,
                valid_count=valid_count,
                skipped_count=skipped_count,
                aggregation_windows=sorted(aggregation_windows),
                aggregation_window_count=len(aggregation_windows),
                latest_event_timestamp_ms=latest_event_timestamp_ms,
                oldest_event_timestamp_ms=oldest_event_timestamp_ms,
                dynamodb_update_count=dynamodb_update_count,
                broadcast_signal_sent=broadcast_signal_sent,
                duration_ms=duration_ms,
                finops_write_reduction={
                    "events": valid_count,
                    "dynamodb_updates": dynamodb_update_count,
                },
            )

            return {
                "statusCode": 200,
                "input_records": len(records),
                "decoded_count": decoded_count,
                "valid_count": valid_count,
                "skipped_count": skipped_count,
                "dynamodb_update_count": dynamodb_update_count,
                "broadcast_signal_sent": broadcast_signal_sent,
                "latest_event_timestamp_ms": latest_event_timestamp_ms,
                "oldest_event_timestamp_ms": oldest_event_timestamp_ms,
            }

    except Exception as exc:
        duration_ms = round((time.perf_counter() - start_time) * 1000, 2)

        realtime_processor_batches_total.add(
            1,
            {
                **metric_base_attrs,
                "result": "failed",
                "error_type": type(exc).__name__,
            },
        )
        processor_batch_duration_ms.record(
            duration_ms,
            {
                **metric_base_attrs,
                "result": "failed",
                "error_type": type(exc).__name__,
            },
        )

        current_span = otel_trace.get_current_span()
        current_span.record_exception(exc)
        current_span.set_attribute("lambda.result", "failed")
        current_span.set_attribute("lambda.duration_ms", duration_ms)
        current_span.set_attribute("error.type", type(exc).__name__)
        current_span.set_status(Status(StatusCode.ERROR, str(exc)))

        log_exception(
            "realtime_processor_batch_failed",
            aws_request_id=aws_request_id,
            error_type=type(exc).__name__,
            error_message=str(exc),
            duration_ms=duration_ms,
        )

        # Preserve the existing reliability behavior:
        # if DynamoDB or critical processing fails, Kinesis should retry the batch.
        raise

    finally:
        # Lambda executions are short-lived. Force flushing prevents metrics/traces
        # from staying in memory until the runtime is frozen.
        flush_otel()