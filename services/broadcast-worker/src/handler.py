from __future__ import annotations

import json
import logging
import os
import random
import time
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from contextlib import nullcontext
from contextvars import copy_context
from datetime import datetime
from decimal import Decimal
from typing import Any, Iterator

import boto3
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.types import TypeSerializer
from botocore.config import Config
from botocore.exceptions import (
    ClientError,
    ConnectionClosedError,
    ConnectTimeoutError,
    EndpointConnectionError,
    ReadTimeoutError,
)
from opentelemetry import propagate, trace as otel_trace
from opentelemetry.context import Context
from opentelemetry.trace import Status, StatusCode

try:
    from opentelemetry.instrumentation.utils import suppress_instrumentation
except ImportError:  # Compatibility with older instrumentation packages.
    def suppress_instrumentation():
        return nullcontext()

from .observability import (
    broadcast_job_queue_delay_ms,
    broadcast_worker_duration_ms,
    broadcast_worker_jobs_failed_total,
    broadcast_worker_jobs_total,
    broadcast_worker_query_duration_ms,
    broadcast_worker_snapshot_read_duration_ms,
    broadcast_worker_subscriptions_found,
    event_to_dashboard_latency_ms,
    fanout_batch_size,
    fanout_duration_ms,
    flush_otel,
    gone_cleanup_duration_ms,
    oldest_event_to_dashboard_latency_ms,
    tracer,
    websocket_connection_gone_total,
    websocket_gone_cleanup_failure_total,
    websocket_messages_sent_total,
    websocket_payload_size_bytes,
    websocket_post_duration_ms,
    websocket_post_failure_total,
    websocket_post_retry_exhausted_total,
    websocket_post_retry_total,
    websocket_post_success_total,
)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
ENVIRONMENT = os.getenv("ENVIRONMENT", "dev")

logger = logging.getLogger()
logger.setLevel(LOG_LEVEL)


def _env_int(
    name: str,
    default: int,
    minimum: int = 0,
    maximum: int | None = None,
) -> int:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    try:
        value = int(raw_value)
    except ValueError:
        logger.warning("invalid_integer_environment_variable: %s=%r", name, raw_value)
        return default
    if value < minimum or (maximum is not None and value > maximum):
        logger.warning("out_of_range_environment_variable: %s=%r", name, raw_value)
        return default
    return value


def _env_float(
    name: str,
    default: float,
    minimum: float = 0.0,
    maximum: float | None = None,
) -> float:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    try:
        value = float(raw_value)
    except ValueError:
        logger.warning("invalid_float_environment_variable: %s=%r", name, raw_value)
        return default
    if value < minimum or (maximum is not None and value > maximum):
        logger.warning("out_of_range_environment_variable: %s=%r", name, raw_value)
        return default
    return value


def _env_bool(name: str, default: bool) -> bool:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    return raw_value.strip().lower() in {"1", "true", "yes", "on"}


SNAPSHOTS_TABLE_NAME = os.environ["SNAPSHOTS_TABLE_NAME"]
SUBSCRIPTIONS_TABLE_NAME = os.environ["SUBSCRIPTIONS_TABLE_NAME"]
CONNECTIONS_TABLE_NAME = os.environ["CONNECTIONS_TABLE_NAME"]
WEBSOCKET_ENDPOINT_URL = os.environ["WEBSOCKET_ENDPOINT_URL"]

MAX_POST_WORKERS = _env_int("MAX_POST_WORKERS", 16, minimum=1, maximum=64)
MAX_CLEANUP_WORKERS = _env_int("MAX_CLEANUP_WORKERS", 8, minimum=1, maximum=32)
APIGW_MAX_POOL_CONNECTIONS = _env_int(
    "APIGW_MAX_POOL_CONNECTIONS",
    max(MAX_POST_WORKERS + 8, 24),
    minimum=MAX_POST_WORKERS,
)
DYNAMODB_MAX_POOL_CONNECTIONS = _env_int(
    "DYNAMODB_MAX_POOL_CONNECTIONS",
    max(MAX_CLEANUP_WORKERS + 8, 16),
    minimum=MAX_CLEANUP_WORKERS,
)

APIGW_CONNECT_TIMEOUT_SECONDS = _env_float(
    "APIGW_CONNECT_TIMEOUT_SECONDS",
    1.0,
    minimum=0.1,
)
APIGW_READ_TIMEOUT_SECONDS = _env_float(
    "APIGW_READ_TIMEOUT_SECONDS",
    2.0,
    minimum=0.1,
)
APIGW_MAX_ATTEMPTS = _env_int("APIGW_MAX_ATTEMPTS", 3, minimum=1, maximum=6)
APIGW_RETRY_BASE_DELAY_MS = _env_int(
    "APIGW_RETRY_BASE_DELAY_MS",
    40,
    minimum=0,
    maximum=5000,
)
APIGW_RETRY_MAX_DELAY_MS = _env_int(
    "APIGW_RETRY_MAX_DELAY_MS",
    250,
    minimum=0,
    maximum=10000,
)
APIGW_RETRY_JITTER_RATIO = _env_float(
    "APIGW_RETRY_JITTER_RATIO",
    0.25,
    minimum=0.0,
    maximum=1.0,
)

MAX_WEBSOCKET_PAYLOAD_BYTES = _env_int(
    "MAX_WEBSOCKET_PAYLOAD_BYTES",
    32000,
    minimum=1024,
)
MAX_FAILURE_LOGS_PER_JOB = _env_int(
    "MAX_FAILURE_LOGS_PER_JOB",
    20,
    minimum=0,
    maximum=1000,
)

FAIL_JOB_ON_POST_ERRORS = _env_bool("FAIL_JOB_ON_POST_ERRORS", False)
FAIL_JOB_IF_NO_DELIVERIES = _env_bool("FAIL_JOB_IF_NO_DELIVERIES", True)
TRACE_POST_TO_CONNECTION_CALLS = _env_bool(
    "TRACE_POST_TO_CONNECTION_CALLS",
    False,
)
ENABLE_OTEL_FLUSH = _env_bool("ENABLE_OTEL_FLUSH", True)

# Kept only to support controlled concurrency demonstrations; production value 0.
BACKBONE_TEST_DELAY_MS = _env_int(
    "BACKBONE_TEST_DELAY_MS",
    0,
    minimum=0,
    maximum=10000,
)


# ---------------------------------------------------------------------------
# AWS clients and reusable executors
# ---------------------------------------------------------------------------

_dynamodb_resource = boto3.resource("dynamodb")
_snapshots_table = _dynamodb_resource.Table(SNAPSHOTS_TABLE_NAME)
_subscriptions_table = _dynamodb_resource.Table(SUBSCRIPTIONS_TABLE_NAME)

_dynamodb_client = boto3.client(
    "dynamodb",
    config=Config(
        max_pool_connections=DYNAMODB_MAX_POOL_CONNECTIONS,
        connect_timeout=1.0,
        read_timeout=2.0,
        retries={"mode": "standard", "total_max_attempts": 3},
    ),
)

# SDK retries are intentionally disabled for postToConnection. The custom retry
# loop below records every retry and uses short bounded backoff.
_apigw_management = boto3.client(
    "apigatewaymanagementapi",
    endpoint_url=WEBSOCKET_ENDPOINT_URL,
    config=Config(
        max_pool_connections=APIGW_MAX_POOL_CONNECTIONS,
        connect_timeout=APIGW_CONNECT_TIMEOUT_SECONDS,
        read_timeout=APIGW_READ_TIMEOUT_SECONDS,
        retries={"mode": "standard", "total_max_attempts": 1},
    ),
)

_POST_EXECUTOR = ThreadPoolExecutor(
    max_workers=MAX_POST_WORKERS,
    thread_name_prefix="ws-post",
)
_CLEANUP_EXECUTOR = ThreadPoolExecutor(
    max_workers=MAX_CLEANUP_WORKERS,
    thread_name_prefix="ws-cleanup",
)
_SERIALIZER = TypeSerializer()


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class FanoutDeliveryError(RuntimeError):
    """At least one connection could not be delivered after bounded retries."""


class PayloadTooLargeError(RuntimeError):
    """Serialized WebSocket message exceeds the configured safe size."""


# ---------------------------------------------------------------------------
# Generic helpers
# ---------------------------------------------------------------------------


def epoch_ms_now() -> int:
    return int(time.time() * 1000)


def to_epoch_ms(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float, Decimal)):
        return int(value)
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        try:
            return int(float(stripped))
        except ValueError:
            try:
                normalized = stripped.replace("Z", "+00:00")
                return int(datetime.fromisoformat(normalized).timestamp() * 1000)
            except ValueError:
                return None
    return None


def json_safe(value: Any) -> Any:
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    if isinstance(value, dict):
        return {str(key): json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(item) for item in value]
    if isinstance(value, set):
        return sorted(json_safe(item) for item in value)
    return value


def serialize_item(item: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {key: _SERIALIZER.serialize(value) for key, value in item.items()}


def topic_type_from_topic(topic: str) -> str:
    if topic == "global":
        return "global"
    if topic == "top_pages":
        return "top_pages"
    if topic.startswith("wiki:"):
        return "wiki"
    return "unknown"


def log_json(level: str, message: str, **fields: Any) -> None:
    """Structured logs enriched with trace_id/span_id for Loki -> Tempo."""
    payload: dict[str, Any] = {"message": message, **fields}

    span_context = otel_trace.get_current_span().get_span_context()
    if span_context.is_valid:
        payload["trace_id"] = format(span_context.trace_id, "032x")
        payload["span_id"] = format(span_context.span_id, "016x")

    log_method = getattr(logger, level.lower(), logger.info)
    log_method(json.dumps(payload, default=str, separators=(",", ":")))


def _submit_with_context(
    executor: ThreadPoolExecutor,
    function: Any,
    *args: Any,
    **kwargs: Any,
) -> Future[Any]:
    context = copy_context()
    return executor.submit(context.run, function, *args, **kwargs)


def get_sqs_trace_carrier(record: dict[str, Any]) -> dict[str, str]:
    carrier: dict[str, str] = {}
    message_attributes = record.get("messageAttributes") or {}
    if not isinstance(message_attributes, dict):
        return carrier

    for key in ("traceparent", "tracestate", "baggage"):
        attribute = message_attributes.get(key)
        if not isinstance(attribute, dict):
            continue
        value = (
            attribute.get("stringValue")
            or attribute.get("StringValue")
            or attribute.get("value")
        )
        if value:
            carrier[key] = str(value)

    return carrier


def extract_parent_context(record: dict[str, Any] | None) -> Context | None:
    if record is None:
        return None
    carrier = get_sqs_trace_carrier(record)
    return propagate.extract(carrier) if carrier else None


def iter_input_jobs(
    event: dict[str, Any],
) -> Iterator[tuple[str | None, dict[str, Any], dict[str, Any] | None]]:
    records = event.get("Records")
    if isinstance(records, list):
        for record in records:
            body = record.get("body")
            if not isinstance(body, str):
                raise ValueError("SQS record body must be a JSON string")
            parsed = json.loads(body)
            if not isinstance(parsed, dict):
                raise ValueError("SQS record body must contain a JSON object")
            yield record.get("messageId"), parsed, record
        return

    yield None, event, None


# ---------------------------------------------------------------------------
# Snapshot and subscription reads
# ---------------------------------------------------------------------------


def load_snapshot(job: dict[str, Any]) -> tuple[dict[str, Any], float]:
    started_at = time.perf_counter()
    response = _snapshots_table.get_item(
        Key={
            "snapshot_id": job["snapshot_id"],
            "topic": job["topic"],
        },
        # Coordinator writes before publishing jobs. A consistent read removes
        # the small race in which the first Worker could miss the new item.
        ConsistentRead=True,
    )
    duration_ms = round((time.perf_counter() - started_at) * 1000, 2)
    broadcast_worker_snapshot_read_duration_ms.record(duration_ms)

    snapshot = response.get("Item")
    if snapshot is None:
        raise RuntimeError(
            f"Snapshot not found: {job['snapshot_id']} / {job['topic']}"
        )

    if str(snapshot.get("topic")) != str(job["topic"]):
        raise ValueError("Snapshot topic does not match job topic")

    snapshot_sequence = int(snapshot.get("sequence", job["sequence"]))
    if snapshot_sequence != int(job["sequence"]):
        raise ValueError("Snapshot sequence does not match job sequence")

    payload = snapshot.get("payload")
    if not isinstance(payload, dict):
        raise ValueError("Snapshot payload must be a DynamoDB Map")

    return {
        **snapshot,
        "payload": json_safe(payload),
    }, duration_ms


def query_active_connection_ids(topic_shard: str) -> tuple[list[str], float]:
    started_at = time.perf_counter()
    current_epoch = int(time.time())
    connection_ids: list[str] = []
    last_evaluated_key: dict[str, Any] | None = None

    while True:
        arguments: dict[str, Any] = {
            "KeyConditionExpression": Key("topic_shard").eq(topic_shard),
            "ProjectionExpression": "connection_id, #ttl",
            "ExpressionAttributeNames": {"#ttl": "ttl"},
            "ConsistentRead": False,
        }
        if last_evaluated_key:
            arguments["ExclusiveStartKey"] = last_evaluated_key

        response = _subscriptions_table.query(**arguments)

        for item in response.get("Items", []):
            connection_id = item.get("connection_id")
            if not connection_id:
                continue

            ttl = item.get("ttl")
            if ttl is not None:
                try:
                    if int(ttl) < current_epoch:
                        continue
                except (TypeError, ValueError):
                    log_json(
                        "WARNING",
                        "invalid_subscription_ttl_skipped",
                        topic_shard=topic_shard,
                        connection_id=str(connection_id),
                        ttl=ttl,
                    )
                    continue

            connection_ids.append(str(connection_id))

        last_evaluated_key = response.get("LastEvaluatedKey")
        if not last_evaluated_key:
            break

    unique_connection_ids = list(dict.fromkeys(connection_ids))
    duration_ms = round((time.perf_counter() - started_at) * 1000, 2)
    broadcast_worker_query_duration_ms.record(duration_ms)
    return unique_connection_ids, duration_ms


# ---------------------------------------------------------------------------
# postToConnection with bounded custom retries
# ---------------------------------------------------------------------------


def _client_error_details(error: ClientError) -> tuple[str | None, int | None]:
    response = error.response or {}
    error_code = response.get("Error", {}).get("Code")
    http_status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    return (
        str(error_code) if error_code is not None else None,
        int(http_status) if http_status is not None else None,
    )


def _is_gone_error(error_code: str | None, http_status: int | None) -> bool:
    return error_code in {"GoneException", "Gone"} or http_status == 410


def _is_retryable_client_error(
    error_code: str | None,
    http_status: int | None,
) -> bool:
    if http_status == 429 or (http_status is not None and 500 <= http_status <= 599):
        return True
    return error_code in {
        "TooManyRequestsException",
        "LimitExceededException",
        "ThrottlingException",
        "Throttling",
        "RequestLimitExceeded",
        "InternalServerErrorException",
        "ServiceUnavailableException",
    }


def _retry_delay_seconds(retry_number: int) -> float:
    # retry_number=1 means delay before the second network attempt.
    base_ms = min(
        APIGW_RETRY_MAX_DELAY_MS,
        APIGW_RETRY_BASE_DELAY_MS * (2 ** max(retry_number - 1, 0)),
    )
    if base_ms <= 0:
        return 0.0

    jitter_min = max(0.0, 1.0 - APIGW_RETRY_JITTER_RATIO)
    jitter_max = 1.0 + APIGW_RETRY_JITTER_RATIO
    return (base_ms * random.uniform(jitter_min, jitter_max)) / 1000.0


def _post_to_connection_worker(
    connection_id: str,
    prepared_message: dict[str, Any],
) -> dict[str, Any]:
    started_at = time.perf_counter()
    latest_attempt_at_ms: int | None = None

    for attempt in range(1, APIGW_MAX_ATTEMPTS + 1):
        latest_attempt_at_ms = epoch_ms_now()
        outbound_message = dict(prepared_message)
        outbound_message["server_send_attempt_at_ms"] = latest_attempt_at_ms

        payload = json.dumps(
            outbound_message,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")

        if len(payload) > MAX_WEBSOCKET_PAYLOAD_BYTES:
            return {
                "connection_id": connection_id,
                "status": "payload_too_large",
                "attempts": attempt,
                "retry_count": attempt - 1,
                "duration_ms": round((time.perf_counter() - started_at) * 1000, 2),
                "server_send_attempt_at_ms": latest_attempt_at_ms,
                "server_post_success_at_ms": None,
                "payload_size_bytes": len(payload),
                "error": f"payload size {len(payload)} exceeds limit",
                "error_type": "PayloadTooLargeError",
                "error_code": "PayloadTooLarge",
                "http_status": None,
            }

        try:
            instrumentation_context = (
                nullcontext()
                if TRACE_POST_TO_CONNECTION_CALLS
                else suppress_instrumentation()
            )
            with instrumentation_context:
                _apigw_management.post_to_connection(
                    ConnectionId=connection_id,
                    Data=payload,
                )

            return {
                "connection_id": connection_id,
                "status": "sent",
                "attempts": attempt,
                "retry_count": attempt - 1,
                "duration_ms": round((time.perf_counter() - started_at) * 1000, 2),
                "server_send_attempt_at_ms": latest_attempt_at_ms,
                "server_post_success_at_ms": epoch_ms_now(),
                "payload_size_bytes": len(payload),
                "error": None,
                "error_type": None,
                "error_code": None,
                "http_status": 200,
            }

        except ClientError as error:
            error_code, http_status = _client_error_details(error)

            if _is_gone_error(error_code, http_status):
                return {
                    "connection_id": connection_id,
                    "status": "gone",
                    "attempts": attempt,
                    "retry_count": attempt - 1,
                    "duration_ms": round((time.perf_counter() - started_at) * 1000, 2),
                    "server_send_attempt_at_ms": latest_attempt_at_ms,
                    "server_post_success_at_ms": None,
                    "payload_size_bytes": len(payload),
                    "error": error,
                    "error_type": type(error).__name__,
                    "error_code": error_code,
                    "http_status": http_status,
                }

            retryable = _is_retryable_client_error(error_code, http_status)
            if retryable and attempt < APIGW_MAX_ATTEMPTS:
                delay_seconds = _retry_delay_seconds(attempt)
                if delay_seconds > 0:
                    time.sleep(delay_seconds)
                continue

            return {
                "connection_id": connection_id,
                "status": "retry_exhausted" if retryable else "error",
                "attempts": attempt,
                "retry_count": attempt - 1,
                "duration_ms": round((time.perf_counter() - started_at) * 1000, 2),
                "server_send_attempt_at_ms": latest_attempt_at_ms,
                "server_post_success_at_ms": None,
                "payload_size_bytes": len(payload),
                "error": error,
                "error_type": type(error).__name__,
                "error_code": error_code,
                "http_status": http_status,
            }

        except (
            ConnectionClosedError,
            ConnectTimeoutError,
            EndpointConnectionError,
            ReadTimeoutError,
        ) as error:
            if attempt < APIGW_MAX_ATTEMPTS:
                delay_seconds = _retry_delay_seconds(attempt)
                if delay_seconds > 0:
                    time.sleep(delay_seconds)
                continue

            return {
                "connection_id": connection_id,
                "status": "retry_exhausted",
                "attempts": attempt,
                "retry_count": attempt - 1,
                "duration_ms": round((time.perf_counter() - started_at) * 1000, 2),
                "server_send_attempt_at_ms": latest_attempt_at_ms,
                "server_post_success_at_ms": None,
                "payload_size_bytes": len(payload),
                "error": error,
                "error_type": type(error).__name__,
                "error_code": None,
                "http_status": None,
            }

        except Exception as error:  # Unknown failures are not retried blindly.
            return {
                "connection_id": connection_id,
                "status": "exception",
                "attempts": attempt,
                "retry_count": attempt - 1,
                "duration_ms": round((time.perf_counter() - started_at) * 1000, 2),
                "server_send_attempt_at_ms": latest_attempt_at_ms,
                "server_post_success_at_ms": None,
                "payload_size_bytes": len(payload),
                "error": error,
                "error_type": type(error).__name__,
                "error_code": None,
                "http_status": None,
            }

    raise AssertionError("unreachable postToConnection retry state")


# ---------------------------------------------------------------------------
# HTTP 410 cleanup
# ---------------------------------------------------------------------------


def _extract_shard_id(topic_shard: str) -> int | None:
    marker = "#SHARD#"
    if marker not in topic_shard:
        return None
    try:
        return int(topic_shard.rsplit(marker, 1)[1])
    except ValueError:
        return None


def _cleanup_gone_connection(connection_id: str, current_topic_shard: str) -> int:
    response = _dynamodb_client.get_item(
        TableName=CONNECTIONS_TABLE_NAME,
        Key={"connection_id": {"S": connection_id}},
        ConsistentRead=True,
    )
    raw_item = response.get("Item")

    if not raw_item:
        # The disconnect handler may already have deleted the connection. Remove
        # the subscription currently encountered by this Worker idempotently.
        _dynamodb_client.delete_item(
            TableName=SUBSCRIPTIONS_TABLE_NAME,
            Key={
                "topic_shard": {"S": current_topic_shard},
                "connection_id": {"S": connection_id},
            },
        )
        return 1

    topics = [item["S"] for item in raw_item.get("topics", {}).get("L", []) if "S" in item]
    if not topics:
        topics = [current_topic_shard.split("#SHARD#", 1)[0].removeprefix("TOPIC#")]

    shard_id_attribute = raw_item.get("subscription_shard", {}).get("N")
    shard_id = (
        int(shard_id_attribute)
        if shard_id_attribute is not None
        else _extract_shard_id(current_topic_shard)
    )
    if shard_id is None:
        raise ValueError(f"Unable to resolve shard for stale connection {connection_id}")

    unique_topics = sorted({str(topic).strip().lower() for topic in topics if str(topic).strip()})
    transact_items: list[dict[str, Any]] = []

    for topic in unique_topics:
        transact_items.append(
            {
                "Delete": {
                    "TableName": SUBSCRIPTIONS_TABLE_NAME,
                    "Key": serialize_item(
                        {
                            "topic_shard": f"TOPIC#{topic}#SHARD#{shard_id:02d}",
                            "connection_id": connection_id,
                        }
                    ),
                }
            }
        )

    transact_items.append(
        {
            "Delete": {
                "TableName": CONNECTIONS_TABLE_NAME,
                "Key": serialize_item({"connection_id": connection_id}),
            }
        }
    )

    # MAX_TOPICS_PER_CONNECTION is 50, so this remains below the 100-action
    # TransactWriteItems limit.
    _dynamodb_client.transact_write_items(TransactItems=transact_items)
    return len(unique_topics)


def cleanup_gone_connections(
    connection_ids: list[str],
    current_topic_shard: str,
) -> dict[str, Any]:
    unique_ids = list(dict.fromkeys(connection_ids))
    if not unique_ids:
        return {"connections_cleaned": 0, "subscriptions_deleted": 0, "errors": 0, "duration_ms": 0.0}

    started_at = time.perf_counter()
    subscriptions_deleted = 0
    errors = 0

    futures = {
        _submit_with_context(
            _CLEANUP_EXECUTOR,
            _cleanup_gone_connection,
            connection_id,
            current_topic_shard,
        ): connection_id
        for connection_id in unique_ids
    }

    for future in as_completed(futures):
        connection_id = futures[future]
        try:
            subscriptions_deleted += int(future.result())
        except Exception as error:
            errors += 1
            websocket_gone_cleanup_failure_total.add(1, {"environment": ENVIRONMENT})
            log_json(
                "ERROR",
                "gone_connection_cleanup_failed",
                connection_id=connection_id,
                topic_shard=current_topic_shard,
                error_type=type(error).__name__,
                error_message=str(error),
            )

    duration_ms = round((time.perf_counter() - started_at) * 1000, 2)
    gone_cleanup_duration_ms.record(duration_ms, {"environment": ENVIRONMENT})

    return {
        "connections_cleaned": len(unique_ids) - errors,
        "subscriptions_deleted": subscriptions_deleted,
        "errors": errors,
        "duration_ms": duration_ms,
    }


# ---------------------------------------------------------------------------
# Result aggregation and freshness
# ---------------------------------------------------------------------------


def _record_post_result(
    result: dict[str, Any],
    *,
    topic: str,
    latest_event_timestamp_ms: int | None,
    oldest_event_timestamp_ms: int | None,
    aws_request_id: str | None,
    failure_log_index: int,
) -> None:
    topic_type = topic_type_from_topic(topic)
    metric_attributes = {
        "environment": ENVIRONMENT,
        "topic_type": topic_type,
    }

    websocket_post_duration_ms.record(
        float(result.get("duration_ms") or 0.0),
        metric_attributes,
    )

    retry_count = int(result.get("retry_count") or 0)
    if retry_count > 0:
        websocket_post_retry_total.add(retry_count, metric_attributes)

    status = str(result.get("status"))

    if status == "sent":
        websocket_post_success_total.add(1, metric_attributes)
        websocket_messages_sent_total.add(1, metric_attributes)

        success_at_ms = to_epoch_ms(result.get("server_post_success_at_ms"))
        if success_at_ms is None:
            return

        if latest_event_timestamp_ms is not None:
            freshness_ms = success_at_ms - latest_event_timestamp_ms
            if freshness_ms >= 0:
                event_to_dashboard_latency_ms.record(
                    freshness_ms,
                    metric_attributes,
                )
            else:
                log_json(
                    "WARNING",
                    "freshness_negative_latency_skipped",
                    aws_request_id=aws_request_id,
                    topic=topic,
                    latest_event_timestamp_ms=latest_event_timestamp_ms,
                    server_post_success_at_ms=success_at_ms,
                    latency_ms=freshness_ms,
                )

        if oldest_event_timestamp_ms is not None:
            oldest_freshness_ms = success_at_ms - oldest_event_timestamp_ms
            if oldest_freshness_ms >= 0:
                oldest_event_to_dashboard_latency_ms.record(
                    oldest_freshness_ms,
                    metric_attributes,
                )
        return

    if status == "gone":
        websocket_connection_gone_total.add(1, metric_attributes)
        return

    websocket_post_failure_total.add(
        1,
        {
            **metric_attributes,
            "error_type": str(result.get("error_type") or "UnknownError"),
        },
    )

    if status == "retry_exhausted":
        websocket_post_retry_exhausted_total.add(1, metric_attributes)

    if failure_log_index < MAX_FAILURE_LOGS_PER_JOB:
        log_json(
            "ERROR",
            "websocket_post_failed",
            aws_request_id=aws_request_id,
            connection_id=result.get("connection_id"),
            topic=topic,
            status=status,
            attempts=result.get("attempts"),
            error_type=result.get("error_type"),
            error_code=result.get("error_code"),
            http_status=result.get("http_status"),
            error_message=str(result.get("error")),
        )


def fanout_message(
    *,
    connection_ids: list[str],
    message: dict[str, Any],
    topic: str,
    topic_shard: str,
    aws_request_id: str | None,
) -> dict[str, Any]:
    attempted = len(connection_ids)
    topic_type = topic_type_from_topic(topic)
    metric_attributes = {
        "environment": ENVIRONMENT,
        "topic_type": topic_type,
    }

    if attempted == 0:
        return {
            "attempted": 0,
            "sent": 0,
            "gone": 0,
            "errors": 0,
            "retry_exhausted": 0,
            "total_retries": 0,
            "fanout_duration_ms": 0.0,
            "cleanup_duration_ms": 0.0,
            "payload_size_bytes": len(
                json.dumps(message, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
            ),
        }

    prepared_message = json_safe(message)
    baseline_size = len(
        json.dumps(prepared_message, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    )
    if baseline_size + 64 > MAX_WEBSOCKET_PAYLOAD_BYTES:
        raise PayloadTooLargeError(
            f"WebSocket payload is {baseline_size} bytes before send metadata; "
            f"configured limit is {MAX_WEBSOCKET_PAYLOAD_BYTES} bytes"
        )

    websocket_payload_size_bytes.record(baseline_size, metric_attributes)
    fanout_batch_size.record(attempted, metric_attributes)

    latest_event_timestamp_ms = to_epoch_ms(
        prepared_message.get("latest_event_timestamp_ms")
    )
    oldest_event_timestamp_ms = to_epoch_ms(
        prepared_message.get("oldest_event_timestamp_ms")
    )

    sent = 0
    gone_connection_ids: list[str] = []
    errors = 0
    retry_exhausted = 0
    total_retries = 0
    failure_log_index = 0
    started_at = time.perf_counter()

    with tracer.start_as_current_span("broadcast_worker.fanout") as span:
        span.set_attribute("messaging.destination.name", topic)
        span.set_attribute("broadcast.topic_shard", topic_shard)
        span.set_attribute("fanout.connections_attempted", attempted)
        span.set_attribute("fanout.max_post_workers", MAX_POST_WORKERS)

        futures = [
            _submit_with_context(
                _POST_EXECUTOR,
                _post_to_connection_worker,
                connection_id,
                prepared_message,
            )
            for connection_id in connection_ids
        ]

        for future in as_completed(futures):
            try:
                result = future.result()
            except BaseException as error:
                result = {
                    "connection_id": None,
                    "status": "exception",
                    "attempts": 1,
                    "retry_count": 0,
                    "duration_ms": 0.0,
                    "server_post_success_at_ms": None,
                    "error": error,
                    "error_type": type(error).__name__,
                    "error_code": None,
                    "http_status": None,
                }

            _record_post_result(
                result,
                topic=topic,
                latest_event_timestamp_ms=latest_event_timestamp_ms,
                oldest_event_timestamp_ms=oldest_event_timestamp_ms,
                aws_request_id=aws_request_id,
                failure_log_index=failure_log_index,
            )

            status = result.get("status")
            total_retries += int(result.get("retry_count") or 0)

            if status == "sent":
                sent += 1
            elif status == "gone":
                gone_connection_ids.append(str(result["connection_id"]))
            else:
                errors += 1
                failure_log_index += 1
                if status == "retry_exhausted":
                    retry_exhausted += 1

        fanout_only_duration_ms = round((time.perf_counter() - started_at) * 1000, 2)
        fanout_duration_ms.record(fanout_only_duration_ms, metric_attributes)

        cleanup_result = cleanup_gone_connections(
            gone_connection_ids,
            topic_shard,
        )

        span.set_attribute("fanout.connections_sent", sent)
        span.set_attribute("fanout.connections_gone", len(gone_connection_ids))
        span.set_attribute("fanout.connections_failed", errors)
        span.set_attribute("fanout.retry_exhausted", retry_exhausted)
        span.set_attribute("fanout.total_retries", total_retries)
        span.set_attribute("fanout.duration_ms", fanout_only_duration_ms)
        span.set_attribute("fanout.cleanup_duration_ms", cleanup_result["duration_ms"])

        if errors > 0:
            span.set_status(Status(StatusCode.ERROR, f"{errors} post failures"))
        else:
            span.set_status(Status(StatusCode.OK))

    log_json(
        "INFO",
        "fanout_completed",
        aws_request_id=aws_request_id,
        topic=topic,
        topic_shard=topic_shard,
        attempted=attempted,
        sent=sent,
        gone=len(gone_connection_ids),
        errors=errors,
        retry_exhausted=retry_exhausted,
        total_retries=total_retries,
        max_post_workers=MAX_POST_WORKERS,
        fanout_duration_ms=fanout_only_duration_ms,
        gone_cleanup_duration_ms=cleanup_result["duration_ms"],
        cleanup_errors=cleanup_result["errors"],
    )

    return {
        "attempted": attempted,
        "sent": sent,
        "gone": len(gone_connection_ids),
        "errors": errors,
        "retry_exhausted": retry_exhausted,
        "total_retries": total_retries,
        "fanout_duration_ms": fanout_only_duration_ms,
        "cleanup_duration_ms": cleanup_result["duration_ms"],
        "payload_size_bytes": baseline_size,
    }


# ---------------------------------------------------------------------------
# Job processing
# ---------------------------------------------------------------------------


def validate_job(job: dict[str, Any]) -> None:
    required_fields = (
        "snapshot_id",
        "sequence",
        "topic",
        "topic_shard",
        "subscription_shard",
    )
    missing = [field for field in required_fields if field not in job]
    if missing:
        raise ValueError("Missing job fields: " + ",".join(missing))

    topic = str(job["topic"])
    expected_topic_shard = (
        f"TOPIC#{topic}#SHARD#{int(job['subscription_shard']):02d}"
    )
    if str(job["topic_shard"]) != expected_topic_shard:
        raise ValueError(
            f"Invalid topic_shard: expected {expected_topic_shard}, "
            f"received {job['topic_shard']}"
        )


def process_job(
    job: dict[str, Any],
    *,
    aws_request_id: str | None,
    message_group_id: str | None,
) -> dict[str, Any]:
    job_started_at = time.perf_counter()
    worker_received_at_ms = epoch_ms_now()
    validate_job(job)

    topic = str(job["topic"])
    topic_type = topic_type_from_topic(topic)
    metric_attributes = {
        "environment": ENVIRONMENT,
        "topic_type": topic_type,
    }

    job_created_at_ms = to_epoch_ms(job.get("created_at"))
    queue_delay_ms: int | None = None
    if job_created_at_ms is not None:
        queue_delay_ms = max(0, worker_received_at_ms - job_created_at_ms)
        broadcast_job_queue_delay_ms.record(queue_delay_ms, metric_attributes)

    log_json(
        "INFO",
        "broadcast_worker_job_started",
        aws_request_id=aws_request_id,
        snapshot_id=job["snapshot_id"],
        sequence=job["sequence"],
        topic=topic,
        topic_shard=job["topic_shard"],
        subscription_shard=job["subscription_shard"],
        message_group_id=message_group_id,
        queue_delay_ms=queue_delay_ms,
    )

    with tracer.start_as_current_span("broadcast_worker.load_snapshot") as span:
        snapshot, snapshot_read_duration_ms = load_snapshot(job)
        span.set_attribute("broadcast.snapshot_id", str(job["snapshot_id"]))
        span.set_attribute("db.operation.duration_ms", snapshot_read_duration_ms)

    with tracer.start_as_current_span("broadcast_worker.query_subscriptions") as span:
        connection_ids, query_duration_ms = query_active_connection_ids(
            str(job["topic_shard"])
        )
        span.set_attribute("broadcast.topic_shard", str(job["topic_shard"]))
        span.set_attribute("broadcast.subscription_count", len(connection_ids))
        span.set_attribute("db.operation.duration_ms", query_duration_ms)

    broadcast_worker_subscriptions_found.record(
        len(connection_ids),
        metric_attributes,
    )

    if BACKBONE_TEST_DELAY_MS > 0:
        time.sleep(BACKBONE_TEST_DELAY_MS / 1000.0)

    payload = dict(snapshot["payload"])
    payload["worker_job_received_at_ms"] = worker_received_at_ms
    payload["worker_fanout_started_at_ms"] = epoch_ms_now()

    fanout_result = fanout_message(
        connection_ids=connection_ids,
        message=payload,
        topic=topic,
        topic_shard=str(job["topic_shard"]),
        aws_request_id=aws_request_id,
    )

    duration_ms = round((time.perf_counter() - job_started_at) * 1000, 2)
    broadcast_worker_duration_ms.record(duration_ms, metric_attributes)

    result = {
        "snapshot_id": job["snapshot_id"],
        "sequence": int(job["sequence"]),
        "topic": topic,
        "topic_shard": job["topic_shard"],
        "subscription_shard": int(job["subscription_shard"]),
        "subscription_count": len(connection_ids),
        "snapshot_found": True,
        "snapshot_read_duration_ms": snapshot_read_duration_ms,
        "query_duration_ms": query_duration_ms,
        "queue_delay_ms": queue_delay_ms,
        "worker_duration_ms": duration_ms,
        **fanout_result,
    }

    log_json(
        "INFO",
        "broadcast_worker_fanout_summary",
        aws_request_id=aws_request_id,
        message_group_id=message_group_id,
        latest_event_timestamp_ms=payload.get("latest_event_timestamp_ms"),
        oldest_event_timestamp_ms=payload.get("oldest_event_timestamp_ms"),
        **result,
    )

    should_fail_job = (
        fanout_result["errors"] > 0
        and (
            FAIL_JOB_ON_POST_ERRORS
            or (
                FAIL_JOB_IF_NO_DELIVERIES
                and fanout_result["sent"] == 0
                and fanout_result["gone"] == 0
            )
        )
    )

    if should_fail_job:
        raise FanoutDeliveryError(
            f"{fanout_result['errors']} postToConnection calls failed for "
            f"{job['topic_shard']} after bounded retries"
        )

    broadcast_worker_jobs_total.add(1, metric_attributes)
    log_json(
        "INFO",
        "broadcast_worker_job_completed",
        aws_request_id=aws_request_id,
        message_group_id=message_group_id,
        **result,
    )
    return result


# ---------------------------------------------------------------------------
# Lambda entry point
# ---------------------------------------------------------------------------


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    invocation_started_at = time.perf_counter()
    aws_request_id = getattr(context, "aws_request_id", None)
    batch_item_failures: list[dict[str, str]] = []
    direct_results: list[dict[str, Any]] = []

    try:
        for message_id, job, sqs_record in iter_input_jobs(event):
            parent_context = extract_parent_context(sqs_record)
            message_group_id = None
            if sqs_record is not None:
                message_group_id = (
                    sqs_record.get("attributes", {}).get("MessageGroupId")
                )

            with tracer.start_as_current_span(
                "broadcast_worker.process_job",
                context=parent_context,
            ) as span:
                span.set_attribute(
                    "faas.trigger",
                    "sqs" if sqs_record is not None else "manual",
                )
                span.set_attribute("messaging.system", "aws_sqs")
                span.set_attribute("messaging.operation", "process")
                if message_id:
                    span.set_attribute("messaging.message.id", message_id)
                if message_group_id:
                    span.set_attribute("messaging.message.group_id", message_group_id)

                try:
                    result = process_job(
                        job,
                        aws_request_id=aws_request_id,
                        message_group_id=message_group_id,
                    )

                    span.set_attribute("broadcast.snapshot_id", str(result["snapshot_id"]))
                    span.set_attribute("broadcast.topic", str(result["topic"]))
                    span.set_attribute("broadcast.topic_shard", str(result["topic_shard"]))
                    span.set_attribute("broadcast.subscription_count", result["subscription_count"])
                    span.set_attribute("broadcast.connections_sent", result["sent"])
                    span.set_attribute("broadcast.connections_gone", result["gone"])
                    span.set_attribute("broadcast.connections_failed", result["errors"])
                    span.set_attribute("broadcast.worker_duration_ms", result["worker_duration_ms"])
                    span.set_status(Status(StatusCode.OK))
                    direct_results.append(result)

                except Exception as error:
                    topic = str(job.get("topic", "unknown"))
                    broadcast_worker_jobs_failed_total.add(
                        1,
                        {
                            "environment": ENVIRONMENT,
                            "topic_type": topic_type_from_topic(topic),
                            "error_type": type(error).__name__,
                        },
                    )

                    span.record_exception(error)
                    span.set_status(Status(StatusCode.ERROR, str(error)))

                    # This log is intentionally emitted while the job span is
                    # current, so trace_id and span_id are present in Loki.
                    log_json(
                        "ERROR",
                        "broadcast_worker_job_failed",
                        aws_request_id=aws_request_id,
                        message_id=message_id,
                        snapshot_id=job.get("snapshot_id"),
                        topic=job.get("topic"),
                        topic_shard=job.get("topic_shard"),
                        error_type=type(error).__name__,
                        error_message=str(error),
                    )

                    if message_id:
                        batch_item_failures.append({"itemIdentifier": message_id})
                    else:
                        raise

        if event.get("Records") is not None:
            return {"batchItemFailures": batch_item_failures}

        return {"status": "success", "results": direct_results}

    finally:
        business_duration_ms = round(
            (time.perf_counter() - invocation_started_at) * 1000,
            2,
        )
        flush_result = None
        if ENABLE_OTEL_FLUSH:
            flush_result = flush_otel()

        log_json(
            "INFO",
            "broadcast_worker_invocation_completed",
            aws_request_id=aws_request_id,
            business_duration_ms=business_duration_ms,
            otel_flush=flush_result,
        )
