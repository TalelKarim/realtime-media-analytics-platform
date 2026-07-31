from __future__ import annotations

import json
import logging
import os
import random
import time
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from contextlib import nullcontext
from contextvars import copy_context
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Iterator

import boto3
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer
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
except ImportError:  # pragma: no cover - compatibility fallback
    def suppress_instrumentation():
        return nullcontext()

from .observability import (
    broadcast_job_queue_delay_ms,
    broadcast_worker_connections_found,
    broadcast_worker_duration_ms,
    broadcast_worker_jobs_failed_total,
    broadcast_worker_jobs_total,
    broadcast_worker_manifest_read_duration_ms,
    broadcast_worker_query_duration_ms,
    broadcast_worker_snapshot_read_duration_ms,
    broadcast_worker_topics_loaded,
    event_to_dashboard_latency_ms,
    fanout_batch_size,
    fanout_duration_ms,
    flush_otel,
    gone_cleanup_duration_ms,
    oldest_event_to_dashboard_latency_ms,
    stale_broadcast_jobs_skipped_total,
    tracer,
    websocket_batch_topics_count,
    websocket_chunks_per_connection,
    websocket_connection_gone_total,
    websocket_delivery_total,
    websocket_gone_cleanup_failure_total,
    websocket_messages_sent_total,
    websocket_payload_build_failure_total,
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
CONNECTIONS_TABLE_NAME = os.environ["CONNECTIONS_TABLE_NAME"]
CONNECTION_SHARD_INDEX_NAME = os.getenv(
    "CONNECTION_SHARD_INDEX_NAME",
    "connection-shard-index",
)
SUBSCRIPTIONS_TABLE_NAME = os.getenv("SUBSCRIPTIONS_TABLE_NAME", "")
WEBSOCKET_ENDPOINT_URL = os.environ["WEBSOCKET_ENDPOINT_URL"]

MAX_POST_WORKERS = _env_int("MAX_POST_WORKERS", 16, minimum=1, maximum=64)
MAX_CLEANUP_WORKERS = _env_int("MAX_CLEANUP_WORKERS", 4, minimum=1, maximum=32)
SNAPSHOT_READ_WORKERS = _env_int("SNAPSHOT_READ_WORKERS", 4, minimum=1, maximum=16)
APIGW_MAX_POOL_CONNECTIONS = _env_int(
    "APIGW_MAX_POOL_CONNECTIONS",
    max(MAX_POST_WORKERS + 8, 24),
    minimum=MAX_POST_WORKERS,
)
DYNAMODB_MAX_POOL_CONNECTIONS = _env_int(
    "DYNAMODB_MAX_POOL_CONNECTIONS",
    max(MAX_POST_WORKERS + MAX_CLEANUP_WORKERS + SNAPSHOT_READ_WORKERS + 8, 32),
    minimum=16,
)

APIGW_CONNECT_TIMEOUT_SECONDS = _env_float(
    "APIGW_CONNECT_TIMEOUT_SECONDS", 1.0, minimum=0.1
)
APIGW_READ_TIMEOUT_SECONDS = _env_float(
    "APIGW_READ_TIMEOUT_SECONDS", 2.0, minimum=0.1
)
APIGW_MAX_ATTEMPTS = _env_int("APIGW_MAX_ATTEMPTS", 3, minimum=1, maximum=6)
APIGW_RETRY_BASE_DELAY_MS = _env_int(
    "APIGW_RETRY_BASE_DELAY_MS", 40, minimum=0, maximum=5000
)
APIGW_RETRY_MAX_DELAY_MS = _env_int(
    "APIGW_RETRY_MAX_DELAY_MS", 250, minimum=0, maximum=10000
)
APIGW_RETRY_JITTER_RATIO = _env_float(
    "APIGW_RETRY_JITTER_RATIO", 0.25, minimum=0.0, maximum=1.0
)
DYNAMODB_BATCH_GET_MAX_RETRIES = _env_int(
    "DYNAMODB_BATCH_GET_MAX_RETRIES", 5, minimum=1, maximum=10
)
MAX_WEBSOCKET_PAYLOAD_BYTES = _env_int(
    "MAX_WEBSOCKET_PAYLOAD_BYTES", 30000, minimum=1024, maximum=32000
)
CHUNK_SIZE_SAFETY_BYTES = _env_int(
    "CHUNK_SIZE_SAFETY_BYTES", 512, minimum=128, maximum=4096
)
MAX_FAILURE_LOGS_PER_JOB = _env_int(
    "MAX_FAILURE_LOGS_PER_JOB", 20, minimum=0, maximum=1000
)
TRACE_POST_TO_CONNECTION_CALLS = _env_bool(
    "TRACE_POST_TO_CONNECTION_CALLS", False
)
ENABLE_OTEL_FLUSH = _env_bool("ENABLE_OTEL_FLUSH", True)
BACKBONE_TEST_DELAY_MS = _env_int(
    "BACKBONE_TEST_DELAY_MS", 0, minimum=0, maximum=10000
)

# Individual WebSocket delivery errors are terminal for that connection/chunk,
# not for the complete SQS shard job. Structural errors still fail the job.
FAIL_JOB_ON_POST_ERRORS = _env_bool("FAIL_JOB_ON_POST_ERRORS", False)
FAIL_JOB_IF_NO_DELIVERIES = _env_bool("FAIL_JOB_IF_NO_DELIVERIES", False)


# ---------------------------------------------------------------------------
# AWS clients and executors reused across warm invocations
# ---------------------------------------------------------------------------

_dynamodb_resource = boto3.resource("dynamodb")
_snapshots_table = _dynamodb_resource.Table(SNAPSHOTS_TABLE_NAME)
_connections_table = _dynamodb_resource.Table(CONNECTIONS_TABLE_NAME)
_dynamodb_client = boto3.client(
    "dynamodb",
    config=Config(
        max_pool_connections=DYNAMODB_MAX_POOL_CONNECTIONS,
        connect_timeout=1.0,
        read_timeout=3.0,
        retries={"mode": "standard", "total_max_attempts": 3},
    ),
)
_apigw_management = boto3.client(
    "apigatewaymanagementapi",
    endpoint_url=WEBSOCKET_ENDPOINT_URL,
    config=Config(
        max_pool_connections=APIGW_MAX_POOL_CONNECTIONS,
        connect_timeout=APIGW_CONNECT_TIMEOUT_SECONDS,
        read_timeout=APIGW_READ_TIMEOUT_SECONDS,
        # Retries are explicit below so every retry is observable and bounded.
        retries={"mode": "standard", "total_max_attempts": 1},
    ),
)

_POST_EXECUTOR = ThreadPoolExecutor(
    max_workers=MAX_POST_WORKERS,
    thread_name_prefix="connection-sender",
)
_CLEANUP_EXECUTOR = ThreadPoolExecutor(
    max_workers=MAX_CLEANUP_WORKERS,
    thread_name_prefix="ws-cleanup",
)
_SERIALIZER = TypeSerializer()
_DESERIALIZER = TypeDeserializer()


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class PayloadTooLargeError(RuntimeError):
    """One topic update cannot fit in the configured WebSocket frame limit."""


# ---------------------------------------------------------------------------
# Generic helpers
# ---------------------------------------------------------------------------


def epoch_ms_now() -> int:
    return int(time.time() * 1000)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace(
        "+00:00", "Z"
    )


def to_int(value: Any, default: int = 0) -> int:
    if value is None or isinstance(value, bool):
        return default
    if isinstance(value, Decimal):
        return int(value)
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        try:
            return int(float(value.strip()))
        except (ValueError, TypeError):
            return default
    return default


def iso_to_epoch_ms(value: Any) -> int:
    if not isinstance(value, str) or not value.strip():
        return 0
    try:
        normalized = value.strip().replace("Z", "+00:00")
        parsed = datetime.fromisoformat(normalized)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return int(parsed.timestamp() * 1000)
    except ValueError:
        return 0


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


def deserialize_item(item: dict[str, Any]) -> dict[str, Any]:
    return {key: _DESERIALIZER.deserialize(value) for key, value in item.items()}


def normalize_topic(topic: Any) -> str:
    return str(topic).strip().lower()


def topic_type_from_topic(topic: str) -> str:
    if topic == "global":
        return "global"
    if topic == "top_pages":
        return "top_pages"
    if topic.startswith("wiki:"):
        return "wiki"
    return "unknown"


def log_json(level: str, message: str, **fields: Any) -> None:
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
    attributes = record.get("messageAttributes") or {}
    if not isinstance(attributes, dict):
        return carrier
    for key in ("traceparent", "tracestate", "baggage"):
        attribute = attributes.get(key)
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


def job_cursor(job: dict[str, Any]) -> tuple[int, int]:
    sequence = to_int(job.get("sequence"), 0)
    window_epoch_ms = to_int(job.get("aggregation_window_epoch_ms"), 0)
    if window_epoch_ms <= 0:
        window_epoch_ms = iso_to_epoch_ms(job.get("aggregation_window"))
    return sequence, window_epoch_ms


def pointer_cursor(pointer: dict[str, Any]) -> tuple[int, int]:
    return (
        to_int(pointer.get("sequence"), 0),
        to_int(pointer.get("aggregation_window_epoch_ms"), 0),
    )


# ---------------------------------------------------------------------------
# Latest pointer, manifest and connections
# ---------------------------------------------------------------------------


def load_latest_pointer() -> tuple[dict[str, Any], float]:
    started_at = time.perf_counter()
    response = _snapshots_table.get_item(
        Key={"snapshot_id": "LATEST", "topic": "MANIFEST"},
        ConsistentRead=True,
    )
    duration_ms = round((time.perf_counter() - started_at) * 1000, 2)
    pointer = response.get("Item")
    if not isinstance(pointer, dict):
        raise RuntimeError("LATEST manifest pointer does not exist")
    return pointer, duration_ms


def classify_job_against_latest(
    job: dict[str, Any],
    pointer: dict[str, Any],
) -> str:
    current = job_cursor(job)
    latest = pointer_cursor(pointer)
    if current < latest:
        return "stale"
    if current > latest:
        return "ahead"
    if str(pointer.get("manifest_id", "")) != str(job.get("manifest_id", "")):
        return "stale"
    return "current"


def load_manifest(job: dict[str, Any]) -> tuple[dict[str, Any], float]:
    started_at = time.perf_counter()
    response = _snapshots_table.get_item(
        Key={"snapshot_id": str(job["manifest_id"]), "topic": "MANIFEST"},
        ConsistentRead=True,
    )
    duration_ms = round((time.perf_counter() - started_at) * 1000, 2)
    broadcast_worker_manifest_read_duration_ms.record(
        duration_ms, {"environment": ENVIRONMENT}
    )
    manifest = response.get("Item")
    if not isinstance(manifest, dict):
        raise RuntimeError(f"Manifest not found: {job['manifest_id']}")
    if str(manifest.get("item_type")) != "MANIFEST":
        raise ValueError("Manifest item_type is invalid")
    if to_int(manifest.get("sequence"), -1) != to_int(job.get("sequence"), -2):
        raise ValueError("Manifest sequence does not match the Worker job")
    if str(manifest.get("snapshot_id")) != str(job.get("manifest_id")):
        raise ValueError("Manifest ID does not match the Worker job")
    snapshots = manifest.get("snapshots")
    if not isinstance(snapshots, dict):
        raise ValueError("Manifest snapshots must be a DynamoDB Map")
    return json_safe(manifest), duration_ms


def query_connections_by_shard(
    connection_shard: str,
) -> tuple[list[dict[str, Any]], float]:
    started_at = time.perf_counter()
    current_epoch = int(time.time())
    connections: list[dict[str, Any]] = []
    last_evaluated_key: dict[str, Any] | None = None

    while True:
        arguments: dict[str, Any] = {
            "IndexName": CONNECTION_SHARD_INDEX_NAME,
            "KeyConditionExpression": Key("connection_shard").eq(connection_shard),
            "ProjectionExpression": (
                "connection_id, connection_shard, subscription_shard, topics, #ttl"
            ),
            "ExpressionAttributeNames": {"#ttl": "ttl"},
        }
        if last_evaluated_key:
            arguments["ExclusiveStartKey"] = last_evaluated_key
        response = _connections_table.query(**arguments)
        for item in response.get("Items", []):
            connection_id = item.get("connection_id")
            if not connection_id:
                continue
            ttl = to_int(item.get("ttl"), 0)
            if ttl and ttl < current_epoch:
                continue
            raw_topics = item.get("topics") or ["global"]
            topics = list(
                dict.fromkeys(
                    normalize_topic(topic)
                    for topic in raw_topics
                    if isinstance(topic, str) and topic.strip()
                )
            )
            if "global" not in topics:
                topics.insert(0, "global")
            connections.append(
                {
                    "connection_id": str(connection_id),
                    "connection_shard": str(
                        item.get("connection_shard", connection_shard)
                    ),
                    "subscription_shard": to_int(
                        item.get("subscription_shard"),
                        to_int(connection_shard.removeprefix("SHARD#"), 0),
                    ),
                    "topics": topics,
                    "ttl": ttl or None,
                }
            )
        last_evaluated_key = response.get("LastEvaluatedKey")
        if not last_evaluated_key:
            break

    # GSI propagation can briefly expose duplicates during updates; deduplicate
    # defensively by connection_id without changing the stable Query order.
    unique = list({item["connection_id"]: item for item in connections}.values())
    duration_ms = round((time.perf_counter() - started_at) * 1000, 2)
    broadcast_worker_query_duration_ms.record(
        duration_ms, {"environment": ENVIRONMENT}
    )
    return unique, duration_ms


# ---------------------------------------------------------------------------
# Snapshot BatchGetItem
# ---------------------------------------------------------------------------


def _chunks(values: list[Any], size: int) -> list[list[Any]]:
    return [values[index : index + size] for index in range(0, len(values), size)]


def _batch_get_snapshot_chunk(
    references: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    request_items: dict[str, Any] = {
        SNAPSHOTS_TABLE_NAME: {
            "Keys": [
                {
                    "snapshot_id": {"S": str(reference["snapshot_id"])},
                    "topic": {"S": str(reference["topic"])},
                }
                for reference in references
            ],
            "ConsistentRead": True,
        }
    }
    items: list[dict[str, Any]] = []
    attempt = 0

    while request_items:
        response = _dynamodb_client.batch_get_item(RequestItems=request_items)
        items.extend(
            deserialize_item(raw_item)
            for raw_item in response.get("Responses", {}).get(
                SNAPSHOTS_TABLE_NAME, []
            )
        )
        unprocessed = response.get("UnprocessedKeys") or {}
        request_items = {
            table_name: value
            for table_name, value in unprocessed.items()
            if value.get("Keys")
        }
        if not request_items:
            break
        attempt += 1
        if attempt >= DYNAMODB_BATCH_GET_MAX_RETRIES:
            raise RuntimeError("DynamoDB BatchGetItem left unprocessed snapshot keys")
        time.sleep(min(0.5, 0.025 * (2**attempt)) * random.uniform(0.8, 1.2))

    return items


def load_snapshots(
    manifest: dict[str, Any],
    required_topics: set[str],
) -> tuple[dict[str, dict[str, Any]], float]:
    started_at = time.perf_counter()
    raw_references = manifest.get("snapshots") or {}
    references: list[dict[str, Any]] = []
    for topic in sorted(required_topics):
        reference = raw_references.get(topic)
        if not isinstance(reference, dict):
            continue
        references.append(
            {
                "snapshot_id": str(reference["snapshot_id"]),
                "topic": topic,
            }
        )

    snapshot_items: list[dict[str, Any]] = []
    reference_chunks = _chunks(references, 100)
    if len(reference_chunks) <= 1:
        if reference_chunks:
            snapshot_items.extend(_batch_get_snapshot_chunk(reference_chunks[0]))
    else:
        with ThreadPoolExecutor(
            max_workers=min(SNAPSHOT_READ_WORKERS, len(reference_chunks)),
            thread_name_prefix="snapshot-batch-get",
        ) as executor:
            futures = [
                _submit_with_context(executor, _batch_get_snapshot_chunk, chunk)
                for chunk in reference_chunks
            ]
            for future in as_completed(futures):
                snapshot_items.extend(future.result())

    snapshots: dict[str, dict[str, Any]] = {}
    for snapshot in snapshot_items:
        topic = normalize_topic(snapshot.get("topic"))
        if not topic:
            continue
        if str(snapshot.get("item_type")) != "SNAPSHOT":
            raise ValueError(f"Unexpected item_type for snapshot topic {topic}")
        if to_int(snapshot.get("sequence"), -1) != to_int(
            manifest.get("sequence"), -2
        ):
            raise ValueError(f"Snapshot sequence mismatch for topic {topic}")
        payload = snapshot.get("payload")
        if not isinstance(payload, dict):
            raise ValueError(f"Snapshot payload is not a Map for topic {topic}")
        snapshots[topic] = {
            **json_safe(snapshot),
            "payload": json_safe(payload),
        }

    missing_topics = sorted(required_topics.difference(snapshots))
    if missing_topics:
        raise RuntimeError(
            "Required snapshots were not returned: " + ",".join(missing_topics)
        )

    duration_ms = round((time.perf_counter() - started_at) * 1000, 2)
    broadcast_worker_snapshot_read_duration_ms.record(
        duration_ms, {"environment": ENVIRONMENT}
    )
    return snapshots, duration_ms


# ---------------------------------------------------------------------------
# Per-connection batching and chunking
# ---------------------------------------------------------------------------


def build_topic_update(topic: str, snapshot: dict[str, Any]) -> dict[str, Any]:
    return {
        "topic": topic,
        "data": snapshot["payload"],
        "latest_event_timestamp_ms": snapshot.get("latest_event_timestamp_ms"),
        "oldest_event_timestamp_ms": snapshot.get("oldest_event_timestamp_ms"),
    }


def build_envelope(
    *,
    manifest: dict[str, Any],
    updates: list[dict[str, Any]],
    chunk_index: int,
    chunk_count: int,
) -> dict[str, Any]:
    aggregation_window = str(manifest.get("aggregation_window", ""))
    return {
        "type": "stats.batch_update",
        "schema_version": 3,
        "sequence": to_int(manifest.get("sequence"), 0),
        "manifest_id": str(manifest.get("snapshot_id")),
        "aggregation_window": aggregation_window,
        "aggregation_window_epoch_ms": iso_to_epoch_ms(aggregation_window),
        "chunk_index": chunk_index,
        "chunk_count": chunk_count,
        "updates": updates,
    }


def serialized_payload_size(message: dict[str, Any]) -> int:
    candidate = {
        **message,
        "server_send_attempt_at_ms": 9_999_999_999_999,
    }
    return len(
        json.dumps(candidate, ensure_ascii=False, separators=(",", ":")).encode(
            "utf-8"
        )
    )


def chunk_updates(
    *,
    manifest: dict[str, Any],
    updates: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if not updates:
        return []

    safe_limit = MAX_WEBSOCKET_PAYLOAD_BYTES - CHUNK_SIZE_SAFETY_BYTES
    grouped_updates: list[list[dict[str, Any]]] = []
    current: list[dict[str, Any]] = []

    for update in updates:
        candidate_updates = [*current, update]
        candidate = build_envelope(
            manifest=manifest,
            updates=candidate_updates,
            chunk_index=9999,
            chunk_count=9999,
        )
        if serialized_payload_size(candidate) <= safe_limit:
            current = candidate_updates
            continue

        if not current:
            topic = str(update.get("topic", "unknown"))
            raise PayloadTooLargeError(
                f"One topic update cannot fit in {safe_limit} bytes: {topic}"
            )

        grouped_updates.append(current)
        current = [update]
        single_candidate = build_envelope(
            manifest=manifest,
            updates=current,
            chunk_index=9999,
            chunk_count=9999,
        )
        if serialized_payload_size(single_candidate) > safe_limit:
            topic = str(update.get("topic", "unknown"))
            raise PayloadTooLargeError(
                f"One topic update cannot fit in {safe_limit} bytes: {topic}"
            )

    if current:
        grouped_updates.append(current)

    chunk_count = len(grouped_updates)
    prepared_chunks: list[dict[str, Any]] = []
    for index, chunk_topic_updates in enumerate(grouped_updates):
        message = build_envelope(
            manifest=manifest,
            updates=chunk_topic_updates,
            chunk_index=index,
            chunk_count=chunk_count,
        )
        latest_references = [
            to_int(update.get("latest_event_timestamp_ms"), 0)
            for update in chunk_topic_updates
            if to_int(update.get("latest_event_timestamp_ms"), 0) > 0
        ]
        oldest_references = [
            to_int(update.get("oldest_event_timestamp_ms"), 0)
            for update in chunk_topic_updates
            if to_int(update.get("oldest_event_timestamp_ms"), 0) > 0
        ]
        prepared_chunks.append(
            {
                "message": message,
                # Primary freshness: least-fresh topic among the latest events.
                "latest_reference_ms": min(latest_references)
                if latest_references
                else None,
                # Diagnostic: oldest source event included in the chunk.
                "oldest_reference_ms": min(oldest_references)
                if oldest_references
                else None,
                "topic_count": len(chunk_topic_updates),
            }
        )
    return prepared_chunks


def prepare_connection_messages(
    *,
    connections: list[dict[str, Any]],
    manifest: dict[str, Any],
    snapshots: dict[str, dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    prepared: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    snapshot_topics = set(snapshots)
    for connection in connections:
        relevant_topics = [
            topic for topic in connection["topics"] if topic in snapshot_topics
        ]
        if not relevant_topics:
            continue
        updates = [
            build_topic_update(topic, snapshots[topic])
            for topic in relevant_topics
        ]
        try:
            chunks = chunk_updates(manifest=manifest, updates=updates)
        except PayloadTooLargeError as error:
            failures.append(
                {
                    "connection_id": connection["connection_id"],
                    "connection_shard": connection["connection_shard"],
                    "topic_count": len(relevant_topics),
                    "error_type": type(error).__name__,
                    "error_message": str(error),
                }
            )
            continue
        if not chunks:
            continue
        prepared.append({**connection, "chunks": chunks})
    return prepared, failures


# ---------------------------------------------------------------------------
# postToConnection and 410 cleanup
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
    base_ms = min(
        APIGW_RETRY_MAX_DELAY_MS,
        APIGW_RETRY_BASE_DELAY_MS * (2 ** max(retry_number - 1, 0)),
    )
    if base_ms <= 0:
        return 0.0
    return (
        base_ms
        * random.uniform(
            max(0.0, 1.0 - APIGW_RETRY_JITTER_RATIO),
            1.0 + APIGW_RETRY_JITTER_RATIO,
        )
        / 1000.0
    )


def _send_one_chunk(
    connection_id: str,
    prepared_chunk: dict[str, Any],
) -> dict[str, Any]:
    started_at = time.perf_counter()
    message = prepared_chunk["message"]

    for attempt in range(1, APIGW_MAX_ATTEMPTS + 1):
        attempt_at_ms = epoch_ms_now()
        payload = json.dumps(
            {**message, "server_send_attempt_at_ms": attempt_at_ms},
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")

        if len(payload) > MAX_WEBSOCKET_PAYLOAD_BYTES:
            return {
                **prepared_chunk,
                "status": "payload_too_large",
                "attempts": attempt,
                "retry_count": attempt - 1,
                "duration_ms": round((time.perf_counter() - started_at) * 1000, 2),
                "payload_size_bytes": len(payload),
                "success_at_ms": None,
                "error_type": "PayloadTooLargeError",
                "error_message": (
                    f"payload size {len(payload)} exceeds "
                    f"{MAX_WEBSOCKET_PAYLOAD_BYTES}"
                ),
            }

        try:
            span_context = (
                tracer.start_as_current_span("broadcast_worker.post_to_connection")
                if TRACE_POST_TO_CONNECTION_CALLS
                else nullcontext()
            )
            with span_context:
                with suppress_instrumentation():
                    _apigw_management.post_to_connection(
                        ConnectionId=connection_id,
                        Data=payload,
                    )
            return {
                **prepared_chunk,
                "status": "success",
                "attempts": attempt,
                "retry_count": attempt - 1,
                "duration_ms": round((time.perf_counter() - started_at) * 1000, 2),
                "payload_size_bytes": len(payload),
                "success_at_ms": epoch_ms_now(),
                "error_type": None,
                "error_message": None,
            }
        except ClientError as error:
            error_code, http_status = _client_error_details(error)
            if _is_gone_error(error_code, http_status):
                return {
                    **prepared_chunk,
                    "status": "gone",
                    "attempts": attempt,
                    "retry_count": attempt - 1,
                    "duration_ms": round((time.perf_counter() - started_at) * 1000, 2),
                    "payload_size_bytes": len(payload),
                    "success_at_ms": None,
                    "error_type": error_code or type(error).__name__,
                    "error_message": str(error),
                    "http_status": http_status,
                }
            retryable = _is_retryable_client_error(error_code, http_status)
            if not retryable or attempt >= APIGW_MAX_ATTEMPTS:
                return {
                    **prepared_chunk,
                    "status": "error",
                    "attempts": attempt,
                    "retry_count": attempt - 1,
                    "duration_ms": round((time.perf_counter() - started_at) * 1000, 2),
                    "payload_size_bytes": len(payload),
                    "success_at_ms": None,
                    "error_type": error_code or type(error).__name__,
                    "error_message": str(error),
                    "http_status": http_status,
                    "retry_exhausted": retryable,
                }
        except (
            ConnectionClosedError,
            ConnectTimeoutError,
            EndpointConnectionError,
            ReadTimeoutError,
        ) as error:
            if attempt >= APIGW_MAX_ATTEMPTS:
                return {
                    **prepared_chunk,
                    "status": "error",
                    "attempts": attempt,
                    "retry_count": attempt - 1,
                    "duration_ms": round((time.perf_counter() - started_at) * 1000, 2),
                    "payload_size_bytes": len(payload),
                    "success_at_ms": None,
                    "error_type": type(error).__name__,
                    "error_message": str(error),
                    "retry_exhausted": True,
                }

        time.sleep(_retry_delay_seconds(attempt))

    raise RuntimeError("unreachable postToConnection retry state")


def _send_connection(connection: dict[str, Any]) -> dict[str, Any]:
    chunk_results: list[dict[str, Any]] = []
    for prepared_chunk in connection["chunks"]:
        result = _send_one_chunk(connection["connection_id"], prepared_chunk)
        chunk_results.append(result)
        if result["status"] in {"gone", "error", "payload_too_large"}:
            # Preserve per-connection ordering and do not continue after a
            # terminal error: later chunks would produce a partial batch.
            break
    return {
        "connection_id": connection["connection_id"],
        "connection_shard": connection["connection_shard"],
        "subscription_shard": connection["subscription_shard"],
        "topics": connection["topics"],
        "chunks_planned": len(connection["chunks"]),
        "chunks": chunk_results,
    }


def cleanup_gone_connection(connection_result: dict[str, Any]) -> dict[str, Any]:
    started_at = time.perf_counter()
    connection_id = str(connection_result["connection_id"])
    topics = sorted(set(connection_result.get("topics") or []))
    shard_id = to_int(connection_result.get("subscription_shard"), 0)

    try:
        if SUBSCRIPTIONS_TABLE_NAME:
            topic_chunks = _chunks(topics, 99) or [[]]
            for index, topic_chunk in enumerate(topic_chunks):
                actions: list[dict[str, Any]] = [
                    {
                        "Delete": {
                            "TableName": SUBSCRIPTIONS_TABLE_NAME,
                            "Key": serialize_item(
                                {
                                    "topic_shard": (
                                        f"TOPIC#{topic}#SHARD#{shard_id:02d}"
                                    ),
                                    "connection_id": connection_id,
                                }
                            ),
                        }
                    }
                    for topic in topic_chunk
                ]
                if index == len(topic_chunks) - 1:
                    actions.append(
                        {
                            "Delete": {
                                "TableName": CONNECTIONS_TABLE_NAME,
                                "Key": serialize_item(
                                    {"connection_id": connection_id}
                                ),
                            }
                        }
                    )
                _dynamodb_client.transact_write_items(TransactItems=actions)
        else:
            _connections_table.delete_item(Key={"connection_id": connection_id})

        return {
            "connection_id": connection_id,
            "status": "success",
            "duration_ms": round((time.perf_counter() - started_at) * 1000, 2),
        }
    except Exception as error:  # cleanup failure must not replay the fan-out
        return {
            "connection_id": connection_id,
            "status": "error",
            "duration_ms": round((time.perf_counter() - started_at) * 1000, 2),
            "error_type": type(error).__name__,
            "error_message": str(error),
        }


def fanout_connections(
    prepared_connections: list[dict[str, Any]],
    *,
    connection_shard: str,
    aws_request_id: str | None,
) -> dict[str, Any]:
    started_at = time.perf_counter()
    future_to_connection = {
        _submit_with_context(_POST_EXECUTOR, _send_connection, connection): connection
        for connection in prepared_connections
    }

    connection_results: list[dict[str, Any]] = []
    for future in as_completed(future_to_connection):
        connection = future_to_connection[future]
        try:
            connection_results.append(future.result())
        except Exception as error:  # defensive: sender normally returns errors
            connection_results.append(
                {
                    "connection_id": connection["connection_id"],
                    "connection_shard": connection_shard,
                    "subscription_shard": connection["subscription_shard"],
                    "topics": connection["topics"],
                    "chunks_planned": len(connection["chunks"]),
                    "chunks": [
                        {
                            "status": "error",
                            "retry_count": 0,
                            "duration_ms": 0.0,
                            "payload_size_bytes": 0,
                            "success_at_ms": None,
                            "error_type": type(error).__name__,
                            "error_message": str(error),
                            "topic_count": 0,
                            "latest_reference_ms": None,
                            "oldest_reference_ms": None,
                        }
                    ],
                }
            )

    sent_chunks = 0
    failed_chunks = 0
    gone_connections: list[dict[str, Any]] = []
    total_retries = 0
    retry_exhausted = 0
    failure_logs = 0

    for connection_result in connection_results:
        connection_gone = False
        websocket_chunks_per_connection.record(
            connection_result["chunks_planned"],
            {"environment": ENVIRONMENT},
        )
        for chunk in connection_result["chunks"]:
            status = str(chunk.get("status"))
            retries = to_int(chunk.get("retry_count"), 0)
            total_retries += retries
            if retries:
                websocket_post_retry_total.add(
                    retries, {"environment": ENVIRONMENT}
                )
            websocket_post_duration_ms.record(
                float(chunk.get("duration_ms") or 0),
                {"environment": ENVIRONMENT},
            )
            websocket_payload_size_bytes.record(
                to_int(chunk.get("payload_size_bytes"), 0),
                {"environment": ENVIRONMENT},
            )
            websocket_batch_topics_count.record(
                to_int(chunk.get("topic_count"), 0),
                {"environment": ENVIRONMENT},
            )

            if status == "success":
                sent_chunks += 1
                websocket_delivery_total.add(
                    1,
                    {"environment": ENVIRONMENT, "result": "success"},
                )
                websocket_messages_sent_total.add(1, {"environment": ENVIRONMENT})
                success_at_ms = to_int(chunk.get("success_at_ms"), 0)
                latest_reference_ms = to_int(
                    chunk.get("latest_reference_ms"), 0
                )
                oldest_reference_ms = to_int(
                    chunk.get("oldest_reference_ms"), 0
                )
                if success_at_ms and latest_reference_ms:
                    event_to_dashboard_latency_ms.record(
                        max(0, success_at_ms - latest_reference_ms),
                        {"environment": ENVIRONMENT},
                    )
                if success_at_ms and oldest_reference_ms:
                    oldest_event_to_dashboard_latency_ms.record(
                        max(0, success_at_ms - oldest_reference_ms),
                        {"environment": ENVIRONMENT},
                    )
            elif status == "gone":
                if not connection_gone:
                    connection_gone = True
                    gone_connections.append(connection_result)
                    websocket_delivery_total.add(
                        1,
                        {"environment": ENVIRONMENT, "result": "gone"},
                    )
            else:
                failed_chunks += 1
                if chunk.get("retry_exhausted"):
                    retry_exhausted += 1
                    delivery_result = "retry_exhausted"
                else:
                    delivery_result = "failure"
                websocket_delivery_total.add(
                    1,
                    {"environment": ENVIRONMENT, "result": delivery_result},
                )
                if failure_logs < MAX_FAILURE_LOGS_PER_JOB:
                    failure_logs += 1
                    log_json(
                        "WARNING",
                        "websocket_chunk_delivery_failed",
                        aws_request_id=aws_request_id,
                        connection_id=connection_result["connection_id"],
                        connection_shard=connection_shard,
                        error_type=chunk.get("error_type"),
                        error_message=chunk.get("error_message"),
                        retries=retries,
                    )

    cleanup_started_at = time.perf_counter()
    cleanup_futures = [
        _submit_with_context(_CLEANUP_EXECUTOR, cleanup_gone_connection, result)
        for result in gone_connections
    ]
    cleanup_failures = 0
    for future in as_completed(cleanup_futures):
        cleanup_result = future.result()
        if cleanup_result["status"] != "success":
            cleanup_failures += 1
            websocket_gone_cleanup_failure_total.add(
                1, {"environment": ENVIRONMENT}
            )
            log_json(
                "WARNING",
                "websocket_gone_cleanup_failed",
                aws_request_id=aws_request_id,
                **cleanup_result,
            )
    cleanup_duration = round((time.perf_counter() - cleanup_started_at) * 1000, 2)
    gone_cleanup_duration_ms.record(
        cleanup_duration, {"environment": ENVIRONMENT}
    )

    duration_ms = round((time.perf_counter() - started_at) * 1000, 2)
    fanout_duration_ms.record(duration_ms, {"environment": ENVIRONMENT})
    fanout_batch_size.record(
        len(prepared_connections), {"environment": ENVIRONMENT}
    )
    return {
        "connections_attempted": len(prepared_connections),
        "chunks_sent": sent_chunks,
        "chunks_failed": failed_chunks,
        "connections_gone": len(gone_connections),
        "cleanup_failures": cleanup_failures,
        "total_retries": total_retries,
        "retry_exhausted": retry_exhausted,
        "fanout_duration_ms": duration_ms,
        "cleanup_duration_ms": cleanup_duration,
    }


# ---------------------------------------------------------------------------
# Job processing
# ---------------------------------------------------------------------------


def validate_job(job: dict[str, Any]) -> None:
    required_fields = (
        "schema_version",
        "message_type",
        "manifest_id",
        "sequence",
        "shard_id",
        "connection_shard",
        "aggregation_window",
    )
    missing = [field for field in required_fields if field not in job]
    if missing:
        raise ValueError("Missing job fields: " + ",".join(missing))
    if to_int(job.get("schema_version"), 0) != 3:
        raise ValueError("Worker only accepts schema_version=3")
    if str(job.get("message_type")) != "broadcast.shard.job":
        raise ValueError("Unsupported Worker message_type")
    expected_shard = f"SHARD#{to_int(job.get('shard_id'), -1):02d}"
    if str(job.get("connection_shard")) != expected_shard:
        raise ValueError(
            f"Invalid connection_shard: expected {expected_shard}, "
            f"received {job.get('connection_shard')}"
        )
    if job_cursor(job)[0] <= 0 or job_cursor(job)[1] <= 0:
        raise ValueError("Job cursor is invalid")


def _stale_result(
    *,
    job: dict[str, Any],
    stage: str,
    pointer: dict[str, Any],
    job_started_at: float,
) -> dict[str, Any]:
    duration_ms = round((time.perf_counter() - job_started_at) * 1000, 2)
    result = {
        "status": "stale_skipped",
        "stage": stage,
        "sequence": to_int(job.get("sequence"), 0),
        "manifest_id": str(job.get("manifest_id")),
        "connection_shard": str(job.get("connection_shard")),
        "latest_sequence": to_int(pointer.get("sequence"), 0),
        "latest_manifest_id": str(pointer.get("manifest_id")),
        "worker_duration_ms": duration_ms,
    }
    log_json("INFO", "stale_broadcast_job_skipped", **result)
    return result


def process_job(
    job: dict[str, Any],
    *,
    aws_request_id: str | None,
    message_group_id: str | None,
) -> dict[str, Any]:
    job_started_at = time.perf_counter()
    worker_received_at_ms = epoch_ms_now()
    validate_job(job)
    connection_shard = str(job["connection_shard"])

    if message_group_id and message_group_id != connection_shard:
        raise ValueError(
            f"SQS MessageGroupId {message_group_id} does not match "
            f"{connection_shard}"
        )

    created_at_ms = to_int(job.get("created_at_ms"), 0)
    queue_delay_ms = (
        max(0, worker_received_at_ms - created_at_ms) if created_at_ms else None
    )
    if queue_delay_ms is not None:
        broadcast_job_queue_delay_ms.record(
            queue_delay_ms, {"environment": ENVIRONMENT}
        )

    log_json(
        "INFO",
        "broadcast_worker_job_started",
        aws_request_id=aws_request_id,
        message_group_id=message_group_id,
        sequence=job["sequence"],
        manifest_id=job["manifest_id"],
        connection_shard=connection_shard,
        queue_delay_ms=queue_delay_ms,
    )

    # Check #1: avoid all expensive reads when the job was already superseded.
    with tracer.start_as_current_span("broadcast_worker.check_latest_before_load"):
        latest_pointer, _ = load_latest_pointer()
    classification = classify_job_against_latest(job, latest_pointer)
    if classification == "stale":
        return _stale_result(
            job=job,
            stage="before_load",
            pointer=latest_pointer,
            job_started_at=job_started_at,
        )
    if classification == "ahead":
        raise RuntimeError("Worker job is ahead of the strongly consistent LATEST pointer")

    with tracer.start_as_current_span("broadcast_worker.load_manifest") as span:
        manifest, manifest_duration_ms = load_manifest(job)
        span.set_attribute("broadcast.manifest_id", str(job["manifest_id"]))
        span.set_attribute("db.operation.duration_ms", manifest_duration_ms)

    with tracer.start_as_current_span("broadcast_worker.query_connections") as span:
        connections, query_duration_ms = query_connections_by_shard(
            connection_shard
        )
        span.set_attribute("broadcast.connection_shard", connection_shard)
        span.set_attribute("broadcast.connection_count", len(connections))
        span.set_attribute("db.operation.duration_ms", query_duration_ms)

    broadcast_worker_connections_found.record(
        len(connections), {"environment": ENVIRONMENT}
    )

    manifest_topics = set((manifest.get("snapshots") or {}).keys())
    required_topics = {
        topic
        for connection in connections
        for topic in connection["topics"]
        if topic in manifest_topics
    }

    with tracer.start_as_current_span("broadcast_worker.batch_get_snapshots") as span:
        snapshots, snapshot_read_duration_ms = load_snapshots(
            manifest, required_topics
        )
        span.set_attribute("broadcast.snapshot_topic_count", len(snapshots))
        span.set_attribute("db.operation.duration_ms", snapshot_read_duration_ms)

    broadcast_worker_topics_loaded.record(
        len(snapshots), {"environment": ENVIRONMENT}
    )

    with tracer.start_as_current_span("broadcast_worker.build_connection_batches") as span:
        prepared_connections, payload_build_failures = prepare_connection_messages(
            connections=connections,
            manifest=manifest,
            snapshots=snapshots,
        )
        chunks_planned = sum(
            len(connection["chunks"]) for connection in prepared_connections
        )
        span.set_attribute(
            "broadcast.connections_with_updates", len(prepared_connections)
        )
        span.set_attribute("broadcast.chunks_planned", chunks_planned)
        span.set_attribute(
            "broadcast.payload_build_failures", len(payload_build_failures)
        )

    for failure in payload_build_failures[:MAX_FAILURE_LOGS_PER_JOB]:
        websocket_payload_build_failure_total.add(
            1, {"environment": ENVIRONMENT}
        )
        log_json(
            "WARNING",
            "websocket_payload_build_failed",
            aws_request_id=aws_request_id,
            **failure,
        )

    if BACKBONE_TEST_DELAY_MS > 0:
        time.sleep(BACKBONE_TEST_DELAY_MS / 1000.0)

    # Check #2: the job may have become stale while connections/snapshots were
    # being prepared. This check occurs immediately before any WebSocket send.
    with tracer.start_as_current_span("broadcast_worker.check_latest_before_fanout"):
        latest_pointer_before_fanout, _ = load_latest_pointer()
    classification = classify_job_against_latest(
        job, latest_pointer_before_fanout
    )
    if classification == "stale":
        return _stale_result(
            job=job,
            stage="before_fanout",
            pointer=latest_pointer_before_fanout,
            job_started_at=job_started_at,
        )
    if classification == "ahead":
        raise RuntimeError("Worker job became ahead of LATEST before fan-out")

    with tracer.start_as_current_span("broadcast_worker.fanout") as span:
        fanout_result = fanout_connections(
            prepared_connections,
            connection_shard=connection_shard,
            aws_request_id=aws_request_id,
        )
        span.set_attribute(
            "broadcast.connections_attempted",
            fanout_result["connections_attempted"],
        )
        span.set_attribute("broadcast.chunks_sent", fanout_result["chunks_sent"])
        span.set_attribute(
            "broadcast.chunks_failed", fanout_result["chunks_failed"]
        )

    duration_ms = round((time.perf_counter() - job_started_at) * 1000, 2)
    broadcast_worker_duration_ms.record(
        duration_ms, {"environment": ENVIRONMENT}
    )

    should_fail_job = (
        fanout_result["chunks_failed"] > 0
        and (
            FAIL_JOB_ON_POST_ERRORS
            or (
                FAIL_JOB_IF_NO_DELIVERIES
                and fanout_result["chunks_sent"] == 0
                and fanout_result["connections_gone"] == 0
            )
        )
    )
    if should_fail_job:
        raise RuntimeError("Configured policy requested a complete shard job retry")

    result = {
        "status": "success",
        "sequence": to_int(job["sequence"], 0),
        "manifest_id": str(job["manifest_id"]),
        "connection_shard": connection_shard,
        "connections_found": len(connections),
        "connections_with_updates": len(prepared_connections),
        "topics_loaded": len(snapshots),
        "chunks_planned": sum(
            len(connection["chunks"]) for connection in prepared_connections
        ),
        "payload_build_failures": len(payload_build_failures),
        "manifest_read_duration_ms": manifest_duration_ms,
        "query_duration_ms": query_duration_ms,
        "snapshot_read_duration_ms": snapshot_read_duration_ms,
        "queue_delay_ms": queue_delay_ms,
        "worker_duration_ms": duration_ms,
        **fanout_result,
    }
    broadcast_worker_jobs_total.add(
        1, {"environment": ENVIRONMENT, "result": "success"}
    )
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
    should_flush_metrics = False

    try:
        for message_id, job, sqs_record in iter_input_jobs(event):
            parent_context = extract_parent_context(sqs_record)
            message_group_id = None
            if sqs_record is not None:
                message_group_id = (
                    sqs_record.get("attributes", {}).get("MessageGroupId")
                )

            with tracer.start_as_current_span(
                "broadcast_worker.process_shard",
                context=parent_context,
            ) as span:
                span.set_attribute(
                    "faas.trigger", "sqs" if sqs_record is not None else "manual"
                )
                span.set_attribute("messaging.system", "aws_sqs")
                span.set_attribute("messaging.operation", "process")
                if message_id:
                    span.set_attribute("messaging.message.id", message_id)
                if message_group_id:
                    span.set_attribute(
                        "messaging.message.group_id", message_group_id
                    )

                try:
                    result = process_job(
                        job,
                        aws_request_id=aws_request_id,
                        message_group_id=message_group_id,
                    )
                    span.set_attribute(
                        "broadcast.manifest_id", str(result.get("manifest_id"))
                    )
                    span.set_attribute(
                        "broadcast.connection_shard",
                        str(result.get("connection_shard")),
                    )
                    span.set_attribute("broadcast.result", str(result.get("status")))
                    span.set_status(Status(StatusCode.OK))
                    direct_results.append(result)
                    if result.get("status") != "stale_skipped":
                        should_flush_metrics = True
                except Exception as error:
                    should_flush_metrics = True
                    broadcast_worker_jobs_total.add(
                        1,
                        {"environment": ENVIRONMENT, "result": "failed"},
                    )
                    span.record_exception(error)
                    span.set_status(Status(StatusCode.ERROR, str(error)))
                    log_json(
                        "ERROR",
                        "broadcast_worker_job_failed",
                        aws_request_id=aws_request_id,
                        message_id=message_id,
                        manifest_id=job.get("manifest_id"),
                        sequence=job.get("sequence"),
                        connection_shard=job.get("connection_shard"),
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
            (time.perf_counter() - invocation_started_at) * 1000, 2
        )
        flush_result = (
            flush_otel(
                flush_metrics=should_flush_metrics,
                flush_traces=True,
            )
            if ENABLE_OTEL_FLUSH
            else None
        )
        log_json(
            "INFO",
            "broadcast_worker_invocation_completed",
            aws_request_id=aws_request_id,
            business_duration_ms=business_duration_ms,
            otel_flush=flush_result,
            completed_at=now_iso(),
        )
