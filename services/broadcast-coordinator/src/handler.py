import hashlib
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Iterator

import boto3
from opentelemetry import propagate
from opentelemetry.trace import Status, StatusCode

from .observability import flush_otel, meter, tracer


LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logger = logging.getLogger()
logger.setLevel(LOG_LEVEL)


SNAPSHOTS_TABLE_NAME = os.environ["SNAPSHOTS_TABLE_NAME"]
BROADCAST_JOBS_QUEUE_URL = os.environ["BROADCAST_JOBS_QUEUE_URL"]

SUBSCRIPTION_SHARD_COUNT = int(
    os.getenv(
        "SUBSCRIPTION_SHARD_COUNT",
        "20",
    )
)

SNAPSHOT_TTL_SECONDS = int(
    os.getenv(
        "SNAPSHOT_TTL_SECONDS",
        "900",
    )
)

ENABLE_OTEL_FLUSH = (
    os.getenv(
        "ENABLE_OTEL_FLUSH",
        "true",
    ).lower()
    == "true"
)


dynamodb = boto3.resource("dynamodb")
snapshots_table = dynamodb.Table(SNAPSHOTS_TABLE_NAME)

sqs_client = boto3.client("sqs")


coordinator_signals_total = meter.create_counter(
    name="broadcast_coordinator_signals_total",
    unit="1",
    description="Broadcast signals processed by the Coordinator.",
)

coordinator_signals_failed_total = meter.create_counter(
    name="broadcast_coordinator_signals_failed_total",
    unit="1",
    description="Broadcast signals that failed in the Coordinator.",
)

broadcast_snapshots_created_total = meter.create_counter(
    name="broadcast_snapshots_created_total",
    unit="1",
    description="Test snapshots created by the Coordinator.",
)

broadcast_jobs_created_total = meter.create_counter(
    name="broadcast_jobs_created_total",
    unit="1",
    description="Fan-out jobs created by the Coordinator.",
)

broadcast_coordinator_duration_ms = meter.create_histogram(
    name="broadcast_coordinator_duration_ms",
    unit="ms",
    description="Coordinator signal processing duration.",
)


def log_json(
    level: str,
    message: str,
    **fields: Any,
) -> None:
    payload = {
        "message": message,
        **fields,
    }

    log_method = getattr(
        logger,
        level.lower(),
        logger.info,
    )

    log_method(
        json.dumps(
            payload,
            default=str,
            separators=(",", ":"),
        )
    )


def iso_to_epoch_ms(value: str) -> int:
    normalized = value.replace(
        "Z",
        "+00:00",
    )

    timestamp = datetime.fromisoformat(
        normalized
    )

    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(
            tzinfo=timezone.utc
        )

    return int(timestamp.timestamp() * 1000)


def now_iso() -> str:
    return datetime.now(
        timezone.utc
    ).isoformat().replace(
        "+00:00",
        "Z",
    )


def get_sqs_trace_carrier(
    record: dict[str, Any],
) -> dict[str, str]:
    carrier: dict[str, str] = {}

    message_attributes = record.get(
        "messageAttributes",
        {},
    )

    for key in (
        "traceparent",
        "tracestate",
        "baggage",
    ):
        attribute = message_attributes.get(key)

        if not isinstance(attribute, dict):
            continue

        value = (
            attribute.get("stringValue")
            or attribute.get("StringValue")
        )

        if value:
            carrier[key] = value

    return carrier


def build_trace_message_attributes() -> dict[str, dict[str, str]]:
    carrier: dict[str, str] = {}

    propagate.inject(carrier)

    message_attributes: dict[str, dict[str, str]] = {}

    for key in (
        "traceparent",
        "tracestate",
        "baggage",
    ):
        value = carrier.get(key)

        if value:
            message_attributes[key] = {
                "DataType": "String",
                "StringValue": value,
            }

    return message_attributes


def iter_input_records(
    event: dict[str, Any],
) -> Iterator[tuple[str | None, dict[str, Any], dict[str, Any] | None]]:
    records = event.get("Records")

    if isinstance(records, list):
        for record in records:
            body = record.get("body")

            if not isinstance(body, str):
                raise ValueError(
                    "SQS record body must be a JSON string"
                )

            yield (
                record.get("messageId"),
                json.loads(body),
                record,
            )

        return

    yield (
        None,
        event,
        None,
    )


def build_snapshot_id(
    sequence: int,
    aggregation_window: str,
) -> str:
    aggregation_window_ms = iso_to_epoch_ms(
        aggregation_window
    )

    return (
        f"SNAPSHOT#{sequence}"
        f"#WINDOW#{aggregation_window_ms}"
    )


def build_test_snapshot(
    snapshot_id: str,
    topic: str,
    sequence: int,
    broadcast_window: str,
    aggregation_window: str,
) -> dict[str, Any]:
    created_at = now_iso()

    return {
        "snapshot_id": snapshot_id,
        "topic": topic,
        "sequence": sequence,
        "broadcast_window": broadcast_window,
        "aggregation_window": aggregation_window,
        "created_at": created_at,
        "payload": {
            "type": "stats.update.test",
            "topic": topic,
            "sequence": sequence,
            "snapshot_id": snapshot_id,
            "timestamp": created_at,
            "data": {
                "message": (
                    "Broadcasting V2 backbone test"
                )
            },
        },
        "ttl": int(time.time()) + SNAPSHOT_TTL_SECONDS,
    }


def build_message_deduplication_id(
    snapshot_id: str,
    topic: str,
    shard_id: int,
) -> str:
    raw_value = (
        f"{snapshot_id}|{topic}|{shard_id}"
    )

    return hashlib.sha256(
        raw_value.encode("utf-8")
    ).hexdigest()


def build_jobs(
    snapshot_id: str,
    sequence: int,
    topic: str,
    aggregation_window: str,
    broadcast_window: str,
) -> list[dict[str, Any]]:
    jobs: list[dict[str, Any]] = []

    for shard_id in range(
        SUBSCRIPTION_SHARD_COUNT
    ):
        topic_shard = (
            f"TOPIC#{topic}"
            f"#SHARD#{shard_id:02d}"
        )

        jobs.append(
            {
                "schema_version": 1,
                "message_type": (
                    "broadcast.fanout.job.test"
                ),
                "snapshot_id": snapshot_id,
                "sequence": sequence,
                "topic": topic,
                "subscription_shard": shard_id,
                "topic_shard": topic_shard,
                "aggregation_window": aggregation_window,
                "broadcast_window": broadcast_window,
                "created_at": now_iso(),
            }
        )

    return jobs


def send_jobs(
    jobs: list[dict[str, Any]],
) -> int:
    sent_count = 0

    for batch_start in range(
        0,
        len(jobs),
        10,
    ):
        batch_jobs = jobs[
            batch_start : batch_start + 10
        ]

        entries: list[dict[str, Any]] = []

        for index, job in enumerate(
            batch_jobs,
            start=batch_start,
        ):
            trace_attributes = (
                build_trace_message_attributes()
            )

            entry: dict[str, Any] = {
                "Id": f"job-{index:03d}",
                "MessageBody": json.dumps(
                    job,
                    separators=(",", ":"),
                ),
                "MessageGroupId": job[
                    "topic_shard"
                ],
                "MessageDeduplicationId": (
                    build_message_deduplication_id(
                        snapshot_id=job[
                            "snapshot_id"
                        ],
                        topic=job["topic"],
                        shard_id=job[
                            "subscription_shard"
                        ],
                    )
                ),
            }

            if trace_attributes:
                entry["MessageAttributes"] = (
                    trace_attributes
                )

            entries.append(entry)

        response = sqs_client.send_message_batch(
            QueueUrl=BROADCAST_JOBS_QUEUE_URL,
            Entries=entries,
        )

        failed = response.get(
            "Failed",
            [],
        )

        if failed:
            raise RuntimeError(
                "Some broadcast jobs failed to publish: "
                + json.dumps(
                    failed,
                    default=str,
                )
            )

        sent_count += len(
            response.get(
                "Successful",
                [],
            )
        )

    return sent_count


def process_signal(
    signal: dict[str, Any],
    aws_request_id: str | None,
) -> dict[str, Any]:
    message_type = signal.get(
        "message_type",
        "aggregates.updated",
    )

    if not str(message_type).startswith(
        "aggregates.updated"
    ):
        raise ValueError(
            f"Unsupported message_type: {message_type}"
        )

    broadcast_window = signal.get(
        "broadcast_window"
    ) or now_iso()

    sequence = int(
        signal.get(
            "sequence",
            iso_to_epoch_ms(
                broadcast_window
            ),
        )
    )

    aggregation_windows = signal.get(
        "aggregation_windows"
    )

    if not aggregation_windows:
        aggregation_windows = [
            broadcast_window
        ]

    topics = signal.get(
        "updated_topics"
    ) or ["global"]

    topics = sorted(
        {
            str(topic)
            for topic in topics
            if topic
        }
    )

    created_snapshots = 0
    created_jobs = 0

    for aggregation_window in aggregation_windows:
        for topic in topics:
            snapshot_id = build_snapshot_id(
                sequence=sequence,
                aggregation_window=aggregation_window,
            )

            snapshot = build_test_snapshot(
                snapshot_id=snapshot_id,
                topic=topic,
                sequence=sequence,
                broadcast_window=broadcast_window,
                aggregation_window=aggregation_window,
            )

            snapshots_table.put_item(
                Item=snapshot
            )

            created_snapshots += 1

            broadcast_snapshots_created_total.add(
                1,
                {
                    "broadcast.topic": topic,
                },
            )

            jobs = build_jobs(
                snapshot_id=snapshot_id,
                sequence=sequence,
                topic=topic,
                aggregation_window=aggregation_window,
                broadcast_window=broadcast_window,
            )

            sent_jobs = send_jobs(jobs)

            created_jobs += sent_jobs

            broadcast_jobs_created_total.add(
                sent_jobs,
                {
                    "broadcast.topic": topic,
                },
            )

            log_json(
                "INFO",
                "broadcast_jobs_published",
                aws_request_id=aws_request_id,
                snapshot_id=snapshot_id,
                topic=topic,
                sequence=sequence,
                aggregation_window=aggregation_window,
                shard_count=SUBSCRIPTION_SHARD_COUNT,
                jobs_created=sent_jobs,
            )

    coordinator_signals_total.add(1)

    return {
        "sequence": sequence,
        "topics": topics,
        "aggregation_windows": aggregation_windows,
        "snapshots_created": created_snapshots,
        "jobs_created": created_jobs,
    }


def lambda_handler(
    event: dict[str, Any],
    context: Any,
) -> dict[str, Any]:
    started_at = time.perf_counter()

    aws_request_id = getattr(
        context,
        "aws_request_id",
        None,
    )

    batch_item_failures: list[
        dict[str, str]
    ] = []

    direct_results: list[dict[str, Any]] = []

    try:
        for (
            message_id,
            signal,
            sqs_record,
        ) in iter_input_records(event):
            parent_context = None

            if sqs_record is not None:
                parent_context = propagate.extract(
                    get_sqs_trace_carrier(
                        sqs_record
                    )
                )

            try:
                with tracer.start_as_current_span(
                    "broadcast_coordinator.process_signal",
                    context=parent_context,
                ) as span:
                    span.set_attribute(
                        "faas.trigger",
                        (
                            "sqs"
                            if sqs_record
                            else "manual"
                        ),
                    )

                    result = process_signal(
                        signal=signal,
                        aws_request_id=aws_request_id,
                    )

                    span.set_attribute(
                        "broadcast.jobs_created",
                        result["jobs_created"],
                    )

                    span.set_attribute(
                        "broadcast.snapshots_created",
                        result[
                            "snapshots_created"
                        ],
                    )

                    span.set_status(
                        Status(StatusCode.OK)
                    )

                    direct_results.append(
                        result
                    )

            except Exception as error:
                coordinator_signals_failed_total.add(
                    1
                )

                log_json(
                    "ERROR",
                    "broadcast_coordinator_record_failed",
                    aws_request_id=aws_request_id,
                    message_id=message_id,
                    error_type=type(error).__name__,
                    error_message=str(error),
                )

                if message_id:
                    batch_item_failures.append(
                        {
                            "itemIdentifier": (
                                message_id
                            )
                        }
                    )
                else:
                    raise

        duration_ms = round(
            (
                time.perf_counter()
                - started_at
            )
            * 1000,
            2,
        )

        broadcast_coordinator_duration_ms.record(
            duration_ms
        )

        log_json(
            "INFO",
            "broadcast_coordinator_completed",
            aws_request_id=aws_request_id,
            failed_records=len(
                batch_item_failures
            ),
            duration_ms=duration_ms,
        )

        if event.get("Records") is not None:
            return {
                "batchItemFailures": (
                    batch_item_failures
                )
            }

        return {
            "status": "success",
            "results": direct_results,
        }

    finally:
        if ENABLE_OTEL_FLUSH:
            flush_otel()
