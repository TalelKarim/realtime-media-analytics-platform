import json
import logging
import os
import time
from typing import Any, Iterator

import boto3
from boto3.dynamodb.conditions import Key
from opentelemetry import propagate
from opentelemetry.trace import Status, StatusCode

from .observability import flush_otel, meter, tracer


LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logger = logging.getLogger()
logger.setLevel(LOG_LEVEL)


SNAPSHOTS_TABLE_NAME = os.environ["SNAPSHOTS_TABLE_NAME"]
SUBSCRIPTIONS_TABLE_NAME = os.environ["SUBSCRIPTIONS_TABLE_NAME"]

BACKBONE_TEST_DELAY_MS = int(
    os.getenv(
        "BACKBONE_TEST_DELAY_MS",
        "0",
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

snapshots_table = dynamodb.Table(
    SNAPSHOTS_TABLE_NAME
)

subscriptions_table = dynamodb.Table(
    SUBSCRIPTIONS_TABLE_NAME
)


broadcast_worker_jobs_total = meter.create_counter(
    name="broadcast_worker_jobs_total",
    unit="1",
    description="Broadcast jobs processed by Workers.",
)

broadcast_worker_jobs_failed_total = meter.create_counter(
    name="broadcast_worker_jobs_failed_total",
    unit="1",
    description="Broadcast jobs that failed in Workers.",
)

broadcast_worker_subscriptions_found = meter.create_histogram(
    name="broadcast_worker_subscriptions_found",
    unit="1",
    description="Active subscriptions found for one topic-shard.",
)

broadcast_worker_duration_ms = meter.create_histogram(
    name="broadcast_worker_duration_ms",
    unit="ms",
    description="Worker job processing duration.",
)

broadcast_worker_query_duration_ms = meter.create_histogram(
    name="broadcast_worker_query_duration_ms",
    unit="ms",
    description="DynamoDB subscription Query duration.",
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


def iter_input_jobs(
    event: dict[str, Any],
) -> Iterator[
    tuple[
        str | None,
        dict[str, Any],
        dict[str, Any] | None,
    ]
]:
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


def query_active_subscriptions(
    topic_shard: str,
) -> list[dict[str, Any]]:
    started_at = time.perf_counter()

    items: list[dict[str, Any]] = []
    last_evaluated_key = None
    current_epoch = int(time.time())

    while True:
        query_arguments: dict[str, Any] = {
            "KeyConditionExpression": (
                Key("topic_shard").eq(
                    topic_shard
                )
            )
        }

        if last_evaluated_key:
            query_arguments[
                "ExclusiveStartKey"
            ] = last_evaluated_key

        response = subscriptions_table.query(
            **query_arguments
        )

        for item in response.get(
            "Items",
            [],
        ):
            ttl = item.get("ttl")

            if ttl is None:
                items.append(item)
                continue

            try:
                if int(ttl) >= current_epoch:
                    items.append(item)
            except (TypeError, ValueError):
                logger.warning(
                    "invalid_subscription_ttl: %r",
                    ttl,
                )

        last_evaluated_key = response.get(
            "LastEvaluatedKey"
        )

        if not last_evaluated_key:
            break

    duration_ms = round(
        (
            time.perf_counter()
            - started_at
        )
        * 1000,
        2,
    )

    broadcast_worker_query_duration_ms.record(
        duration_ms
    )

    return items


def process_job(
    job: dict[str, Any],
    aws_request_id: str | None,
    message_group_id: str | None,
) -> dict[str, Any]:
    required_fields = (
        "snapshot_id",
        "topic",
        "topic_shard",
        "subscription_shard",
        "sequence",
    )

    missing_fields = [
        field
        for field in required_fields
        if field not in job
    ]

    if missing_fields:
        raise ValueError(
            "Missing job fields: "
            + ",".join(missing_fields)
        )

    snapshot_response = snapshots_table.get_item(
        Key={
            "snapshot_id": job[
                "snapshot_id"
            ],
            "topic": job["topic"],
        },
        ConsistentRead=False,
    )

    snapshot = snapshot_response.get(
        "Item"
    )

    if snapshot is None:
        raise RuntimeError(
            "Snapshot not found: "
            f"{job['snapshot_id']} / "
            f"{job['topic']}"
        )

    subscriptions = query_active_subscriptions(
        job["topic_shard"]
    )

    if BACKBONE_TEST_DELAY_MS > 0:
        time.sleep(
            BACKBONE_TEST_DELAY_MS / 1000
        )

    subscription_count = len(
        subscriptions
    )

    attributes = {
        "broadcast.topic": job["topic"],
        "broadcast.shard_id": str(
            job["subscription_shard"]
        ),
    }

    broadcast_worker_jobs_total.add(
        1,
        attributes,
    )

    broadcast_worker_subscriptions_found.record(
        subscription_count,
        attributes,
    )

    result = {
        "snapshot_id": job["snapshot_id"],
        "sequence": job["sequence"],
        "topic": job["topic"],
        "topic_shard": job["topic_shard"],
        "subscription_shard": job[
            "subscription_shard"
        ],
        "subscription_count": (
            subscription_count
        ),
        "snapshot_found": True,
    }

    log_json(
        "INFO",
        "broadcast_worker_job_completed",
        aws_request_id=aws_request_id,
        message_group_id=message_group_id,
        **result,
    )

    return result


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

    direct_results: list[
        dict[str, Any]
    ] = []

    try:
        for (
            message_id,
            job,
            sqs_record,
        ) in iter_input_jobs(event):
            parent_context = None
            message_group_id = None

            if sqs_record is not None:
                parent_context = propagate.extract(
                    get_sqs_trace_carrier(
                        sqs_record
                    )
                )

                message_group_id = (
                    sqs_record.get(
                        "attributes",
                        {},
                    ).get(
                        "MessageGroupId"
                    )
                )

            try:
                with tracer.start_as_current_span(
                    "broadcast_worker.process_job",
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

                    result = process_job(
                        job=job,
                        aws_request_id=aws_request_id,
                        message_group_id=message_group_id,
                    )

                    span.set_attribute(
                        "broadcast.snapshot_id",
                        result["snapshot_id"],
                    )

                    span.set_attribute(
                        "broadcast.topic",
                        result["topic"],
                    )

                    span.set_attribute(
                        "broadcast.shard_id",
                        result[
                            "subscription_shard"
                        ],
                    )

                    span.set_attribute(
                        "broadcast.subscription_count",
                        result[
                            "subscription_count"
                        ],
                    )

                    span.set_status(
                        Status(StatusCode.OK)
                    )

                    direct_results.append(
                        result
                    )

            except Exception as error:
                broadcast_worker_jobs_failed_total.add(
                    1
                )

                log_json(
                    "ERROR",
                    "broadcast_worker_job_failed",
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

        broadcast_worker_duration_ms.record(
            duration_ms
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
