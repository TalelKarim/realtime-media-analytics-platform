"""Shared helpers for WebSocket connection and subscription handlers."""

from __future__ import annotations

import hashlib
import json
import time
from datetime import datetime, timezone
from typing import Any, Mapping

from boto3.dynamodb.types import TypeSerializer


_SERIALIZER = TypeSerializer()


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def normalize_topic(topic: str) -> str:
    return topic.strip().lower()


def calculate_subscription_shard(
    connection_id: str,
    shard_count: int,
) -> int:
    if not connection_id:
        raise ValueError("connection_id must not be empty")

    if shard_count <= 0:
        raise ValueError("shard_count must be greater than zero")

    digest = hashlib.sha256(
        connection_id.encode("utf-8")
    ).digest()

    numeric_value = int.from_bytes(
        digest[:8],
        byteorder="big",
        signed=False,
    )

    return numeric_value % shard_count


def build_topic_shard(
    topic: str,
    shard_id: int,
) -> str:
    if shard_id < 0:
        raise ValueError(
            "shard_id must be non-negative"
        )

    return (
        f"TOPIC#{normalize_topic(topic)}"
        f"#SHARD#{shard_id:02d}"
    )


def serialize_item(
    item: Mapping[str, Any],
) -> dict[str, dict[str, Any]]:
    return {
        key: _SERIALIZER.serialize(value)
        for key, value in item.items()
    }


def serialize_value(
    value: Any,
) -> dict[str, Any]:
    return _SERIALIZER.serialize(value)


def log_event(
    level: str,
    message: str,
    **fields: Any,
) -> None:
    payload = {
        "level": level.lower(),
        "message": message,
        **fields,
    }

    print(
        json.dumps(
            payload,
            default=str,
            separators=(",", ":"),
        )
    )


def emit_metrics(
    *,
    namespace: str,
    dimensions: Mapping[str, str],
    metrics: Mapping[
        str,
        tuple[float | int, str],
    ],
    **properties: Any,
) -> None:
    """
    Emit CloudWatch Embedded Metric Format.

    No metric uses connection_id or topic as a dimension,
    preventing high-cardinality CloudWatch metrics.
    """

    payload: dict[str, Any] = {
        "_aws": {
            "Timestamp": int(time.time() * 1000),
            "CloudWatchMetrics": [
                {
                    "Namespace": namespace,
                    "Dimensions": [
                        list(dimensions.keys())
                    ],
                    "Metrics": [
                        {
                            "Name": metric_name,
                            "Unit": unit,
                        }
                        for (
                            metric_name,
                            (_, unit),
                        ) in metrics.items()
                    ],
                }
            ],
        },
        **dimensions,
        **properties,
    }

    for metric_name, (
        metric_value,
        _,
    ) in metrics.items():
        payload[metric_name] = metric_value

    print(
        json.dumps(
            payload,
            default=str,
            separators=(",", ":"),
        )
    )
