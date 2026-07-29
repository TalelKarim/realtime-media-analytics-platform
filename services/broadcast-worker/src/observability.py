from __future__ import annotations

import logging
import os
import time
from typing import Any

from opentelemetry import metrics, trace
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.botocore import BotocoreInstrumentor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.metrics.view import ExplicitBucketHistogramAggregation, View
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor


logger = logging.getLogger(__name__)
_initialized = False


def _env_int(name: str, default: int, minimum: int = 1) -> int:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    try:
        value = int(raw_value)
    except ValueError:
        logger.warning("invalid_integer_environment_variable: %s=%r", name, raw_value)
        return default
    return value if value >= minimum else default


def setup_otel() -> None:
    global _initialized
    if _initialized:
        return
    _initialized = True
    if os.getenv("OTEL_ENABLED", "true").lower() != "true":
        logger.info("OpenTelemetry disabled with OTEL_ENABLED=false")
        return

    try:
        environment = os.getenv("ENVIRONMENT", "dev")
        aws_region = os.getenv("AWS_REGION", "unknown")
        service_name = os.getenv(
            "OTEL_SERVICE_NAME",
            "realtime-media-analytics-broadcast-worker",
        )
        resource = Resource.create(
            {
                "service.name": service_name,
                "service.namespace": "realtime-media-analytics",
                "deployment.environment": environment,
                "cloud.provider": "aws",
                "cloud.region": aws_region,
            }
        )

        trace_provider = TracerProvider(resource=resource)
        trace_provider.add_span_processor(
            BatchSpanProcessor(
                OTLPSpanExporter(),
                schedule_delay_millis=_env_int("OTEL_BSP_SCHEDULE_DELAY_MS", 5000),
                max_export_batch_size=_env_int(
                    "OTEL_BSP_MAX_EXPORT_BATCH_SIZE", 128
                ),
                max_queue_size=_env_int("OTEL_BSP_MAX_QUEUE_SIZE", 2048),
            )
        )
        trace.set_tracer_provider(trace_provider)

        metric_reader = PeriodicExportingMetricReader(
            OTLPMetricExporter(),
            export_interval_millis=_env_int(
                "OTEL_METRIC_EXPORT_INTERVAL_MS", 10000
            ),
        )
        freshness_boundaries_ms = [
            0.0,
            1000.0,
            2000.0,
            3000.0,
            4000.0,
            5000.0,
            6000.0,
            7500.0,
            10000.0,
            12500.0,
            15000.0,
            20000.0,
            30000.0,
            45000.0,
            60000.0,
            90000.0,
            120000.0,
        ]
        views = [
            View(
                instrument_name="event_to_dashboard_latency_ms",
                aggregation=ExplicitBucketHistogramAggregation(
                    boundaries=freshness_boundaries_ms,
                    record_min_max=True,
                ),
            ),
            View(
                instrument_name="oldest_event_to_dashboard_latency_ms",
                aggregation=ExplicitBucketHistogramAggregation(
                    boundaries=freshness_boundaries_ms,
                    record_min_max=True,
                ),
            ),
        ]
        metrics.set_meter_provider(
            MeterProvider(
                resource=resource,
                metric_readers=[metric_reader],
                views=views,
            )
        )
        BotocoreInstrumentor().instrument()
        logger.info(
            "otel_initialized: service=%s endpoint=%s protocol=%s",
            service_name,
            os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "default"),
            os.getenv("OTEL_EXPORTER_OTLP_PROTOCOL", "default"),
        )
    except Exception as error:  # telemetry must never break delivery
        logger.warning("otel_setup_failed: %s", error)


setup_otel()
tracer = trace.get_tracer("realtime-media-analytics.broadcast-worker")
meter = metrics.get_meter("realtime-media-analytics.broadcast-worker")

# Delivery metrics retained for dashboard continuity.
websocket_post_success_total = meter.create_counter(
    "websocket_post_success_total", unit="1",
    description="Successful API Gateway WebSocket postToConnection calls."
)
websocket_post_failure_total = meter.create_counter(
    "websocket_post_failure_total", unit="1",
    description="Failed postToConnection calls excluding HTTP 410."
)
websocket_connection_gone_total = meter.create_counter(
    "websocket_connection_gone_total", unit="1",
    description="Stale WebSocket connections detected with HTTP 410."
)
websocket_messages_sent_total = meter.create_counter(
    "websocket_messages_sent_total", unit="1",
    description="WebSocket chunks successfully accepted by API Gateway."
)
websocket_post_duration_ms = meter.create_histogram(
    "websocket_post_duration_ms", unit="ms",
    description="Duration of one postToConnection including bounded retries."
)
event_to_dashboard_latency_ms = meter.create_histogram(
    "event_to_dashboard_latency_ms", unit="ms",
    description=(
        "Primary freshness: successful WebSocket send time minus the earliest "
        "latest_event_timestamp_ms among topics contained in that chunk."
    )
)
oldest_event_to_dashboard_latency_ms = meter.create_histogram(
    "oldest_event_to_dashboard_latency_ms", unit="ms",
    description=(
        "Diagnostic latency: successful WebSocket send time minus the oldest "
        "source event timestamp contained in that chunk."
    )
)
fanout_duration_ms = meter.create_histogram(
    "fanout_duration_ms", unit="ms",
    description="Duration of one bounded-parallel connection-shard fan-out."
)
fanout_batch_size = meter.create_histogram(
    "fanout_batch_size", unit="1",
    description="Connections with at least one update in one Worker job."
)
gone_cleanup_duration_ms = meter.create_histogram(
    "gone_cleanup_duration_ms", unit="ms",
    description="Duration of stale WebSocket state cleanup after HTTP 410."
)

broadcast_worker_jobs_total = meter.create_counter(
    "broadcast_worker_jobs_total", unit="1",
    description="Shard-centric Worker jobs completed or stale-skipped."
)
broadcast_worker_jobs_failed_total = meter.create_counter(
    "broadcast_worker_jobs_failed_total", unit="1",
    description="Structurally failed Worker jobs returned to SQS."
)
stale_broadcast_jobs_skipped_total = meter.create_counter(
    "stale_broadcast_jobs_skipped_total", unit="1",
    description="Jobs skipped because LATEST already points to a newer state."
)
broadcast_worker_connections_found = meter.create_histogram(
    "broadcast_worker_connections_found", unit="1",
    description="Active connections returned by the connection-shard GSI Query."
)
broadcast_worker_topics_loaded = meter.create_histogram(
    "broadcast_worker_topics_loaded", unit="1",
    description="Distinct topic snapshots loaded for one connection shard."
)
broadcast_worker_duration_ms = meter.create_histogram(
    "broadcast_worker_duration_ms", unit="ms",
    description="Worker job duration excluding the bounded OTel flush."
)
broadcast_worker_manifest_read_duration_ms = meter.create_histogram(
    "broadcast_worker_manifest_read_duration_ms", unit="ms",
    description="Duration of the strongly consistent manifest GetItem."
)
broadcast_worker_snapshot_read_duration_ms = meter.create_histogram(
    "broadcast_worker_snapshot_read_duration_ms", unit="ms",
    description="Duration of snapshot BatchGetItem operations."
)
broadcast_worker_query_duration_ms = meter.create_histogram(
    "broadcast_worker_query_duration_ms", unit="ms",
    description="Duration of the paginated connection-shard GSI Query."
)
broadcast_job_queue_delay_ms = meter.create_histogram(
    "broadcast_job_queue_delay_ms", unit="ms",
    description="Delay between Coordinator job creation and Worker start."
)
websocket_payload_size_bytes = meter.create_histogram(
    "websocket_payload_size_bytes", unit="By",
    description="Serialized WebSocket chunk size."
)
websocket_payload_build_failure_total = meter.create_counter(
    "websocket_payload_build_failure_total", unit="1",
    description="Connections skipped because one topic update could not fit safely."
)
websocket_batch_topics_count = meter.create_histogram(
    "websocket_batch_topics_count", unit="1",
    description="Number of topic updates contained in one WebSocket chunk."
)
websocket_chunks_per_connection = meter.create_histogram(
    "websocket_chunks_per_connection", unit="1",
    description="Chunks planned for one connection in one broadcast."
)
websocket_post_retry_total = meter.create_counter(
    "websocket_post_retry_total", unit="1",
    description="Additional postToConnection attempts after retryable errors."
)
websocket_post_retry_exhausted_total = meter.create_counter(
    "websocket_post_retry_exhausted_total", unit="1",
    description="Chunks still failing after all bounded retry attempts."
)
websocket_gone_cleanup_failure_total = meter.create_counter(
    "websocket_gone_cleanup_failure_total", unit="1",
    description="HTTP 410 cleanup operations that failed."
)


def _force_flush_provider(
    provider: Any,
    timeout_millis: int,
    signal_name: str,
) -> dict[str, Any]:
    started_at = time.perf_counter()
    force_flush = getattr(provider, "force_flush", None)
    if not callable(force_flush):
        return {
            "attempted": False,
            "succeeded": False,
            "duration_ms": 0.0,
            "timeout_ms": timeout_millis,
            "error": None,
        }
    error_message = None
    succeeded = False
    try:
        result = force_flush(timeout_millis=timeout_millis)
        succeeded = result is not False
    except Exception as error:  # pragma: no cover
        error_message = str(error)
        logger.warning("otel_%s_flush_failed: %s", signal_name, error)
    return {
        "attempted": True,
        "succeeded": succeeded,
        "duration_ms": round((time.perf_counter() - started_at) * 1000, 2),
        "timeout_ms": timeout_millis,
        "error": error_message,
    }


def flush_otel() -> dict[str, Any]:
    started_at = time.perf_counter()
    metric_result = _force_flush_provider(
        metrics.get_meter_provider(),
        _env_int("OTEL_METRIC_FLUSH_TIMEOUT_MS", 200),
        "metric",
    )
    trace_result = _force_flush_provider(
        trace.get_tracer_provider(),
        _env_int("OTEL_TRACE_FLUSH_TIMEOUT_MS", 100),
        "trace",
    )
    return {
        "total_duration_ms": round((time.perf_counter() - started_at) * 1000, 2),
        "metric": metric_result,
        "trace": trace_result,
    }
