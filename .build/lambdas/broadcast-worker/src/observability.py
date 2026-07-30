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


class _NoopInstrument:
    """Preserve the handler API without exporting low-value metric series."""

    def add(self, *_args: Any, **_kwargs: Any) -> None:
        return None

    def record(self, *_args: Any, **_kwargs: Any) -> None:
        return None


_NOOP = _NoopInstrument()


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

        # Export only the product-critical freshness histogram.
        # The exact 10-second boundary is retained for the dashboard SLO.
        freshness_boundaries_ms = [
            0.0,
            2000.0,
            4000.0,
            6000.0,
            8000.0,
            10000.0,
            15000.0,
            30000.0,
        ]
        views = [
            View(
                instrument_name="event_to_dashboard_latency_ms",
                aggregation=ExplicitBucketHistogramAggregation(
                    boundaries=freshness_boundaries_ms,
                    record_min_max=False,
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
            "otel_initialized_metrics_lite: service=%s endpoint=%s protocol=%s",
            service_name,
            os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "default"),
            os.getenv("OTEL_EXPORTER_OTLP_PROTOCOL", "default"),
        )
    except Exception as error:  # telemetry must never break delivery
        logger.warning("otel_setup_failed: %s", error)


setup_otel()
tracer = trace.get_tracer("realtime-media-analytics.broadcast-worker")
meter = metrics.get_meter("realtime-media-analytics.broadcast-worker")

# ---------------------------------------------------------------------------
# Minimal Mimir metric surface
# ---------------------------------------------------------------------------
# Keep only one histogram and two bounded-label counters. Operational details
# remain available in structured logs, native AWS metrics and Tempo spans.

websocket_delivery_total = meter.create_counter(
    "websocket_delivery_total",
    unit="1",
    description=(
        "WebSocket delivery outcomes. The result label is bounded to "
        "success, failure, gone or retry_exhausted."
    ),
)

broadcast_worker_jobs_total = meter.create_counter(
    "broadcast_worker_jobs_total",
    unit="1",
    description=(
        "Non-stale Worker job outcomes. The result label is bounded to "
        "success or failed."
    ),
)

event_to_dashboard_latency_ms = meter.create_histogram(
    "event_to_dashboard_latency_ms",
    unit="ms",
    description=(
        "Primary freshness: successful WebSocket send time minus the earliest "
        "latest_event_timestamp_ms among topics contained in that chunk."
    ),
)

# ---------------------------------------------------------------------------
# Disabled diagnostics
# ---------------------------------------------------------------------------
# Preserve the handler API with no-op instruments. These values are already
# emitted in `broadcast_worker_job_completed`, failure logs, AWS/Lambda metrics,
# AWS/SQS metrics and Tempo spans. No Mimir series are created for them.

websocket_post_success_total = _NOOP
websocket_post_failure_total = _NOOP
websocket_connection_gone_total = _NOOP
broadcast_worker_jobs_failed_total = _NOOP
stale_broadcast_jobs_skipped_total = _NOOP
websocket_payload_build_failure_total = _NOOP
websocket_post_retry_total = _NOOP
websocket_post_retry_exhausted_total = _NOOP
websocket_gone_cleanup_failure_total = _NOOP
websocket_messages_sent_total = _NOOP
websocket_post_duration_ms = _NOOP
oldest_event_to_dashboard_latency_ms = _NOOP
fanout_batch_size = _NOOP
gone_cleanup_duration_ms = _NOOP
broadcast_worker_connections_found = _NOOP
broadcast_worker_topics_loaded = _NOOP
broadcast_worker_manifest_read_duration_ms = _NOOP
broadcast_worker_snapshot_read_duration_ms = _NOOP
broadcast_worker_query_duration_ms = _NOOP
websocket_payload_size_bytes = _NOOP
websocket_batch_topics_count = _NOOP
websocket_chunks_per_connection = _NOOP
broadcast_worker_duration_ms = _NOOP
fanout_duration_ms = _NOOP
broadcast_job_queue_delay_ms = _NOOP


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


def _skipped_flush_result(timeout_millis: int, reason: str) -> dict[str, Any]:
    return {
        "attempted": False,
        "succeeded": True,
        "duration_ms": 0.0,
        "timeout_ms": timeout_millis,
        "error": None,
        "skipped_reason": reason,
    }


def flush_otel(
    *,
    flush_metrics: bool = True,
    flush_traces: bool = True,
) -> dict[str, Any]:
    started_at = time.perf_counter()
    metric_timeout_ms = _env_int("OTEL_METRIC_FLUSH_TIMEOUT_MS", 200)
    trace_timeout_ms = _env_int("OTEL_TRACE_FLUSH_TIMEOUT_MS", 100)

    metric_result = (
        _force_flush_provider(
            metrics.get_meter_provider(),
            metric_timeout_ms,
            "metric",
        )
        if flush_metrics
        else _skipped_flush_result(metric_timeout_ms, "no_metric_work")
    )
    trace_result = (
        _force_flush_provider(
            trace.get_tracer_provider(),
            trace_timeout_ms,
            "trace",
        )
        if flush_traces
        else _skipped_flush_result(trace_timeout_ms, "disabled")
    )
    return {
        "total_duration_ms": round((time.perf_counter() - started_at) * 1000, 2),
        "metric": metric_result,
        "trace": trace_result,
    }
