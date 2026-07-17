import logging
import os
import time
from typing import Any, Dict

from opentelemetry import metrics, trace
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.botocore import BotocoreInstrumentor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor


logger = logging.getLogger(__name__)

_initialized = False


def _env_int(name: str, default: int, minimum: int = 1) -> int:
    """Read a positive integer environment variable defensively."""
    raw_value = os.getenv(name)

    if raw_value is None:
        return default

    try:
        value = int(raw_value)
    except ValueError:
        logger.warning("invalid_integer_environment_variable: %s=%r", name, raw_value)
        return default

    if value < minimum:
        logger.warning(
            "environment_variable_below_minimum: %s=%r minimum=%s",
            name,
            raw_value,
            minimum,
        )
        return default

    return value


def setup_otel() -> None:
    """
    Configure OpenTelemetry metrics and traces for the broadcaster Lambda.

    The Python SDK exports OTLP/HTTP to the endpoint configured through the
    standard OTEL_* environment variables. In the target architecture this
    endpoint is the OpenTelemetry Collector Lambda Extension listening locally:

      http://127.0.0.1:4318

    The Collector is then responsible for batching, retrying, and exporting the
    telemetry to Grafana Cloud.

    Observability must never break the business path. If setup fails, the Lambda
    continues to run and the OpenTelemetry API falls back to no-op behavior.
    """
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

        resource = Resource.create(
            {
                "service.name": os.getenv(
                    "OTEL_SERVICE_NAME",
                    "realtime-media-analytics-broadcaster",
                ),
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
                schedule_delay_millis=_env_int(
                    "OTEL_BSP_SCHEDULE_DELAY_MS",
                    5000,
                ),
                max_export_batch_size=_env_int(
                    "OTEL_BSP_MAX_EXPORT_BATCH_SIZE",
                    128,
                ),
                max_queue_size=_env_int(
                    "OTEL_BSP_MAX_QUEUE_SIZE",
                    2048,
                ),
            )
        )
        trace.set_tracer_provider(trace_provider)

        metric_reader = PeriodicExportingMetricReader(
            OTLPMetricExporter(),
            export_interval_millis=_env_int(
                "OTEL_METRIC_EXPORT_INTERVAL_MS",
                10000,
            ),
        )

        meter_provider = MeterProvider(
            resource=resource,
            metric_readers=[metric_reader],
        )
        metrics.set_meter_provider(meter_provider)

        # boto3 uses botocore internally. This creates spans for AWS SDK calls
        # such as DynamoDB and API Gateway Management API.
        BotocoreInstrumentor().instrument()

        logger.info(
            "otel_initialized: endpoint=%s protocol=%s",
            os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "default"),
            os.getenv("OTEL_EXPORTER_OTLP_PROTOCOL", "default"),
        )

    except Exception as error:
        logger.warning("otel_setup_failed: %s", error)


setup_otel()

tracer = trace.get_tracer("realtime-media-analytics.broadcaster")
meter = metrics.get_meter("realtime-media-analytics.broadcaster")


websocket_post_success_total = meter.create_counter(
    name="websocket_post_success_total",
    unit="1",
    description="Successful API Gateway WebSocket postToConnection calls.",
)

websocket_post_failure_total = meter.create_counter(
    name="websocket_post_failure_total",
    unit="1",
    description="Failed API Gateway WebSocket postToConnection calls excluding GoneException.",
)

websocket_connection_gone_total = meter.create_counter(
    name="websocket_connection_gone_total",
    unit="1",
    description="Stale WebSocket connections cleaned after GoneException.",
)

websocket_messages_sent_total = meter.create_counter(
    name="websocket_messages_sent_total",
    unit="1",
    description="WebSocket dashboard messages successfully sent.",
)

broadcast_completed_total = meter.create_counter(
    name="broadcast_completed_total",
    unit="1",
    description="Completed broadcast aggregation windows.",
)

broadcast_failed_total = meter.create_counter(
    name="broadcast_failed_total",
    unit="1",
    description="Failed broadcast aggregation windows.",
)

broadcast_duration_ms = meter.create_histogram(
    name="broadcast_duration_ms",
    unit="ms",
    description="Duration of one broadcast aggregation window.",
)

websocket_post_duration_ms = meter.create_histogram(
    name="websocket_post_duration_ms",
    unit="ms",
    description="Duration of one postToConnection call.",
)

event_to_dashboard_latency_ms = meter.create_histogram(
    name="event_to_dashboard_latency_ms",
    unit="ms",
    description=(
        "Latency between the latest Wikimedia event timestamp included in a "
        "dashboard update and a successful WebSocket postToConnection."
    ),
)

oldest_event_to_dashboard_latency_ms = meter.create_histogram(
    name="oldest_event_to_dashboard_latency_ms",
    unit="ms",
    description=(
        "Diagnostic latency between the oldest Wikimedia event timestamp included "
        "in a dashboard update and a successful WebSocket postToConnection."
    ),
)

active_connections_scanned = meter.create_histogram(
    name="active_connections_scanned",
    unit="1",
    description="Number of active WebSocket connections scanned by broadcaster.",
)


def _force_flush_provider(
    provider: Any,
    timeout_millis: int,
    signal_name: str,
) -> Dict[str, Any]:
    """Force-flush one provider and return measurement details."""
    started_at = time.perf_counter()
    attempted = hasattr(provider, "force_flush")
    succeeded = False
    error_message = None

    if not attempted:
        return {
            "attempted": False,
            "succeeded": False,
            "duration_ms": 0.0,
            "timeout_ms": timeout_millis,
            "error": None,
        }

    try:
        result = provider.force_flush(timeout_millis=timeout_millis)

        # TracerProvider.force_flush returns a bool. Some MeterProvider versions
        # return None when the operation completes successfully.
        succeeded = result is not False
    except Exception as error:  # pragma: no cover - defensive runtime path
        error_message = str(error)
        logger.warning("otel_%s_flush_failed: %s", signal_name, error)

    duration_ms = round((time.perf_counter() - started_at) * 1000, 2)

    return {
        "attempted": attempted,
        "succeeded": succeeded,
        "duration_ms": duration_ms,
        "timeout_ms": timeout_millis,
        "error": error_message,
    }


def flush_otel() -> Dict[str, Any]:
    """
    Hand pending telemetry to the local Collector with a bounded time budget.

    Metrics are flushed first because the freshness SLI is operationally more
    important than retaining every successful trace. Both operations target the
    local Collector endpoint, not Grafana Cloud directly.

    The Collector's batch + decouple processors perform the remote Grafana Cloud
    export outside the application handler's critical path.
    """
    metric_timeout_ms = _env_int(
        "OTEL_METRIC_FLUSH_TIMEOUT_MS",
        250,
    )
    trace_timeout_ms = _env_int(
        "OTEL_TRACE_FLUSH_TIMEOUT_MS",
        150,
    )

    flush_started_at = time.perf_counter()

    metric_result = _force_flush_provider(
        provider=metrics.get_meter_provider(),
        timeout_millis=metric_timeout_ms,
        signal_name="metric",
    )

    trace_result = _force_flush_provider(
        provider=trace.get_tracer_provider(),
        timeout_millis=trace_timeout_ms,
        signal_name="trace",
    )

    total_duration_ms = round((time.perf_counter() - flush_started_at) * 1000, 2)

    return {
        "total_duration_ms": total_duration_ms,
        "metric": metric_result,
        "trace": trace_result,
    }