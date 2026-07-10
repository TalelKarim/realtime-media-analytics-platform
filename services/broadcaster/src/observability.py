import logging
import os

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


def setup_otel() -> None:
    """
    Configure OpenTelemetry metrics and traces for the broadcaster Lambda.

    Export configuration is read from standard OTEL_* environment variables:
      - OTEL_EXPORTER_OTLP_ENDPOINT
      - OTEL_EXPORTER_OTLP_HEADERS
      - OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf
      - OTEL_SERVICE_NAME

    The setup is intentionally defensive: observability must never break the
    business path. If OTel setup fails, the Lambda continues to run and the OTel
    API falls back to no-op behavior.
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
                schedule_delay_millis=5000,
                max_export_batch_size=128,
            )
        )
        trace.set_tracer_provider(trace_provider)

        metric_reader = PeriodicExportingMetricReader(
            OTLPMetricExporter(),
            export_interval_millis=10000,
        )

        meter_provider = MeterProvider(
            resource=resource,
            metric_readers=[metric_reader],
        )
        metrics.set_meter_provider(meter_provider)

        # boto3 uses botocore internally. This creates spans for AWS SDK calls
        # such as DynamoDB and API Gateway Management API.
        BotocoreInstrumentor().instrument()

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

active_connections_scanned = meter.create_histogram(
    name="active_connections_scanned",
    unit="1",
    description="Number of active WebSocket connections scanned by broadcaster.",
)


def flush_otel() -> None:
    """
    Flush telemetry before Lambda runtime freeze.

    This is best-effort only. Export errors should not fail the Lambda handler.
    """
    try:
        tracer_provider = trace.get_tracer_provider()
        if hasattr(tracer_provider, "force_flush"):
            tracer_provider.force_flush(timeout_millis=2000)
    except Exception as error:
        logger.warning("otel_trace_flush_failed: %s", error)

    try:
        meter_provider = metrics.get_meter_provider()
        if hasattr(meter_provider, "force_flush"):
            meter_provider.force_flush(timeout_millis=2000)
    except Exception as error:
        logger.warning("otel_metric_flush_failed: %s", error)