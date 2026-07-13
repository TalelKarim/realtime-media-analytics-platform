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
    Configure OpenTelemetry metrics and traces for the realtime-processor Lambda.

    Export configuration is read from standard OTEL_* environment variables:
      - OTEL_EXPORTER_OTLP_ENDPOINT
      - OTEL_EXPORTER_OTLP_HEADERS
      - OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf
      - OTEL_SERVICE_NAME

    The setup is defensive: observability must never break the business path.
    If OTel setup fails, the Lambda continues and the OTel API falls back to no-op behavior.
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
                    "realtime-media-analytics-realtime-processor",
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

        # boto3 uses botocore internally. This creates automatic spans for AWS SDK calls
        # such as DynamoDB UpdateItem and SQS SendMessage.
        BotocoreInstrumentor().instrument()

    except Exception as error:
        logger.warning("otel_setup_failed: %s", error)


setup_otel()

tracer = trace.get_tracer("realtime-media-analytics.realtime-processor")
meter = metrics.get_meter("realtime-media-analytics.realtime-processor")


realtime_processor_batches_total = meter.create_counter(
    name="realtime_processor_batches_total",
    unit="1",
    description="Kinesis batches processed by the realtime processor Lambda.",
)

realtime_processor_records_received_total = meter.create_counter(
    name="realtime_processor_records_received_total",
    unit="1",
    description="Kinesis records received by the realtime processor.",
)

realtime_processor_records_decoded_total = meter.create_counter(
    name="realtime_processor_records_decoded_total",
    unit="1",
    description="Kinesis records successfully decoded from base64 JSON.",
)

realtime_processor_records_valid_total = meter.create_counter(
    name="realtime_processor_records_valid_total",
    unit="1",
    description="Kinesis records accepted as valid Wikimedia recentchange events.",
)

realtime_processor_records_skipped_total = meter.create_counter(
    name="realtime_processor_records_skipped_total",
    unit="1",
    description="Kinesis records skipped because they are invalid, unsupported, or malformed.",
)

realtime_processor_records_failed_total = meter.create_counter(
    name="realtime_processor_records_failed_total",
    unit="1",
    description="Kinesis records that failed during decoding or normalization.",
)

dynamodb_aggregate_updates_total = meter.create_counter(
    name="dynamodb_aggregate_updates_total",
    unit="1",
    description="Successful DynamoDB aggregate counter UpdateItem operations.",
)

dynamodb_aggregate_update_failure_total = meter.create_counter(
    name="dynamodb_aggregate_update_failure_total",
    unit="1",
    description="Failed DynamoDB aggregate counter UpdateItem operations.",
)

broadcast_signals_sent_total = meter.create_counter(
    name="broadcast_signals_sent_total",
    unit="1",
    description="Successful SQS broadcast signal messages sent by realtime processor.",
)

broadcast_signal_failure_total = meter.create_counter(
    name="broadcast_signal_failure_total",
    unit="1",
    description="Failed SQS broadcast signal send attempts by realtime processor.",
)

broadcast_signal_skipped_total = meter.create_counter(
    name="broadcast_signal_skipped_total",
    unit="1",
    description="Broadcast signal send operations skipped by realtime processor.",
)

processor_batch_duration_ms = meter.create_histogram(
    name="processor_batch_duration_ms",
    unit="ms",
    description="Duration of one realtime processor Lambda batch execution.",
)

dynamodb_update_batch_duration_ms = meter.create_histogram(
    name="dynamodb_update_batch_duration_ms",
    unit="ms",
    description="Duration of DynamoDB aggregate update phase for one batch.",
)

broadcast_signal_duration_ms = meter.create_histogram(
    name="broadcast_signal_duration_ms",
    unit="ms",
    description="Duration of sending one SQS broadcast signal.",
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