from __future__ import annotations

import logging
import os
import socket
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Optional

from opentelemetry import metrics, propagate, trace
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.botocore import BotocoreInstrumentor
from opentelemetry.metrics import Observation
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

LOGGER = logging.getLogger("wikimedia-collector")


def _env_int(name: str, default: int, minimum: int = 1) -> int:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default

    try:
        value = int(raw_value)
    except ValueError:
        LOGGER.warning("invalid_integer_environment_variable: %s=%r", name, raw_value)
        return default

    if value < minimum:
        LOGGER.warning(
            "environment_variable_below_minimum: %s=%r minimum=%s",
            name,
            raw_value,
            minimum,
        )
        return default

    return value


@dataclass
class _RuntimeSnapshot:
    sse_connected: int
    buffer_size: int
    seconds_since_last_event: float
    seconds_since_last_successful_put: float
    process_uptime_seconds: float


class CollectorRuntimeState:
    """Thread-safe state used by asynchronous observable gauges."""

    def __init__(self) -> None:
        now = time.time()
        self._lock = threading.Lock()
        self._started_at = now
        self._sse_connected = False
        self._buffer_size = 0
        self._last_event_at: Optional[float] = None
        self._last_successful_put_at: Optional[float] = None

    def set_sse_connected(self, connected: bool) -> None:
        with self._lock:
            self._sse_connected = connected

    def set_buffer_size(self, size: int) -> None:
        with self._lock:
            self._buffer_size = max(0, size)

    def mark_event_received(self) -> None:
        with self._lock:
            self._last_event_at = time.time()

    def mark_successful_put(self) -> None:
        with self._lock:
            self._last_successful_put_at = time.time()

    def snapshot(self) -> _RuntimeSnapshot:
        now = time.time()
        with self._lock:
            return _RuntimeSnapshot(
                sse_connected=1 if self._sse_connected else 0,
                buffer_size=self._buffer_size,
                seconds_since_last_event=(
                    max(0.0, now - self._last_event_at)
                    if self._last_event_at is not None
                    else -1.0
                ),
                seconds_since_last_successful_put=(
                    max(0.0, now - self._last_successful_put_at)
                    if self._last_successful_put_at is not None
                    else -1.0
                ),
                process_uptime_seconds=max(0.0, now - self._started_at),
            )


runtime_state = CollectorRuntimeState()

_initialized = False
_botocore_instrumented = False


def setup_otel() -> None:
    """Configure OTel metrics and traces for the long-running ECS collector."""
    global _initialized, _botocore_instrumented

    if _initialized:
        return
    _initialized = True

    if os.getenv("OTEL_ENABLED", "true").lower() != "true":
        LOGGER.info("OpenTelemetry disabled with OTEL_ENABLED=false")
        return

    try:
        environment = os.getenv("ENVIRONMENT", "dev")
        aws_region = os.getenv("AWS_REGION", "unknown")
        service_name = os.getenv(
            "OTEL_SERVICE_NAME",
            "realtime-media-analytics-collector",
        )

        resource = Resource.create(
            {
                "service.name": service_name,
                "service.namespace": "realtime-media-analytics",
                "service.version": os.getenv("SERVICE_VERSION", "unknown"),
                "service.instance.id": os.getenv(
                    "SERVICE_INSTANCE_ID",
                    socket.gethostname(),
                ),
                "deployment.environment": environment,
                "cloud.provider": "aws",
                "cloud.region": aws_region,
                "cloud.platform": "aws_ecs",
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
                    256,
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
            export_timeout_millis=_env_int(
                "OTEL_METRIC_EXPORT_TIMEOUT_MS",
                3000,
            ),
        )
        meter_provider = MeterProvider(
            resource=resource,
            metric_readers=[metric_reader],
        )
        metrics.set_meter_provider(meter_provider)

        if not _botocore_instrumented:
            BotocoreInstrumentor().instrument()
            _botocore_instrumented = True

        LOGGER.info(
            "otel_initialized: endpoint=%s protocol=%s service=%s",
            os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "default"),
            os.getenv("OTEL_EXPORTER_OTLP_PROTOCOL", "default"),
            service_name,
        )
    except Exception as error:  # pragma: no cover - defensive runtime path
        LOGGER.warning("otel_setup_failed: %s", error)


setup_otel()

tracer = trace.get_tracer("realtime-media-analytics.collector")
meter = metrics.get_meter("realtime-media-analytics.collector")

# ---------------------------------------------------------------------------
# Counters
# ---------------------------------------------------------------------------

collector_sse_events_received_total = meter.create_counter(
    "collector_sse_events_received_total",
    unit="1",
    description="Wikimedia SSE data events received by the collector.",
)
collector_sse_parse_failures_total = meter.create_counter(
    "collector_sse_parse_failures_total",
    unit="1",
    description="Wikimedia SSE data events that could not be parsed as JSON.",
)
collector_events_invalid_total = meter.create_counter(
    "collector_events_invalid_total",
    unit="1",
    description="Parsed events rejected because mandatory metadata was invalid.",
)
collector_events_canary_dropped_total = meter.create_counter(
    "collector_events_canary_dropped_total",
    unit="1",
    description="Wikimedia canary events intentionally dropped.",
)
collector_events_sampled_out_total = meter.create_counter(
    "collector_events_sampled_out_total",
    unit="1",
    description="Valid events excluded by deterministic application sampling.",
)
collector_events_kept_total = meter.create_counter(
    "collector_events_kept_total",
    unit="1",
    description="Valid events retained after deterministic sampling.",
)
collector_sse_reconnects_total = meter.create_counter(
    "collector_sse_reconnects_total",
    unit="1",
    description="Wikimedia SSE reconnection attempts.",
)
collector_batch_flushes_total = meter.create_counter(
    "collector_batch_flushes_total",
    unit="1",
    description="Collector buffer flushes attempted toward Kinesis.",
)
collector_kinesis_records_sent_total = meter.create_counter(
    "collector_kinesis_records_sent_total",
    unit="1",
    description="Kinesis records accepted after retries.",
)
collector_kinesis_records_failed_total = meter.create_counter(
    "collector_kinesis_records_failed_total",
    unit="1",
    description="Kinesis records dropped after all retries.",
)
collector_kinesis_retry_attempts_total = meter.create_counter(
    "collector_kinesis_retry_attempts_total",
    unit="1",
    description="Additional PutRecords attempts after the initial request.",
)
collector_kinesis_partial_failures_total = meter.create_counter(
    "collector_kinesis_partial_failures_total",
    unit="1",
    description="PutRecords responses containing one or more failed records.",
)

# ---------------------------------------------------------------------------
# Histograms
# ---------------------------------------------------------------------------

collector_batch_size = meter.create_histogram(
    "collector_batch_size",
    unit="1",
    description="Number of records included in one collector flush.",
)
collector_batch_flush_duration = meter.create_histogram(
    "collector_batch_flush_duration",
    unit="ms",
    description="End-to-end duration of one collector buffer flush.",
)
collector_kinesis_put_records_duration = meter.create_histogram(
    "collector_kinesis_put_records_duration",
    unit="ms",
    description="Duration of individual Kinesis PutRecords API attempts.",
)
collector_event_to_kinesis_latency = meter.create_histogram(
    "collector_event_to_kinesis_latency",
    unit="ms",
    description="Wikimedia source event timestamp to successful Kinesis delivery.",
)


def _base_attributes() -> Dict[str, str]:
    return {
        "environment": os.getenv("ENVIRONMENT", "dev"),
    }


def _observe_sse_connection_state(_options: Any) -> Iterable[Observation]:
    yield Observation(runtime_state.snapshot().sse_connected, _base_attributes())


def _observe_buffer_size(_options: Any) -> Iterable[Observation]:
    yield Observation(runtime_state.snapshot().buffer_size, _base_attributes())


def _observe_seconds_since_last_event(_options: Any) -> Iterable[Observation]:
    yield Observation(
        runtime_state.snapshot().seconds_since_last_event,
        _base_attributes(),
    )


def _observe_seconds_since_last_successful_put(
    _options: Any,
) -> Iterable[Observation]:
    yield Observation(
        runtime_state.snapshot().seconds_since_last_successful_put,
        _base_attributes(),
    )


def _observe_process_uptime(_options: Any) -> Iterable[Observation]:
    yield Observation(
        runtime_state.snapshot().process_uptime_seconds,
        _base_attributes(),
    )


meter.create_observable_gauge(
    "collector_sse_connection_state",
    callbacks=[_observe_sse_connection_state],
    unit="1",
    description="1 while the Wikimedia SSE stream is connected, otherwise 0.",
)
meter.create_observable_gauge(
    "collector_buffer_size",
    callbacks=[_observe_buffer_size],
    unit="1",
    description="Current number of retained events waiting for a Kinesis flush.",
)
meter.create_observable_gauge(
    "collector_seconds_since_last_event",
    callbacks=[_observe_seconds_since_last_event],
    unit="s",
    description="Seconds since the latest Wikimedia data event; -1 before first event.",
)
meter.create_observable_gauge(
    "collector_seconds_since_last_successful_put",
    callbacks=[_observe_seconds_since_last_successful_put],
    unit="s",
    description="Seconds since the latest successful Kinesis delivery; -1 before first success.",
)
meter.create_observable_gauge(
    "collector_process_uptime_seconds",
    callbacks=[_observe_process_uptime],
    unit="s",
    description="Collector process uptime.",
)


def inject_current_trace_context() -> Dict[str, str]:
    """Return W3C trace context from the active producer span."""
    carrier: Dict[str, str] = {}
    propagate.inject(carrier)
    return {
        key: carrier[key]
        for key in ("traceparent", "tracestate", "baggage")
        if carrier.get(key)
    }


def shutdown_otel() -> None:
    """Best-effort final export during ECS SIGTERM shutdown."""
    try:
        tracer_provider = trace.get_tracer_provider()
        if hasattr(tracer_provider, "force_flush"):
            tracer_provider.force_flush(timeout_millis=3000)
        if hasattr(tracer_provider, "shutdown"):
            tracer_provider.shutdown()
    except Exception as error:  # pragma: no cover - defensive runtime path
        LOGGER.warning("otel_trace_shutdown_failed: %s", error)

    try:
        meter_provider = metrics.get_meter_provider()
        if hasattr(meter_provider, "force_flush"):
            meter_provider.force_flush(timeout_millis=3000)
        if hasattr(meter_provider, "shutdown"):
            meter_provider.shutdown(timeout_millis=3000)
    except Exception as error:  # pragma: no cover - defensive runtime path
        LOGGER.warning("otel_metric_shutdown_failed: %s", error)