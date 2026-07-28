import logging
import os
import time
from typing import Any

from opentelemetry import metrics, trace
from opentelemetry.exporter.otlp.proto.http.metric_exporter import (
    OTLPMetricExporter,
)
from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
    OTLPSpanExporter,
)
from opentelemetry.instrumentation.botocore import BotocoreInstrumentor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import (
    PeriodicExportingMetricReader,
)
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
        logger.warning(
            "invalid_integer_environment_variable: %s=%r",
            name,
            raw_value,
        )
        return default

    if value < minimum:
        return default

    return value


def setup_otel() -> None:
    global _initialized

    if _initialized:
        return

    _initialized = True

    if os.getenv("OTEL_ENABLED", "true").lower() != "true":
        logger.info("OpenTelemetry disabled")
        return

    try:
        service_name = os.getenv(
            "OTEL_SERVICE_NAME",
            "realtime-media-analytics-unknown-service",
        )

        resource = Resource.create(
            {
                "service.name": service_name,
                "service.namespace": "realtime-media-analytics",
                "deployment.environment": os.getenv(
                    "ENVIRONMENT",
                    "dev",
                ),
                "cloud.provider": "aws",
                "cloud.region": os.getenv(
                    "AWS_REGION",
                    "unknown",
                ),
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

        metrics.set_meter_provider(
            MeterProvider(
                resource=resource,
                metric_readers=[metric_reader],
            )
        )

        BotocoreInstrumentor().instrument()

        logger.info(
            "otel_initialized: service=%s endpoint=%s",
            service_name,
            os.getenv(
                "OTEL_EXPORTER_OTLP_ENDPOINT",
                "default",
            ),
        )

    except Exception as error:
        logger.warning(
            "otel_setup_failed: %s",
            error,
        )


def _force_flush(
    provider: Any,
    timeout_ms: int,
    signal_name: str,
) -> dict[str, Any]:
    started_at = time.perf_counter()

    result = {
        "signal": signal_name,
        "attempted": False,
        "succeeded": False,
        "duration_ms": 0.0,
        "error": None,
    }

    force_flush = getattr(provider, "force_flush", None)

    if not callable(force_flush):
        return result

    result["attempted"] = True

    try:
        flush_result = force_flush(
            timeout_millis=timeout_ms,
        )

        result["succeeded"] = flush_result is not False

    except Exception as error:
        result["error"] = str(error)

    result["duration_ms"] = round(
        (time.perf_counter() - started_at) * 1000,
        2,
    )

    return result


def flush_otel() -> dict[str, Any]:
    started_at = time.perf_counter()

    metric_result = _force_flush(
        metrics.get_meter_provider(),
        _env_int(
            "OTEL_METRIC_FLUSH_TIMEOUT_MS",
            250,
        ),
        "metric",
    )

    trace_result = _force_flush(
        trace.get_tracer_provider(),
        _env_int(
            "OTEL_TRACE_FLUSH_TIMEOUT_MS",
            150,
        ),
        "trace",
    )

    return {
        "total_duration_ms": round(
            (time.perf_counter() - started_at) * 1000,
            2,
        ),
        "metric": metric_result,
        "trace": trace_result,
    }


setup_otel()

instrumentation_name = os.getenv(
    "OTEL_SERVICE_NAME",
    "realtime-media-analytics",
)

tracer = trace.get_tracer(instrumentation_name)
meter = metrics.get_meter(instrumentation_name)
