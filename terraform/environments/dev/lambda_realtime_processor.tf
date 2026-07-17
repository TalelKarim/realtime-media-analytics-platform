locals {
  # OpenTelemetry Collector Lambda Extension for arm64.
  # Use the same tested release as the broadcaster.
  realtime_processor_otel_collector_layer_arn = "arn:aws:lambda:${var.aws_region}:184161586896:layer:opentelemetry-collector-arm64-0_22_0:1"

  # The Python SDK header is normally formatted as:
  #   Authorization=Basic%20<base64>
  # The Collector YAML expects only the HTTP header value:
  #   Basic <base64>
  realtime_processor_grafana_otlp_authorization = replace(
    replace(var.grafana_otlp_headers, "Authorization=", ""),
    "Basic%20",
    "Basic "
  )
}

module "realtime_processor_lambda" {
  source = "../../modules/lambda"

  function_name = "${local.name_prefix}-realtime-processor"
  description   = "Consumes Kinesis records and updates realtime DynamoDB aggregates."

  runtime = "python3.12"
  handler = "src.handler.lambda_handler"

  source_dir = abspath("${path.root}/../../../.build/lambdas/realtime-processor")

  role_arn = module.iam.realtime_processor_role_arn

  memory_size = 256
  timeout     = 30

  # Keep the existing shared Python dependencies layer and add the local
  # OpenTelemetry Collector Lambda Extension.
  layers = [
    module.common_python_layer.layer_arn,
    local.realtime_processor_otel_collector_layer_arn
  ]

  environment_variables = {
    ENVIRONMENT = var.environment
    LOG_LEVEL   = "INFO"

    AGGREGATES_TABLE_NAME = module.dynamodb.realtime_aggregates_table_name
    BROADCAST_QUEUE_URL   = module.sqs.broadcast_signal_queue_url

    AGGREGATION_WINDOW_SECONDS  = "60"
    BROADCAST_WINDOW_SECONDS    = "4"
    GLOBAL_ACTIVITY_SHARD_COUNT = "10"
    TOP_METRIC_SHARD_COUNT      = "10"
    AGGREGATE_TTL_DAYS          = "2"

    OTEL_ENABLED      = "true"
    OTEL_SERVICE_NAME = "realtime-media-analytics-${var.environment}-realtime-processor"

    OTEL_RESOURCE_ATTRIBUTES = join(",", [
      "service.namespace=realtime-media-analytics",
      "deployment.environment=${var.environment}",
      "cloud.provider=aws",
      "cloud.region=${var.aws_region}"
    ])

    # Python SDK -> local Collector Extension.
    OTEL_EXPORTER_OTLP_ENDPOINT = "http://127.0.0.1:4318"
    OTEL_EXPORTER_OTLP_PROTOCOL = "http/protobuf"
    OTEL_EXPORTER_OTLP_TIMEOUT  = "1"

    OTEL_TRACES_EXPORTER  = "otlp"
    OTEL_METRICS_EXPORTER = "otlp"
    OTEL_LOGS_EXPORTER    = "none"

    # Bounded end-of-invocation handoff to localhost.
    ENABLE_OTEL_FLUSH              = "true"
    OTEL_METRIC_FLUSH_TIMEOUT_MS   = "250"
    OTEL_TRACE_FLUSH_TIMEOUT_MS    = "150"
    OTEL_METRIC_EXPORT_INTERVAL_MS = "10000"
    OTEL_BSP_SCHEDULE_DELAY_MS     = "5000"
    OTEL_BSP_MAX_EXPORT_BATCH_SIZE = "128"
    OTEL_BSP_MAX_QUEUE_SIZE        = "2048"

    # Collector configuration bundled in the Lambda deployment package.
    OPENTELEMETRY_COLLECTOR_CONFIG_URI = "/var/task/src/collector.yaml"

    # Local Collector -> Grafana Cloud.
    GRAFANA_OTLP_ENDPOINT      = var.grafana_otlp_endpoint
    GRAFANA_OTLP_AUTHORIZATION = local.realtime_processor_grafana_otlp_authorization
  }

  tags = local.tags
}

# Kinesis trigger: intentionally unchanged for the A/B comparison.
resource "aws_lambda_event_source_mapping" "realtime_processor_kinesis" {
  event_source_arn  = module.kinesis.stream_arn
  function_name     = module.realtime_processor_lambda.function_name
  starting_position = "LATEST"

  batch_size                         = 20
  maximum_batching_window_in_seconds = 1

  enabled = true
}
