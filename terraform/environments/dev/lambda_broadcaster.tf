locals {
  # Community OpenTelemetry Collector Lambda Extension.
  # This ARN targets the default Lambda architecture (x86_64 / amd64).
  otel_collector_layer_arn = "arn:aws:lambda:${var.aws_region}:184161586896:layer:opentelemetry-collector-amd64-0_22_0:1"

  # The current Python SDK header is normally formatted as:
  #   Authorization=Basic%20<base64>
  # The Collector YAML expects only the HTTP header value:
  #   Basic <base64>
  grafana_otlp_authorization = replace(
    replace(var.grafana_otlp_headers, "Authorization=", ""),
    "Basic%20",
    "Basic "
  )
}

module "lambda_broadcaster" {
  source = "../../modules/lambda"

  function_name = "${local.name_prefix}-broadcaster"
  description   = "Consumes broadcast signals and pushes realtime snapshots to WebSocket clients"

  runtime = "python3.12"
  handler = "src.handler.lambda_handler"

  source_dir = "../../../.build/lambdas/broadcaster"

  role_arn = module.iam.broadcaster_role_arn

  timeout     = 30
  memory_size = 256

  # The Collector runs as a Lambda Extension in the same execution environment.
  layers = [
    local.otel_collector_layer_arn
  ]

  environment_variables = {
    OTEL_ENABLED      = "true"
    OTEL_SERVICE_NAME = "realtime-media-analytics-${var.environment}-broadcaster"

    OTEL_RESOURCE_ATTRIBUTES = join(",", [
      "service.namespace=realtime-media-analytics",
      "deployment.environment=${var.environment}",
      "cloud.provider=aws",
      "cloud.region=${var.aws_region}"
    ])

    # Python SDK -> local Collector Extension.
    # With the generic base endpoint, the SDK appends /v1/traces and /v1/metrics.
    OTEL_EXPORTER_OTLP_ENDPOINT = "http://127.0.0.1:4318"
    OTEL_EXPORTER_OTLP_PROTOCOL = "http/protobuf"
    OTEL_EXPORTER_OTLP_TIMEOUT  = "1"

    OTEL_TRACES_EXPORTER  = "otlp"
    OTEL_METRICS_EXPORTER = "otlp"
    OTEL_LOGS_EXPORTER    = "none"

    # Keep a bounded end-of-invocation handoff to localhost.
    ENABLE_OTEL_FLUSH              = "true"
    OTEL_METRIC_FLUSH_TIMEOUT_MS   = "250"
    OTEL_TRACE_FLUSH_TIMEOUT_MS    = "150"
    OTEL_METRIC_EXPORT_INTERVAL_MS = "10000"
    OTEL_BSP_SCHEDULE_DELAY_MS     = "5000"
    OTEL_BSP_MAX_EXPORT_BATCH_SIZE = "128"
    OTEL_BSP_MAX_QUEUE_SIZE        = "2048"

    # Collector Extension configuration bundled with the Lambda package.
    OPENTELEMETRY_COLLECTOR_CONFIG_URI = "/var/task/src/collector.yaml"

    # Local Collector -> Grafana Cloud.
    GRAFANA_OTLP_ENDPOINT      = var.grafana_otlp_endpoint
    GRAFANA_OTLP_AUTHORIZATION = local.grafana_otlp_authorization

    ENVIRONMENT                 = var.environment
    AGGREGATES_TABLE_NAME       = module.dynamodb.realtime_aggregates_table_name
    CONNECTIONS_TABLE_NAME      = module.dynamodb.websocket_connections_table_name
    WEBSOCKET_ENDPOINT_URL      = module.apigw_websocket.management_endpoint_url
    GLOBAL_ACTIVITY_SHARD_COUNT = 10
    TOP_METRIC_SHARD_COUNT      = 10
    TOP_WIKIS_LIMIT             = 10
    TOP_PAGES_LIMIT             = 10
    ENABLE_TOP_PAGES_TOPIC      = true
    LOG_LEVEL                   = "INFO"
  }

  tags = var.tags
}

resource "aws_lambda_event_source_mapping" "broadcaster_sqs" {
  event_source_arn = module.sqs.broadcast_signal_queue_arn
  function_name    = module.lambda_broadcaster.function_name

  batch_size = 1
  enabled    = true

  function_response_types = [
    "ReportBatchItemFailures"
  ]
}
