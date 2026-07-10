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

  environment_variables = {

    OTEL_SERVICE_NAME = "realtime-media-analytics-${var.environment}-broadcaster"

    OTEL_RESOURCE_ATTRIBUTES = join(",", [
      "service.namespace=realtime-media-analytics",
      "deployment.environment=${var.environment}",
      "cloud.provider=aws",
      "cloud.region=${var.aws_region}"
    ])

    OTEL_EXPORTER_OTLP_ENDPOINT = var.grafana_otlp_endpoint
    OTEL_EXPORTER_OTLP_HEADERS  = var.grafana_otlp_headers
    OTEL_EXPORTER_OTLP_PROTOCOL = "http/protobuf"

    OTEL_TRACES_EXPORTER  = "otlp"
    OTEL_METRICS_EXPORTER = "otlp"
    OTEL_LOGS_EXPORTER    = "none"

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