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

  layers = [
    module.common_python_layer.layer_arn
  ]

  environment_variables = {
    ENVIRONMENT = var.environment
    LOG_LEVEL   = "INFO"

    AGGREGATES_TABLE_NAME = module.dynamodb.realtime_aggregates_table_name
    BROADCAST_QUEUE_URL   = module.sqs.broadcast_signal_queue_url

    AGGREGATION_WINDOW_SECONDS  = "60"
    BROADCAST_WINDOW_SECONDS    = "2"
    GLOBAL_ACTIVITY_SHARD_COUNT = "10"
    AGGREGATE_TTL_DAYS          = "2"


    OTEL_ENABLED = "true"

    OTEL_SERVICE_NAME = "realtime-media-analytics-${var.environment}-realtime-processor"

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

    OTEL_EXPORTER_OTLP_TIMEOUT         = "3000"
    OTEL_EXPORTER_OTLP_TRACES_TIMEOUT  = "3000"
    OTEL_EXPORTER_OTLP_METRICS_TIMEOUT = "3000"
  }

  tags = local.tags
}


# kienesis triggering

resource "aws_lambda_event_source_mapping" "realtime_processor_kinesis" {
  event_source_arn  = module.kinesis.stream_arn
  function_name     = module.realtime_processor_lambda.function_name
  starting_position = "LATEST"

  batch_size                         = 100
  maximum_batching_window_in_seconds = 5

  enabled = true
}
