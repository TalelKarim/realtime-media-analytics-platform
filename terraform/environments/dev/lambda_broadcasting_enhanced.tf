locals {
  broadcasting_v2_otel_collector_layer_arn = "arn:aws:lambda:${var.aws_region}:184161586896:layer:opentelemetry-collector-arm64-0_22_0:1"

  broadcasting_v2_grafana_otlp_authorization = replace(
    replace(
      var.grafana_otlp_headers,
      "Authorization=",
      ""
    ),
    "Basic%20",
    "Basic "
  )
}

module "lambda_broadcast_coordinator" {
  source = "../../modules/lambda"

  function_name = "${local.name_prefix}-broadcast-coordinator"
  description   = "Creates immutable snapshots and sharded SQS fan-out jobs"

  runtime = "python3.12"
  handler = "src.handler.lambda_handler"

  source_dir = "../../../.build/lambdas/broadcast-coordinator"

  role_arn = module.iam.broadcast_coordinator_role_arn

  timeout     = 30
  memory_size = 512

  layers = [
    local.broadcasting_v2_otel_collector_layer_arn
  ]

  environment_variables = {
    ENVIRONMENT = var.environment
    LOG_LEVEL   = "INFO"

    SNAPSHOTS_TABLE_NAME     = module.dynamodb.broadcast_snapshots_table_name
    BROADCAST_JOBS_QUEUE_URL = module.sqs.broadcast_jobs_queue_url

    SUBSCRIPTION_SHARD_COUNT = tostring(
      var.subscription_shard_count
    )

    SNAPSHOT_TTL_SECONDS = tostring(
      var.broadcast_snapshot_ttl_seconds
    )

    BACKBONE_TEST_MODE = "true"

    OTEL_ENABLED      = "true"
    OTEL_SERVICE_NAME = "realtime-media-analytics-${var.environment}-broadcast-coordinator"

    OTEL_RESOURCE_ATTRIBUTES = join(",", [
      "service.namespace=realtime-media-analytics",
      "deployment.environment=${var.environment}",
      "cloud.provider=aws",
      "cloud.region=${var.aws_region}"
    ])

    OTEL_EXPORTER_OTLP_ENDPOINT = "http://127.0.0.1:4318"
    OTEL_EXPORTER_OTLP_PROTOCOL = "http/protobuf"
    OTEL_EXPORTER_OTLP_TIMEOUT  = "1"

    OTEL_TRACES_EXPORTER  = "otlp"
    OTEL_METRICS_EXPORTER = "otlp"
    OTEL_LOGS_EXPORTER    = "none"

    ENABLE_OTEL_FLUSH              = "true"
    OTEL_METRIC_FLUSH_TIMEOUT_MS   = "250"
    OTEL_TRACE_FLUSH_TIMEOUT_MS    = "150"
    OTEL_METRIC_EXPORT_INTERVAL_MS = "10000"
    OTEL_BSP_SCHEDULE_DELAY_MS     = "5000"
    OTEL_BSP_MAX_EXPORT_BATCH_SIZE = "128"
    OTEL_BSP_MAX_QUEUE_SIZE        = "2048"

    OPENTELEMETRY_COLLECTOR_CONFIG_URI = "/var/task/src/collector.yaml"

    GRAFANA_OTLP_ENDPOINT = var.grafana_otlp_endpoint

    GRAFANA_OTLP_AUTHORIZATION = (
      local.broadcasting_v2_grafana_otlp_authorization
    )
  }

  tags = var.tags
}

module "lambda_broadcast_worker" {
  source = "../../modules/lambda"

  function_name = "${local.name_prefix}-broadcast-worker"
  description   = "Consumes one topic-shard job and queries its WebSocket subscriptions"

  runtime = "python3.12"
  handler = "src.handler.lambda_handler"

  source_dir = "../../../.build/lambdas/broadcast-worker"

  role_arn = module.iam.broadcast_worker_role_arn

  timeout     = 30
  memory_size = 512

  reserved_concurrent_executions = (
    var.broadcast_worker_max_concurrency
  )

  layers = [
    local.broadcasting_v2_otel_collector_layer_arn
  ]

  environment_variables = {
    ENVIRONMENT = var.environment
    LOG_LEVEL   = "INFO"

    SNAPSHOTS_TABLE_NAME = module.dynamodb.broadcast_snapshots_table_name

    SUBSCRIPTIONS_TABLE_NAME = (
      module.dynamodb.websocket_subscriptions_table_name
    )

    BACKBONE_TEST_MODE     = "true"
    BACKBONE_TEST_DELAY_MS = "1500"

    OTEL_ENABLED      = "true"
    OTEL_SERVICE_NAME = "realtime-media-analytics-${var.environment}-broadcast-worker"

    OTEL_RESOURCE_ATTRIBUTES = join(",", [
      "service.namespace=realtime-media-analytics",
      "deployment.environment=${var.environment}",
      "cloud.provider=aws",
      "cloud.region=${var.aws_region}"
    ])

    OTEL_EXPORTER_OTLP_ENDPOINT = "http://127.0.0.1:4318"
    OTEL_EXPORTER_OTLP_PROTOCOL = "http/protobuf"
    OTEL_EXPORTER_OTLP_TIMEOUT  = "1"

    OTEL_TRACES_EXPORTER  = "otlp"
    OTEL_METRICS_EXPORTER = "otlp"
    OTEL_LOGS_EXPORTER    = "none"

    ENABLE_OTEL_FLUSH              = "true"
    OTEL_METRIC_FLUSH_TIMEOUT_MS   = "250"
    OTEL_TRACE_FLUSH_TIMEOUT_MS    = "150"
    OTEL_METRIC_EXPORT_INTERVAL_MS = "10000"
    OTEL_BSP_SCHEDULE_DELAY_MS     = "5000"
    OTEL_BSP_MAX_EXPORT_BATCH_SIZE = "128"
    OTEL_BSP_MAX_QUEUE_SIZE        = "2048"

    OPENTELEMETRY_COLLECTOR_CONFIG_URI = "/var/task/src/collector.yaml"

    GRAFANA_OTLP_ENDPOINT = var.grafana_otlp_endpoint

    GRAFANA_OTLP_AUTHORIZATION = (
      local.broadcasting_v2_grafana_otlp_authorization
    )
  }

  tags = var.tags
}
