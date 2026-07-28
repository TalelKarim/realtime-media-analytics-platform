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


    AGGREGATES_TABLE_NAME = (
      module.dynamodb.realtime_aggregates_table_name
    )

    GLOBAL_ACTIVITY_SHARD_COUNT = "10"
    TOP_METRIC_SHARD_COUNT      = "10"

    TOP_WIKIS_LIMIT = "10"
    TOP_PAGES_LIMIT = "10"

    DYNAMODB_READ_WORKERS          = "24"
    DYNAMODB_MAX_POOL_CONNECTIONS  = "32"
    DYNAMODB_BATCH_GET_MAX_RETRIES = "5"

    CHANGE_TYPES      = "edit,new,categorize,log,external"
    NAMESPACES        = "-1,0,1,2,4,6,10,14"
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
resource "aws_iam_role_policy" "broadcast_worker" {
  name = "${local.name_prefix}-broadcast-worker-policy"
  role = aws_iam_role.broadcast_worker.id

  policy = jsonencode({
    Version = "2012-10-17"

    Statement = [
      {
        Sid    = "ConsumeBroadcastJobs"
        Effect = "Allow"

        Action = [
          "sqs:ReceiveMessage",
          "sqs:DeleteMessage",
          "sqs:ChangeMessageVisibility",
          "sqs:GetQueueAttributes",
          "sqs:GetQueueUrl"
        ]

        Resource = local.broadcast_jobs_queue_arn
      },
      {
        Sid    = "ReadBroadcastSnapshots"
        Effect = "Allow"

        Action = [
          "dynamodb:GetItem",
          "dynamodb:DescribeTable"
        ]

        Resource = local.broadcast_snapshots_table_arn
      },
      {
        Sid    = "ReadAndCleanupWebsocketState"
        Effect = "Allow"

        Action = [
          "dynamodb:Query",
          "dynamodb:GetItem",
          "dynamodb:DeleteItem",
          "dynamodb:TransactWriteItems",
          "dynamodb:DescribeTable"
        ]

        Resource = [
          local.websocket_connections_table_arn,
          local.websocket_subscriptions_table_arn
        ]
      },
      {
        Sid    = "PushToWebsocketConnections"
        Effect = "Allow"

        Action = [
          "execute-api:ManageConnections"
        ]

        Resource = local.websocket_manage_connections_arn
      },
      {
        Sid    = "UseDynamoDbKmsKey"
        Effect = "Allow"

        Action = [
          "kms:Decrypt",
          "kms:Encrypt",
          "kms:GenerateDataKey",
          "kms:DescribeKey"
        ]

        Resource = var.dynamodb_key_arn
      },
      {
        Sid    = "UseSqsKmsKey"
        Effect = "Allow"

        Action = [
          "kms:Decrypt",
          "kms:DescribeKey"
        ]

        Resource = var.sqs_key_arn
      }
    ]
  })
}


module "lambda_broadcast_worker" {
  source = "../../modules/lambda"

  function_name = "${local.name_prefix}-broadcast-worker"
  description   = "Consumes one topic-shard job and performs bounded-parallel WebSocket fan-out"

  runtime = "python3.12"
  handler = "src.handler.lambda_handler"

  source_dir = "../../../.build/lambdas/broadcast-worker"

  role_arn = module.iam.broadcast_worker_role_arn

  # 1024 MB is a good latency/cost starting point for JSON serialization plus
  # concurrent network I/O. Benchmark 1536 MB later during 1000/2000-client tests.
  timeout     = 30
  memory_size = 1024

  reserved_concurrent_executions = var.broadcast_worker_max_concurrency

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

    CONNECTIONS_TABLE_NAME = (
      module.dynamodb.websocket_connections_table_name
    )

    WEBSOCKET_ENDPOINT_URL = (
      module.apigw_websocket.management_endpoint_url
    )

    # Up to 20 Worker Lambdas can run concurrently. Each Worker opens at most
    # 16 concurrent postToConnection calls: global cap ~= 320 in-flight calls.
    MAX_POST_WORKERS           = "16"
    APIGW_MAX_POOL_CONNECTIONS = "24"

    # Short, explicit, observable retries. Botocore retries are disabled for
    # postToConnection inside the code to avoid hidden double-retry behavior.
    APIGW_MAX_ATTEMPTS          = "3"
    APIGW_CONNECT_TIMEOUT_SECONDS = "1.0"
    APIGW_READ_TIMEOUT_SECONDS    = "2.0"
    APIGW_RETRY_BASE_DELAY_MS     = "40"
    APIGW_RETRY_MAX_DELAY_MS      = "250"
    APIGW_RETRY_JITTER_RATIO      = "0.25"

    MAX_CLEANUP_WORKERS             = "8"
    DYNAMODB_MAX_POOL_CONNECTIONS   = "16"
    MAX_WEBSOCKET_PAYLOAD_BYTES     = "32000"
    MAX_FAILURE_LOGS_PER_JOB        = "20"
    # A partial delivery failure does not replay the complete shard: the next
    # live snapshot will arrive shortly and freshness must not be polluted by
    # stale duplicate sends. A total delivery outage still fails the SQS job.
    FAIL_JOB_ON_POST_ERRORS         = "false"
    FAIL_JOB_IF_NO_DELIVERIES       = "true"
    TRACE_POST_TO_CONNECTION_CALLS  = "false"
    BACKBONE_TEST_DELAY_MS          = "0"

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

    # Metrics are flushed first because freshness is the primary SLI.
    ENABLE_OTEL_FLUSH              = "true"
    OTEL_METRIC_FLUSH_TIMEOUT_MS   = "200"
    OTEL_TRACE_FLUSH_TIMEOUT_MS    = "100"
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