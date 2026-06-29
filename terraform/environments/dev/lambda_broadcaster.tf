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
    ENVIRONMENT = var.environment
    LOG_LEVEL   = "INFO"

    REALTIME_AGGREGATES_TABLE_NAME   = "${local.name_prefix}-realtime-aggregates"
    WEBSOCKET_CONNECTIONS_TABLE_NAME = "${local.name_prefix}-websocket-connections"

    WEBSOCKET_MANAGEMENT_ENDPOINT = replace(module.apigw_websocket.invoke_url, "wss://", "https://")

    BROADCAST_SIGNAL_QUEUE_URL = module.sqs.broadcast_signal_queue_url
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