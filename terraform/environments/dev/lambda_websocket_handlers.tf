module "lambda_websocket_connect" {
  source = "../../modules/lambda"

  function_name = "${local.name_prefix}-websocket-connect"
  description   = "Handles API Gateway WebSocket $connect route."

  runtime = "python3.11"
  handler = "src.handler.lambda_handler"

  source_dir = "${path.root}/../../../.build/lambdas/websocket-connect-handler"

  role_arn = module.iam.websocket_connect_role_arn

  memory_size = 128
  timeout     = 10

  create_log_group = false

  environment_variables = {
    ENVIRONMENT = var.environment
    LOG_LEVEL   = "INFO"

    WEBSOCKET_CONNECTIONS_TABLE_NAME = module.dynamodb.websocket_connections_table_name
    CONNECTION_TTL_SECONDS           = "7200"
    DEFAULT_TOPIC                    = "global"
  }

  tags = var.tags
}

module "lambda_websocket_disconnect" {
  source = "../../modules/lambda"

  function_name = "${local.name_prefix}-websocket-disconnect"
  description   = "Handles API Gateway WebSocket $disconnect route."

  runtime = "python3.11"
  handler = "src.handler.lambda_handler"

  source_dir = "${path.root}/../../../.build/lambdas/websocket-disconnect-handler"

  role_arn = module.iam.websocket_disconnect_role_arn

  memory_size = 128
  timeout     = 10

  create_log_group = false

  environment_variables = {
    ENVIRONMENT = var.environment
    LOG_LEVEL   = "INFO"

    WEBSOCKET_CONNECTIONS_TABLE_NAME = module.dynamodb.websocket_connections_table_name
  }

  tags = var.tags
}

module "lambda_websocket_default" {
  source = "../../modules/lambda"

  function_name = "${local.name_prefix}-websocket-default"
  description   = "Handles API Gateway WebSocket $default route for subscribe and unsubscribe messages."

  runtime = "python3.11"
  handler = "src.handler.lambda_handler"

  source_dir = "${path.root}/../../../.build/lambdas/websocket-default-handler"

  role_arn = module.iam.websocket_default_role_arn

  memory_size = 128
  timeout     = 10

  create_log_group = false

  environment_variables = {
    ENVIRONMENT = var.environment
    LOG_LEVEL   = "INFO"

    WEBSOCKET_CONNECTIONS_TABLE_NAME = module.dynamodb.websocket_connections_table_name
  }

  tags = var.tags
}