module "lambda_websocket_connect" {
  source = "../../modules/lambda"

  function_name = "${local.name_prefix}-websocket-connect"
  description   = "Handles API Gateway WebSocket $connect and creates the default sharded subscription."

  runtime = "python3.12"
  handler = "src.handler.lambda_handler"

  source_dir = "${path.root}/../../../.build/lambdas/websocket-connect-handler"

  role_arn = module.iam.websocket_connect_role_arn

  memory_size = 128
  timeout     = 10

  create_log_group = false

  layers = [
    module.websocket_python_layer.layer_arn
  ]

  environment_variables = {
    ENVIRONMENT = var.environment
    LOG_LEVEL   = "INFO"

    WEBSOCKET_CONNECTIONS_TABLE_NAME = (
      module.dynamodb.websocket_connections_table_name
    )

    WEBSOCKET_SUBSCRIPTIONS_TABLE_NAME = (
      module.dynamodb.websocket_subscriptions_table_name
    )

    SUBSCRIPTION_SHARD_COUNT = tostring(
      var.subscription_shard_count
    )

    CONNECTION_TTL_SECONDS = "7200"
    DEFAULT_TOPIC          = "global"
  }

  tags = var.tags
}

module "lambda_websocket_disconnect" {
  source = "../../modules/lambda"

  function_name = "${local.name_prefix}-websocket-disconnect"
  description   = "Handles API Gateway WebSocket $disconnect and removes all sharded subscriptions."

  runtime = "python3.12"
  handler = "src.handler.lambda_handler"

  source_dir = "${path.root}/../../../.build/lambdas/websocket-disconnect-handler"

  role_arn = module.iam.websocket_disconnect_role_arn

  memory_size = 128
  timeout     = 10

  create_log_group = false

  layers = [
    module.websocket_python_layer.layer_arn
  ]

  environment_variables = {
    ENVIRONMENT = var.environment
    LOG_LEVEL   = "INFO"

    WEBSOCKET_CONNECTIONS_TABLE_NAME = (
      module.dynamodb.websocket_connections_table_name
    )

    WEBSOCKET_SUBSCRIPTIONS_TABLE_NAME = (
      module.dynamodb.websocket_subscriptions_table_name
    )

    SUBSCRIPTION_SHARD_COUNT = tostring(
      var.subscription_shard_count
    )
  }

  tags = var.tags
}

module "lambda_websocket_default" {
  source = "../../modules/lambda"

  function_name = "${local.name_prefix}-websocket-default"
  description   = "Handles WebSocket subscribe and unsubscribe transactions."

  runtime = "python3.12"
  handler = "src.handler.lambda_handler"

  source_dir = "${path.root}/../../../.build/lambdas/websocket-default-handler"

  role_arn = module.iam.websocket_default_role_arn

  memory_size = 128
  timeout     = 10

  create_log_group = false

  layers = [
    module.websocket_python_layer.layer_arn
  ]

  environment_variables = {
    ENVIRONMENT = var.environment
    LOG_LEVEL   = "INFO"

    WEBSOCKET_CONNECTIONS_TABLE_NAME = (
      module.dynamodb.websocket_connections_table_name
    )

    WEBSOCKET_SUBSCRIPTIONS_TABLE_NAME = (
      module.dynamodb.websocket_subscriptions_table_name
    )

    SUBSCRIPTION_SHARD_COUNT = tostring(
      var.subscription_shard_count
    )

    CONNECTION_TTL_SECONDS    = "7200"
    MAX_TOPICS_PER_CONNECTION = "50"
  }

  tags = var.tags
}
