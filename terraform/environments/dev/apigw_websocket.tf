module "apigw_websocket" {
  source = "../../modules/apigw_websocket"

  name       = "${local.name_prefix}-websocket-api"
  stage_name = var.environment

  route_selection_expression = "$request.body.action"
  auto_deploy                = true

  routes = {
    "$connect" = {
      lambda_function_name = module.lambda_websocket_connect.function_name
      lambda_invoke_arn    = module.lambda_websocket_connect.invoke_arn
    }

    "$disconnect" = {
      lambda_function_name = module.lambda_websocket_disconnect.function_name
      lambda_invoke_arn    = module.lambda_websocket_disconnect.invoke_arn
    }

    "$default" = {
      lambda_function_name = module.lambda_websocket_default.function_name
      lambda_invoke_arn    = module.lambda_websocket_default.invoke_arn
    }
  }

  tags = var.tags
}