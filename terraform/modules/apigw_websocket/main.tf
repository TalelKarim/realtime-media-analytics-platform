locals {
  route_statement_ids = {
    for route_key, route in var.routes :
    route_key => substr(
      replace(
        replace(
          replace(route_key, "$", ""),
          "/",
          "-"
        ),
        " ",
        "-"
      ),
      0,
      60
    )
  }
}


resource "aws_cloudwatch_log_group" "access" {
  count = var.enable_access_logs ? 1 : 0

  name              = "/aws/apigateway/${var.name}"
  retention_in_days = var.access_log_retention_in_days

  tags = var.tags
}



resource "aws_apigatewayv2_api" "this" {
  name                       = var.name
  protocol_type              = "WEBSOCKET"
  route_selection_expression = var.route_selection_expression

  tags = var.tags
}

resource "aws_apigatewayv2_integration" "this" {
  for_each = var.routes

  api_id = aws_apigatewayv2_api.this.id

  integration_type   = "AWS_PROXY"
  integration_method = "POST"
  integration_uri    = each.value.lambda_invoke_arn
}

resource "aws_apigatewayv2_route" "this" {
  for_each = var.routes

  api_id    = aws_apigatewayv2_api.this.id
  route_key = each.key
  target    = "integrations/${aws_apigatewayv2_integration.this[each.key].id}"
}

resource "aws_apigatewayv2_stage" "this" {
  api_id = aws_apigatewayv2_api.this.id

  name        = var.stage_name
  auto_deploy = var.auto_deploy

  default_route_settings {
    logging_level            = var.logging_level
    data_trace_enabled       = var.data_trace_enabled
    detailed_metrics_enabled = var.detailed_metrics_enabled
    throttling_rate_limit    = var.throttling_rate_limit
    throttling_burst_limit   = var.throttling_burst_limit
  }

  dynamic "access_log_settings" {
    for_each = var.enable_access_logs ? [1] : []

    content {
      destination_arn = aws_cloudwatch_log_group.access[0].arn

      format = jsonencode({
        requestId          = "$context.requestId"
        extendedRequestId  = "$context.extendedRequestId"
        ip                 = "$context.identity.sourceIp"
        requestTime        = "$context.requestTime"
        routeKey           = "$context.routeKey"
        eventType          = "$context.eventType"
        connectionId       = "$context.connectionId"
        status             = "$context.status"
        integrationStatus  = "$context.integrationStatus"
        integrationError   = "$context.integrationErrorMessage"
        errorMessage       = "$context.error.message"
        errorResponseType  = "$context.error.responseType"
        integrationLatency = "$context.integrationLatency"
        responseLatency    = "$context.responseLatency"
      })
    }
  }

  tags = var.tags
}

resource "aws_lambda_permission" "allow_apigateway" {
  for_each = var.routes

  statement_id  = "AllowExecutionFromWebSocket-${local.route_statement_ids[each.key]}"
  action        = "lambda:InvokeFunction"
  function_name = each.value.lambda_function_name
  principal     = "apigateway.amazonaws.com"


  source_arn = "${aws_apigatewayv2_api.this.execution_arn}/*/*"
}