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