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

# ============================================================
# WebSocket custom domain managed in the same workspace/state
# ============================================================

locals {
  custom_domain_enabled = (
    var.custom_domain_name != null &&
    trim(coalesce(var.custom_domain_name, ""), " ") != ""
  )

  custom_domain_mapping_key = (
    trim(
    var.custom_domain_api_mapping_key != null
    ? var.custom_domain_api_mapping_key
    : "",
    "/"
  ) == ""
    ? null
    : trim(coalesce(var.custom_domain_api_mapping_key, ""), "/")
  )

  custom_domain_mapping_path = (
    local.custom_domain_mapping_key == null
    ? ""
    : "/${local.custom_domain_mapping_key}"
  )
}

check "custom_domain_hosted_zone" {
  assert {
    condition = (
      !local.custom_domain_enabled ||
      (
        var.hosted_zone_name != null &&
        trim(coalesce(var.hosted_zone_name, ""), " ") != ""
      )
    )
    error_message = "hosted_zone_name must be set when custom_domain_name is enabled."
  }
}

data "aws_route53_zone" "websocket" {
  count = local.custom_domain_enabled ? 1 : 0

  name         = var.hosted_zone_name
  private_zone = false
}

resource "aws_acm_certificate" "websocket" {
  count = local.custom_domain_enabled ? 1 : 0

  domain_name       = var.custom_domain_name
  validation_method = "DNS"

  tags = var.tags

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_route53_record" "websocket_certificate_validation" {
  for_each = local.custom_domain_enabled ? {
    for dvo in aws_acm_certificate.websocket[0].domain_validation_options :
    dvo.domain_name => {
      name   = dvo.resource_record_name
      type   = dvo.resource_record_type
      record = dvo.resource_record_value
    }
  } : {}

  zone_id = data.aws_route53_zone.websocket[0].zone_id
  name    = each.value.name
  type    = each.value.type
  ttl     = 60
  records = [each.value.record]
}

resource "aws_acm_certificate_validation" "websocket" {
  count = local.custom_domain_enabled ? 1 : 0

  certificate_arn = aws_acm_certificate.websocket[0].arn
  validation_record_fqdns = [
    for record in aws_route53_record.websocket_certificate_validation :
    record.fqdn
  ]
}

resource "aws_apigatewayv2_domain_name" "websocket" {
  count = local.custom_domain_enabled ? 1 : 0

  domain_name = var.custom_domain_name

  domain_name_configuration {
    certificate_arn = aws_acm_certificate_validation.websocket[0].certificate_arn
    endpoint_type   = "REGIONAL"
    security_policy = "TLS_1_2"
  }

  tags = var.tags
}

resource "aws_apigatewayv2_api_mapping" "custom_domain" {
  count = local.custom_domain_enabled ? 1 : 0

  api_id          = aws_apigatewayv2_api.this.id
  domain_name     = aws_apigatewayv2_domain_name.websocket[0].id
  stage           = aws_apigatewayv2_stage.this.name
  api_mapping_key = local.custom_domain_mapping_key
}

resource "aws_route53_record" "websocket_alias" {
  count = local.custom_domain_enabled ? 1 : 0

  zone_id = data.aws_route53_zone.websocket[0].zone_id
  name    = var.custom_domain_name
  type    = "A"

  alias {
    name                   = aws_apigatewayv2_domain_name.websocket[0].domain_name_configuration[0].target_domain_name
    zone_id                = aws_apigatewayv2_domain_name.websocket[0].domain_name_configuration[0].hosted_zone_id
    evaluate_target_health = false
  }
}
