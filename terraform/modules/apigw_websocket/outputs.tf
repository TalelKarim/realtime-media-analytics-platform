output "api_id" {
  description = "WebSocket API ID."
  value       = aws_apigatewayv2_api.this.id
}

output "api_endpoint" {
  description = "WebSocket API endpoint without stage."
  value       = aws_apigatewayv2_api.this.api_endpoint
}

output "execution_arn" {
  description = "WebSocket API execution ARN."
  value       = aws_apigatewayv2_api.this.execution_arn
}

output "stage_name" {
  description = "WebSocket API stage name."
  value       = aws_apigatewayv2_stage.this.name
}

output "invoke_url" {
  description = "WebSocket API invoke URL including stage."
  value       = aws_apigatewayv2_stage.this.invoke_url
}

output "management_endpoint_url" {
  description = "HTTPS endpoint used by Lambda Broadcaster for API Gateway Management API. Do not include /@connections."
  value       = replace(trimsuffix(aws_apigatewayv2_stage.this.invoke_url, "/"), "wss://", "https://")
}

output "connections_url" {
  description = "Full @connections URL shown by API Gateway console. Not used directly as boto3 endpoint_url."
  value       = "${replace(trimsuffix(aws_apigatewayv2_stage.this.invoke_url, "/"), "wss://", "https://")}/@connections"
}

output "manage_connections_arn" {
  description = "ARN used by Lambdas to manage WebSocket connections."
  value       = "${aws_apigatewayv2_api.this.execution_arn}/${aws_apigatewayv2_stage.this.name}/POST/@connections/*"
}

output "route_ids" {
  description = "Map of route keys to route IDs."
  value = {
    for route_key, route in aws_apigatewayv2_route.this :
    route_key => route.id
  }
}

output "integration_ids" {
  description = "Map of route keys to integration IDs."
  value = {
    for route_key, integration in aws_apigatewayv2_integration.this :
    route_key => integration.id
  }
}