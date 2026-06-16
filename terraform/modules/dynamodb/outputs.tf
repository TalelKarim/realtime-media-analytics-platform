output "realtime_aggregates_table_name" {
  description = "Name of the realtime aggregates table."
  value       = aws_dynamodb_table.realtime_aggregates.name
}

output "realtime_aggregates_table_arn" {
  description = "ARN of the realtime aggregates table."
  value       = aws_dynamodb_table.realtime_aggregates.arn
}

output "websocket_connections_table_name" {
  description = "Name of the WebSocket connections table."
  value       = aws_dynamodb_table.websocket_connections.name
}

output "websocket_connections_table_arn" {
  description = "ARN of the WebSocket connections table."
  value       = aws_dynamodb_table.websocket_connections.arn
}

output "alert_state_table_name" {
  description = "Name of the alert state table."
  value       = aws_dynamodb_table.alert_state.name
}

output "alert_state_table_arn" {
  description = "ARN of the alert state table."
  value       = aws_dynamodb_table.alert_state.arn
}

output "table_names" {
  description = "Map of DynamoDB table names."
  value = {
    realtime_aggregates   = aws_dynamodb_table.realtime_aggregates.name
    websocket_connections = aws_dynamodb_table.websocket_connections.name
    alert_state           = aws_dynamodb_table.alert_state.name
  }
}

output "table_arns" {
  description = "Map of DynamoDB table ARNs."
  value = {
    realtime_aggregates   = aws_dynamodb_table.realtime_aggregates.arn
    websocket_connections = aws_dynamodb_table.websocket_connections.arn
    alert_state           = aws_dynamodb_table.alert_state.arn
  }
}