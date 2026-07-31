# =============================================================================
# Realtime aggregates
# =============================================================================

output "realtime_aggregates_table_name" {
  description = "Name of the realtime aggregates table."
  value       = aws_dynamodb_table.realtime_aggregates.name
}

output "realtime_aggregates_table_arn" {
  description = "ARN of the realtime aggregates table."
  value       = aws_dynamodb_table.realtime_aggregates.arn
}

# =============================================================================
# WebSocket connections
# =============================================================================

output "websocket_connections_table_name" {
  description = "Name of the WebSocket connections table."
  value       = aws_dynamodb_table.websocket_connections.name
}

output "websocket_connections_table_arn" {
  description = "ARN of the WebSocket connections table."
  value       = aws_dynamodb_table.websocket_connections.arn
}

output "websocket_connections_connection_shard_index_name" {
  description = "Name of the GSI used to query WebSocket connections by logical shard."
  value       = "connection-shard-index"
}

output "websocket_connections_connection_shard_index_arn" {
  description = "ARN of the GSI used to query WebSocket connections by logical shard."
  value       = "${aws_dynamodb_table.websocket_connections.arn}/index/connection-shard-index"
}

# =============================================================================
# WebSocket subscriptions V2
# =============================================================================

output "websocket_subscriptions_table_name" {
  description = "Name of the sharded WebSocket subscriptions table."
  value       = aws_dynamodb_table.websocket_subscriptions.name
}

output "websocket_subscriptions_table_arn" {
  description = "ARN of the sharded WebSocket subscriptions table."
  value       = aws_dynamodb_table.websocket_subscriptions.arn
}

# =============================================================================
# Broadcast snapshots V2
# =============================================================================

output "broadcast_snapshots_table_name" {
  description = "Name of the broadcast snapshots table."
  value       = aws_dynamodb_table.broadcast_snapshots.name
}

output "broadcast_snapshots_table_arn" {
  description = "ARN of the broadcast snapshots table."
  value       = aws_dynamodb_table.broadcast_snapshots.arn
}

# =============================================================================
# Alert state
# =============================================================================

output "alert_state_table_name" {
  description = "Name of the alert state table."
  value       = aws_dynamodb_table.alert_state.name
}

output "alert_state_table_arn" {
  description = "ARN of the alert state table."
  value       = aws_dynamodb_table.alert_state.arn
}

# =============================================================================
# Aggregate maps
# =============================================================================

output "table_names" {
  description = "Map of DynamoDB table names."
  value = {
    realtime_aggregates     = aws_dynamodb_table.realtime_aggregates.name
    websocket_connections   = aws_dynamodb_table.websocket_connections.name
    websocket_subscriptions = aws_dynamodb_table.websocket_subscriptions.name
    broadcast_snapshots     = aws_dynamodb_table.broadcast_snapshots.name
    alert_state             = aws_dynamodb_table.alert_state.name
  }
}

output "table_arns" {
  description = "Map of DynamoDB table ARNs."
  value = {
    realtime_aggregates     = aws_dynamodb_table.realtime_aggregates.arn
    websocket_connections   = aws_dynamodb_table.websocket_connections.arn
    websocket_subscriptions = aws_dynamodb_table.websocket_subscriptions.arn
    broadcast_snapshots     = aws_dynamodb_table.broadcast_snapshots.arn
    alert_state             = aws_dynamodb_table.alert_state.arn
  }
}