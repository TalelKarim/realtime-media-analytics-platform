locals {
  name_prefix = "${var.project}-${var.environment}"

  realtime_aggregates_table_name     = "${local.name_prefix}-realtime-aggregates"
  websocket_connections_table_name   = "${local.name_prefix}-websocket-connections"
  websocket_subscriptions_table_name = "${local.name_prefix}-websocket-subscriptions"
  broadcast_snapshots_table_name     = "${local.name_prefix}-broadcast-snapshots"
  alert_state_table_name             = "${local.name_prefix}-alert-state"

  common_tags = merge(
    {
      Project     = var.project
      Environment = var.environment
      ManagedBy   = "terraform"
      Component   = "state"
      Service     = "dynamodb"
    },
    var.tags
  )
}

# =============================================================================
# Existing table: realtime aggregates
# =============================================================================

resource "aws_dynamodb_table" "realtime_aggregates" {
  name         = local.realtime_aggregates_table_name
  billing_mode = "PAY_PER_REQUEST"

  hash_key  = "metric_key"
  range_key = "window_key"

  attribute {
    name = "metric_key"
    type = "S"
  }

  attribute {
    name = "window_key"
    type = "S"
  }

  ttl {
    attribute_name = "ttl"
    enabled        = true
  }

  point_in_time_recovery {
    enabled = var.point_in_time_recovery_enabled
  }

  server_side_encryption {
    enabled     = true
    kms_key_arn = var.kms_key_arn
  }

  deletion_protection_enabled = var.deletion_protection_enabled

  tags = merge(local.common_tags, {
    Name = local.realtime_aggregates_table_name
    Role = "realtime-aggregates"
  })
}

# =============================================================================
# Existing table: WebSocket connections
# =============================================================================

resource "aws_dynamodb_table" "websocket_connections" {
  name         = local.websocket_connections_table_name
  billing_mode = "PAY_PER_REQUEST"

  hash_key = "connection_id"

  attribute {
    name = "connection_id"
    type = "S"
  }

  ttl {
    attribute_name = "ttl"
    enabled        = true
  }

  point_in_time_recovery {
    enabled = var.point_in_time_recovery_enabled
  }

  server_side_encryption {
    enabled     = true
    kms_key_arn = var.kms_key_arn
  }

  deletion_protection_enabled = var.deletion_protection_enabled

  tags = merge(local.common_tags, {
    Name = local.websocket_connections_table_name
    Role = "websocket-connections"
  })
}

# =============================================================================
# V2 table: WebSocket subscriptions partitioned by topic and logical shard
#
# Example:
#   topic_shard  = TOPIC#global#SHARD#07
#   connection_id = abc123
# =============================================================================

resource "aws_dynamodb_table" "websocket_subscriptions" {
  name         = local.websocket_subscriptions_table_name
  billing_mode = "PAY_PER_REQUEST"

  hash_key  = "topic_shard"
  range_key = "connection_id"

  attribute {
    name = "topic_shard"
    type = "S"
  }

  attribute {
    name = "connection_id"
    type = "S"
  }

  ttl {
    attribute_name = "ttl"
    enabled        = true
  }

  point_in_time_recovery {
    enabled = var.point_in_time_recovery_enabled
  }

  server_side_encryption {
    enabled     = true
    kms_key_arn = var.kms_key_arn
  }

  deletion_protection_enabled = var.deletion_protection_enabled

  tags = merge(local.common_tags, {
    Name         = local.websocket_subscriptions_table_name
    Role         = "websocket-subscriptions"
    Architecture = "coordinator-worker"
  })
}

# =============================================================================
# V2 table: immutable broadcast snapshots
#
# Regular snapshot:
#   snapshot_id = SNAPSHOT#1785232803000#WINDOW#1785232800000
#   topic       = global
#
# Future latest-state pointer:
#   snapshot_id = LATEST
#   topic       = global
#
# Items without the ttl attribute, such as LATEST pointers, are not expired.
# =============================================================================

resource "aws_dynamodb_table" "broadcast_snapshots" {
  name         = local.broadcast_snapshots_table_name
  billing_mode = "PAY_PER_REQUEST"

  hash_key  = "snapshot_id"
  range_key = "topic"

  attribute {
    name = "snapshot_id"
    type = "S"
  }

  attribute {
    name = "topic"
    type = "S"
  }

  ttl {
    attribute_name = "ttl"
    enabled        = true
  }

  point_in_time_recovery {
    enabled = var.point_in_time_recovery_enabled
  }

  server_side_encryption {
    enabled     = true
    kms_key_arn = var.kms_key_arn
  }

  deletion_protection_enabled = var.deletion_protection_enabled

  tags = merge(local.common_tags, {
    Name         = local.broadcast_snapshots_table_name
    Role         = "broadcast-snapshots"
    Architecture = "coordinator-worker"
  })
}

# =============================================================================
# Existing table: alert state
# =============================================================================

resource "aws_dynamodb_table" "alert_state" {
  name         = local.alert_state_table_name
  billing_mode = "PAY_PER_REQUEST"

  hash_key  = "alert_key"
  range_key = "window_key"

  attribute {
    name = "alert_key"
    type = "S"
  }

  attribute {
    name = "window_key"
    type = "S"
  }

  ttl {
    attribute_name = "ttl"
    enabled        = true
  }

  point_in_time_recovery {
    enabled = var.point_in_time_recovery_enabled
  }

  server_side_encryption {
    enabled     = true
    kms_key_arn = var.kms_key_arn
  }

  deletion_protection_enabled = var.deletion_protection_enabled

  tags = merge(local.common_tags, {
    Name = local.alert_state_table_name
    Role = "alert-state"
  })
}