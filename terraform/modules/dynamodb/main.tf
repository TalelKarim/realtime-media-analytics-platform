locals {
  name_prefix = "${var.project}-${var.environment}"

  realtime_aggregates_table_name   = "${local.name_prefix}-realtime-aggregates"
  websocket_connections_table_name = "${local.name_prefix}-websocket-connections"
  alert_state_table_name           = "${local.name_prefix}-alert-state"

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
    attribute_name = "expires_at"
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

resource "aws_dynamodb_table" "websocket_connections" {
  name         = local.websocket_connections_table_name
  billing_mode = "PAY_PER_REQUEST"

  hash_key = "connection_id"

  attribute {
    name = "connection_id"
    type = "S"
  }

  ttl {
    attribute_name = "expires_at"
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
    attribute_name = "expires_at"
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