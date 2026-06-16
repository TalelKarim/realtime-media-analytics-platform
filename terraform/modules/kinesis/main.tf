locals {
  name_prefix = "${var.project}-${var.environment}"

  stream_name = var.stream_name != null && var.stream_name != "" ? var.stream_name : "${local.name_prefix}-wikimedia-events"

  common_tags = merge(
    {
      Name        = local.stream_name
      Project     = var.project
      Environment = var.environment
      ManagedBy   = "terraform"
      Component   = "streaming"
      Service     = "kinesis"
    },
    var.tags
  )
}

resource "aws_kinesis_stream" "this" {
  name             = local.stream_name
  shard_count      = var.shard_count
  retention_period = var.retention_period_hours

  encryption_type = "KMS"
  kms_key_id      = var.kms_key_arn

  shard_level_metrics = var.shard_level_metrics

  tags = local.common_tags
}