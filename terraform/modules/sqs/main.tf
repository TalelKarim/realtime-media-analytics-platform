locals {
  name_prefix = "${var.project}-${var.environment}"

  broadcast_queue_name = "${local.name_prefix}-broadcast-signal.fifo"
  broadcast_dlq_name   = "${local.name_prefix}-broadcast-signal-dlq.fifo"

  common_tags = merge(
    {
      Project     = var.project
      Environment = var.environment
      ManagedBy   = "terraform"
      Component   = "messaging"
      Service     = "sqs"
    },
    var.tags
  )
}

resource "aws_sqs_queue" "broadcast_signal_dlq" {
  name                        = local.broadcast_dlq_name
  fifo_queue                  = true
  content_based_deduplication = false

  message_retention_seconds = var.dlq_message_retention_seconds

  kms_master_key_id                 = var.kms_key_arn
  kms_data_key_reuse_period_seconds = var.kms_data_key_reuse_period_seconds

  tags = merge(local.common_tags, {
    Name = local.broadcast_dlq_name
    Role = "broadcast-signal-dlq"
  })
}

resource "aws_sqs_queue" "broadcast_signal" {
  name                        = local.broadcast_queue_name
  fifo_queue                  = true
  content_based_deduplication = false

  visibility_timeout_seconds = var.visibility_timeout_seconds
  message_retention_seconds  = var.message_retention_seconds
  receive_wait_time_seconds  = var.receive_wait_time_seconds

  kms_master_key_id                 = var.kms_key_arn
  kms_data_key_reuse_period_seconds = var.kms_data_key_reuse_period_seconds

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.broadcast_signal_dlq.arn
    maxReceiveCount     = var.max_receive_count
  })

  tags = merge(local.common_tags, {
    Name = local.broadcast_queue_name
    Role = "broadcast-signal"
  })
}