locals {
  name_prefix = "${var.project}-${var.environment}"

  broadcast_signal_queue_name = "${local.name_prefix}-broadcast-signal.fifo"
  broadcast_signal_dlq_name   = "${local.name_prefix}-broadcast-signal-dlq.fifo"

  # Broadcasting :
  # Coordinator -> Workers
  broadcast_jobs_queue_name = "${local.name_prefix}-broadcast-jobs.fifo"
  broadcast_jobs_dlq_name   = "${local.name_prefix}-broadcast-jobs-dlq.fifo"

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
  name                        = local.broadcast_signal_dlq_name
  fifo_queue                  = true
  content_based_deduplication = false

  message_retention_seconds = var.dlq_message_retention_seconds

  kms_master_key_id                 = var.kms_key_arn
  kms_data_key_reuse_period_seconds = var.kms_data_key_reuse_period_seconds

  tags = merge(local.common_tags, {
    Name         = local.broadcast_signal_dlq_name
    Role         = "broadcast-signal-dlq"
    Architecture = "shared"
  })
}


resource "aws_sqs_queue" "broadcast_signal" {
  name                        = local.broadcast_signal_queue_name
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
    Name         = local.broadcast_signal_queue_name
    Role         = "broadcast-signal"
    Architecture = "shared"
  })
}

# =============================================================================
# Broadcasting V2: jobs DLQ
#
# Contains jobs that could not be completed after several Worker attempts.
#
# Example failed job:
#   snapshot = SNAPSHOT#123
#   topic    = global
#   shard    = 07
# =============================================================================

resource "aws_sqs_queue" "broadcast_jobs_dlq" {
  name                        = local.broadcast_jobs_dlq_name
  fifo_queue                  = true
  content_based_deduplication = false

  message_retention_seconds = var.dlq_message_retention_seconds

  kms_master_key_id                 = var.kms_key_arn
  kms_data_key_reuse_period_seconds = var.kms_data_key_reuse_period_seconds

  tags = merge(local.common_tags, {
    Name         = local.broadcast_jobs_dlq_name
    Role         = "broadcast-jobs-dlq"
    Architecture = "coordinator-worker"
  })
}

# =============================================================================
# Broadcasting V2: jobs queue
#
# Producer:
#   Broadcast Coordinator
#
# Consumer:
#   Broadcast Worker
#
# One SQS message = one fan-out job for one topic-shard.
#
# Example:
#   MessageGroupId        = TOPIC#global#SHARD#07
#   MessageDeduplicationId = JOB#<snapshot-id>#global#07
# =============================================================================

resource "aws_sqs_queue" "broadcast_jobs" {
  name                        = local.broadcast_jobs_queue_name
  fifo_queue                  = true
  content_based_deduplication = false

  visibility_timeout_seconds = var.broadcast_jobs_visibility_timeout_seconds
  message_retention_seconds  = var.broadcast_jobs_message_retention_seconds
  receive_wait_time_seconds  = var.receive_wait_time_seconds

  kms_master_key_id                 = var.kms_key_arn
  kms_data_key_reuse_period_seconds = var.kms_data_key_reuse_period_seconds

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.broadcast_jobs_dlq.arn
    maxReceiveCount     = var.broadcast_jobs_max_receive_count
  })

  tags = merge(local.common_tags, {
    Name         = local.broadcast_jobs_queue_name
    Role         = "broadcast-jobs"
    Architecture = "coordinator-worker"
  })
}

# Restrict the jobs DLQ so that only the broadcast jobs queue
# can use it as a dead-letter queue.
resource "aws_sqs_queue_redrive_allow_policy" "broadcast_jobs_dlq" {
  queue_url = aws_sqs_queue.broadcast_jobs_dlq.url

  redrive_allow_policy = jsonencode({
    redrivePermission = "byQueue"
    sourceQueueArns = [
      aws_sqs_queue.broadcast_jobs.arn
    ]
  })
}