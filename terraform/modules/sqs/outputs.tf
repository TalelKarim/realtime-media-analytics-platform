# =============================================================================
# Existing broadcast signal queue
# =============================================================================



output "broadcast_signal_queue_url" {
  description = "URL of the broadcast signal FIFO queue."
  value       = aws_sqs_queue.broadcast_signal.url
}

output "broadcast_signal_queue_arn" {
  description = "ARN of the broadcast signal FIFO queue."
  value       = aws_sqs_queue.broadcast_signal.arn
}

output "broadcast_signal_dlq_name" {
  description = "Name of the broadcast signal dead-letter FIFO queue."
  value       = aws_sqs_queue.broadcast_signal_dlq.name
}

output "broadcast_signal_dlq_url" {
  description = "URL of the broadcast signal dead-letter FIFO queue."
  value       = aws_sqs_queue.broadcast_signal_dlq.url
}

output "broadcast_signal_dlq_arn" {
  description = "ARN of the broadcast signal dead-letter FIFO queue."
  value       = aws_sqs_queue.broadcast_signal_dlq.arn
}

# =============================================================================
# Broadcasting V2 jobs queue
# =============================================================================

output "broadcast_jobs_queue_name" {
  description = "Name of the broadcasting V2 FIFO jobs queue."
  value       = aws_sqs_queue.broadcast_jobs.name
}

output "broadcast_jobs_queue_url" {
  description = "URL of the broadcasting V2 FIFO jobs queue."
  value       = aws_sqs_queue.broadcast_jobs.url
}

output "broadcast_jobs_queue_arn" {
  description = "ARN of the broadcasting V2 FIFO jobs queue."
  value       = aws_sqs_queue.broadcast_jobs.arn
}

output "broadcast_jobs_dlq_name" {
  description = "Name of the broadcasting V2 jobs dead-letter FIFO queue."
  value       = aws_sqs_queue.broadcast_jobs_dlq.name
}

output "broadcast_jobs_dlq_url" {
  description = "URL of the broadcasting V2 jobs dead-letter FIFO queue."
  value       = aws_sqs_queue.broadcast_jobs_dlq.url
}

output "broadcast_jobs_dlq_arn" {
  description = "ARN of the broadcasting V2 jobs dead-letter FIFO queue."
  value       = aws_sqs_queue.broadcast_jobs_dlq.arn
}

# =============================================================================
# Aggregate maps
# =============================================================================

output "queue_names" {
  description = "Map of all SQS queue names managed by this module."

  value = {
    broadcast_signal     = aws_sqs_queue.broadcast_signal.name
    broadcast_signal_dlq = aws_sqs_queue.broadcast_signal_dlq.name
    broadcast_jobs       = aws_sqs_queue.broadcast_jobs.name
    broadcast_jobs_dlq   = aws_sqs_queue.broadcast_jobs_dlq.name
  }
}

output "queue_urls" {
  description = "Map of all SQS queue URLs managed by this module."

  value = {
    broadcast_signal     = aws_sqs_queue.broadcast_signal.url
    broadcast_signal_dlq = aws_sqs_queue.broadcast_signal_dlq.url
    broadcast_jobs       = aws_sqs_queue.broadcast_jobs.url
    broadcast_jobs_dlq   = aws_sqs_queue.broadcast_jobs_dlq.url
  }
}

output "queue_arns" {
  description = "Map of all SQS queue ARNs managed by this module."

  value = {
    broadcast_signal     = aws_sqs_queue.broadcast_signal.arn
    broadcast_signal_dlq = aws_sqs_queue.broadcast_signal_dlq.arn
    broadcast_jobs       = aws_sqs_queue.broadcast_jobs.arn
    broadcast_jobs_dlq   = aws_sqs_queue.broadcast_jobs_dlq.arn
  }
}