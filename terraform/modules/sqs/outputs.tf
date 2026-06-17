output "broadcast_signal_queue_name" {
  description = "Name of the broadcast signal FIFO queue."
  value       = aws_sqs_queue.broadcast_signal.name
}

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