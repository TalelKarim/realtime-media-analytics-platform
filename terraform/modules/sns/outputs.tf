output "alerts_topic_name" {
  description = "Name of the alerts SNS topic."
  value       = aws_sns_topic.alerts.name
}

output "alerts_topic_arn" {
  description = "ARN of the alerts SNS topic."
  value       = aws_sns_topic.alerts.arn
}

output "alerts_topic_id" {
  description = "ID of the alerts SNS topic."
  value       = aws_sns_topic.alerts.id
}