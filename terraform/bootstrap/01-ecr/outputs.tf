output "collector_repository_name" {
  description = "Collector ECR repository name."
  value       = aws_ecr_repository.collector.name
}

output "collector_repository_url" {
  description = "Collector ECR repository URL."
  value       = aws_ecr_repository.collector.repository_url
}

output "collector_repository_arn" {
  description = "Collector ECR repository ARN."
  value       = aws_ecr_repository.collector.arn
}