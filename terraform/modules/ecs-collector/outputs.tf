output "cluster_name" {
  description = "ECS cluster name."
  value       = aws_ecs_cluster.this.name
}

output "cluster_arn" {
  description = "ECS cluster ARN."
  value       = aws_ecs_cluster.this.arn
}

output "service_name" {
  description = "ECS service name."
  value       = aws_ecs_service.this.name
}

output "service_arn" {
  description = "ECS service ARN."
  value       = aws_ecs_service.this.id
}

output "task_definition_arn" {
  description = "ECS task definition ARN."
  value       = aws_ecs_task_definition.this.arn
}

output "task_definition_family" {
  description = "ECS task definition family."
  value       = aws_ecs_task_definition.this.family
}

output "collector_container_image" {
  description = "Collector application image."
  value       = local.collector_container_image
}

output "alloy_container_image" {
  description = "Grafana Alloy sidecar image."
  value       = var.alloy_image
}