variable "project" {
  description = "Project name used as resource prefix."
  type        = string
}

variable "environment" {
  description = "Deployment environment."
  type        = string
}

variable "aws_region" {
  description = "AWS region."
  type        = string
}

variable "cluster_name" {
  description = "Optional ECS cluster name override."
  type        = string
  default     = null
}

variable "service_name" {
  description = "Optional ECS service name override."
  type        = string
  default     = null
}

variable "task_family" {
  description = "Optional ECS task definition family override."
  type        = string
  default     = null
}

variable "repository_url" {
  description = "ECR repository URL for the collector image."
  type        = string
}

variable "image_tag" {
  description = "Versioned collector application image tag to deploy."
  type        = string
}

variable "alloy_image" {
  description = "Full image reference for the custom Grafana Alloy sidecar image."
  type        = string
}

variable "collector_service_version" {
  description = "Version resource attribute attached to collector telemetry."
  type        = string
  default     = "unknown"
}

variable "grafana_otlp_endpoint" {
  description = "Grafana Cloud OTLP/HTTP base endpoint, normally ending in /otlp."
  type        = string
}

variable "grafana_otlp_authorization_secret_arn" {
  description = "Secrets Manager ARN containing the complete Authorization header value."
  type        = string
}

variable "execution_role_arn" {
  description = "ECS task execution role ARN."
  type        = string
}

variable "task_role_arn" {
  description = "ECS task role ARN used by collector application code."
  type        = string
}

variable "subnet_ids" {
  description = "Private subnet IDs where Fargate tasks run."
  type        = list(string)
}

variable "security_group_id" {
  description = "Security group ID attached to the collector task ENI."
  type        = string
}

variable "assign_public_ip" {
  description = "Whether to assign a public IP to the Fargate task."
  type        = bool
  default     = false
}

variable "log_group_name" {
  description = "CloudWatch Log Group shared by collector and Alloy streams."
  type        = string
}

variable "kinesis_stream_name" {
  description = "Kinesis Data Stream name where collector sends records."
  type        = string
}

variable "wikimedia_stream_url" {
  description = "Wikimedia EventStreams SSE endpoint."
  type        = string
  default     = "https://stream.wikimedia.org/v2/stream/recentchange"
}

variable "batch_size" {
  description = "Maximum number of retained events buffered before PutRecords."
  type        = number
  default     = 100
}

variable "flush_interval_seconds" {
  description = "Maximum time before flushing buffered events."
  type        = number
  default     = 2
}

variable "sample_rate" {
  description = "Deterministic application sampling rate."
  type        = number
  default     = 0.01
}

variable "log_level" {
  description = "Application log level."
  type        = string
  default     = "INFO"
}

variable "kinesis_max_retries" {
  description = "Maximum additional PutRecords retries."
  type        = number
  default     = 3
}

variable "kinesis_retry_base_sleep_seconds" {
  description = "Base backoff delay for PutRecords retries."
  type        = number
  default     = 0.5
}

variable "reconnect_sleep_seconds" {
  description = "Delay before reconnecting to Wikimedia SSE."
  type        = number
  default     = 5
}

variable "task_cpu" {
  description = "Fargate task CPU units. Use at least 512 with Alloy."
  type        = number
  default     = 512
}

variable "task_memory" {
  description = "Fargate task memory in MiB. Use at least 1024 with Alloy."
  type        = number
  default     = 1024
}

variable "collector_container_cpu" {
  description = "CPU units assigned to the Python collector container."
  type        = number
  default     = 320
}

variable "collector_container_memory_reservation" {
  description = "Collector soft memory reservation in MiB."
  type        = number
  default     = 512
}

variable "collector_container_memory" {
  description = "Collector hard memory limit in MiB."
  type        = number
  default     = 640
}

variable "alloy_container_cpu" {
  description = "CPU units assigned to Grafana Alloy."
  type        = number
  default     = 192
}

variable "alloy_container_memory_reservation" {
  description = "Alloy soft memory reservation in MiB."
  type        = number
  default     = 256
}

variable "alloy_container_memory" {
  description = "Alloy hard memory limit in MiB."
  type        = number
  default     = 384
}

variable "desired_count" {
  description = "Number of collector tasks to run. Keep 1 for this source."
  type        = number
  default     = 1
}

variable "cpu_architecture" {
  description = "CPU architecture for the Fargate task."
  type        = string
  default     = "X86_64"

  validation {
    condition     = contains(["X86_64", "ARM64"], var.cpu_architecture)
    error_message = "cpu_architecture must be X86_64 or ARM64."
  }
}

variable "deployment_minimum_healthy_percent" {
  description = "Minimum healthy percent during deployment. Keep 0 to avoid overlapping collectors."
  type        = number
  default     = 0
}

variable "deployment_maximum_percent" {
  description = "Maximum running percent during deployment. Keep 100 to avoid overlapping collectors."
  type        = number
  default     = 100
}

variable "container_insights_enabled" {
  description = "Enable ECS Container Insights on the cluster."
  type        = bool
  default     = false
}

variable "enable_execute_command" {
  description = "Enable ECS Exec."
  type        = bool
  default     = false
}

variable "tags" {
  description = "Additional tags."
  type        = map(string)
  default     = {}
}
