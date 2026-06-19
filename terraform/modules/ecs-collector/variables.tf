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
  description = "Versioned image tag to deploy."
  type        = string
}

variable "execution_role_arn" {
  description = "ECS task execution role ARN. Used by ECS to pull image and write logs."
  type        = string
}

variable "task_role_arn" {
  description = "ECS task role ARN. Used by collector application code."
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
  description = "CloudWatch Log Group used by the collector container."
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
  description = "Maximum number of records buffered before PutRecords."
  type        = number
  default     = 100
}

variable "flush_interval_seconds" {
  description = "Maximum time before flushing buffered records."
  type        = number
  default     = 2
}

variable "sample_rate" {
  description = "Sampling rate applied by collector. 1.0 means all events, 0.01 means 1 percent."
  type        = number
  default     = 0.01
}

variable "log_level" {
  description = "Application log level."
  type        = string
  default     = "INFO"
}

variable "task_cpu" {
  description = "Fargate task CPU units."
  type        = number
  default     = 256
}

variable "task_memory" {
  description = "Fargate task memory in MiB."
  type        = number
  default     = 512
}

variable "desired_count" {
  description = "Number of collector tasks to run."
  type        = number
  default     = 0
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

variable "container_insights_enabled" {
  description = "Enable ECS Container Insights on the cluster."
  type        = bool
  default     = false
}

variable "enable_execute_command" {
  description = "Enable ECS Exec. Keep false until SSM endpoints and permissions are ready."
  type        = bool
  default     = false
}

variable "tags" {
  description = "Additional tags."
  type        = map(string)
  default     = {}
}