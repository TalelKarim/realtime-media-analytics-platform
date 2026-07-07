variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "project" {
  description = "Project name — used as prefix for all resource names"
  type        = string
  default     = "realtime-media-analytics"
}

variable "environment" {
  description = "Deployment environment"
  type        = string
  default     = "dev"
}



# kinesis variables

variable "kinesis_shard_count" {
  description = "Number of provisioned Kinesis shards for the Wikimedia event stream."
  type        = number
  default     = 1
}

variable "kinesis_retention_period_hours" {
  description = "Kinesis record retention period in hours."
  type        = number
  default     = 48
}

variable "kinesis_shard_level_metrics" {
  description = "Optional enhanced shard-level metrics for Kinesis."
  type        = list(string)
  default     = []
}


# SNS Variables

variable "sns_email_subscriptions" {
  description = "Email addresses subscribed to the alerts SNS topic."
  type        = list(string)
  default     = []
}





# ECS 


# ============================================================
# Bootstrap state
# ============================================================

variable "bootstrap_tfstate_bucket_name" {
  description = "S3 bucket containing bootstrap Terraform states."
  type        = string
  default     = "realtime-media-analytics-tfstate-156358246560-us-east-1"
}

# ============================================================
# ECS Fargate Collector
# ============================================================

variable "collector_image_tag" {
  description = "Versioned collector image tag stored in ECR."
  type        = string
  default     = "dev-001"
}

variable "collector_desired_count" {
  description = "Number of collector tasks to run. Keep 0 until image is pushed."
  type        = number
  default     = 0
}

variable "collector_task_cpu" {
  description = "Collector Fargate task CPU units."
  type        = number
  default     = 256
}

variable "collector_task_memory" {
  description = "Collector Fargate task memory in MiB."
  type        = number
  default     = 512
}

variable "collector_sample_rate" {
  description = "Initial sampling rate for the collector."
  type        = number
  default     = 0.01
}

variable "collector_batch_size" {
  description = "Collector batch size for Kinesis PutRecords."
  type        = number
  default     = 100
}

variable "collector_flush_interval_seconds" {
  description = "Collector flush interval in seconds."
  type        = number
  default     = 2
}

variable "collector_log_level" {
  description = "Collector application log level."
  type        = string
  default     = "INFO"
}

# general

variable "tags" {
  description = "Common tags applied to all resources."
  type        = map(string)
  default     = {}
}



variable "quicksight_principal_arn" {
  description = "QuickSight user or group ARN that owns/manages the QuickSight assets."
  type        = string
}



variable "domain_name" {
  description = "Root domain name."
  type        = string
  default     = "talelkarimchebbi.com"
}

variable "www_domain_name" {
  description = "WWW domain name."
  type        = string
  default     = "realtimeWiki.talelkarimchebbi.com"
}

variable "github_repository" {
  description = "GitHub repository allowed to deploy the dashboard. Example: TalelKarim/talelkarim-portfolio"
  type        = string
  default     = "TalelKarim/realtime-media-analytics-platform"
}
