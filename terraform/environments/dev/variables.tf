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