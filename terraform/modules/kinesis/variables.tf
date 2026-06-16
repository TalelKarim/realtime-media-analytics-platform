variable "project" {
  description = "Project name used as a prefix for resource naming."
  type        = string
}

variable "environment" {
  description = "Deployment environment name, for example dev, staging, or prod."
  type        = string
}

variable "stream_name" {
  description = "Optional explicit Kinesis stream name. If null, a name is generated from project and environment."
  type        = string
  default     = null
}

variable "kms_key_arn" {
  description = "KMS key ARN used to encrypt the Kinesis stream."
  type        = string

  validation {
    condition     = length(var.kms_key_arn) > 0
    error_message = "kms_key_arn must not be empty."
  }
}

variable "shard_count" {
  description = "Number of provisioned shards for the stream."
  type        = number
  default     = 1

  validation {
    condition     = var.shard_count >= 1
    error_message = "shard_count must be greater than or equal to 1."
  }
}

variable "retention_period_hours" {
  description = "Kinesis record retention period in hours. Minimum is 24h."
  type        = number
  default     = 48

  validation {
    condition     = var.retention_period_hours >= 24 && var.retention_period_hours <= 8760
    error_message = "retention_period_hours must be between 24 and 8760 hours."
  }
}

variable "shard_level_metrics" {
  description = "Optional enhanced shard-level CloudWatch metrics for the stream."
  type        = list(string)
  default     = []

  validation {
    condition = alltrue([
      for metric in var.shard_level_metrics :
      contains([
        "IncomingBytes",
        "IncomingRecords",
        "OutgoingBytes",
        "OutgoingRecords",
        "WriteProvisionedThroughputExceeded",
        "ReadProvisionedThroughputExceeded",
        "IteratorAgeMilliseconds"
      ], metric)
    ])
    error_message = "shard_level_metrics contains an invalid Kinesis metric name."
  }
}

variable "tags" {
  description = "Additional tags to apply to the Kinesis stream."
  type        = map(string)
  default     = {}
}