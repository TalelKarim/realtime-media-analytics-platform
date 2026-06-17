variable "project" {
  description = "Project name used as a prefix for SQS queue names."
  type        = string
}

variable "environment" {
  description = "Deployment environment name, for example dev, staging, or prod."
  type        = string
}

variable "kms_key_arn" {
  description = "KMS key ARN used to encrypt SQS queues."
  type        = string

  validation {
    condition     = length(var.kms_key_arn) > 0
    error_message = "kms_key_arn must not be empty."
  }
}

variable "visibility_timeout_seconds" {
  description = "SQS visibility timeout for the broadcast signal queue."
  type        = number
  default     = 30
}

variable "message_retention_seconds" {
  description = "How long broadcast signal messages are retained in the main queue."
  type        = number
  default     = 86400
}

variable "dlq_message_retention_seconds" {
  description = "How long failed broadcast signal messages are retained in the DLQ."
  type        = number
  default     = 1209600
}

variable "receive_wait_time_seconds" {
  description = "Long polling wait time for SQS ReceiveMessage."
  type        = number
  default     = 20
}

variable "max_receive_count" {
  description = "Number of receives before a message is moved to the DLQ."
  type        = number
  default     = 3
}

variable "kms_data_key_reuse_period_seconds" {
  description = "Length of time, in seconds, for which SQS can reuse a data key."
  type        = number
  default     = 300
}

variable "tags" {
  description = "Additional tags to apply to SQS queues."
  type        = map(string)
  default     = {}
}