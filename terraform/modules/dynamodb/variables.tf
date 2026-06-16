variable "project" {
  description = "Project name used as a prefix for DynamoDB table names."
  type        = string
}

variable "environment" {
  description = "Deployment environment name, for example dev, staging, or prod."
  type        = string
}

variable "kms_key_arn" {
  description = "KMS key ARN used to encrypt DynamoDB tables."
  type        = string

  validation {
    condition     = length(var.kms_key_arn) > 0
    error_message = "kms_key_arn must not be empty."
  }
}

variable "point_in_time_recovery_enabled" {
  description = "Enable DynamoDB point-in-time recovery."
  type        = bool
  default     = false
}

variable "deletion_protection_enabled" {
  description = "Enable DynamoDB deletion protection."
  type        = bool
  default     = false
}

variable "tags" {
  description = "Additional tags to apply to DynamoDB tables."
  type        = map(string)
  default     = {}
}