variable "project" {
  description = "Project name."
  type        = string
}

variable "environment" {
  description = "Environment name."
  type        = string
}

variable "log_group_names" {
  description = "List of CloudWatch log group names to create."
  type        = list(string)
}

variable "retention_in_days" {
  description = "CloudWatch Logs retention in days."
  type        = number
  default     = 14
}

variable "kms_key_arn" {
  description = "KMS key ARN used to encrypt CloudWatch log groups."
  type        = string
}

variable "tags" {
  description = "Additional tags."
  type        = map(string)
  default     = {}
}