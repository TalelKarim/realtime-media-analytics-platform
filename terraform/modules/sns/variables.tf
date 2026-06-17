variable "project" {
  description = "Project name used as a prefix for SNS topic names."
  type        = string
}

variable "environment" {
  description = "Deployment environment name, for example dev, staging, or prod."
  type        = string
}

variable "kms_key_id" {
  description = "Optional KMS key ID or ARN used to encrypt the SNS topic. If null, SNS uses its default configuration."
  type        = string
  default     = null
}

variable "email_subscriptions" {
  description = "List of email endpoints subscribed to the alerts topic."
  type        = list(string)
  default     = []
}

variable "tags" {
  description = "Additional tags to apply to SNS resources."
  type        = map(string)
  default     = {}
}