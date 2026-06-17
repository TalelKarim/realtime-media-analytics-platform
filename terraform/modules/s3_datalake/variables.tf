variable "project" {
  description = "Project name used as a prefix for the data lake bucket."
  type        = string
}

variable "environment" {
  description = "Deployment environment name, for example dev, staging, or prod."
  type        = string
}

variable "bucket_name" {
  description = "Optional explicit S3 data lake bucket name. If null, a default name is generated."
  type        = string
  default     = null
}

variable "kms_key_arn" {
  description = "KMS key ARN used to encrypt the S3 data lake bucket."
  type        = string
}

variable "force_destroy" {
  description = "Whether Terraform can destroy the bucket even if it contains objects. Useful for dev only."
  type        = bool
  default     = true
}

variable "versioning_enabled" {
  description = "Whether S3 bucket versioning is enabled."
  type        = bool
  default     = true
}

variable "prefixes" {
  description = "Logical prefixes created as empty S3 objects for data lake readability."
  type        = list(string)
  default = [
    "bronze/",
    "silver/",
    "gold/",
    "athena-results/"
  ]
}

variable "abort_incomplete_multipart_upload_days" {
  description = "Number of days after which incomplete multipart uploads are aborted."
  type        = number
  default     = 7
}

variable "noncurrent_version_expiration_days" {
  description = "Number of days after which noncurrent object versions expire."
  type        = number
  default     = 30
}

variable "bronze_transition_to_ia_days" {
  description = "Days before bronze objects transition to STANDARD_IA."
  type        = number
  default     = 30
}

variable "bronze_transition_to_glacier_ir_days" {
  description = "Days before bronze objects transition to GLACIER_IR."
  type        = number
  default     = 90
}

variable "bronze_expiration_days" {
  description = "Days before bronze objects expire in dev."
  type        = number
  default     = 180
}

variable "silver_transition_to_ia_days" {
  description = "Days before silver objects transition to STANDARD_IA."
  type        = number
  default     = 60
}

variable "silver_expiration_days" {
  description = "Days before silver objects expire in dev."
  type        = number
  default     = 365
}

variable "gold_transition_to_ia_days" {
  description = "Days before gold objects transition to STANDARD_IA."
  type        = number
  default     = 90
}

variable "gold_expiration_days" {
  description = "Days before gold objects expire in dev."
  type        = number
  default     = 365
}

variable "tags" {
  description = "Additional tags to apply to S3 resources."
  type        = map(string)
  default     = {}
}