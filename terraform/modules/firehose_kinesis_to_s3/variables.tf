variable "name" {
  description = "Firehose delivery stream name."
  type        = string
}

variable "kinesis_stream_arn" {
  description = "Source Kinesis Data Stream ARN."
  type        = string
}

variable "s3_bucket_arn" {
  description = "Destination S3 bucket ARN."
  type        = string
}

variable "s3_kms_key_arn" {
  description = "KMS key ARN used to encrypt delivered S3 objects."
  type        = string
}

variable "firehose_role_arn" {
  description = "IAM role ARN assumed by Amazon Data Firehose."
  type        = string
}

variable "buffering_size_mb" {
  description = "S3 buffering size in MB."
  type        = number
  default     = 64
}

variable "buffering_interval_seconds" {
  description = "S3 buffering interval in seconds."
  type        = number
  default     = 300
}

variable "log_retention_days" {
  description = "CloudWatch Logs retention in days."
  type        = number
  default     = 14
}

variable "tags" {
  description = "Common tags."
  type        = map(string)
  default     = {}
}