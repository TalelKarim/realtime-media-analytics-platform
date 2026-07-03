variable "name_prefix" {
  description = "Project name prefix."
  type        = string
}

variable "aws_account_id" {
  description = "AWS account ID."
  type        = string
}

variable "quicksight_principal_arn" {
  description = "QuickSight user or group ARN that owns/manages the assets."
  type        = string
}

variable "glue_database_name" {
  description = "Glue database containing Gold tables."
  type        = string
}

variable "datalake_bucket_name" {
  description = "Data lake bucket name."
  type        = string
}

variable "s3_kms_key_arn" {
  description = "KMS key used for S3 encryption."
  type        = string
}

variable "tags" {
  description = "Common tags."
  type        = map(string)
  default     = {}
}