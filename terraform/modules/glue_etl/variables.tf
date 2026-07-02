variable "name_prefix" {
  description = "Project name prefix."
  type        = string
}

variable "datalake_bucket_name" {
  description = "Data lake bucket name."
  type        = string
}

variable "datalake_bucket_arn" {
  description = "Data lake bucket ARN."
  type        = string
}

variable "s3_kms_key_arn" {
  description = "KMS key ARN used to encrypt the data lake bucket."
  type        = string
}

variable "bronze_to_silver_script_path" {
  description = "Local path to the Bronze to Silver Glue script."
  type        = string
  default     = "../glue/jobs/bronze_to_silver/bronze_to_silver.py"
}

variable "glue_version" {
  description = "AWS Glue version."
  type        = string
  default     = "4.0"
}

variable "worker_type" {
  description = "Glue worker type."
  type        = string
  default     = "G.1X"
}

variable "number_of_workers" {
  description = "Number of Glue workers."
  type        = number
  default     = 2
}

variable "timeout_minutes" {
  description = "Glue job timeout in minutes."
  type        = number
  default     = 30
}

variable "tags" {
  description = "Common tags."
  type        = map(string)
  default     = {}
}