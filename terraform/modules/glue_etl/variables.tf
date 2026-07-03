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



# scheduler 

variable "enable_bronze_to_silver_schedule" {
  description = "Enable hourly Bronze to Silver Glue trigger."
  type        = bool
  default     = false
}

# gold

variable "silver_to_gold_worker_type" {
  description = "Glue worker type for Silver to Gold job."
  type        = string
  default     = "G.1X"
}

variable "silver_to_gold_number_of_workers" {
  description = "Number of Glue workers for Silver to Gold job."
  type        = number
  default     = 2
}

variable "enable_silver_to_gold_schedule" {
  description = "Enable hourly Silver to Gold Glue trigger."
  type        = bool
  default     = false
}

variable "top_pages_limit" {
  description = "Maximum number of top pages kept per hourly Gold partition."
  type        = number
  default     = 100
}