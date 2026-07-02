variable "database_name" {
  description = "Glue database name."
  type        = string
}

variable "datalake_bucket_name" {
  description = "S3 data lake bucket name."
  type        = string
}

variable "tags" {
  description = "Common tags."
  type        = map(string)
  default     = {}
}