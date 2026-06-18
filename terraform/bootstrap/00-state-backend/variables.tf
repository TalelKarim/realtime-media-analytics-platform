variable "aws_region" {
  description = "AWS region used for the Terraform state backend bucket."
  type        = string
  default     = "us-east-1"
}

variable "tfstate_bucket_name" {
  description = "Globally unique S3 bucket name for Terraform states."
  type        = string
  default     = "realtime-media-analytics-tfstate-156358246560-us-east-1"
}

variable "tags" {
  description = "Common bootstrap tags."
  type        = map(string)
  default = {
    Project     = "realtime-media-analytics"
    Environment = "bootstrap"
    ManagedBy   = "terraform"
    Owner       = "talel-karim"
  }
}