variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "project" {
  description = "Project name — used as prefix for all resource names"
  type        = string
  default     = "realtime-media-analytics"
}

variable "environment" {
  description = "Deployment environment"
  type        = string
  default     = "dev"
}
