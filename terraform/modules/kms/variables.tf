variable "project" {
  description = "Project name  used as prefix for all resource names"
  type        = string
}

variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
}

variable "deletion_window_in_days" {
  description = "KMS key deletion window in days — 7 for dev, 30 for prod"
  type        = number
  default     = 7
}

variable "tags" {
  description = "Additional tags to merge into all resources"
  type        = map(string)
  default     = {}
}

