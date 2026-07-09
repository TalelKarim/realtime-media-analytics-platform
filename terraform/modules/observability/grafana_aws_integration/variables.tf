variable "name_prefix" {
  description = "Project/environment prefix used for IAM resources."
  type        = string
}

variable "environment" {
  description = "Environment name."
  type        = string
}

variable "grafana_aws_account_id" {
  description = "Grafana Cloud AWS account ID allowed to assume the role."
  type        = string
}

variable "grafana_external_id" {
  description = "External ID provided by Grafana Cloud."
  type        = string
  sensitive   = true
}

variable "tags" {
  description = "Common tags."
  type        = map(string)
  default     = {}
}