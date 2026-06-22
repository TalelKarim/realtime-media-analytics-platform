variable "function_name" {
  description = "Name of the Lambda function."
  type        = string
}

variable "description" {
  description = "Description of the Lambda function."
  type        = string
  default     = null
}

variable "runtime" {
  description = "Lambda runtime."
  type        = string
  default     = "python3.12"
}

variable "handler" {
  description = "Lambda handler, for example src.handler.lambda_handler."
  type        = string
}

variable "source_dir" {
  description = "Path to the Lambda source directory to package."
  type        = string
}

variable "role_arn" {
  description = "IAM role ARN used by the Lambda function."
  type        = string
}

variable "memory_size" {
  description = "Amount of memory in MB."
  type        = number
  default     = 256
}

variable "timeout" {
  description = "Lambda timeout in seconds."
  type        = number
  default     = 30
}

variable "environment_variables" {
  description = "Environment variables injected into the Lambda function."
  type        = map(string)
  default     = {}
}

variable "package_excludes" {
  description = "Files or folders excluded from the Lambda zip package."
  type        = list(string)
  default = [
    ".venv/*",
    "__pycache__/*",
    "*.pyc",
    ".pytest_cache/*",
    ".DS_Store",
    "tests/*"
  ]
}

variable "layers" {
  description = "Optional Lambda layer ARNs."
  type        = list(string)
  default     = []
}

variable "architectures" {
  description = "Instruction set architecture for the Lambda function."
  type        = list(string)
  default     = ["arm64"]
}

variable "create_log_group" {
  description = "Whether this module should create the CloudWatch log group."
  type        = bool
  default     = false
}

variable "log_retention_in_days" {
  description = "CloudWatch log group retention in days."
  type        = number
  default     = 14
}

variable "tags" {
  description = "Tags applied to resources."
  type        = map(string)
  default     = {}
}