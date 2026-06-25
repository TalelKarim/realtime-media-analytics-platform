variable "name" {
  description = "Name of the API Gateway WebSocket API."
  type        = string
}

variable "stage_name" {
  description = "Name of the WebSocket API stage."
  type        = string
}

variable "route_selection_expression" {
  description = "WebSocket route selection expression."
  type        = string
  default     = "$request.body.action"
}

variable "auto_deploy" {
  description = "Whether to automatically deploy changes to the stage."
  type        = bool
  default     = true
}

variable "routes" {
  description = "Map of WebSocket routes to Lambda integrations."
  type = map(object({
    lambda_function_name = string
    lambda_invoke_arn    = string
  }))

  validation {
    condition     = length(var.routes) > 0
    error_message = "At least one WebSocket route must be provided."
  }
}

variable "tags" {
  description = "Tags to apply to API Gateway resources."
  type        = map(string)
  default     = {}
}



variable "enable_access_logs" {
  description = "Whether to enable API Gateway WebSocket access logs."
  type        = bool
  default     = true
}

variable "access_log_retention_in_days" {
  description = "CloudWatch retention in days for API Gateway WebSocket access logs."
  type        = number
  default     = 14
}

variable "logging_level" {
  description = "WebSocket route execution logging level. Valid values: OFF, ERROR, INFO."
  type        = string
  default     = "INFO"

  validation {
    condition     = contains(["OFF", "ERROR", "INFO"], var.logging_level)
    error_message = "logging_level must be one of: OFF, ERROR, INFO."
  }
}

variable "data_trace_enabled" {
  description = "Whether to enable full request/response data tracing for WebSocket routes."
  type        = bool
  default     = false
}

variable "detailed_metrics_enabled" {
  description = "Whether to enable detailed CloudWatch metrics for WebSocket routes."
  type        = bool
  default     = true
}


variable "throttling_rate_limit" {
  description = "Default WebSocket route throttling rate limit."
  type        = number
  default     = 100
}

variable "throttling_burst_limit" {
  description = "Default WebSocket route throttling burst limit."
  type        = number
  default     = 50
}