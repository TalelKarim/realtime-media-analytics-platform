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